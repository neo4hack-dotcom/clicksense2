import json
import sys
from pathlib import Path

import pytest

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import server


class _FakeResult:
    def __init__(self, rows, cols):
        self.result_rows = rows
        self.column_names = cols


class _OkClient:
    def query(self, sql, settings=None):
        _ = settings
        return _FakeResult(rows=[(1, "2024-01-01")], cols=["metric", "event_date"])

    def command(self, sql):
        _ = sql
        return None


class _FailUnknownTableClient:
    def query(self, sql, settings=None):
        _ = sql
        _ = settings
        raise Exception("Unknown table 'missing_table'")

    def command(self, sql):
        _ = sql
        raise Exception("Unknown table 'missing_table'")


class _LargeRowsClient:
    def query(self, sql, settings=None):
        _ = sql
        _ = settings
        rows = [(i % 11, float(i) * 1.5, f"seg_{i % 5}") for i in range(600)]
        return _FakeResult(rows=rows, cols=["client_id", "amount", "segment"])

    def command(self, sql):
        _ = sql
        return None


@pytest.fixture(autouse=True)
def _reset_model_cache():
    server._model_context_cache.clear()
    with server._data_analyst_sessions_lock:
        server._data_analyst_sessions.clear()
    with server._data_wrangling_sessions_lock:
        server._data_wrangling_sessions.clear()
    yield
    server._model_context_cache.clear()
    with server._data_analyst_sessions_lock:
        server._data_analyst_sessions.clear()
    with server._data_wrangling_sessions_lock:
        server._data_wrangling_sessions.clear()


@pytest.fixture()
def client(monkeypatch):
    monkeypatch.setattr(
        server,
        "read_db",
        lambda: {
            "knowledge_folders": [],
            "table_mappings": [],
            "fk_relations": [],
            "table_metadata": [],
            "saved_queries": [],
            "query_history": [],
            "users": [{"id": 1, "name": "Default User"}],
        },
    )
    server.app.config["TESTING"] = True
    with server.app.test_client() as test_client:
        yield test_client


def _base_agent_payload(**overrides):
    payload = {
        "question": "Analyse la tendance des événements",
        "schema": {
            "events": [
                {"name": "event_date", "type": "Date"},
                {"name": "metric", "type": "Int32"},
            ]
        },
        "tableMetadata": {"events": {"description": "Event facts table"}},
        "maxSteps": 6,
        "use_knowledge_base": True,
        "use_knowledge_agent": False,
    }
    payload.update(overrides)
    return payload


@pytest.mark.parametrize(
    "knowledge_mode_raw,use_kb_raw,use_agent_raw,expected",
    [
        ("kb_context_once", True, False, ("kb_context_once", True, False)),
        ("kb_agentic", True, False, ("kb_agentic", True, True)),
        ("schema_only", True, True, ("schema_only", False, False)),
        ("minimal", True, False, ("minimal", False, True)),
        ("", False, True, ("minimal", False, True)),
    ],
)
def test_resolve_knowledge_mode_flags_supports_unified_modes(
    knowledge_mode_raw, use_kb_raw, use_agent_raw, expected
):
    resolved = server._resolve_knowledge_mode_flags(
        knowledge_mode_raw=knowledge_mode_raw,
        use_knowledge_base_raw=use_kb_raw,
        use_knowledge_agent_raw=use_agent_raw,
    )
    assert resolved == expected


def test_agent_retry_budget_caps_total_attempts(client, monkeypatch):
    monkeypatch.setattr(server, "get_clickhouse_client", lambda: _FailUnknownTableClient())

    def fake_call(system_prompt, messages, temperature=0.7, language=None):
        _ = system_prompt
        _ = temperature
        _ = language
        user_msg = messages[-1]["content"]
        if user_msg == "Build the plan.":
            return json.dumps({"plan_steps": ["Baseline query"], "reasoning": "Start simple"})
        if user_msg == "Review and revise now.":
            return json.dumps(
                {
                    "judgement": "off_track",
                    "guidance": "Keep trying simple checks",
                    "should_finish_early": False,
                    "updated_plan_steps": ["Retry with basic select"],
                }
            )
        if user_msg == "Proceed with the next step.":
            return json.dumps(
                {
                    "action": "query",
                    "reasoning": "Need baseline",
                    "hypothesis": "Table exists",
                    "expected_signal": "Count returns rows",
                    "sql": "SELECT count() FROM missing_table",
                }
            )
        if user_msg == "Generate the corrective query.":
            return json.dumps(
                {
                    "action": "query",
                    "reasoning": "Retry technical issue",
                    "sql": "SELECT count() FROM missing_table",
                }
            )
        if user_msg in {"Synthesise the final answer.", "Final answer now."}:
            return "Final: unable to retrieve data due to repeated technical failures."
        raise AssertionError(f"Unexpected LLM call payload: {user_msg!r}")

    monkeypatch.setattr(server, "_call_llm", fake_call)

    resp = client.post("/api/agent", json=_base_agent_payload(maxSteps=6))
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["technical_retries_used"] == 3
    assert data["max_total_query_attempts"] == 9
    assert data["query_attempts_used"] == 9
    assert len(data["steps"]) == 6


def test_agent_context_injection_once_and_disabled_mode(client, monkeypatch):
    monkeypatch.setattr(server, "get_clickhouse_client", lambda: _OkClient())

    prompts_standard = []

    def fake_call_standard(system_prompt, messages, temperature=0.7, language=None):
        _ = temperature
        _ = language
        user_msg = messages[-1]["content"]
        if user_msg == "Build the plan.":
            return json.dumps({"plan_steps": ["Baseline", "Finish"], "reasoning": "2 steps"})
        if user_msg == "Proceed with the next step.":
            prompts_standard.append(system_prompt)
            if len(prompts_standard) == 1:
                return json.dumps(
                    {
                        "action": "query",
                        "reasoning": "Run baseline",
                        "hypothesis": "Data is available",
                        "expected_signal": "At least one row",
                        "sql": "SELECT count() AS metric FROM events",
                    }
                )
            return json.dumps(
                {
                    "action": "finish",
                    "reasoning": "Enough evidence",
                    "final_answer": "Done.",
                }
            )
        if user_msg == "Review and revise now.":
            return json.dumps(
                {
                    "judgement": "on_track",
                    "guidance": "Proceed",
                    "should_finish_early": False,
                    "updated_plan_steps": ["Finish"],
                }
            )
        if user_msg in {"Synthesise the final answer.", "Final answer now."}:
            return "Done."
        raise AssertionError(f"Unexpected call: {user_msg!r}")

    monkeypatch.setattr(server, "_call_llm", fake_call_standard)
    resp_standard = client.post(
        "/api/agent",
        json=_base_agent_payload(maxSteps=4, use_knowledge_base=True, use_knowledge_agent=False),
    )
    assert resp_standard.status_code == 200
    assert len(prompts_standard) >= 2
    assert "DATABASE SCHEMA:" in prompts_standard[0]
    assert "Static schema/metadata/knowledge were already injected earlier in this run." in prompts_standard[1]
    assert "DATABASE SCHEMA:" not in prompts_standard[1]

    prompts_knowledge_agent = []

    def fake_call_knowledge_agent(system_prompt, messages, temperature=0.7, language=None):
        _ = temperature
        _ = language
        user_msg = messages[-1]["content"]
        if user_msg == "Build the plan.":
            return json.dumps({"plan_steps": ["Baseline", "Finish"], "reasoning": "2 steps"})
        if user_msg == "Proceed with the next step.":
            prompts_knowledge_agent.append(system_prompt)
            if len(prompts_knowledge_agent) == 1:
                return json.dumps(
                    {
                        "action": "query",
                        "reasoning": "Run baseline",
                        "hypothesis": "Data is available",
                        "expected_signal": "At least one row",
                        "sql": "SELECT count() AS metric FROM events",
                    }
                )
            return json.dumps(
                {
                    "action": "finish",
                    "reasoning": "Enough evidence",
                    "final_answer": "Done.",
                }
            )
        if user_msg in {"Synthesise the final answer.", "Final answer now."}:
            return "Done."
        if user_msg == "Review and revise now.":
            return json.dumps(
                {
                    "judgement": "on_track",
                    "guidance": "Proceed",
                    "should_finish_early": False,
                    "updated_plan_steps": ["Finish"],
                }
            )
        raise AssertionError(f"Unexpected call: {user_msg!r}")

    monkeypatch.setattr(server, "_call_llm", fake_call_knowledge_agent)
    resp_knowledge_agent = client.post(
        "/api/agent",
        json=_base_agent_payload(maxSteps=4, use_knowledge_base=True, use_knowledge_agent=True),
    )
    assert resp_knowledge_agent.status_code == 200
    assert len(prompts_knowledge_agent) >= 1
    assert "Static schema/metadata/knowledge injection is disabled for this run." in prompts_knowledge_agent[0]
    assert "DATABASE SCHEMA:" not in prompts_knowledge_agent[0]


def test_agent_synthesis_has_context_overflow_fallback(client, monkeypatch):
    monkeypatch.setattr(server, "get_clickhouse_client", lambda: _OkClient())
    synthesis_calls = {"count": 0}

    def fake_call(system_prompt, messages, temperature=0.7, language=None):
        _ = system_prompt
        _ = temperature
        _ = language
        user_msg = messages[-1]["content"]
        if user_msg == "Build the plan.":
            return json.dumps({"plan_steps": ["Single query"], "reasoning": "Minimal"})
        if user_msg == "Proceed with the next step.":
            return json.dumps(
                {
                    "action": "query",
                    "reasoning": "Run one query",
                    "hypothesis": "There is data",
                    "expected_signal": "One row",
                    "sql": "SELECT count() AS metric FROM events",
                }
            )
        if user_msg == "Synthesise the final answer.":
            synthesis_calls["count"] += 1
            raise Exception("context window exceeded")
        if user_msg == "Final answer now.":
            return "Fallback final answer after compact synthesis."
        if user_msg == "Review and revise now.":
            return json.dumps(
                {
                    "judgement": "on_track",
                    "guidance": "Proceed",
                    "should_finish_early": False,
                    "updated_plan_steps": ["Finish"],
                }
            )
        raise AssertionError(f"Unexpected call: {user_msg!r}")

    monkeypatch.setattr(server, "_call_llm", fake_call)
    resp = client.post("/api/agent", json=_base_agent_payload(maxSteps=1))
    assert resp.status_code == 200
    data = resp.get_json()
    assert "Fallback final answer" in data["final_answer"]
    assert synthesis_calls["count"] == 1


def test_api_query_can_enforce_simple_compat(client, monkeypatch):
    monkeypatch.setattr(server, "get_clickhouse_client", lambda: _OkClient())
    resp = client.post(
        "/api/query",
        json={
            "query": "SELECT countIf(metric > 0) FROM events",
            "enforce_simple_compat": True,
        },
    )
    assert resp.status_code == 500
    payload = resp.get_json()
    assert payload["error_class"] == "simple_compat"


def test_data_analyst_session_queue_when_auto_run_disabled(client):
    resp = client.post(
        "/api/agents/ai-data-analyst/chat",
        json={
            "messages": [{"role": "user", "content": "Analyse revenue by week"}],
            "params": {"auto_run": "no", "max_steps": 4},
        },
    )
    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["session_id"]
    assert payload["running"] is False
    assert payload["pending_user_inputs"] == 1
    assert "Message queued. Click Run to start." in payload["content"]


def test_data_analyst_session_pause_note_then_resume(client, monkeypatch):
    status_resp = client.post(
        "/api/agents/ai-data-analyst/chat",
        json={"control": "status", "params": {"auto_run": "yes"}},
    )
    assert status_resp.status_code == 200
    session_id = status_resp.get_json()["session_id"]

    pause_resp = client.post(
        "/api/agents/ai-data-analyst/chat",
        json={"control": "pause", "session_id": session_id, "params": {"auto_run": "yes"}},
    )
    assert pause_resp.status_code == 200
    assert pause_resp.get_json()["status"] == "paused"

    note_text = "Use only the B2B segment and keep date filter explicit."
    note_resp = client.post(
        "/api/agents/ai-data-analyst/chat",
        json={
            "session_id": session_id,
            "messages": [{"role": "user", "content": note_text}],
            "params": {"auto_run": "yes", "memory_token_budget": 240},
        },
    )
    assert note_resp.status_code == 200
    note_payload = note_resp.get_json()
    assert note_payload["pending_user_inputs"] == 0
    assert "Context saved (paused)" in note_payload["content"]
    assert note_text[:20] in note_payload["memory_summary"]

    started = {"count": 0}

    def _fake_start_worker(session):
        started["count"] += 1
        session["running"] = True
        session["status"] = "running"
        return True

    monkeypatch.setattr(server, "_da_start_worker_if_needed", _fake_start_worker)

    resume_resp = client.post(
        "/api/agents/ai-data-analyst/chat",
        json={
            "control": "resume",
            "session_id": session_id,
            "messages": [{"role": "user", "content": "Now compute monthly churn for 2025"}],
            "params": {"auto_run": "yes"},
        },
    )
    assert resume_resp.status_code == 200
    resume_payload = resume_resp.get_json()
    assert started["count"] == 1
    assert resume_payload["status"] == "running"
    assert "Session resumed. Agent run started." in resume_payload["content"]


def test_data_analyst_compose_question_trims_memory_and_notes():
    session = {
        "params": {"memory_token_budget": 260, "memory_turn_limit": 8},
        "memory_summary": "Recent discussion summary: conversion impacted by channel mix.",
        "paused_notes": ["note_1", "note_2", "note_3", "note_4", "note_5"],
    }
    question = "What changed in conversion rate this month compared to the previous one?"
    combined = server._da_compose_question(session, question)
    assert combined.startswith("What changed in conversion rate")
    assert "note_1" not in combined
    assert "note_2" not in combined
    assert "note_5" in combined

    heavy_session = {
        "params": {"memory_token_budget": 120, "memory_turn_limit": 8},
        "memory_summary": "A" * 6000,
        "paused_notes": [],
    }
    heavy_combined = server._da_compose_question(heavy_session, question)
    assert len(heavy_combined) <= 600


def test_execute_sql_guarded_condenses_large_result_sets():
    out = server._execute_sql_guarded(
        "SELECT client_id, amount, segment FROM big_table",
        read_only=True,
        client=_LargeRowsClient(),
    )
    assert out["ok"] is True
    assert out["total_rows"] == 600
    assert "Condensed summary applied for token safety." in out["summary"]
    assert "Descriptive stats computed on a" in out["summary"]


def test_resolve_sql_memory_placeholders_supports_last_alias_and_unknown_refs():
    memory = {
        "artifacts": {
            "step1": {"id_sets": {"client_id": [101, 202, 303]}},
        },
        "order": ["step1"],
    }
    sql = (
        "SELECT * FROM sales "
        "WHERE client_id IN ({{step1.client_id}}) "
        "OR client_id IN ({{last.client_id}})"
    )
    resolved = server._resolve_sql_memory_placeholders(sql, memory)
    assert "{{" not in resolved
    assert "101, 202, 303" in resolved

    with pytest.raises(ValueError):
        server._resolve_sql_memory_placeholders(
            "SELECT * FROM sales WHERE client_id IN ({{step99.client_id}})",
            memory,
        )


def test_wrangling_finalize_plan_filters_unknown_columns():
    columns = [
        {"name": "id", "type": "Int64"},
        {"name": "email", "type": "String"},
        {"name": "amount", "type": "Float64"},
    ]
    raw_plan = {
        "plan_steps": ["check nulls", "check nulls", "inspect outliers"],
        "focus_columns": ["email", "unknown_col", "amount"],
        "sql_checks": [
            {"name": "Null check", "sql": "SELECT count() FROM t LIMIT 100"},
            {"name": "Bad row", "sql": ""},
        ],
    }
    final = server._dw_finalize_plan(raw_plan, columns, max_steps=12)
    assert final["plan_steps"] == ["check nulls", "inspect outliers"]
    assert final["focus_columns"] == ["email", "amount"]
    assert len(final["sql_checks"]) == 1


def test_wrangling_detect_batch_anomalies_flags_duplicate_id_and_bad_email():
    rows = [
        {"id": 1, "email": "ok@example.com", "amount": 10},
        {"id": 1, "email": "bad@@example", "amount": 11},
    ]
    anomalies = server._dw_detect_batch_anomalies(
        rows,
        table_name="customers",
        line_offset=100,
        column_types={"id": "Int64", "email": "String", "amount": "Float64"},
        focus_columns={"email", "amount"},
        column_state={},
        primary_id_column="id",
        primary_id_seen=set(),
        date_pairs=[],
    )
    issue_types = {a["issue_type"] for a in anomalies}
    assert "duplicate_primary_id" in issue_types
    assert "invalid_email_format" in issue_types
