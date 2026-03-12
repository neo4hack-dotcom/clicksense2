import json
import re
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


class _CommandRecorderClient:
    def __init__(self):
        self.commands = []

    def query(self, sql, settings=None):
        _ = sql
        _ = settings
        return _FakeResult(rows=[], cols=[])

    def command(self, sql):
        self.commands.append(sql)
        return None


class _WriterCleanupClient(_CommandRecorderClient):
    def __init__(self, tables):
        super().__init__()
        self.tables = list(tables)

    def query(self, sql, settings=None):
        _ = settings
        q = " ".join(str(sql).split()).upper()
        if q.startswith("SHOW TABLES FROM"):
            return _FakeResult(rows=[(t,) for t in self.tables], cols=["name"])
        return _FakeResult(rows=[], cols=[])


class _DQClient:
    def __init__(self, columns):
        self.columns = columns

    def query(self, sql, settings=None):
        _ = settings
        q = " ".join(str(sql).split())
        if q.startswith("DESCRIBE TABLE"):
            return _FakeResult(rows=[(c, "String") for c in self.columns], cols=["name", "type"])
        if "count() AS total" in q and "approx_distinct" in q:
            return _FakeResult(rows=[(1000, 42, 120)], cols=["total", "null_count", "approx_distinct"])
        if "min(length(" in q and "avg(length(" in q:
            return _FakeResult(
                rows=[(8, 1, 120, 14.5, 2, 6, 18, 12, 4)],
                cols=[
                    "empty_count", "min_length", "max_length", "avg_length", "very_long_count",
                    "whitespace_padded_count", "all_caps_count", "numeric_string_count", "email_like_count",
                ],
            )
        if "lower(toString(" in q and "sentinel" not in q:
            return _FakeResult(rows=[(5,)], cols=["sentinel_count"])
        if "GROUP BY" in q and "ORDER BY cnt DESC LIMIT 10" in q:
            return _FakeResult(rows=[("A", 120), ("B", 80), ("C", 40)], cols=["val", "cnt"])
        # Fallback for any optional date/time-volume query paths
        return _FakeResult(rows=[(0,)], cols=["v"])

    def command(self, sql):
        _ = sql
        return None


class _FakeHttpResponse:
    def __init__(self, *, ok=True, status_code=200, payload=None, text=""):
        self.ok = ok
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.text = text

    def json(self):
        return self._payload


@pytest.fixture()
def _preserve_llm_config():
    original = dict(server.llm_config)
    yield
    server.llm_config.clear()
    server.llm_config.update(original)


@pytest.fixture(autouse=True)
def _reset_model_cache():
    server._model_context_cache.clear()
    server._writer_sessions.clear()
    with server._dq_prepared_runs_lock:
        server._dq_prepared_runs.clear()
    with server._data_analyst_sessions_lock:
        server._data_analyst_sessions.clear()
    with server._data_wrangling_sessions_lock:
        server._data_wrangling_sessions.clear()
    yield
    server._model_context_cache.clear()
    server._writer_sessions.clear()
    with server._dq_prepared_runs_lock:
        server._dq_prepared_runs.clear()
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


def test_agent_final_answer_strips_actionable_recommendations_section(client, monkeypatch):
    monkeypatch.setattr(server, "get_clickhouse_client", lambda: _OkClient())

    def fake_call(system_prompt, messages, temperature=0.7, language=None):
        _ = system_prompt
        _ = temperature
        _ = language
        user_msg = messages[-1]["content"]
        if user_msg == "Build the plan.":
            return json.dumps({"plan_steps": ["Immediate finish"], "reasoning": "Enough"})
        if user_msg == "Proceed with the next step.":
            return json.dumps(
                {
                    "action": "finish",
                    "reasoning": "Sufficient evidence for this test",
                    "final_answer": (
                        "## Résumé exécutif\n"
                        "Vue globale.\n\n"
                        "## Deep Analysis et insights issus des résultats\n"
                        "- Insight clé.\n\n"
                        "## Recommandations actionnables\n"
                        "- Action 1\n"
                        "- Action 2\n\n"
                        "## Conclusion finale détaillée\n"
                        "Conclusion détaillée."
                    ),
                }
            )
        if user_msg in {"Synthesise the final answer.", "Final answer now."}:
            return "Fallback answer."
        raise AssertionError(f"Unexpected call: {user_msg!r}")

    monkeypatch.setattr(server, "_call_llm", fake_call)
    resp = client.post("/api/agent", json=_base_agent_payload(maxSteps=2))
    assert resp.status_code == 200
    payload = resp.get_json()
    final_answer = payload["final_answer"]
    assert "Recommandations actionnables" not in final_answer
    assert "Deep Analysis et insights issus des résultats" in final_answer
    assert "Conclusion finale détaillée" in final_answer


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


def test_summarize_executive_supports_custom_count(client, monkeypatch):
    captured = {"prompt": ""}

    def fake_call(system_prompt, messages, temperature=0.7, language=None):
        _ = temperature
        _ = language
        captured["prompt"] = messages[-1]["content"]
        bullets = [
            {"point": f"Point {i+1}", "risk": i % 2 == 0, "severity": "medium" if i % 2 == 0 else "info"}
            for i in range(10)
        ]
        return json.dumps({"preamble": "Synthèse", "bullets": bullets})

    monkeypatch.setattr(server, "_call_llm", fake_call)
    resp = client.post(
        "/api/summarize_executive",
        json={
            "text": "Analyse longue avec éléments fonctionnels et quantitatifs.",
            "lang": "fr",
            "count": 10,
            "functional_focus": True,
        },
    )
    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["requested_count"] == 10
    assert payload["actual_count"] == 10
    assert len(payload["bullets"]) == 10
    assert "Priorise fortement la lecture FONCTIONNELLE" in captured["prompt"]


def test_summarize_executive_backfills_when_llm_returns_few_bullets(client, monkeypatch):
    def fake_call(system_prompt, messages, temperature=0.7, language=None):
        _ = system_prompt
        _ = messages
        _ = temperature
        _ = language
        return json.dumps({
            "preamble": "Fallback",
            "bullets": [{"point": "Unique point", "risk": False, "severity": "info"}],
        })

    monkeypatch.setattr(server, "_call_llm", fake_call)
    text = (
        "La marge chute sur le segment B2B. "
        "Le coût d'acquisition augmente sur le canal paid. "
        "Le churn reste stable malgré une hausse des tickets de support. "
        "La qualité des leads se dégrade sur la période récente."
    )
    resp = client.post(
        "/api/summarize_executive",
        json={"text": text, "lang": "fr", "count": 7},
    )
    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["requested_count"] == 7
    assert payload["actual_count"] == 7
    assert len(payload["bullets"]) == 7


def test_clickhouse_writer_security_blocks_non_bot_drop_and_multi_drop():
    ok, reason = server._cw_is_sql_safe("DROP TABLE users")
    assert ok is False
    assert "BOT_" in reason

    ok_multi, reason_multi = server._cw_is_sql_safe("DROP TABLE BOT_tmp, users")
    assert ok_multi is False
    assert "multi-cibles" in reason_multi

    ok_bot, _ = server._cw_is_sql_safe("DROP TABLE IF EXISTS BOT_tmp")
    assert ok_bot is True


def test_clickhouse_writer_cleanup_drops_only_bot_tables(client, monkeypatch):
    recorder = _CommandRecorderClient()
    monkeypatch.setattr(server, "get_clickhouse_client", lambda: recorder)

    session_id = "writer-cleanup-1"
    server._writer_sessions[session_id] = {
        "created_tables": ["BOT_tmp_sales", "users", "BOT_ETL_stage"],
        "database": "default",
    }

    resp = client.post(
        "/api/agents/clickhouse-writer/cleanup",
        json={"session_id": session_id},
    )
    assert resp.status_code == 200
    payload = resp.get_json()
    assert sorted(payload["dropped"]) == ["BOT_ETL_stage", "BOT_tmp_sales"]
    assert payload["skipped_non_bot"] == ["users"]
    assert len(recorder.commands) == 2
    assert all("BOT_" in cmd for cmd in recorder.commands)


def test_clickhouse_writer_cleanup_also_drops_preexisting_bot_tables(client, monkeypatch):
    recorder = _WriterCleanupClient(["BOT_preexisting_a", "users", "BOT_preexisting_b"])
    monkeypatch.setattr(server, "get_clickhouse_client", lambda: recorder)

    session_id = "writer-cleanup-2"
    server._writer_sessions[session_id] = {
        "created_tables": ["BOT_tmp_sales"],
        "database": "default",
    }

    resp = client.post(
        "/api/agents/clickhouse-writer/cleanup",
        json={"session_id": session_id},
    )
    assert resp.status_code == 200
    payload = resp.get_json()
    assert sorted(payload["dropped"]) == ["BOT_preexisting_a", "BOT_preexisting_b", "BOT_tmp_sales"]
    assert payload["not_found"] == []
    assert payload["skipped_non_bot"] == []
    assert len(recorder.commands) == 3
    assert all("BOT_" in cmd for cmd in recorder.commands)


def test_clickhouse_writer_done_status_can_drop_specific_preexisting_bot_table(client, monkeypatch):
    recorder = _WriterCleanupClient(["BOT_old_one", "BOT_keep_me"])
    monkeypatch.setattr(server, "get_clickhouse_client", lambda: recorder)

    session_id = "writer-done-cleanup-1"
    server._writer_sessions[session_id] = {
        "status": "done",
        "database": "default",
        "max_actions": 12,
        "action_log": [],
        "technical_retries": 0,
        "max_technical_retries": 3,
        "created_tables": [],
        "replan_log": [],
        "synthesis": {"conclusion": "ok"},
        "plan": {"steps": []},
    }

    resp = client.post(
        "/api/agents/clickhouse-writer/chat",
        json={
            "session_id": session_id,
            "params": {"database": "default"},
            "messages": [{"role": "user", "content": "drop BOT_old_one"}],
        },
    )
    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["status"] == "done"
    assert payload["cleanup_done"] is True
    assert payload["tables_dropped"] == ["BOT_old_one"]
    assert "BOT_keep_me" in payload["remaining_bot_tables"]
    assert len(recorder.commands) == 1
    assert "BOT_old_one" in recorder.commands[0]


def test_extract_llm_content_supports_common_http_shapes():
    payload_openai_text = {"choices": [{"text": "hello from choices.text"}]}
    assert server._extract_llm_content(payload_openai_text) == "hello from choices.text"

    payload_structured = {
        "choices": [
            {
                "message": {
                    "content": [
                        {"type": "text", "text": "part 1"},
                        {"type": "text", "text": "part 2"},
                    ]
                }
            }
        ]
    }
    assert "part 1" in server._extract_llm_content(payload_structured)
    assert "part 2" in server._extract_llm_content(payload_structured)

    payload_n8n_nested = [{"data": {"output": "hello from n8n list payload"}}]
    assert server._extract_llm_content(payload_n8n_nested) == "hello from n8n list payload"


def test_call_llm_local_http_accepts_choices_text(monkeypatch, _preserve_llm_config):
    monkeypatch.setitem(server.llm_config, "provider", "local_http")
    monkeypatch.setitem(server.llm_config, "model", "test-model")
    monkeypatch.setitem(server.llm_config, "baseUrl", "http://localhost:8000/v1/chat/completions")
    monkeypatch.setitem(server.llm_config, "apiKey", "")
    monkeypatch.setitem(server.llm_config, "maxOutputTokens", 256)

    def fake_post(url, **kwargs):
        _ = url
        _ = kwargs
        payload = {"choices": [{"text": "{\"ok\": true}"}]}
        return _FakeHttpResponse(ok=True, status_code=200, payload=payload, text=json.dumps(payload))

    monkeypatch.setattr(server, "_http_post", fake_post)
    content = server._call_llm(
        "System prompt",
        [{"role": "user", "content": "hello"}],
        temperature=0.1,
    )
    assert content == "{\"ok\": true}"


def test_call_llm_n8n_accepts_list_payload(monkeypatch, _preserve_llm_config):
    monkeypatch.setitem(server.llm_config, "provider", "n8n")
    monkeypatch.setitem(server.llm_config, "model", "test-model")
    monkeypatch.setitem(server.llm_config, "baseUrl", "https://example.com/webhook/test")
    monkeypatch.setitem(server.llm_config, "apiKey", "")
    monkeypatch.setitem(server.llm_config, "maxOutputTokens", 256)

    def fake_post(url, **kwargs):
        _ = url
        _ = kwargs
        payload = [{"output": "{\"quality\": \"ok\"}"}]
        return _FakeHttpResponse(ok=True, status_code=200, payload=payload, text=json.dumps(payload))

    monkeypatch.setattr(server, "_http_post", fake_post)
    content = server._call_llm(
        "System prompt",
        [{"role": "user", "content": "hello"}],
        temperature=0.1,
    )
    assert content == "{\"quality\": \"ok\"}"


def test_parse_llm_json_repairs_common_malformed_prefix_and_trailing_comma():
    malformed = '{n "action": "query",\n"reasoning": "ok",\n}'
    parsed = server._parse_llm_json(malformed)
    assert isinstance(parsed, dict)
    assert parsed.get("action") == "query"
    assert parsed.get("reasoning") == "ok"


def test_parse_llm_json_repairs_unescaped_newline_in_string():
    malformed = '{\n"action": "query",\n"reasoning": "line1\nline2"\n}'
    parsed = server._parse_llm_json(malformed)
    assert isinstance(parsed, dict)
    assert parsed.get("action") == "query"
    assert "line1" in str(parsed.get("reasoning", ""))
    assert "line2" in str(parsed.get("reasoning", ""))


def test_parse_llm_json_accepts_single_object_wrapped_in_list():
    wrapped = '[{"action":"query","reasoning":"ok"}]'
    parsed = server._parse_llm_json(wrapped)
    assert isinstance(parsed, dict)
    assert parsed.get("action") == "query"


def test_parse_llm_json_rejects_non_object_root_arrays():
    with pytest.raises(ValueError):
        server._parse_llm_json('["a", "b", "c"]')


def test_parse_llm_json_supports_key_value_plaintext_format():
    payload = (
        "action: query\n"
        "reasoning: Base check\n"
        "sql: SELECT count() FROM events"
    )
    parsed = server._parse_llm_json(payload)
    assert isinstance(parsed, dict)
    assert parsed.get("action") == "query"
    assert "SELECT count()" in str(parsed.get("sql", ""))


def test_parse_llm_json_supports_single_quoted_python_dict():
    payload = "{'action': 'query', 'reasoning': 'ok', 'sql': 'SELECT 1'}"
    parsed = server._parse_llm_json(payload)
    assert isinstance(parsed, dict)
    assert parsed.get("action") == "query"
    assert parsed.get("reasoning") == "ok"


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


def test_data_quality_plan_estimates_token_aware_llm_calls(client, monkeypatch):
    columns = [f"col_{i}" for i in range(1, 15)]
    monkeypatch.setattr(server, "get_clickhouse_client", lambda: _DQClient(columns))
    monkeypatch.setattr(server, "_get_model_context_limit", lambda: 4096)
    monkeypatch.setattr(server, "_get_effective_context_limit", lambda: 1500)

    resp = client.post(
        "/api/data-quality/plan",
        json={"table": "events", "columns": columns, "sample_size": 10000},
    )
    assert resp.status_code == 200
    payload = resp.get_json()

    assert payload["status"] == "awaiting_llm_approval"
    assert payload["approval_required"] is True
    assert payload["prepared_run_id"].startswith("dq_")
    assert payload["columns_count"] == len(columns)
    assert payload["llm_plan"]["estimated_calls"] >= 2
    assert payload["llm_plan"]["estimated_total_synthesis_tokens"] > 0
    assert len(payload["llm_plan"]["columns_per_call"]) == payload["llm_plan"]["estimated_calls"]


def test_data_quality_analyze_requires_approval_then_runs_prepared_batches(client, monkeypatch):
    columns = [f"col_{i}" for i in range(1, 13)]
    monkeypatch.setattr(server, "get_clickhouse_client", lambda: _DQClient(columns))
    monkeypatch.setattr(server, "_get_model_context_limit", lambda: 4096)
    monkeypatch.setattr(server, "_get_effective_context_limit", lambda: 1500)

    llm_calls = {"count": 0}

    def _fake_llm(system_prompt, messages, temperature=0.7, language=None):
        _ = system_prompt
        _ = temperature
        _ = language
        llm_calls["count"] += 1
        content = messages[-1]["content"]
        names = []
        for name in re.findall(r'"column"\s*:\s*"([^"]+)"', content):
            if name not in names:
                names.append(name)
        if not names:
            names = columns[:4]
        return json.dumps(
            {
                "summary": "Batch assessed.",
                "quality_score": 88,
                "columns": [
                    {"column": c, "quality_score": 90, "issues": [], "insights": "Looks consistent."}
                    for c in names
                ],
                "recommendations": ["Standardize sentinel values."],
            }
        )

    monkeypatch.setattr(server, "_call_llm", _fake_llm)

    plan_resp = client.post(
        "/api/data-quality/plan",
        json={"table": "events", "columns": columns, "sample_size": 10000},
    )
    assert plan_resp.status_code == 200
    plan = plan_resp.get_json()
    run_id = plan["prepared_run_id"]

    blocked = client.post("/api/data-quality/analyze", json={"prepared_run_id": run_id})
    assert blocked.status_code == 400
    blocked_payload = blocked.get_json()
    assert blocked_payload["approval_required"] is True

    ok = client.post(
        "/api/data-quality/analyze",
        json={"prepared_run_id": run_id, "llm_approval": "OUI"},
    )
    assert ok.status_code == 200
    payload = ok.get_json()
    assert payload["table"] == "events"
    assert payload["analysis"]["summary"]
    assert len(payload["analysis"]["columns"]) == len(columns)
    assert payload["llm_plan"]["executed_calls"] == plan["llm_plan"]["estimated_calls"]
    assert llm_calls["count"] == payload["llm_plan"]["executed_calls"]

    expired = client.post(
        "/api/data-quality/analyze",
        json={"prepared_run_id": run_id, "llm_approval": "OUI"},
    )
    assert expired.status_code == 404


def test_get_embedding_local_http_uses_same_connection_and_embedding_model(monkeypatch):
    captured = {"url": "", "json": None, "auth": ""}

    def _fake_post(url, **kwargs):
        captured["url"] = url
        captured["json"] = kwargs.get("json")
        captured["auth"] = (kwargs.get("headers") or {}).get("Authorization", "")
        return _FakeHttpResponse(
            ok=True,
            status_code=200,
            payload={"data": [{"embedding": [0.1, 0.2, 0.3]}]},
        )

    monkeypatch.setattr(server, "_http_post", _fake_post)

    vec = server._get_embedding(
        "hello",
        rag_cfg={"embeddingModel": "bge-m3"},
        llm_cfg={
            "provider": "local_http",
            "baseUrl": "http://localhost:1234/v1/chat/completions",
            "apiKey": "tok",
            "model": "chat-model",
        },
    )
    assert vec == [0.1, 0.2, 0.3]
    assert captured["url"] == "http://localhost:1234/v1/embeddings"
    assert captured["json"]["model"] == "bge-m3"
    assert captured["auth"] == "Bearer tok"


def test_embedding_models_local_http_uses_llm_override_base(client, monkeypatch):
    captured = {"url": "", "auth": ""}

    def _fake_get(url, **kwargs):
        captured["url"] = url
        captured["auth"] = (kwargs.get("headers") or {}).get("Authorization", "")
        return _FakeHttpResponse(
            ok=True,
            status_code=200,
            payload={"data": [{"id": "bge-m3"}, {"id": "text-embedding-3-small"}]},
        )

    monkeypatch.setattr(server, "_http_get", _fake_get)

    resp = client.post(
        "/api/rag/embedding-models",
        json={
            "llm": {
                "provider": "local_http",
                "baseUrl": "http://localhost:8000/v1/chat/completions",
                "apiKey": "abc",
            }
        },
    )
    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["models"] == ["bge-m3", "text-embedding-3-small"]
    assert captured["url"] == "http://localhost:8000/v1/models"
    assert captured["auth"] == "Bearer abc"


def test_test_embedding_accepts_unsaved_llm_and_rag_overrides(client, monkeypatch):
    captured = {"model": "", "url": ""}

    def _fake_post(url, **kwargs):
        captured["url"] = url
        captured["model"] = (kwargs.get("json") or {}).get("model", "")
        return _FakeHttpResponse(
            ok=True,
            status_code=200,
            payload={"data": [{"embedding": [0.9, 0.1]}]},
        )

    monkeypatch.setattr(server, "_http_post", _fake_post)

    resp = client.post(
        "/api/rag/test-embedding",
        json={
            "ragConfig": {"embeddingModel": "bge-small"},
            "llm": {
                "provider": "local_http",
                "baseUrl": "http://localhost:9000/v1/chat/completions",
                "model": "chat-only-model",
            },
        },
    )
    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["success"] is True
    assert payload["dims"] == 2
    assert payload["model"] == "bge-small"
    assert captured["model"] == "bge-small"
    assert captured["url"] == "http://localhost:9000/v1/embeddings"
