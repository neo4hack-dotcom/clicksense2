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


@pytest.fixture(autouse=True)
def _reset_model_cache():
    server._model_context_cache.clear()
    yield
    server._model_context_cache.clear()


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
