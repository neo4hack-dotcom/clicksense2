import copy
import sys
from pathlib import Path

import pytest

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import server


@pytest.fixture()
def client(monkeypatch):
    mem_db = {
        "users": [{"id": 1, "name": "Default User"}],
        "saved_queries": [],
        "query_history": [],
        "table_metadata": [],
        "knowledge_folders": [],
        "table_mappings": [],
        "fk_relations": [],
        "agent_manager_workflows": [],
        "agent_manager_runs": [],
    }

    def fake_read_db():
        return copy.deepcopy(mem_db)

    def fake_write_db(data):
        mem_db.clear()
        mem_db.update(copy.deepcopy(data))

    monkeypatch.setattr(server, "read_db", fake_read_db)
    monkeypatch.setattr(server, "write_db", fake_write_db)
    monkeypatch.setattr(server, "_am_start_scheduler_if_needed", lambda: None)

    with server._agent_manager_runtime_lock:
        server._agent_manager_runtime.clear()

    server.app.config["TESTING"] = True
    with server.app.test_client() as test_client:
        yield test_client


def _base_workflow_payload():
    return {
        "name": "Daily KPI orchestrator",
        "description": "Runs analyst then dictionary sync",
        "objective": "Deliver daily KPI interpretation",
        "default_input": "Analyse yesterday KPI trends",
        "enabled": True,
        "schedule": {
            "mode": "interval",
            "interval_minutes": 30,
            "timezone": "Europe/Paris",
        },
        "steps": [
            {
                "agent_id": "ai-data-analyst",
                "title": "KPI analysis",
                "prompt": "Analyse KPI trends and anomalies.",
                "params": {"max_steps": 4},
                "halt_on_error": True,
            }
        ],
    }


def test_agent_manager_workflow_crud(client):
    res_create = client.post("/api/agent-manager/workflows", json=_base_workflow_payload())
    assert res_create.status_code == 201
    created = res_create.get_json()["workflow"]
    assert created["name"] == "Daily KPI orchestrator"
    assert created["schedule"]["mode"] == "interval"
    assert created["next_run_at"]
    assert len(created["steps"]) == 1

    workflow_id = created["id"]
    res_list = client.get("/api/agent-manager/workflows")
    assert res_list.status_code == 200
    workflows = res_list.get_json()["workflows"]
    assert len(workflows) == 1
    assert workflows[0]["id"] == workflow_id

    res_update = client.put(
        f"/api/agent-manager/workflows/{workflow_id}",
        json={
            "name": "Daily KPI orchestrator v2",
            "enabled": False,
            "schedule": {"mode": "disabled"},
            "steps": _base_workflow_payload()["steps"],
        },
    )
    assert res_update.status_code == 200
    updated = res_update.get_json()["workflow"]
    assert updated["name"] == "Daily KPI orchestrator v2"
    assert updated["enabled"] is False
    assert updated["next_run_at"] is None


def test_agent_manager_run_and_stop_lifecycle(client, monkeypatch):
    monkeypatch.setattr(server, "_am_start_run_thread", lambda run_id: True)

    res_create = client.post("/api/agent-manager/workflows", json=_base_workflow_payload())
    workflow_id = res_create.get_json()["workflow"]["id"]

    res_run_1 = client.post(
        f"/api/agent-manager/workflows/{workflow_id}/run",
        json={"input": "Manual run input", "trigger": "manual"},
    )
    assert res_run_1.status_code == 200
    run_id = res_run_1.get_json()["run_id"]
    assert run_id

    # Should refuse a second active run for the same workflow.
    res_run_2 = client.post(
        f"/api/agent-manager/workflows/{workflow_id}/run",
        json={"input": "Second run", "trigger": "manual"},
    )
    assert res_run_2.status_code == 409

    res_stop = client.post(f"/api/agent-manager/runs/{run_id}/stop")
    assert res_stop.status_code == 200
    stopped = res_stop.get_json()["run"]
    assert stopped["stop_requested"] is True

    res_get = client.get(f"/api/agent-manager/runs/{run_id}")
    assert res_get.status_code == 200
    full_run = res_get.get_json()["run"]
    assert full_run["status"] in {"stopped", "stopping"}

    # After stop, a new run can be queued.
    res_run_3 = client.post(
        f"/api/agent-manager/workflows/{workflow_id}/run",
        json={"input": "Third run", "trigger": "manual"},
    )
    assert res_run_3.status_code == 200


def test_agent_manager_scheduler_tick_enqueues_due_workflows(monkeypatch):
    due_workflow_id = "wf-due-1"
    mem_db = {
        "users": [{"id": 1, "name": "Default User"}],
        "saved_queries": [],
        "query_history": [],
        "table_metadata": [],
        "knowledge_folders": [],
        "table_mappings": [],
        "fk_relations": [],
        "agent_manager_workflows": [
            {
                "id": due_workflow_id,
                "name": "Due workflow",
                "description": "",
                "objective": "",
                "default_input": "",
                "enabled": True,
                "schedule": {"mode": "interval", "interval_minutes": 10, "timezone": "UTC", "daily_time": "09:00"},
                "steps": [{"id": "s1", "agent_id": "ai-data-analyst", "title": "step", "prompt": "run"}],
                "created_at": "2026-01-01T00:00:00+00:00",
                "updated_at": "2026-01-01T00:00:00+00:00",
                "last_run_at": None,
                "next_run_at": "2020-01-01T00:00:00+00:00",
            }
        ],
        "agent_manager_runs": [],
    }

    called_workflow_ids = []

    def fake_read_db():
        return copy.deepcopy(mem_db)

    def fake_write_db(data):
        mem_db.clear()
        mem_db.update(copy.deepcopy(data))

    def fake_enqueue(workflow_id, trigger, input_text=""):
        called_workflow_ids.append((workflow_id, trigger, input_text))
        return {"id": "run-1", "workflow_name": "Due workflow"}, "", 200

    monkeypatch.setattr(server, "read_db", fake_read_db)
    monkeypatch.setattr(server, "write_db", fake_write_db)
    monkeypatch.setattr(server, "_am_enqueue_run", fake_enqueue)

    server._am_scheduler_tick()
    assert called_workflow_ids
    assert called_workflow_ids[0][0] == due_workflow_id
    assert called_workflow_ids[0][1] == "scheduled"

