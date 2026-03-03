import os
import json
import requests as http_requests
from datetime import datetime, timezone
from urllib.parse import urlparse
from dotenv import load_dotenv
from flask import Flask, request, jsonify, send_from_directory

load_dotenv()

PORT = int(os.environ.get("PORT", 3000))
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DIST_DIR = os.path.join(BASE_DIR, "dist")

app = Flask(__name__, static_folder=DIST_DIR, static_url_path="")


# ---------------------------------------------------------------------------
# CORS – allow the Vite dev server (different port) to call the API
# ---------------------------------------------------------------------------
@app.after_request
def add_cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
    return response


@app.route("/api", methods=["OPTIONS"])
@app.route("/api/<path:path>", methods=["OPTIONS"])
def options_handler(path=""):
    return "", 204


# ---------------------------------------------------------------------------
# JSON file database
# ---------------------------------------------------------------------------
DB_DIR = os.path.join(BASE_DIR, ".data")
os.makedirs(DB_DIR, exist_ok=True)
DB_FILE = os.path.join(DB_DIR, "app.json")

# Migrate old app.json from root if it exists
OLD_DB_FILE = os.path.join(BASE_DIR, "app.json")
if os.path.exists(OLD_DB_FILE) and not os.path.exists(DB_FILE):
    os.rename(OLD_DB_FILE, DB_FILE)

DEFAULT_DB = {
    "users": [{"id": 1, "name": "Default User"}],
    "saved_queries": [],
    "query_history": [],
    "table_metadata": [],
}


def read_db():
    try:
        if os.path.exists(DB_FILE):
            with open(DB_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        print(f"Error reading DB: {e}")
    return {**DEFAULT_DB}


def write_db(data):
    try:
        with open(DB_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"Error writing DB: {e}")


if not os.path.exists(DB_FILE):
    write_db(DEFAULT_DB)


# ---------------------------------------------------------------------------
# In-memory configuration (persists for the lifetime of the process)
# ---------------------------------------------------------------------------
clickhouse_config = {
    "host": os.environ.get("CLICKHOUSE_HOST", "http://localhost:8123"),
    "username": os.environ.get("CLICKHOUSE_USER", "default"),
    "password": os.environ.get("CLICKHOUSE_PASSWORD", ""),
    "database": os.environ.get("CLICKHOUSE_DB", "default"),
}

llm_config = {
    "provider": "ollama",
    "model": "llama3",
    "ollamaUrl": "http://localhost:11434",
    "httpUrl": "http://localhost:1234",
    "apiKey": "",
}

knowledge_base = ""


# ---------------------------------------------------------------------------
# ClickHouse helper
# ---------------------------------------------------------------------------
def _parse_clickhouse_url(url: str) -> dict:
    """Parse a URL like http://localhost:8123 into host/port/secure parts."""
    parsed = urlparse(url)
    return {
        "host": parsed.hostname or "localhost",
        "port": parsed.port or 8123,
        "secure": parsed.scheme == "https",
    }


def get_clickhouse_client(cfg=None):
    if cfg is None:
        cfg = clickhouse_config
    try:
        import clickhouse_connect  # noqa: PLC0415
    except ImportError as exc:
        raise RuntimeError(
            "clickhouse-connect is not installed. Run: pip install clickhouse-connect"
        ) from exc
    parts = _parse_clickhouse_url(cfg["host"])
    return clickhouse_connect.get_client(
        host=parts["host"],
        port=parts["port"],
        username=cfg["username"],
        password=cfg["password"],
        database=cfg["database"],
        secure=parts["secure"],
    )


def _rows_to_dicts(result) -> list:
    """Convert a clickhouse_connect QueryResult to a list of JSON-serialisable dicts."""
    cols = result.column_names
    rows = []
    for row in result.result_rows:
        record = {}
        for col, val in zip(cols, row):
            if isinstance(val, (int, float, str, bool)) or val is None:
                record[col] = val
            else:
                record[col] = str(val)
        rows.append(record)
    return rows


# ---------------------------------------------------------------------------
# Configuration endpoints
# ---------------------------------------------------------------------------
@app.route("/api/config", methods=["GET"])
def get_config():
    return jsonify(
        {
            "clickhouseConfig": clickhouse_config,
            "llmConfig": llm_config,
            "knowledgeBase": knowledge_base,
        }
    )


@app.route("/api/config", methods=["POST"])
def update_config():
    global clickhouse_config, llm_config, knowledge_base
    data = request.get_json()
    if data.get("clickhouse"):
        clickhouse_config.update(data["clickhouse"])
    if data.get("llm"):
        llm_config.update(data["llm"])
    if "knowledge" in data:
        knowledge_base = data["knowledge"]
    return jsonify({"success": True})


# ---------------------------------------------------------------------------
# ClickHouse connection test
# ---------------------------------------------------------------------------
@app.route("/api/clickhouse/test", methods=["POST"])
def test_clickhouse():
    data = request.get_json()
    try:
        client = get_clickhouse_client(
            {
                "host": data["host"],
                "username": data["username"],
                "password": data["password"],
                "database": data["database"],
            }
        )
        client.query("SELECT 1")
        return jsonify({"success": True})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ---------------------------------------------------------------------------
# LLM endpoints
# ---------------------------------------------------------------------------
@app.route("/api/llm/test", methods=["POST"])
def test_llm():
    data = request.get_json()
    provider = data.get("provider")
    try:
        if provider == "ollama":
            resp = http_requests.get(
                f"{data['ollamaUrl']}/api/tags", timeout=10
            )
            if not resp.ok:
                raise Exception(f"Ollama error: {resp.status_code}")
        elif provider == "http":
            headers = {}
            if data.get("apiKey"):
                headers["Authorization"] = f"Bearer {data['apiKey']}"
            resp = http_requests.get(
                f"{data['httpUrl']}/v1/models", headers=headers, timeout=10
            )
            if not resp.ok:
                raise Exception(f"HTTP error: {resp.status_code}")
        return jsonify({"success": True})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/llm/models", methods=["GET"])
def get_llm_models():
    try:
        if llm_config["provider"] == "ollama":
            resp = http_requests.get(
                f"{llm_config['ollamaUrl']}/api/tags", timeout=10
            )
            if not resp.ok:
                raise Exception(f"Ollama error: {resp.status_code}")
            data = resp.json()
            models = [m["name"] for m in data.get("models", [])]
            return jsonify({"models": models})
        elif llm_config["provider"] == "http":
            headers = {}
            if llm_config.get("apiKey"):
                headers["Authorization"] = f"Bearer {llm_config['apiKey']}"
            resp = http_requests.get(
                f"{llm_config['httpUrl']}/v1/models", headers=headers, timeout=10
            )
            if not resp.ok:
                raise Exception(f"HTTP error: {resp.status_code}")
            data = resp.json()
            models = [m["id"] for m in data.get("data", [])]
            return jsonify({"models": models})
        else:
            return jsonify({"models": []})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
@app.route("/api/schema", methods=["GET"])
def get_schema():
    try:
        client = get_clickhouse_client()
        # Use currentDatabase() to avoid SQL-injection from the config value
        result = client.query(
            "SELECT table, name, type FROM system.columns"
            " WHERE database = currentDatabase()"
            " ORDER BY table, name"
        )
        rows = _rows_to_dicts(result)
        schema: dict = {}
        for row in rows:
            tbl = row["table"]
            if tbl not in schema:
                schema[tbl] = []
            schema[tbl].append({"name": row["name"], "type": row["type"]})
        return jsonify({"schema": schema})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ---------------------------------------------------------------------------
# Users
# ---------------------------------------------------------------------------
@app.route("/api/users", methods=["GET"])
def get_users():
    db = read_db()
    return jsonify(db["users"])


# ---------------------------------------------------------------------------
# Table metadata
# ---------------------------------------------------------------------------
@app.route("/api/tables/metadata", methods=["GET"])
def get_table_metadata():
    db = read_db()
    result = {}
    for row in db["table_metadata"]:
        result[row["table_name"]] = {
            "description": row["description"],
            "is_favorite": bool(row["is_favorite"]),
        }
    return jsonify(result)


@app.route("/api/tables/metadata", methods=["POST"])
def update_table_metadata():
    data = request.get_json()
    table_name = data["table_name"]
    description = data.get("description", "")
    is_favorite = 1 if data.get("is_favorite") else 0

    db = read_db()
    idx = next(
        (i for i, m in enumerate(db["table_metadata"]) if m["table_name"] == table_name),
        None,
    )
    if idx is not None:
        db["table_metadata"][idx]["description"] = description
        db["table_metadata"][idx]["is_favorite"] = is_favorite
    else:
        db["table_metadata"].append(
            {
                "table_name": table_name,
                "description": description,
                "is_favorite": is_favorite,
            }
        )
    write_db(db)
    return jsonify({"success": True})


# ---------------------------------------------------------------------------
# Query history
# ---------------------------------------------------------------------------
@app.route("/api/history", methods=["POST"])
def add_history():
    data = request.get_json()
    db = read_db()
    new_id = max((h["id"] for h in db["query_history"]), default=0) + 1
    db["query_history"].append(
        {
            "id": new_id,
            "user_id": data["user_id"],
            "query_text": data["query_text"],
            "sql": data["sql"],
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    write_db(db)
    return jsonify({"success": True})


@app.route("/api/history/<int:user_id>", methods=["GET"])
def get_history(user_id):
    db = read_db()
    history = [h for h in db["query_history"] if h["user_id"] == user_id]
    history.sort(key=lambda h: h["created_at"], reverse=True)
    return jsonify(history[:20])


# ---------------------------------------------------------------------------
# Saved queries (dashboard)
# ---------------------------------------------------------------------------
@app.route("/api/saved_queries", methods=["POST"])
def add_saved_query():
    data = request.get_json()
    db = read_db()
    new_id = max((q["id"] for q in db["saved_queries"]), default=0) + 1
    db["saved_queries"].append(
        {
            "id": new_id,
            "user_id": data["user_id"],
            "name": data["name"],
            "sql": data["sql"],
            "config": json.dumps(data.get("config", {})),
            "visual_type": data.get("visual_type", "table"),
        }
    )
    write_db(db)
    return jsonify({"success": True})


@app.route("/api/saved_queries/<int:user_id>", methods=["GET"])
def get_saved_queries(user_id):
    db = read_db()
    queries = [q for q in db["saved_queries"] if q["user_id"] == user_id]
    result = []
    for q in queries:
        q_copy = {**q}
        try:
            q_copy["config"] = json.loads(q_copy["config"])
        except Exception:
            q_copy["config"] = {}
        result.append(q_copy)
    return jsonify(result)


@app.route("/api/saved_queries/<int:query_id>", methods=["DELETE"])
def delete_saved_query(query_id):
    db = read_db()
    db["saved_queries"] = [q for q in db["saved_queries"] if q["id"] != query_id]
    write_db(db)
    return jsonify({"success": True})


# ---------------------------------------------------------------------------
# Query execution (with guardrails)
# ---------------------------------------------------------------------------
@app.route("/api/query", methods=["POST"])
def execute_query():
    data = request.get_json()
    query = data["query"]
    try:
        client = get_clickhouse_client()
        result = client.query(
            query,
            settings={
                "max_execution_time": 15,
                "max_rows_to_read": 1_000_000_000,
                "max_bytes_to_read": 50_000_000_000,
                "readonly": 1,
            },
        )
        rows = _rows_to_dicts(result)
        return jsonify({"data": rows})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ---------------------------------------------------------------------------
# AI chat (SQL generation)
# ---------------------------------------------------------------------------
@app.route("/api/chat", methods=["POST"])
def chat():
    data = request.get_json()
    messages = data["messages"]
    schema = data.get("schema", {})
    table_metadata = data.get("tableMetadata", {})

    system_prompt = f"""
      You are an expert ClickHouse data analyst.
      Your goal is to help the user query their database.

      Here is the database schema:
      {json.dumps(schema, indent=2)}

      Here is the table metadata (functional descriptions):
      {json.dumps(table_metadata, indent=2)}

      Here is the functional knowledge base to help you understand the business context:
      {knowledge_base}

      CRITICAL INSTRUCTIONS FOR CLICKHOUSE:
      - Use advanced ClickHouse functions when appropriate to answer business questions efficiently.
      - For funnels, use windowFunnel().
      - For retention, use retention().
      - For pattern matching, use sequenceMatch().
      - For cross-selling or arrays, use arrayJoin().
      - For latest status, use argMax().
      - For fast top trends, use topK().
      - For unique visitors on large datasets, prefer uniqHLL12() over count(distinct).
      - For response times, use quantilesTiming().
      - For JSON parsing, use JSONExtract().
      - For conditional pivots, use sumIf(), countIf(), etc.
      - For A/B testing, use studentTTest() or welchTTest().
      - For geospatial, use geoDistance().
      - Always write highly optimized SQL.

      When the user asks a question, you should provide a valid ClickHouse SQL query to answer it.
      Return ONLY a JSON object with the following structure:
      {{
        "sql": "SELECT ...",
        "explanation": "A brief explanation of what the query does and which advanced ClickHouse function was used.",
        "suggestedVisual": "table" | "bar" | "line" | "pie"
      }}
      Do not include markdown formatting like ```json. Just the raw JSON object.
    """

    formatted_messages = [{"role": m["role"], "content": m["content"]} for m in messages]

    try:
        if llm_config["provider"] == "http":
            headers = {"Content-Type": "application/json"}
            if llm_config.get("apiKey"):
                headers["Authorization"] = f"Bearer {llm_config['apiKey']}"

            resp = http_requests.post(
                f"{llm_config['httpUrl']}/v1/chat/completions",
                json={
                    "model": llm_config["model"],
                    "messages": [{"role": "system", "content": system_prompt}]
                    + formatted_messages,
                    "response_format": {"type": "json_object"},
                },
                headers=headers,
                timeout=120,
            )

            if not resp.ok:
                raise Exception(f"HTTP LLM Error: {resp.status_code} - {resp.text}")

            resp_data = resp.json()
            content = resp_data["choices"][0]["message"]["content"]
            content = content.replace("```json", "").replace("```", "").strip()
            return jsonify(json.loads(content))

        elif llm_config["provider"] == "ollama":
            resp = http_requests.post(
                f"{llm_config['ollamaUrl']}/api/chat",
                json={
                    "model": llm_config.get("model", "llama3"),
                    "messages": [{"role": "system", "content": system_prompt}]
                    + formatted_messages,
                    "stream": False,
                    "format": "json",
                },
                timeout=120,
            )

            resp_data = resp.json()
            return jsonify(json.loads(resp_data["message"]["content"]))

        else:
            return jsonify({"error": "Invalid LLM provider"}), 400

    except Exception as exc:
        print(f"Chat error: {exc}")
        return jsonify({"error": str(exc)}), 500


# ---------------------------------------------------------------------------
# Serve built frontend (production)
# ---------------------------------------------------------------------------
@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def serve_frontend(path):
    # Never intercept API routes (safety guard)
    if path.startswith("api/"):
        return jsonify({"error": "Not found"}), 404
    if os.path.exists(DIST_DIR):
        full_path = os.path.join(DIST_DIR, path)
        if path and os.path.isfile(full_path):
            return send_from_directory(DIST_DIR, path)
        return send_from_directory(DIST_DIR, "index.html")
    return (
        "<h2>Frontend not built.</h2><p>Run <code>npm run build</code> first,"
        " or start the Vite dev server with <code>npm run dev</code>.</p>",
        404,
    )


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print(f"ClickSense backend running on http://localhost:{PORT}")
    app.run(
        host="0.0.0.0",
        port=PORT,
        debug=os.environ.get("FLASK_DEBUG", "false").lower() == "true",
    )
