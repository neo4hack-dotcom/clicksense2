import os
import json
import requests as http_requests
import urllib3
from datetime import datetime, timezone
from urllib.parse import urlparse
from dotenv import load_dotenv
from flask import Flask, request, jsonify, send_from_directory

# Suppress InsecureRequestWarning for plain-HTTP endpoints
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def _http_get(url, **kwargs):
    """GET with SSL verification disabled (supports self-signed certificates)."""
    kwargs.setdefault("verify", False)
    return http_requests.get(url, **kwargs)


def _http_post(url, **kwargs):
    """POST with SSL verification disabled (supports self-signed certificates)."""
    kwargs.setdefault("verify", False)
    return http_requests.post(url, **kwargs)


load_dotenv()

PORT = int(os.environ.get("PORT", 3000))
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DIST_DIR = os.path.join(BASE_DIR, "dist")

app = Flask(__name__, static_folder=DIST_DIR, static_url_path="")


# ---------------------------------------------------------------------------
# CORS
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

OLD_DB_FILE = os.path.join(BASE_DIR, "app.json")
if os.path.exists(OLD_DB_FILE) and not os.path.exists(DB_FILE):
    os.rename(OLD_DB_FILE, DB_FILE)

DEFAULT_DB = {
    "users": [{"id": 1, "name": "Default User"}],
    "saved_queries": [],
    "query_history": [],
    "table_metadata": [],
    "knowledge_folders": [],
    "table_mappings": [],
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
# In-memory configuration
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
    "baseUrl": "http://localhost:11434",
    "apiKey": "",
}

knowledge_base = ""

rag_config = {
    "esHost": os.environ.get("ES_HOST", "http://localhost:9200"),
    "esIndex": os.environ.get("ES_INDEX", "clicksense_rag"),
    "esUsername": os.environ.get("ES_USER", ""),
    "esPassword": os.environ.get("ES_PASSWORD", ""),
    "embeddingModel": "",
    "topK": 5,
    "chunkSize": 500,
}


# ---------------------------------------------------------------------------
# ClickHouse helper
# ---------------------------------------------------------------------------
def _parse_clickhouse_url(url: str) -> dict:
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
        import clickhouse_connect
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
# Embedding helper — uses the configured local LLM connection
# ---------------------------------------------------------------------------
def _get_embedding(text: str) -> list:
    """Compute embedding via the configured local LLM endpoint (Ollama or local_http).

    Uses the same provider/baseUrl as the LLM config so no separate embedding
    endpoint configuration is required. The embedding model is taken from
    rag_config['embeddingModel'] (can differ from the chat model).
    """
    provider = llm_config.get("provider", "ollama")
    base_url = (llm_config.get("baseUrl") or "").rstrip("/")
    model = (rag_config.get("embeddingModel") or "").strip() or llm_config.get("model") or "llama3"
    headers = {"Content-Type": "application/json"}

    if provider == "ollama":
        ollama_url = base_url or "http://localhost:11434"
        # Ollama /api/embed (v0.3+) — supports batch; use single input as list
        resp = _http_post(
            f"{ollama_url}/api/embed",
            json={"model": model, "input": text},
            headers=headers,
            timeout=120,
        )
        if resp.ok:
            data = resp.json()
            # /api/embed returns {"embeddings": [[...], ...]}
            if "embeddings" in data and data["embeddings"]:
                return data["embeddings"][0]
        # Fallback: legacy /api/embeddings endpoint
        resp2 = _http_post(
            f"{ollama_url}/api/embeddings",
            json={"model": model, "prompt": text},
            headers=headers,
            timeout=120,
        )
        if not resp2.ok:
            err_body = resp2.text
            # Detect model-not-found or embedding-unsupported errors and give a
            # clear, actionable message instead of the raw Ollama error.
            is_model_error = resp2.status_code == 404 or "not found" in err_body.lower()
            is_fallback_model = not (rag_config.get("embeddingModel") or "").strip()
            if is_model_error and is_fallback_model:
                raise Exception(
                    f"The LLM model '{model}' does not support embeddings via Ollama. "
                    f"Please configure a dedicated embedding model (e.g. nomic-embed-text, bge-m3, all-minilm) "
                    f"in Settings > Embedding Model, then click 'Save RAG Config'."
                )
            raise Exception(f"Ollama embedding error {resp2.status_code}: {err_body}")
        data2 = resp2.json()
        if "embedding" in data2:
            return data2["embedding"]
        raise Exception(f"Unexpected Ollama embedding response: {list(data2.keys())}")

    elif provider == "local_http":
        # OpenAI-compatible /v1/embeddings endpoint derived from the LLM base URL
        endpoint = base_url or "http://localhost:8000"
        if "/v1/embeddings" not in endpoint:
            endpoint = endpoint.rstrip("/") + "/v1/embeddings"
        if llm_config.get("apiKey"):
            headers["Authorization"] = f"Bearer {llm_config['apiKey']}"
        resp = _http_post(
            endpoint,
            json={"model": model, "input": text},
            headers=headers,
            timeout=120,
        )
        if not resp.ok:
            raise Exception(f"Embedding error {resp.status_code}: {resp.text}")
        data = resp.json()
        if "data" in data and data["data"]:
            return data["data"][0]["embedding"]
        raise Exception(f"Unexpected embedding response: {list(data.keys())}")

    else:
        raise Exception(
            f"Embedding not supported for provider '{provider}'. Use 'ollama' or 'local_http'."
        )


# ---------------------------------------------------------------------------
# Elasticsearch helper
# ---------------------------------------------------------------------------
def _es_request(method: str, path: str, cfg: dict, **kwargs):
    """Make a request to Elasticsearch."""
    host = cfg.get("esHost", "http://localhost:9200").rstrip("/")
    url = f"{host}{path}"
    auth = None
    if cfg.get("esUsername"):
        auth = (cfg["esUsername"], cfg.get("esPassword", ""))
    fn = _http_post if method.upper() == "POST" else _http_get
    if method.upper() == "PUT":
        fn = lambda u, **kw: http_requests.put(u, verify=False, **kw)
    elif method.upper() == "DELETE":
        fn = lambda u, **kw: http_requests.delete(u, verify=False, **kw)
    elif method.upper() == "HEAD":
        fn = lambda u, **kw: http_requests.head(u, verify=False, **kw)

    return fn(url, auth=auth, **kwargs)


def _chunk_text(text: str, chunk_size: int) -> list:
    """Split text into chunks of roughly chunk_size chars, trying to split on newlines."""
    chunks = []
    while len(text) > chunk_size:
        split_at = text.rfind("\n", 0, chunk_size)
        if split_at == -1:
            split_at = chunk_size
        chunks.append(text[:split_at].strip())
        text = text[split_at:].strip()
    if text:
        chunks.append(text)
    return chunks


def _ensure_es_index(cfg: dict, dims: int):
    """Create ES index with dense_vector mapping if it doesn't exist."""
    index = cfg.get("esIndex", "clicksense_rag")
    host = cfg.get("esHost", "http://localhost:9200").rstrip("/")
    auth = None
    if cfg.get("esUsername"):
        auth = (cfg["esUsername"], cfg.get("esPassword", ""))

    check = http_requests.head(f"{host}/{index}", auth=auth, verify=False, timeout=10)
    if check.status_code == 404:
        mapping = {
            "mappings": {
                "properties": {
                    "folder_id": {"type": "integer"},
                    "title": {"type": "text"},
                    "content": {"type": "text"},
                    "chunk_index": {"type": "integer"},
                    "embedding": {
                        "type": "dense_vector",
                        "dims": dims,
                        "index": True,
                        "similarity": "cosine",
                    },
                }
            }
        }
        r = http_requests.put(
            f"{host}/{index}",
            auth=auth,
            json=mapping,
            verify=False,
            timeout=10,
        )
        if not r.ok and r.status_code != 400:
            raise Exception(f"Failed to create ES index: {r.text}")


# ---------------------------------------------------------------------------
# LLM call helper (shared)
# ---------------------------------------------------------------------------
def _strip_llm_markdown(text: str) -> str:
    """Remove markdown code fences that LLMs sometimes wrap around JSON."""
    text = text.strip()
    # Remove ```json ... ``` or ``` ... ``` blocks
    for fence in ("```json", "```JSON", "```"):
        if text.startswith(fence):
            text = text[len(fence):]
            break
    if text.endswith("```"):
        text = text[:-3]
    return text.strip()


def _clean_llm_output(text: str) -> str:
    """Remove <think>...</think> blocks (DeepSeek etc.) and residual HTML tags."""
    import re
    text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)
    text = re.sub(r'<[^>]+>', '', text)
    return text.strip()


def _parse_response_json(resp, label: str = "LLM") -> dict:
    """Safely parse JSON from an HTTP response with descriptive errors.

    Also handles SSE (Server-Sent Events) streaming responses by extracting
    the last complete data chunk that contains the assistant message.
    """
    if not resp.text or not resp.text.strip():
        raise Exception(
            f"{label} returned an empty response body (HTTP {resp.status_code}). "
            "Check that the server is running and the model is loaded."
        )
    try:
        return resp.json()
    except Exception:
        pass

    # Fallback: try to parse as an SSE stream (lines starting with "data: ")
    text = resp.text.strip()
    if "data:" in text:
        import re
        chunks = re.findall(r"^data:\s*(.+)$", text, re.MULTILINE)
        # Accumulate all delta content from streaming chunks
        accumulated_content = ""
        has_valid_chunk = False
        for chunk in chunks:
            chunk = chunk.strip()
            if chunk == "[DONE]":
                continue
            try:
                chunk_data = json.loads(chunk)
                delta_content = (
                    chunk_data.get("choices", [{}])[0]
                    .get("delta", {})
                    .get("content") or ""
                )
                accumulated_content += delta_content
                has_valid_chunk = True
            except json.JSONDecodeError:
                continue
        if has_valid_chunk and accumulated_content:
            return {"choices": [{"message": {"content": accumulated_content}}]}
        # If no delta content found, try returning the last parseable chunk as-is
        for chunk in reversed(chunks):
            chunk = chunk.strip()
            if chunk == "[DONE]":
                continue
            try:
                return json.loads(chunk)
            except json.JSONDecodeError:
                continue

    raise Exception(
        f"{label} response is not valid JSON (HTTP {resp.status_code}): "
        f"{resp.text[:300]!r}"
    )


def _parse_llm_json(content: str) -> dict:
    """Parse JSON from LLM output, handling markdown fences and extra surrounding text."""
    import re

    if not content or not content.strip():
        raise ValueError("LLM returned an empty response")

    cleaned = _strip_llm_markdown(content)

    # Fast path: try the cleaned content directly
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    # Fallback: find the first JSON object or array in the text
    match = re.search(r"(\{[\s\S]*\}|\[[\s\S]*\])", cleaned)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            pass

    raise ValueError(
        f"LLM response is not valid JSON. Raw content (first 500 chars): {content[:500]!r}"
    )


# ---------------------------------------------------------------------------
# Token budget utilities
# ---------------------------------------------------------------------------
import re as _re

# Cache: model name -> max context window (tokens)
_model_context_cache: dict = {}

# Known context windows for common models
_KNOWN_CONTEXT_LIMITS: dict = {
    "llama3": 8192,
    "llama3.1": 131072,
    "llama3.2": 131072,
    "llama3.3": 131072,
    "mistral": 32768,
    "mixtral": 32768,
    "gemma2": 8192,
    "gemma3": 131072,
    "qwen2.5": 131072,
    "qwen2": 32768,
    "phi3": 131072,
    "phi4": 131072,
    "deepseek": 65536,
    "codellama": 16384,
    "vicuna": 4096,
    "gpt-4": 128000,
    "gpt-3.5": 16385,
    "claude": 200000,
}

# Budget: reserve this fraction of the context for conversation + output
_SYSTEM_PROMPT_BUDGET_FRACTION = 0.5


def _estimate_tokens(text: str) -> int:
    """Rough token estimate: ~4 chars per token for typical text."""
    return max(1, len(text) // 4)


def _get_model_context_limit() -> int:
    """Return the context window (tokens) for the current LLM model.

    Order of precedence:
    1. In-memory cache (from a previous call).
    2. Known limits dict (matched by substring of model name).
    3. Ask the LLM itself, cache the result.
    4. Conservative default (4 096 tokens).
    """
    model = (llm_config.get("model") or "unknown").lower()

    if model in _model_context_cache:
        return _model_context_cache[model]

    for known_name, limit in _KNOWN_CONTEXT_LIMITS.items():
        if known_name in model:
            _model_context_cache[model] = limit
            print(f"[Token budget] Known context limit for '{model}': {limit} tokens")
            return limit

    # Unknown model – ask the LLM
    try:
        answer = _call_llm(
            "You are a helpful assistant. Answer with a single integer only.",
            [{"role": "user", "content": "What is your maximum context window size in tokens? Reply with just the number, no other text."}],
            temperature=0.0,
        )
        # Extract first integer-like sequence from the answer
        match = _re.search(r"\d[\d,. ]*", answer)
        if match:
            limit = int(_re.sub(r"[^0-9]", "", match.group()))
            _model_context_cache[model] = limit
            print(f"[Token budget] Model '{model}' self-reported context limit: {limit} tokens")
            return limit
    except Exception as exc:
        print(f"[Token budget] Could not query model context limit: {exc}")

    default = 4096
    _model_context_cache[model] = default
    print(f"[Token budget] Unknown model '{model}', using conservative default: {default} tokens")
    return default


def _truncate_prompt_context(
    schema: dict,
    table_metadata: dict,
    knowledge_context: str,
    base_tokens: int,
) -> tuple:
    """Progressively shrink schema/metadata/knowledge to fit within the token budget.

    Returns (schema, table_metadata, knowledge_context) – possibly truncated.
    """
    context_limit = _get_model_context_limit()
    budget = int(context_limit * _SYSTEM_PROMPT_BUDGET_FRACTION)
    remaining = budget - base_tokens

    if remaining <= 0:
        # Base prompt alone is already too large – send nothing extra
        print("[Token budget] Base prompt exceeds budget, sending empty schema/metadata/knowledge")
        return {}, {}, ""

    def _fits(s_str: str, m_str: str, k_str: str) -> bool:
        return _estimate_tokens(s_str + m_str + k_str) <= remaining

    schema_full = json.dumps(schema, indent=2)
    meta_full = json.dumps(table_metadata, indent=2)

    # Step 1 – full schema + metadata + knowledge
    if _fits(schema_full, meta_full, knowledge_context):
        return schema, table_metadata, knowledge_context

    # Step 2 – compress schema: keep only column names (drop types)
    schema_compact = {tbl: [c["name"] if isinstance(c, dict) else c for c in cols]
                      for tbl, cols in schema.items()}
    schema_compact_str = json.dumps(schema_compact, indent=2)
    if _fits(schema_compact_str, meta_full, knowledge_context):
        print("[Token budget] Schema compressed (column names only) to fit budget")
        return schema_compact, table_metadata, knowledge_context

    # Step 3 – compact schema + no metadata
    if _fits(schema_compact_str, "{}", knowledge_context):
        print("[Token budget] Table metadata dropped to fit budget")
        return schema_compact, {}, knowledge_context

    # Step 4 – compact schema + no metadata + truncated knowledge
    available_for_knowledge = remaining - _estimate_tokens(schema_compact_str) - 20
    if available_for_knowledge > 50:
        truncated_knowledge = knowledge_context[:available_for_knowledge * 4]
        print(f"[Token budget] Knowledge context truncated to ~{available_for_knowledge} tokens")
        return schema_compact, {}, truncated_knowledge

    # Step 5 – only table names (no columns)
    schema_names_only = {tbl: [] for tbl in schema}
    print("[Token budget] Sending table names only – context extremely tight")
    return schema_names_only, {}, ""


def _is_list_tables_request(messages: list) -> bool:
    """Return True when the user's last message is asking to list all tables."""
    for msg in reversed(messages):
        if msg.get("role") == "user":
            text = msg.get("content", "").lower().strip()
            patterns = [
                "list of all tables", "show tables", "list tables",
                "all tables", "what tables", "which tables", "show all tables",
                "liste des tables", "lister les tables", "toutes les tables",
                "liste toutes les tables",
            ]
            return any(p in text for p in patterns)
    return False


def _messages_to_prompt(system_prompt: str, messages: list) -> str:
    """Flatten system prompt + conversation history into a single prompt string."""
    parts = []
    if system_prompt:
        parts.append(f"System: {system_prompt}")
    for msg in messages:
        role = msg.get("role", "user").capitalize()
        content = msg.get("content", "")
        parts.append(f"{role}: {content}")
    parts.append("Assistant:")
    return "\n\n".join(parts)


def _call_llm(system_prompt: str, messages: list, temperature: float = 0.7,
              language: str = None) -> str:
    """Call the configured LLM and return the content string.

    Supports providers: ollama, local_http, n8n.
    """
    # Append language instruction to the last user message if requested
    if language:
        lang_instruction = (
            "Vous DEVEZ écrire votre réponse ENTIÈRE en FRANÇAIS"
            if language == "fr"
            else "You MUST write your entire response in ENGLISH"
        )
        if messages:
            last = messages[-1]
            messages = messages[:-1] + [{
                **last,
                "content": last.get("content", "") + f"\n\n{lang_instruction}",
            }]
        else:
            system_prompt = system_prompt + f"\n\n{lang_instruction}"

    provider = llm_config.get("provider", "ollama")
    base_url = (llm_config.get("baseUrl") or "").rstrip("/")

    if provider == "local_http":
        endpoint = base_url or "http://localhost:8000"
        if "/v1/chat" not in endpoint:
            endpoint = endpoint.rstrip("/") + "/v1/chat/completions"
        headers = {"Content-Type": "application/json"}
        if llm_config.get("apiKey"):
            headers["Authorization"] = f"Bearer {llm_config['apiKey']}"
        resp = _http_post(
            endpoint,
            json={
                "model": llm_config.get("model") or "local-model",
                "messages": [{"role": "system", "content": system_prompt}] + messages,
                "temperature": temperature,
                "stream": False,
            },
            headers=headers,
            timeout=120,
        )
        if not resp.ok:
            raise Exception(f"local_http LLM Error: {resp.status_code} - {resp.text}")
        resp_data = _parse_response_json(resp, "local_http LLM")
        content = (
            resp_data.get("choices", [{}])[0].get("message", {}).get("content")
            or resp_data.get("content")
            or ""
        )
        return _clean_llm_output(_strip_llm_markdown(content))

    elif provider == "ollama":
        ollama_url = base_url or "http://localhost:11434"
        prompt = _messages_to_prompt(system_prompt, messages)
        body = {
            "model": llm_config.get("model", "llama3"),
            "prompt": prompt,
            "stream": False,
        }
        resp = _http_post(
            f"{ollama_url}/api/generate",
            json=body,
            timeout=120,
        )
        if not resp.ok:
            raise Exception(f"Ollama LLM Error: {resp.status_code} - {resp.text}")
        resp_data = _parse_response_json(resp, "Ollama LLM")
        content = resp_data.get("response", "")
        return _clean_llm_output(_strip_llm_markdown(content))

    elif provider == "n8n":
        endpoint = base_url or ""
        if not endpoint:
            raise Exception("n8n provider requires a baseUrl (webhook URL)")
        headers = {"Content-Type": "application/json"}
        if llm_config.get("apiKey"):
            headers["Authorization"] = llm_config["apiKey"]  # raw value, no Bearer
        prompt = _messages_to_prompt(system_prompt, messages)
        resp = _http_post(
            endpoint,
            json={
                "prompt": prompt,
                "model": llm_config.get("model", ""),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
            headers=headers,
            timeout=120,
        )
        if not resp.ok:
            raise Exception(f"n8n LLM Error: {resp.status_code} - {resp.text}")
        resp_data = _parse_response_json(resp, "n8n LLM")
        content = (
            resp_data.get("output")
            or resp_data.get("text")
            or resp_data.get("response")
            or resp_data.get("content")
            or ""
        )
        return _clean_llm_output(content)

    else:
        raise Exception(f"Invalid LLM provider: {provider!r}")


# ---------------------------------------------------------------------------
# Configuration endpoints
# ---------------------------------------------------------------------------
@app.route("/api/config", methods=["GET"])
def get_config():
    return jsonify({
        "clickhouseConfig": clickhouse_config,
        "llmConfig": llm_config,
        "knowledgeBase": knowledge_base,
    })


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
# RAG configuration endpoints
# ---------------------------------------------------------------------------
@app.route("/api/rag/config", methods=["GET"])
def get_rag_config():
    return jsonify(rag_config)


@app.route("/api/rag/config", methods=["POST"])
def update_rag_config():
    global rag_config
    data = request.get_json()
    # Drop legacy fields that were replaced by the LLM connection
    for legacy in ("embeddingUrl", "embeddingApiKey"):
        data.pop(legacy, None)
    rag_config.update(data)
    return jsonify({"success": True})


@app.route("/api/rag/test", methods=["POST"])
def test_elasticsearch():
    data = request.get_json()
    try:
        host = data.get("esHost", "http://localhost:9200").rstrip("/")
        auth = None
        if data.get("esUsername"):
            auth = (data["esUsername"], data.get("esPassword", ""))
        resp = http_requests.get(f"{host}/_cluster/health", auth=auth, verify=False, timeout=10)
        if not resp.ok:
            raise Exception(f"ES returned {resp.status_code}: {resp.text}")
        return jsonify({"success": True, "status": resp.json().get("status")})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/rag/embedding-models", methods=["POST"])
def get_embedding_models():
    """List models available at the LLM local endpoint for use as embedding models."""
    provider = llm_config.get("provider", "ollama")
    base_url = (llm_config.get("baseUrl") or "").rstrip("/")
    headers = {"Content-Type": "application/json"}
    if llm_config.get("apiKey"):
        headers["Authorization"] = f"Bearer {llm_config['apiKey']}"
    try:
        if provider == "ollama":
            ollama_url = base_url or "http://localhost:11434"
            resp = _http_get(f"{ollama_url}/api/tags", headers=headers, timeout=10)
            if not resp.ok:
                return jsonify({"models": []})
            models = [m.get("name", "") for m in resp.json().get("models", [])]
            return jsonify({"models": [m for m in models if m]})
        elif provider == "local_http":
            endpoint = base_url or "http://localhost:8000"
            resp = _http_get(f"{endpoint}/v1/models", headers=headers, timeout=10)
            if not resp.ok:
                return jsonify({"models": []})
            models = [m.get("id") or m.get("name", "") for m in resp.json().get("data", [])]
            return jsonify({"models": [m for m in models if m]})
        return jsonify({"models": []})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/rag/test-embedding", methods=["POST"])
def test_embedding():
    data = request.get_json()
    try:
        embedding = _get_embedding("test")
        return jsonify({"success": True, "dims": len(embedding)})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/rag/index", methods=["POST"])
def index_knowledge_to_es():
    """Embed all knowledge folders and index them in Elasticsearch."""
    data = request.get_json()
    cfg = data.get("ragConfig", rag_config)
    db = read_db()
    folders = db.get("knowledge_folders", [])

    if not folders:
        return jsonify({"error": "No knowledge folders to index"}), 400

    try:
        # Determine embedding dimension by running one test embedding
        test_vec = _get_embedding("test")
        dims = len(test_vec)
        _ensure_es_index(cfg, dims)

        host = cfg.get("esHost", "http://localhost:9200").rstrip("/")
        index = cfg.get("esIndex", "clicksense_rag")
        chunk_size = int(cfg.get("chunkSize", 500))
        auth = None
        if cfg.get("esUsername"):
            auth = (cfg["esUsername"], cfg.get("esPassword", ""))

        # Delete existing documents for a fresh index
        http_requests.post(
            f"{host}/{index}/_delete_by_query",
            auth=auth,
            json={"query": {"match_all": {}}},
            verify=False,
            timeout=30,
        )

        total_chunks = 0
        for folder in folders:
            content = folder.get("content", "")
            title = folder.get("title", "")
            folder_id = folder.get("id")
            if not content.strip():
                continue

            chunks = _chunk_text(content, chunk_size)
            for chunk_idx, chunk in enumerate(chunks):
                embedding = _get_embedding(f"{title}\n\n{chunk}")
                doc = {
                    "folder_id": folder_id,
                    "title": title,
                    "content": chunk,
                    "chunk_index": chunk_idx,
                    "embedding": embedding,
                }
                http_requests.post(
                    f"{host}/{index}/_doc",
                    auth=auth,
                    json=doc,
                    verify=False,
                    timeout=30,
                )
                total_chunks += 1

        # Refresh index
        http_requests.post(f"{host}/{index}/_refresh", auth=auth, verify=False, timeout=10)

        return jsonify({"success": True, "indexed": total_chunks, "folders": len(folders)})

    except Exception as exc:
        print(f"Index error: {exc}")
        return jsonify({"error": str(exc)}), 500


@app.route("/api/rag/chat", methods=["POST"])
def rag_chat():
    """RAG chat: embed query → ES kNN search → LLM augmented generation."""
    data = request.get_json()
    query = data.get("query", "")
    history = data.get("history", [])
    cfg = data.get("ragConfig", rag_config)
    top_k = int(cfg.get("topK", 5))

    if not query:
        return jsonify({"error": "Empty query"}), 400

    try:
        # 1. Embed the user query
        query_embedding = _get_embedding(query)

        # 2. kNN search in Elasticsearch
        host = cfg.get("esHost", "http://localhost:9200").rstrip("/")
        index = cfg.get("esIndex", "clicksense_rag")
        auth = None
        if cfg.get("esUsername"):
            auth = (cfg["esUsername"], cfg.get("esPassword", ""))

        knn_query = {
            "knn": {
                "field": "embedding",
                "query_vector": query_embedding,
                "k": top_k,
                "num_candidates": top_k * 10,
            },
            "_source": ["title", "content", "folder_id", "chunk_index"],
        }

        es_resp = http_requests.post(
            f"{host}/{index}/_search",
            auth=auth,
            json=knn_query,
            verify=False,
            timeout=30,
        )

        sources = []
        context_parts = []

        if es_resp.ok:
            hits = es_resp.json().get("hits", {}).get("hits", [])
            for hit in hits:
                src = hit.get("_source", {})
                score = hit.get("_score", 0)
                # Normalize cosine score (ES returns 0-1 for cosine similarity mapping)
                sources.append({
                    "title": src.get("title", ""),
                    "score": round(float(score), 4),
                    "excerpt": src.get("content", "")[:300],
                    "folder_id": src.get("folder_id"),
                })
                context_parts.append(
                    f"[Source: {src.get('title', 'Unknown')}]\n{src.get('content', '')}"
                )

        # 3. Build augmented prompt
        context_text = "\n\n---\n\n".join(context_parts) if context_parts else "No relevant knowledge found."

        system_prompt = f"""You are a knowledgeable assistant with access to a specialized knowledge base.
Use the following retrieved context to answer the user's question accurately and concisely.
If the context doesn't contain enough information, say so clearly rather than guessing.

RETRIEVED CONTEXT:
{context_text}

Instructions:
- Base your answer primarily on the retrieved context
- Be specific and cite which source you are referencing when relevant
- If multiple sources provide complementary information, synthesize them
- If the context is insufficient, indicate what information is missing
"""

        # 4. Call LLM
        formatted_messages = [{"role": m["role"], "content": m["content"]} for m in history]
        formatted_messages.append({"role": "user", "content": query})

        answer = _call_llm(system_prompt, formatted_messages, temperature=0.3)
        return jsonify({"answer": answer, "sources": sources})

    except Exception as exc:
        print(f"RAG chat error: {exc}")
        return jsonify({"error": str(exc)}), 500


# ---------------------------------------------------------------------------
# ClickHouse connection test
# ---------------------------------------------------------------------------
@app.route("/api/clickhouse/test", methods=["POST"])
def test_clickhouse():
    data = request.get_json()
    try:
        client = get_clickhouse_client({
            "host": data["host"],
            "username": data["username"],
            "password": data["password"],
            "database": data["database"],
        })
        client.query("SELECT 1")
        return jsonify({"success": True})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ---------------------------------------------------------------------------
# LLM endpoints
# ---------------------------------------------------------------------------
@app.route("/api/llm/test", methods=["POST"])
def test_llm():
    """Test LLM connection by sending a simple hello prompt."""
    data = request.get_json()
    provider = data.get("provider")
    base_url = (data.get("baseUrl") or "").rstrip("/")
    test_prompt = "Hello, are you online? Respond with 'Yes'."
    try:
        if provider == "ollama":
            ollama_url = base_url or "http://localhost:11434"
            resp = _http_post(
                f"{ollama_url}/api/generate",
                json={"model": data.get("model", "llama3"), "prompt": test_prompt, "stream": False},
                timeout=30,
            )
            if not resp.ok:
                raise Exception(f"Ollama error: {resp.status_code} - {resp.text}")
        elif provider == "local_http":
            endpoint = base_url or "http://localhost:8000"
            if "/v1/chat" not in endpoint:
                endpoint = endpoint.rstrip("/") + "/v1/chat/completions"
            headers = {"Content-Type": "application/json"}
            if data.get("apiKey"):
                headers["Authorization"] = f"Bearer {data['apiKey']}"
            resp = _http_post(
                endpoint,
                json={
                    "model": data.get("model") or "local-model",
                    "messages": [{"role": "user", "content": test_prompt}],
                    "temperature": 0.7,
                },
                headers=headers,
                timeout=30,
            )
            if not resp.ok:
                raise Exception(f"local_http LLM error: {resp.status_code} - {resp.text}")
        elif provider == "n8n":
            endpoint = base_url
            if not endpoint:
                raise Exception("n8n provider requires a baseUrl (webhook URL)")
            headers = {"Content-Type": "application/json"}
            if data.get("apiKey"):
                headers["Authorization"] = data["apiKey"]
            resp = _http_post(
                endpoint,
                json={
                    "prompt": test_prompt,
                    "model": data.get("model", ""),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
                headers=headers,
                timeout=30,
            )
            if not resp.ok:
                raise Exception(f"n8n LLM error: {resp.status_code} - {resp.text}")
        return jsonify({"success": True})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/llm/models", methods=["GET"])
def get_llm_models():
    """Fetch available model list (Ollama and local_http only)."""
    try:
        provider = llm_config.get("provider", "ollama")
        base_url = (llm_config.get("baseUrl") or "").rstrip("/")
        if provider == "ollama":
            ollama_url = base_url or "http://localhost:11434"
            resp = _http_get(f"{ollama_url}/api/tags", timeout=10)
            if not resp.ok:
                raise Exception(f"Ollama error: {resp.status_code}")
            models = [m["name"] for m in resp.json().get("models", [])]
            return jsonify({"models": models})
        elif provider == "local_http":
            # Derive base from endpoint URL (strip /v1/chat/completions suffix if present)
            endpoint = base_url or "http://localhost:8000/v1/chat/completions"
            api_base = endpoint.split("/v1/chat")[0].split("/v1/models")[0]
            headers = {}
            if llm_config.get("apiKey"):
                headers["Authorization"] = f"Bearer {llm_config['apiKey']}"
            resp = _http_get(f"{api_base}/v1/models", headers=headers, timeout=10)
            if not resp.ok:
                if resp.status_code == 404:
                    return jsonify({"models": []})
                raise Exception(f"HTTP error: {resp.status_code}")
            models = [m.get("id") or m.get("name", "") for m in resp.json().get("data", []) if m.get("id") or m.get("name")]
            return jsonify({"models": models})
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
    idx = next((i for i, m in enumerate(db["table_metadata"]) if m["table_name"] == table_name), None)
    if idx is not None:
        db["table_metadata"][idx]["description"] = description
        db["table_metadata"][idx]["is_favorite"] = is_favorite
    else:
        db["table_metadata"].append({
            "table_name": table_name,
            "description": description,
            "is_favorite": is_favorite,
        })
    write_db(db)
    return jsonify({"success": True})


# ---------------------------------------------------------------------------
# Knowledge Base Folders
# ---------------------------------------------------------------------------
@app.route("/api/knowledge/folders", methods=["GET"])
def get_knowledge_folders():
    db = read_db()
    folders = db.get("knowledge_folders", [])
    return jsonify(folders)


@app.route("/api/knowledge/folders", methods=["POST"])
def create_knowledge_folder():
    data = request.get_json()
    db = read_db()
    if "knowledge_folders" not in db:
        db["knowledge_folders"] = []

    new_id = max((f["id"] for f in db["knowledge_folders"]), default=0) + 1
    now = datetime.now(timezone.utc).isoformat()
    folder = {
        "id": new_id,
        "title": data.get("title", ""),
        "content": data.get("content", ""),
        "created_at": now,
        "updated_at": now,
    }
    db["knowledge_folders"].append(folder)
    write_db(db)
    return jsonify(folder)


@app.route("/api/knowledge/folders/<int:folder_id>", methods=["PUT"])
def update_knowledge_folder(folder_id):
    data = request.get_json()
    db = read_db()
    folders = db.get("knowledge_folders", [])
    idx = next((i for i, f in enumerate(folders) if f["id"] == folder_id), None)
    if idx is None:
        return jsonify({"error": "Folder not found"}), 404

    now = datetime.now(timezone.utc).isoformat()
    if "title" in data:
        folders[idx]["title"] = data["title"]
    if "content" in data:
        folders[idx]["content"] = data["content"]
    folders[idx]["updated_at"] = now

    write_db(db)
    return jsonify(folders[idx])


@app.route("/api/knowledge/folders/<int:folder_id>", methods=["DELETE"])
def delete_knowledge_folder(folder_id):
    db = read_db()
    db["knowledge_folders"] = [f for f in db.get("knowledge_folders", []) if f["id"] != folder_id]
    write_db(db)
    return jsonify({"success": True})


# ---------------------------------------------------------------------------
# Table Mappings (friendly names for ClickHouse tables)
# ---------------------------------------------------------------------------
@app.route("/api/table-mappings", methods=["GET"])
def get_table_mappings():
    db = read_db()
    return jsonify(db.get("table_mappings", []))


@app.route("/api/table-mappings", methods=["POST"])
def upsert_table_mapping():
    data = request.get_json()
    table_name = data.get("table_name", "").strip()
    mapping_name = data.get("mapping_name", "").strip()
    if not table_name:
        return jsonify({"error": "table_name is required"}), 400

    db = read_db()
    mappings = db.get("table_mappings", [])
    idx = next((i for i, m in enumerate(mappings) if m["table_name"] == table_name), None)
    if mapping_name:
        if idx is not None:
            mappings[idx]["mapping_name"] = mapping_name
        else:
            mappings.append({"table_name": table_name, "mapping_name": mapping_name})
    else:
        # Empty mapping_name removes the entry
        if idx is not None:
            mappings.pop(idx)

    db["table_mappings"] = mappings
    write_db(db)
    return jsonify({"success": True})


@app.route("/api/table-mappings/<path:table_name>", methods=["DELETE"])
def delete_table_mapping(table_name):
    db = read_db()
    db["table_mappings"] = [m for m in db.get("table_mappings", []) if m["table_name"] != table_name]
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
    db["query_history"].append({
        "id": new_id,
        "user_id": data["user_id"],
        "query_text": data["query_text"],
        "sql": data["sql"],
        "created_at": datetime.now(timezone.utc).isoformat(),
    })
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
    db["saved_queries"].append({
        "id": new_id,
        "user_id": data["user_id"],
        "name": data["name"],
        "sql": data["sql"],
        "config": json.dumps(data.get("config", {})),
        "visual_type": data.get("visual_type", "table"),
    })
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
# Query execution
# ---------------------------------------------------------------------------
@app.route("/api/query", methods=["POST"])
def execute_query():
    data = request.get_json()
    query = data["query"]
    try:
        client = get_clickhouse_client()
        result = client.query(query, settings={"max_execution_time": 15, "readonly": 1})
        rows = _rows_to_dicts(result)
        return jsonify({"data": rows})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ---------------------------------------------------------------------------
# AI chat (SQL generation) with ambiguity detection
# ---------------------------------------------------------------------------
@app.route("/api/chat", methods=["POST"])
def chat():
    data = request.get_json()
    messages = data["messages"]
    schema = data.get("schema", {})
    table_metadata = data.get("tableMetadata", {})
    # tableMappingFilter: list of technical table names to restrict the schema to
    table_mapping_filter = data.get("tableMappingFilter", [])

    # Build knowledge context from folders
    db = read_db()
    folders = db.get("knowledge_folders", [])
    knowledge_context = "\n\n".join(
        f"[{f['title']}]\n{f['content']}" for f in folders if f.get("content")
    ) or knowledge_base

    # Build a map of technical name -> friendly mapping name
    all_mappings = {m["table_name"]: m["mapping_name"] for m in db.get("table_mappings", [])}

    # If a filter is active, restrict the schema to selected tables only
    if table_mapping_filter:
        schema = {t: cols for t, cols in schema.items() if t in table_mapping_filter}
        table_metadata = {t: v for t, v in table_metadata.items() if t in table_mapping_filter}

    formatted_messages = [{"role": m["role"], "content": m["content"]} for m in messages]

    # ------------------------------------------------------------------
    # Special case: "list all tables" → generate SQL directly, no need
    # to send the full schema to the LLM (would be very token-expensive).
    # ------------------------------------------------------------------
    if _is_list_tables_request(messages):
        return jsonify({
            "sql": "SHOW TABLES",
            "explanation": "Lists all tables available in the current ClickHouse database.",
            "suggestedVisual": "table",
        })

    # Build a mapping note for the system prompt
    mapping_lines = []
    for tbl in schema:
        if tbl in all_mappings:
            mapping_lines.append(f"  - {tbl}  →  \"{all_mappings[tbl]}\"")
    mapping_note = (
        "The following tables have friendly business names. When communicating with the user use the friendly name, but always use the technical name in SQL:\n"
        + "\n".join(mapping_lines)
    ) if mapping_lines else ""

    # ------------------------------------------------------------------
    # Token budget: base prompt without dynamic content
    # ------------------------------------------------------------------
    base_prompt_template = """You are an expert ClickHouse data analyst.
Your goal is to help the user query their database.

KNOWLEDGE BASE — CRITICAL:
Before generating SQL or asking for clarification, you MUST consult the functional knowledge base provided.
If the knowledge base contains a definition or mapping for a concept mentioned by the user
(e.g., "a trade corresponds to a row in table toto"), use that information directly to build the SQL.
Do NOT ask for clarification on a concept that is already explained in the knowledge base.

AMBIGUITY HANDLING — CRITICAL:
Before generating SQL, check if the user request is ambiguous:

1. TABLE AMBIGUITY: If the user mentions a concept (e.g., "orders") but there are multiple tables that could match,
   AND the knowledge base does not resolve which table to use,
   return a clarification request instead of generating SQL.

2. FIELD AMBIGUITY: If the user asks to display or use a field type (e.g., "date", "id", "name")
   and there are MULTIPLE fields of that type in the same table,
   AND the knowledge base does not indicate which field to use, return a clarification.

When ambiguous, return ONLY this JSON:
{
  "needs_clarification": true,
  "question": "Clear question in the user's language asking them to choose",
  "options": ["option1", "option2", "option3"],
  "type": "field_selection" | "table_selection"
}

CLICKHOUSE INSTRUCTIONS:
- Use advanced ClickHouse functions when appropriate.
- For funnels: windowFunnel(). For retention: retention(). For patterns: sequenceMatch().
- For latest status: argMax(). For top-K: topK(). For unique counts: uniqHLL12().
- For response times: quantilesTiming(). For JSON: JSONExtract(). For conditionals: sumIf(), countIf().
- Always write highly optimized SQL.

When NOT ambiguous, return ONLY this JSON:
{
  "sql": "SELECT ...",
  "explanation": "Brief explanation of what the query does.",
  "suggestedVisual": "table" | "bar" | "line" | "pie"
}

Do not include markdown formatting. Just the raw JSON.
"""
    messages_tokens = sum(_estimate_tokens(m.get("content", "")) for m in formatted_messages)
    base_tokens = _estimate_tokens(base_prompt_template) + _estimate_tokens(mapping_note) + messages_tokens

    # Truncate dynamic context (schema / metadata / knowledge) to fit the budget
    schema, table_metadata, knowledge_context = _truncate_prompt_context(
        schema, table_metadata, knowledge_context, base_tokens
    )

    system_prompt = f"""You are an expert ClickHouse data analyst.
Your goal is to help the user query their database.

Here is the database schema:
{json.dumps(schema, indent=2)}

Here is the table metadata (functional descriptions):
{json.dumps(table_metadata, indent=2)}

{mapping_note}

Here is the functional knowledge base:
{knowledge_context}

KNOWLEDGE BASE — CRITICAL:
Before generating SQL or asking for clarification, you MUST consult the functional knowledge base above.
If the knowledge base contains a definition or mapping for a concept mentioned by the user
(e.g., "a trade corresponds to a row in table toto"), use that information directly to build the SQL.
Do NOT ask for clarification on a concept that is already explained in the knowledge base.

AMBIGUITY HANDLING — CRITICAL:
Before generating SQL, check if the user request is ambiguous:

1. TABLE AMBIGUITY: If the user mentions a concept (e.g., "orders") but there are multiple tables that could match,
   AND the knowledge base does not resolve which table to use,
   return a clarification request instead of generating SQL.

2. FIELD AMBIGUITY: If the user asks to display or use a field type (e.g., "date", "id", "name")
   and there are MULTIPLE fields of that type in the same table,
   AND the knowledge base does not indicate which field to use, return a clarification.

When ambiguous, return ONLY this JSON:
{{
  "needs_clarification": true,
  "question": "Clear question in the user's language asking them to choose",
  "options": ["option1", "option2", "option3"],
  "type": "field_selection" | "table_selection"
}}

CLICKHOUSE INSTRUCTIONS:
- Use advanced ClickHouse functions when appropriate.
- For funnels: windowFunnel(). For retention: retention(). For patterns: sequenceMatch().
- For latest status: argMax(). For top-K: topK(). For unique counts: uniqHLL12().
- For response times: quantilesTiming(). For JSON: JSONExtract(). For conditionals: sumIf(), countIf().
- Always write highly optimized SQL.

When NOT ambiguous, return ONLY this JSON:
{{
  "sql": "SELECT ...",
  "explanation": "Brief explanation of what the query does.",
  "suggestedVisual": "table" | "bar" | "line" | "pie"
}}

Do not include markdown formatting. Just the raw JSON.
"""

    try:
        content = _call_llm(system_prompt, formatted_messages, temperature=0.3)
    except Exception as exc:
        print(f"Chat error: {exc}")
        return jsonify({"error": str(exc)}), 500

    try:
        return jsonify(_parse_llm_json(content))
    except ValueError:
        # The LLM replied with plain text (e.g. a conversational answer not
        # requiring a SQL query). Return it as an explanation-only response so
        # the frontend can display it without treating it as an error.
        return jsonify({"explanation": content})


# ---------------------------------------------------------------------------
# Agent Analysis — agentic loop (up to 10 distinct ClickHouse queries)
# ---------------------------------------------------------------------------
@app.route("/api/agent", methods=["POST"])
def agent_analysis():
    """Orchestrated multi-step analysis: the LLM autonomously decides which
    ClickHouse queries to run (up to MAX_AGENT_STEPS), analyses each result,
    and finally synthesises a detailed answer for the user."""

    MAX_AGENT_STEPS = 10

    data = request.get_json()
    user_question = data.get("question", "")
    schema = data.get("schema", {})
    table_metadata = data.get("tableMetadata", {})
    table_mapping_filter = data.get("tableMappingFilter", [])

    if not user_question.strip():
        return jsonify({"error": "No question provided"}), 400

    # Apply table filter
    if table_mapping_filter:
        schema = {t: cols for t, cols in schema.items() if t in table_mapping_filter}
        table_metadata = {t: v for t, v in table_metadata.items() if t in table_mapping_filter}

    # Knowledge base context
    db = read_db()
    folders = db.get("knowledge_folders", [])
    knowledge_context = "\n\n".join(
        f"[{f['title']}]\n{f['content']}" for f in folders if f.get("content")
    ) or knowledge_base

    # Build friendly-name mapping note
    all_mappings = {m["table_name"]: m["mapping_name"] for m in db.get("table_mappings", [])}
    mapping_lines = [
        f"  - {tbl}  →  \"{all_mappings[tbl]}\""
        for tbl in schema if tbl in all_mappings
    ]
    mapping_note = (
        "Friendly business names for tables (use technical name in SQL, friendly name when talking to the user):\n"
        + "\n".join(mapping_lines)
    ) if mapping_lines else ""

    schema_str = json.dumps(schema, indent=2)
    metadata_str = json.dumps(table_metadata, indent=2)

    # Accumulate steps
    steps: list = []

    def _search_knowledge_for_agent(query: str) -> str:
        """Search the knowledge base (ES RAG) for context relevant to the query.

        Falls back to full folder dump if ES is unavailable or not configured.
        """
        try:
            query_embedding = _get_embedding(query)
            host = rag_config.get("esHost", "http://localhost:9200").rstrip("/")
            index = rag_config.get("esIndex", "clicksense_rag")
            auth = None
            if rag_config.get("esUsername"):
                auth = (rag_config["esUsername"], rag_config.get("esPassword", ""))
            top_k = int(rag_config.get("topK", 5))
            knn_query = {
                "knn": {
                    "field": "embedding",
                    "query_vector": query_embedding,
                    "k": top_k,
                    "num_candidates": top_k * 10,
                },
                "_source": ["title", "content"],
            }
            es_resp = http_requests.post(
                f"{host}/{index}/_search",
                auth=auth,
                json=knn_query,
                verify=False,
                timeout=15,
            )
            if es_resp.ok:
                hits = es_resp.json().get("hits", {}).get("hits", [])
                if hits:
                    parts = [
                        f"[Source: {h['_source'].get('title', 'Unknown')} | score={round(h.get('_score', 0), 3)}]\n{h['_source'].get('content', '')}"
                        for h in hits
                    ]
                    return "\n\n---\n\n".join(parts)
        except Exception as exc:
            print(f"[Agent] Knowledge search failed: {exc}")
        # Fallback: static folder text
        return knowledge_context or "No knowledge base content available."

    def _run_agent_step(steps_so_far: list) -> dict:
        """Ask the LLM for its next action given the accumulated context."""
        steps_context = ""
        for i, s in enumerate(steps_so_far, 1):
            action_label = "SQL executed" if s.get("type") == "query" else "Knowledge search"
            steps_context += (
                f"\n--- Step {i} ({s.get('type', 'query')}) ---\n"
                f"Reasoning: {s['reasoning']}\n"
                f"{action_label}:\n{s.get('sql') or s.get('search_query', '')}\n"
                f"Result summary: {s['result_summary']}\n"
            )

        system_prompt = f"""You are an autonomous ClickHouse data analyst agent.
Your goal is to answer the user's question by executing a sequence of targeted actions.
You may run up to {MAX_AGENT_STEPS} distinct actions in total. Each action must bring NEW information.
After gathering enough evidence you MUST produce a final answer.

DATABASE SCHEMA:
{schema_str}

TABLE METADATA (functional descriptions):
{metadata_str}

{mapping_note}

STATIC KNOWLEDGE BASE (always available):
{knowledge_context}

CLICKHOUSE INSTRUCTIONS:
- Use advanced ClickHouse functions when appropriate (windowFunnel, retention, argMax, topK, uniqHLL12, quantilesTiming, JSONExtract, sumIf, countIf …).
- Always write highly optimised SQL with a LIMIT where appropriate.
- Each query must explore a genuinely distinct angle (different aggregation, filter, dimension, or sub-question).

CURRENT ANALYSIS PROGRESS ({len(steps_so_far)}/{MAX_AGENT_STEPS} steps used):
{steps_context if steps_context else "None yet — this is the first step."}

USER QUESTION:
{user_question}

INSTRUCTIONS:
You have FOUR possible actions:
1. Run a ClickHouse SQL query to fetch data → action "query"
2. Search the knowledge base for business rules, definitions or context → action "search_knowledge"
3. Export data to a CSV file (pipe-separated) — ONLY if the user explicitly asks to export or save results → action "export_csv"
4. Produce the final answer when you have enough information → action "finish"

{"IMPORTANT: You have used all available steps — you MUST respond with action 'finish'." if len(steps_so_far) >= MAX_AGENT_STEPS else ""}

Respond ONLY with valid JSON in one of these three forms:

For a SQL query:
{{
  "action": "query",
  "reasoning": "Why this specific query is needed and what new insight it will provide",
  "sql": "SELECT ..."
}}

For a knowledge base search:
{{
  "action": "search_knowledge",
  "reasoning": "Why you need to search the knowledge base and what concept you're looking for",
  "search_query": "the search terms or question to look up"
}}

For an export request (only when the user asks to save/export data):
{{
  "action": "export_csv",
  "reasoning": "Why this export is needed",
  "sql": "SELECT ... (the query whose results will be exported, include LIMIT 1000000)",
  "suggested_path": "/home/user/export.csv"
}}

For the final answer:
{{
  "action": "finish",
  "reasoning": "Why you have enough information to conclude",
  "final_answer": "A detailed, structured answer for the user based on all gathered information"
}}

No markdown fences. Only raw JSON."""

        content = _call_llm(system_prompt, [{"role": "user", "content": "Proceed with the next step."}], temperature=0.2)
        return _parse_llm_json(content)

    def _execute_and_summarise(sql: str, max_rows: int = 20) -> dict:
        """Execute a ClickHouse query and return a compact summary."""
        try:
            client = get_clickhouse_client()
            result = client.query(sql)
            rows = _rows_to_dicts(result)
            total_rows = len(rows)
            preview = rows[:max_rows]
            col_names = list(preview[0].keys()) if preview else []
            summary_lines = [", ".join(str(r.get(c, "")) for c in col_names) for r in preview[:10]]
            summary = (
                f"{total_rows} row(s) returned. Columns: {col_names}.\n"
                f"Sample data:\n" + "\n".join(summary_lines)
            )
            return {"ok": True, "summary": summary, "rows": preview, "total_rows": total_rows}
        except Exception as exc:
            return {"ok": False, "summary": f"Query failed: {exc}", "rows": [], "total_rows": 0}

    # ---- Agentic loop ----
    try:
        final_answer = None
        for iteration in range(MAX_AGENT_STEPS):
            decision = _run_agent_step(steps)

            action = decision.get("action", "finish")
            reasoning = decision.get("reasoning", "")

            if action == "query" and iteration < MAX_AGENT_STEPS:
                sql = decision.get("sql", "").strip()
                exec_result = _execute_and_summarise(sql)
                steps.append({
                    "step": iteration + 1,
                    "type": "query",
                    "reasoning": reasoning,
                    "sql": sql,
                    "result_summary": exec_result["summary"],
                    "row_count": exec_result["total_rows"],
                    "ok": exec_result["ok"],
                })
            elif action == "search_knowledge" and iteration < MAX_AGENT_STEPS:
                search_query = decision.get("search_query", "").strip()
                kb_result = _search_knowledge_for_agent(search_query)
                kb_summary = kb_result[:1500] if len(kb_result) > 1500 else kb_result
                steps.append({
                    "step": iteration + 1,
                    "type": "search_knowledge",
                    "reasoning": reasoning,
                    "sql": None,
                    "search_query": search_query,
                    "result_summary": f"Knowledge base results:\n{kb_summary}",
                    "row_count": 0,
                    "ok": True,
                })
            elif action == "export_csv" and iteration < MAX_AGENT_STEPS:
                export_sql = decision.get("sql", "").strip()
                suggested_path = decision.get("suggested_path", "/tmp/export.csv").strip()
                steps.append({
                    "step": iteration + 1,
                    "type": "export_csv",
                    "reasoning": reasoning,
                    "sql": export_sql,
                    "suggested_path": suggested_path,
                    "result_summary": f"Export CSV demandé → {suggested_path}",
                    "row_count": 0,
                    "ok": True,
                })
            else:
                # action == "finish" or max steps reached
                final_answer = decision.get("final_answer", "")
                break

        # If the loop exhausted all steps without a finish, ask for synthesis
        if final_answer is None:
            step_lines = []
            for s in steps:
                if s.get("type") == "search_knowledge":
                    step_lines.append(f"Step {s['step']} (knowledge search): {s['reasoning']}\nSearch: {s.get('search_query', '')}\nResult: {s['result_summary']}")
                else:
                    step_lines.append(f"Step {s['step']}: {s['reasoning']}\nSQL: {s.get('sql', '')}\nResult: {s['result_summary']}")
            synth_prompt = f"""Based on the following analysis steps, write a detailed final answer for the user.

USER QUESTION: {user_question}

STEPS AND RESULTS:
""" + "\n\n".join(step_lines) + "\n\nProvide a comprehensive, structured answer."
            final_answer = _call_llm(synth_prompt, [{"role": "user", "content": "Synthesise the final answer."}], temperature=0.3)

        return jsonify({
            "steps": steps,
            "final_answer": final_answer,
            "total_steps": len(steps),
        })

    except Exception as exc:
        print(f"Agent error: {exc}")
        return jsonify({"error": str(exc)}), 500


# ---------------------------------------------------------------------------
# AI Query Analyzer
# ---------------------------------------------------------------------------
@app.route("/api/analyze", methods=["POST"])
def analyze_query():
    data = request.get_json()
    sql = data.get("sql", "")
    schema = data.get("schema", {})

    # Token budget: base prompt without the schema blob
    base_analyze_prompt = """You are an expert ClickHouse SQL performance analyst.
Analyze the following SQL query and provide:
1. Performance alerts (full table scans, missing LIMIT, unoptimized aggregations)
2. Correctness concerns (wrong results, type mismatches, NULL handling)
3. Optimization suggestions (better functions, indexes, partitioning hints)
4. Data projections (estimated result size, cardinality warnings)

Return ONLY a JSON object (no markdown) with:
{
  "alerts": ["alert message 1"],
  "suggestions": ["suggestion 1"],
  "projections": ["projection 1"],
  "optimized_sql": "improved SQL or empty string",
  "risk_level": "low" | "medium" | "high"
}
"""
    user_message = f"Analyze this ClickHouse SQL query:\n\n{sql}"
    base_tokens = _estimate_tokens(base_analyze_prompt) + _estimate_tokens(user_message)

    # Truncate schema context to fit within budget
    schema, _, _ = _truncate_prompt_context(schema, {}, "", base_tokens)

    system_prompt = f"""You are an expert ClickHouse SQL performance analyst.
Analyze the following SQL query and provide:
1. Performance alerts (full table scans, missing LIMIT, unoptimized aggregations)
2. Correctness concerns (wrong results, type mismatches, NULL handling)
3. Optimization suggestions (better functions, indexes, partitioning hints)
4. Data projections (estimated result size, cardinality warnings)

Database schema context:
{json.dumps(schema, indent=2)}

Return ONLY a JSON object (no markdown) with:
{{
  "alerts": ["alert message 1"],
  "suggestions": ["suggestion 1"],
  "projections": ["projection 1"],
  "optimized_sql": "improved SQL or empty string",
  "risk_level": "low" | "medium" | "high"
}}
"""

    try:
        content = _call_llm(system_prompt, [{"role": "user", "content": user_message}], temperature=0.3)
        return jsonify(_parse_llm_json(content))
    except Exception as exc:
        print(f"Analyze error: {exc}")
        return jsonify({"error": str(exc)}), 500


# ---------------------------------------------------------------------------
# Table Profiling
# ---------------------------------------------------------------------------
@app.route("/api/profile/<table_name>", methods=["GET"])
def profile_table(table_name):
    try:
        client = get_clickhouse_client()

        # 1. Column types
        desc_result = client.query(f"DESCRIBE TABLE `{table_name}`")
        columns = [{"name": r[0], "type": r[1]} for r in desc_result.result_rows]

        if not columns:
            return jsonify({"error": "Table not found or empty schema"}), 404

        # 2. Total rows
        count_result = client.query(f"SELECT count() FROM `{table_name}`")
        total_rows = int(count_result.result_rows[0][0]) if count_result.result_rows else 0

        # 3. Single stats query against a row-limited subquery.
        #
        # KEY FIX: The previous approach used "FROM table SAMPLE 0.1" or a
        # trailing LIMIT on the outer query.  Neither correctly limits the
        # rows fed into aggregate functions:
        #   • SAMPLE 0.1 requires a sampling key and fails silently on tables
        #     that don't have one — stats_row stays {}, every column gets
        #     notnull=0, which produces null_pct=100% for every column.
        #   • A trailing LIMIT on a single-row aggregate result is a no-op for
        #     the underlying scan.
        #
        # The correct pattern for large tables is to wrap the table in a
        # subquery with LIMIT so ClickHouse stops reading after N rows:
        #   SELECT agg(col) FROM (SELECT col FROM table LIMIT N)
        #
        # We also use uniq() (HyperLogLog, ~2% error) instead of uniqExact()
        # to avoid materializing huge hash sets in memory.
        profile_sample = 500_000 if total_rows > 500_000 else total_rows
        col_names_quoted = ", ".join(f"`{c['name'].replace('`','``')}`" for c in columns)
        sub = f"(SELECT {col_names_quoted} FROM `{table_name}` LIMIT {profile_sample})"

        select_parts = []
        for col in columns:
            cn = col["name"].replace("`", "``")
            select_parts.append(f"countIf(`{cn}` IS NOT NULL) AS `{cn}__notnull`")
            select_parts.append(f"uniq(`{cn}`) AS `{cn}__distinct`")

        stats_row = {}
        if select_parts:
            stats_sql = f"SELECT {', '.join(select_parts)} FROM {sub}"
            try:
                sr = client.query(stats_sql)
                if sr.result_rows:
                    stats_row = dict(zip(sr.column_names, sr.result_rows[0]))
            except Exception as e:
                print(f"Stats query warning: {e}")

        # 4. Build per-column stats
        # Scale sampled counts back to total_rows estimate when a partial sample
        # was used.  When stats_row is empty (query failed), mark as unknown
        # rather than reporting a misleading 100% null rate.
        scale = (total_rows / profile_sample) if profile_sample < total_rows and profile_sample > 0 else 1.0
        col_stats = []
        for col in columns:
            cn = col["name"]
            notnull_raw = stats_row.get(f"{cn}__notnull")
            distinct_raw = stats_row.get(f"{cn}__distinct")

            if notnull_raw is None:
                # Stats query failed for this column — report unknown
                col_stats.append({
                    "name": cn,
                    "type": col["type"],
                    "notnull": None,
                    "null_count": None,
                    "null_pct": None,
                    "distinct": None,
                    "distinct_pct": None,
                    "top_values": [],
                })
                continue

            notnull = int(notnull_raw)
            distinct = int(distinct_raw) if distinct_raw is not None else 0
            estimated_notnull = round(notnull * scale)
            null_count = max(0, total_rows - estimated_notnull)
            null_pct = round(null_count / total_rows * 100, 1) if total_rows > 0 else 0.0
            distinct_pct = round(distinct / profile_sample * 100, 1) if profile_sample > 0 else 0.0
            col_stats.append({
                "name": cn,
                "type": col["type"],
                "notnull": estimated_notnull,
                "null_count": null_count,
                "null_pct": null_pct,
                "distinct": distinct,
                "distinct_pct": distinct_pct,
                "top_values": [],
            })

        # 5. Top values for low-cardinality columns (distinct <= 30)
        # Use the same LIMIT-bounded subquery to avoid full-table scans.
        for cs in col_stats:
            if cs["distinct"] is not None and 0 < cs["distinct"] <= 30:
                try:
                    cn_q = cs['name'].replace('`', '``')
                    tv = client.query(
                        f"SELECT toString(`{cn_q}`) AS val, count() AS cnt"
                        f" FROM (SELECT `{cn_q}` FROM `{table_name}` LIMIT {profile_sample})"
                        f" GROUP BY val ORDER BY cnt DESC LIMIT 8"
                    )
                    cs["top_values"] = [
                        {"value": str(r[0]), "count": int(r[1])}
                        for r in tv.result_rows
                    ]
                except Exception:
                    pass

        return jsonify({
            "table": table_name,
            "total_rows": total_rows,
            "total_columns": len(columns),
            "columns": col_stats,
        })
    except Exception as exc:
        print(f"Profile error: {exc}")
        return jsonify({"error": str(exc)}), 500


@app.route("/api/profile/<table_name>/insights", methods=["POST"])
def profile_insights(table_name):
    data = request.get_json() or {}
    stats = data.get("stats", {})

    # Compact stats for LLM — metadata only, no raw rows
    compact = {
        "table": stats.get("table"),
        "total_rows": stats.get("total_rows"),
        "total_columns": stats.get("total_columns"),
        "columns": [
            {
                "name": c["name"],
                "type": c["type"],
                "null_pct": c["null_pct"],
                "distinct": c["distinct"],
                "top_values": c.get("top_values", [])[:3],
            }
            for c in stats.get("columns", [])
        ],
    }

    system_prompt = """You are a senior data analyst. Given a table profile (metadata only, no raw data),
identify data quality issues, patterns, and recommendations.

Return ONLY a JSON object:
{
  "summary": "2-3 sentence overview of the table and its likely purpose",
  "quality_issues": ["issue 1", "issue 2"],
  "key_insights": ["insight 1", "insight 2", "insight 3"],
  "recommendations": ["recommendation 1", "recommendation 2"]
}"""

    user_msg = f"Profile data:\n{json.dumps(compact)}"
    try:
        content = _call_llm(system_prompt, [{"role": "user", "content": user_msg}], temperature=0.4)
        return jsonify(_parse_llm_json(content))
    except Exception as exc:
        print(f"Profile insights error: {exc}")
        return jsonify({"error": str(exc)}), 500


# ---------------------------------------------------------------------------
# Data Quality Analysis
# ---------------------------------------------------------------------------

def _dq_column_stats(
    client,
    table: str,
    column: str,
    col_type: str,
    sample_size: int,
    filter_col: str | None = None,
    filter_op: str | None = None,
    filter_val: str | None = None,
) -> dict:
    """Collect a statistical profile for one column using ClickHouse queries.

    Uses a subquery with LIMIT to bound the number of rows processed — essential
    for tables with billions of rows.  A trailing LIMIT on an aggregate query only
    limits the *result* rows (always 1 for a single aggregate), NOT the rows read
    from the table, so we must wrap the table scan in a subquery.

    If filter_col/filter_op/filter_val are provided, only rows matching that
    condition are included in the analysis (applied inside the bounding subquery).
    """
    import re as _re
    stats: dict = {"column": column, "type": col_type}
    safe_table = _re.sub(r"[^\w.]", "", table)
    safe_col = _re.sub(r"[^\w]", "", column)
    n = int(sample_size) if sample_size is not None else None

    # Build optional WHERE filter clause (injected inside the bounding subquery)
    where_clause = ""
    if filter_col and filter_op and filter_val is not None:
        safe_fcol = _re.sub(r"[^\w]", "", filter_col)
        # Escape single quotes in the filter value
        escaped_val = str(filter_val).replace("'", "''")
        op_map = {"=": "=", "!=": "!=", "<": "<", ">": ">", "<=": "<=", ">=": ">=", "LIKE": "LIKE"}
        op = op_map.get(filter_op, "=")
        where_clause = f" WHERE `{safe_fcol}` {op} '{escaped_val}'"
        stats["filter_applied"] = f"`{safe_fcol}` {op} '{escaped_val}'"

    # Subquery that limits input rows — this is the correct way to bound scans
    # on large tables in ClickHouse.  Every aggregate below queries this sub.
    limit_clause = f" LIMIT {n}" if n is not None else ""
    sub = f"(SELECT `{safe_col}` FROM {safe_table}{where_clause}{limit_clause})"

    try:
        # ── Basic counts ──────────────────────────────────────────────────────
        # uniq() is approximate (~2% error) but orders-of-magnitude faster than
        # uniqExact() on large cardinality columns.
        r = client.query(
            f"SELECT count() AS total,"
            f" countIf(`{safe_col}` IS NULL) AS null_count,"
            f" uniq(`{safe_col}`) AS approx_distinct"
            f" FROM {sub}"
        )
        row = r.result_rows[0]
        total = int(row[0])
        null_count = int(row[1])
        distinct_count = int(row[2])
        stats.update({
            "total": total,
            "null_count": null_count,
            "null_pct": round(100 * null_count / max(total, 1), 2),
            "distinct_count": distinct_count,
            "distinct_pct": round(100 * distinct_count / max(total, 1), 2),
        })

        # ── Type-specific stats ───────────────────────────────────────────────
        ct_upper = col_type.upper()
        is_numeric = any(t in ct_upper for t in ("INT", "FLOAT", "DECIMAL", "DOUBLE", "NUMBER"))
        is_string = any(t in ct_upper for t in ("STRING", "VARCHAR", "FIXEDSTRING", "TEXT"))
        is_date = any(t in ct_upper for t in ("DATE", "DATETIME"))

        if is_numeric:
            r2 = client.query(
                f"SELECT min(`{safe_col}`), max(`{safe_col}`),"
                f" avg(`{safe_col}`), stddevPop(`{safe_col}`),"
                f" quantile(0.25)(`{safe_col}`), quantile(0.5)(`{safe_col}`),"
                f" quantile(0.75)(`{safe_col}`),"
                f" countIf(`{safe_col}` < 0),"
                f" countIf(`{safe_col}` = 0)"
                f" FROM {sub}"
                f" WHERE `{safe_col}` IS NOT NULL"
            )
            rr = r2.result_rows[0]
            def _f(v):
                return round(float(v), 6) if v is not None else None
            avg_val = _f(rr[2])
            stddev_val = _f(rr[3])
            p50_val = _f(rr[5])
            stats.update({
                "min": _f(rr[0]), "max": _f(rr[1]),
                "avg": avg_val, "stddev": stddev_val,
                "p25": _f(rr[4]), "p50": p50_val, "p75": _f(rr[6]),
                "negative_count": int(rr[7]) if rr[7] is not None else 0,
                "zero_count": int(rr[8]) if rr[8] is not None else 0,
            })
            # Coefficient of variation (stddev / avg) — indicates relative spread
            if avg_val and avg_val != 0 and stddev_val is not None:
                stats["coeff_variation"] = round(abs(stddev_val / avg_val), 4)
            # Pearson skewness approximation: 3*(mean - median) / stddev
            if avg_val is not None and p50_val is not None and stddev_val and stddev_val != 0:
                stats["skewness_approx"] = round(3 * (avg_val - p50_val) / stddev_val, 4)
            # Outlier detection via IQR
            if stats["p25"] is not None and stats["p75"] is not None:
                iqr = stats["p75"] - stats["p25"]
                lower = stats["p25"] - 1.5 * iqr
                upper = stats["p75"] + 1.5 * iqr
                r3 = client.query(
                    f"SELECT countIf(`{safe_col}` < {lower} OR `{safe_col}` > {upper})"
                    f" FROM {sub}"
                    f" WHERE `{safe_col}` IS NOT NULL"
                )
                oc = int(r3.result_rows[0][0])
                stats["outlier_count"] = oc
                stats["outlier_pct"] = round(100 * oc / max(total - null_count, 1), 2)
            # Outlier detection via Z-Score (|value - mean| > 3 * stddev)
            if avg_val is not None and stddev_val and stddev_val > 0:
                r4 = client.query(
                    f"SELECT countIf(abs(`{safe_col}` - {avg_val}) > 3 * {stddev_val})"
                    f" FROM {sub}"
                    f" WHERE `{safe_col}` IS NOT NULL"
                )
                zoc = int(r4.result_rows[0][0])
                stats["zscore_outlier_count"] = zoc
                stats["zscore_outlier_pct"] = round(100 * zoc / max(total - null_count, 1), 2)

        elif is_string:
            r2 = client.query(
                f"SELECT countIf(`{safe_col}` = ''),"
                f" min(length(`{safe_col}`)), max(length(`{safe_col}`)),"
                f" avg(length(`{safe_col}`)),"
                f" countIf(length(`{safe_col}`) > 1000),"
                f" countIf(length(trimBoth(`{safe_col}`)) < length(`{safe_col}`)),"
                f" countIf(upperUTF8(`{safe_col}`) = `{safe_col}` AND lowerUTF8(`{safe_col}`) != `{safe_col}`),"
                f" countIf(match(`{safe_col}`, '^[0-9]+$')),"
                f" countIf(match(`{safe_col}`, '^[^@\\\\s]+@[^@\\\\s]+\\\\.[^@\\\\s]+$'))"
                f" FROM {sub}"
                f" WHERE `{safe_col}` IS NOT NULL"
            )
            rr = r2.result_rows[0]
            empty_count = int(rr[0]) if rr[0] is not None else 0
            stats.update({
                "empty_count": empty_count,
                "empty_pct": round(100 * empty_count / max(total, 1), 2),
                "min_length": int(rr[1]) if rr[1] is not None else 0,
                "max_length": int(rr[2]) if rr[2] is not None else 0,
                "avg_length": round(float(rr[3]), 2) if rr[3] is not None else 0,
                "very_long_count": int(rr[4]) if rr[4] is not None else 0,
                "whitespace_padded_count": int(rr[5]) if rr[5] is not None else 0,
                "all_caps_count": int(rr[6]) if rr[6] is not None else 0,
                "numeric_string_count": int(rr[7]) if rr[7] is not None else 0,
                "email_like_count": int(rr[8]) if rr[8] is not None else 0,
            })
            # Detect common sentinel/garbage values
            r3 = client.query(
                f"SELECT countIf(lower(toString(`{safe_col}`)) IN"
                f" ('null','none','n/a','na','#na','#n/a','unknown','undefined','nan','nil','-','0'))"
                f" FROM {sub}"
                f" WHERE `{safe_col}` IS NOT NULL"
            )
            stats["sentinel_count"] = int(r3.result_rows[0][0])

        elif is_date:
            r2 = client.query(
                f"SELECT min(`{safe_col}`), max(`{safe_col}`),"
                f" countIf(`{safe_col}` > now()),"
                f" countIf(toDate(`{safe_col}`) = '1970-01-01'),"
                f" countIf(toDayOfWeek(`{safe_col}`) >= 6),"
                f" countIf(toYear(`{safe_col}`) < 1900)"
                f" FROM {sub}"
                f" WHERE `{safe_col}` IS NOT NULL"
            )
            rr = r2.result_rows[0]
            stats.update({
                "min_date": str(rr[0]) if rr[0] is not None else None,
                "max_date": str(rr[1]) if rr[1] is not None else None,
                "future_count": int(rr[2]) if rr[2] is not None else 0,
                "epoch_sentinel_count": int(rr[3]) if rr[3] is not None else 0,
                "weekend_count": int(rr[4]) if rr[4] is not None else 0,
                "pre_1900_count": int(rr[5]) if rr[5] is not None else 0,
            })

        # ── Top 10 most frequent values (from the same limited sample) ────────
        r_top = client.query(
            f"SELECT toString(`{safe_col}`) AS val, count() AS cnt"
            f" FROM {sub}"
            f" WHERE `{safe_col}` IS NOT NULL"
            f" GROUP BY `{safe_col}` ORDER BY cnt DESC LIMIT 10"
        )
        stats["top_values"] = [
            {"value": str(row[0]), "count": int(row[1])} for row in r_top.result_rows
        ]

    except Exception as e:
        stats["query_error"] = str(e)

    return stats


@app.route("/api/data-quality/analyze", methods=["POST"])
def analyze_data_quality():
    """Run AI-powered data quality analysis on selected table columns."""
    import re as _re
    data = request.get_json()
    table = (data.get("table") or "").strip()
    columns = data.get("columns", [])
    _sample_raw = data.get("sample_size")
    sample_size = min(int(_sample_raw), 500000) if _sample_raw is not None else None
    # Optional row filter
    filter_col = (data.get("filter_column") or "").strip() or None
    filter_op = (data.get("filter_operator") or "=").strip()
    filter_val = data.get("filter_value")
    if filter_val is not None:
        filter_val = str(filter_val)
    # Optional time series column for temporal/volume analysis
    time_col = (data.get("time_column") or "").strip() or None

    if not table:
        return jsonify({"error": "Table name is required"}), 400
    if not columns:
        return jsonify({"error": "At least one column is required"}), 400
    # Basic name validation to prevent SQL injection
    if not _re.match(r"^[\w.]+$", table):
        return jsonify({"error": "Invalid table name"}), 400
    for col in columns:
        if not _re.match(r"^\w+$", col):
            return jsonify({"error": f"Invalid column name: {col}"}), 400
    if filter_col and not _re.match(r"^\w+$", filter_col):
        return jsonify({"error": "Invalid filter column name"}), 400
    allowed_ops = {"=", "!=", "<", ">", "<=", ">=", "LIKE"}
    if filter_op not in allowed_ops:
        filter_op = "="

    safe_table = _re.sub(r"[^\w.]", "", table)

    # Build filter WHERE clause used both for per-column stats and volume analysis
    where_clause = ""
    if filter_col and filter_op and filter_val is not None:
        safe_fcol = _re.sub(r"[^\w]", "", filter_col)
        escaped_val = str(filter_val).replace("'", "''")
        op_map = {"=": "=", "!=": "!=", "<": "<", ">": ">", "<=": "<=", ">=": ">=", "LIKE": "LIKE"}
        op = op_map.get(filter_op, "=")
        where_clause = f" WHERE `{safe_fcol}` {op} '{escaped_val}'"

    try:
        client = get_clickhouse_client()

        # Get column types from DESCRIBE TABLE
        desc = client.query(f"DESCRIBE TABLE {table}")
        col_types = {row[0]: row[1] for row in desc.result_rows}

        # Collect per-column stats
        column_stats = []
        for col in columns:
            col_type = col_types.get(col, "String")
            cs = _dq_column_stats(
                client, table, col, col_type, sample_size,
                filter_col=filter_col, filter_op=filter_op, filter_val=filter_val,
            )
            column_stats.append(cs)

        # ── Volume Consistency Analysis (Temporal) ────────────────────────────
        volume_analysis = None
        if time_col and _re.match(r"^\w+$", time_col):
            safe_time_col = _re.sub(r"[^\w]", "", time_col)
            try:
                # Auto-detect granularity: use hourly if range < 7 days, else daily
                range_r = client.query(
                    f"SELECT min(`{safe_time_col}`), max(`{safe_time_col}`) FROM {safe_table}"
                )
                rr = range_r.result_rows[0]
                min_ts, max_ts = rr[0], rr[1]
                try:
                    from datetime import timezone as _tz
                    delta_days = (max_ts - min_ts).days if hasattr(max_ts, 'days') else 999
                except Exception:
                    delta_days = 999
                granularity = "hour" if delta_days <= 7 else "day"
                trunc_fn = "toStartOfHour" if granularity == "hour" else "toStartOfDay"

                if where_clause:
                    where_vol = where_clause + f" AND `{safe_time_col}` IS NOT NULL"
                else:
                    where_vol = f" WHERE `{safe_time_col}` IS NOT NULL"
                vol_r = client.query(
                    f"SELECT {trunc_fn}(`{safe_time_col}`) AS period, count() AS cnt"
                    f" FROM {safe_table}{where_vol}"
                    f" GROUP BY period ORDER BY period"
                )
                vol_rows = [{"period": str(row[0]), "count": int(row[1])} for row in vol_r.result_rows]

                if vol_rows:
                    counts = [r["count"] for r in vol_rows]
                    vol_avg = sum(counts) / len(counts)
                    vol_stddev = (sum((c - vol_avg) ** 2 for c in counts) / max(len(counts), 1)) ** 0.5
                    vol_p25 = sorted(counts)[len(counts) // 4]
                    vol_p75 = sorted(counts)[3 * len(counts) // 4]
                    iqr_vol = vol_p75 - vol_p25
                    low_threshold = max(1, vol_p25 - 1.5 * iqr_vol)
                    anomaly_periods = [r for r in vol_rows if r["count"] < low_threshold]
                    volume_analysis = {
                        "time_column": time_col,
                        "granularity": granularity,
                        "periods": len(vol_rows),
                        "avg_volume": round(vol_avg, 1),
                        "stddev_volume": round(vol_stddev, 1),
                        "min_volume": min(counts),
                        "max_volume": max(counts),
                        "p25_volume": vol_p25,
                        "p75_volume": vol_p75,
                        "low_volume_threshold": round(low_threshold, 1),
                        "anomaly_count": len(anomaly_periods),
                        "anomaly_periods": anomaly_periods[:20],
                        "recent_periods": vol_rows[-10:],
                    }
            except Exception as ve:
                volume_analysis = {"error": str(ve), "time_column": time_col}

        # ── LLM analysis ─────────────────────────────────────────────────────
        stats_json = json.dumps(column_stats, indent=2, default=str)

        filter_note = ""
        if filter_col and filter_val is not None:
            filter_note = f" (filtered to rows where `{filter_col}` {filter_op} '{filter_val}')"

        volume_note = ""
        if volume_analysis and "error" not in volume_analysis:
            volume_note = f"\n\nVOLUME CONSISTENCY (by {volume_analysis['granularity']}):\n{json.dumps(volume_analysis, indent=2, default=str)}"

        system_prompt = """You are an expert data quality analyst specializing in database analytics and anomaly detection.
Analyze the statistical profiles of the provided columns and identify all data quality issues.

Evaluate each column for:
1. **Null/Empty anomalies**: High null rates, sentinel values used as nulls (0, -1, 'N/A', 'null', 'none'…)
2. **Format anomalies**: Inconsistent patterns, unexpected length distributions, mixed formats
3. **Content anomalies**: Impossible values, values outside expected business domain
4. **Cardinality anomalies**: Suspiciously low distinct counts (near-constant column), or unexpectedly high cardinality
5. **Distribution anomalies**: Extreme skew (use skewness_approx), statistical outliers (IQR method), Z-Score outliers (zscore_outlier_count/zscore_outlier_pct), zero inflation (zero_count), highly unbalanced categories
6. **Temporal anomalies**: Future dates, epoch sentinel dates (1970-01-01, epoch_sentinel_count), implausible date ranges (pre_1900_count), weekend concentration (weekend_count)
7. **String quality**: Whitespace padding (whitespace_padded_count), ALL-CAPS inconsistency (all_caps_count), numeric strings in text fields (numeric_string_count), unexpected email-like values (email_like_count)
8. **Spread anomalies**: High coefficient of variation (coeff_variation > 1 indicates extreme spread), high skewness (|skewness_approx| > 2 indicates strong skew)
9. **Volume/Temporal consistency** (if provided): Periods with abnormally low record counts may indicate pipeline issues or data loss

For outlier severity guidance:
- IQR outliers > 10% of rows = warning, > 25% = critical
- Z-Score outliers (3σ) > 5% = warning, > 15% = critical
- If both IQR and Z-Score detect outliers, the column likely has heavy tails

Return ONLY valid JSON with this exact structure:
{
  "summary": "<2-3 sentence overall data quality assessment>",
  "quality_score": <integer 0-100>,
  "columns": [
    {
      "column": "<column_name>",
      "quality_score": <integer 0-100>,
      "issues": [
        {
          "severity": "critical|warning|info",
          "category": "nulls|format|content|cardinality|distribution|temporal|string_quality|spread|volume",
          "title": "<short issue title>",
          "description": "<detailed description with specific numbers>",
          "affected_rows": <number or null>,
          "recommendation": "<concrete actionable fix>"
        }
      ],
      "insights": "<what looks good or any useful context about this column>"
    }
  ],
  "recommendations": ["<global recommendation 1>", "<global recommendation 2>"]
}"""

        user_msg = (
            f"Analyze data quality for table `{table}` (sample of {sample_size:,} rows{filter_note}).\n\n"
            f"Column Statistics:\n{stats_json}{volume_note}\n\n"
            "Provide a thorough analysis identifying all data quality issues with specific numbers."
        )

        llm_response = _call_llm(system_prompt, [{"role": "user", "content": user_msg}], temperature=0.1)

        try:
            analysis = _parse_llm_json(llm_response)
        except Exception:
            analysis = {
                "summary": llm_response,
                "quality_score": None,
                "columns": [],
                "recommendations": [],
            }

        return jsonify({
            "table": table,
            "sample_size": sample_size,  # None when full scan (no LIMIT)
            "column_stats": column_stats,
            "analysis": analysis,
            "volume_analysis": volume_analysis,
        })

    except Exception as exc:
        print(f"Data quality error: {exc}")
        return jsonify({"error": str(exc)}), 500


# ---------------------------------------------------------------------------
# CSV Export
# ---------------------------------------------------------------------------
MAX_EXPORT_ROWS = 1_000_000


@app.route("/api/export_csv", methods=["POST"])
def export_csv():
    """Export a ClickHouse query result to a pipe-separated CSV file.

    Body parameters:
      - sql  (required): the SELECT query to execute
      - output_path (optional): absolute server-side path to write the file.
        When provided the file is written locally and the path + row count are
        returned. When absent the CSV content is sent as an HTTP download.

    Row count is capped at MAX_EXPORT_ROWS (1 000 000).
    """
    import csv
    import io as _io

    data = request.get_json(silent=True) or {}
    sql = (data.get("sql") or "").strip()
    output_path = (data.get("output_path") or "").strip()

    if not sql:
        return jsonify({"error": "Aucune requête SQL fournie."}), 400

    # Ensure the query has a LIMIT that does not exceed MAX_EXPORT_ROWS.
    # We inject one when absent; when the user already has LIMIT we leave it
    # so that their intentional cap is respected (it will still be capped
    # client-side at MAX_EXPORT_ROWS rows after fetch).
    if "LIMIT" not in sql.upper():
        sql = f"{sql} LIMIT {MAX_EXPORT_ROWS}"

    try:
        client = get_clickhouse_client()
        result = client.query(sql)
        rows = _rows_to_dicts(result)
        rows = rows[:MAX_EXPORT_ROWS]

        if not rows:
            return jsonify({"error": "La requête n'a retourné aucune donnée."}), 400

        # Build pipe-separated CSV
        output = _io.StringIO()
        writer = csv.writer(output, delimiter="|", quoting=csv.QUOTE_MINIMAL)
        writer.writerow(rows[0].keys())
        for row in rows:
            writer.writerow(row.values())
        csv_content = output.getvalue()

        if output_path:
            parent_dir = os.path.dirname(output_path)
            if parent_dir:
                os.makedirs(parent_dir, exist_ok=True)
            with open(output_path, "w", encoding="utf-8", newline="") as f:
                f.write(csv_content)
            return jsonify({"path": output_path, "row_count": len(rows)})
        else:
            from flask import Response
            return Response(
                csv_content,
                mimetype="text/csv; charset=utf-8",
                headers={"Content-Disposition": 'attachment; filename="export.csv"'},
            )

    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ---------------------------------------------------------------------------
# Config export / import
# ---------------------------------------------------------------------------
@app.route("/api/config/export", methods=["GET"])
def export_config():
    """Export all configuration and data as a single JSON file."""
    db = read_db()
    payload = {
        "version": 1,
        "clickhouseConfig": clickhouse_config,
        "llmConfig": llm_config,
        "ragConfig": rag_config,
        "knowledge_folders": db.get("knowledge_folders", []),
        "table_mappings": db.get("table_mappings", []),
        "table_metadata": db.get("table_metadata", []),
        "saved_queries": db.get("saved_queries", []),
    }
    return app.response_class(
        json.dumps(payload, indent=2, ensure_ascii=False),
        mimetype="application/json",
        headers={"Content-Disposition": "attachment; filename=clicksense-config.json"},
    )


@app.route("/api/config/import", methods=["POST"])
def import_config():
    """Import configuration and data from a previously exported JSON file."""
    global clickhouse_config, llm_config, rag_config
    data = request.get_json()
    if not data:
        return jsonify({"error": "Empty payload"}), 400

    if "clickhouseConfig" in data:
        clickhouse_config.update(data["clickhouseConfig"])
    if "llmConfig" in data:
        llm_config.update(data["llmConfig"])
    if "ragConfig" in data:
        rag_config.update(data["ragConfig"])

    db = read_db()
    if "knowledge_folders" in data:
        db["knowledge_folders"] = data["knowledge_folders"]
    if "table_mappings" in data:
        db["table_mappings"] = data["table_mappings"]
    if "table_metadata" in data:
        db["table_metadata"] = data["table_metadata"]
    if "saved_queries" in data:
        db["saved_queries"] = data["saved_queries"]
    write_db(db)

    return jsonify({"success": True})


# ---------------------------------------------------------------------------
# Serve built frontend
# ---------------------------------------------------------------------------
@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def serve_frontend(path):
    if path.startswith("api/"):
        return jsonify({"error": "Not found"}), 404
    if os.path.exists(DIST_DIR):
        full_path = os.path.join(DIST_DIR, path)
        if path and os.path.isfile(full_path):
            return send_from_directory(DIST_DIR, path)
        return send_from_directory(DIST_DIR, "index.html")
    return (
        "<h2>Frontend not built.</h2><p>Run <code>npm run build</code> first.</p>",
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
