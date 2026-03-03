<div align="center">
<img width="1200" height="475" alt="GHBanner" src="https://github.com/user-attachments/assets/0aa67016-6eaf-458a-adb2-6e31a0763ed6" />
</div>

# ClickSense AI

Self-service ClickHouse analysis tool with an AI assistant, drag-and-drop visual query builder, saved dashboards, and a knowledge base — all powered by a Python/Flask backend.

## Features

- **AI Chat** — natural language → ClickHouse SQL via any Ollama or OpenAI-compatible LLM
- **Visual Query Builder** — drag-and-drop dimensions & measures, table/bar/line/pie views
- **Grid table** — sortable columns, inline column filters, drag-to-reorder columns, per-column colour styling
- **Dashboard** — save and replay queries as mini charts
- **Knowledge Base** — inject business context into the LLM system prompt
- **Settings** — configure ClickHouse and LLM connections at runtime

## Stack

| Layer | Technology |
|---|---|
| Backend | Python 3.9+ · Flask 3 |
| Analytics DB | ClickHouse (via `clickhouse-connect`) |
| AI / LLM | Ollama or any OpenAI-compatible HTTP endpoint |
| Frontend | React 19 · TypeScript · Vite 6 |
| Styling | Tailwind CSS 4 |
| State | Zustand |
| Charts | Recharts |
| Drag-and-drop | @dnd-kit |

---

## Prerequisites

- **Python 3.9+**
- **Node.js 18+**
- A running **ClickHouse** instance
- An **LLM** reachable via Ollama or an OpenAI-compatible API

---

## Getting Started

### 1 — Install Python dependencies

```bash
pip install -r requirements.txt
```

### 2 — Install Node dependencies

```bash
npm install
```

### 3 — Environment variables (optional)

Copy `.env.example` to `.env` and set your defaults:

```env
CLICKHOUSE_HOST=http://localhost:8123
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
CLICKHOUSE_DB=default
PORT=3000           # Flask port (default 3000)
FLASK_DEBUG=false
```

All values can also be changed at runtime from the **Settings** panel in the UI.

---

## Running in Development

Two processes must run in parallel — open two terminals:

**Terminal 1 — Flask backend (API)**
```bash
python server.py
```

**Terminal 2 — Vite frontend (HMR dev server)**
```bash
npm run dev
```

The Vite dev server proxies every `/api/*` request to `http://localhost:3000`, so the frontend always talks to the Flask backend automatically.

Open **http://localhost:5173** in your browser.

---

## Building for Production

```bash
npm run build        # generates dist/
python server.py     # serves API + static frontend on port 3000
```

Open **http://localhost:3000**.

---

## Project Structure

```
ClickSense/
├── server.py                  # Flask backend (API + static serving)
├── requirements.txt           # Python dependencies
├── src/
│   ├── components/
│   │   ├── ChatPane.tsx       # AI chat interface
│   │   ├── BuilderPane.tsx    # Visual query builder + grid/charts
│   │   ├── DashboardPane.tsx  # Saved queries dashboard
│   │   ├── SettingsPane.tsx   # DB & LLM configuration
│   │   ├── KnowledgeBasePane.tsx
│   │   └── Sidebar.tsx
│   ├── App.tsx
│   ├── store.ts               # Zustand global state
│   └── main.tsx
├── vite.config.ts             # Vite config (dev proxy → Flask)
├── package.json
└── .data/
    └── app.json               # Local JSON file database
```

---

## API Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/api/config` | Get current configuration |
| POST | `/api/config` | Update ClickHouse / LLM / knowledge base config |
| POST | `/api/clickhouse/test` | Test ClickHouse connection |
| GET | `/api/schema` | Fetch database schema |
| POST | `/api/query` | Execute a ClickHouse query (read-only, guardrails) |
| POST | `/api/chat` | Generate SQL from natural language (LLM) |
| GET | `/api/llm/models` | List available LLM models |
| POST | `/api/llm/test` | Test LLM connection |
| GET | `/api/users` | List users |
| GET/POST | `/api/tables/metadata` | Read / write table descriptions & favourites |
| POST | `/api/history` | Save a query to history |
| GET | `/api/history/:user_id` | Get query history for a user |
| POST | `/api/saved_queries` | Save a query to the dashboard |
| GET | `/api/saved_queries/:user_id` | Get dashboard queries for a user |
| DELETE | `/api/saved_queries/:id` | Delete a saved query |
