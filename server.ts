import express from "express";
import { createServer as createViteServer } from "vite";
import { createClient } from "@clickhouse/client";
import dotenv from "dotenv";
import fs from "fs";
import path from "path";

dotenv.config();

const app = express();
const PORT = 3000;

app.use(express.json());

// Simple JSON File Database for maximum compatibility
const DB_DIR = path.join(process.cwd(), '.data');
if (!fs.existsSync(DB_DIR)) {
  fs.mkdirSync(DB_DIR, { recursive: true });
}
const DB_FILE = path.join(DB_DIR, 'app.json');

// Migrate existing app.json if it exists
const OLD_DB_FILE = path.join(process.cwd(), 'app.json');
if (fs.existsSync(OLD_DB_FILE) && !fs.existsSync(DB_FILE)) {
  fs.renameSync(OLD_DB_FILE, DB_FILE);
}

interface DBSchema {
  users: { id: number; name: string }[];
  saved_queries: { id: number; user_id: number; name: string; sql: string; config: string; visual_type: string }[];
  query_history: { id: number; user_id: number; query_text: string; sql: string; created_at: string }[];
  table_metadata: { table_name: string; description: string; is_favorite: number }[];
}

const defaultDb: DBSchema = {
  users: [{ id: 1, name: 'Default User' }],
  saved_queries: [],
  query_history: [],
  table_metadata: []
};

function readDb(): DBSchema {
  try {
    if (fs.existsSync(DB_FILE)) {
      return JSON.parse(fs.readFileSync(DB_FILE, 'utf-8'));
    }
  } catch (e) {
    console.error("Error reading DB:", e);
  }
  return { ...defaultDb };
}

function writeDb(data: DBSchema) {
  try {
    fs.writeFileSync(DB_FILE, JSON.stringify(data, null, 2));
  } catch (e) {
    console.error("Error writing DB:", e);
  }
}

// Initialize DB file if not exists
if (!fs.existsSync(DB_FILE)) {
  writeDb(defaultDb);
}

// In-memory store for simplicity, could use sqlite
let clickhouseConfig = {
  host: process.env.CLICKHOUSE_HOST || "http://localhost:8123",
  username: process.env.CLICKHOUSE_USER || "default",
  password: process.env.CLICKHOUSE_PASSWORD || "",
  database: process.env.CLICKHOUSE_DB || "default",
};

let llmConfig = {
  provider: "ollama", // "ollama" or "http"
  model: "llama3",
  ollamaUrl: "http://localhost:11434",
  httpUrl: "http://localhost:1234",
  apiKey: "",
};

let knowledgeBase = "";

// ClickHouse Client
const getClickHouseClient = () => {
  return createClient({
    url: clickhouseConfig.host,
    username: clickhouseConfig.username,
    password: clickhouseConfig.password,
    database: clickhouseConfig.database,
  });
};

// API Routes
app.get("/api/config", (req, res) => {
  res.json({ clickhouseConfig, llmConfig, knowledgeBase });
});

app.post("/api/config", (req, res) => {
  const { clickhouse, llm, knowledge } = req.body;
  if (clickhouse) clickhouseConfig = { ...clickhouseConfig, ...clickhouse };
  if (llm) llmConfig = { ...llmConfig, ...llm };
  if (knowledge !== undefined) knowledgeBase = knowledge;
  res.json({ success: true });
});

app.post("/api/clickhouse/test", async (req, res) => {
  try {
    const { host, username, password, database } = req.body;
    const client = createClient({
      url: host,
      username: username,
      password: password,
      database: database,
    });
    await client.query({ query: "SELECT 1", format: "JSONEachRow" });
    res.json({ success: true });
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

app.post("/api/llm/test", async (req, res) => {
  try {
    const { provider, ollamaUrl, httpUrl, apiKey } = req.body;
    if (provider === "ollama") {
      const response = await fetch(`${ollamaUrl}/api/tags`);
      if (!response.ok) throw new Error(`Ollama error: ${response.statusText}`);
    } else if (provider === "http") {
      const response = await fetch(`${httpUrl}/v1/models`, {
        headers: apiKey ? { "Authorization": `Bearer ${apiKey}` } : {}
      });
      if (!response.ok) throw new Error(`HTTP error: ${response.statusText}`);
    }
    res.json({ success: true });
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

app.get("/api/llm/models", async (req, res) => {
  try {
    if (llmConfig.provider === "ollama") {
      const response = await fetch(`${llmConfig.ollamaUrl}/api/tags`);
      if (!response.ok) throw new Error(`Ollama error: ${response.statusText}`);
      const data = await response.json();
      res.json({ models: data.models.map((m: any) => m.name) });
    } else if (llmConfig.provider === "http") {
      const response = await fetch(`${llmConfig.httpUrl}/v1/models`, {
        headers: llmConfig.apiKey ? { "Authorization": `Bearer ${llmConfig.apiKey}` } : {}
      });
      if (!response.ok) throw new Error(`HTTP error: ${response.statusText}`);
      const data = await response.json();
      res.json({ models: data.data.map((m: any) => m.id) });
    } else {
      res.json({ models: [] });
    }
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

app.get("/api/schema", async (req, res) => {
  try {
    const client = getClickHouseClient();
    const query = `
      SELECT table, name, type 
      FROM system.columns 
      WHERE database = '${clickhouseConfig.database}'
      ORDER BY table, name
    `;
    const resultSet = await client.query({ query, format: "JSONEachRow" });
    const rows = await resultSet.json();
    
    // Group by table
    const schema = rows.reduce((acc: any, row: any) => {
      if (!acc[row.table]) acc[row.table] = [];
      acc[row.table].push({ name: row.name, type: row.type });
      return acc;
    }, {});
    
    res.json({ schema });
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

// --- User & Dashboard APIs ---
app.get("/api/users", (req, res) => {
  const db = readDb();
  res.json(db.users);
});

app.get("/api/tables/metadata", (req, res) => {
  const db = readDb();
  const result = db.table_metadata.reduce((acc: any, row: any) => {
    acc[row.table_name] = { description: row.description, is_favorite: Boolean(row.is_favorite) };
    return acc;
  }, {});
  res.json(result);
});

app.post("/api/tables/metadata", (req, res) => {
  const { table_name, description, is_favorite } = req.body;
  const db = readDb();
  const existingIndex = db.table_metadata.findIndex(m => m.table_name === table_name);
  
  if (existingIndex >= 0) {
    db.table_metadata[existingIndex].description = description || '';
    db.table_metadata[existingIndex].is_favorite = is_favorite ? 1 : 0;
  } else {
    db.table_metadata.push({ table_name, description: description || '', is_favorite: is_favorite ? 1 : 0 });
  }
  
  writeDb(db);
  res.json({ success: true });
});

app.post("/api/history", (req, res) => {
  const { user_id, query_text, sql } = req.body;
  const db = readDb();
  const newId = db.query_history.length > 0 ? Math.max(...db.query_history.map(h => h.id)) + 1 : 1;
  db.query_history.push({
    id: newId,
    user_id,
    query_text,
    sql,
    created_at: new Date().toISOString()
  });
  writeDb(db);
  res.json({ success: true });
});

app.get("/api/history/:user_id", (req, res) => {
  const db = readDb();
  const history = db.query_history
    .filter(h => h.user_id === Number(req.params.user_id))
    .sort((a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime())
    .slice(0, 20);
  res.json(history);
});

app.post("/api/saved_queries", (req, res) => {
  const { user_id, name, sql, config, visual_type } = req.body;
  const db = readDb();
  const newId = db.saved_queries.length > 0 ? Math.max(...db.saved_queries.map(q => q.id)) + 1 : 1;
  db.saved_queries.push({
    id: newId,
    user_id,
    name,
    sql,
    config: JSON.stringify(config),
    visual_type
  });
  writeDb(db);
  res.json({ success: true });
});

app.get("/api/saved_queries/:user_id", (req, res) => {
  const db = readDb();
  const queries = db.saved_queries.filter(q => q.user_id === Number(req.params.user_id));
  res.json(queries.map((q: any) => ({ ...q, config: JSON.parse(q.config) })));
});

app.delete("/api/saved_queries/:id", (req, res) => {
  const db = readDb();
  db.saved_queries = db.saved_queries.filter(q => q.id !== Number(req.params.id));
  writeDb(db);
  res.json({ success: true });
});

app.post("/api/query", async (req, res) => {
  try {
    const { query } = req.body;
    const client = getClickHouseClient();
    const resultSet = await client.query({ 
      query, 
      format: "JSONEachRow",
      // --- QUERY GUARDRAILS ---
      // Prevent accidental cluster overload from AI-generated queries
      clickhouse_settings: {
        max_execution_time: 15,          // Kill query after 15 seconds
        max_rows_to_read: "1000000000",    // Stop if reading more than 1 Billion rows
        max_bytes_to_read: "50000000000",  // Stop if reading more than 50GB
        readonly: "1"                      // Strictly prevent any mutations (INSERT/DROP/ALTER)
      }
    });
    const data = await resultSet.json();
    res.json({ data });
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

app.post("/api/chat", async (req, res) => {
  try {
    const { messages, schema, tableMetadata } = req.body;
    
    const systemPrompt = `
      You are an expert ClickHouse data analyst.
      Your goal is to help the user query their database.
      
      Here is the database schema:
      ${JSON.stringify(schema, null, 2)}
      
      Here is the table metadata (functional descriptions):
      ${JSON.stringify(tableMetadata || {}, null, 2)}
      
      Here is the functional knowledge base to help you understand the business context:
      ${knowledgeBase}
      
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
      {
        "sql": "SELECT ...",
        "explanation": "A brief explanation of what the query does and which advanced ClickHouse function was used.",
        "suggestedVisual": "table" | "bar" | "line" | "pie"
      }
      Do not include markdown formatting like \`\`\`json. Just the raw JSON object.
    `;

    // Map messages to remove extra properties like sql/visual before sending to LLM
    const formattedMessages = messages.map((m: any) => ({
      role: m.role,
      content: m.content
    }));

    if (llmConfig.provider === "http") {
      const response = await fetch(`${llmConfig.httpUrl}/v1/chat/completions`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          ...(llmConfig.apiKey ? { "Authorization": `Bearer ${llmConfig.apiKey}` } : {})
        },
        body: JSON.stringify({
          model: llmConfig.model,
          messages: [
            { role: "system", content: systemPrompt },
            ...formattedMessages
          ],
          response_format: { type: "json_object" }
        })
      });
      
      if (!response.ok) {
        const errText = await response.text();
        throw new Error(`HTTP LLM Error: ${response.status} - ${errText}`);
      }
      
      const data = await response.json();
      let content = data.choices[0].message.content;
      content = content.replace(/```json/g, '').replace(/```/g, '').trim();
      res.json(JSON.parse(content));
    } else if (llmConfig.provider === "ollama") {
      const response = await fetch(`${llmConfig.ollamaUrl}/api/chat`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          model: llmConfig.model || "llama3",
          messages: [
            { role: "system", content: systemPrompt },
            ...formattedMessages
          ],
          stream: false,
          format: "json"
        })
      });
      
      const data = await response.json();
      res.json(JSON.parse(data.message.content));
    } else {
      res.status(400).json({ error: "Invalid LLM provider" });
    }
  } catch (error: any) {
    console.error("Chat error:", error);
    res.status(500).json({ error: error.message });
  }
});

async function startServer() {
  // Vite middleware for development
  if (process.env.NODE_ENV !== "production") {
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: "spa",
    });
    app.use(vite.middlewares);
  }

  app.listen(PORT, "0.0.0.0", () => {
    console.log(`Server running on http://localhost:${PORT}`);
  });
}

startServer();
