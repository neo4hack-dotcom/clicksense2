import { useState, useRef, useEffect } from 'react';
import {
  Send, Bot, Loader2, Sparkles, Play, Save, History, Trash2,
  Maximize2, Minimize2, Minus, CheckSquare, Filter, X,
  Brain, ChevronDown, ChevronRight, CheckCircle2, XCircle, Database,
  AlertTriangle, Info, Lightbulb, TrendingUp, TrendingDown, BarChart2, BookOpen,
  Download, FolderOpen,
} from 'lucide-react';
import { useAppStore } from '../store';
import clsx from 'clsx';
import { motion, AnimatePresence } from 'motion/react';

// ── Markdown renderer ──────────────────────────────────────────────────────

function renderInline(text: string, isUser = false) {
  const parts: any[] = [];
  let remaining = text;
  let key = 0;

  while (remaining.length > 0) {
    const boldMatch = remaining.match(/^([\s\S]*?)(\*\*|__)(.+?)\2/);
    const italicMatch = remaining.match(/^([\s\S]*?)(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)/);
    const codeMatch = remaining.match(/^([\s\S]*?)`([^`]+)`/);

    const bPos = boldMatch ? (boldMatch[1] || '').length : Infinity;
    const iPos = italicMatch ? (italicMatch[1] || '').length : Infinity;
    const cPos = codeMatch ? (codeMatch[1] || '').length : Infinity;
    const first = Math.min(bPos, iPos, cPos);

    if (first === Infinity) {
      parts.push(<span key={key++}>{remaining}</span>);
      break;
    }

    if (boldMatch && first === bPos) {
      if (boldMatch[1]) parts.push(<span key={key++}>{boldMatch[1]}</span>);
      parts.push(<strong key={key++} className={isUser ? 'font-bold text-white' : 'font-bold text-slate-900'}>{boldMatch[3]}</strong>);
      remaining = remaining.slice(boldMatch[0].length);
    } else if (italicMatch && first === iPos) {
      if (italicMatch[1]) parts.push(<span key={key++}>{italicMatch[1]}</span>);
      parts.push(<em key={key++} className="italic">{italicMatch[2]}</em>);
      remaining = remaining.slice(italicMatch[0].length);
    } else if (codeMatch && first === cPos) {
      if (codeMatch[1]) parts.push(<span key={key++}>{codeMatch[1]}</span>);
      parts.push(
        <code key={key++} className={clsx(
          'font-mono text-xs px-1.5 py-0.5 rounded mx-0.5 border',
          isUser ? 'bg-blue-600/40 text-blue-100 border-blue-400/30' : 'bg-slate-100 text-slate-700 border-slate-200'
        )}>{codeMatch[2]}</code>
      );
      remaining = remaining.slice(codeMatch[0].length);
    } else {
      parts.push(<span key={key++}>{remaining[0]}</span>);
      remaining = remaining.slice(1);
    }
  }
  return parts;
}

function MarkdownContent({ text, isUser = false }: { text: string; isUser?: boolean }) {
  if (!text) return null;

  const lines = text.split('\n');
  const elements: any[] = [];
  let listBuffer: { text: string; ordered: boolean }[] = [];
  let listOrdered = false;
  let key = 0;

  const flushList = () => {
    if (listBuffer.length === 0) return;
    const isOl = listOrdered;
    const items = [...listBuffer];
    listBuffer = [];
    elements.push(
      <div key={key++} className="my-1">
        {items.map((item, i) => (
          <div key={i} className="flex items-start gap-2 text-sm leading-relaxed py-0.5">
            {isOl
              ? <span className={clsx('shrink-0 text-xs font-bold mt-0.5 min-w-[1.2rem]', isUser ? 'text-blue-200' : 'text-slate-400')}>{i + 1}.</span>
              : <span className={clsx('shrink-0 mt-2 w-1.5 h-1.5 rounded-full', isUser ? 'bg-blue-200' : 'bg-slate-400')} />
            }
            <span>{renderInline(item.text, isUser)}</span>
          </div>
        ))}
      </div>
    );
  };

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];

    // Heading ### / ## / #
    const headingMatch = line.match(/^(#{1,3})\s+(.+)/);
    if (headingMatch) {
      flushList();
      const level = headingMatch[1].length;
      const cls = level === 1
        ? clsx('text-base font-bold mt-2.5 mb-1', isUser ? 'text-white' : 'text-slate-900')
        : level === 2
          ? clsx('text-sm font-bold mt-2 mb-0.5', isUser ? 'text-blue-100 border-b border-blue-400/30 pb-0.5' : 'text-slate-800 border-b border-slate-200 pb-0.5')
          : clsx('text-xs font-bold uppercase tracking-wide mt-1.5', isUser ? 'text-blue-200' : 'text-slate-600');
      elements.push(<p key={key++} className={cls}>{renderInline(headingMatch[2], isUser)}</p>);
      continue;
    }

    // Horizontal rule
    if (/^-{3,}$/.test(line.trim()) || /^\*{3,}$/.test(line.trim())) {
      flushList();
      elements.push(<hr key={key++} className={clsx('my-2', isUser ? 'border-blue-400/30' : 'border-slate-200')} />);
      continue;
    }

    // Blockquote > text
    const bqMatch = line.match(/^>\s*(.*)/);
    if (bqMatch) {
      flushList();
      elements.push(
        <div key={key++} className={clsx(
          'border-l-2 pl-3 my-1 text-sm italic',
          isUser ? 'border-blue-300/60 text-blue-100' : 'border-slate-300 text-slate-500'
        )}>
          {renderInline(bqMatch[1], isUser)}
        </div>
      );
      continue;
    }

    // Unordered list: - * •
    const ulMatch = line.match(/^[\s]*[-*•]\s+(.+)/);
    if (ulMatch) {
      if (listBuffer.length > 0 && listOrdered) flushList();
      listOrdered = false;
      listBuffer.push({ text: ulMatch[1], ordered: false });
      continue;
    }

    // Ordered list: 1. 2. etc.
    const olMatch = line.match(/^[\s]*\d+[.)]\s+(.+)/);
    if (olMatch) {
      if (listBuffer.length > 0 && !listOrdered) flushList();
      listOrdered = true;
      listBuffer.push({ text: olMatch[1], ordered: true });
      continue;
    }

    // Empty line
    if (line.trim() === '') {
      flushList();
      if (i < lines.length - 1 && lines[i + 1]?.trim() !== '') {
        elements.push(<div key={key++} className="h-1" />);
      }
      continue;
    }

    // Regular paragraph
    flushList();
    elements.push(
      <p key={key++} className={clsx('text-sm leading-relaxed', isUser ? 'text-white' : 'text-slate-800')}>
        {renderInline(line, isUser)}
      </p>
    );
  }

  flushList();
  return <div className="space-y-0.5">{elements}</div>;
}

export function ChatPane() {
  const {
    chatHistory, addChatMessage, clearChatHistory,
    schema, setQueryResult, queryHistory, setQueryHistory,
    tableMetadata, chatPaneSize, setChatPaneSize,
    tableMappings, selectedTableMappings, setSelectedTableMappings,
  } = useAppStore();

  const [filterOpen, setFilterOpen] = useState(false);

  const [input, setInput] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [isAgentLoading, setIsAgentLoading] = useState(false);
  const [expandedSteps, setExpandedSteps] = useState<Record<number, boolean>>({});

  // CSV export state
  const [exportDialogOpen, setExportDialogOpen] = useState(false);
  const [exportSql, setExportSql] = useState('');
  const [exportPath, setExportPath] = useState('');
  const [isExporting, setIsExporting] = useState(false);
  const [exportResult, setExportResult] = useState<{ success: boolean; message: string } | null>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [chatHistory]);

  const handleSend = async (overrideInput?: string) => {
    const text = overrideInput ?? input;
    if (!text.trim()) return;

    setInput('');
    addChatMessage({ role: 'user', content: text });
    setIsLoading(true);

    try {
      const messagesToSend = [
        ...chatHistory,
        { role: 'user', content: text }
      ];

      const res = await fetch('/api/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          messages: messagesToSend,
          schema,
          tableMetadata,
          tableMappingFilter: selectedTableMappings,
        }),
      });

      const data = await res.json();

      if (data.error) {
        addChatMessage({ role: 'assistant', content: `Error: ${data.error}` });
      } else if (data.needs_clarification) {
        addChatMessage({
          role: 'assistant',
          content: data.question || 'Could you be more specific?',
          needs_clarification: true,
          question: data.question,
          options: data.options || [],
          clarification_type: data.type || 'field_selection',
        });
      } else {
        addChatMessage({
          role: 'assistant',
          content: data.explanation || 'Here is the query I generated:',
          sql: data.sql,
          visual: data.suggestedVisual
        });
      }
    } catch (error: any) {
      addChatMessage({ role: 'assistant', content: `Failed to connect to AI: ${error.message}` });
    } finally {
      setIsLoading(false);
    }
  };

  const handleAgentSend = async () => {
    const text = input.trim();
    if (!text) return;

    setInput('');
    addChatMessage({ role: 'user', content: text });
    setIsAgentLoading(true);

    try {
      const res = await fetch('/api/agent', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          question: text,
          schema,
          tableMetadata,
          tableMappingFilter: selectedTableMappings,
        }),
      });

      const data = await res.json();

      if (data.error) {
        addChatMessage({ role: 'assistant', content: `Agent error: ${data.error}` });
      } else {
        addChatMessage({
          role: 'assistant',
          content: data.final_answer || 'Analysis complete.',
          is_agent: true,
          agent_steps: data.steps || [],
        });
      }
    } catch (error: any) {
      addChatMessage({ role: 'assistant', content: `Failed to run agent: ${error.message}` });
    } finally {
      setIsAgentLoading(false);
    }
  };

  const handleExecuteQuery = async (sql: string, queryText: string) => {
    try {
      const res = await fetch('/api/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query: sql }),
      });
      const data = await res.json();
      if (data.error) {
        alert(`Query Error: ${data.error}`);
      } else {
        setQueryResult(data.data);
      }
    } catch (error: any) {
      alert(`Execution Error: ${error.message}`);
    }
  };

  const handleSaveToDashboard = async (sql: string, visual: string, name: string) => {
    try {
      await fetch('/api/saved_queries', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          user_id: 1,
          name: name || "Saved from Chat",
          sql,
          config: { dimensions: [], measures: [] },
          visual_type: visual || 'table'
        }),
      });
      alert("Saved to dashboard!");
    } catch (e) {
      console.error(e);
      alert("Failed to save");
    }
  };

  const openExportDialog = (sql: string, suggestedPath?: string) => {
    setExportSql(sql);
    const defaultName = suggestedPath || `export_${new Date().toISOString().slice(0, 10)}.csv`;
    setExportPath(defaultName);
    setExportResult(null);
    setExportDialogOpen(true);
  };

  const handleExportCsv = async () => {
    setIsExporting(true);
    setExportResult(null);
    try {
      const res = await fetch('/api/export_csv', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql: exportSql, output_path: exportPath }),
      });
      const data = await res.json();
      if (data.error) throw new Error(data.error);
      setExportResult({ success: true, message: `${data.row_count.toLocaleString()} lignes exportées → ${data.path}` });
    } catch (e: any) {
      setExportResult({ success: false, message: e.message });
    } finally {
      setIsExporting(false);
    }
  };

  const defaultSuggestions = [
    "Show me the list of all tables",
    "Show me the list of fields for the table [table_name]",
    "Search for the value '[value]' in the table [table_name]",
  ];

  const suggestions = queryHistory.length > 0
    ? Array.from(new Set(queryHistory.map((h: any) => h.query_text))).filter(q => q !== 'Built via Visual Builder').slice(0, 3)
    : defaultSuggestions;

  if (suggestions.length === 0) suggestions.push(...defaultSuggestions);

  const toggleSize = () => {
    if (chatPaneSize === 'normal') setChatPaneSize('expanded');
    else setChatPaneSize('normal');
  };

  // Only show mapped tables in the filter
  const mappedTables = tableMappings.filter(m => m.mapping_name);
  const toggleTableFilter = (tableName: string) => {
    if (selectedTableMappings.includes(tableName)) {
      setSelectedTableMappings(selectedTableMappings.filter(t => t !== tableName));
    } else {
      setSelectedTableMappings([...selectedTableMappings, tableName]);
    }
  };

  return (
    <div className="flex flex-col h-full bg-slate-50 border-r border-slate-200">
      {/* Header */}
      <div className="border-b border-slate-200 bg-white shrink-0">
        <div className="p-4 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="bg-emerald-100 p-2 rounded-full text-emerald-600">
              <Sparkles size={18} />
            </div>
            <div>
              <h2 className="text-base font-semibold text-slate-800">AI Data Analyst</h2>
              <p className="text-xs text-slate-500">Ask questions in plain English</p>
            </div>
          </div>
          <div className="flex items-center gap-1">
            {mappedTables.length > 0 && (
              <button
                onClick={() => setFilterOpen(o => !o)}
                className={clsx(
                  "p-1.5 rounded-lg transition-colors relative",
                  filterOpen || selectedTableMappings.length > 0
                    ? "text-emerald-600 bg-emerald-50 hover:bg-emerald-100"
                    : "text-slate-400 hover:text-slate-700 hover:bg-slate-100"
                )}
                title="Filter by table scope"
              >
                <Filter size={15} />
                {selectedTableMappings.length > 0 && (
                  <span className="absolute -top-1 -right-1 w-4 h-4 bg-emerald-500 text-white text-[10px] font-bold rounded-full flex items-center justify-center">
                    {selectedTableMappings.length}
                  </span>
                )}
              </button>
            )}
            {chatHistory.length > 0 && (
              <button
                onClick={clearChatHistory}
                className="p-1.5 text-slate-400 hover:text-red-500 hover:bg-red-50 rounded-lg transition-colors"
                title="Clear conversation"
              >
                <Trash2 size={15} />
              </button>
            )}
            <button
              onClick={toggleSize}
              className="p-1.5 text-slate-400 hover:text-slate-700 hover:bg-slate-100 rounded-lg transition-colors"
              title={chatPaneSize === 'expanded' ? 'Restore size' : 'Expand'}
            >
              {chatPaneSize === 'expanded' ? <Minimize2 size={15} /> : <Maximize2 size={15} />}
            </button>
            <button
              onClick={() => setChatPaneSize('minimized')}
              className="p-1.5 text-slate-400 hover:text-slate-700 hover:bg-slate-100 rounded-lg transition-colors"
              title="Minimize"
            >
              <Minus size={15} />
            </button>
          </div>
        </div>

        {/* Table scope filter panel */}
        <AnimatePresence>
          {filterOpen && mappedTables.length > 0 && (
            <motion.div
              initial={{ height: 0, opacity: 0 }}
              animate={{ height: 'auto', opacity: 1 }}
              exit={{ height: 0, opacity: 0 }}
              className="overflow-hidden border-t border-slate-100"
            >
              <div className="px-4 py-3">
                <div className="flex items-center justify-between mb-2">
                  <p className="text-xs font-semibold text-slate-500 uppercase tracking-wide flex items-center gap-1.5">
                    <Filter size={11} />
                    Restrict to tables
                  </p>
                  {selectedTableMappings.length > 0 && (
                    <button
                      onClick={() => setSelectedTableMappings([])}
                      className="text-xs text-slate-400 hover:text-slate-600 flex items-center gap-1"
                    >
                      <X size={11} /> Clear all
                    </button>
                  )}
                </div>
                <div className="flex flex-wrap gap-1.5">
                  {mappedTables.map(m => {
                    const active = selectedTableMappings.includes(m.table_name);
                    return (
                      <button
                        key={m.table_name}
                        onClick={() => toggleTableFilter(m.table_name)}
                        className={clsx(
                          "px-2.5 py-1 rounded-full text-xs font-medium transition-colors border",
                          active
                            ? "bg-emerald-500 text-white border-emerald-500"
                            : "bg-white text-slate-600 border-slate-200 hover:border-emerald-400 hover:text-emerald-600"
                        )}
                        title={`Technical: ${m.table_name}`}
                      >
                        {m.mapping_name}
                      </button>
                    );
                  })}
                </div>
                {selectedTableMappings.length > 0 && (
                  <p className="mt-2 text-xs text-emerald-600">
                    AI will only search in {selectedTableMappings.length} selected table{selectedTableMappings.length > 1 ? 's' : ''}.
                  </p>
                )}
              </div>
            </motion.div>
          )}
        </AnimatePresence>

        {/* Active filter chips (always visible when filter active but panel closed) */}
        {!filterOpen && selectedTableMappings.length > 0 && (
          <div className="px-4 pb-2 flex flex-wrap gap-1.5 border-t border-slate-100 pt-2">
            {selectedTableMappings.map(tName => {
              const m = tableMappings.find(x => x.table_name === tName);
              return (
                <span
                  key={tName}
                  className="flex items-center gap-1 px-2 py-0.5 bg-emerald-100 text-emerald-700 rounded-full text-xs font-medium"
                >
                  {m?.mapping_name ?? tName}
                  <button onClick={() => toggleTableFilter(tName)} className="hover:text-emerald-900">
                    <X size={10} />
                  </button>
                </span>
              );
            })}
          </div>
        )}
      </div>

      {/* Messages */}
      <div className="flex-1 overflow-y-auto p-4 space-y-4">
        {chatHistory.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-full text-center space-y-5">
            <div className="w-14 h-14 bg-emerald-100 rounded-full flex items-center justify-center text-emerald-500">
              <Bot size={28} />
            </div>
            <div>
              <h3 className="text-lg font-medium text-slate-800 mb-1">How can I help you analyze your data?</h3>
              <p className="text-slate-500 text-sm max-w-md mx-auto">
                I can write ClickHouse queries, build charts, and find insights automatically.
              </p>
            </div>
            <div className="flex flex-wrap gap-2 justify-center max-w-lg">
              {suggestions.map((s, i) => (
                <button
                  key={i}
                  onClick={() => setInput(s)}
                  className="bg-white border border-slate-200 px-3 py-1.5 rounded-full text-xs text-slate-600 hover:border-emerald-500 hover:text-emerald-600 transition-colors shadow-sm flex items-center gap-1.5"
                >
                  <History size={12} className="opacity-50" />
                  {s}
                </button>
              ))}
            </div>
          </div>
        ) : (
          chatHistory.map((msg, i) => (
            <motion.div
              initial={{ opacity: 0, y: 8 }}
              animate={{ opacity: 1, y: 0 }}
              key={i}
              className={clsx("flex gap-3", msg.role === 'user' ? "flex-row-reverse" : "")}
            >
              <div className={clsx(
                "w-7 h-7 rounded-full flex items-center justify-center shrink-0 text-xs font-bold",
                msg.role === 'user'
                  ? "bg-blue-500 text-white"
                  : msg.is_agent
                    ? "bg-indigo-500 text-white"
                    : "bg-emerald-500 text-white"
              )}>
                {msg.role === 'user' ? 'U' : msg.is_agent ? <Brain size={14} /> : <Bot size={14} />}
              </div>
              <div className={clsx(
                "max-w-[85%] rounded-2xl p-3 shadow-sm",
                msg.role === 'user'
                  ? "bg-blue-500 text-white rounded-tr-none"
                  : msg.is_agent
                    ? "bg-white border border-indigo-200 text-slate-800 rounded-tl-none"
                    : "bg-white border border-slate-200 text-slate-800 rounded-tl-none"
              )}>
                {msg.is_agent && (
                  <div className="flex items-center gap-1.5 mb-2">
                    <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full bg-indigo-100 text-indigo-700 text-[10px] font-semibold uppercase tracking-wide">
                      <Brain size={9} />
                      Analyse Agent
                    </span>
                    {msg.agent_steps && (
                      <span className="text-[10px] text-indigo-400">{msg.agent_steps.length} étape{msg.agent_steps.length > 1 ? 's' : ''}</span>
                    )}
                  </div>
                )}
                <MarkdownContent text={msg.content} isUser={msg.role === 'user'} />

                {/* Clarification options */}
                {msg.needs_clarification && msg.options && msg.options.length > 0 && (
                  <div className="mt-3 space-y-2">
                    <p className="text-xs font-semibold text-slate-500 uppercase tracking-wide">
                      {msg.clarification_type === 'table_selection' ? 'Select a table:' : 'Select a field:'}
                    </p>
                    <div className="flex flex-wrap gap-2">
                      {msg.options.map((opt, j) => (
                        <button
                          key={j}
                          onClick={() => handleSend(opt)}
                          className="flex items-center gap-1.5 px-3 py-1.5 bg-emerald-50 hover:bg-emerald-100 text-emerald-700 border border-emerald-200 rounded-lg text-xs font-medium transition-colors"
                        >
                          <CheckSquare size={12} />
                          {opt}
                        </button>
                      ))}
                    </div>
                  </div>
                )}

                {/* SQL block */}
                {msg.sql && (
                  <div className="mt-3 bg-slate-900 rounded-xl overflow-hidden border border-slate-800">
                    <div className="flex items-center justify-between px-3 py-2 bg-slate-800/50 border-b border-slate-800">
                      <span className="text-xs font-mono text-slate-400">Generated SQL</span>
                      <div className="flex gap-1.5">
                        <button
                          onClick={() => handleSaveToDashboard(msg.sql!, msg.visual || 'table', chatHistory[i - 1]?.content || 'Chat Query')}
                          className="flex items-center gap-1 text-xs bg-slate-700 hover:bg-slate-600 text-white px-2.5 py-1 rounded-md transition-colors"
                        >
                          <Save size={11} />
                          Save
                        </button>
                        <button
                          onClick={() => openExportDialog(msg.sql!)}
                          className="flex items-center gap-1 text-xs bg-amber-500 hover:bg-amber-400 text-white px-2.5 py-1 rounded-md transition-colors"
                          title="Exporter en CSV (séparateur pipe, max 1M lignes)"
                        >
                          <Download size={11} />
                          Export CSV
                        </button>
                        <button
                          onClick={() => handleExecuteQuery(msg.sql!, chatHistory[i - 1]?.content || 'Chat Query')}
                          className="flex items-center gap-1 text-xs bg-emerald-500 hover:bg-emerald-400 text-white px-2.5 py-1 rounded-md transition-colors"
                        >
                          <Play size={11} />
                          Run
                        </button>
                      </div>
                    </div>
                    <pre className="p-3 text-xs font-mono text-emerald-400 overflow-x-auto">
                      {msg.sql}
                    </pre>
                  </div>
                )}

                {/* Agent analysis steps */}
                {msg.is_agent && msg.agent_steps && msg.agent_steps.length > 0 && (
                  <div className="mt-3 border border-indigo-200 rounded-xl overflow-hidden bg-indigo-50/40">
                    <div className="px-3 py-2 bg-indigo-100/60 border-b border-indigo-200 flex items-center gap-2">
                      <Brain size={13} className="text-indigo-600" />
                      <span className="text-xs font-semibold text-indigo-700">
                        Agent — {msg.agent_steps.length} analyse{msg.agent_steps.length > 1 ? 's' : ''} effectuée{msg.agent_steps.length > 1 ? 's' : ''}
                      </span>
                    </div>
                    <div className="divide-y divide-indigo-100">
                      {msg.agent_steps.map((step, si) => {
                        const key = i * 100 + si;
                        const open = expandedSteps[key];
                        return (
                          <div key={si} className="text-xs">
                            <button
                              onClick={() => setExpandedSteps(prev => ({ ...prev, [key]: !prev[key] }))}
                              className="w-full flex items-center gap-2 px-3 py-2 text-left hover:bg-indigo-100/50 transition-colors"
                            >
                              {step.ok
                                ? <CheckCircle2 size={12} className="text-emerald-500 shrink-0" />
                                : <XCircle size={12} className="text-red-400 shrink-0" />
                              }
                              <span className="font-medium text-indigo-800">Étape {step.step}</span>
                              <span className="text-indigo-500 truncate flex-1">{step.reasoning}</span>
                              <span className="text-indigo-400 shrink-0 flex items-center gap-1">
                                {step.type === 'search_knowledge'
                                  ? <BookOpen size={10} className="text-violet-400" />
                                  : step.type === 'export_csv'
                                    ? <Download size={10} className="text-amber-400" />
                                    : <Database size={10} />
                                }
                                {step.type === 'search_knowledge' ? 'KB' : step.type === 'export_csv' ? 'Export' : `${step.row_count} ligne${step.row_count !== 1 ? 's' : ''}`}
                              </span>
                              {open
                                ? <ChevronDown size={12} className="text-indigo-400 shrink-0" />
                                : <ChevronRight size={12} className="text-indigo-400 shrink-0" />
                              }
                            </button>
                            {open && (
                              <div className="px-3 pb-3 space-y-2">
                                {step.type === 'search_knowledge' ? (
                                  <div className="bg-violet-50 border border-violet-200 rounded-lg overflow-hidden">
                                    <div className="px-2 py-1 bg-violet-100/60 border-b border-violet-200">
                                      <span className="text-[10px] font-mono text-violet-600">Recherche knowledge base</span>
                                    </div>
                                    <div className="p-2">
                                      <p className="text-[10px] font-semibold text-violet-600 mb-1">Requête</p>
                                      <p className="text-[10px] text-violet-700 font-mono italic">{step.search_query}</p>
                                    </div>
                                  </div>
                                ) : step.type === 'export_csv' ? (
                                  <div className="bg-amber-50 border border-amber-200 rounded-lg overflow-hidden">
                                    <div className="px-2 py-1 bg-amber-100/60 border-b border-amber-200">
                                      <span className="text-[10px] font-mono text-amber-700 flex items-center gap-1">
                                        <Download size={9} /> Export CSV demandé
                                      </span>
                                    </div>
                                    <div className="p-2 space-y-2">
                                      <div>
                                        <p className="text-[10px] font-semibold text-amber-700 mb-1">SQL d'export</p>
                                        <pre className="text-[10px] font-mono text-slate-700 whitespace-pre-wrap bg-white border border-amber-100 rounded p-1.5 overflow-x-auto">
                                          {step.sql}
                                        </pre>
                                      </div>
                                      <div className="flex items-center gap-1.5">
                                        <FolderOpen size={10} className="text-amber-600 shrink-0" />
                                        <span className="text-[10px] text-amber-700 font-mono">{step.suggested_path}</span>
                                      </div>
                                      <button
                                        onClick={() => openExportDialog(step.sql!, step.suggested_path)}
                                        className="w-full flex items-center justify-center gap-1.5 bg-amber-500 hover:bg-amber-600 text-white text-[11px] font-medium py-1.5 rounded-lg transition-colors"
                                      >
                                        <Download size={10} />
                                        Confirmer et exporter
                                      </button>
                                    </div>
                                  </div>
                                ) : (
                                  <div className="bg-slate-900 rounded-lg overflow-hidden">
                                    <div className="px-2 py-1 bg-slate-800/50 border-b border-slate-700">
                                      <span className="text-[10px] font-mono text-slate-400">SQL exécuté</span>
                                    </div>
                                    <pre className="p-2 text-[10px] font-mono text-emerald-400 overflow-x-auto whitespace-pre-wrap">
                                      {step.sql}
                                    </pre>
                                  </div>
                                )}
                                {step.type !== 'export_csv' && (
                                  <div className="bg-white border border-indigo-100 rounded-lg p-2">
                                    <p className="text-[10px] font-semibold text-slate-500 mb-1">Résultat</p>
                                    <pre className="text-[10px] text-slate-600 whitespace-pre-wrap">{step.result_summary}</pre>
                                  </div>
                                )}
                              </div>
                            )}
                          </div>
                        );
                      })}
                    </div>
                  </div>
                )}
              </div>
            </motion.div>
          ))
        )}
        {isLoading && (
          <div className="flex gap-3">
            <div className="w-7 h-7 rounded-full bg-emerald-500 text-white flex items-center justify-center shrink-0">
              <Bot size={14} />
            </div>
            <div className="bg-white border border-slate-200 rounded-2xl rounded-tl-none p-3 shadow-sm flex items-center gap-2">
              <Loader2 className="animate-spin text-emerald-500" size={14} />
              <span className="text-xs text-slate-500">Analyzing and generating SQL...</span>
            </div>
          </div>
        )}
        {isAgentLoading && (
          <div className="flex gap-3">
            <div className="w-7 h-7 rounded-full bg-indigo-500 text-white flex items-center justify-center shrink-0">
              <Brain size={14} />
            </div>
            <div className="bg-indigo-50 border border-indigo-200 rounded-2xl rounded-tl-none p-3 shadow-sm">
              <div className="flex items-center gap-2 mb-1">
                <Loader2 className="animate-spin text-indigo-500" size={14} />
                <span className="text-xs font-medium text-indigo-700">Agent en cours d'analyse…</span>
              </div>
              <p className="text-[11px] text-indigo-500">
                L'agent mène des requêtes itératives sur vos données ClickHouse (jusqu'à 10 étapes).
              </p>
            </div>
          </div>
        )}
        <div ref={messagesEndRef} />
      </div>

      {/* Input */}
      <div className="p-3 bg-white border-t border-slate-200 shrink-0 space-y-2">
        <div className="relative flex items-center">
          <input
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && !e.shiftKey && handleSend()}
            placeholder="Ask a question about your data..."
            className="w-full bg-slate-50 border border-slate-200 rounded-full pl-5 pr-12 py-3 text-sm focus:outline-none focus:ring-2 focus:ring-emerald-500/20 focus:border-emerald-500 transition-all shadow-sm"
          />
          <button
            onClick={() => handleSend()}
            disabled={!input.trim() || isLoading || isAgentLoading}
            className="absolute right-2 p-2 bg-emerald-500 text-white rounded-full hover:bg-emerald-600 disabled:opacity-50 disabled:cursor-not-allowed transition-colors shadow-sm"
            title="Envoyer (chat rapide)"
          >
            <Send size={16} />
          </button>
        </div>
        <button
          onClick={handleAgentSend}
          disabled={!input.trim() || isLoading || isAgentLoading}
          className={clsx(
            "w-full flex items-center justify-center gap-2 py-2 px-4 rounded-full text-sm font-medium transition-all shadow-sm border",
            "bg-indigo-600 hover:bg-indigo-700 text-white border-indigo-600",
            "disabled:opacity-40 disabled:cursor-not-allowed"
          )}
          title="Analyse approfondie multi-étapes par l'agent IA (jusqu'à 10 requêtes)"
        >
          {isAgentLoading
            ? <Loader2 size={15} className="animate-spin" />
            : <Brain size={15} />
          }
          {isAgentLoading ? 'Agent en cours…' : 'Analyser avec l\'Agent'}
        </button>
      </div>

      {/* ------------------------------------------------------------------ */}
      {/* CSV Export Dialog                                                    */}
      {/* ------------------------------------------------------------------ */}
      {exportDialogOpen && (
        <div className="fixed inset-0 z-[350] flex items-center justify-center bg-black/50 backdrop-blur-sm p-4">
          <div className="bg-white rounded-2xl shadow-2xl w-full max-w-md overflow-hidden">
            <div className="p-4 border-b border-slate-200 flex items-center justify-between">
              <div className="flex items-center gap-2">
                <Download size={18} className="text-amber-600" />
                <h3 className="text-sm font-bold text-slate-800">Exporter en CSV</h3>
              </div>
              <button onClick={() => setExportDialogOpen(false)} className="text-slate-400 hover:text-slate-600 p-1.5 hover:bg-slate-100 rounded-md">
                <X size={16} />
              </button>
            </div>
            <div className="p-5 space-y-4">
              <div className="bg-amber-50 border border-amber-200 rounded-xl p-3 text-xs text-amber-700 space-y-1">
                <p className="font-semibold">Format : CSV avec séparateur pipe ( | )</p>
                <p>Limite : 1 000 000 lignes maximum</p>
              </div>
              <div>
                <label className="block text-xs font-semibold text-slate-600 uppercase tracking-wide mb-1">
                  <FolderOpen size={12} className="inline mr-1" />
                  Répertoire / fichier de destination
                </label>
                <input
                  type="text"
                  value={exportPath}
                  onChange={e => { setExportPath(e.target.value); setExportResult(null); }}
                  placeholder="/home/user/mes_données/export.csv"
                  className="w-full bg-slate-50 border border-slate-200 rounded-lg px-3 py-2 text-sm font-mono focus:ring-2 focus:ring-amber-500/20 focus:border-amber-500 transition-all"
                />
                <p className="mt-1 text-xs text-slate-400">Chemin absolu sur le serveur (ex : /home/user/export.csv)</p>
              </div>
              {exportResult && (
                <div className={clsx(
                  "flex items-start gap-2 p-3 rounded-lg text-xs",
                  exportResult.success ? "bg-emerald-50 text-emerald-700 border border-emerald-200" : "bg-red-50 text-red-700 border border-red-200"
                )}>
                  <CheckCircle2 size={14} className="shrink-0 mt-0.5" />
                  <span>{exportResult.message}</span>
                </div>
              )}
              <div className="flex justify-end gap-2 pt-1">
                <button onClick={() => setExportDialogOpen(false)} className="px-4 py-2 text-sm text-slate-600 hover:bg-slate-100 rounded-lg transition-colors">
                  {exportResult?.success ? 'Fermer' : 'Annuler'}
                </button>
                {!exportResult?.success && (
                  <button
                    onClick={handleExportCsv}
                    disabled={isExporting || !exportPath.trim()}
                    className="flex items-center gap-2 bg-amber-500 hover:bg-amber-600 disabled:opacity-50 text-white px-4 py-2 rounded-lg text-sm font-medium transition-colors"
                  >
                    {isExporting ? <><Loader2 size={14} className="animate-spin" />Export en cours…</> : <><Download size={14} />Exporter</>}
                  </button>
                )}
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
