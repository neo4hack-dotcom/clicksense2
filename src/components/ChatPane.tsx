import { useState, useRef, useEffect } from 'react';
import {
  Send, Bot, Loader2, Sparkles, Play, Save, History, Trash2,
  Maximize2, Minimize2, Minus, CheckSquare, Filter, X
} from 'lucide-react';
import { useAppStore } from '../store';
import clsx from 'clsx';
import { motion, AnimatePresence } from 'motion/react';

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

  const defaultSuggestions = [
    "Show me the list of all tables",
    "Show me the list of fields for the table [table_name]",
    "Search for the value '[value]' in the table [table_name]"
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
                msg.role === 'user' ? "bg-blue-500 text-white" : "bg-emerald-500 text-white"
              )}>
                {msg.role === 'user' ? 'U' : <Bot size={14} />}
              </div>
              <div className={clsx(
                "max-w-[85%] rounded-2xl p-3 shadow-sm",
                msg.role === 'user'
                  ? "bg-blue-500 text-white rounded-tr-none"
                  : "bg-white border border-slate-200 text-slate-800 rounded-tl-none"
              )}>
                <p className="text-sm leading-relaxed whitespace-pre-wrap">{msg.content}</p>

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
        <div ref={messagesEndRef} />
      </div>

      {/* Input */}
      <div className="p-3 bg-white border-t border-slate-200 shrink-0">
        <div className="relative flex items-center">
          <input
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && handleSend()}
            placeholder="Ask a question about your data..."
            className="w-full bg-slate-50 border border-slate-200 rounded-full pl-5 pr-12 py-3 text-sm focus:outline-none focus:ring-2 focus:ring-emerald-500/20 focus:border-emerald-500 transition-all shadow-sm"
          />
          <button
            onClick={() => handleSend()}
            disabled={!input.trim() || isLoading}
            className="absolute right-2 p-2 bg-emerald-500 text-white rounded-full hover:bg-emerald-600 disabled:opacity-50 disabled:cursor-not-allowed transition-colors shadow-sm"
          >
            <Send size={16} />
          </button>
        </div>
      </div>
    </div>
  );
}
