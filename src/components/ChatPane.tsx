import { useState, useRef, useEffect } from 'react';
import { Send, Bot, User, Loader2, Sparkles, Play, Save, History, Trash2, Tags, X, ChevronDown } from 'lucide-react';
import { useAppStore } from '../store';
import clsx from 'clsx';
import { motion } from 'motion/react';

function MappingSelector() {
  const { tableMappings, setTableMappings, selectedMappings, setSelectedMappings } = useAppStore();
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  // Load mappings on mount
  useEffect(() => {
    fetch('/api/table_mappings')
      .then(res => res.json())
      .then(data => setTableMappings(data));
  }, []);

  // Close dropdown when clicking outside
  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    }
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, []);

  const mappedTables = Object.entries(tableMappings).filter(([, name]) => name.trim());
  if (mappedTables.length === 0) return null;

  const toggle = (techName: string) => {
    setSelectedMappings(
      selectedMappings.includes(techName)
        ? selectedMappings.filter(t => t !== techName)
        : [...selectedMappings, techName]
    );
  };

  const clearAll = () => setSelectedMappings([]);

  return (
    <div className="px-4 pb-2" ref={ref}>
      <div className="relative">
        <div className="flex items-center gap-2 flex-wrap">
          {selectedMappings.length > 0 && (
            <div className="flex items-center gap-1.5 flex-wrap">
              {selectedMappings.map(tech => (
                <span
                  key={tech}
                  className="flex items-center gap-1 bg-emerald-100 text-emerald-800 text-xs font-medium px-2.5 py-1 rounded-full"
                >
                  {tableMappings[tech] || tech}
                  <button
                    onClick={() => toggle(tech)}
                    className="ml-0.5 text-emerald-600 hover:text-emerald-900"
                  >
                    <X size={11} />
                  </button>
                </span>
              ))}
              <button
                onClick={clearAll}
                className="text-xs text-slate-400 hover:text-red-500 px-1 transition-colors"
              >
                Clear all
              </button>
            </div>
          )}
          <button
            onClick={() => setOpen(o => !o)}
            className={clsx(
              'flex items-center gap-1.5 text-xs font-medium px-3 py-1.5 rounded-full border transition-all',
              selectedMappings.length > 0
                ? 'border-emerald-300 text-emerald-700 bg-emerald-50 hover:bg-emerald-100'
                : 'border-slate-200 text-slate-500 bg-slate-50 hover:bg-slate-100'
            )}
          >
            <Tags size={13} />
            {selectedMappings.length > 0 ? `${selectedMappings.length} scope(s) actif${selectedMappings.length > 1 ? 's' : ''}` : 'Filtrer par table'}
            <ChevronDown size={12} className={clsx('transition-transform', open && 'rotate-180')} />
          </button>
        </div>

        {open && (
          <div className="absolute bottom-full mb-2 left-0 z-20 bg-white border border-slate-200 rounded-xl shadow-lg w-72 max-h-60 overflow-y-auto">
            <div className="p-2 border-b border-slate-100 flex items-center justify-between">
              <span className="text-xs font-semibold text-slate-600 px-1">Restreindre l'IA à ces tables</span>
              {selectedMappings.length > 0 && (
                <button onClick={clearAll} className="text-xs text-slate-400 hover:text-red-500 px-2">
                  Tout désélectionner
                </button>
              )}
            </div>
            {mappedTables.map(([tech, friendly]) => {
              const checked = selectedMappings.includes(tech);
              return (
                <label
                  key={tech}
                  className="flex items-center gap-3 px-3 py-2.5 hover:bg-slate-50 cursor-pointer transition-colors"
                >
                  <input
                    type="checkbox"
                    checked={checked}
                    onChange={() => toggle(tech)}
                    className="rounded border-slate-300 text-emerald-500 focus:ring-emerald-500"
                  />
                  <div className="min-w-0">
                    <div className="text-sm font-medium text-slate-800 truncate">{friendly}</div>
                    <div className="text-[11px] text-slate-400 font-mono truncate">{tech}</div>
                  </div>
                </label>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}

export function ChatPane() {
  const { chatHistory, addChatMessage, clearChatHistory, schema, setQueryResult, currentUser, queryHistory, setQueryHistory, tableMetadata, tableMappings, selectedMappings } = useAppStore();
  const [input, setInput] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (currentUser) {
      fetch(`/api/history/${currentUser.id}`)
        .then(res => res.json())
        .then(data => setQueryHistory(data));
    }
  }, [currentUser]);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [chatHistory]);

  const handleSend = async () => {
    if (!input.trim()) return;

    const userMsg = input;
    setInput('');
    addChatMessage({ role: 'user', content: userMsg });
    setIsLoading(true);

    try {
      const messagesToSend = [
        ...chatHistory,
        { role: 'user', content: userMsg }
      ];

      const res = await fetch('/api/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          messages: messagesToSend,
          schema,
          tableMetadata,
          tableMappings,
          selectedMappings,
        }),
      });

      const data = await res.json();

      if (data.error) {
        addChatMessage({ role: 'assistant', content: `Error: ${data.error}` });
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

        // Save to history
        if (currentUser) {
          fetch('/api/history', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ user_id: currentUser.id, query_text: queryText, sql }),
          }).then(() => {
            fetch(`/api/history/${currentUser.id}`)
              .then(r => r.json())
              .then(d => setQueryHistory(d));
          });
        }
      }
    } catch (error: any) {
      alert(`Execution Error: ${error.message}`);
    }
  };

  const handleSaveToDashboard = async (sql: string, visual: string, name: string) => {
    if (!currentUser) return alert("Please select a user first");

    try {
      await fetch('/api/saved_queries', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          user_id: currentUser.id,
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
    ? Array.from(new Set(queryHistory.map(h => h.query_text))).filter(q => q !== 'Built via Visual Builder').slice(0, 3)
    : defaultSuggestions;

  if (suggestions.length === 0) suggestions.push(...defaultSuggestions);

  return (
    <div className="flex flex-col h-full bg-slate-50 border-r border-slate-200">
      <div className="p-6 border-b border-slate-200 bg-white flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="bg-emerald-100 p-2 rounded-full text-emerald-600">
            <Sparkles size={20} />
          </div>
          <div>
            <h2 className="text-lg font-semibold text-slate-800">AI Data Analyst</h2>
            <p className="text-sm text-slate-500">Ask questions in plain English</p>
          </div>
        </div>
        {chatHistory.length > 0 && (
          <button
            onClick={clearChatHistory}
            className="flex items-center gap-2 px-3 py-1.5 text-sm font-medium text-slate-600 hover:text-red-600 hover:bg-red-50 rounded-lg transition-colors border border-transparent hover:border-red-100"
            title="Clear conversation"
          >
            <Trash2 size={16} />
            Clear Chat
          </button>
        )}
      </div>

      <div className="flex-1 overflow-y-auto p-6 space-y-6">
        {chatHistory.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-full text-center space-y-6">
            <div className="w-16 h-16 bg-emerald-100 rounded-full flex items-center justify-center text-emerald-500">
              <Bot size={32} />
            </div>
            <div>
              <h3 className="text-xl font-medium text-slate-800 mb-2">How can I help you analyze your data?</h3>
              <p className="text-slate-500 max-w-md mx-auto">
                I can write ClickHouse queries, build charts, and find insights automatically.
              </p>
            </div>
            <div className="flex flex-wrap gap-2 justify-center max-w-lg">
              {suggestions.map((s, i) => (
                <button
                  key={i}
                  onClick={() => setInput(s)}
                  className="bg-white border border-slate-200 px-4 py-2 rounded-full text-sm text-slate-600 hover:border-emerald-500 hover:text-emerald-600 transition-colors shadow-sm flex items-center gap-2"
                >
                  <History size={14} className="opacity-50" />
                  {s}
                </button>
              ))}
            </div>
          </div>
        ) : (
          chatHistory.map((msg, i) => (
            <motion.div
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              key={i}
              className={clsx("flex gap-4", msg.role === 'user' ? "flex-row-reverse" : "")}
            >
              <div className={clsx(
                "w-8 h-8 rounded-full flex items-center justify-center shrink-0",
                msg.role === 'user' ? "bg-blue-500 text-white" : "bg-emerald-500 text-white"
              )}>
                {msg.role === 'user' ? <User size={16} /> : <Bot size={16} />}
              </div>
              <div className={clsx(
                "max-w-[80%] rounded-2xl p-4 shadow-sm",
                msg.role === 'user' ? "bg-blue-500 text-white rounded-tr-none" : "bg-white border border-slate-200 text-slate-800 rounded-tl-none"
              )}>
                <p className="text-sm leading-relaxed whitespace-pre-wrap">{msg.content}</p>
                {msg.sql && (
                  <div className="mt-4 bg-slate-900 rounded-xl overflow-hidden border border-slate-800">
                    <div className="flex items-center justify-between px-4 py-2 bg-slate-800/50 border-b border-slate-800">
                      <span className="text-xs font-mono text-slate-400">Generated SQL</span>
                      <div className="flex gap-2">
                        <button
                          onClick={() => handleSaveToDashboard(msg.sql!, msg.visual || 'table', chatHistory[i-1]?.content)}
                          className="flex items-center gap-1 text-xs bg-slate-700 hover:bg-slate-600 text-white px-3 py-1.5 rounded-md transition-colors font-medium"
                        >
                          <Save size={12} />
                          Save
                        </button>
                        <button
                          onClick={() => handleExecuteQuery(msg.sql!, chatHistory[i-1]?.content || 'Chat Query')}
                          className="flex items-center gap-1 text-xs bg-emerald-500 hover:bg-emerald-400 text-white px-3 py-1.5 rounded-md transition-colors font-medium"
                        >
                          <Play size={12} />
                          Run Query
                        </button>
                      </div>
                    </div>
                    <pre className="p-4 text-xs font-mono text-emerald-400 overflow-x-auto">
                      {msg.sql}
                    </pre>
                  </div>
                )}
              </div>
            </motion.div>
          ))
        )}
        {isLoading && (
          <div className="flex gap-4">
            <div className="w-8 h-8 rounded-full bg-emerald-500 text-white flex items-center justify-center shrink-0">
              <Bot size={16} />
            </div>
            <div className="bg-white border border-slate-200 rounded-2xl rounded-tl-none p-4 shadow-sm flex items-center gap-2">
              <Loader2 className="animate-spin text-emerald-500" size={16} />
              <span className="text-sm text-slate-500">Analyzing schema and generating SQL...</span>
            </div>
          </div>
        )}
        <div ref={messagesEndRef} />
      </div>

      <div className="bg-white border-t border-slate-200">
        <MappingSelector />
        <div className="p-4 pt-2">
          <div className="relative flex items-center">
            <input
              type="text"
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && handleSend()}
              placeholder="Ask a question about your data..."
              className="w-full bg-slate-50 border border-slate-200 rounded-full pl-6 pr-14 py-4 text-sm focus:outline-none focus:ring-2 focus:ring-emerald-500/20 focus:border-emerald-500 transition-all shadow-sm"
            />
            <button
              onClick={handleSend}
              disabled={!input.trim() || isLoading}
              className="absolute right-2 p-2.5 bg-emerald-500 text-white rounded-full hover:bg-emerald-600 disabled:opacity-50 disabled:cursor-not-allowed transition-colors shadow-sm"
            >
              <Send size={18} />
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
