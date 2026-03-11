import { useState, useEffect, useRef } from 'react';
import { MessageSquare, LayoutDashboard, Settings, BookOpen, Database, Grid, Layers, ShieldCheck, Bot, BarChart2, Wrench, Cpu, Terminal, X, ChevronDown } from 'lucide-react';
import { useAppStore } from '../store';
import clsx from 'clsx';

const navGroups = [
  {
    label: 'Agent assistant',
    icon: Bot,
    items: [
      { id: 'chat', icon: MessageSquare, label: 'AI Assistant' },
      { id: 'agents', icon: Cpu, label: 'Agents' },
      { id: 'rag', icon: Layers, label: 'RAG' },
    ],
  },
  {
    label: 'Big data tools',
    icon: BarChart2,
    items: [
      { id: 'builder', icon: LayoutDashboard, label: 'Visual Builder' },
      { id: 'dashboard', icon: Grid, label: 'My Dashboard' },
    ],
  },
  {
    label: 'Configuration',
    icon: Wrench,
    items: [
      { id: 'knowledge', icon: BookOpen, label: 'Knowledge Base' },
      { id: 'settings', icon: Settings, label: 'Settings' },
    ],
  },
] as const;

interface LogEntry {
  ts: string;
  level: string;
  source: string;
  msg: string;
}

function ConsolePanel({ onClose }: { onClose: () => void }) {
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [nextIdx, setNextIdx] = useState(-1);
  const [autoScroll, setAutoScroll] = useState(true);
  const bottomRef = useRef<HTMLDivElement>(null);
  const scrollRef = useRef<HTMLDivElement>(null);

  const fetchLogs = async (since: number) => {
    try {
      const res = await fetch(`/api/console-logs?since=${since}`);
      const data = await res.json();
      if (data.logs && data.logs.length > 0) {
        setLogs(prev => [...prev, ...data.logs]);
        setNextIdx(data.next_idx ?? since + data.logs.length);
      }
    } catch {
      // Server not available
    }
  };

  useEffect(() => {
    fetchLogs(-1);
  }, []);

  useEffect(() => {
    const interval = setInterval(() => {
      setNextIdx(prev => {
        fetchLogs(prev);
        return prev;
      });
    }, 1500);
    return () => clearInterval(interval);
  }, []);

  useEffect(() => {
    if (autoScroll && bottomRef.current) {
      bottomRef.current.scrollIntoView({ behavior: 'smooth' });
    }
  }, [logs, autoScroll]);

  const handleScroll = () => {
    const el = scrollRef.current;
    if (!el) return;
    const atBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 40;
    setAutoScroll(atBottom);
  };

  const levelColor = (level: string) => {
    switch (level.toLowerCase()) {
      case 'error': return 'text-red-400';
      case 'warn': return 'text-amber-400';
      case 'info': return 'text-emerald-400';
      default: return 'text-slate-400';
    }
  };

  return (
    <div className="fixed bottom-0 left-64 z-[500] w-[600px] max-w-[calc(100vw-16rem)] bg-slate-900 border border-slate-700 rounded-t-xl shadow-2xl flex flex-col" style={{ height: '320px' }}>
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-2 border-b border-slate-700 bg-slate-800 rounded-t-xl shrink-0">
        <div className="flex items-center gap-2">
          <Terminal size={14} className="text-emerald-400" />
          <span className="text-sm font-semibold text-slate-200">Console</span>
          <span className="text-xs text-slate-500 bg-slate-700 px-1.5 py-0.5 rounded">{logs.length} entries</span>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={() => setLogs([])}
            className="text-xs text-slate-500 hover:text-slate-300 transition-colors"
            title="Clear console"
          >
            Clear
          </button>
          <button
            onClick={onClose}
            className="text-slate-500 hover:text-slate-300 transition-colors p-0.5 rounded"
            title="Close console (logs continue in background)"
          >
            <ChevronDown size={16} />
          </button>
        </div>
      </div>
      {/* Log output */}
      <div
        ref={scrollRef}
        onScroll={handleScroll}
        className="flex-1 overflow-y-auto p-2 space-y-0.5 font-mono text-xs"
      >
        {logs.length === 0 ? (
          <div className="text-slate-600 italic p-2">No logs yet — run an agent or query to see activity here.</div>
        ) : (
          logs.map((log, i) => (
            <div key={i} className="flex items-start gap-2 hover:bg-slate-800/60 rounded px-1 py-0.5">
              <span className="text-slate-600 shrink-0 w-16">{log.ts}</span>
              <span className={clsx('shrink-0 w-14 font-bold uppercase text-[10px]', levelColor(log.level))}>{log.level}</span>
              <span className="text-violet-400 shrink-0 w-20 truncate">[{log.source}]</span>
              <span className="text-slate-300 break-all flex-1">{log.msg}</span>
            </div>
          ))
        )}
        <div ref={bottomRef} />
      </div>
      {!autoScroll && (
        <div className="absolute bottom-2 right-2">
          <button
            onClick={() => { setAutoScroll(true); bottomRef.current?.scrollIntoView({ behavior: 'smooth' }); }}
            className="text-xs bg-emerald-600 hover:bg-emerald-700 text-white px-2 py-1 rounded shadow"
          >
            ↓ scroll to bottom
          </button>
        </div>
      )}
    </div>
  );
}

export function Sidebar() {
  const { activeTab, setActiveTab, consoleOpen, setConsoleOpen } = useAppStore();

  return (
    <>
      <div className="w-64 bg-slate-900 text-slate-300 flex flex-col h-screen border-r border-slate-800">
        <div className="p-4 flex items-center gap-3 border-b border-slate-800">
          <div className="bg-emerald-500 p-2 rounded-lg text-white">
            <Database size={20} />
          </div>
          <h1 className="font-bold text-white text-lg tracking-tight">ClickSense AI</h1>
        </div>

        <nav className="flex-1 p-4 space-y-5 overflow-y-auto">
          {navGroups.map((group) => (
            <div key={group.label}>
              <div className="flex items-center gap-2 px-2 mb-1.5">
                <group.icon size={12} className="text-slate-500" />
                <span className="text-[10px] font-semibold uppercase tracking-widest text-slate-500">
                  {group.label}
                </span>
              </div>
              <div className="space-y-0.5">
                {group.items.map((item) => (
                  <button
                    key={item.id}
                    onClick={() => setActiveTab(item.id)}
                    className={clsx(
                      "w-full flex items-center gap-3 px-4 py-2.5 rounded-xl transition-all duration-200 text-sm font-medium",
                      activeTab === item.id
                        ? "bg-emerald-500/10 text-emerald-400"
                        : "hover:bg-slate-800 hover:text-white"
                    )}
                  >
                    <item.icon size={17} />
                    {item.label}
                  </button>
                ))}
              </div>
            </div>
          ))}
        </nav>

        {/* Console button at bottom */}
        <div className="p-3 border-t border-slate-800">
          <button
            onClick={() => setConsoleOpen(!consoleOpen)}
            className={clsx(
              "w-full flex items-center gap-3 px-4 py-2.5 rounded-xl transition-all duration-200 text-sm font-medium",
              consoleOpen
                ? "bg-emerald-500/10 text-emerald-400"
                : "text-slate-400 hover:bg-slate-800 hover:text-white"
            )}
            title="Open/close real-time log console"
          >
            <Terminal size={17} />
            Console
          </button>
        </div>
      </div>

      {/* Console panel popup (portal-like, rendered outside sidebar flow) */}
      {consoleOpen && <ConsolePanel onClose={() => setConsoleOpen(false)} />}
    </>
  );
}
