import { useState, useRef, useEffect, KeyboardEvent } from 'react';
import {
  Cpu, Send, ChevronDown, ChevronRight, CheckCircle2, XCircle,
  Loader2, Database, FileText, Settings2, Table2, Columns3,
  MessageSquare, Play, RefreshCw,
} from 'lucide-react';
import clsx from 'clsx';

// ── Types ──────────────────────────────────────────────────────────────────

interface AgentParam {
  name: string;
  label: string;
  type: 'string' | 'number' | 'select';
  default: string | number;
  description: string;
  options?: string[];
}

interface Agent {
  id: string;
  name: string;
  description: string;
  parameters: AgentParam[];
}

interface DictColumn {
  name: string;
  type: string;
  business_description: string;
  format?: string;
  possible_values?: string;
}

interface DictEntry {
  table: string;
  table_description: string;
  columns: DictColumn[];
}

interface StepInfo {
  table: string;
  ok: boolean;
  columns_count?: number;
  error?: string;
}

interface ChatMessage {
  role: 'user' | 'assistant';
  content: string;
  steps?: StepInfo[];
  data_dictionary?: DictEntry[];
  tables_processed?: number;
  total_tables?: number;
  error?: string;
}

// ── Sub-components ─────────────────────────────────────────────────────────

function AgentCard({ agent, selected, onClick }: { agent: Agent; selected: boolean; onClick: () => void }) {
  return (
    <button
      onClick={onClick}
      className={clsx(
        'w-full text-left p-4 rounded-xl border transition-all duration-200 group',
        selected
          ? 'border-emerald-500 bg-emerald-500/10'
          : 'border-slate-200 bg-white hover:border-emerald-300 hover:shadow-sm',
      )}
    >
      <div className="flex items-start gap-3">
        <div className={clsx(
          'p-2 rounded-lg flex-shrink-0 transition-colors',
          selected ? 'bg-emerald-500 text-white' : 'bg-slate-100 text-slate-500 group-hover:bg-emerald-100 group-hover:text-emerald-600',
        )}>
          <Cpu size={16} />
        </div>
        <div className="min-w-0">
          <p className={clsx('text-sm font-semibold truncate', selected ? 'text-emerald-700' : 'text-slate-800')}>
            {agent.name}
          </p>
          <p className="text-xs text-slate-500 mt-0.5 line-clamp-2 leading-relaxed">
            {agent.description}
          </p>
          <p className="text-[10px] text-slate-400 mt-1.5 font-medium">
            {agent.parameters.length} paramètre{agent.parameters.length !== 1 ? 's' : ''}
          </p>
        </div>
      </div>
    </button>
  );
}

function ParamsForm({
  agent,
  params,
  onChange,
}: {
  agent: Agent;
  params: Record<string, string | number>;
  onChange: (name: string, value: string | number) => void;
}) {
  if (agent.parameters.length === 0) return null;

  return (
    <div className="grid grid-cols-1 gap-3">
      {agent.parameters.map((p) => (
        <div key={p.name}>
          <label className="block text-xs font-semibold text-slate-600 mb-1">
            {p.label}
          </label>
          {p.type === 'select' ? (
            <select
              value={params[p.name] as string}
              onChange={(e) => onChange(p.name, e.target.value)}
              className="w-full px-3 py-2 text-sm border border-slate-200 rounded-lg bg-white focus:outline-none focus:ring-2 focus:ring-emerald-400 focus:border-transparent"
            >
              {p.options?.map((opt) => (
                <option key={opt} value={opt}>{opt}</option>
              ))}
            </select>
          ) : (
            <input
              type={p.type === 'number' ? 'number' : 'text'}
              value={params[p.name] as string}
              onChange={(e) => onChange(p.name, p.type === 'number' ? Number(e.target.value) : e.target.value)}
              placeholder={p.description}
              className="w-full px-3 py-2 text-sm border border-slate-200 rounded-lg bg-white focus:outline-none focus:ring-2 focus:ring-emerald-400 focus:border-transparent"
            />
          )}
          <p className="text-[10px] text-slate-400 mt-0.5">{p.description}</p>
        </div>
      ))}
    </div>
  );
}

function StepsPanel({ steps }: { steps: StepInfo[] }) {
  const [open, setOpen] = useState(false);
  return (
    <div className="mt-2 border border-slate-200 rounded-lg overflow-hidden text-xs">
      <button
        onClick={() => setOpen(o => !o)}
        className="w-full flex items-center gap-2 px-3 py-2 bg-slate-50 hover:bg-slate-100 transition-colors text-slate-600 font-medium"
      >
        {open ? <ChevronDown size={13} /> : <ChevronRight size={13} />}
        <Table2 size={13} />
        {steps.length} table{steps.length !== 1 ? 's' : ''} analysée{steps.length !== 1 ? 's' : ''}
        <span className="ml-auto flex gap-1">
          <span className="text-emerald-600">{steps.filter(s => s.ok).length} ✓</span>
          {steps.filter(s => !s.ok).length > 0 && (
            <span className="text-red-500">{steps.filter(s => !s.ok).length} ✗</span>
          )}
        </span>
      </button>
      {open && (
        <div className="divide-y divide-slate-100 max-h-40 overflow-y-auto">
          {steps.map((s, i) => (
            <div key={i} className="flex items-center gap-2 px-3 py-1.5">
              {s.ok
                ? <CheckCircle2 size={12} className="text-emerald-500 flex-shrink-0" />
                : <XCircle size={12} className="text-red-400 flex-shrink-0" />
              }
              <span className="font-mono text-slate-700 truncate">{s.table}</span>
              {s.ok && s.columns_count !== undefined && (
                <span className="ml-auto text-slate-400 flex-shrink-0">{s.columns_count} col.</span>
              )}
              {!s.ok && s.error && (
                <span className="ml-auto text-red-400 truncate max-w-[120px]">{s.error}</span>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function DataDictionaryView({ entries }: { entries: DictEntry[] }) {
  const [expanded, setExpanded] = useState<Record<number, boolean>>({});

  function toggle(i: number) {
    setExpanded(prev => ({ ...prev, [i]: !prev[i] }));
  }

  return (
    <div className="mt-3 space-y-2">
      {entries.map((entry, i) => (
        <div key={i} className="border border-slate-200 rounded-xl overflow-hidden">
          <button
            onClick={() => toggle(i)}
            className="w-full flex items-start gap-3 p-3 bg-white hover:bg-slate-50 transition-colors text-left"
          >
            <div className="p-1.5 bg-emerald-50 rounded-lg flex-shrink-0 mt-0.5">
              <Database size={14} className="text-emerald-600" />
            </div>
            <div className="flex-1 min-w-0">
              <p className="text-sm font-semibold text-slate-800 font-mono">{entry.table}</p>
              <p className="text-xs text-slate-500 mt-0.5 line-clamp-2">{entry.table_description}</p>
            </div>
            <div className="flex items-center gap-2 flex-shrink-0 mt-1">
              <span className="text-[10px] text-slate-400 font-medium">{entry.columns?.length ?? 0} col.</span>
              {expanded[i] ? <ChevronDown size={14} className="text-slate-400" /> : <ChevronRight size={14} className="text-slate-400" />}
            </div>
          </button>
          {expanded[i] && entry.columns && entry.columns.length > 0 && (
            <div className="border-t border-slate-100 overflow-x-auto">
              <table className="w-full text-xs">
                <thead className="bg-slate-50">
                  <tr>
                    <th className="text-left px-3 py-2 text-slate-500 font-semibold w-36">Colonne</th>
                    <th className="text-left px-3 py-2 text-slate-500 font-semibold w-28">Type</th>
                    <th className="text-left px-3 py-2 text-slate-500 font-semibold">Description métier</th>
                    <th className="text-left px-3 py-2 text-slate-500 font-semibold w-32">Format</th>
                    <th className="text-left px-3 py-2 text-slate-500 font-semibold w-40">Valeurs possibles</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-100">
                  {entry.columns.map((col, j) => (
                    <tr key={j} className="hover:bg-slate-50 transition-colors">
                      <td className="px-3 py-2 font-mono text-slate-700 font-medium">{col.name}</td>
                      <td className="px-3 py-2 text-slate-500 font-mono">{col.type}</td>
                      <td className="px-3 py-2 text-slate-600">{col.business_description || '—'}</td>
                      <td className="px-3 py-2 text-slate-500">{col.format || '—'}</td>
                      <td className="px-3 py-2 text-slate-500">{col.possible_values || '—'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      ))}
    </div>
  );
}

function AssistantMessage({ msg }: { msg: ChatMessage }) {
  if (msg.error) {
    return (
      <div className="flex gap-3 justify-start">
        <div className="p-2 bg-red-100 rounded-full flex-shrink-0 self-start">
          <XCircle size={14} className="text-red-500" />
        </div>
        <div className="bg-red-50 border border-red-200 rounded-xl px-4 py-3 text-sm text-red-700 max-w-lg">
          {msg.content}
        </div>
      </div>
    );
  }

  return (
    <div className="flex gap-3 justify-start">
      <div className="p-2 bg-emerald-100 rounded-full flex-shrink-0 self-start mt-1">
        <Cpu size={14} className="text-emerald-600" />
      </div>
      <div className="flex-1 max-w-full overflow-hidden">
        <div className="bg-white border border-slate-200 rounded-xl px-4 py-3 shadow-sm">
          <p className="text-sm text-slate-700 mb-1">{msg.content}</p>
          {msg.tables_processed !== undefined && (
            <div className="flex items-center gap-2 mt-2">
              <CheckCircle2 size={13} className="text-emerald-500" />
              <span className="text-xs text-emerald-700 font-medium">
                {msg.tables_processed}/{msg.total_tables} table{(msg.total_tables ?? 0) > 1 ? 's' : ''} documentée{(msg.tables_processed ?? 0) > 1 ? 's' : ''}
              </span>
            </div>
          )}
        </div>
        {msg.steps && msg.steps.length > 0 && <StepsPanel steps={msg.steps} />}
        {msg.data_dictionary && msg.data_dictionary.length > 0 && (
          <DataDictionaryView entries={msg.data_dictionary} />
        )}
      </div>
    </div>
  );
}

// ── Main Component ─────────────────────────────────────────────────────────

export function AgentsPane() {
  const [agents, setAgents] = useState<Agent[]>([]);
  const [loadingAgents, setLoadingAgents] = useState(true);
  const [selectedAgent, setSelectedAgent] = useState<Agent | null>(null);
  const [params, setParams] = useState<Record<string, string | number>>({});
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const [showParams, setShowParams] = useState(true);
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    fetch('/api/agents')
      .then(r => r.json())
      .then((data: Agent[]) => { setAgents(data); setLoadingAgents(false); })
      .catch(() => setLoadingAgents(false));
  }, []);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, loading]);

  function selectAgent(agent: Agent) {
    setSelectedAgent(agent);
    setMessages([]);
    const defaults: Record<string, string | number> = {};
    agent.parameters.forEach(p => { defaults[p.name] = p.default; });
    setParams(defaults);
  }

  function setParam(name: string, value: string | number) {
    setParams(prev => ({ ...prev, [name]: value }));
  }

  async function sendMessage() {
    if (!input.trim() || !selectedAgent || loading) return;
    const userMsg: ChatMessage = { role: 'user', content: input };
    setMessages(prev => [...prev, userMsg]);
    setInput('');
    setLoading(true);
    try {
      const res = await fetch(`/api/agents/${selectedAgent.id}/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          messages: [...messages, userMsg].map(m => ({ role: m.role, content: m.content })),
          params,
        }),
      });
      const data = await res.json();
      if (data.error) {
        setMessages(prev => [...prev, { role: 'assistant', content: data.error, error: data.error }]);
      } else {
        const processed = data.tables_processed ?? 0;
        const total = data.total_tables ?? 0;
        setMessages(prev => [...prev, {
          role: 'assistant',
          content: `Documentation générée pour ${processed}/${total} table${total > 1 ? 's' : ''}.`,
          ...data,
        }]);
      }
    } catch {
      setMessages(prev => [...prev, {
        role: 'assistant',
        content: 'Erreur de connexion au serveur.',
        error: 'Connection error',
      }]);
    } finally {
      setLoading(false);
    }
  }

  function handleKey(e: KeyboardEvent<HTMLTextAreaElement>) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      sendMessage();
    }
  }

  // ── Render ─────────────────────────────────────────────────────────────

  return (
    <div className="flex h-full bg-slate-50 overflow-hidden">

      {/* ── Left panel: agent list ─────────────────────────────────────── */}
      <div className="w-72 flex-shrink-0 bg-white border-r border-slate-200 flex flex-col">
        <div className="p-4 border-b border-slate-100">
          <div className="flex items-center gap-2 mb-0.5">
            <Cpu size={16} className="text-emerald-500" />
            <h2 className="text-sm font-bold text-slate-800">Agents disponibles</h2>
          </div>
          <p className="text-xs text-slate-400">Sélectionnez un agent pour démarrer</p>
        </div>

        <div className="flex-1 overflow-y-auto p-3 space-y-2">
          {loadingAgents ? (
            <div className="flex items-center justify-center py-10">
              <Loader2 size={20} className="animate-spin text-slate-400" />
            </div>
          ) : agents.length === 0 ? (
            <p className="text-xs text-slate-400 text-center py-10">Aucun agent disponible</p>
          ) : (
            agents.map(agent => (
              <AgentCard
                key={agent.id}
                agent={agent}
                selected={selectedAgent?.id === agent.id}
                onClick={() => selectAgent(agent)}
              />
            ))
          )}
        </div>
      </div>

      {/* ── Right panel: chat ──────────────────────────────────────────── */}
      {!selectedAgent ? (
        <div className="flex-1 flex flex-col items-center justify-center text-center p-8">
          <div className="p-5 bg-emerald-50 rounded-2xl mb-4">
            <Cpu size={40} className="text-emerald-400" />
          </div>
          <h3 className="text-lg font-semibold text-slate-700 mb-2">Choisissez un agent</h3>
          <p className="text-sm text-slate-400 max-w-xs leading-relaxed">
            Sélectionnez un agent dans le panneau de gauche pour démarrer une conversation dédiée.
          </p>
        </div>
      ) : (
        <div className="flex-1 flex flex-col overflow-hidden">

          {/* Agent header */}
          <div className="bg-white border-b border-slate-200 px-6 py-3 flex items-center gap-3 flex-shrink-0">
            <div className="p-2 bg-emerald-500 rounded-lg">
              <Cpu size={16} className="text-white" />
            </div>
            <div className="flex-1 min-w-0">
              <h2 className="text-sm font-bold text-slate-800">{selectedAgent.name}</h2>
              <p className="text-xs text-slate-400 truncate">{selectedAgent.description}</p>
            </div>
            <button
              onClick={() => { setMessages([]); }}
              title="Réinitialiser la conversation"
              className="p-1.5 rounded-lg hover:bg-slate-100 text-slate-400 hover:text-slate-600 transition-colors"
            >
              <RefreshCw size={14} />
            </button>
          </div>

          {/* Parameters panel */}
          {selectedAgent.parameters.length > 0 && (
            <div className="bg-white border-b border-slate-200 flex-shrink-0">
              <button
                onClick={() => setShowParams(p => !p)}
                className="w-full flex items-center gap-2 px-6 py-2.5 text-xs font-semibold text-slate-500 hover:bg-slate-50 transition-colors"
              >
                <Settings2 size={13} />
                Paramètres de l'agent
                <span className="ml-auto">
                  {showParams ? <ChevronDown size={13} /> : <ChevronRight size={13} />}
                </span>
              </button>
              {showParams && (
                <div className="px-6 pb-4 grid grid-cols-2 gap-x-6 gap-y-3">
                  {selectedAgent.parameters.map((p) => (
                    <div key={p.name}>
                      <label className="block text-xs font-semibold text-slate-600 mb-1">
                        {p.label}
                      </label>
                      {p.type === 'select' ? (
                        <select
                          value={params[p.name] as string}
                          onChange={(e) => setParam(p.name, e.target.value)}
                          className="w-full px-3 py-1.5 text-xs border border-slate-200 rounded-lg bg-white focus:outline-none focus:ring-2 focus:ring-emerald-400 focus:border-transparent"
                        >
                          {p.options?.map((opt) => (
                            <option key={opt} value={opt}>{opt}</option>
                          ))}
                        </select>
                      ) : (
                        <input
                          type={p.type === 'number' ? 'number' : 'text'}
                          value={params[p.name] as string}
                          onChange={(e) => setParam(p.name, p.type === 'number' ? Number(e.target.value) : e.target.value)}
                          placeholder={p.description}
                          className="w-full px-3 py-1.5 text-xs border border-slate-200 rounded-lg bg-white focus:outline-none focus:ring-2 focus:ring-emerald-400 focus:border-transparent"
                        />
                      )}
                      <p className="text-[10px] text-slate-400 mt-0.5">{p.description}</p>
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}

          {/* Messages area */}
          <div className="flex-1 overflow-y-auto px-6 py-4 space-y-4">
            {messages.length === 0 && (
              <div className="flex flex-col items-center justify-center h-full text-center opacity-60">
                <MessageSquare size={32} className="text-slate-300 mb-3" />
                <p className="text-sm text-slate-400 font-medium">Démarrez la conversation</p>
                <p className="text-xs text-slate-400 mt-1 max-w-xs">
                  Tapez votre demande ci-dessous ou utilisez les suggestions de démarrage.
                </p>
                <div className="mt-4 flex flex-wrap gap-2 justify-center">
                  {selectedAgent.id === 'data-dictionary' && [
                    'Génère le dictionnaire de données complet',
                    'Documente toutes les tables disponibles',
                    'Analyse et décris le schéma de la base',
                  ].map(s => (
                    <button
                      key={s}
                      onClick={() => { setInput(s); }}
                      className="px-3 py-1.5 text-xs bg-white border border-slate-200 rounded-full text-slate-600 hover:border-emerald-400 hover:text-emerald-600 transition-colors"
                    >
                      {s}
                    </button>
                  ))}
                </div>
              </div>
            )}

            {messages.map((msg, i) => (
              <div key={i}>
                {msg.role === 'user' ? (
                  <div className="flex justify-end">
                    <div className="bg-emerald-500 text-white rounded-xl px-4 py-2.5 max-w-md text-sm">
                      {msg.content}
                    </div>
                  </div>
                ) : (
                  <AssistantMessage msg={msg} />
                )}
              </div>
            ))}

            {loading && (
              <div className="flex gap-3 justify-start">
                <div className="p-2 bg-emerald-100 rounded-full flex-shrink-0">
                  <Loader2 size={14} className="text-emerald-600 animate-spin" />
                </div>
                <div className="bg-white border border-slate-200 rounded-xl px-4 py-3 shadow-sm">
                  <div className="flex items-center gap-2 text-sm text-slate-400">
                    <span>Analyse en cours</span>
                    <span className="flex gap-0.5">
                      {[0, 150, 300].map(d => (
                        <span
                          key={d}
                          className="w-1 h-1 bg-emerald-400 rounded-full animate-bounce"
                          style={{ animationDelay: `${d}ms` }}
                        />
                      ))}
                    </span>
                  </div>
                </div>
              </div>
            )}

            <div ref={bottomRef} />
          </div>

          {/* Input area */}
          <div className="bg-white border-t border-slate-200 p-4 flex-shrink-0">
            <div className="flex gap-3 items-end">
              <textarea
                value={input}
                onChange={e => setInput(e.target.value)}
                onKeyDown={handleKey}
                placeholder={`Interrogez l'agent "${selectedAgent.name}"…`}
                rows={2}
                disabled={loading}
                className="flex-1 resize-none px-4 py-3 text-sm border border-slate-200 rounded-xl bg-slate-50 focus:outline-none focus:ring-2 focus:ring-emerald-400 focus:border-transparent disabled:opacity-50 transition-colors"
              />
              <button
                onClick={sendMessage}
                disabled={!input.trim() || loading}
                className="p-3 bg-emerald-500 hover:bg-emerald-600 disabled:bg-slate-200 disabled:cursor-not-allowed text-white rounded-xl transition-colors flex-shrink-0"
              >
                {loading ? <Loader2 size={18} className="animate-spin" /> : <Send size={18} />}
              </button>
            </div>
            <p className="text-[10px] text-slate-400 mt-1.5 ml-1">Entrée pour envoyer · Maj+Entrée pour sauter une ligne</p>
          </div>
        </div>
      )}
    </div>
  );
}
