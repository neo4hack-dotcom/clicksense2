import { useState, useRef, useEffect, KeyboardEvent, CSSProperties } from 'react';
import {
  Cpu, Send, ChevronDown, ChevronRight, CheckCircle2, XCircle,
  Loader2, Database, FileText, Settings2, Table2, Columns3,
  MessageSquare, Play, RefreshCw, Zap, AlertTriangle, Info,
  RotateCcw, Trash2, BookOpen, TrendingUp, Star, Code2, Download,
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

// ── Writer Agent Types ──────────────────────────────────────────────────────

interface WriterStep {
  id: number;
  description: string;
  type: string;
  rationale: string;
  creates_table?: string | null;
}

interface WriterPlan {
  objective: string;
  approach: string;
  estimated_steps: number;
  complexity: string;
  steps: WriterStep[];
  replan_note?: string;
}

interface ActionEntry {
  step_id: number;
  description: string;
  sql: string;
  ok: boolean;
  result_preview: unknown;
  rows_affected?: number | null;
  explanation: string;
}

interface StepReflection {
  step_id: number;
  description: string;
  outcome: string;
  insight: string;
  status: 'success' | 'failed' | 'partial';
}

interface TableCreated {
  name: string;
  purpose: string;
  useful_for: string;
}

interface Synthesis {
  executive_summary: string;
  key_findings: string[];
  step_reflections: StepReflection[];
  data_insights: string;
  recommendations: string[];
  conclusion: string;
  tables_created: TableCreated[];
}

interface WriterChoice {
  label: string;
  value: string;
}

interface WriterQuestion {
  text: string;
  choices: WriterChoice[];
}

interface ReplanEntry {
  should_replan: boolean;
  reason: string;
  checked_after_step?: number;
}

interface ChatMessage {
  role: 'user' | 'assistant';
  content: string;
  // Data Dictionary fields
  steps?: StepInfo[];
  data_dictionary?: DictEntry[];
  tables_processed?: number;
  total_tables?: number;
  error?: string;
  // Writer Agent fields
  status?: string;
  plan?: WriterPlan;
  action_log?: ActionEntry[];
  action_count?: number;
  remaining_credits?: number;
  synthesis?: Synthesis;
  created_tables?: string[];
  question?: WriterQuestion;
  replan_log?: ReplanEntry[];
  session_id?: string;
  cleanup_done?: boolean;
  tables_dropped?: string[];
}

// ── Data Dictionary Sub-components ─────────────────────────────────────────

function AgentCard({ agent, selected, onClick }: { agent: Agent; selected: boolean; onClick: () => void }) {
  const isWriter = agent.id === 'clickhouse-writer';
  return (
    <button
      onClick={onClick}
      className={clsx(
        'w-full text-left p-4 rounded-xl border transition-all duration-200 group',
        selected
          ? isWriter ? 'border-violet-500 bg-violet-500/10' : 'border-emerald-500 bg-emerald-500/10'
          : isWriter
            ? 'border-slate-200 bg-white hover:border-violet-300 hover:shadow-sm'
            : 'border-slate-200 bg-white hover:border-emerald-300 hover:shadow-sm',
      )}
    >
      <div className="flex items-start gap-3">
        <div className={clsx(
          'p-2 rounded-lg flex-shrink-0 transition-colors',
          selected
            ? isWriter ? 'bg-violet-500 text-white' : 'bg-emerald-500 text-white'
            : isWriter
              ? 'bg-slate-100 text-slate-500 group-hover:bg-violet-100 group-hover:text-violet-600'
              : 'bg-slate-100 text-slate-500 group-hover:bg-emerald-100 group-hover:text-emerald-600',
        )}>
          {isWriter ? <Zap size={16} /> : <Cpu size={16} />}
        </div>
        <div className="min-w-0">
          <p className={clsx('text-sm font-semibold truncate',
            selected ? isWriter ? 'text-violet-700' : 'text-emerald-700' : 'text-slate-800')}>
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

// ── Technical Specification Document ─────────────────────────────────────────

function getTypeColorInline(type: string): CSSProperties {
  const t = type.toLowerCase();
  if (t.includes('int') || t.includes('float') || t.includes('decimal') || t.includes('numeric'))
    return { color: '#1d4ed8', backgroundColor: '#dbeafe', borderColor: '#bfdbfe' };
  if (t.includes('string') || t.includes('varchar') || t.includes('text') || t.includes('char'))
    return { color: '#6d28d9', backgroundColor: '#ede9fe', borderColor: '#c4b5fd' };
  if (t.includes('date') || t.includes('time') || t.includes('timestamp'))
    return { color: '#b45309', backgroundColor: '#fef3c7', borderColor: '#fde68a' };
  if (t.includes('bool'))
    return { color: '#065f46', backgroundColor: '#d1fae5', borderColor: '#a7f3d0' };
  if (t.includes('uuid') || t.includes('fixedstring'))
    return { color: '#9d174d', backgroundColor: '#fce7f3', borderColor: '#fbcfe8' };
  if (t.includes('array') || t.includes('map') || t.includes('tuple'))
    return { color: '#9a3412', backgroundColor: '#fff7ed', borderColor: '#fed7aa' };
  return { color: '#374151', backgroundColor: '#f3f4f6', borderColor: '#e5e7eb' };
}

function TechSpecView({ entries }: { entries: DictEntry[] }) {
  const totalColumns = entries.reduce((acc, e) => acc + (e.columns?.length || 0), 0);
  const allTypes = [...new Set(
    entries.flatMap(e => e.columns?.map(c => c.type.split('(')[0].split('<')[0]) || [])
  )].sort();
  const documented = entries.filter(e => e.table_description && e.table_description !== '—').length;
  const coverage = entries.length ? Math.round((documented / entries.length) * 100) : 0;
  const genDate = new Date().toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' });

  return (
    <div className="mt-3 space-y-4">
      {/* Cover / Header */}
      <div className="rounded-2xl overflow-hidden" style={{ background: 'linear-gradient(135deg, #059669 0%, #0d9488 50%, #0891b2 100%)' }}>
        <div className="px-6 pt-6 pb-5">
          <div className="flex items-start justify-between mb-5">
            <div>
              <span className="inline-block px-2.5 py-0.5 rounded-full text-[10px] font-bold uppercase tracking-widest mb-2"
                style={{ background: 'rgba(255,255,255,0.2)', color: 'rgba(255,255,255,0.9)' }}>
                Technical Specification Document
              </span>
              <h1 className="text-2xl font-extrabold text-white leading-tight">Data Dictionary</h1>
              <p className="text-sm mt-0.5" style={{ color: 'rgba(255,255,255,0.75)' }}>
                Database Schema &amp; Column Reference Guide
              </p>
            </div>
            <div className="p-3 rounded-2xl" style={{ background: 'rgba(255,255,255,0.15)' }}>
              <Database size={28} className="text-white" />
            </div>
          </div>
          <div className="flex flex-wrap gap-6 pt-4" style={{ borderTop: '1px solid rgba(255,255,255,0.2)' }}>
            {[
              { label: 'Tables', value: entries.length },
              { label: 'Columns', value: totalColumns },
              { label: 'Data Types', value: allTypes.length },
              { label: 'Coverage', value: `${coverage}%` },
            ].map(({ label, value }) => (
              <div key={label}>
                <p className="text-2xl font-black text-white">{value}</p>
                <p className="text-xs" style={{ color: 'rgba(255,255,255,0.65)' }}>{label}</p>
              </div>
            ))}
          </div>
        </div>
        <div className="px-6 py-2.5 flex items-center gap-3 text-[10px]"
          style={{ background: 'rgba(0,0,0,0.2)', color: 'rgba(255,255,255,0.6)' }}>
          <span>Generated: {genDate}</span>
          <span>·</span>
          <span>ClickSense AI Data Agent</span>
          <span>·</span>
          <span>v1.0</span>
        </div>
      </div>

      {/* Section 1: Executive Overview */}
      <div className="bg-white rounded-2xl border border-slate-200 overflow-hidden">
        <div className="px-4 py-3 bg-slate-50 border-b border-slate-100 flex items-center gap-2">
          <div className="w-6 h-6 rounded-lg bg-emerald-500 text-white text-xs font-black flex items-center justify-center flex-shrink-0">1</div>
          <h2 className="text-sm font-bold text-slate-800">Executive Overview</h2>
        </div>
        <div className="p-4 grid grid-cols-2 sm:grid-cols-4 gap-3">
          {[
            { label: 'Total Tables', value: entries.length, sub: 'tables analyzed', bg: '#ecfdf5', border: '#a7f3d0', color: '#065f46' },
            { label: 'Total Columns', value: totalColumns, sub: 'fields documented', bg: '#f0fdfa', border: '#99f6e4', color: '#0f766e' },
            { label: 'Distinct Types', value: allTypes.length, sub: 'data types found', bg: '#ecfeff', border: '#a5f3fc', color: '#0e7490' },
            { label: 'Coverage', value: `${coverage}%`, sub: 'tables with desc.', bg: '#eff6ff', border: '#bfdbfe', color: '#1d4ed8' },
          ].map(({ label, value, sub, bg, border, color }) => (
            <div key={label} className="p-3 rounded-xl border" style={{ backgroundColor: bg, borderColor: border, color }}>
              <p className="text-2xl font-black">{value}</p>
              <p className="text-xs font-semibold mt-0.5">{label}</p>
              <p className="text-[10px] mt-0.5 opacity-70">{sub}</p>
            </div>
          ))}
        </div>
      </div>

      {/* Section 2: Table of Contents */}
      <div className="bg-white rounded-2xl border border-slate-200 overflow-hidden">
        <div className="px-4 py-3 bg-slate-50 border-b border-slate-100 flex items-center gap-2">
          <div className="w-6 h-6 rounded-lg bg-emerald-500 text-white text-xs font-black flex items-center justify-center flex-shrink-0">2</div>
          <h2 className="text-sm font-bold text-slate-800">Table of Contents</h2>
          <span className="ml-auto text-xs text-slate-400">{entries.length} tables</span>
        </div>
        <div className="p-3">
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-0.5">
            {entries.map((entry, i) => (
              <div key={i} className="flex items-center gap-2 px-3 py-1.5 rounded-lg hover:bg-slate-50 transition-colors">
                <span className="text-[10px] font-bold text-slate-400 w-5 text-right flex-shrink-0">{i + 1}</span>
                <span className="text-xs font-mono font-semibold text-emerald-700 truncate">{entry.table}</span>
                <span className="ml-auto text-[10px] text-slate-400 flex-shrink-0">{entry.columns?.length || 0} cols</span>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Section 3: Schema Documentation */}
      <div className="bg-white rounded-2xl border border-slate-200 overflow-hidden">
        <div className="px-4 py-3 bg-slate-50 border-b border-slate-100 flex items-center gap-2">
          <div className="w-6 h-6 rounded-lg bg-emerald-500 text-white text-xs font-black flex items-center justify-center flex-shrink-0">3</div>
          <h2 className="text-sm font-bold text-slate-800">Schema Documentation</h2>
          <span className="ml-auto text-xs text-slate-400">{entries.length} tables</span>
        </div>
        <div className="divide-y divide-slate-100">
          {entries.map((entry, i) => (
            <div key={i} className="p-4">
              <div className="flex items-start gap-3 mb-3">
                <div className="w-7 h-7 rounded-lg bg-emerald-50 border border-emerald-200 flex items-center justify-center flex-shrink-0">
                  <span className="text-[10px] font-black text-emerald-600">{i + 1}</span>
                </div>
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2 flex-wrap">
                    <h3 className="text-sm font-bold font-mono text-slate-800">{entry.table}</h3>
                    <span className="px-1.5 py-0.5 rounded bg-slate-100 text-[9px] font-bold text-slate-500">
                      {entry.columns?.length || 0} columns
                    </span>
                  </div>
                  {entry.table_description && (
                    <p className="text-xs text-slate-500 mt-0.5 leading-relaxed">{entry.table_description}</p>
                  )}
                </div>
              </div>
              {entry.columns && entry.columns.length > 0 ? (
                <div className="overflow-x-auto rounded-xl border border-slate-100">
                  <table className="w-full text-xs">
                    <thead>
                      <tr className="bg-slate-50">
                        <th className="text-left px-3 py-2 text-slate-500 font-semibold border-b border-slate-100 whitespace-nowrap">Column</th>
                        <th className="text-left px-3 py-2 text-slate-500 font-semibold border-b border-slate-100 whitespace-nowrap">Type</th>
                        <th className="text-left px-3 py-2 text-slate-500 font-semibold border-b border-slate-100">Business Description</th>
                        <th className="text-left px-3 py-2 text-slate-500 font-semibold border-b border-slate-100 whitespace-nowrap">Format</th>
                        <th className="text-left px-3 py-2 text-slate-500 font-semibold border-b border-slate-100 whitespace-nowrap">Possible Values</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-50">
                      {entry.columns.map((col, j) => (
                        <tr key={j} className={j % 2 === 0 ? 'bg-white' : 'bg-slate-50/50'}>
                          <td className="px-3 py-2 font-mono font-semibold text-slate-800 whitespace-nowrap">{col.name}</td>
                          <td className="px-3 py-2 whitespace-nowrap">
                            <span className="px-1.5 py-0.5 rounded text-[9px] font-bold font-mono border"
                              style={getTypeColorInline(col.type)}>
                              {col.type}
                            </span>
                          </td>
                          <td className="px-3 py-2 text-slate-600 leading-relaxed">{col.business_description || '—'}</td>
                          <td className="px-3 py-2 text-slate-500 whitespace-nowrap">{col.format || '—'}</td>
                          <td className="px-3 py-2 text-slate-500">{col.possible_values || '—'}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <p className="text-xs text-slate-400 italic">No columns documented.</p>
              )}
            </div>
          ))}
        </div>
      </div>

      {/* Section 4: Data Types Reference */}
      {allTypes.length > 0 && (
        <div className="bg-white rounded-2xl border border-slate-200 overflow-hidden">
          <div className="px-4 py-3 bg-slate-50 border-b border-slate-100 flex items-center gap-2">
            <div className="w-6 h-6 rounded-lg bg-emerald-500 text-white text-xs font-black flex items-center justify-center flex-shrink-0">4</div>
            <h2 className="text-sm font-bold text-slate-800">Data Types Reference</h2>
          </div>
          <div className="p-4 flex flex-wrap gap-2">
            {allTypes.map((t, i) => (
              <span key={i} className="px-2 py-1 rounded-lg text-xs font-bold font-mono border"
                style={getTypeColorInline(t)}>
                {t}
              </span>
            ))}
          </div>
        </div>
      )}

      {/* Document Footer */}
      <div className="flex items-center justify-between px-4 py-2.5 bg-slate-50 rounded-xl border border-slate-200 text-[10px] text-slate-400">
        <span>Data Dictionary · ClickSense AI Agent</span>
        <span>{genDate}</span>
      </div>
    </div>
  );
}

function generateTechSpecHTML(entries: DictEntry[]): string {
  const genDate = new Date().toLocaleDateString('en-US', {
    year: 'numeric', month: 'long', day: 'numeric', hour: '2-digit', minute: '2-digit',
  });
  const totalColumns = entries.reduce((acc, e) => acc + (e.columns?.length || 0), 0);
  const allTypes = [...new Set(
    entries.flatMap(e => e.columns?.map(c => c.type.split('(')[0].split('<')[0]) || [])
  )].sort();
  const documented = entries.filter(e => e.table_description && e.table_description !== '—').length;
  const coverage = entries.length ? Math.round((documented / entries.length) * 100) : 0;

  function typeStyle(type: string): string {
    const t = type.toLowerCase();
    if (t.includes('int') || t.includes('float') || t.includes('decimal') || t.includes('numeric'))
      return 'color:#1d4ed8;background-color:#dbeafe;border:1px solid #bfdbfe';
    if (t.includes('string') || t.includes('varchar') || t.includes('text') || t.includes('char'))
      return 'color:#6d28d9;background-color:#ede9fe;border:1px solid #c4b5fd';
    if (t.includes('date') || t.includes('time') || t.includes('timestamp'))
      return 'color:#b45309;background-color:#fef3c7;border:1px solid #fde68a';
    if (t.includes('bool'))
      return 'color:#065f46;background-color:#d1fae5;border:1px solid #a7f3d0';
    if (t.includes('uuid') || t.includes('fixedstring'))
      return 'color:#9d174d;background-color:#fce7f3;border:1px solid #fbcfe8';
    if (t.includes('array') || t.includes('map') || t.includes('tuple'))
      return 'color:#9a3412;background-color:#fff7ed;border:1px solid #fed7aa';
    return 'color:#374151;background-color:#f3f4f6;border:1px solid #e5e7eb';
  }

  const tocRows = entries.map((entry, i) => `
    <tr>
      <td style="color:#94a3b8;font-weight:700;width:36px;text-align:right;padding:6px 12px;border-bottom:1px solid #f8fafc;">${i + 1}</td>
      <td style="font-family:'Courier New',monospace;color:#059669;font-weight:600;padding:6px 12px;border-bottom:1px solid #f8fafc;">${entry.table}</td>
      <td style="color:#94a3b8;text-align:right;width:64px;padding:6px 12px;border-bottom:1px solid #f8fafc;">${entry.columns?.length || 0} cols</td>
    </tr>`).join('');

  const schemaSections = entries.map((entry, i) => {
    const colRows = entry.columns?.length ? entry.columns.map((col, j) => `
      <tr style="background:${j % 2 === 0 ? '#ffffff' : '#fafafa'};">
        <td style="padding:7px 10px;font-family:'Courier New',monospace;font-weight:600;color:#1e293b;white-space:nowrap;border-bottom:1px solid #f1f5f9;">${col.name}</td>
        <td style="padding:7px 10px;white-space:nowrap;border-bottom:1px solid #f1f5f9;">
          <span style="display:inline-block;padding:2px 6px;border-radius:4px;font-size:9px;font-weight:700;font-family:'Courier New',monospace;${typeStyle(col.type)};">${col.type}</span>
        </td>
        <td style="padding:7px 10px;color:#475569;line-height:1.5;border-bottom:1px solid #f1f5f9;">${col.business_description || '—'}</td>
        <td style="padding:7px 10px;color:#64748b;white-space:nowrap;border-bottom:1px solid #f1f5f9;">${col.format || '—'}</td>
        <td style="padding:7px 10px;color:#64748b;border-bottom:1px solid #f1f5f9;">${col.possible_values || '—'}</td>
      </tr>`).join('') : '';
    return `
    <div style="padding:20px;border-bottom:1px solid #f1f5f9;page-break-inside:avoid;">
      <div style="display:flex;align-items:flex-start;gap:12px;margin-bottom:12px;">
        <div style="width:28px;height:28px;border-radius:8px;background:#ecfdf5;border:1px solid #a7f3d0;display:flex;align-items:center;justify-content:center;flex-shrink:0;">
          <span style="font-size:10px;font-weight:900;color:#059669;">${i + 1}</span>
        </div>
        <div style="flex:1;min-width:0;">
          <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;">
            <span style="font-family:'Courier New',monospace;font-size:13px;font-weight:700;color:#1e293b;">${entry.table}</span>
            <span style="padding:2px 8px;border-radius:20px;font-size:9px;font-weight:700;background:#f1f5f9;color:#64748b;">${entry.columns?.length || 0} columns</span>
          </div>
          ${entry.table_description ? `<p style="font-size:11px;color:#64748b;margin-top:4px;line-height:1.6;">${entry.table_description}</p>` : ''}
        </div>
      </div>
      ${entry.columns?.length ? `
        <table style="width:100%;border-collapse:collapse;border:1px solid #e2e8f0;border-radius:8px;overflow:hidden;font-size:11px;">
          <thead>
            <tr style="background:#f8fafc;">
              <th style="text-align:left;padding:8px 10px;color:#64748b;font-weight:600;border-bottom:1px solid #e2e8f0;">Column</th>
              <th style="text-align:left;padding:8px 10px;color:#64748b;font-weight:600;border-bottom:1px solid #e2e8f0;">Type</th>
              <th style="text-align:left;padding:8px 10px;color:#64748b;font-weight:600;border-bottom:1px solid #e2e8f0;">Business Description</th>
              <th style="text-align:left;padding:8px 10px;color:#64748b;font-weight:600;border-bottom:1px solid #e2e8f0;">Format</th>
              <th style="text-align:left;padding:8px 10px;color:#64748b;font-weight:600;border-bottom:1px solid #e2e8f0;">Possible Values</th>
            </tr>
          </thead>
          <tbody>${colRows}</tbody>
        </table>
      ` : '<p style="font-size:11px;color:#94a3b8;font-style:italic;">No columns documented.</p>'}
    </div>`;
  }).join('');

  const typePills = allTypes.map(t =>
    `<span style="display:inline-block;padding:4px 10px;border-radius:8px;font-size:11px;font-weight:700;font-family:'Courier New',monospace;${typeStyle(t)};">${t}</span>`
  ).join(' ');

  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<title>Data Dictionary — Technical Specification Document</title>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: 'Segoe UI', system-ui, -apple-system, sans-serif; color: #1e293b; background: #f8fafc; -webkit-print-color-adjust: exact; print-color-adjust: exact; }
@media print {
  body { background: white; }
  @page { margin: 15mm 12mm; size: A4; }
  .no-break { page-break-inside: avoid; }
}
</style>
</head>
<body style="max-width:900px;margin:0 auto;padding:24px;">

  <!-- Cover -->
  <div style="border-radius:16px;overflow:hidden;margin-bottom:28px;background:linear-gradient(135deg,#059669 0%,#0d9488 50%,#0891b2 100%);">
    <div style="padding:40px 36px 28px;">
      <span style="display:inline-block;background:rgba(255,255,255,0.2);color:rgba(255,255,255,0.9);font-size:9px;font-weight:700;letter-spacing:0.12em;text-transform:uppercase;padding:3px 10px;border-radius:20px;margin-bottom:12px;">Technical Specification Document</span>
      <h1 style="font-size:32px;font-weight:900;color:white;line-height:1.1;margin-bottom:6px;">Data Dictionary</h1>
      <p style="font-size:14px;color:rgba(255,255,255,0.75);margin-bottom:24px;">Database Schema &amp; Column Reference Guide</p>
      <div style="display:flex;gap:32px;padding-top:20px;border-top:1px solid rgba(255,255,255,0.2);">
        <div><div style="font-size:24px;font-weight:900;color:white;">${entries.length}</div><div style="font-size:11px;color:rgba(255,255,255,0.65);">Tables</div></div>
        <div><div style="font-size:24px;font-weight:900;color:white;">${totalColumns}</div><div style="font-size:11px;color:rgba(255,255,255,0.65);">Columns</div></div>
        <div><div style="font-size:24px;font-weight:900;color:white;">${allTypes.length}</div><div style="font-size:11px;color:rgba(255,255,255,0.65);">Data Types</div></div>
        <div><div style="font-size:24px;font-weight:900;color:white;">${coverage}%</div><div style="font-size:11px;color:rgba(255,255,255,0.65);">Coverage</div></div>
      </div>
    </div>
    <div style="padding:10px 36px;background:rgba(0,0,0,0.2);font-size:10px;color:rgba(255,255,255,0.6);display:flex;gap:16px;">
      <span>Generated: ${genDate}</span><span>·</span><span>ClickSense AI Data Agent</span><span>·</span><span>v1.0</span>
    </div>
  </div>

  <!-- Section 1: Executive Overview -->
  <div style="background:white;border:1px solid #e2e8f0;border-radius:16px;overflow:hidden;margin-bottom:20px;" class="no-break">
    <div style="display:flex;align-items:center;gap:10px;padding:12px 16px;background:#f8fafc;border-bottom:1px solid #f1f5f9;">
      <div style="width:24px;height:24px;border-radius:8px;background:#059669;color:white;font-size:11px;font-weight:900;display:flex;align-items:center;justify-content:center;flex-shrink:0;">1</div>
      <span style="font-size:13px;font-weight:700;color:#1e293b;">Executive Overview</span>
    </div>
    <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;padding:16px;">
      <div style="padding:12px;border-radius:12px;background:#ecfdf5;border:1px solid #a7f3d0;color:#065f46;">
        <div style="font-size:24px;font-weight:900;">${entries.length}</div>
        <div style="font-size:11px;font-weight:600;margin-top:2px;">Total Tables</div>
        <div style="font-size:9px;opacity:0.7;margin-top:2px;">tables analyzed</div>
      </div>
      <div style="padding:12px;border-radius:12px;background:#f0fdfa;border:1px solid #99f6e4;color:#0f766e;">
        <div style="font-size:24px;font-weight:900;">${totalColumns}</div>
        <div style="font-size:11px;font-weight:600;margin-top:2px;">Total Columns</div>
        <div style="font-size:9px;opacity:0.7;margin-top:2px;">fields documented</div>
      </div>
      <div style="padding:12px;border-radius:12px;background:#ecfeff;border:1px solid #a5f3fc;color:#0e7490;">
        <div style="font-size:24px;font-weight:900;">${allTypes.length}</div>
        <div style="font-size:11px;font-weight:600;margin-top:2px;">Distinct Types</div>
        <div style="font-size:9px;opacity:0.7;margin-top:2px;">data types detected</div>
      </div>
      <div style="padding:12px;border-radius:12px;background:#eff6ff;border:1px solid #bfdbfe;color:#1d4ed8;">
        <div style="font-size:24px;font-weight:900;">${coverage}%</div>
        <div style="font-size:11px;font-weight:600;margin-top:2px;">Documentation</div>
        <div style="font-size:9px;opacity:0.7;margin-top:2px;">tables with descriptions</div>
      </div>
    </div>
  </div>

  <!-- Section 2: Table of Contents -->
  <div style="background:white;border:1px solid #e2e8f0;border-radius:16px;overflow:hidden;margin-bottom:20px;" class="no-break">
    <div style="display:flex;align-items:center;gap:10px;padding:12px 16px;background:#f8fafc;border-bottom:1px solid #f1f5f9;">
      <div style="width:24px;height:24px;border-radius:8px;background:#059669;color:white;font-size:11px;font-weight:900;display:flex;align-items:center;justify-content:center;flex-shrink:0;">2</div>
      <span style="font-size:13px;font-weight:700;color:#1e293b;">Table of Contents</span>
    </div>
    <table style="width:100%;border-collapse:collapse;">${tocRows}</table>
  </div>

  <!-- Section 3: Schema Documentation -->
  <div style="background:white;border:1px solid #e2e8f0;border-radius:16px;overflow:hidden;margin-bottom:20px;">
    <div style="display:flex;align-items:center;gap:10px;padding:12px 16px;background:#f8fafc;border-bottom:1px solid #f1f5f9;">
      <div style="width:24px;height:24px;border-radius:8px;background:#059669;color:white;font-size:11px;font-weight:900;display:flex;align-items:center;justify-content:center;flex-shrink:0;">3</div>
      <span style="font-size:13px;font-weight:700;color:#1e293b;">Schema Documentation</span>
      <span style="margin-left:auto;font-size:10px;color:#94a3b8;">${entries.length} tables</span>
    </div>
    ${schemaSections}
  </div>

  ${allTypes.length ? `
  <!-- Section 4: Data Types Reference -->
  <div style="background:white;border:1px solid #e2e8f0;border-radius:16px;overflow:hidden;margin-bottom:20px;" class="no-break">
    <div style="display:flex;align-items:center;gap:10px;padding:12px 16px;background:#f8fafc;border-bottom:1px solid #f1f5f9;">
      <div style="width:24px;height:24px;border-radius:8px;background:#059669;color:white;font-size:11px;font-weight:900;display:flex;align-items:center;justify-content:center;flex-shrink:0;">4</div>
      <span style="font-size:13px;font-weight:700;color:#1e293b;">Data Types Reference</span>
    </div>
    <div style="padding:16px;display:flex;flex-wrap:wrap;gap:8px;">${typePills}</div>
  </div>` : ''}

  <!-- Footer -->
  <div style="display:flex;justify-content:space-between;padding:10px 16px;background:#f8fafc;border:1px solid #e2e8f0;border-radius:12px;font-size:10px;color:#94a3b8;">
    <span>Data Dictionary · ClickSense AI Data Agent</span>
    <span>${genDate}</span>
  </div>

</body>
</html>`;
}

function DictionaryOutputPanel({ entries }: { entries: DictEntry[] }) {
  const [view, setView] = useState<'spec' | 'table'>('spec');

  function exportToPDF() {
    const html = generateTechSpecHTML(entries);
    const win = window.open('', '_blank');
    if (!win) return;
    win.document.write(html);
    win.document.close();
    setTimeout(() => { try { win.print(); } catch { /* ignore */ } }, 800);
  }

  return (
    <div>
      <div className="flex items-center gap-2 mt-3">
        <div className="flex bg-slate-100 rounded-lg p-0.5 text-xs">
          <button
            onClick={() => setView('spec')}
            className={clsx(
              'flex items-center gap-1 px-3 py-1.5 rounded-md font-medium transition-all',
              view === 'spec' ? 'bg-white text-emerald-700 shadow-sm' : 'text-slate-500 hover:text-slate-700',
            )}
          >
            <FileText size={11} />
            Tech Spec
          </button>
          <button
            onClick={() => setView('table')}
            className={clsx(
              'flex items-center gap-1 px-3 py-1.5 rounded-md font-medium transition-all',
              view === 'table' ? 'bg-white text-slate-700 shadow-sm' : 'text-slate-500 hover:text-slate-700',
            )}
          >
            <Table2 size={11} />
            Table view
          </button>
        </div>
        <button
          onClick={exportToPDF}
          className="ml-auto flex items-center gap-1.5 px-3 py-1.5 text-xs font-semibold bg-emerald-500 hover:bg-emerald-600 text-white rounded-lg transition-colors shadow-sm"
        >
          <Download size={12} />
          Export PDF
        </button>
      </div>
      {view === 'spec' ? (
        <TechSpecView entries={entries} />
      ) : (
        <DataDictionaryView entries={entries} />
      )}
    </div>
  );
}

// ── Writer Agent Sub-components ─────────────────────────────────────────────

const COMPLEXITY_COLOR: Record<string, string> = {
  simple: 'text-emerald-600 bg-emerald-50 border-emerald-200',
  medium: 'text-amber-600 bg-amber-50 border-amber-200',
  complex: 'text-orange-600 bg-orange-50 border-orange-200',
  very_complex: 'text-red-600 bg-red-50 border-red-200',
};

const STEP_TYPE_COLOR: Record<string, string> = {
  explore: 'bg-blue-100 text-blue-700',
  compute: 'bg-purple-100 text-purple-700',
  create_table: 'bg-violet-100 text-violet-700',
  insert: 'bg-indigo-100 text-indigo-700',
  verify: 'bg-emerald-100 text-emerald-700',
  aggregate: 'bg-amber-100 text-amber-700',
  cleanup: 'bg-red-100 text-red-700',
};

function PlanView({ plan, actionCount }: { plan: WriterPlan; actionCount?: number }) {
  const [open, setOpen] = useState(true);
  const complexity = plan.complexity || 'medium';
  const badgeClass = COMPLEXITY_COLOR[complexity] || COMPLEXITY_COLOR.medium;

  return (
    <div className="mt-3 border border-violet-200 rounded-xl overflow-hidden">
      <button
        onClick={() => setOpen(o => !o)}
        className="w-full flex items-center gap-2 px-4 py-3 bg-violet-50 hover:bg-violet-100 transition-colors text-left"
      >
        {open ? <ChevronDown size={13} className="text-violet-500" /> : <ChevronRight size={13} className="text-violet-500" />}
        <Zap size={14} className="text-violet-600" />
        <span className="text-sm font-bold text-violet-800">Plan d'exécution</span>
        <span className={clsx('ml-2 px-2 py-0.5 text-[10px] font-bold rounded-full border', badgeClass)}>
          {complexity.toUpperCase()}
        </span>
        <span className="ml-auto flex items-center gap-2">
          {actionCount !== undefined && (
            <span className="text-xs text-violet-600 font-medium">
              {actionCount}/{plan.steps.length} étapes
            </span>
          )}
        </span>
      </button>

      {open && (
        <div className="p-4 bg-white space-y-3">
          {/* Objective */}
          <div className="p-3 bg-violet-50 rounded-lg border border-violet-100">
            <p className="text-[10px] font-bold text-violet-500 uppercase tracking-wide mb-1">Objectif</p>
            <p className="text-sm text-violet-900 font-medium">{plan.objective}</p>
          </div>

          {/* Approach */}
          <div>
            <p className="text-[10px] font-bold text-slate-400 uppercase tracking-wide mb-1">Approche</p>
            <p className="text-xs text-slate-600 leading-relaxed">{plan.approach}</p>
          </div>

          {/* Replan note */}
          {plan.replan_note && (
            <div className="flex items-start gap-2 p-2 bg-amber-50 border border-amber-200 rounded-lg">
              <RotateCcw size={12} className="text-amber-500 mt-0.5 flex-shrink-0" />
              <p className="text-xs text-amber-700">{plan.replan_note}</p>
            </div>
          )}

          {/* Steps */}
          <div className="space-y-1.5">
            {plan.steps.map((step, i) => {
              const isDone = actionCount !== undefined && step.id <= actionCount;
              const isActive = actionCount !== undefined && step.id === actionCount + 1;
              return (
                <div
                  key={step.id}
                  className={clsx(
                    'flex items-start gap-3 p-2.5 rounded-lg border transition-colors',
                    isDone ? 'bg-emerald-50 border-emerald-200' :
                    isActive ? 'bg-violet-50 border-violet-300 shadow-sm' :
                    'bg-slate-50 border-slate-100',
                  )}
                >
                  <div className={clsx(
                    'flex-shrink-0 w-5 h-5 rounded-full flex items-center justify-center text-[10px] font-bold mt-0.5',
                    isDone ? 'bg-emerald-500 text-white' :
                    isActive ? 'bg-violet-500 text-white' :
                    'bg-slate-200 text-slate-500',
                  )}>
                    {isDone ? '✓' : step.id}
                  </div>
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 mb-0.5">
                      <span className={clsx(
                        'px-1.5 py-0.5 rounded text-[9px] font-bold uppercase',
                        STEP_TYPE_COLOR[step.type] || 'bg-slate-100 text-slate-600',
                      )}>
                        {step.type}
                      </span>
                      {step.creates_table && (
                        <span className="px-1.5 py-0.5 rounded text-[9px] font-bold bg-violet-100 text-violet-700">
                          → {step.creates_table}
                        </span>
                      )}
                    </div>
                    <p className="text-xs font-medium text-slate-800">{step.description}</p>
                    <p className="text-[10px] text-slate-400 mt-0.5 italic">{step.rationale}</p>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}

function ActionLogView({ log }: { log: ActionEntry[] }) {
  const [expandedIdx, setExpandedIdx] = useState<Record<number, boolean>>({});

  function toggle(i: number) {
    setExpandedIdx(prev => ({ ...prev, [i]: !prev[i] }));
  }

  return (
    <div className="mt-3 border border-slate-200 rounded-xl overflow-hidden">
      <div className="px-4 py-2.5 bg-slate-50 border-b border-slate-200 flex items-center gap-2">
        <Code2 size={13} className="text-slate-500" />
        <span className="text-xs font-bold text-slate-700">Journal d'exécution</span>
        <span className="ml-auto flex gap-2 text-xs">
          <span className="text-emerald-600 font-medium">{log.filter(e => e.ok).length} OK</span>
          {log.filter(e => !e.ok).length > 0 && (
            <span className="text-red-500 font-medium">{log.filter(e => !e.ok).length} Erreur</span>
          )}
        </span>
      </div>

      <div className="divide-y divide-slate-100 max-h-80 overflow-y-auto">
        {log.map((entry, i) => (
          <div key={i} className="bg-white">
            <button
              onClick={() => toggle(i)}
              className="w-full flex items-center gap-3 px-4 py-2.5 hover:bg-slate-50 transition-colors text-left"
            >
              {entry.ok
                ? <CheckCircle2 size={13} className="text-emerald-500 flex-shrink-0" />
                : <XCircle size={13} className="text-red-400 flex-shrink-0" />
              }
              <div className="flex-shrink-0 w-5 h-5 rounded-full bg-slate-100 flex items-center justify-center text-[9px] font-bold text-slate-600">
                {entry.step_id}
              </div>
              <div className="flex-1 min-w-0">
                <p className="text-xs font-medium text-slate-800 truncate">{entry.description}</p>
                {entry.explanation && (
                  <p className="text-[10px] text-slate-400 truncate">{entry.explanation}</p>
                )}
              </div>
              <div className="flex items-center gap-2 flex-shrink-0">
                {entry.rows_affected != null && (
                  <span className="text-[10px] text-slate-400">{entry.rows_affected} lignes</span>
                )}
                {expandedIdx[i] ? <ChevronDown size={12} className="text-slate-400" /> : <ChevronRight size={12} className="text-slate-400" />}
              </div>
            </button>

            {expandedIdx[i] && (
              <div className="px-4 pb-3 space-y-2">
                {entry.sql && (
                  <div>
                    <p className="text-[10px] font-bold text-slate-400 uppercase tracking-wide mb-1">SQL exécuté</p>
                    <pre className="text-[10px] bg-slate-900 text-green-300 rounded-lg p-3 overflow-x-auto whitespace-pre-wrap font-mono leading-relaxed">
                      {entry.sql}
                    </pre>
                  </div>
                )}
                {entry.result_preview != null && (
                  <div>
                    <p className="text-[10px] font-bold text-slate-400 uppercase tracking-wide mb-1">Résultat</p>
                    {typeof entry.result_preview === 'string' ? (
                      <p className={clsx(
                        'text-xs p-2 rounded-lg',
                        entry.ok ? 'bg-emerald-50 text-emerald-700' : 'bg-red-50 text-red-700',
                      )}>
                        {entry.result_preview}
                      </p>
                    ) : Array.isArray(entry.result_preview) && entry.result_preview.length > 0 ? (
                      <div className="overflow-x-auto rounded-lg border border-slate-100">
                        <table className="w-full text-[10px]">
                          <thead className="bg-slate-50">
                            <tr>
                              {Object.keys(entry.result_preview[0] as Record<string, unknown>).map(col => (
                                <th key={col} className="text-left px-2 py-1 text-slate-500 font-semibold border-b border-slate-100">
                                  {col}
                                </th>
                              ))}
                            </tr>
                          </thead>
                          <tbody>
                            {(entry.result_preview as Record<string, unknown>[]).map((row, ri) => (
                              <tr key={ri} className="hover:bg-slate-50">
                                {Object.values(row).map((val, vi) => (
                                  <td key={vi} className="px-2 py-1 text-slate-700 font-mono border-b border-slate-50">
                                    {String(val)}
                                  </td>
                                ))}
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    ) : null}
                  </div>
                )}
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}

function ChoicesPanel({
  question,
  onChoice,
  disabled,
}: {
  question: WriterQuestion;
  onChoice: (value: string) => void;
  disabled?: boolean;
}) {
  return (
    <div className="mt-3 p-4 bg-amber-50 border border-amber-200 rounded-xl">
      <div className="flex items-start gap-2 mb-3">
        <AlertTriangle size={15} className="text-amber-500 flex-shrink-0 mt-0.5" />
        <p className="text-sm font-medium text-amber-900">{question.text}</p>
      </div>
      <div className="flex flex-wrap gap-2">
        {question.choices.map((choice, i) => (
          <button
            key={i}
            onClick={() => !disabled && onChoice(choice.value)}
            disabled={disabled}
            className={clsx(
              'px-4 py-2 rounded-lg text-sm font-medium border transition-all duration-150',
              disabled
                ? 'bg-slate-100 text-slate-400 border-slate-200 cursor-not-allowed'
                : 'bg-white text-amber-800 border-amber-300 hover:bg-amber-500 hover:text-white hover:border-amber-500 cursor-pointer shadow-sm',
            )}
          >
            {choice.label}
          </button>
        ))}
      </div>
    </div>
  );
}

function SynthesisView({ synthesis }: { synthesis: Synthesis }) {
  const [activeSection, setActiveSection] = useState<string>('summary');

  const sections = [
    { id: 'summary', label: 'Résumé', icon: Star },
    { id: 'findings', label: 'Découvertes', icon: TrendingUp },
    { id: 'steps', label: 'Réflexion', icon: BookOpen },
    { id: 'recommendations', label: 'Recommandations', icon: Info },
    { id: 'tables', label: 'Tables créées', icon: Database },
  ] as const;

  return (
    <div className="mt-3 border border-violet-200 rounded-xl overflow-hidden">
      {/* Header */}
      <div className="px-4 py-3 bg-gradient-to-r from-violet-600 to-violet-800">
        <div className="flex items-center gap-2">
          <Star size={15} className="text-violet-200" />
          <h3 className="text-sm font-bold text-white">Synthèse finale de l'agent</h3>
        </div>
      </div>

      {/* Navigation tabs */}
      <div className="flex border-b border-violet-100 bg-violet-50 overflow-x-auto">
        {sections.map(({ id, label, icon: Icon }) => (
          <button
            key={id}
            onClick={() => setActiveSection(id)}
            className={clsx(
              'flex items-center gap-1.5 px-3 py-2 text-xs font-medium transition-colors whitespace-nowrap flex-shrink-0',
              activeSection === id
                ? 'text-violet-700 border-b-2 border-violet-500 bg-white'
                : 'text-slate-500 hover:text-violet-600',
            )}
          >
            <Icon size={11} />
            {label}
          </button>
        ))}
      </div>

      {/* Content */}
      <div className="p-4 bg-white">
        {activeSection === 'summary' && (
          <div className="space-y-3">
            <div className="p-4 bg-violet-50 rounded-xl border border-violet-100">
              <p className="text-sm text-violet-900 leading-relaxed font-medium">
                {synthesis.executive_summary}
              </p>
            </div>
            {synthesis.data_insights && (
              <div>
                <p className="text-[10px] font-bold text-slate-400 uppercase tracking-wide mb-2">
                  Insights analytiques
                </p>
                <p className="text-xs text-slate-700 leading-relaxed">{synthesis.data_insights}</p>
              </div>
            )}
            {synthesis.conclusion && (
              <div className="p-3 bg-slate-50 rounded-lg border border-slate-100">
                <p className="text-[10px] font-bold text-slate-500 uppercase tracking-wide mb-1">
                  Conclusion
                </p>
                <p className="text-xs text-slate-700 leading-relaxed">{synthesis.conclusion}</p>
              </div>
            )}
          </div>
        )}

        {activeSection === 'findings' && (
          <div className="space-y-2">
            {synthesis.key_findings?.length ? (
              synthesis.key_findings.map((finding, i) => (
                <div key={i} className="flex items-start gap-3 p-3 bg-slate-50 rounded-lg border border-slate-100">
                  <div className="flex-shrink-0 w-5 h-5 rounded-full bg-violet-500 flex items-center justify-center text-[9px] font-bold text-white mt-0.5">
                    {i + 1}
                  </div>
                  <p className="text-xs text-slate-700 leading-relaxed">{finding}</p>
                </div>
              ))
            ) : (
              <p className="text-xs text-slate-400 italic">Aucune découverte renseignée.</p>
            )}
          </div>
        )}

        {activeSection === 'steps' && (
          <div className="space-y-2">
            {synthesis.step_reflections?.length ? (
              synthesis.step_reflections.map((ref, i) => {
                const statusColors = {
                  success: 'bg-emerald-50 border-emerald-200',
                  failed: 'bg-red-50 border-red-200',
                  partial: 'bg-amber-50 border-amber-200',
                };
                const statusIcon = {
                  success: <CheckCircle2 size={12} className="text-emerald-500" />,
                  failed: <XCircle size={12} className="text-red-400" />,
                  partial: <AlertTriangle size={12} className="text-amber-500" />,
                };
                return (
                  <div key={i} className={clsx('p-3 rounded-lg border', statusColors[ref.status] || statusColors.success)}>
                    <div className="flex items-center gap-2 mb-1.5">
                      {statusIcon[ref.status] || statusIcon.success}
                      <span className="text-xs font-bold text-slate-700">
                        Étape {ref.step_id}: {ref.description}
                      </span>
                    </div>
                    <p className="text-xs text-slate-600 mb-1"><strong>Résultat:</strong> {ref.outcome}</p>
                    {ref.insight && (
                      <p className="text-[10px] text-slate-500 italic">{ref.insight}</p>
                    )}
                  </div>
                );
              })
            ) : (
              <p className="text-xs text-slate-400 italic">Aucune réflexion disponible.</p>
            )}
          </div>
        )}

        {activeSection === 'recommendations' && (
          <div className="space-y-2">
            {synthesis.recommendations?.length ? (
              synthesis.recommendations.map((rec, i) => (
                <div key={i} className="flex items-start gap-3 p-3 bg-blue-50 rounded-lg border border-blue-100">
                  <div className="flex-shrink-0 w-5 h-5 rounded-full bg-blue-500 flex items-center justify-center text-[9px] font-bold text-white mt-0.5">
                    {i + 1}
                  </div>
                  <p className="text-xs text-blue-900 leading-relaxed">{rec}</p>
                </div>
              ))
            ) : (
              <p className="text-xs text-slate-400 italic">Aucune recommandation disponible.</p>
            )}
          </div>
        )}

        {activeSection === 'tables' && (
          <div className="space-y-2">
            {synthesis.tables_created?.length ? (
              synthesis.tables_created.map((tbl, i) => (
                <div key={i} className="p-3 bg-violet-50 rounded-lg border border-violet-100">
                  <div className="flex items-center gap-2 mb-1">
                    <Table2 size={12} className="text-violet-600" />
                    <span className="text-xs font-bold font-mono text-violet-800">{tbl.name}</span>
                  </div>
                  <p className="text-xs text-slate-600 mb-0.5"><strong>Contenu:</strong> {tbl.purpose}</p>
                  {tbl.useful_for && (
                    <p className="text-[10px] text-slate-500 italic">Utile pour: {tbl.useful_for}</p>
                  )}
                </div>
              ))
            ) : (
              <p className="text-xs text-slate-400 italic">Aucune table temporaire créée.</p>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

function ReplanBadge({ replanLog }: { replanLog: ReplanEntry[] }) {
  if (!replanLog?.length) return null;
  const replanned = replanLog.filter(r => r.should_replan);
  return (
    <div className="mt-2 flex flex-wrap gap-1.5">
      {replanLog.map((r, i) => (
        <div
          key={i}
          className={clsx(
            'flex items-center gap-1 px-2 py-1 rounded-full text-[10px] font-medium border',
            r.should_replan
              ? 'bg-amber-50 border-amber-200 text-amber-700'
              : 'bg-slate-50 border-slate-200 text-slate-500',
          )}
        >
          <RotateCcw size={9} />
          Contrôle {i + 1}{r.should_replan ? ' → réévalué' : ' → OK'}
        </div>
      ))}
      {replanned.length > 0 && (
        <div className="flex items-center gap-1 px-2 py-1 rounded-full text-[10px] font-medium bg-violet-50 border border-violet-200 text-violet-700">
          <Zap size={9} />
          {replanned.length} réévaluation{replanned.length > 1 ? 's' : ''}
        </div>
      )}
    </div>
  );
}

function WriterMessageView({
  msg,
  onChoice,
  isLast,
}: {
  msg: ChatMessage;
  onChoice: (value: string) => void;
  isLast: boolean;
}) {
  const hasWriter = msg.plan || msg.action_log || msg.synthesis;
  if (!hasWriter) return null;

  return (
    <div className="mt-2 space-y-1">
      {/* Progress bar */}
      {msg.action_count !== undefined && msg.plan && (
        <div className="flex items-center gap-2 text-xs text-slate-500">
          <div className="flex-1 bg-slate-100 rounded-full h-1.5 overflow-hidden">
            <div
              className="h-full bg-gradient-to-r from-violet-400 to-violet-600 rounded-full transition-all duration-500"
              style={{ width: `${Math.min(100, (msg.action_count / (msg.plan.steps?.length || 1)) * 100)}%` }}
            />
          </div>
          <span className="flex-shrink-0 font-medium text-violet-600">
            {msg.action_count}/{msg.plan.steps?.length ?? '?'} étapes
            {msg.remaining_credits !== undefined && (
              <span className="text-slate-400 ml-1">· {msg.remaining_credits} crédits restants</span>
            )}
          </span>
        </div>
      )}

      {/* Plan */}
      {msg.plan && (
        <PlanView plan={msg.plan} actionCount={msg.action_count} />
      )}

      {/* Replan log */}
      {msg.replan_log && msg.replan_log.length > 0 && (
        <ReplanBadge replanLog={msg.replan_log} />
      )}

      {/* Action log */}
      {msg.action_log && msg.action_log.length > 0 && (
        <ActionLogView log={msg.action_log} />
      )}

      {/* Synthesis */}
      {msg.synthesis && <SynthesisView synthesis={msg.synthesis} />}

      {/* Cleanup result */}
      {msg.cleanup_done === true && msg.tables_dropped && (
        <div className="flex items-center gap-2 p-3 bg-red-50 border border-red-200 rounded-lg">
          <Trash2 size={13} className="text-red-500" />
          <span className="text-xs text-red-700 font-medium">
            Tables supprimées: {msg.tables_dropped.join(', ')}
          </span>
        </div>
      )}
      {msg.cleanup_done === false && msg.created_tables && msg.created_tables.length > 0 && (
        <div className="flex items-center gap-2 p-3 bg-emerald-50 border border-emerald-200 rounded-lg">
          <Database size={13} className="text-emerald-600" />
          <span className="text-xs text-emerald-700 font-medium">
            Tables conservées: {msg.created_tables.join(', ')}
          </span>
        </div>
      )}

      {/* Question / choices — only on the last message */}
      {msg.question && isLast && (
        <ChoicesPanel
          question={msg.question}
          onChoice={onChoice}
          disabled={!isLast}
        />
      )}
    </div>
  );
}

// ── Generic AssistantMessage ────────────────────────────────────────────────

function AssistantMessage({
  msg,
  onChoice,
  isLast,
}: {
  msg: ChatMessage;
  onChoice: (value: string) => void;
  isLast: boolean;
}) {
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

  const isWriter = !!(msg.plan || msg.action_log || msg.synthesis
    || msg.question || msg.status);

  return (
    <div className="flex gap-3 justify-start">
      <div className={clsx(
        'p-2 rounded-full flex-shrink-0 self-start mt-1',
        isWriter ? 'bg-violet-100' : 'bg-emerald-100',
      )}>
        {isWriter
          ? <Zap size={14} className="text-violet-600" />
          : <Cpu size={14} className="text-emerald-600" />}
      </div>
      <div className="flex-1 max-w-full overflow-hidden">
        <div className={clsx(
          'border rounded-xl px-4 py-3 shadow-sm',
          isWriter ? 'bg-white border-violet-100' : 'bg-white border-slate-200',
        )}>
          <p className="text-sm text-slate-700">{msg.content}</p>
          {/* Data dictionary summary */}
          {msg.tables_processed !== undefined && (
            <div className="flex items-center gap-2 mt-2">
              <CheckCircle2 size={13} className="text-emerald-500" />
              <span className="text-xs text-emerald-700 font-medium">
                {msg.tables_processed}/{msg.total_tables} table{(msg.total_tables ?? 0) > 1 ? 's' : ''} documentée{(msg.tables_processed ?? 0) > 1 ? 's' : ''}
              </span>
            </div>
          )}
        </div>

        {/* Data dictionary views */}
        {msg.steps && msg.steps.length > 0 && <StepsPanel steps={msg.steps} />}
        {msg.data_dictionary && msg.data_dictionary.length > 0 && (
          <DictionaryOutputPanel entries={msg.data_dictionary} />
        )}

        {/* Writer agent views */}
        {isWriter && (
          <WriterMessageView msg={msg} onChoice={onChoice} isLast={isLast} />
        )}
      </div>
    </div>
  );
}

// ── Main Component ──────────────────────────────────────────────────────────

export function AgentsPane() {
  const [agents, setAgents] = useState<Agent[]>([]);
  const [loadingAgents, setLoadingAgents] = useState(true);
  const [selectedAgent, setSelectedAgent] = useState<Agent | null>(null);
  const [params, setParams] = useState<Record<string, string | number>>({});
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const [showParams, setShowParams] = useState(true);
  const [sessionId, setSessionId] = useState<string | null>(null);
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
    setSessionId(null);
    const defaults: Record<string, string | number> = {};
    agent.parameters.forEach(p => { defaults[p.name] = p.default; });
    setParams(defaults);
  }

  function setParam(name: string, value: string | number) {
    setParams(prev => ({ ...prev, [name]: value }));
  }

  async function sendMessage(overrideContent?: string) {
    const text = (overrideContent ?? input).trim();
    if (!text || !selectedAgent || loading) return;

    const userMsg: ChatMessage = { role: 'user', content: text };
    setMessages(prev => [...prev, userMsg]);
    if (!overrideContent) setInput('');
    setLoading(true);

    try {
      const allMsgs = [...messages, userMsg];
      const body: Record<string, unknown> = {
        messages: allMsgs.map(m => ({ role: m.role, content: m.content })),
        params,
      };
      if (sessionId) body.session_id = sessionId;

      const res = await fetch(`/api/agents/${selectedAgent.id}/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      const data = await res.json();

      // Save session id for writer agent
      if (data.session_id) setSessionId(data.session_id);

      if (data.error) {
        setMessages(prev => [...prev, { role: 'assistant', content: data.error, error: data.error }]);
      } else if (selectedAgent.id === 'clickhouse-writer') {
        // Writer agent: rich message
        setMessages(prev => [...prev, {
          role: 'assistant',
          content: data.content ?? 'Opération terminée.',
          ...data,
        }]);
      } else {
        // Data dictionary agent
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

  function handleChoice(value: string) {
    sendMessage(value);
  }

  function handleKey(e: KeyboardEvent<HTMLTextAreaElement>) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      sendMessage();
    }
  }

  const isWriter = selectedAgent?.id === 'clickhouse-writer';

  // ── Render ────────────────────────────────────────────────────────────

  return (
    <div className="flex h-full bg-slate-50 overflow-hidden">

      {/* ── Left panel: agent list ──────────────────────────────────────── */}
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

      {/* ── Right panel: chat ───────────────────────────────────────────── */}
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
          <div className={clsx(
            'bg-white border-b px-6 py-3 flex items-center gap-3 flex-shrink-0',
            isWriter ? 'border-violet-100' : 'border-slate-200',
          )}>
            <div className={clsx('p-2 rounded-lg', isWriter ? 'bg-violet-600' : 'bg-emerald-500')}>
              {isWriter ? <Zap size={16} className="text-white" /> : <Cpu size={16} className="text-white" />}
            </div>
            <div className="flex-1 min-w-0">
              <h2 className="text-sm font-bold text-slate-800">{selectedAgent.name}</h2>
              <p className="text-xs text-slate-400 truncate">{selectedAgent.description}</p>
            </div>
            {isWriter && sessionId && (
              <span className="px-2 py-1 bg-violet-50 border border-violet-200 rounded-full text-[9px] font-mono text-violet-600">
                Session active
              </span>
            )}
            <button
              onClick={() => { setMessages([]); setSessionId(null); }}
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
                {isWriter ? (
                  <>
                    <div className="p-4 bg-violet-50 rounded-2xl mb-4">
                      <Zap size={36} className="text-violet-400" />
                    </div>
                    <p className="text-sm text-slate-600 font-semibold mb-1">
                      Agent ClickHouse Writer
                    </p>
                    <p className="text-xs text-slate-400 max-w-sm leading-relaxed mb-4">
                      Décrivez votre besoin en langage naturel. L'agent planifiera et exécutera
                      automatiquement jusqu'à 12 opérations complexes, créera des tables
                      intermédiaires si nécessaire, et produira une synthèse détaillée.
                    </p>
                    <div className="flex flex-wrap gap-2 justify-center">
                      {[
                        'Analyse les 10 tables les plus volumineuses et synthétise leur contenu',
                        'Crée une table agrégée BOT_ avec les métriques clés de ma base',
                        'Identifie les doublons dans toutes les tables et produis un rapport',
                        'Calcule la distribution des valeurs pour chaque colonne numérique',
                      ].map(s => (
                        <button
                          key={s}
                          onClick={() => setInput(s)}
                          className="px-3 py-1.5 text-xs bg-white border border-violet-200 rounded-full text-violet-700 hover:border-violet-400 hover:bg-violet-50 transition-colors text-left"
                        >
                          {s}
                        </button>
                      ))}
                    </div>
                  </>
                ) : (
                  <>
                    <MessageSquare size={32} className="text-slate-300 mb-3" />
                    <p className="text-sm text-slate-400 font-medium">Démarrez la conversation</p>
                    <p className="text-xs text-slate-400 mt-1 max-w-xs">
                      Tapez votre demande ci-dessous ou utilisez les suggestions de démarrage.
                    </p>
                    <div className="mt-4 flex flex-wrap gap-2 justify-center">
                      {[
                        'Génère le dictionnaire de données complet',
                        'Documente toutes les tables disponibles',
                        'Analyse et décris le schéma de la base',
                      ].map(s => (
                        <button
                          key={s}
                          onClick={() => setInput(s)}
                          className="px-3 py-1.5 text-xs bg-white border border-slate-200 rounded-full text-slate-600 hover:border-emerald-400 hover:text-emerald-600 transition-colors"
                        >
                          {s}
                        </button>
                      ))}
                    </div>
                  </>
                )}
              </div>
            )}

            {messages.map((msg, i) => (
              <div key={i}>
                {msg.role === 'user' ? (
                  <div className="flex justify-end">
                    <div className={clsx(
                      'text-white rounded-xl px-4 py-2.5 max-w-md text-sm',
                      isWriter ? 'bg-violet-600' : 'bg-emerald-500',
                    )}>
                      {msg.content}
                    </div>
                  </div>
                ) : (
                  <AssistantMessage
                    msg={msg}
                    onChoice={handleChoice}
                    isLast={i === messages.length - 1}
                  />
                )}
              </div>
            ))}

            {loading && (
              <div className="flex gap-3 justify-start">
                <div className={clsx(
                  'p-2 rounded-full flex-shrink-0',
                  isWriter ? 'bg-violet-100' : 'bg-emerald-100',
                )}>
                  <Loader2 size={14} className={clsx(
                    'animate-spin',
                    isWriter ? 'text-violet-600' : 'text-emerald-600',
                  )} />
                </div>
                <div className="bg-white border border-slate-200 rounded-xl px-4 py-3 shadow-sm">
                  <div className="flex items-center gap-2 text-sm text-slate-400">
                    <span>{isWriter ? "L'agent planifie et exécute" : 'Analyse en cours'}</span>
                    <span className="flex gap-0.5">
                      {[0, 150, 300].map(d => (
                        <span
                          key={d}
                          className={clsx(
                            'w-1 h-1 rounded-full animate-bounce',
                            isWriter ? 'bg-violet-400' : 'bg-emerald-400',
                          )}
                          style={{ animationDelay: `${d}ms` }}
                        />
                      ))}
                    </span>
                  </div>
                  {isWriter && (
                    <p className="text-[10px] text-slate-300 mt-1">
                      Cela peut prendre quelques instants selon la complexité…
                    </p>
                  )}
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
                placeholder={
                  isWriter
                    ? 'Décrivez votre analyse en langage naturel…'
                    : `Interrogez l'agent "${selectedAgent.name}"…`
                }
                rows={2}
                disabled={loading}
                className={clsx(
                  'flex-1 resize-none px-4 py-3 text-sm border rounded-xl bg-slate-50 focus:outline-none focus:ring-2 focus:border-transparent disabled:opacity-50 transition-colors',
                  isWriter
                    ? 'border-violet-200 focus:ring-violet-400'
                    : 'border-slate-200 focus:ring-emerald-400',
                )}
              />
              <button
                onClick={() => sendMessage()}
                disabled={!input.trim() || loading}
                className={clsx(
                  'p-3 disabled:bg-slate-200 disabled:cursor-not-allowed text-white rounded-xl transition-colors flex-shrink-0',
                  isWriter
                    ? 'bg-violet-600 hover:bg-violet-700'
                    : 'bg-emerald-500 hover:bg-emerald-600',
                )}
              >
                {loading ? <Loader2 size={18} className="animate-spin" /> : <Send size={18} />}
              </button>
            </div>
            <p className="text-[10px] text-slate-400 mt-1.5 ml-1">
              Entrée pour envoyer · Maj+Entrée pour sauter une ligne
              {isWriter && sessionId && (
                <span className="ml-2 text-violet-400 font-medium">
                  · Session en cours
                </span>
              )}
            </p>
          </div>
        </div>
      )}
    </div>
  );
}
