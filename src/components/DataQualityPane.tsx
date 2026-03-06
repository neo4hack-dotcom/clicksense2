import { useState } from 'react';
import {
  ShieldCheck, ChevronDown, ChevronRight, AlertTriangle,
  Info, AlertCircle, Loader2, BarChart3, CheckCircle2,
  Search, X, RefreshCw, Table2
} from 'lucide-react';
import { useAppStore } from '../store';
import { motion, AnimatePresence } from 'motion/react';
import clsx from 'clsx';

// ── Types ──────────────────────────────────────────────────────────────────

interface DQIssue {
  severity: 'critical' | 'warning' | 'info';
  category: string;
  title: string;
  description: string;
  affected_rows: number | null;
  recommendation: string;
}

interface DQColumnResult {
  column: string;
  quality_score: number;
  issues: DQIssue[];
  insights: string;
}

interface DQAnalysis {
  summary: string;
  quality_score: number | null;
  columns: DQColumnResult[];
  recommendations: string[];
}

interface ColumnStat {
  column: string;
  type: string;
  total?: number;
  null_count?: number;
  null_pct?: number;
  distinct_count?: number;
  distinct_pct?: number;
  empty_count?: number;
  empty_pct?: number;
  min?: number;
  max?: number;
  avg?: number;
  stddev?: number;
  p25?: number;
  p50?: number;
  p75?: number;
  outlier_count?: number;
  outlier_pct?: number;
  negative_count?: number;
  min_length?: number;
  max_length?: number;
  avg_length?: number;
  sentinel_count?: number;
  min_date?: string;
  max_date?: string;
  future_count?: number;
  top_values?: { value: string; count: number }[];
  query_error?: string;
}

interface DQResult {
  table: string;
  sample_size: number;
  column_stats: ColumnStat[];
  analysis: DQAnalysis;
}

// ── Helpers ────────────────────────────────────────────────────────────────

const severityConfig = {
  critical: { icon: AlertCircle, color: 'text-red-600', bg: 'bg-red-50 border-red-200', badge: 'bg-red-100 text-red-700' },
  warning:  { icon: AlertTriangle, color: 'text-amber-600', bg: 'bg-amber-50 border-amber-200', badge: 'bg-amber-100 text-amber-700' },
  info:     { icon: Info, color: 'text-blue-500', bg: 'bg-blue-50 border-blue-200', badge: 'bg-blue-100 text-blue-700' },
};

function QualityBadge({ score }: { score: number | null }) {
  if (score === null) return null;
  const color = score >= 80 ? 'text-emerald-700 bg-emerald-50 border-emerald-200'
    : score >= 60 ? 'text-amber-700 bg-amber-50 border-amber-200'
    : 'text-red-700 bg-red-50 border-red-200';
  return (
    <span className={clsx('text-sm font-bold px-3 py-1 rounded-full border', color)}>
      {score}/100
    </span>
  );
}

function StatPill({ label, value, accent = false }: { label: string; value: string | number | null | undefined; accent?: boolean }) {
  if (value === null || value === undefined) return null;
  return (
    <div className={clsx('rounded-lg px-3 py-1.5 text-xs', accent ? 'bg-amber-50 border border-amber-200' : 'bg-slate-50 border border-slate-200')}>
      <span className="text-slate-500">{label}: </span>
      <span className={clsx('font-semibold', accent ? 'text-amber-700' : 'text-slate-700')}>{value}</span>
    </div>
  );
}

function ColumnStatCard({ stat }: { stat: ColumnStat }) {
  const isNumeric = ['INT', 'FLOAT', 'DECIMAL', 'DOUBLE'].some(t => stat.type.toUpperCase().includes(t));
  const isString = ['STRING', 'VARCHAR', 'FIXEDSTRING', 'TEXT'].some(t => stat.type.toUpperCase().includes(t));
  const isDate = ['DATE', 'DATETIME'].some(t => stat.type.toUpperCase().includes(t));

  return (
    <div className="bg-slate-50 rounded-xl p-4 space-y-3">
      <div className="flex items-center gap-2 flex-wrap">
        <StatPill label="Total" value={stat.total?.toLocaleString()} />
        <StatPill label="Nulls" value={stat.null_pct !== undefined ? `${stat.null_pct}%` : undefined} accent={(stat.null_pct ?? 0) > 5} />
        <StatPill label="Distinct" value={stat.distinct_count?.toLocaleString()} />
        <StatPill label="Distinct %" value={stat.distinct_pct !== undefined ? `${stat.distinct_pct}%` : undefined} />
        {isString && (
          <>
            <StatPill label="Empty" value={stat.empty_pct !== undefined ? `${stat.empty_pct}%` : undefined} accent={(stat.empty_pct ?? 0) > 1} />
            <StatPill label="Avg len" value={stat.avg_length} />
            <StatPill label="Max len" value={stat.max_length} />
            {(stat.sentinel_count ?? 0) > 0 && <StatPill label="Sentinels" value={stat.sentinel_count} accent />}
          </>
        )}
        {isNumeric && (
          <>
            <StatPill label="Min" value={stat.min} />
            <StatPill label="Max" value={stat.max} />
            <StatPill label="Avg" value={stat.avg !== undefined ? Number(stat.avg).toFixed(2) : undefined} />
            <StatPill label="Stddev" value={stat.stddev !== undefined ? Number(stat.stddev).toFixed(2) : undefined} />
            <StatPill label="Outliers" value={stat.outlier_count !== undefined ? `${stat.outlier_count} (${stat.outlier_pct}%)` : undefined} accent={(stat.outlier_pct ?? 0) > 5} />
          </>
        )}
        {isDate && (
          <>
            <StatPill label="Min date" value={stat.min_date} />
            <StatPill label="Max date" value={stat.max_date} />
            <StatPill label="Future" value={stat.future_count} accent={(stat.future_count ?? 0) > 0} />
          </>
        )}
      </div>

      {/* Top values */}
      {stat.top_values && stat.top_values.length > 0 && (
        <div>
          <p className="text-xs font-semibold text-slate-500 uppercase tracking-wide mb-1.5">Top values</p>
          <div className="flex flex-wrap gap-1.5">
            {stat.top_values.slice(0, 8).map((tv, i) => (
              <span key={i} className="bg-white border border-slate-200 rounded-lg px-2 py-0.5 text-xs text-slate-600">
                <span className="font-mono">{tv.value || <em className="text-slate-400">empty</em>}</span>
                <span className="text-slate-400 ml-1">× {tv.count.toLocaleString()}</span>
              </span>
            ))}
          </div>
        </div>
      )}

      {stat.query_error && (
        <p className="text-xs text-red-600 bg-red-50 rounded-lg px-3 py-2">Query error: {stat.query_error}</p>
      )}
    </div>
  );
}

// ── Main component ─────────────────────────────────────────────────────────

export function DataQualityPane() {
  const { schema } = useAppStore();

  const [selectedTable, setSelectedTable] = useState('');
  const [selectedColumns, setSelectedColumns] = useState<string[]>([]);
  const [sampleSize, setSampleSize] = useState(50000);
  const [tableSearch, setTableSearch] = useState('');
  const [isAnalyzing, setIsAnalyzing] = useState(false);
  const [result, setResult] = useState<DQResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [expandedColumns, setExpandedColumns] = useState<Set<string>>(new Set());

  const allTables = Object.keys(schema).sort();
  const filteredTables = tableSearch.trim()
    ? allTables.filter(t => t.toLowerCase().includes(tableSearch.toLowerCase()))
    : allTables;

  const tableColumns = selectedTable ? (schema[selectedTable] ?? []) : [];

  const toggleColumn = (col: string) => {
    setSelectedColumns(prev =>
      prev.includes(col) ? prev.filter(c => c !== col) : [...prev, col]
    );
  };

  const selectAllColumns = () => {
    setSelectedColumns(tableColumns.map(c => c.name));
  };

  const toggleExpandedColumn = (col: string) => {
    setExpandedColumns(prev => {
      const next = new Set(prev);
      if (next.has(col)) next.delete(col); else next.add(col);
      return next;
    });
  };

  const handleAnalyze = async () => {
    if (!selectedTable || selectedColumns.length === 0) return;
    setIsAnalyzing(true);
    setError(null);
    setResult(null);
    setExpandedColumns(new Set());

    try {
      const res = await fetch('/api/data-quality/analyze', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ table: selectedTable, columns: selectedColumns, sample_size: sampleSize }),
      });
      const data = await res.json();
      if (data.error) throw new Error(data.error);
      setResult(data);
      // auto-expand all columns
      setExpandedColumns(new Set(selectedColumns));
    } catch (e: any) {
      setError(e.message);
    } finally {
      setIsAnalyzing(false);
    }
  };

  const colResultMap: Record<string, DQColumnResult> = {};
  result?.analysis?.columns?.forEach(c => { colResultMap[c.column] = c; });

  const statMap: Record<string, ColumnStat> = {};
  result?.column_stats?.forEach(s => { statMap[s.column] = s; });

  const issueCount = result?.analysis?.columns?.reduce((acc, c) => acc + c.issues.length, 0) ?? 0;
  const criticalCount = result?.analysis?.columns?.reduce(
    (acc, c) => acc + c.issues.filter(i => i.severity === 'critical').length, 0
  ) ?? 0;

  return (
    <div className="flex h-full overflow-hidden">
      {/* ── Left panel: configuration ──────────────────────────────────────── */}
      <div className="w-80 border-r border-slate-200 bg-white flex flex-col shrink-0">
        <div className="p-5 border-b border-slate-200">
          <div className="flex items-center gap-2.5 mb-1">
            <div className="bg-violet-100 p-1.5 rounded-lg text-violet-600">
              <ShieldCheck size={17} />
            </div>
            <h2 className="font-bold text-slate-800 text-base">AI Data Quality</h2>
          </div>
          <p className="text-xs text-slate-500">
            Statistical analysis + LLM anomaly detection for format, content, nulls and cardinality issues.
          </p>
        </div>

        <div className="flex-1 overflow-y-auto p-4 space-y-5">
          {/* Table picker */}
          <div className="space-y-2">
            <label className="text-xs font-semibold text-slate-600 uppercase tracking-wide">Table</label>
            <div className="relative">
              <Search size={13} className="absolute left-2.5 top-1/2 -translate-y-1/2 text-slate-400" />
              <input
                type="text"
                value={tableSearch}
                onChange={e => setTableSearch(e.target.value)}
                placeholder="Search tables…"
                className="w-full pl-8 pr-3 py-2 text-xs bg-slate-50 border border-slate-200 rounded-lg focus:ring-2 focus:ring-violet-500/20 focus:border-violet-500 transition-all"
              />
            </div>
            <div className="max-h-48 overflow-y-auto space-y-0.5 border border-slate-200 rounded-xl bg-white">
              {allTables.length === 0 ? (
                <p className="text-xs text-slate-400 p-3 text-center">No tables. Configure ClickHouse first.</p>
              ) : filteredTables.length === 0 ? (
                <p className="text-xs text-slate-400 p-3 text-center">No match.</p>
              ) : (
                filteredTables.map(t => (
                  <button
                    key={t}
                    onClick={() => { setSelectedTable(t); setSelectedColumns([]); setResult(null); }}
                    className={clsx(
                      'w-full text-left flex items-center gap-2 px-3 py-2 text-xs transition-colors',
                      selectedTable === t
                        ? 'bg-violet-50 text-violet-700 font-semibold'
                        : 'text-slate-700 hover:bg-slate-50'
                    )}
                  >
                    <Table2 size={12} className="shrink-0 text-slate-400" />
                    <span className="font-mono truncate">{t}</span>
                  </button>
                ))
              )}
            </div>
          </div>

          {/* Column picker */}
          {selectedTable && (
            <div className="space-y-2">
              <div className="flex items-center justify-between">
                <label className="text-xs font-semibold text-slate-600 uppercase tracking-wide">
                  Columns ({selectedColumns.length} selected)
                </label>
                <div className="flex gap-1">
                  <button onClick={selectAllColumns} className="text-xs text-violet-600 hover:underline">All</button>
                  <span className="text-slate-300">·</span>
                  <button onClick={() => setSelectedColumns([])} className="text-xs text-slate-500 hover:underline">None</button>
                </div>
              </div>
              <div className="max-h-56 overflow-y-auto border border-slate-200 rounded-xl bg-white divide-y divide-slate-50">
                {tableColumns.map(col => (
                  <button
                    key={col.name}
                    onClick={() => toggleColumn(col.name)}
                    className={clsx(
                      'w-full text-left flex items-center gap-2 px-3 py-2 text-xs transition-colors',
                      selectedColumns.includes(col.name) ? 'bg-violet-50' : 'hover:bg-slate-50'
                    )}
                  >
                    <div className={clsx(
                      'w-3.5 h-3.5 rounded border-2 shrink-0 flex items-center justify-center',
                      selectedColumns.includes(col.name) ? 'bg-violet-500 border-violet-500' : 'border-slate-300'
                    )}>
                      {selectedColumns.includes(col.name) && <CheckCircle2 size={9} className="text-white" />}
                    </div>
                    <span className={clsx('font-mono truncate', selectedColumns.includes(col.name) ? 'text-violet-700 font-semibold' : 'text-slate-700')}>{col.name}</span>
                    <span className="ml-auto text-slate-400 font-mono text-[10px] shrink-0">{col.type}</span>
                  </button>
                ))}
              </div>
            </div>
          )}

          {/* Sample size */}
          {selectedTable && (
            <div className="space-y-1.5">
              <label className="text-xs font-semibold text-slate-600 uppercase tracking-wide">Sample Size</label>
              <select
                value={sampleSize}
                onChange={e => setSampleSize(Number(e.target.value))}
                className="w-full text-xs bg-slate-50 border border-slate-200 rounded-lg px-3 py-2 focus:ring-2 focus:ring-violet-500/20 focus:border-violet-500 transition-all"
              >
                <option value={10000}>10 000 rows</option>
                <option value={50000}>50 000 rows</option>
                <option value={100000}>100 000 rows</option>
                <option value={500000}>500 000 rows</option>
              </select>
            </div>
          )}
        </div>

        {/* Analyze button */}
        <div className="p-4 border-t border-slate-200">
          <button
            onClick={handleAnalyze}
            disabled={!selectedTable || selectedColumns.length === 0 || isAnalyzing}
            className="w-full flex items-center justify-center gap-2 bg-violet-600 hover:bg-violet-700 disabled:opacity-50 text-white py-3 rounded-xl text-sm font-semibold transition-colors shadow-sm"
          >
            {isAnalyzing ? (
              <><Loader2 size={16} className="animate-spin" /> Analyzing…</>
            ) : (
              <><BarChart3 size={16} /> Analyze Data Quality</>
            )}
          </button>
          {selectedColumns.length > 0 && !isAnalyzing && (
            <p className="text-center text-xs text-slate-400 mt-2">
              {selectedColumns.length} column{selectedColumns.length > 1 ? 's' : ''} · {sampleSize.toLocaleString()} rows
            </p>
          )}
        </div>
      </div>

      {/* ── Right panel: results ───────────────────────────────────────────── */}
      <div className="flex-1 overflow-y-auto bg-slate-50">
        {/* Empty state */}
        {!isAnalyzing && !result && !error && (
          <div className="flex flex-col items-center justify-center h-full text-center space-y-4 p-8">
            <div className="w-16 h-16 bg-violet-100 rounded-full flex items-center justify-center text-violet-500">
              <ShieldCheck size={28} />
            </div>
            <div>
              <h3 className="text-lg font-semibold text-slate-800 mb-1">AI Data Quality Analysis</h3>
              <p className="text-slate-500 text-sm max-w-md">
                Select a table and columns on the left, then click <strong>Analyze</strong>.
                The LLM will detect format anomalies, outliers, null issues, cardinality problems and more.
              </p>
            </div>
            <div className="grid grid-cols-3 gap-3 max-w-lg text-left mt-2">
              {[
                { icon: AlertCircle, color: 'text-red-500 bg-red-50', label: 'Format & Content', desc: 'Invalid emails, wrong patterns, impossible values' },
                { icon: AlertTriangle, color: 'text-amber-500 bg-amber-50', label: 'Nulls & Sentinels', desc: 'High null rates, "N/A", -1, empty string abuse' },
                { icon: BarChart3, color: 'text-blue-500 bg-blue-50', label: 'Distributions', desc: 'Outliers (IQR), skew, cardinality anomalies' },
              ].map(({ icon: Icon, color, label, desc }) => (
                <div key={label} className="bg-white rounded-xl p-3 border border-slate-200 text-xs space-y-1.5">
                  <div className={clsx('w-8 h-8 rounded-lg flex items-center justify-center', color)}>
                    <Icon size={16} />
                  </div>
                  <p className="font-semibold text-slate-700">{label}</p>
                  <p className="text-slate-400 leading-snug">{desc}</p>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Loading */}
        {isAnalyzing && (
          <div className="flex flex-col items-center justify-center h-full space-y-4">
            <Loader2 size={36} className="animate-spin text-violet-500" />
            <div className="text-center">
              <p className="font-semibold text-slate-700">Running analysis…</p>
              <p className="text-xs text-slate-500 mt-1">Collecting statistics + calling LLM for anomaly detection</p>
            </div>
          </div>
        )}

        {/* Error */}
        {error && (
          <div className="p-8">
            <div className="bg-red-50 border border-red-200 rounded-2xl p-5 flex items-start gap-3">
              <AlertCircle size={20} className="text-red-500 shrink-0 mt-0.5" />
              <div>
                <p className="font-semibold text-red-700 mb-1">Analysis failed</p>
                <p className="text-sm text-red-600">{error}</p>
                <button onClick={() => setError(null)} className="mt-3 text-xs text-red-500 hover:text-red-700 flex items-center gap-1">
                  <X size={12} /> Dismiss
                </button>
              </div>
            </div>
          </div>
        )}

        {/* Results */}
        {result && !isAnalyzing && (
          <div className="p-6 space-y-6 max-w-5xl mx-auto">
            {/* Summary header */}
            <div className="bg-white rounded-2xl border border-slate-200 shadow-sm p-6">
              <div className="flex items-start justify-between gap-4 mb-4">
                <div className="flex-1">
                  <div className="flex items-center gap-2 mb-2">
                    <h3 className="text-lg font-bold text-slate-800">
                      Quality Report — <span className="font-mono text-violet-600">{result.table}</span>
                    </h3>
                    <QualityBadge score={result.analysis.quality_score} />
                  </div>
                  <p className="text-sm text-slate-600 leading-relaxed">{result.analysis.summary}</p>
                </div>
                <button
                  onClick={handleAnalyze}
                  className="shrink-0 flex items-center gap-1.5 text-xs text-slate-500 hover:text-violet-600 border border-slate-200 hover:border-violet-300 px-3 py-1.5 rounded-lg transition-colors"
                >
                  <RefreshCw size={12} /> Re-analyze
                </button>
              </div>

              {/* Quick stats */}
              <div className="flex gap-4 flex-wrap">
                <div className="bg-slate-50 rounded-xl px-4 py-2 text-center">
                  <p className="text-xs text-slate-500">Sample</p>
                  <p className="text-sm font-bold text-slate-800">{result.sample_size.toLocaleString()}</p>
                </div>
                <div className="bg-slate-50 rounded-xl px-4 py-2 text-center">
                  <p className="text-xs text-slate-500">Columns</p>
                  <p className="text-sm font-bold text-slate-800">{result.column_stats.length}</p>
                </div>
                <div className={clsx('rounded-xl px-4 py-2 text-center', criticalCount > 0 ? 'bg-red-50' : 'bg-slate-50')}>
                  <p className="text-xs text-slate-500">Critical issues</p>
                  <p className={clsx('text-sm font-bold', criticalCount > 0 ? 'text-red-600' : 'text-slate-800')}>{criticalCount}</p>
                </div>
                <div className={clsx('rounded-xl px-4 py-2 text-center', issueCount > 0 ? 'bg-amber-50' : 'bg-slate-50')}>
                  <p className="text-xs text-slate-500">Total issues</p>
                  <p className={clsx('text-sm font-bold', issueCount > 0 ? 'text-amber-600' : 'text-slate-800')}>{issueCount}</p>
                </div>
              </div>
            </div>

            {/* Global recommendations */}
            {result.analysis.recommendations?.length > 0 && (
              <div className="bg-violet-50 border border-violet-200 rounded-2xl p-4">
                <h4 className="text-sm font-semibold text-violet-700 mb-2">Global Recommendations</h4>
                <ul className="space-y-1.5">
                  {result.analysis.recommendations.map((rec, i) => (
                    <li key={i} className="flex items-start gap-2 text-sm text-violet-700">
                      <CheckCircle2 size={14} className="shrink-0 mt-0.5 text-violet-500" />
                      {rec}
                    </li>
                  ))}
                </ul>
              </div>
            )}

            {/* Per-column results */}
            <div className="space-y-4">
              {result.column_stats.map(stat => {
                const colResult = colResultMap[stat.column];
                const isExpanded = expandedColumns.has(stat.column);
                const issues = colResult?.issues ?? [];
                const hasIssues = issues.length > 0;
                const worstSeverity = issues.find(i => i.severity === 'critical') ? 'critical'
                  : issues.find(i => i.severity === 'warning') ? 'warning'
                  : issues.length > 0 ? 'info' : null;

                return (
                  <div key={stat.column} className="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden">
                    {/* Column header */}
                    <button
                      className="w-full p-4 flex items-center justify-between hover:bg-slate-50 transition-colors text-left"
                      onClick={() => toggleExpandedColumn(stat.column)}
                    >
                      <div className="flex items-center gap-3 flex-1 min-w-0">
                        {isExpanded ? <ChevronDown size={16} className="text-slate-400 shrink-0" /> : <ChevronRight size={16} className="text-slate-400 shrink-0" />}
                        <div className="min-w-0">
                          <div className="flex items-center gap-2 flex-wrap">
                            <span className="font-semibold text-slate-800 font-mono">{stat.column}</span>
                            <span className="text-xs text-slate-400 bg-slate-100 px-2 py-0.5 rounded font-mono">{stat.type}</span>
                            {colResult && <QualityBadge score={colResult.quality_score} />}
                            {worstSeverity && (
                              <span className={clsx('text-xs px-2 py-0.5 rounded-full font-medium', severityConfig[worstSeverity].badge)}>
                                {issues.length} issue{issues.length > 1 ? 's' : ''}
                              </span>
                            )}
                            {!hasIssues && colResult && (
                              <span className="text-xs bg-emerald-100 text-emerald-700 px-2 py-0.5 rounded-full font-medium">
                                No issues
                              </span>
                            )}
                          </div>
                          {!isExpanded && stat.null_pct !== undefined && (
                            <p className="text-xs text-slate-500 mt-0.5">
                              {stat.total?.toLocaleString()} rows · {stat.null_pct}% nulls · {stat.distinct_count?.toLocaleString()} distinct
                            </p>
                          )}
                        </div>
                      </div>
                    </button>

                    {/* Expanded content */}
                    <AnimatePresence>
                      {isExpanded && (
                        <motion.div
                          initial={{ height: 0 }}
                          animate={{ height: 'auto' }}
                          exit={{ height: 0 }}
                          className="overflow-hidden"
                        >
                          <div className="border-t border-slate-100 p-4 space-y-4">
                            {/* Stats grid */}
                            <ColumnStatCard stat={stat} />

                            {/* LLM insights */}
                            {colResult?.insights && (
                              <div className="bg-blue-50 border border-blue-100 rounded-xl p-3 text-xs text-blue-700">
                                <p className="font-semibold mb-1">Observations</p>
                                <p className="leading-relaxed">{colResult.insights}</p>
                              </div>
                            )}

                            {/* Issues list */}
                            {issues.length > 0 && (
                              <div className="space-y-2">
                                <p className="text-xs font-semibold text-slate-500 uppercase tracking-wide">Issues detected</p>
                                {issues.map((issue, idx) => {
                                  const cfg = severityConfig[issue.severity] ?? severityConfig.info;
                                  const IssueIcon = cfg.icon;
                                  return (
                                    <div key={idx} className={clsx('rounded-xl border p-3 space-y-1', cfg.bg)}>
                                      <div className="flex items-start gap-2">
                                        <IssueIcon size={14} className={clsx('shrink-0 mt-0.5', cfg.color)} />
                                        <div className="flex-1 min-w-0">
                                          <div className="flex items-center gap-2 flex-wrap">
                                            <span className="text-xs font-semibold text-slate-800">{issue.title}</span>
                                            <span className={clsx('text-[10px] px-1.5 py-0.5 rounded font-medium uppercase tracking-wide', cfg.badge)}>
                                              {issue.category}
                                            </span>
                                            {issue.affected_rows !== null && issue.affected_rows !== undefined && (
                                              <span className="text-[10px] text-slate-500">{issue.affected_rows.toLocaleString()} rows</span>
                                            )}
                                          </div>
                                          <p className="text-xs text-slate-700 mt-1 leading-relaxed">{issue.description}</p>
                                          {issue.recommendation && (
                                            <p className="text-xs text-slate-500 mt-1 flex items-start gap-1">
                                              <CheckCircle2 size={11} className="shrink-0 mt-0.5 text-slate-400" />
                                              {issue.recommendation}
                                            </p>
                                          )}
                                        </div>
                                      </div>
                                    </div>
                                  );
                                })}
                              </div>
                            )}
                          </div>
                        </motion.div>
                      )}
                    </AnimatePresence>
                  </div>
                );
              })}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
