import { useState } from 'react';
import {
  ShieldCheck, ChevronDown, ChevronRight, AlertTriangle,
  Info, AlertCircle, Loader2, BarChart3, CheckCircle2,
  Search, X, RefreshCw, Table2, Filter, FileDown,
  TrendingDown, TrendingUp, Clock, Activity, Calendar,
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
  zscore_outlier_count?: number;
  zscore_outlier_pct?: number;
  negative_count?: number;
  zero_count?: number;
  coeff_variation?: number;
  skewness_approx?: number;
  min_length?: number;
  max_length?: number;
  avg_length?: number;
  sentinel_count?: number;
  whitespace_padded_count?: number;
  all_caps_count?: number;
  numeric_string_count?: number;
  email_like_count?: number;
  min_date?: string;
  max_date?: string;
  future_count?: number;
  epoch_sentinel_count?: number;
  weekend_count?: number;
  pre_1900_count?: number;
  filter_applied?: string;
  top_values?: { value: string; count: number }[];
  query_error?: string;
}

interface VolumeAnalysis {
  time_column: string;
  granularity: 'hour' | 'day';
  periods: number;
  avg_volume: number;
  stddev_volume: number;
  min_volume: number;
  max_volume: number;
  p25_volume: number;
  p75_volume: number;
  low_volume_threshold: number;
  anomaly_count: number;
  anomaly_periods: { period: string; count: number }[];
  recent_periods: { period: string; count: number }[];
  error?: string;
}

interface DQResult {
  table: string;
  sample_size: number | null;
  column_stats: ColumnStat[];
  analysis: DQAnalysis;
  volume_analysis?: VolumeAnalysis | null;
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
            {(stat.whitespace_padded_count ?? 0) > 0 && <StatPill label="Whitespace pad" value={stat.whitespace_padded_count} accent />}
            {(stat.all_caps_count ?? 0) > 0 && <StatPill label="ALL CAPS" value={stat.all_caps_count} />}
            {(stat.numeric_string_count ?? 0) > 0 && <StatPill label="Numeric str" value={stat.numeric_string_count} />}
            {(stat.email_like_count ?? 0) > 0 && <StatPill label="Email-like" value={stat.email_like_count} />}
          </>
        )}
        {isNumeric && (
          <>
            <StatPill label="Min" value={stat.min} />
            <StatPill label="Max" value={stat.max} />
            <StatPill label="Avg" value={stat.avg !== undefined ? Number(stat.avg).toFixed(2) : undefined} />
            <StatPill label="Stddev" value={stat.stddev !== undefined ? Number(stat.stddev).toFixed(2) : undefined} />
            <StatPill label="Median" value={stat.p50 !== undefined ? Number(stat.p50).toFixed(2) : undefined} />
            <StatPill label="Zeros" value={stat.zero_count} accent={(stat.zero_count ?? 0) > 0} />
            <StatPill label="Negatives" value={stat.negative_count} accent={(stat.negative_count ?? 0) > 0} />
            <StatPill label="Outliers IQR" value={stat.outlier_count !== undefined ? `${stat.outlier_count} (${stat.outlier_pct}%)` : undefined} accent={(stat.outlier_pct ?? 0) > 5} />
            <StatPill label="Outliers Z-Score" value={stat.zscore_outlier_count !== undefined ? `${stat.zscore_outlier_count} (${stat.zscore_outlier_pct}%)` : undefined} accent={(stat.zscore_outlier_pct ?? 0) > 5} />
            <StatPill label="CV" value={stat.coeff_variation !== undefined ? stat.coeff_variation.toFixed(2) : undefined} accent={(stat.coeff_variation ?? 0) > 1} />
            <StatPill label="Skewness" value={stat.skewness_approx !== undefined ? stat.skewness_approx.toFixed(2) : undefined} accent={Math.abs(stat.skewness_approx ?? 0) > 2} />
          </>
        )}
        {isDate && (
          <>
            <StatPill label="Min date" value={stat.min_date} />
            <StatPill label="Max date" value={stat.max_date} />
            <StatPill label="Future" value={stat.future_count} accent={(stat.future_count ?? 0) > 0} />
            <StatPill label="Epoch (1970)" value={stat.epoch_sentinel_count} accent={(stat.epoch_sentinel_count ?? 0) > 0} />
            <StatPill label="Weekend" value={stat.weekend_count} />
            <StatPill label="Pre-1900" value={stat.pre_1900_count} accent={(stat.pre_1900_count ?? 0) > 0} />
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
  const [sampleSize, setSampleSize] = useState<number | null>(50000);
  const [tableSearch, setTableSearch] = useState('');
  const [isAnalyzing, setIsAnalyzing] = useState(false);
  const [result, setResult] = useState<DQResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [expandedColumns, setExpandedColumns] = useState<Set<string>>(new Set());
  const [timeColumn, setTimeColumn] = useState<string>('');

  // Row filter state
  const [filterEnabled, setFilterEnabled] = useState(false);
  const [filterColumn, setFilterColumn] = useState('');
  const [filterOperator, setFilterOperator] = useState('=');
  const [filterValue, setFilterValue] = useState('');
  const [filterValue2, setFilterValue2] = useState('');

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

    const body: Record<string, unknown> = {
      table: selectedTable,
      columns: selectedColumns,
      sample_size: sampleSize,
    };
    if (filterEnabled && filterColumn && filterValue !== '') {
      body.filter_column = filterColumn;
      body.filter_operator = filterOperator;
      body.filter_value = filterValue;
      if (filterOperator === 'BETWEEN' && filterValue2 !== '') {
        body.filter_value2 = filterValue2;
      }
    }
    if (timeColumn) {
      body.time_column = timeColumn;
    }

    try {
      const res = await fetch('/api/data-quality/analyze', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
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

  const handleExportPDF = () => {
    if (!result) return;
    const severityIcon = (s: string) => s === 'critical' ? '🔴' : s === 'warning' ? '🟡' : '🔵';
    const scoreColor = (score: number | null) => {
      if (score === null) return '#64748b';
      return score >= 80 ? '#059669' : score >= 60 ? '#d97706' : '#dc2626';
    };

    const colsHtml = result.column_stats.map(stat => {
      const cr = colResultMap[stat.column];
      const issues = cr?.issues ?? [];
      const issuesHtml = issues.map(iss => `
        <div style="border-left:3px solid ${iss.severity === 'critical' ? '#dc2626' : iss.severity === 'warning' ? '#d97706' : '#3b82f6'};padding:8px 12px;margin:6px 0;background:#f8fafc;border-radius:4px;">
          <div style="font-weight:600;font-size:12px;">${severityIcon(iss.severity)} ${iss.title} <span style="font-size:10px;color:#64748b;font-weight:400;">[${iss.category}]${iss.affected_rows != null ? ` · ${iss.affected_rows.toLocaleString()} rows` : ''}</span></div>
          <div style="font-size:11px;color:#374151;margin:3px 0;">${iss.description}</div>
          ${iss.recommendation ? `<div style="font-size:11px;color:#6b7280;font-style:italic;">→ ${iss.recommendation}</div>` : ''}
        </div>`).join('');

      const topVals = stat.top_values?.slice(0, 6).map(tv =>
        `<span style="background:#f1f5f9;border:1px solid #e2e8f0;border-radius:4px;padding:2px 6px;font-size:10px;font-family:monospace;margin:2px;">${tv.value || '(empty)'} ×${tv.count.toLocaleString()}</span>`
      ).join('') ?? '';

      const statPills: string[] = [];
      if (stat.total != null) statPills.push(`Total: ${stat.total.toLocaleString()}`);
      if (stat.null_pct != null) statPills.push(`Nulls: ${stat.null_pct}%`);
      if (stat.distinct_count != null) statPills.push(`Distinct: ${stat.distinct_count.toLocaleString()}`);
      if (stat.avg != null) statPills.push(`Avg: ${Number(stat.avg).toFixed(2)}`);
      if (stat.stddev != null) statPills.push(`Stddev: ${Number(stat.stddev).toFixed(2)}`);
      if (stat.min != null) statPills.push(`Min: ${stat.min}`);
      if (stat.max != null) statPills.push(`Max: ${stat.max}`);
      if (stat.zero_count != null && stat.zero_count > 0) statPills.push(`Zeros: ${stat.zero_count}`);
      if (stat.skewness_approx != null) statPills.push(`Skewness: ${stat.skewness_approx.toFixed(2)}`);
      if (stat.coeff_variation != null) statPills.push(`CV: ${stat.coeff_variation.toFixed(2)}`);
      if (stat.outlier_count != null) statPills.push(`Outliers IQR: ${stat.outlier_count} (${stat.outlier_pct}%)`);
      if (stat.zscore_outlier_count != null) statPills.push(`Outliers Z-Score: ${stat.zscore_outlier_count} (${stat.zscore_outlier_pct}%)`);
      if (stat.empty_pct != null) statPills.push(`Empty: ${stat.empty_pct}%`);
      if (stat.avg_length != null) statPills.push(`Avg len: ${stat.avg_length}`);
      if (stat.sentinel_count != null && stat.sentinel_count > 0) statPills.push(`Sentinels: ${stat.sentinel_count}`);
      if (stat.whitespace_padded_count != null && stat.whitespace_padded_count > 0) statPills.push(`Whitespace pad: ${stat.whitespace_padded_count}`);
      if (stat.min_date) statPills.push(`Min date: ${stat.min_date}`);
      if (stat.max_date) statPills.push(`Max date: ${stat.max_date}`);
      if (stat.future_count != null && stat.future_count > 0) statPills.push(`Future: ${stat.future_count}`);
      if (stat.epoch_sentinel_count != null && stat.epoch_sentinel_count > 0) statPills.push(`Epoch 1970: ${stat.epoch_sentinel_count}`);

      return `
      <div style="border:1px solid #e2e8f0;border-radius:8px;padding:16px;margin-bottom:16px;break-inside:avoid;">
        <div style="display:flex;align-items:center;gap:12px;margin-bottom:10px;flex-wrap:wrap;">
          <span style="font-family:monospace;font-weight:700;font-size:14px;color:#1e293b;">${stat.column}</span>
          <span style="background:#f1f5f9;border:1px solid #e2e8f0;border-radius:4px;padding:2px 6px;font-size:10px;font-family:monospace;color:#64748b;">${stat.type}</span>
          ${cr ? `<span style="color:${scoreColor(cr.quality_score)};font-weight:700;font-size:12px;border:1px solid;border-radius:20px;padding:2px 8px;">${cr.quality_score}/100</span>` : ''}
          ${issues.length > 0 ? `<span style="font-size:11px;color:#64748b;">${issues.length} issue${issues.length > 1 ? 's' : ''}</span>` : '<span style="color:#059669;font-size:11px;">✓ No issues</span>'}
        </div>
        <div style="display:flex;flex-wrap:wrap;gap:6px;margin-bottom:10px;">
          ${statPills.map(p => `<span style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:6px;padding:3px 8px;font-size:11px;color:#374151;">${p}</span>`).join('')}
        </div>
        ${topVals ? `<div style="margin-bottom:10px;"><div style="font-size:10px;font-weight:600;color:#94a3b8;text-transform:uppercase;letter-spacing:.05em;margin-bottom:4px;">Top values</div><div style="display:flex;flex-wrap:wrap;gap:4px;">${topVals}</div></div>` : ''}
        ${cr?.insights ? `<div style="background:#eff6ff;border:1px solid #bfdbfe;border-radius:6px;padding:8px 10px;font-size:11px;color:#1d4ed8;margin-bottom:8px;"><strong>Observations:</strong> ${cr.insights}</div>` : ''}
        ${issuesHtml}
      </div>`;
    }).join('');

    const recs = result.analysis.recommendations?.map(r =>
      `<li style="margin:4px 0;font-size:12px;color:#4c1d95;">✓ ${r}</li>`
    ).join('') ?? '';

    const filterNote = result.column_stats[0]?.filter_applied
      ? `<div style="background:#f5f3ff;border:1px solid #ddd6fe;border-radius:6px;padding:6px 12px;font-size:11px;font-family:monospace;color:#7c3aed;margin-bottom:12px;">WHERE ${result.column_stats[0].filter_applied}</div>`
      : '';

    const html = `<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Data Quality Report — ${result.table}</title>
  <style>
    @media print { body { margin: 0; } }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; color: #1e293b; margin: 0; padding: 32px; background: white; }
    h1 { font-size: 22px; margin: 0 0 4px; }
    h2 { font-size: 15px; margin: 20px 0 10px; color: #374151; }
  </style>
</head>
<body>
  <div style="border-bottom:2px solid #7c3aed;padding-bottom:16px;margin-bottom:20px;">
    <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px;">
      <h1>Data Quality Report — <span style="color:#7c3aed;font-family:monospace;">${result.table}</span></h1>
      ${result.analysis.quality_score != null ? `<span style="color:${scoreColor(result.analysis.quality_score)};font-weight:700;font-size:16px;border:1px solid;border-radius:20px;padding:3px 12px;">${result.analysis.quality_score}/100</span>` : ''}
    </div>
    <p style="font-size:13px;color:#475569;margin:0 0 12px;">${result.analysis.summary}</p>
    ${filterNote}
    <div style="display:flex;gap:16px;flex-wrap:wrap;">
      <span style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:6px 14px;font-size:12px;"><strong>${result.sample_size != null ? result.sample_size.toLocaleString() + ' rows' : 'Full table'}</strong> sampled</span>
      <span style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:6px 14px;font-size:12px;"><strong>${result.column_stats.length}</strong> columns</span>
      ${criticalCount > 0 ? `<span style="background:#fef2f2;border:1px solid #fecaca;border-radius:8px;padding:6px 14px;font-size:12px;color:#dc2626;"><strong>${criticalCount}</strong> critical issues</span>` : ''}
      ${issueCount > 0 ? `<span style="background:#fffbeb;border:1px solid #fde68a;border-radius:8px;padding:6px 14px;font-size:12px;color:#d97706;"><strong>${issueCount}</strong> total issues</span>` : ''}
    </div>
  </div>
  ${recs ? `<div style="background:#f5f3ff;border:1px solid #ddd6fe;border-radius:8px;padding:14px 16px;margin-bottom:20px;"><h2 style="margin:0 0 8px;color:#6d28d9;font-size:13px;font-weight:600;text-transform:uppercase;letter-spacing:.05em;">Global Recommendations</h2><ul style="margin:0;padding-left:16px;">${recs}</ul></div>` : ''}
  <h2>Column Results</h2>
  ${colsHtml}
  <p style="font-size:10px;color:#94a3b8;text-align:right;margin-top:20px;border-top:1px solid #e2e8f0;padding-top:8px;">Generated by ClickSense AI Data Quality · ${new Date().toLocaleString()}</p>
</body>
</html>`;

    const win = window.open('', '_blank', 'width=900,height=700');
    if (!win) return;
    win.document.write(html);
    win.document.close();
    win.focus();
    setTimeout(() => win.print(), 400);
  };

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
                    onClick={() => { setSelectedTable(t); setSelectedColumns([]); setResult(null); setFilterColumn(''); setFilterValue(''); setFilterValue2(''); setTimeColumn(''); }}
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
                value={sampleSize === null ? 'full' : String(sampleSize)}
                onChange={e => setSampleSize(e.target.value === 'full' ? null : Number(e.target.value))}
                className="w-full text-xs bg-slate-50 border border-slate-200 rounded-lg px-3 py-2 focus:ring-2 focus:ring-violet-500/20 focus:border-violet-500 transition-all"
              >
                <option value={10000}>10 000 rows</option>
                <option value={50000}>50 000 rows</option>
                <option value={100000}>100 000 rows</option>
                <option value={500000}>500 000 rows</option>
                <option value="full">Full scan (entire table)</option>
              </select>
              {sampleSize === null && (
                <p className="text-[10px] text-amber-600 bg-amber-50 border border-amber-200 rounded-lg px-2 py-1">
                  Full scan reads every row — may be slow on large tables.
                </p>
              )}
            </div>
          )}

          {/* Row filter (optional) */}
          {selectedTable && (
            <div className="space-y-2">
              <div className="flex items-center justify-between">
                <label className="text-xs font-semibold text-slate-600 uppercase tracking-wide flex items-center gap-1.5">
                  <Filter size={11} />
                  Row Filter
                  <span className="text-[10px] font-normal text-slate-400 normal-case">(optional)</span>
                </label>
                <button
                  onClick={() => setFilterEnabled(p => !p)}
                  className={clsx(
                    'relative inline-flex h-4 w-7 shrink-0 rounded-full border-2 border-transparent transition-colors focus:outline-none',
                    filterEnabled ? 'bg-violet-500' : 'bg-slate-200'
                  )}
                >
                  <span className={clsx(
                    'inline-block h-3 w-3 transform rounded-full bg-white shadow transition-transform',
                    filterEnabled ? 'translate-x-3' : 'translate-x-0'
                  )} />
                </button>
              </div>
              {filterEnabled && (
                <div className="space-y-1.5">
                  <select
                    value={filterColumn}
                    onChange={e => { setFilterColumn(e.target.value); setFilterValue(''); setFilterValue2(''); }}
                    className="w-full text-xs bg-slate-50 border border-slate-200 rounded-lg px-3 py-2 focus:ring-2 focus:ring-violet-500/20 focus:border-violet-500 transition-all font-mono"
                  >
                    <option value="">— select column —</option>
                    {tableColumns.map(col => (
                      <option key={col.name} value={col.name}>{col.name} ({col.type})</option>
                    ))}
                  </select>
                  {filterColumn && (() => {
                    const colInfo = tableColumns.find(c => c.name === filterColumn);
                    const isDateCol = colInfo ? ['DATE', 'DATETIME', 'TIMESTAMP'].some(t => colInfo.type.toUpperCase().includes(t)) : false;
                    const inputType = isDateCol ? 'date' : 'text';
                    return (
                      <div className="space-y-1.5">
                        <div className="flex gap-1.5">
                          <select
                            value={filterOperator}
                            onChange={e => { setFilterOperator(e.target.value); setFilterValue2(''); }}
                            className="w-24 shrink-0 text-xs bg-slate-50 border border-slate-200 rounded-lg px-2 py-2 focus:ring-2 focus:ring-violet-500/20 focus:border-violet-500 transition-all"
                          >
                            {['=', '!=', '<', '>', '<=', '>=', 'LIKE', ...(isDateCol ? ['BETWEEN'] : [])].map(op => (
                              <option key={op} value={op}>{op}</option>
                            ))}
                          </select>
                          <input
                            type={inputType}
                            value={filterValue}
                            onChange={e => setFilterValue(e.target.value)}
                            placeholder={filterOperator === 'BETWEEN' ? 'from…' : 'value…'}
                            className="flex-1 text-xs bg-slate-50 border border-slate-200 rounded-lg px-3 py-2 focus:ring-2 focus:ring-violet-500/20 focus:border-violet-500 transition-all font-mono"
                          />
                        </div>
                        {filterOperator === 'BETWEEN' && (
                          <input
                            type={inputType}
                            value={filterValue2}
                            onChange={e => setFilterValue2(e.target.value)}
                            placeholder="to…"
                            className="w-full text-xs bg-slate-50 border border-slate-200 rounded-lg px-3 py-2 focus:ring-2 focus:ring-violet-500/20 focus:border-violet-500 transition-all font-mono"
                          />
                        )}
                        {filterColumn && filterValue && (
                          <p className="text-[10px] text-violet-600 bg-violet-50 border border-violet-200 rounded-lg px-2 py-1 font-mono">
                            {filterOperator === 'BETWEEN' && filterValue2
                              ? `WHERE ${filterColumn} BETWEEN '${filterValue}' AND '${filterValue2}'`
                              : `WHERE ${filterColumn} ${filterOperator} '${filterValue}'`
                            }
                          </p>
                        )}
                      </div>
                    );
                  })()}
                </div>
              )}
            </div>
          )}

          {/* Time Series Column (optional) */}
          {selectedTable && (
            <div className="space-y-2">
              <div className="flex items-center justify-between">
                <label className="text-xs font-semibold text-slate-600 uppercase tracking-wide flex items-center gap-1.5">
                  <Calendar size={11} />
                  Colonne Temporelle
                  <span className="text-[10px] font-normal text-slate-400 normal-case">(optionnelle)</span>
                </label>
                {timeColumn && (
                  <button
                    onClick={() => setTimeColumn('')}
                    className="text-xs text-slate-400 hover:text-slate-600 flex items-center gap-1"
                  >
                    <X size={11} /> Clear
                  </button>
                )}
              </div>
              <select
                value={timeColumn}
                onChange={e => setTimeColumn(e.target.value)}
                className="w-full text-xs bg-slate-50 border border-slate-200 rounded-lg px-3 py-2 focus:ring-2 focus:ring-violet-500/20 focus:border-violet-500 transition-all font-mono"
              >
                <option value="">— aucune (pas d'analyse temporelle) —</option>
                {tableColumns
                  .filter(col => ['DATE', 'DATETIME', 'TIMESTAMP'].some(t => col.type.toUpperCase().includes(t)))
                  .map(col => (
                    <option key={col.name} value={col.name}>{col.name} ({col.type})</option>
                  ))
                }
                {tableColumns
                  .filter(col => !['DATE', 'DATETIME', 'TIMESTAMP'].some(t => col.type.toUpperCase().includes(t)))
                  .map(col => (
                    <option key={col.name} value={col.name} className="text-slate-400">{col.name} ({col.type})</option>
                  ))
                }
              </select>
              {timeColumn && (
                <p className="text-[10px] text-teal-700 bg-teal-50 border border-teal-200 rounded-lg px-2 py-1 flex items-center gap-1">
                  <Activity size={9} />
                  Analyse de cohérence volumétrique par heure / jour activée.
                </p>
              )}
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
              {selectedColumns.length} column{selectedColumns.length > 1 ? 's' : ''} · {sampleSize === null ? 'full scan' : `${sampleSize.toLocaleString()} rows`}
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
            <div className="grid grid-cols-2 gap-3 max-w-lg text-left mt-2">
              {[
                { icon: AlertCircle, color: 'text-red-500 bg-red-50', label: 'Format & Content', desc: 'Invalid emails, wrong patterns, impossible values' },
                { icon: AlertTriangle, color: 'text-amber-500 bg-amber-50', label: 'Nulls & Sentinels', desc: 'High null rates, "N/A", -1, empty string abuse' },
                { icon: BarChart3, color: 'text-blue-500 bg-blue-50', label: 'Outliers IQR + Z-Score', desc: 'Distribution outliers via IQR and 3σ Z-Score methods' },
                { icon: Activity, color: 'text-teal-500 bg-teal-50', label: 'Volume Consistency', desc: 'Detect pipeline gaps and volume drops by hour/day' },
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
                <div className="flex items-center gap-2 shrink-0">
                  <button
                    onClick={handleExportPDF}
                    className="flex items-center gap-1.5 text-xs text-slate-500 hover:text-violet-600 border border-slate-200 hover:border-violet-300 px-3 py-1.5 rounded-lg transition-colors"
                    title="Export report as PDF"
                  >
                    <FileDown size={12} /> Export PDF
                  </button>
                  <button
                    onClick={handleAnalyze}
                    className="flex items-center gap-1.5 text-xs text-slate-500 hover:text-violet-600 border border-slate-200 hover:border-violet-300 px-3 py-1.5 rounded-lg transition-colors"
                  >
                    <RefreshCw size={12} /> Re-analyze
                  </button>
                </div>
              </div>

              {/* Active filter badge */}
              {result.column_stats[0]?.filter_applied && (
                <div className="mb-3 flex items-center gap-2">
                  <Filter size={12} className="text-violet-500 shrink-0" />
                  <span className="text-xs font-mono text-violet-700 bg-violet-50 border border-violet-200 px-2 py-0.5 rounded-lg">
                    WHERE {result.column_stats[0].filter_applied}
                  </span>
                </div>
              )}

              {/* Quick stats */}
              <div className="flex gap-4 flex-wrap">
                <div className="bg-slate-50 rounded-xl px-4 py-2 text-center">
                  <p className="text-xs text-slate-500">Sample</p>
                  <p className="text-sm font-bold text-slate-800">{result.sample_size != null ? result.sample_size.toLocaleString() : 'Full table'}</p>
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

            {/* Volume Consistency (temporal) */}
            {result.volume_analysis && !result.volume_analysis.error && (
              <div className="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden">
                <div className="p-4 border-b border-slate-100">
                  <div className="flex items-center gap-2 mb-1">
                    <div className="bg-teal-100 p-1.5 rounded-lg text-teal-600">
                      <Activity size={15} />
                    </div>
                    <div>
                      <h4 className="text-sm font-bold text-slate-800">Cohérence Volumétrique</h4>
                      <p className="text-xs text-slate-500">
                        Volume par {result.volume_analysis.granularity} sur <span className="font-mono">{result.volume_analysis.time_column}</span>
                      </p>
                    </div>
                    {result.volume_analysis.anomaly_count > 0 && (
                      <span className="ml-auto flex items-center gap-1 text-xs font-semibold text-amber-700 bg-amber-50 border border-amber-200 px-2.5 py-1 rounded-full">
                        <AlertTriangle size={11} />
                        {result.volume_analysis.anomaly_count} période{result.volume_analysis.anomaly_count > 1 ? 's' : ''} anormale{result.volume_analysis.anomaly_count > 1 ? 's' : ''}
                      </span>
                    )}
                    {result.volume_analysis.anomaly_count === 0 && (
                      <span className="ml-auto flex items-center gap-1 text-xs font-semibold text-emerald-700 bg-emerald-50 border border-emerald-200 px-2.5 py-1 rounded-full">
                        <CheckCircle2 size={11} />
                        Volume stable
                      </span>
                    )}
                  </div>
                </div>
                <div className="p-4 space-y-4">
                  {/* Stats summary */}
                  <div className="grid grid-cols-4 gap-3">
                    {[
                      { label: 'Moy. / période', value: result.volume_analysis.avg_volume.toLocaleString() },
                      { label: 'Écart-type', value: result.volume_analysis.stddev_volume.toLocaleString() },
                      { label: 'Min', value: result.volume_analysis.min_volume.toLocaleString(), accent: true },
                      { label: 'Max', value: result.volume_analysis.max_volume.toLocaleString() },
                    ].map(({ label, value, accent }) => (
                      <div key={label} className={clsx(
                        'rounded-xl px-3 py-2 text-center',
                        accent && result.volume_analysis!.min_volume < result.volume_analysis!.low_volume_threshold
                          ? 'bg-red-50 border border-red-200'
                          : 'bg-slate-50'
                      )}>
                        <p className="text-[10px] text-slate-500">{label}</p>
                        <p className={clsx(
                          'text-sm font-bold',
                          accent && result.volume_analysis!.min_volume < result.volume_analysis!.low_volume_threshold
                            ? 'text-red-600'
                            : 'text-slate-800'
                        )}>{value}</p>
                      </div>
                    ))}
                  </div>

                  {/* Anomaly periods */}
                  {result.volume_analysis.anomaly_count > 0 && (
                    <div>
                      <p className="text-xs font-semibold text-amber-700 mb-2 flex items-center gap-1.5">
                        <TrendingDown size={12} />
                        Périodes sous le seuil ({result.volume_analysis.low_volume_threshold.toLocaleString()} entrées)
                      </p>
                      <div className="flex flex-wrap gap-1.5">
                        {result.volume_analysis.anomaly_periods.map((p, i) => (
                          <span key={i} className="inline-flex items-center gap-1.5 bg-amber-50 border border-amber-200 rounded-lg px-2 py-1 text-xs font-mono text-amber-800">
                            <TrendingDown size={9} className="text-amber-600" />
                            {p.period} — {p.count.toLocaleString()}
                          </span>
                        ))}
                      </div>
                    </div>
                  )}

                  {/* Recent periods */}
                  <div>
                    <p className="text-xs font-semibold text-slate-500 mb-2 flex items-center gap-1.5">
                      <Clock size={11} />
                      Périodes récentes
                    </p>
                    <div className="overflow-x-auto">
                      <table className="w-full text-xs">
                        <thead>
                          <tr className="border-b border-slate-100">
                            <th className="text-left py-1.5 px-2 text-slate-500 font-semibold">Période</th>
                            <th className="text-right py-1.5 px-2 text-slate-500 font-semibold">Volume</th>
                            <th className="py-1.5 px-2 w-24"></th>
                          </tr>
                        </thead>
                        <tbody>
                          {result.volume_analysis.recent_periods.map((p, i) => {
                            const isAnomaly = p.count < result.volume_analysis!.low_volume_threshold;
                            const pct = Math.min(100, Math.round(100 * p.count / result.volume_analysis!.avg_volume));
                            return (
                              <tr key={i} className={clsx('border-b border-slate-50', isAnomaly && 'bg-amber-50/60')}>
                                <td className="py-1 px-2 font-mono text-slate-700">{p.period}</td>
                                <td className={clsx('py-1 px-2 text-right font-semibold', isAnomaly ? 'text-amber-700' : 'text-slate-700')}>
                                  {p.count.toLocaleString()}
                                  {isAnomaly && <TrendingDown size={9} className="inline ml-1 text-amber-600" />}
                                </td>
                                <td className="py-1 px-2">
                                  <div className="w-full bg-slate-100 rounded-full h-1.5">
                                    <div
                                      className={clsx('h-1.5 rounded-full', isAnomaly ? 'bg-amber-400' : 'bg-teal-400')}
                                      style={{ width: `${pct}%` }}
                                    />
                                  </div>
                                </td>
                              </tr>
                            );
                          })}
                        </tbody>
                      </table>
                    </div>
                  </div>
                </div>
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
