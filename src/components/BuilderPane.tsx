import React, { useState, useEffect, useRef, useCallback } from 'react';
import { createPortal } from 'react-dom';
import { useAppStore } from '../store';
import {
  DndContext,
  closestCenter,
  KeyboardSensor,
  PointerSensor,
  useSensor,
  useSensors,
  DragEndEvent,
  DragOverlay,
  defaultDropAnimationSideEffects
} from '@dnd-kit/core';
import {
  arrayMove,
  SortableContext,
  sortableKeyboardCoordinates,
  verticalListSortingStrategy,
  horizontalListSortingStrategy,
  useSortable
} from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';
import {
  GripVertical, X, Table, BarChart2, PieChart, LineChart, Play, Sparkles, Star, RefreshCw,
  Maximize2, Minimize2, Palette, ArrowUp, ArrowDown, Filter, RotateCcw, Search, Plus,
  History, Brain, AlertTriangle, Lightbulb, TrendingUp, Grid3X3, AreaChart, ScatterChart,
  ChevronRight, Clock, LayoutGrid, Activity, Telescope, ChevronDown, CheckCircle2,
  Database, Hash, Type, Calendar, ToggleLeft, Percent, Layers
} from 'lucide-react';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip as RechartsTooltip, Legend, ResponsiveContainer,
  LineChart as RechartsLineChart, Line, PieChart as RechartsPieChart, Pie, Cell,
  AreaChart as RechartsAreaChart, Area, ScatterChart as RechartsScatterChart, Scatter,
  RadarChart, Radar, PolarGrid, PolarAngleAxis, PolarRadiusAxis
} from 'recharts';
import clsx from 'clsx';

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------
const CHART_COLORS = ['#10b981', '#3b82f6', '#f59e0b', '#ef4444', '#8b5cf6', '#ec4899', '#06b6d4', '#84cc16'];
const MAX_ROWS = 1000;

const AGG_OPTIONS = [
  { value: 'count',        label: 'COUNT' },
  { value: 'count_distinct', label: 'COUNT DISTINCT' },
  { value: 'sum',          label: 'SUM' },
  { value: 'avg',          label: 'AVG' },
  { value: 'min',          label: 'MIN' },
  { value: 'max',          label: 'MAX' },
  { value: 'median',       label: 'MEDIAN' },
  { value: 'p90',          label: 'P90' },
  { value: 'p95',          label: 'P95' },
  { value: 'p99',          label: 'P99' },
  { value: 'stddevPop',    label: 'STDDEV' },
  { value: 'varPop',       label: 'VARIANCE' },
  { value: 'uniqExact',    label: 'UNIQ (exact)' },
  { value: 'uniqHLL12',    label: 'UNIQ (approx)' },
  { value: 'first_value',  label: 'FIRST VALUE' },
  { value: 'last_value',   label: 'LAST VALUE' },
];

/** Format a cell value: integers and floats get a thousands separator. */
function formatCellValue(value: unknown): string {
  if (value === null || value === undefined) return '';
  const n = Number(value);
  if (typeof value !== 'string' && isFinite(n)) {
    return n.toLocaleString(undefined, { maximumFractionDigits: 6 });
  }
  if (typeof value === 'string' && value.trim() !== '' && !isNaN(Number(value)) && !/^0\d/.test(value.trim())) {
    const parsed = Number(value);
    if (isFinite(parsed)) return parsed.toLocaleString(undefined, { maximumFractionDigits: 6 });
  }
  return String(value);
}

/** Convert #rrggbb to rgba(r,g,b,alpha) */
function hexToRgba(hex: string, alpha: number): string {
  const r = parseInt(hex.slice(1, 3), 16);
  const g = parseInt(hex.slice(3, 5), 16);
  const b = parseInt(hex.slice(5, 7), 16);
  return `rgba(${r},${g},${b},${alpha})`;
}

/** Build the SQL expression for a measure */
function measureSql(m: { column: string; agg: string }): string {
  const alias = m.column === '*' ? `${m.agg}_all` : `${m.agg}_${m.column}`;
  const col = m.column === '*' ? '*' : m.column;
  switch (m.agg) {
    case 'count':         return `count(${col}) AS ${alias}`;
    case 'count_distinct':return `uniqExact(${col}) AS count_distinct_${m.column}`;
    case 'sum':           return `sum(${col}) AS ${alias}`;
    case 'avg':           return `avg(${col}) AS ${alias}`;
    case 'min':           return `min(${col}) AS ${alias}`;
    case 'max':           return `max(${col}) AS ${alias}`;
    case 'median':        return `median(${col}) AS ${alias}`;
    case 'p90':           return `quantile(0.9)(${col}) AS ${alias}`;
    case 'p95':           return `quantile(0.95)(${col}) AS ${alias}`;
    case 'p99':           return `quantile(0.99)(${col}) AS ${alias}`;
    case 'stddevPop':     return `stddevPop(${col}) AS ${alias}`;
    case 'varPop':        return `varPop(${col}) AS ${alias}`;
    case 'uniqExact':     return `uniqExact(${col}) AS ${alias}`;
    case 'uniqHLL12':     return `uniqHLL12(${col}) AS ${alias}`;
    case 'first_value':   return `first_value(${col}) AS ${alias}`;
    case 'last_value':    return `last_value(${col}) AS ${alias}`;
    default:              return `${m.agg}(${col}) AS ${alias}`;
  }
}

const SortableHeader: React.FC<{
  id: string;
  column: string;
  sortConfig: { key: string; direction: 'asc' | 'desc' } | null;
  onSort: (key: string) => void;
  onFilterClick: (key: string) => void;
  colors?: { bg?: string; text?: string };
  isFiltered?: boolean;
}> = ({ id, column, sortConfig, onSort, onFilterClick, colors, isFiltered }) => {
  const { attributes, listeners, setNodeRef, transform, transition, isDragging } = useSortable({ id });

  const bgColor = colors?.bg ? hexToRgba(colors.bg, 0.25) : '#f8fafc';
  const textColor = colors?.text || '#1e293b';

  const style = {
    transform: CSS.Transform.toString(transform),
    transition,
    backgroundColor: bgColor,
    color: textColor,
    opacity: isDragging ? 0.5 : 1,
    zIndex: isDragging ? 10 : 1,
  };

  return (
    <th
      ref={setNodeRef}
      style={style}
      className="px-4 py-3 text-left text-xs font-bold uppercase tracking-wider border-b border-r border-slate-200 relative group select-none"
    >
      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-1 cursor-pointer flex-1 overflow-hidden" onClick={() => onSort(column)}>
          <span className="truncate">{column}</span>
          {sortConfig?.key === column && (
            <span className="shrink-0" style={{ color: colors?.text ? textColor : '#10b981' }}>
              {sortConfig.direction === 'asc' ? <ArrowUp size={14} /> : <ArrowDown size={14} />}
            </span>
          )}
        </div>
        <div className={clsx("flex items-center gap-1 transition-opacity shrink-0", isFiltered ? "opacity-100" : "opacity-0 group-hover:opacity-100")}>
          <button
            onClick={(e) => { e.stopPropagation(); onFilterClick(column); }}
            className={clsx("p-1 hover:bg-black/10 rounded", isFiltered ? "text-emerald-600" : "text-slate-400 hover:text-slate-600")}
            title="Filter"
          >
            <Filter size={14} />
          </button>
          <div {...attributes} {...listeners} className="cursor-grab p-1 hover:bg-black/10 rounded text-slate-400 hover:text-slate-600">
            <GripVertical size={14} />
          </div>
        </div>
      </div>
    </th>
  );
};
const SortableItem: React.FC<{ id: string, item: any, onRemove: () => void }> = ({ id, item, onRemove }) => {
  const {
    attributes,
    listeners,
    setNodeRef,
    transform,
    transition,
  } = useSortable({ id });

  const style = {
    transform: CSS.Transform.toString(transform),
    transition,
  };

  return (
    <div ref={setNodeRef} style={style} className="flex items-center gap-2 bg-white border border-slate-200 px-3 py-1.5 rounded-md shadow-sm text-sm">
      <button {...attributes} {...listeners} className="cursor-grab text-slate-400 hover:text-slate-600">
        <GripVertical size={14} />
      </button>
      <span className="font-medium text-slate-700">{item.name || (item.column === '*' ? 'All Records' : item.column)}</span>
      {item.agg && <span className="text-xs text-emerald-600 bg-emerald-50 px-1.5 py-0.5 rounded font-mono">{item.agg}</span>}
      <button onClick={onRemove} className="ml-auto text-slate-400 hover:text-red-500">
        <X size={14} />
      </button>
    </div>
  );
}

type VisualType = 'table' | 'matrix' | 'bar' | 'line' | 'area' | 'pie' | 'scatter' | 'radar';

interface DrillThroughState {
  visible: boolean;
  sql: string;
  data: any[];
  loading: boolean;
  error: string | null;
  title: string;
}

interface AiAnalysisState {
  visible: boolean;
  loading: boolean;
  alerts: string[];
  suggestions: string[];
  projections: string[];
  optimized_sql: string;
  risk_level: string;
  error: string | null;
  sql: string;
}

interface HistoryEntry {
  id: number;
  query_text: string;
  sql: string;
  created_at: string;
}

interface ColumnStat {
  name: string;
  type: string;
  notnull: number;
  null_count: number;
  null_pct: number;
  distinct: number;
  distinct_pct: number;
  top_values: { value: string; count: number }[];
}

interface ProfileStats {
  table: string;
  total_rows: number;
  total_columns: number;
  columns: ColumnStat[];
}

interface AIInsights {
  summary: string;
  quality_issues: string[];
  key_insights: string[];
  recommendations: string[];
}

interface AIExploreState {
  visible: boolean;
  targetTable: string;
  loading: boolean;
  stats: ProfileStats | null;
  insights: AIInsights | null;
  insightsLoading: boolean;
  error: string | null;
}

export function BuilderPane() {
  const { schema, queryResult, queryConfig, setQueryConfig, setQueryResult, currentUser, savedQueries, setSavedQueries, tableMetadata, setTableMetadata, selectedTable, setSelectedTable } = useAppStore();
  const [visualType, setVisualType] = useState<VisualType>('table');
  const [isExecuting, setIsExecuting] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const [suggestedVisual, setSuggestedVisual] = useState<VisualType | null>(null);
  const [isRefreshingSchema, setIsRefreshingSchema] = useState(false);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const [columnColors, setColumnColors] = useState<Record<string, { bg?: string; text?: string }>>({});
  const [showColorPicker, setShowColorPicker] = useState<string | null>(null);
  const [fieldSearch, setFieldSearch] = useState('');
  const [tableSearch, setTableSearch] = useState('');
  const [showHistory, setShowHistory] = useState(false);
  const [historyEntries, setHistoryEntries] = useState<HistoryEntry[]>([]);
  const [loadingHistory, setLoadingHistory] = useState(false);

  // Drill-through state
  const [drillThrough, setDrillThrough] = useState<DrillThroughState>({
    visible: false, sql: '', data: [], loading: false, error: null, title: ''
  });

  // AI Analysis state
  const [aiAnalysis, setAiAnalysis] = useState<AiAnalysisState>({
    visible: false, loading: false, alerts: [], suggestions: [], projections: [],
    optimized_sql: '', risk_level: 'low', error: null, sql: ''
  });

  // AI Explore / Profiling state
  const [aiExplore, setAiExplore] = useState<AIExploreState>({
    visible: false,
    targetTable: '',
    loading: false,
    stats: null,
    insights: null,
    insightsLoading: false,
    error: null,
  });

  // Row/Column dimension split
  const [rowDims, setRowDims] = useState<{ name: string }[]>([]);
  const [colDims, setColDims] = useState<{ name: string }[]>([]);

  // Pre-query WHERE filters
  const [preFilters, setPreFilters] = useState<{ id: string; column: string; operator: string; value: string }[]>([]);

  // Output limit — always capped at 1000
  const [queryLimit, setQueryLimit] = useState<string>('100');

  // Table display state
  const [sortConfig, setSortConfig] = useState<{ key: string; direction: 'asc' | 'desc' } | null>(null);
  const [filters, setFilters] = useState<Record<string, string>>({});
  const [activeFilterColumn, setActiveFilterColumn] = useState<string | null>(null);
  const [columnOrder, setColumnOrder] = useState<string[]>([]);
  const [activeDragColumn, setActiveDragColumn] = useState<string | null>(null);
  const [addMeasureFor, setAddMeasureFor] = useState<string | null>(null);

  // Initialize column order when results change
  useEffect(() => {
    if (queryResult && queryResult.length > 0) {
      const keys = Object.keys(queryResult[0]);
      // Only update if it's a completely new set of columns
      if (columnOrder.length === 0 || !keys.every(k => columnOrder.includes(k))) {
        setColumnOrder(keys);
      }
    } else {
      setColumnOrder([]);
    }
  }, [queryResult]);

  const tables = Object.keys(schema).sort((a, b) => {
    const favA = tableMetadata[a]?.is_favorite ? 1 : 0;
    const favB = tableMetadata[b]?.is_favorite ? 1 : 0;
    if (favA !== favB) return favB - favA;
    return a.localeCompare(b);
  });

  useEffect(() => {
    if (!selectedTable && tables.length > 0) {
      setSelectedTable(tables[0]);
    }
  }, [tables, selectedTable]);

  const handleTableSelect = (t: string) => {
    if (t !== selectedTable) {
      setSelectedTable(t);
      setQueryConfig({ dimensions: [], measures: [], filters: [] });
      setQueryResult([]);
    }
  };

  const toggleFavorite = async (tableName: string) => {
    const current = tableMetadata[tableName] || { description: '', is_favorite: false };
    const updated = { ...current, is_favorite: !current.is_favorite };
    setTableMetadata({ ...tableMetadata, [tableName]: updated });
    await fetch('/api/tables/metadata', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ table_name: tableName, ...updated })
    });
  };

  const updateDescription = async (tableName: string, description: string) => {
    const current = tableMetadata[tableName] || { description: '', is_favorite: false };
    const updated = { ...current, description };
    setTableMetadata({ ...tableMetadata, [tableName]: updated });
    await fetch('/api/tables/metadata', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ table_name: tableName, ...updated })
    });
  };

  // Visualization Recommendation Engine
  useEffect(() => {
    const allDims = [...rowDims, ...colDims];
    const dims = allDims.length;
    const meas = queryConfig.measures.length;

    if (dims === 0 && meas === 0) {
      setSuggestedVisual(null);
      return;
    }

    if (dims === 1 && meas >= 1) {
      const dimName = allDims[0].name.toLowerCase();
      if (dimName.includes('date') || dimName.includes('time') || dimName.includes('day') || dimName.includes('month') || dimName.includes('year')) {
        setSuggestedVisual('line');
      } else if (meas === 1 && (queryConfig.measures[0] as any).agg === 'count') {
        setSuggestedVisual('bar');
      } else {
        setSuggestedVisual('bar');
      }
    } else if (dims === 0 && meas > 0) {
      setSuggestedVisual('table');
    } else {
      setSuggestedVisual('table');
    }
  }, [rowDims, colDims, queryConfig.measures]);

  const handleApplySuggestion = () => {
    if (suggestedVisual) {
      setVisualType(suggestedVisual);
    }
  };

  const handleRefreshSchema = async () => {
    setIsRefreshingSchema(true);
    try {
      const res = await fetch('/api/schema');
      const data = await res.json();
      if (data.error) throw new Error(data.error);
      useAppStore.getState().setSchema(data.schema);
    } catch (error: any) {
      alert(`Failed to refresh schema: ${error.message}`);
    } finally {
      setIsRefreshingSchema(false);
    }
  };

  const handleClear = () => {
    setQueryConfig({ dimensions: [], measures: [], filters: [] });
    setQueryResult([]);
    setVisualType('table');
    setSortConfig(null);
    setFilters({});
    setActiveFilterColumn(null);
    setColumnOrder([]);
    setColumnColors({});
    setSuggestedVisual(null);
    setRowDims([]);
    setColDims([]);
    setPreFilters([]);
    setQueryLimit('100');
    setAddMeasureFor(null);
  };

  const sensors = useSensors(
    useSensor(PointerSensor),
    useSensor(KeyboardSensor, {
      coordinateGetter: sortableKeyboardCoordinates,
    })
  );

  const handleDragEnd = (event: DragEndEvent, type: 'rowDims' | 'colDims' | 'measures' | 'columns') => {
    const { active, over } = event;
    if (!over || active.id === over.id) {
      setActiveDragColumn(null);
      return;
    }

    if (type === 'columns') {
      const oldIndex = columnOrder.indexOf(active.id as string);
      const newIndex = columnOrder.indexOf(over.id as string);
      setColumnOrder(arrayMove(columnOrder, oldIndex, newIndex));
      setActiveDragColumn(null);
    } else if (type === 'rowDims') {
      const oldIndex = rowDims.findIndex(i => i.name === active.id);
      const newIndex = rowDims.findIndex(i => i.name === over.id);
      setRowDims(arrayMove(rowDims, oldIndex, newIndex));
    } else if (type === 'colDims') {
      const oldIndex = colDims.findIndex(i => i.name === active.id);
      const newIndex = colDims.findIndex(i => i.name === over.id);
      setColDims(arrayMove(colDims, oldIndex, newIndex));
    } else if (type === 'measures') {
      const oldIndex = queryConfig.measures.findIndex((i: any) => i.column === active.id);
      const newIndex = queryConfig.measures.findIndex((i: any) => i.column === over.id);
      setQueryConfig({
        ...queryConfig,
        measures: arrayMove(queryConfig.measures as any[], oldIndex, newIndex),
      });
    }
  };

  const handleDragStart = (event: any) => {
    setActiveDragColumn(event.active.id);
  };

  const addRowDimension = (col: string) => {
    if (!rowDims.find(d => d.name === col) && !colDims.find(d => d.name === col)) {
      const newRowDims = [...rowDims, { name: col }];
      setRowDims(newRowDims);
      setQueryConfig({ ...queryConfig, dimensions: [...newRowDims, ...colDims] });
    }
  };

  const addColDimension = (col: string) => {
    if (!colDims.find(d => d.name === col) && !rowDims.find(d => d.name === col)) {
      const newColDims = [...colDims, { name: col }];
      setColDims(newColDims);
      setQueryConfig({ ...queryConfig, dimensions: [...rowDims, ...newColDims] });
    }
  };

  const addDimension = (col: string) => addRowDimension(col);

  const addMeasure = (col: string, agg: string = 'count') => {
    if (!queryConfig.measures.find((m: any) => m.column === col && m.agg === agg)) {
      setQueryConfig({ ...queryConfig, measures: [...queryConfig.measures, { column: col, agg }] });
    }
    setAddMeasureFor(null);
  };

  // Fetch query history (use currentUser.id or default to 1)
  const fetchHistory = async () => {
    setLoadingHistory(true);
    try {
      const userId = currentUser?.id ?? 1;
      const res = await fetch(`/api/history/${userId}`);
      const data = await res.json();
      setHistoryEntries(Array.isArray(data) ? data : []);
    } catch (e) {
      console.error(e);
    } finally {
      setLoadingHistory(false);
    }
  };

  const handleShowHistory = () => {
    setShowHistory(v => !v);
    if (!showHistory) fetchHistory();
  };

  // Replay a historical query
  const replayHistoryEntry = async (sql: string) => {
    setIsExecuting(true);
    setShowHistory(false);
    try {
      const res = await fetch('/api/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query: sql }),
      });
      const data = await res.json();
      if (data.error) throw new Error(data.error);
      setQueryResult(data.data);
    } catch (error: any) {
      alert(`Query Error: ${error.message}`);
    } finally {
      setIsExecuting(false);
    }
  };

  // Drill-through: run SELECT * — works with or without dimensions
  const handleDrillThrough = async (row: Record<string, unknown>) => {
    const allDims = [...rowDims, ...colDims];

    // No table context at all: just display row data directly
    if (!selectedTable && allDims.length === 0) {
      setDrillThrough({ visible: true, sql: '', data: [row], loading: false, error: null, title: 'Row Details' });
      return;
    }

    let drillSql: string;
    let title: string;

    if (allDims.length > 0) {
      // Builder mode with dimensions: filter by dim values (existing behavior)
      const dimFilters = allDims.map(d => ({ column: d.name, value: row[d.name] }));
      const where = buildWhereClause(dimFilters);
      drillSql = `SELECT * FROM \`${selectedTable}\`${where} LIMIT ${MAX_ROWS}`;
      title = allDims.map(d => `${d.name}=${row[d.name]}`).join(', ');
    } else {
      // No dims (AI chat result or plain table view): use up to 5 row values as filters
      const conditions: string[] = [];
      let filtersAdded = 0;
      for (const [col, val] of Object.entries(row)) {
        if (filtersAdded >= 5) break;
        if (val === null || val === undefined) {
          conditions.push(`\`${col}\` IS NULL`);
        } else {
          const isNum = !isNaN(Number(val)) && val !== '';
          const escaped = isNum ? String(val) : `'${String(val).replace(/'/g, "''")}'`;
          conditions.push(`\`${col}\` = ${escaped}`);
        }
        filtersAdded++;
      }
      const where = conditions.length > 0 ? ` WHERE ${conditions.join(' AND ')}` : '';
      drillSql = `SELECT * FROM \`${selectedTable}\`${where} LIMIT ${MAX_ROWS}`;
      title = Object.entries(row).slice(0, 3).map(([k, v]) => `${k}=${v}`).join(', ');
    }

    setDrillThrough({ visible: true, sql: drillSql, data: [], loading: true, error: null, title });
    try {
      const res = await fetch('/api/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query: drillSql }),
      });
      const data = await res.json();
      if (data.error) throw new Error(data.error);
      setDrillThrough(prev => ({ ...prev, loading: false, data: data.data }));
    } catch (error: any) {
      setDrillThrough(prev => ({ ...prev, loading: false, error: error.message }));
    }
  };

  // AI Query Analysis
  const handleRunAI = async () => {
    let sql: string;
    try { sql = buildSql(); } catch (e: any) { alert(e.message); return; }
    setAiAnalysis({ visible: true, loading: true, alerts: [], suggestions: [], projections: [], optimized_sql: '', risk_level: 'low', error: null, sql });
    try {
      const res = await fetch('/api/analyze', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql, schema }),
      });
      const data = await res.json();
      if (data.error) throw new Error(data.error);
      setAiAnalysis(prev => ({
        ...prev, loading: false,
        alerts: data.alerts || [],
        suggestions: data.suggestions || [],
        projections: data.projections || [],
        optimized_sql: data.optimized_sql || '',
        risk_level: data.risk_level || 'low',
      }));
    } catch (error: any) {
      setAiAnalysis(prev => ({ ...prev, loading: false, error: error.message }));
    }
  };

  const removeRowDimension = (col: string) => {
    const newRowDims = rowDims.filter(d => d.name !== col);
    setRowDims(newRowDims);
    setQueryConfig({ ...queryConfig, dimensions: [...newRowDims, ...colDims] });
  };

  const removeColDimension = (col: string) => {
    const newColDims = colDims.filter(d => d.name !== col);
    setColDims(newColDims);
    setQueryConfig({ ...queryConfig, dimensions: [...rowDims, ...newColDims] });
  };

  const removeDimension = (col: string) => {
    removeRowDimension(col);
    removeColDimension(col);
  };

  const removeMeasure = (col: string, agg: string) => {
    setQueryConfig({
      ...queryConfig,
      measures: queryConfig.measures.filter((m: any) => !(m.column === col && m.agg === agg))
    });
  };

  const addPreFilter = () => {
    setPreFilters(prev => [...prev, { id: Date.now().toString(), column: '', operator: '=', value: '' }]);
  };

  const updatePreFilter = (id: string, field: string, val: string) => {
    setPreFilters(prev => prev.map(f => f.id === id ? { ...f, [field]: val } : f));
  };

  const removePreFilter = (id: string) => {
    setPreFilters(prev => prev.filter(f => f.id !== id));
  };

  const buildWhereClause = useCallback((extraFilters?: { column: string; value: unknown }[]) => {
    const conditions: string[] = [];
    preFilters
      .filter(f => f.column && f.operator && (f.value !== '' || f.operator === 'IS NULL' || f.operator === 'IS NOT NULL'))
      .forEach(f => {
        if (f.operator === 'IS NULL') { conditions.push(`${f.column} IS NULL`); return; }
        if (f.operator === 'IS NOT NULL') { conditions.push(`${f.column} IS NOT NULL`); return; }
        const isLike = f.operator === 'LIKE' || f.operator === 'NOT LIKE';
        const isNum = !isLike && !isNaN(Number(f.value));
        const val = isNum ? f.value : `'${f.value.replace(/'/g, "''")}'`;
        conditions.push(`${f.column} ${f.operator} ${val}`);
      });
    if (extraFilters) {
      extraFilters.forEach(({ column, value }) => {
        if (value === null || value === undefined) {
          conditions.push(`${column} IS NULL`);
        } else {
          const isNum = !isNaN(Number(value));
          const val = isNum ? String(value) : `'${String(value).replace(/'/g, "''")}'`;
          conditions.push(`${column} = ${val}`);
        }
      });
    }
    return conditions.length > 0 ? ` WHERE ${conditions.join(' AND ')}` : '';
  }, [preFilters]);

  const buildSql = useCallback(() => {
    const table = selectedTable;
    if (!table) throw new Error("No table selected");
    const allDims = [...rowDims, ...colDims];
    const selects = [
      ...allDims.map(d => d.name),
      ...queryConfig.measures.map((m: any) => measureSql(m))
    ];
    if (selects.length === 0) throw new Error("Add at least one dimension or measure");
    let sql = `SELECT ${selects.join(', ')} FROM ${table}`;
    sql += buildWhereClause();
    if (allDims.length > 0 && queryConfig.measures.length > 0) {
      sql += ` GROUP BY ${allDims.map(d => d.name).join(', ')}`;
    }
    const lim = Math.min(parseInt(queryLimit, 10) || 100, MAX_ROWS);
    sql += ` LIMIT ${lim}`;
    return sql;
  }, [selectedTable, rowDims, colDims, queryConfig.measures, queryLimit, buildWhereClause]);

  const buildAndExecuteQuery = async () => {
    const allDims = [...rowDims, ...colDims];
    if (allDims.length === 0 && queryConfig.measures.length === 0) return;
    setIsExecuting(true);
    try {
      const sql = buildSql();
      const res = await fetch('/api/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query: sql }),
      });
      const data = await res.json();
      if (data.error) throw new Error(data.error);
      setQueryResult(data.data);
      // Auto-switch to matrix when colDims are set
      if (colDims.length > 0) setVisualType('matrix');
      // Save to history
      if (currentUser) {
        fetch('/api/history', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ user_id: currentUser.id, query_text: 'Built via Visual Builder', sql }),
        }).catch(console.error);
      }
    } catch (error: any) {
      alert(`Query Error: ${error.message}`);
    } finally {
      setIsExecuting(false);
    }
  };

  const handleSaveToDashboard = async () => {
    if (!currentUser) return alert("Please select a user first");
    const name = prompt("Enter a name for this visualization:");
    if (!name) return;

    setIsSaving(true);
    try {
      const sql = buildSql();

      await fetch('/api/saved_queries', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          user_id: currentUser.id,
          name,
          sql,
          config: queryConfig,
          visual_type: visualType
        }),
      });
      
      // Refresh saved queries
      const res = await fetch(`/api/saved_queries/${currentUser.id}`);
      const data = await res.json();
      setSavedQueries(data);
      alert("Saved to dashboard!");
    } catch (e) {
      console.error(e);
      alert("Failed to save");
    } finally {
      setIsSaving(false);
    }
  };

  // ---------------------------------------------------------------------------
  // AI Explore handlers
  // ---------------------------------------------------------------------------
  const openAIExplore = () => {
    setAiExplore(prev => ({
      ...prev,
      visible: true,
      targetTable: selectedTable || (tables.length > 0 ? tables[0] : ''),
      stats: null,
      insights: null,
      error: null,
      loading: false,
      insightsLoading: false,
    }));
  };

  const runProfileTable = async (tableName: string) => {
    if (!tableName) return;
    setAiExplore(prev => ({ ...prev, loading: true, stats: null, insights: null, error: null, insightsLoading: false }));
    try {
      const res = await fetch(`/api/profile/${encodeURIComponent(tableName)}`);
      const data = await res.json();
      if (data.error) throw new Error(data.error);
      setAiExplore(prev => ({ ...prev, loading: false, stats: data, insightsLoading: true }));
      // Now fetch LLM insights
      const insRes = await fetch(`/api/profile/${encodeURIComponent(tableName)}/insights`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ stats: data }),
      });
      const insData = await insRes.json();
      setAiExplore(prev => ({
        ...prev,
        insightsLoading: false,
        insights: insData.error ? null : insData,
      }));
    } catch (err: any) {
      setAiExplore(prev => ({ ...prev, loading: false, insightsLoading: false, error: err.message }));
    }
  };

  // ---------------------------------------------------------------------------
  // Matrix (pivot) renderer
  // ---------------------------------------------------------------------------
  const renderMatrix = () => {
    if (!queryResult || queryResult.length === 0) return null;
    if (rowDims.length === 0 || colDims.length === 0 || queryConfig.measures.length === 0) {
      return <p className="text-sm text-slate-500 italic p-4">Matrix requires at least one Row dimension, one Column dimension, and one Measure.</p>;
    }
    const measureKeys = queryResult.length > 0
      ? Object.keys(queryResult[0]).filter(k => !rowDims.find(d => d.name === k) && !colDims.find(d => d.name === k))
      : [];
    // Build unique col header values (combination of colDim values)
    const colKey = (row: any): string => colDims.map(d => row[d.name]).join(' / ');
    const rowKey = (row: any): string => rowDims.map(d => row[d.name]).join(' / ');
    const uniqueColValues: string[] = Array.from(new Set(queryResult.map(colKey)));
    const uniqueRowValues: string[] = Array.from(new Set(queryResult.map(rowKey)));
    // Build lookup map
    const lookup: Record<string, Record<string, any>> = {};
    queryResult.forEach(row => {
      const rk = rowKey(row);
      const ck = colKey(row);
      if (!lookup[rk]) lookup[rk] = {};
      lookup[rk][ck] = row;
    });
    return (
      <div className="flex-1 border border-slate-200 rounded-lg bg-white shadow-sm min-h-0" style={{ overflowX: 'scroll', overflowY: 'auto', maxHeight: '100%' }}>
        <table className="text-sm text-left" style={{ borderCollapse: 'separate', borderSpacing: 0 }}>
          <thead className="bg-slate-50 sticky top-0 z-10">
            <tr>
              <th className="w-7 px-2 py-3 border-b border-r border-slate-200 bg-slate-100" />
              {rowDims.map(d => (
                <th key={d.name} className="px-4 py-3 text-xs font-bold uppercase tracking-wider border-b border-r border-slate-200 text-slate-600 bg-slate-100">{d.name}</th>
              ))}
              {uniqueColValues.map(cv => (
                <th key={cv} className="px-4 py-3 text-xs font-bold uppercase tracking-wider border-b border-r border-slate-200 text-purple-700 bg-purple-50 text-center" colSpan={measureKeys.length}>
                  {cv}
                </th>
              ))}
            </tr>
            {measureKeys.length > 1 && (
              <tr className="bg-slate-50">
                <th className="w-7 border-b border-r border-slate-100" />
                {rowDims.map(d => <th key={d.name} className="border-b border-r border-slate-100" />)}
                {uniqueColValues.flatMap(cv =>
                  measureKeys.map(mk => (
                    <th key={`${cv}-${mk}`} className="px-2 py-1 text-[10px] text-slate-500 font-medium text-center border-b border-slate-100 uppercase">{mk}</th>
                  ))
                )}
              </tr>
            )}
          </thead>
          <tbody className="divide-y divide-slate-200">
            {uniqueRowValues.map((rv, _ri) => (
              <tr
                key={rv}
                className="hover:bg-slate-50 cursor-context-menu group"
                onContextMenu={(e) => {
                  e.preventDefault();
                  const rowData = lookup[rv]?.[uniqueColValues[0]] || {};
                  handleDrillThrough(rowData);
                }}
                title="Right-click or click → for drill-through"
              >
                <td className="px-2 py-2.5 border-r border-slate-100 w-7">
                  <button
                    onClick={() => { const rowData = lookup[rv]?.[uniqueColValues[0]] || {}; handleDrillThrough(rowData); }}
                    className="opacity-0 group-hover:opacity-100 transition-opacity p-0.5 rounded hover:bg-emerald-100 text-emerald-500"
                    title="Go to detail"
                  >
                    <ChevronRight size={14} />
                  </button>
                </td>
                {rowDims.map(d => {
                  const val = queryResult.find(r => rowKey(r) === rv)?.[d.name];
                  return <td key={d.name} className="px-4 py-2.5 font-medium text-slate-700 border-r border-slate-100 whitespace-nowrap">{formatCellValue(val)}</td>;
                })}
                {uniqueColValues.flatMap(cv =>
                  measureKeys.map(mk => {
                    const cellData = lookup[rv]?.[cv];
                    const val = cellData ? cellData[mk] : null;
                    return (
                      <td key={`${cv}-${mk}`} className="px-4 py-2.5 text-center text-slate-600 tabular-nums whitespace-nowrap border-r border-slate-200">
                        {val !== null && val !== undefined ? formatCellValue(val) : <span className="text-slate-300 text-xs">—</span>}
                      </td>
                    );
                  })
                )}
              </tr>
            ))}
          </tbody>
        </table>
        <div className="px-4 py-2 text-xs text-slate-400 border-t border-slate-100 bg-slate-50">
          {uniqueRowValues.length} rows × {uniqueColValues.length} columns — Right-click a row for drill-through
        </div>
      </div>
    );
  };

  const renderVisual = () => {
    if (!queryResult || queryResult.length === 0) {
      return (
        <div className="flex flex-col items-center justify-center h-64 text-slate-400">
          <Table size={48} className="mb-4 opacity-20" />
          <p>No data to display. Run a query first.</p>
        </div>
      );
    }

    const keys = columnOrder.length > 0 ? columnOrder : Object.keys(queryResult[0]);
    const dimKey = rowDims[0]?.name || colDims[0]?.name || keys[0];
    const measureKeys = keys.filter(k => k !== dimKey);

    // Apply sorting and filtering
    let processedData = [...queryResult];
    
    // Filter
    Object.entries(filters).forEach(([key, value]) => {
      if (value && typeof value === 'string') {
        const lowerValue = value.toLowerCase();
        processedData = processedData.filter(row => {
          const cellValue = row[key];
          if (cellValue == null) return false;
          return String(cellValue).toLowerCase().includes(lowerValue);
        });
      }
    });

    // Sort
    if (sortConfig) {
      processedData.sort((a, b) => {
        const aVal = a[sortConfig.key];
        const bVal = b[sortConfig.key];
        
        if (aVal === bVal) return 0;
        
        const aIsNum = !isNaN(Number(aVal)) && aVal !== null && aVal !== '';
        const bIsNum = !isNaN(Number(bVal)) && bVal !== null && bVal !== '';
        
        let comparison = 0;
        if (aIsNum && bIsNum) {
          comparison = Number(aVal) - Number(bVal);
        } else {
          comparison = String(aVal || '').localeCompare(String(bVal || ''));
        }
        
        return sortConfig.direction === 'asc' ? comparison : -comparison;
      });
    }

    const handleSort = (key: string) => {
      let direction: 'asc' | 'desc' = 'asc';
      if (sortConfig && sortConfig.key === key && sortConfig.direction === 'asc') {
        direction = 'desc';
      }
      setSortConfig({ key, direction });
    };

    const tooltipStyle = { borderRadius: '8px', border: 'none', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)', fontSize: '12px' };

    switch (visualType) {
      case 'matrix':
        return renderMatrix();

      case 'bar':
        return (
          <div className="overflow-x-auto overflow-y-hidden min-h-0 flex-1 min-w-0">
            <div style={{ minWidth: Math.max(600, processedData.length * 40) }}>
              <ResponsiveContainer width="100%" height={400}>
                <BarChart data={processedData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                  <XAxis dataKey={dimKey} axisLine={false} tickLine={false} tick={{ fill: '#64748b', fontSize: 12 }} />
                  <YAxis axisLine={false} tickLine={false} tick={{ fill: '#64748b', fontSize: 12 }} />
                  <RechartsTooltip cursor={{ fill: '#f1f5f9' }} contentStyle={tooltipStyle} />
                  <Legend iconType="circle" wrapperStyle={{ fontSize: '12px' }} />
                  {measureKeys.map((key, i) => (
                    <Bar key={key} dataKey={key} fill={CHART_COLORS[i % CHART_COLORS.length]} radius={[4, 4, 0, 0]} />
                  ))}
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>
        );

      case 'line':
        return (
          <div className="overflow-x-auto overflow-y-hidden min-h-0 flex-1 min-w-0">
            <div style={{ minWidth: Math.max(600, processedData.length * 40) }}>
              <ResponsiveContainer width="100%" height={400}>
                <RechartsLineChart data={processedData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                  <XAxis dataKey={dimKey} axisLine={false} tickLine={false} tick={{ fill: '#64748b', fontSize: 12 }} />
                  <YAxis axisLine={false} tickLine={false} tick={{ fill: '#64748b', fontSize: 12 }} />
                  <RechartsTooltip contentStyle={tooltipStyle} />
                  <Legend iconType="circle" wrapperStyle={{ fontSize: '12px' }} />
                  {measureKeys.map((key, i) => (
                    <Line key={key} type="monotone" dataKey={key} stroke={CHART_COLORS[i % CHART_COLORS.length]} strokeWidth={3} dot={{ r: 4, strokeWidth: 2 }} activeDot={{ r: 6 }} />
                  ))}
                </RechartsLineChart>
              </ResponsiveContainer>
            </div>
          </div>
        );

      case 'area':
        return (
          <div className="overflow-x-auto overflow-y-hidden min-h-0 flex-1 min-w-0">
            <div style={{ minWidth: Math.max(600, processedData.length * 40) }}>
              <ResponsiveContainer width="100%" height={400}>
                <RechartsAreaChart data={processedData}>
                  <defs>
                    {measureKeys.map((key, i) => (
                      <linearGradient key={key} id={`grad-${i}`} x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor={CHART_COLORS[i % CHART_COLORS.length]} stopOpacity={0.3} />
                        <stop offset="95%" stopColor={CHART_COLORS[i % CHART_COLORS.length]} stopOpacity={0} />
                      </linearGradient>
                    ))}
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                  <XAxis dataKey={dimKey} axisLine={false} tickLine={false} tick={{ fill: '#64748b', fontSize: 12 }} />
                  <YAxis axisLine={false} tickLine={false} tick={{ fill: '#64748b', fontSize: 12 }} />
                  <RechartsTooltip contentStyle={tooltipStyle} />
                  <Legend iconType="circle" wrapperStyle={{ fontSize: '12px' }} />
                  {measureKeys.map((key, i) => (
                    <Area key={key} type="monotone" dataKey={key} stroke={CHART_COLORS[i % CHART_COLORS.length]} strokeWidth={2} fill={`url(#grad-${i})`} />
                  ))}
                </RechartsAreaChart>
              </ResponsiveContainer>
            </div>
          </div>
        );

      case 'pie':
        return (
          <div className="overflow-auto min-h-0 flex-1">
            <ResponsiveContainer width="100%" height={420}>
              <RechartsPieChart>
                <Pie data={processedData} dataKey={measureKeys[0]} nameKey={dimKey} cx="50%" cy="50%" outerRadius={150} innerRadius={80} paddingAngle={2}>
                  {processedData.map((_entry, index) => (
                    <Cell key={`cell-${index}`} fill={CHART_COLORS[index % CHART_COLORS.length]} />
                  ))}
                </Pie>
                <RechartsTooltip contentStyle={tooltipStyle} />
                <Legend iconType="circle" wrapperStyle={{ fontSize: '12px' }} />
              </RechartsPieChart>
            </ResponsiveContainer>
          </div>
        );

      case 'scatter':
        return (
          <div className="overflow-auto min-h-0 flex-1">
            <ResponsiveContainer width="100%" height={420}>
              <RechartsScatterChart>
                <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                <XAxis dataKey={measureKeys[0] || dimKey} name={measureKeys[0] || dimKey} axisLine={false} tickLine={false} tick={{ fill: '#64748b', fontSize: 12 }} />
                <YAxis dataKey={measureKeys[1] || measureKeys[0]} name={measureKeys[1] || measureKeys[0]} axisLine={false} tickLine={false} tick={{ fill: '#64748b', fontSize: 12 }} />
                <RechartsTooltip cursor={{ strokeDasharray: '3 3' }} contentStyle={tooltipStyle} />
                <Scatter data={processedData} fill={CHART_COLORS[0]} fillOpacity={0.7} />
              </RechartsScatterChart>
            </ResponsiveContainer>
          </div>
        );

      case 'radar':
        return (
          <div className="overflow-auto min-h-0 flex-1">
            <ResponsiveContainer width="100%" height={420}>
              <RadarChart data={processedData} cx="50%" cy="50%" outerRadius={150}>
                <PolarGrid stroke="#e2e8f0" />
                <PolarAngleAxis dataKey={dimKey} tick={{ fill: '#64748b', fontSize: 12 }} />
                <PolarRadiusAxis axisLine={false} tick={{ fill: '#94a3b8', fontSize: 10 }} />
                {measureKeys.map((key, i) => (
                  <Radar key={key} name={key} dataKey={key} stroke={CHART_COLORS[i % CHART_COLORS.length]} fill={CHART_COLORS[i % CHART_COLORS.length]} fillOpacity={0.2} />
                ))}
                <Legend iconType="circle" wrapperStyle={{ fontSize: '12px' }} />
                <RechartsTooltip contentStyle={tooltipStyle} />
              </RadarChart>
            </ResponsiveContainer>
          </div>
        );

      case 'table':
      default: {
        const hasActiveFilters = Object.values(filters).some(v => v);
        const showFilterRow = activeFilterColumn !== null || hasActiveFilters;

        return (
          <div className="flex flex-col h-full">
            {/* Color Picker Toolbar */}
            <div className="flex flex-wrap items-center gap-2 mb-3 p-2 bg-slate-50 border border-slate-200 rounded-lg shrink-0">
              <span className="text-xs font-semibold text-slate-500 uppercase tracking-wider flex items-center gap-1">
                <Palette size={14} /> Style Columns:
              </span>
              {keys.map(key => (
                <div key={key} className="relative">
                  <button
                    onClick={() => setShowColorPicker(showColorPicker === key ? null : key)}
                    className={clsx(
                      "text-xs px-2 py-1 rounded border transition-colors",
                      showColorPicker === key ? "bg-emerald-100 border-emerald-300 text-emerald-700" : "bg-white border-slate-200 text-slate-600 hover:bg-slate-100"
                    )}
                  >
                    {key}
                  </button>
                  {showColorPicker === key && (
                    <div className="absolute top-full left-0 mt-1 p-3 bg-white border border-slate-200 rounded-lg shadow-xl z-50 flex flex-col gap-3 min-w-[180px]">
                      <div>
                        <label className="text-[10px] font-bold text-slate-500 uppercase mb-1 block">Background Color</label>
                        <div className="flex items-center gap-2">
                          <input
                            type="color"
                            value={columnColors[key]?.bg || '#ffffff'}
                            onChange={(e) => setColumnColors(prev => ({ ...prev, [key]: { ...prev[key], bg: e.target.value } }))}
                            className="w-8 h-8 cursor-pointer rounded border border-slate-200 p-0"
                          />
                          <span className="text-xs text-slate-500 font-mono">{columnColors[key]?.bg || '#ffffff'}</span>
                        </div>
                      </div>
                      <div>
                        <label className="text-[10px] font-bold text-slate-500 uppercase mb-1 block">Text Color</label>
                        <div className="flex items-center gap-2">
                          <input
                            type="color"
                            value={columnColors[key]?.text || '#000000'}
                            onChange={(e) => setColumnColors(prev => ({ ...prev, [key]: { ...prev[key], text: e.target.value } }))}
                            className="w-8 h-8 cursor-pointer rounded border border-slate-200 p-0"
                          />
                          <span className="text-xs text-slate-500 font-mono">{columnColors[key]?.text || '#000000'}</span>
                        </div>
                      </div>
                      <button
                        onClick={() => {
                          const newColors = { ...columnColors };
                          delete newColors[key];
                          setColumnColors(newColors);
                          setShowColorPicker(null);
                        }}
                        className="text-xs text-red-600 hover:bg-red-50 border border-red-200 p-1.5 rounded mt-1 font-medium transition-colors"
                      >
                        Reset to Default
                      </button>
                    </div>
                  )}
                </div>
              ))}
            </div>

            {/*
             * Fix: wrap the whole table (not just <tr>) with the column DndContext
             * so DragOverlay can be placed outside the <table> element.
             * A <th> rendered by DragOverlay outside a <table> is invalid HTML
             * and causes layout issues — we use a <div> in the overlay instead.
             */}
            <DndContext
              sensors={sensors}
              collisionDetection={closestCenter}
              onDragStart={handleDragStart}
              onDragEnd={(e) => handleDragEnd(e, 'columns')}
            >
              {/* Custom Table — overflow-x: scroll ensures scrollbar is always visible at the bottom */}
              <div className="flex-1 border border-slate-200 rounded-lg bg-white shadow-sm min-h-0" style={{ overflowX: 'scroll', overflowY: 'auto', maxHeight: '100%' }}>
                <table className="min-w-max text-sm text-left" style={{ borderCollapse: 'separate', borderSpacing: 0 }}>
                  <thead className="bg-slate-50 sticky top-0 z-20 shadow-sm">
                    <tr>
                      <th className="w-7 px-2 py-3 border-b border-r border-slate-200 bg-slate-50" />
                      <SortableContext items={keys} strategy={horizontalListSortingStrategy}>
                        {keys.map(key => (
                          <SortableHeader
                            key={key}
                            id={key}
                            column={key}
                            sortConfig={sortConfig}
                            onSort={handleSort}
                            onFilterClick={(col) => setActiveFilterColumn(activeFilterColumn === col ? null : col)}
                            colors={columnColors[key]}
                            isFiltered={!!filters[key]}
                          />
                        ))}
                      </SortableContext>
                    </tr>
                    {/* Inline filter row — avoids floating popup positioning bugs */}
                    {showFilterRow && (
                      <tr className="bg-white border-b border-slate-100">
                        <th className="w-7 px-2" />
                        {keys.map(key => (
                          <th key={key} className="px-2 py-1.5">
                            <input
                              type="text"
                              placeholder={`Filter ${key}…`}
                              value={filters[key] || ''}
                              onChange={(e) => setFilters({ ...filters, [key]: e.target.value })}
                              autoFocus={activeFilterColumn === key}
                              className="w-full text-xs p-1.5 border border-slate-200 rounded focus:ring-1 focus:ring-emerald-500 outline-none min-w-[60px] font-normal"
                            />
                          </th>
                        ))}
                      </tr>
                    )}
                  </thead>
                  <tbody className="divide-y divide-slate-200">
                    {processedData.length > 0 ? (
                      processedData.map((row, i) => (
                        <tr
                          key={i}
                          className="hover:bg-slate-50 transition-colors cursor-context-menu group"
                          onContextMenu={(e) => { e.preventDefault(); handleDrillThrough(row); }}
                          title="Right-click or click → for row details"
                        >
                          {/* Drill-through icon — visible on hover */}
                          <td className="px-2 py-2.5 border-r border-slate-100 w-7 shrink-0">
                            <button
                              onClick={() => handleDrillThrough(row)}
                              className="opacity-0 group-hover:opacity-100 transition-opacity p-0.5 rounded hover:bg-emerald-100 text-emerald-500"
                              title="Go to detail"
                            >
                              <ChevronRight size={14} />
                            </button>
                          </td>
                          {keys.map(key => {
                            const col = columnColors[key];
                            const style = col ? {
                              backgroundColor: col.bg ? hexToRgba(col.bg, 0.15) : undefined,
                              color: col.text || undefined,
                            } : {};
                            return (
                              <td key={key} className="px-4 py-2.5 whitespace-nowrap text-slate-600 border-r border-slate-200" style={style}>
                                {row[key] !== null && row[key] !== undefined ? formatCellValue(row[key]) : <span className="text-slate-300 italic">null</span>}
                              </td>
                            );
                          })}
                        </tr>
                      ))
                    ) : (
                      <tr>
                        <td colSpan={keys.length + 1} className="px-4 py-8 text-center text-slate-500 italic">
                          No results match the current filters.
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>

              {/*
               * Fix: DragOverlay is now outside the <table> so its portal-rendered
               * element is semantically valid. We render a <div> (not <th>).
               * Fix: dropAnimation must be a DropAnimation config object —
               * defaultDropAnimationSideEffects returns a SideEffect function and
               * must be assigned to the `sideEffects` key, not used directly.
               */}
              <DragOverlay dropAnimation={{ sideEffects: defaultDropAnimationSideEffects({ styles: { active: { opacity: '0.5' } } }) }}>
                {activeDragColumn ? (
                  <div className="px-4 py-3 text-left text-xs font-bold uppercase tracking-wider border border-emerald-500 bg-emerald-50 text-emerald-700 shadow-lg rounded opacity-90">
                    {activeDragColumn}
                  </div>
                ) : null}
              </DragOverlay>
            </DndContext>

            {/* Status Bar */}
            <div className="mt-2 text-xs text-slate-500 flex justify-between items-center shrink-0">
              <span>Showing {processedData.length} of {queryResult.length} rows</span>
              {hasActiveFilters && (
                <button
                  onClick={() => { setFilters({}); setActiveFilterColumn(null); }}
                  className="text-emerald-600 hover:underline flex items-center gap-1"
                >
                  <X size={12} /> Clear all filters
                </button>
              )}
            </div>
          </div>
        );
      }
    }
  };

  return (
    <div className="flex flex-col h-full bg-slate-50">
      {/* Top Configuration Bar — shrink-0 prevents it from being squashed by the results area */}
      <div className="bg-white border-b border-slate-200 p-4 space-y-3 shrink-0">
        <div className="flex items-center justify-between gap-4">
          {/* Left: title + utility buttons */}
          <div className="flex items-center gap-2 flex-wrap">
            <h2 className="text-lg font-semibold text-slate-800 shrink-0">Visual Builder</h2>
            <div className="w-px h-5 bg-slate-200 shrink-0" />
            <button
              onClick={handleClear}
              disabled={queryConfig.dimensions.length === 0 && queryConfig.measures.length === 0 && (!queryResult || queryResult.length === 0)}
              className="flex items-center gap-1.5 bg-white border border-slate-200 hover:bg-red-50 hover:border-red-200 hover:text-red-600 text-slate-500 px-3 py-1.5 rounded-lg text-sm font-medium transition-colors disabled:opacity-40 disabled:cursor-not-allowed shadow-sm"
              title="Clear all"
            >
              <RotateCcw size={14} />
              Clear
            </button>
            <button
              onClick={handleShowHistory}
              className={clsx(
                "flex items-center gap-1.5 border px-3 py-1.5 rounded-lg text-sm font-medium transition-colors shadow-sm",
                showHistory ? "bg-slate-800 border-slate-700 text-white" : "bg-white border-slate-200 text-slate-600 hover:bg-slate-50"
              )}
              title="Query History"
            >
              <History size={14} />
              History
            </button>
            <button
              onClick={openAIExplore}
              className="flex items-center gap-1.5 bg-gradient-to-r from-indigo-500 to-purple-600 hover:from-indigo-600 hover:to-purple-700 text-white px-3 py-1.5 rounded-lg text-sm font-medium transition-all shadow-sm"
              title="AI Explore — profile a table with AI"
            >
              <Telescope size={14} />
              AI Explore
            </button>
          </div>
          {/* Right: run buttons */}
          <div className="flex items-center gap-2 shrink-0">
            <button
              onClick={handleRunAI}
              disabled={[...rowDims, ...colDims].length === 0 && queryConfig.measures.length === 0}
              className="flex items-center gap-2 bg-violet-500 hover:bg-violet-600 text-white px-4 py-2 rounded-lg text-sm font-medium transition-colors disabled:opacity-50 disabled:cursor-not-allowed shadow-sm"
              title="Analyze query with AI before running"
            >
              <Brain size={16} />
              Run AI
            </button>
            <button
              onClick={buildAndExecuteQuery}
              disabled={isExecuting || ([...rowDims, ...colDims].length === 0 && queryConfig.measures.length === 0)}
              className="flex items-center gap-2 bg-emerald-500 hover:bg-emerald-600 text-white px-4 py-2 rounded-lg text-sm font-medium transition-colors disabled:opacity-50 disabled:cursor-not-allowed shadow-sm"
            >
              <Play size={16} />
              {isExecuting ? 'Running...' : 'Run Query'}
            </button>
          </div>
        </div>

        {/* Dimensions + Measures — 3 columns */}
        <div className="grid grid-cols-3 gap-3">
          {/* Rows Dropzone */}
          <div className="bg-slate-50 border border-slate-200 rounded-xl p-3">
            <div className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-2 flex items-center gap-1">
              <span className="w-2 h-2 rounded-full bg-emerald-400 inline-block"></span> Rows
            </div>
            <DndContext sensors={sensors} collisionDetection={closestCenter} onDragEnd={(e) => handleDragEnd(e, 'rowDims')}>
              <SortableContext items={rowDims.map(d => d.name)} strategy={verticalListSortingStrategy}>
                <div className="min-h-[40px] max-h-24 overflow-y-auto flex flex-wrap gap-2 pr-0.5">
                  {rowDims.length === 0 && (
                    <div className="text-sm text-slate-400 italic w-full text-center py-2 border-2 border-dashed border-slate-200 rounded-lg">
                      Drag fields here
                    </div>
                  )}
                  {rowDims.map((dim) => (
                    <SortableItem key={dim.name} id={dim.name} item={dim} onRemove={() => removeRowDimension(dim.name)} />
                  ))}
                </div>
              </SortableContext>
            </DndContext>
          </div>

          {/* Columns Dropzone */}
          <div className="bg-slate-50 border border-purple-200 rounded-xl p-3">
            <div className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-2 flex items-center gap-1">
              <span className="w-2 h-2 rounded-full bg-purple-400 inline-block"></span> Columns
            </div>
            <DndContext sensors={sensors} collisionDetection={closestCenter} onDragEnd={(e) => handleDragEnd(e, 'colDims')}>
              <SortableContext items={colDims.map(d => d.name)} strategy={verticalListSortingStrategy}>
                <div className="min-h-[40px] max-h-24 overflow-y-auto flex flex-wrap gap-2 pr-0.5">
                  {colDims.length === 0 && (
                    <div className="text-sm text-slate-400 italic w-full text-center py-2 border-2 border-dashed border-purple-100 rounded-lg">
                      Drag fields here
                    </div>
                  )}
                  {colDims.map((dim) => (
                    <SortableItem key={dim.name} id={dim.name} item={dim} onRemove={() => removeColDimension(dim.name)} />
                  ))}
                </div>
              </SortableContext>
            </DndContext>
          </div>

          {/* Measures Dropzone */}
          <div className="bg-slate-50 border border-slate-200 rounded-xl p-3">
            <div className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-2 flex items-center gap-1">
              <span className="w-2 h-2 rounded-full bg-blue-400 inline-block"></span> Measures (Values)
            </div>
            <DndContext sensors={sensors} collisionDetection={closestCenter} onDragEnd={(e) => handleDragEnd(e, 'measures')}>
              <SortableContext items={queryConfig.measures.map((m: any) => m.column)} strategy={verticalListSortingStrategy}>
                <div className="min-h-[40px] max-h-24 overflow-y-auto flex flex-wrap gap-2 pr-0.5">
                  {queryConfig.measures.length === 0 && (
                    <div className="text-sm text-slate-400 italic w-full text-center py-2 border-2 border-dashed border-slate-200 rounded-lg">
                      Drag fields here
                    </div>
                  )}
                  {queryConfig.measures.map((measure: any) => (
                    <SortableItem key={measure.column} id={measure.column} item={measure} onRemove={() => removeMeasure(measure.column, measure.agg)} />
                  ))}
                </div>
              </SortableContext>
            </DndContext>
          </div>
        </div>

        {/* Filters + Limit row */}
        <div className="flex flex-wrap items-center gap-2 bg-slate-50 border border-slate-200 rounded-xl px-3 py-2">
          <span className="text-xs font-semibold text-slate-400 uppercase tracking-wider shrink-0">WHERE</span>
          {preFilters.map(f => (
            <div key={f.id} className="flex items-center gap-1 bg-white border border-slate-200 rounded-lg px-2 py-1 shadow-sm">
              <select
                value={f.column}
                onChange={e => updatePreFilter(f.id, 'column', e.target.value)}
                className="text-xs border-none outline-none bg-transparent text-slate-700 max-w-[110px] cursor-pointer"
              >
                <option value="">column…</option>
                {selectedTable && schema[selectedTable]?.map((col: any) => (
                  <option key={col.name} value={col.name}>{col.name}</option>
                ))}
              </select>
              <select
                value={f.operator}
                onChange={e => updatePreFilter(f.id, 'operator', e.target.value)}
                className="text-xs border-none outline-none bg-transparent text-emerald-700 font-mono cursor-pointer"
              >
                {['=', '!=', '>', '<', '>=', '<=', 'LIKE', 'NOT LIKE', 'IS NULL', 'IS NOT NULL'].map(op => (
                  <option key={op} value={op}>{op}</option>
                ))}
              </select>
              {!['IS NULL', 'IS NOT NULL'].includes(f.operator) && (
                <input
                  type="text"
                  value={f.value}
                  onChange={e => updatePreFilter(f.id, 'value', e.target.value)}
                  placeholder="value…"
                  className="text-xs border-none outline-none bg-transparent text-slate-700 w-20 placeholder:text-slate-300"
                />
              )}
              <button onClick={() => removePreFilter(f.id)} className="text-slate-300 hover:text-red-500 transition-colors ml-0.5">
                <X size={12} />
              </button>
            </div>
          ))}
          <button
            onClick={addPreFilter}
            className="flex items-center gap-1 text-xs text-emerald-600 hover:text-emerald-700 bg-emerald-50 hover:bg-emerald-100 border border-emerald-200 px-2 py-1 rounded-lg transition-colors font-medium shrink-0"
          >
            <Plus size={12} /> Filter
          </button>
          <div className="ml-auto flex items-center gap-2 shrink-0">
            <span className="text-xs font-semibold text-slate-400 uppercase tracking-wider">LIMIT</span>
            <input
              type="number"
              min="1"
              value={queryLimit}
              onChange={e => setQueryLimit(e.target.value)}
              placeholder="∞"
              className="text-xs w-20 px-2 py-1 border border-slate-200 rounded-lg focus:ring-1 focus:ring-emerald-500 outline-none bg-white text-slate-700"
            />
          </div>
        </div>
      </div>

      {/* Main Content Area */}
      <div className="flex-1 flex overflow-hidden">
        {/* Schema Sidebar */}
        <div className="w-80 bg-white border-r border-slate-200 flex flex-col h-full">
          {/* Table Selector */}
          <div className="p-4 border-b border-slate-200 shrink-0">
            <div className="flex items-center justify-between mb-2">
              <h3 className="text-sm font-semibold text-slate-800">Select Table</h3>
              <button
                onClick={handleRefreshSchema}
                disabled={isRefreshingSchema}
                className="p-1.5 text-slate-400 hover:text-emerald-600 hover:bg-emerald-50 rounded-md transition-colors disabled:opacity-50"
                title="Refresh tables"
              >
                <RefreshCw size={14} className={isRefreshingSchema ? "animate-spin" : ""} />
              </button>
            </div>
            {/* Table search */}
            <div className="relative mb-2">
              <Search size={12} className="absolute left-2 top-1/2 -translate-y-1/2 text-slate-400 pointer-events-none" />
              <input
                type="text"
                placeholder="Search tables…"
                value={tableSearch}
                onChange={(e) => setTableSearch(e.target.value)}
                className="w-full text-xs pl-6 pr-6 py-1.5 border border-slate-200 rounded-md focus:ring-1 focus:ring-emerald-500 outline-none bg-slate-50"
              />
              {tableSearch && (
                <button onClick={() => setTableSearch('')} className="absolute right-2 top-1/2 -translate-y-1/2 text-slate-400 hover:text-slate-600">
                  <X size={11} />
                </button>
              )}
            </div>
            <div className="max-h-[38px] overflow-y-auto border border-slate-200 rounded-lg bg-slate-50">
              {tables.filter(t => !tableSearch || t.toLowerCase().includes(tableSearch.toLowerCase())).map(t => (
                <div
                  key={t}
                  onClick={() => handleTableSelect(t)}
                  className={clsx("px-3 py-1.5 text-sm cursor-pointer flex items-center justify-between hover:bg-slate-100 border-b border-slate-100 last:border-0", selectedTable === t && "bg-emerald-50 text-emerald-700 font-medium")}
                >
                  <span className="truncate">{t}</span>
                  <button
                    onClick={(e) => { e.stopPropagation(); toggleFavorite(t); }}
                    className={clsx("shrink-0 p-1 rounded hover:bg-slate-200", tableMetadata[t]?.is_favorite ? "text-amber-400" : "text-slate-300")}
                  >
                    <Star size={14} fill={tableMetadata[t]?.is_favorite ? "currentColor" : "none"} />
                  </button>
                </div>
              ))}
            </div>
          </div>

          {/* Columns & Measures */}
          <div className="flex-1 overflow-y-auto p-4">
            {selectedTable && schema[selectedTable] && (
              <>
                {/* Suggested Measures */}
                <div className="mb-6">
                  <div className="text-xs font-bold text-slate-400 uppercase tracking-wider mb-2">Suggested Measures</div>
                  <div className="flex flex-wrap gap-2">
                    <button onClick={() => addMeasure('*', 'count')} className="text-xs font-medium bg-emerald-100 text-emerald-700 px-2.5 py-1.5 rounded-md hover:bg-emerald-200 transition-colors border border-emerald-200 shadow-sm">
                      Count *
                    </button>
                    {schema[selectedTable].filter((c: any) => c.type.includes('Int') || c.type.includes('Float') || c.type.includes('Decimal')).slice(0, 3).map((col: any) => (
                      <button key={`sum-${col.name}`} onClick={() => addMeasure(col.name, 'sum')} className="text-xs font-medium bg-blue-100 text-blue-700 px-2.5 py-1.5 rounded-md hover:bg-blue-200 transition-colors border border-blue-200 shadow-sm">
                        SUM({col.name})
                      </button>
                    ))}
                    {schema[selectedTable].filter((c: any) => c.type.includes('Int') || c.type.includes('Float') || c.type.includes('Decimal')).slice(0, 2).map((col: any) => (
                      <button key={`avg-${col.name}`} onClick={() => addMeasure(col.name, 'avg')} className="text-xs font-medium bg-amber-100 text-amber-700 px-2.5 py-1.5 rounded-md hover:bg-amber-200 transition-colors border border-amber-200 shadow-sm">
                        AVG({col.name})
                      </button>
                    ))}
                  </div>
                </div>

                {/* Columns */}
                <div>
                  <div className="text-xs font-bold text-slate-400 uppercase tracking-wider mb-2">Columns</div>
                  {/* Field search */}
                  <div className="relative mb-2">
                    <Search size={12} className="absolute left-2 top-1/2 -translate-y-1/2 text-slate-400 pointer-events-none" />
                    <input
                      type="text"
                      placeholder="Search fields…"
                      value={fieldSearch}
                      onChange={(e) => setFieldSearch(e.target.value)}
                      className="w-full text-xs pl-6 pr-6 py-1.5 border border-slate-200 rounded-md focus:ring-1 focus:ring-emerald-500 outline-none bg-slate-50"
                    />
                    {fieldSearch && (
                      <button onClick={() => setFieldSearch('')} className="absolute right-2 top-1/2 -translate-y-1/2 text-slate-400 hover:text-slate-600">
                        <X size={11} />
                      </button>
                    )}
                  </div>
                  {(() => {
                    const filtered = schema[selectedTable].filter((col: any) =>
                      !fieldSearch || col.name.toLowerCase().includes(fieldSearch.toLowerCase())
                    );
                    if (filtered.length === 0) return (
                      <p className="text-xs text-slate-400 italic py-2 px-1">No fields match "{fieldSearch}"</p>
                    );
                    return (
                      <div className="space-y-1">
                        {filtered.map((col: any) => {
                          const isNumeric = col.type.includes('Int') || col.type.includes('Float') || col.type.includes('Decimal');
                          return (
                            <div key={col.name} className="group flex items-center justify-between p-2 hover:bg-slate-50 rounded-lg cursor-pointer border border-transparent hover:border-slate-200 transition-all">
                              <div className="flex items-center gap-2 overflow-hidden">
                                <div className={clsx("w-2 h-2 rounded-full shrink-0", isNumeric ? "bg-blue-400" : "bg-emerald-400")} />
                                <span className="text-sm text-slate-700 truncate" title={col.name}>{col.name}</span>
                              </div>
                              <div className="opacity-0 group-hover:opacity-100 flex items-center gap-1 transition-opacity relative">
                                <button onClick={() => addRowDimension(col.name)} className="text-[10px] font-medium bg-emerald-100 hover:bg-emerald-200 text-emerald-700 px-1.5 py-0.5 rounded">Row</button>
                                <button onClick={() => addColDimension(col.name)} className="text-[10px] font-medium bg-purple-100 hover:bg-purple-200 text-purple-700 px-1.5 py-0.5 rounded">Col</button>
                                <div className="relative">
                                  <button
                                    onClick={(e) => { e.stopPropagation(); setAddMeasureFor(addMeasureFor === col.name ? null : col.name); }}
                                    className="text-[10px] font-medium bg-blue-100 hover:bg-blue-200 text-blue-700 px-1.5 py-0.5 rounded flex items-center gap-0.5"
                                  >
                                    Agg <ChevronRight size={9} className={clsx("transition-transform", addMeasureFor === col.name ? "rotate-90" : "")} />
                                  </button>
                                  {addMeasureFor === col.name && (
                                    <div className="absolute right-0 top-full mt-1 z-50 bg-white border border-slate-200 rounded-lg shadow-xl py-1 min-w-[150px]">
                                      {AGG_OPTIONS.map(opt => (
                                        <button
                                          key={opt.value}
                                          onClick={() => addMeasure(col.name, opt.value)}
                                          className="w-full text-left px-3 py-1.5 text-xs hover:bg-slate-50 text-slate-700 font-mono"
                                        >
                                          {opt.label}
                                        </button>
                                      ))}
                                    </div>
                                  )}
                                </div>
                              </div>
                            </div>
                          );
                        })}
                      </div>
                    );
                  })()}
                </div>
              </>
            )}
          </div>
        </div>

        {/* History Panel (slide-in from right) */}
        {showHistory && (
          <div className="w-80 bg-white border-l border-slate-200 flex flex-col h-full overflow-hidden shrink-0">
            <div className="p-4 border-b border-slate-200 flex items-center justify-between bg-white shrink-0">
              <div className="flex items-center gap-2">
                <History size={16} className="text-slate-600" />
                <h3 className="text-sm font-semibold text-slate-800">Query History</h3>
              </div>
              <button onClick={() => setShowHistory(false)} className="text-slate-400 hover:text-slate-600"><X size={16} /></button>
            </div>
            <div className="flex-1 overflow-y-auto p-3 space-y-2">
              {loadingHistory ? (
                <div className="text-sm text-slate-400 text-center py-8">Loading…</div>
              ) : historyEntries.length === 0 ? (
                <div className="text-sm text-slate-400 text-center py-8 italic">No history yet.</div>
              ) : (
                historyEntries.map(entry => (
                  <div key={entry.id} className="group bg-slate-50 border border-slate-200 rounded-lg p-3 hover:border-slate-300 transition-colors">
                    <div className="flex items-start justify-between gap-2 mb-1">
                      <div className="flex items-center gap-1 text-[10px] text-slate-400">
                        <Clock size={10} />
                        {new Date(entry.created_at).toLocaleString()}
                      </div>
                      <button
                        onClick={() => replayHistoryEntry(entry.sql)}
                        className="shrink-0 opacity-0 group-hover:opacity-100 transition-opacity text-[10px] font-medium bg-emerald-100 text-emerald-700 hover:bg-emerald-200 px-2 py-0.5 rounded border border-emerald-200 flex items-center gap-1"
                        title="Replay this query"
                      >
                        <Play size={9} /> Run
                      </button>
                    </div>
                    <pre className="text-[10px] text-slate-600 whitespace-pre-wrap break-all font-mono leading-relaxed line-clamp-4 overflow-hidden">{entry.sql}</pre>
                  </div>
                ))
              )}
            </div>
          </div>
        )}

        {/* Results Area */}
        <div className="flex-1 p-6 overflow-y-auto overflow-x-auto bg-slate-50/50 flex flex-col gap-4 min-w-0">
          {suggestedVisual && suggestedVisual !== visualType && (
            <div className="bg-blue-50 border border-blue-100 rounded-xl p-3 flex items-center justify-between shadow-sm">
              <div className="flex items-center gap-2 text-blue-700 text-sm">
                <Sparkles size={16} className="text-blue-500" />
                <span>AI Suggestion: A <strong>{suggestedVisual}</strong> chart might be the best way to visualize this data.</span>
              </div>
              <button 
                onClick={handleApplySuggestion}
                className="bg-white text-blue-600 hover:bg-blue-50 border border-blue-200 px-3 py-1.5 rounded-lg text-xs font-medium transition-colors shadow-sm"
              >
                Apply Suggestion
              </button>
            </div>
          )}

          {(() => {
            // Results panel — rendered via portal when fullscreen so it escapes
            // parent overflow/stacking contexts and fully overlays the chat pane.
            const panel = (
              <div className={clsx(
                "bg-white border border-slate-200 shadow-sm overflow-hidden flex flex-col",
                isFullscreen
                  ? "fixed inset-0 z-[200] shadow-2xl rounded-none"
                  : "flex-1 rounded-2xl min-w-0"
              )}>
                <div className="p-4 border-b border-slate-100 flex items-center justify-between bg-white shrink-0">
                  <h3 className="text-sm font-semibold text-slate-800">Results</h3>
                  <div className="flex items-center gap-2">
                    <div className="flex items-center gap-1 bg-slate-100 p-1 rounded-lg">
                      {[
                        { id: 'table',  icon: Table,       title: 'Table' },
                        { id: 'matrix', icon: LayoutGrid,   title: 'Matrix / Pivot' },
                        { id: 'bar',    icon: BarChart2,    title: 'Bar Chart' },
                        { id: 'line',   icon: LineChart,    title: 'Line Chart' },
                        { id: 'area',   icon: Activity,     title: 'Area Chart' },
                        { id: 'pie',    icon: PieChart,     title: 'Pie Chart' },
                        { id: 'scatter',icon: ScatterChart, title: 'Scatter Plot' },
                        { id: 'radar',  icon: Grid3X3,      title: 'Radar Chart' },
                      ].map((v) => (
                        <button
                          key={v.id}
                          onClick={() => setVisualType(v.id as VisualType)}
                          title={v.title}
                          className={clsx(
                            "p-1.5 rounded-md transition-all",
                            visualType === v.id ? "bg-white shadow-sm text-emerald-600" : "text-slate-500 hover:text-slate-700"
                          )}
                        >
                          <v.icon size={16} />
                        </button>
                      ))}
                    </div>
                    {queryResult && queryResult.length > 0 && (
                      <>
                        <div className="w-px h-6 bg-slate-200 mx-1"></div>
                        <div className="flex items-center gap-1">
                          <span className="text-xs text-slate-400 shrink-0">Sort:</span>
                          <select
                            value={sortConfig?.key || ''}
                            onChange={e => {
                              if (!e.target.value) { setSortConfig(null); return; }
                              setSortConfig({ key: e.target.value, direction: sortConfig?.direction || 'asc' });
                            }}
                            className="text-xs border border-slate-200 rounded px-1.5 py-0.5 bg-white text-slate-700 outline-none focus:ring-1 focus:ring-emerald-500 max-w-[120px]"
                          >
                            <option value="">— none —</option>
                            {(columnOrder.length > 0 ? columnOrder : Object.keys(queryResult[0])).map(k => (
                              <option key={k} value={k}>{k}</option>
                            ))}
                          </select>
                          {sortConfig && (
                            <>
                              <button
                                onClick={() => setSortConfig({ key: sortConfig.key, direction: sortConfig.direction === 'asc' ? 'desc' : 'asc' })}
                                className="p-1 rounded hover:bg-slate-100 text-slate-500"
                                title={sortConfig.direction === 'asc' ? 'Ascending — click for descending' : 'Descending — click for ascending'}
                              >
                                {sortConfig.direction === 'asc' ? <ArrowUp size={13} /> : <ArrowDown size={13} />}
                              </button>
                              <button
                                onClick={() => setSortConfig(null)}
                                className="p-1 rounded hover:bg-slate-100 text-slate-400 hover:text-red-500"
                                title="Clear sort"
                              >
                                <X size={12} />
                              </button>
                            </>
                          )}
                        </div>
                      </>
                    )}
                    <div className="w-px h-6 bg-slate-200 mx-1"></div>
                    <button
                      onClick={() => setIsFullscreen(!isFullscreen)}
                      className="p-1.5 text-slate-500 hover:text-slate-800 hover:bg-slate-100 rounded-md transition-colors"
                      title={isFullscreen ? "Exit fullscreen" : "Fullscreen"}
                    >
                      {isFullscreen ? <Minimize2 size={18} /> : <Maximize2 size={18} />}
                    </button>
                  </div>
                </div>
                <div className="p-4 flex-1 flex flex-col min-h-0 min-w-0 overflow-hidden">
                  {renderVisual()}
                </div>
              </div>
            );
            return isFullscreen ? createPortal(panel, document.body) : panel;
          })()}
        </div>
      </div>

      {/* ------------------------------------------------------------------ */}
      {/* Drill-through Modal                                                  */}
      {/* ------------------------------------------------------------------ */}
      {drillThrough.visible && createPortal(
        <div className="fixed inset-0 z-[300] flex items-center justify-center bg-black/50 backdrop-blur-sm p-4">
          <div className="bg-white rounded-2xl shadow-2xl w-full max-w-5xl max-h-[80vh] flex flex-col overflow-hidden">
            <div className="p-4 border-b border-slate-200 flex items-center justify-between shrink-0">
              <div>
                <h3 className="text-sm font-bold text-slate-800">Drill-Through Details</h3>
                <p className="text-xs text-slate-500 mt-0.5 font-mono">{drillThrough.title}</p>
              </div>
              <div className="flex items-center gap-2">
                <span className="text-xs text-slate-400 font-mono bg-slate-100 px-2 py-1 rounded">LIMIT {MAX_ROWS}</span>
                <button onClick={() => setDrillThrough(prev => ({ ...prev, visible: false }))} className="text-slate-400 hover:text-slate-600 p-1.5 hover:bg-slate-100 rounded-md">
                  <X size={16} />
                </button>
              </div>
            </div>
            <div className="flex-1 overflow-auto p-1">
              {drillThrough.loading ? (
                <div className="flex items-center justify-center h-40 text-slate-400 text-sm">Fetching rows…</div>
              ) : drillThrough.error ? (
                <div className="p-4 text-red-600 text-sm bg-red-50 rounded-lg m-4">{drillThrough.error}</div>
              ) : drillThrough.data.length === 0 ? (
                <div className="flex items-center justify-center h-40 text-slate-400 text-sm italic">No rows found.</div>
              ) : (
                <table className="w-full text-xs text-left min-w-max">
                  <thead className="bg-slate-50 sticky top-0">
                    <tr>
                      {Object.keys(drillThrough.data[0]).map(k => (
                        <th key={k} className="px-3 py-2 text-[10px] font-bold uppercase tracking-wider text-slate-500 border-b border-slate-200 whitespace-nowrap">{k}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-100">
                    {drillThrough.data.map((row, i) => (
                      <tr key={i} className="hover:bg-slate-50">
                        {Object.values(row).map((val, j) => (
                          <td key={j} className="px-3 py-1.5 text-slate-600 whitespace-nowrap font-mono">
                            {val === null || val === undefined ? <span className="text-slate-300 italic">null</span> : String(val)}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
            <div className="p-3 border-t border-slate-100 bg-slate-50 text-xs text-slate-400 shrink-0 font-mono">
              SELECT * FROM {selectedTable} … — {drillThrough.data.length} rows
            </div>
          </div>
        </div>,
        document.body
      )}

      {/* ------------------------------------------------------------------ */}
      {/* AI Explore / Profiling Modal                                         */}
      {/* ------------------------------------------------------------------ */}
      {aiExplore.visible && createPortal(
        <div className="fixed inset-0 z-[400] flex items-center justify-center bg-black/60 backdrop-blur-sm p-4">
          <div className="bg-white rounded-2xl shadow-2xl w-full max-w-6xl max-h-[92vh] flex flex-col overflow-hidden">
            {/* Header */}
            <div className="px-6 py-4 border-b border-slate-200 flex items-center justify-between shrink-0 bg-gradient-to-r from-indigo-600 to-purple-700">
              <div className="flex items-center gap-3">
                <div className="p-2 bg-white/20 rounded-lg">
                  <Telescope size={20} className="text-white" />
                </div>
                <div>
                  <h2 className="text-base font-bold text-white">AI Explore</h2>
                  <p className="text-xs text-indigo-200">Profiling IA de vos tables</p>
                </div>
              </div>
              <div className="flex items-center gap-3">
                <select
                  value={aiExplore.targetTable}
                  onChange={e => setAiExplore(prev => ({ ...prev, targetTable: e.target.value }))}
                  className="text-sm bg-white/20 text-white border border-white/30 rounded-lg px-3 py-1.5 outline-none focus:ring-2 focus:ring-white/50 cursor-pointer"
                >
                  {tables.map(t => <option key={t} value={t} className="text-slate-800 bg-white">{t}</option>)}
                </select>
                <button
                  onClick={() => runProfileTable(aiExplore.targetTable)}
                  disabled={aiExplore.loading || !aiExplore.targetTable}
                  className="flex items-center gap-2 bg-white text-indigo-700 hover:bg-indigo-50 px-4 py-1.5 rounded-lg text-sm font-semibold transition-colors disabled:opacity-50 shadow"
                >
                  {aiExplore.loading ? (
                    <><span className="animate-spin inline-block w-3 h-3 border-2 border-indigo-400 border-t-transparent rounded-full" /> Profiling…</>
                  ) : (
                    <><Telescope size={14} /> Analyser</>
                  )}
                </button>
                <button onClick={() => setAiExplore(prev => ({ ...prev, visible: false }))} className="text-white/70 hover:text-white p-1.5 hover:bg-white/20 rounded-lg">
                  <X size={18} />
                </button>
              </div>
            </div>

            {/* Body */}
            <div className="flex-1 overflow-y-auto bg-slate-50">
              {aiExplore.error && (
                <div className="m-6 p-4 bg-red-50 border border-red-200 rounded-xl text-red-700 text-sm flex items-start gap-2">
                  <AlertTriangle size={16} className="shrink-0 mt-0.5" />
                  {aiExplore.error}
                </div>
              )}

              {!aiExplore.stats && !aiExplore.loading && !aiExplore.error && (
                <div className="flex flex-col items-center justify-center h-64 text-slate-400 gap-3">
                  <Telescope size={48} className="opacity-20" />
                  <p className="text-sm">Sélectionnez une table et cliquez sur <strong>Analyser</strong></p>
                </div>
              )}

              {aiExplore.loading && (
                <div className="flex flex-col items-center justify-center h-64 gap-4">
                  <div className="relative w-16 h-16">
                    <div className="absolute inset-0 rounded-full border-4 border-indigo-100" />
                    <div className="absolute inset-0 rounded-full border-4 border-indigo-500 border-t-transparent animate-spin" />
                    <Telescope size={20} className="absolute inset-0 m-auto text-indigo-500" />
                  </div>
                  <div className="text-center">
                    <p className="text-sm font-semibold text-slate-700">Analyse en cours…</p>
                    <p className="text-xs text-slate-400 mt-1">Calcul des statistiques et profiling IA</p>
                  </div>
                </div>
              )}

              {aiExplore.stats && (() => {
                const s = aiExplore.stats!;
                const nullyColumns = s.columns.filter(c => c.null_pct > 10);
                const highCardColumns = s.columns.filter(c => c.distinct_pct > 80);
                const qualityScore = Math.round((1 - nullyColumns.length / Math.max(s.columns.length, 1)) * 100);

                // Type grouping
                const typeGroups: Record<string, number> = {};
                s.columns.forEach(c => {
                  const baseType = c.type.replace(/\(.*\)/, '').replace(/Nullable\(/, '').replace(/\)$/, '').split(' ')[0];
                  typeGroups[baseType] = (typeGroups[baseType] || 0) + 1;
                });
                const typePieData = Object.entries(typeGroups).map(([name, value]) => ({ name, value }));

                // Null rate bar chart data (top 15 worst)
                const nullBarData = [...s.columns]
                  .sort((a, b) => b.null_pct - a.null_pct)
                  .slice(0, 15)
                  .map(c => ({ name: c.name.length > 14 ? c.name.slice(0, 12) + '…' : c.name, null_pct: c.null_pct, fullName: c.name }));

                // Distinct count bar chart (top 12)
                const distinctBarData = [...s.columns]
                  .sort((a, b) => b.distinct - a.distinct)
                  .slice(0, 12)
                  .map(c => ({ name: c.name.length > 14 ? c.name.slice(0, 12) + '…' : c.name, distinct: c.distinct, distinct_pct: c.distinct_pct }));

                const TYPE_PIE_COLORS = ['#6366f1', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#06b6d4', '#ec4899', '#84cc16'];

                return (
                  <div className="p-6 space-y-6">
                    {/* Metric cards */}
                    <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
                      {[
                        { label: 'Lignes totales', value: s.total_rows.toLocaleString(), icon: Database, color: 'from-blue-500 to-blue-600' },
                        { label: 'Colonnes', value: s.total_columns, icon: Layers, color: 'from-emerald-500 to-emerald-600' },
                        { label: 'Score qualité', value: `${qualityScore}%`, icon: CheckCircle2, color: qualityScore >= 80 ? 'from-green-500 to-green-600' : qualityScore >= 60 ? 'from-amber-500 to-amber-600' : 'from-red-500 to-red-600' },
                        { label: 'Colonnes >10% null', value: nullyColumns.length, icon: AlertTriangle, color: nullyColumns.length === 0 ? 'from-slate-400 to-slate-500' : 'from-orange-500 to-red-500' },
                      ].map((m, i) => (
                        <div key={i} className={`bg-gradient-to-br ${m.color} rounded-2xl p-4 text-white shadow-lg`}>
                          <div className="flex items-center justify-between mb-2">
                            <span className="text-xs font-medium opacity-80">{m.label}</span>
                            <m.icon size={18} className="opacity-80" />
                          </div>
                          <div className="text-2xl font-bold">{m.value}</div>
                        </div>
                      ))}
                    </div>

                    {/* AI Summary */}
                    {(aiExplore.insightsLoading || aiExplore.insights) && (
                      <div className="bg-gradient-to-r from-indigo-50 to-purple-50 border border-indigo-100 rounded-2xl p-5">
                        <div className="flex items-center gap-2 mb-3">
                          <Brain size={16} className="text-indigo-600" />
                          <span className="text-sm font-bold text-indigo-700">Analyse IA</span>
                          {aiExplore.insightsLoading && <span className="animate-pulse text-xs text-indigo-400">En cours…</span>}
                        </div>
                        {aiExplore.insightsLoading ? (
                          <div className="space-y-2">
                            {[80, 65, 90].map((w, i) => <div key={i} className="h-3 bg-indigo-200/60 rounded animate-pulse" style={{ width: `${w}%` }} />)}
                          </div>
                        ) : aiExplore.insights && (
                          <div className="space-y-4">
                            <p className="text-sm text-indigo-900 leading-relaxed">{aiExplore.insights.summary}</p>
                            <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
                              {aiExplore.insights.quality_issues?.length > 0 && (
                                <div className="bg-red-50 border border-red-100 rounded-xl p-3">
                                  <div className="flex items-center gap-1.5 mb-2 text-red-700 text-xs font-bold uppercase tracking-wider">
                                    <AlertTriangle size={12} /> Problèmes qualité
                                  </div>
                                  <ul className="space-y-1">
                                    {aiExplore.insights.quality_issues.map((q, i) => <li key={i} className="text-xs text-red-800">• {q}</li>)}
                                  </ul>
                                </div>
                              )}
                              {aiExplore.insights.key_insights?.length > 0 && (
                                <div className="bg-amber-50 border border-amber-100 rounded-xl p-3">
                                  <div className="flex items-center gap-1.5 mb-2 text-amber-700 text-xs font-bold uppercase tracking-wider">
                                    <Lightbulb size={12} /> Insights clés
                                  </div>
                                  <ul className="space-y-1">
                                    {aiExplore.insights.key_insights.map((q, i) => <li key={i} className="text-xs text-amber-800">• {q}</li>)}
                                  </ul>
                                </div>
                              )}
                              {aiExplore.insights.recommendations?.length > 0 && (
                                <div className="bg-emerald-50 border border-emerald-100 rounded-xl p-3">
                                  <div className="flex items-center gap-1.5 mb-2 text-emerald-700 text-xs font-bold uppercase tracking-wider">
                                    <CheckCircle2 size={12} /> Recommandations
                                  </div>
                                  <ul className="space-y-1">
                                    {aiExplore.insights.recommendations.map((q, i) => <li key={i} className="text-xs text-emerald-800">• {q}</li>)}
                                  </ul>
                                </div>
                              )}
                            </div>
                          </div>
                        )}
                      </div>
                    )}

                    {/* Charts row */}
                    <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
                      {/* Type distribution pie */}
                      <div className="bg-white rounded-2xl border border-slate-200 p-4 shadow-sm">
                        <h3 className="text-xs font-bold text-slate-500 uppercase tracking-wider mb-3 flex items-center gap-1.5">
                          <Type size={12} /> Distribution des types
                        </h3>
                        <ResponsiveContainer width="100%" height={200}>
                          <RechartsPieChart>
                            <Pie data={typePieData} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={70} innerRadius={30} paddingAngle={3} label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`} labelLine={false} fontSize={10}>
                              {typePieData.map((_, i) => <Cell key={i} fill={TYPE_PIE_COLORS[i % TYPE_PIE_COLORS.length]} />)}
                            </Pie>
                            <RechartsTooltip contentStyle={{ fontSize: 11, borderRadius: 8 }} />
                          </RechartsPieChart>
                        </ResponsiveContainer>
                      </div>

                      {/* Null rate bar chart */}
                      <div className="bg-white rounded-2xl border border-slate-200 p-4 shadow-sm">
                        <h3 className="text-xs font-bold text-slate-500 uppercase tracking-wider mb-3 flex items-center gap-1.5">
                          <Percent size={12} /> Taux de null par colonne (top 15)
                        </h3>
                        <ResponsiveContainer width="100%" height={200}>
                          <BarChart data={nullBarData} layout="vertical" margin={{ left: 0, right: 8 }}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" horizontal={false} />
                            <XAxis type="number" domain={[0, 100]} tickFormatter={v => `${v}%`} tick={{ fontSize: 9, fill: '#94a3b8' }} axisLine={false} tickLine={false} />
                            <YAxis type="category" dataKey="name" tick={{ fontSize: 9, fill: '#64748b' }} width={75} axisLine={false} tickLine={false} />
                            <RechartsTooltip formatter={(v: any) => [`${v}%`, 'Null']} contentStyle={{ fontSize: 11, borderRadius: 8 }} />
                            <Bar dataKey="null_pct" radius={[0, 4, 4, 0]}>
                              {nullBarData.map((entry, i) => (
                                <Cell key={i} fill={entry.null_pct > 50 ? '#ef4444' : entry.null_pct > 20 ? '#f59e0b' : '#10b981'} />
                              ))}
                            </Bar>
                          </BarChart>
                        </ResponsiveContainer>
                      </div>

                      {/* Distinct count chart */}
                      <div className="bg-white rounded-2xl border border-slate-200 p-4 shadow-sm">
                        <h3 className="text-xs font-bold text-slate-500 uppercase tracking-wider mb-3 flex items-center gap-1.5">
                          <Hash size={12} /> Valeurs distinctes (top 12)
                        </h3>
                        <ResponsiveContainer width="100%" height={200}>
                          <BarChart data={distinctBarData} margin={{ left: 0, right: 8 }}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
                            <XAxis dataKey="name" tick={{ fontSize: 9, fill: '#64748b' }} axisLine={false} tickLine={false} />
                            <YAxis tick={{ fontSize: 9, fill: '#94a3b8' }} axisLine={false} tickLine={false} />
                            <RechartsTooltip contentStyle={{ fontSize: 11, borderRadius: 8 }} />
                            <Bar dataKey="distinct" fill="#6366f1" radius={[4, 4, 0, 0]} />
                          </BarChart>
                        </ResponsiveContainer>
                      </div>
                    </div>

                    {/* Column cards grid */}
                    <div>
                      <h3 className="text-xs font-bold text-slate-500 uppercase tracking-wider mb-3 flex items-center gap-1.5">
                        <Layers size={12} /> Détail des colonnes ({s.total_columns})
                      </h3>
                      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-3">
                        {s.columns.map(col => {
                          const baseType = col.type.replace(/Nullable\(/, '').replace(/\)$/, '').replace(/\(.*\)/, '');
                          const isNum = baseType.match(/Int|Float|Decimal|Double/i);
                          const isDate = baseType.match(/Date|Time/i);
                          const isStr = baseType.match(/String|FixedString/i);
                          const TypeIcon = isNum ? Hash : isDate ? Calendar : isStr ? Type : ToggleLeft;
                          const typeColor = isNum ? 'text-blue-600 bg-blue-50' : isDate ? 'text-purple-600 bg-purple-50' : isStr ? 'text-emerald-600 bg-emerald-50' : 'text-slate-600 bg-slate-50';

                          return (
                            <div key={col.name} className="bg-white border border-slate-200 rounded-xl p-3 shadow-sm hover:shadow-md hover:border-indigo-200 transition-all">
                              <div className="flex items-start justify-between mb-2 gap-1">
                                <span className="text-xs font-semibold text-slate-800 truncate flex-1" title={col.name}>{col.name}</span>
                                <span className={`text-[9px] font-bold px-1.5 py-0.5 rounded-full shrink-0 flex items-center gap-0.5 ${typeColor}`}>
                                  <TypeIcon size={8} /> {baseType.slice(0, 8)}
                                </span>
                              </div>

                              {/* Null rate bar */}
                              <div className="mb-2">
                                <div className="flex justify-between text-[10px] text-slate-400 mb-0.5">
                                  <span>Null</span>
                                  <span className={col.null_pct > 20 ? 'text-red-500 font-medium' : 'text-slate-400'}>{col.null_pct}%</span>
                                </div>
                                <div className="h-1.5 bg-slate-100 rounded-full overflow-hidden">
                                  <div
                                    className={`h-full rounded-full transition-all ${col.null_pct > 50 ? 'bg-red-400' : col.null_pct > 20 ? 'bg-amber-400' : 'bg-emerald-400'}`}
                                    style={{ width: `${Math.min(col.null_pct, 100)}%` }}
                                  />
                                </div>
                              </div>

                              {/* Distinct count */}
                              <div className="flex items-center justify-between text-[10px]">
                                <span className="text-slate-400">Distinct</span>
                                <span className="font-semibold text-slate-700">{col.distinct.toLocaleString()}</span>
                              </div>
                              <div className="h-1.5 bg-slate-100 rounded-full overflow-hidden mt-0.5 mb-2">
                                <div className="h-full bg-indigo-400 rounded-full" style={{ width: `${Math.min(col.distinct_pct, 100)}%` }} />
                              </div>

                              {/* Top values mini bars */}
                              {col.top_values.length > 0 && (
                                <div className="mt-2 pt-2 border-t border-slate-100">
                                  <p className="text-[9px] text-slate-400 uppercase tracking-wider mb-1">Top valeurs</p>
                                  <div className="space-y-1">
                                    {col.top_values.slice(0, 4).map((tv, i) => {
                                      const maxCount = col.top_values[0]?.count || 1;
                                      const pct = Math.round((tv.count / maxCount) * 100);
                                      return (
                                        <div key={i} className="flex items-center gap-1.5">
                                          <div className="h-1 bg-indigo-200 rounded-full flex-1 overflow-hidden">
                                            <div className="h-full bg-indigo-500 rounded-full" style={{ width: `${pct}%` }} />
                                          </div>
                                          <span className="text-[9px] text-slate-500 truncate max-w-[60px]" title={tv.value}>{tv.value || '(vide)'}</span>
                                          <span className="text-[9px] text-slate-400 shrink-0">{tv.count.toLocaleString()}</span>
                                        </div>
                                      );
                                    })}
                                  </div>
                                </div>
                              )}
                            </div>
                          );
                        })}
                      </div>
                    </div>
                  </div>
                );
              })()}
            </div>
          </div>
        </div>,
        document.body
      )}

      {/* ------------------------------------------------------------------ */}
      {/* AI Analysis Modal                                                    */}
      {/* ------------------------------------------------------------------ */}
      {aiAnalysis.visible && createPortal(
        <div className="fixed inset-0 z-[300] flex items-center justify-center bg-black/50 backdrop-blur-sm p-4">
          <div className="bg-white rounded-2xl shadow-2xl w-full max-w-2xl max-h-[85vh] flex flex-col overflow-hidden">
            <div className="p-4 border-b border-slate-200 flex items-center justify-between shrink-0">
              <div className="flex items-center gap-2">
                <Brain size={18} className="text-violet-600" />
                <h3 className="text-sm font-bold text-slate-800">AI Query Analysis</h3>
                {!aiAnalysis.loading && aiAnalysis.risk_level && (
                  <span className={clsx(
                    "text-[10px] font-bold px-2 py-0.5 rounded-full uppercase",
                    aiAnalysis.risk_level === 'high' ? "bg-red-100 text-red-700" :
                    aiAnalysis.risk_level === 'medium' ? "bg-amber-100 text-amber-700" :
                    "bg-emerald-100 text-emerald-700"
                  )}>
                    {aiAnalysis.risk_level} risk
                  </span>
                )}
              </div>
              <button onClick={() => setAiAnalysis(prev => ({ ...prev, visible: false }))} className="text-slate-400 hover:text-slate-600 p-1.5 hover:bg-slate-100 rounded-md">
                <X size={16} />
              </button>
            </div>

            <div className="flex-1 overflow-y-auto p-5 space-y-4">
              {/* SQL Preview */}
              <div className="bg-slate-900 rounded-lg p-3">
                <div className="text-[10px] text-slate-400 uppercase tracking-wider mb-2 font-bold">Query to be executed</div>
                <pre className="text-xs text-emerald-400 font-mono whitespace-pre-wrap break-all leading-relaxed">{aiAnalysis.sql}</pre>
              </div>

              {aiAnalysis.loading ? (
                <div className="flex flex-col items-center justify-center py-10 text-slate-400 gap-3">
                  <Brain size={32} className="text-violet-400 animate-pulse" />
                  <p className="text-sm">AI is analyzing your query…</p>
                </div>
              ) : aiAnalysis.error ? (
                <div className="p-4 text-red-600 text-sm bg-red-50 rounded-lg border border-red-200">{aiAnalysis.error}</div>
              ) : (
                <>
                  {aiAnalysis.alerts.length > 0 && (
                    <div>
                      <div className="flex items-center gap-2 mb-2">
                        <AlertTriangle size={14} className="text-red-500" />
                        <span className="text-xs font-bold text-red-700 uppercase tracking-wider">Alerts</span>
                      </div>
                      <ul className="space-y-1.5">
                        {aiAnalysis.alerts.map((a, i) => (
                          <li key={i} className="flex items-start gap-2 text-sm text-red-700 bg-red-50 rounded-lg px-3 py-2 border border-red-100">
                            <span className="text-red-400 mt-0.5 shrink-0">•</span>{a}
                          </li>
                        ))}
                      </ul>
                    </div>
                  )}

                  {aiAnalysis.suggestions.length > 0 && (
                    <div>
                      <div className="flex items-center gap-2 mb-2">
                        <Lightbulb size={14} className="text-amber-500" />
                        <span className="text-xs font-bold text-amber-700 uppercase tracking-wider">Suggestions</span>
                      </div>
                      <ul className="space-y-1.5">
                        {aiAnalysis.suggestions.map((s, i) => (
                          <li key={i} className="flex items-start gap-2 text-sm text-amber-800 bg-amber-50 rounded-lg px-3 py-2 border border-amber-100">
                            <span className="text-amber-400 mt-0.5 shrink-0">•</span>{s}
                          </li>
                        ))}
                      </ul>
                    </div>
                  )}

                  {aiAnalysis.projections.length > 0 && (
                    <div>
                      <div className="flex items-center gap-2 mb-2">
                        <TrendingUp size={14} className="text-blue-500" />
                        <span className="text-xs font-bold text-blue-700 uppercase tracking-wider">Projections</span>
                      </div>
                      <ul className="space-y-1.5">
                        {aiAnalysis.projections.map((p, i) => (
                          <li key={i} className="flex items-start gap-2 text-sm text-blue-800 bg-blue-50 rounded-lg px-3 py-2 border border-blue-100">
                            <span className="text-blue-400 mt-0.5 shrink-0">•</span>{p}
                          </li>
                        ))}
                      </ul>
                    </div>
                  )}

                  {aiAnalysis.optimized_sql && (
                    <div>
                      <div className="flex items-center gap-2 mb-2">
                        <Sparkles size={14} className="text-violet-500" />
                        <span className="text-xs font-bold text-violet-700 uppercase tracking-wider">Optimized SQL</span>
                      </div>
                      <div className="bg-slate-900 rounded-lg p-3">
                        <pre className="text-xs text-violet-300 font-mono whitespace-pre-wrap break-all leading-relaxed">{aiAnalysis.optimized_sql}</pre>
                      </div>
                    </div>
                  )}

                  {aiAnalysis.alerts.length === 0 && aiAnalysis.suggestions.length === 0 && aiAnalysis.projections.length === 0 && (
                    <div className="flex items-center gap-2 text-emerald-700 bg-emerald-50 rounded-lg px-4 py-3 border border-emerald-200">
                      <span className="text-emerald-500">✓</span>
                      <span className="text-sm font-medium">Query looks good — no issues detected.</span>
                    </div>
                  )}
                </>
              )}
            </div>

            <div className="p-4 border-t border-slate-200 flex justify-end gap-3 shrink-0 bg-slate-50">
              <button
                onClick={() => setAiAnalysis(prev => ({ ...prev, visible: false }))}
                className="px-4 py-2 text-sm font-medium text-slate-600 hover:bg-slate-200 rounded-lg transition-colors border border-slate-200 bg-white"
              >
                Cancel
              </button>
              <button
                onClick={() => { setAiAnalysis(prev => ({ ...prev, visible: false })); buildAndExecuteQuery(); }}
                disabled={aiAnalysis.loading}
                className="flex items-center gap-2 px-4 py-2 text-sm font-medium bg-emerald-500 hover:bg-emerald-600 text-white rounded-lg transition-colors disabled:opacity-50"
              >
                <Play size={14} />
                Run Query Anyway
              </button>
            </div>
          </div>
        </div>,
        document.body
      )}
    </div>
  );
}
