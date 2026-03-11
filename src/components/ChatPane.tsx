import { useState, useRef, useEffect, type ReactNode } from 'react';
import {
  Send, Bot, Loader2, Sparkles, Play, Save, History, Trash2,
  Maximize2, Minimize2, Minus, CheckSquare, Filter, X,
  Brain, ChevronDown, ChevronRight, CheckCircle2, XCircle, Database,
  AlertTriangle, Info, Lightbulb, TrendingUp, TrendingDown, BarChart2, BookOpen,
  Download, FolderOpen, FileText, Zap, ShieldAlert, ShieldCheck,
  Tag, Calculator, Calendar, Layers, Search, Table2, HelpCircle, LineChart, PieChart, AreaChart, SlidersHorizontal,
} from 'lucide-react';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip as RechartsTooltip, Legend,
  ResponsiveContainer, LineChart as RechartsLineChart, Line,
  AreaChart as RechartsAreaChart, Area, PieChart as RechartsPieChart, Pie, Cell,
} from 'recharts';
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

// ── Executive Summary Bullet Result ────────────────────────────────────────

interface ExecBullet {
  point: string;
  risk: boolean;
  severity: 'high' | 'medium' | 'info';
}

interface ExecSummaryResult {
  preamble: string;
  bullets: ExecBullet[];
}

function ExecBulletView({ result }: { result: ExecSummaryResult }) {
  const severityStyle = {
    high: 'border-red-200 bg-red-50 text-red-800',
    medium: 'border-amber-200 bg-amber-50 text-amber-800',
    info: 'border-emerald-200 bg-emerald-50 text-emerald-800',
  };
  const severityDot = {
    high: 'bg-red-500',
    medium: 'bg-amber-400',
    info: 'bg-emerald-400',
  };

  return (
    <div className="mt-3 rounded-xl overflow-hidden border border-indigo-200 shadow-sm">
      <div className="px-4 py-2.5 bg-gradient-to-r from-indigo-600 to-violet-600 flex items-center gap-2">
        <Zap size={13} className="text-indigo-200" />
        <span className="text-xs font-bold text-white uppercase tracking-wide">5 Key Points — Executive Committee</span>
      </div>
      {result.preamble && (
        <div className="px-4 pt-3 pb-1">
          <p className="text-xs text-slate-500 italic leading-relaxed">{result.preamble}</p>
        </div>
      )}
      <div className="p-3 space-y-2">
        {result.bullets.map((b, i) => (
          <div
            key={i}
            className={clsx(
              'flex items-start gap-3 p-3 rounded-lg border text-xs leading-relaxed',
              severityStyle[b.severity]
            )}
          >
            <div className="flex items-center gap-1.5 flex-shrink-0 mt-0.5">
              <span className={clsx('w-2 h-2 rounded-full', severityDot[b.severity])} />
              <span className="font-black text-slate-400 text-[10px] w-3">{i + 1}</span>
            </div>
            <span className="flex-1">{b.point}</span>
            {b.risk && (
              <ShieldAlert size={13} className={clsx(
                'flex-shrink-0 mt-0.5',
                b.severity === 'high' ? 'text-red-500' : 'text-amber-500'
              )} />
            )}
            {!b.risk && <ShieldCheck size={13} className="flex-shrink-0 mt-0.5 text-emerald-500" />}
          </div>
        ))}
      </div>
    </div>
  );
}

// ── PDF Generator for Executive Summary ─────────────────────────────────────

function generateExecSummaryPDF(content: string, title?: string) {
  const genDate = new Date().toLocaleDateString('en-US', {
    year: 'numeric', month: 'long', day: 'numeric', hour: '2-digit', minute: '2-digit',
  });
  const reportTitle = title || 'Executive Summary';

  // Convert markdown to styled HTML sections
  function mdToHtml(md: string): string {
    const lines = md.split('\n');
    let html = '';
    let inList = false;
    let listOrdered = false;

    const flushList = () => {
      if (!inList) return '';
      inList = false;
      return listOrdered ? '</ol>' : '</ul>';
    };

    const inlineStyles = (text: string) =>
      text
        .replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>')
        .replace(/\*([^*]+)\*/g, '<em>$1</em>')
        .replace(/`([^`]+)`/g, '<code style="font-family:\'Courier New\',monospace;background:#f1f5f9;padding:1px 5px;border-radius:3px;font-size:11px;">$1</code>');

    for (const line of lines) {
      const h1 = line.match(/^#\s+(.+)/);
      const h2 = line.match(/^##\s+(.+)/);
      const h3 = line.match(/^###\s+(.+)/);
      const ul = line.match(/^[\s]*[-*•]\s+(.+)/);
      const ol = line.match(/^[\s]*\d+[.)]\s+(.+)/);
      const hr = /^-{3,}$/.test(line.trim());
      const bq = line.match(/^>\s*(.*)/);

      if (h1) {
        html += flushList();
        html += `<h1 style="font-size:20px;font-weight:900;color:#0f172a;margin:20px 0 8px;line-height:1.2;">${inlineStyles(h1[1])}</h1>`;
      } else if (h2) {
        html += flushList();
        html += `<h2 style="font-size:15px;font-weight:700;color:#1e293b;margin:16px 0 6px;padding-bottom:4px;border-bottom:1px solid #e2e8f0;">${inlineStyles(h2[1])}</h2>`;
      } else if (h3) {
        html += flushList();
        html += `<h3 style="font-size:12px;font-weight:700;color:#334155;margin:12px 0 4px;text-transform:uppercase;letter-spacing:0.05em;">${inlineStyles(h3[1])}</h3>`;
      } else if (hr) {
        html += flushList();
        html += `<hr style="border:none;border-top:1px solid #e2e8f0;margin:16px 0;" />`;
      } else if (bq) {
        html += flushList();
        html += `<blockquote style="border-left:3px solid #6366f1;padding:6px 12px;margin:8px 0;background:#f8f7ff;color:#4338ca;font-style:italic;font-size:12px;">${inlineStyles(bq[1])}</blockquote>`;
      } else if (ul) {
        if (!inList || listOrdered) { html += flushList(); html += '<ul style="margin:6px 0;padding-left:20px;">'; inList = true; listOrdered = false; }
        html += `<li style="font-size:12px;line-height:1.7;color:#334155;margin:2px 0;">${inlineStyles(ul[1])}</li>`;
      } else if (ol) {
        if (!inList || !listOrdered) { html += flushList(); html += '<ol style="margin:6px 0;padding-left:20px;">'; inList = true; listOrdered = true; }
        html += `<li style="font-size:12px;line-height:1.7;color:#334155;margin:2px 0;">${inlineStyles(ol[1])}</li>`;
      } else if (line.trim() === '') {
        html += flushList();
        html += '<div style="height:8px;"></div>';
      } else {
        html += flushList();
        html += `<p style="font-size:12px;line-height:1.75;color:#334155;margin:4px 0;">${inlineStyles(line)}</p>`;
      }
    }
    html += flushList();
    return html;
  }

  const bodyHtml = mdToHtml(content);

  const html = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<title>${reportTitle} — ClickSense</title>
<style>
* { box-sizing:border-box; margin:0; padding:0; }
body { font-family:'Segoe UI',system-ui,-apple-system,sans-serif; color:#0f172a; background:#f8fafc; -webkit-print-color-adjust:exact; print-color-adjust:exact; }
@media print {
  body { background:white; }
  @page { margin:15mm 14mm; size:A4; }
  .no-print { display:none !important; }
  .page-break { page-break-before:always; }
}
strong { font-weight:700; color:#0f172a; }
em { font-style:italic; }
</style>
</head>
<body style="max-width:860px;margin:0 auto;padding:28px 24px;">

  <!-- Cover gradient banner -->
  <div style="border-radius:16px;overflow:hidden;margin-bottom:32px;background:linear-gradient(135deg,#4f46e5 0%,#6d28d9 40%,#0891b2 100%);">
    <div style="padding:36px 40px 28px;">
      <div style="display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:20px;">
        <div>
          <span style="display:inline-block;background:rgba(255,255,255,0.18);color:rgba(255,255,255,0.92);font-size:9px;font-weight:700;letter-spacing:0.14em;text-transform:uppercase;padding:3px 11px;border-radius:20px;margin-bottom:12px;">Confidential Report · Executive Committee</span>
          <h1 style="font-size:28px;font-weight:900;color:white;line-height:1.15;margin-bottom:6px;">${reportTitle}</h1>
          <p style="font-size:13px;color:rgba(255,255,255,0.72);">Multi-step in-depth analysis · ClickSense AI Agent</p>
        </div>
        <div style="padding:14px;border-radius:16px;background:rgba(255,255,255,0.12);flex-shrink:0;">
          <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="rgba(255,255,255,0.9)" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
            <polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"></polygon>
          </svg>
        </div>
      </div>
      <div style="display:flex;gap:24px;padding-top:18px;border-top:1px solid rgba(255,255,255,0.2);">
        <div>
          <div style="font-size:10px;color:rgba(255,255,255,0.6);text-transform:uppercase;letter-spacing:0.08em;">Generated on</div>
          <div style="font-size:12px;font-weight:600;color:rgba(255,255,255,0.9);margin-top:2px;">${genDate}</div>
        </div>
        <div>
          <div style="font-size:10px;color:rgba(255,255,255,0.6);text-transform:uppercase;letter-spacing:0.08em;">Tool</div>
          <div style="font-size:12px;font-weight:600;color:rgba(255,255,255,0.9);margin-top:2px;">ClickSense AI</div>
        </div>
        <div>
          <div style="font-size:10px;color:rgba(255,255,255,0.6);text-transform:uppercase;letter-spacing:0.08em;">Classification</div>
          <div style="font-size:12px;font-weight:600;color:rgba(255,255,255,0.9);margin-top:2px;">Confidential</div>
        </div>
      </div>
    </div>
    <div style="padding:8px 40px;background:rgba(0,0,0,0.18);font-size:9px;color:rgba(255,255,255,0.55);letter-spacing:0.04em;">
      Automatically generated document by ClickSense · For executive committee use only
    </div>
  </div>

  <!-- Main content -->
  <div style="background:white;border:1px solid #e2e8f0;border-radius:16px;overflow:hidden;margin-bottom:24px;">
    <div style="display:flex;align-items:center;gap:10px;padding:14px 20px;background:#f8fafc;border-bottom:1px solid #f1f5f9;">
      <div style="width:6px;height:6px;border-radius:50%;background:linear-gradient(135deg,#4f46e5,#0891b2);"></div>
      <span style="font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:0.08em;">Complete analysis</span>
    </div>
    <div style="padding:28px 32px;line-height:1.75;">
      ${bodyHtml}
    </div>
  </div>

  <!-- Footer -->
  <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 18px;background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;font-size:9px;color:#94a3b8;">
    <span>ClickSense Executive Report · Confidential</span>
    <span>${genDate}</span>
  </div>

  <!-- Print button (hidden in print) -->
  <div class="no-print" style="text-align:center;margin-top:20px;">
    <button onclick="window.print()" style="padding:10px 28px;background:linear-gradient(135deg,#4f46e5,#6d28d9);color:white;border:none;border-radius:10px;font-size:13px;font-weight:600;cursor:pointer;box-shadow:0 2px 12px rgba(79,70,229,0.3);">
      Print / Save as PDF
    </button>
  </div>

</body>
</html>`;

  const win = window.open('', '_blank');
  if (!win) return;
  win.document.write(html);
  win.document.close();
  setTimeout(() => { try { win.print(); } catch { /* ignore */ } }, 900);
}

// ── Executive Summary Action Buttons ────────────────────────────────────────

function ExecSummaryActions({ content, dismissed, onDismiss }: {
  content: string;
  dismissed: boolean;
  onDismiss: () => void;
}) {
  const [loading5, setLoading5] = useState(false);
  const [result5, setResult5] = useState<ExecSummaryResult | null>(null);
  const [error5, setError5] = useState('');

  if (dismissed) return null;

  const handle5Points = async () => {
    setLoading5(true);
    setError5('');
    try {
      const res = await fetch('/api/summarize_executive', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ text: content, lang: 'en' }),
      });
      const data = await res.json();
      if (data.error) { setError5(data.error); return; }
      setResult5(data);
    } catch (e: any) {
      setError5(e.message);
    } finally {
      setLoading5(false);
    }
  };

  return (
    <div className="mt-3">
      {!result5 && (
        <div className="flex items-center gap-2 p-3 rounded-xl bg-gradient-to-r from-indigo-50 to-violet-50 border border-indigo-100">
          <div className="flex-1 min-w-0">
            <p className="text-[11px] font-semibold text-indigo-700 mb-0.5">Available actions</p>
            <p className="text-[10px] text-indigo-400 leading-relaxed">
              Summarize this analysis or export it as PDF for your executive committee.
            </p>
          </div>
          <div className="flex items-center gap-1.5 flex-shrink-0">
            <button
              onClick={handle5Points}
              disabled={loading5}
              title="5-point summary with key risks"
              className="flex items-center gap-1.5 px-3 py-1.5 bg-indigo-600 hover:bg-indigo-700 disabled:opacity-60 text-white rounded-lg text-[11px] font-semibold transition-colors shadow-sm"
            >
              {loading5 ? <Loader2 size={11} className="animate-spin" /> : <Zap size={11} />}
              5 key points
            </button>
            <button
              onClick={() => generateExecSummaryPDF(content)}
              title="Export PDF — executive committee presentation"
              className="flex items-center gap-1.5 px-3 py-1.5 bg-violet-600 hover:bg-violet-700 text-white rounded-lg text-[11px] font-semibold transition-colors shadow-sm"
            >
              <FileText size={11} />
              Export PDF
            </button>
            <button
              onClick={onDismiss}
              title="Ignorer"
              className="p-1.5 text-indigo-300 hover:text-indigo-500 hover:bg-indigo-100 rounded-md transition-colors"
            >
              <X size={12} />
            </button>
          </div>
        </div>
      )}
      {error5 && (
        <div className="mt-2 flex items-center gap-2 p-2 bg-red-50 border border-red-200 rounded-lg text-xs text-red-600">
          <AlertTriangle size={12} />
          {error5}
        </div>
      )}
      {result5 && (
        <div>
          <ExecBulletView result={result5} />
          <div className="mt-2 flex items-center justify-between">
            <button
              onClick={() => generateExecSummaryPDF(content)}
              className="flex items-center gap-1.5 px-3 py-1.5 bg-violet-600 hover:bg-violet-700 text-white rounded-lg text-[11px] font-semibold transition-colors shadow-sm"
            >
              <FileText size={11} />
              Full PDF Export
            </button>
            <button
              onClick={onDismiss}
              className="text-[10px] text-slate-400 hover:text-slate-600 flex items-center gap-1"
            >
              <X size={10} /> Close
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

// ── Clarification Panel ─────────────────────────────────────────────────────

type ClarificationType = 'field_selection' | 'table_selection' | 'value_selection' | 'metric_selection' | 'period_selection' | 'dimension_selection';

const clarificationConfig: Record<ClarificationType, {
  label: string;
  icon: ReactNode;
  color: string;
  pillColor: string;
}> = {
  table_selection: {
    label: 'Choose a table',
    icon: <Table2 size={12} />,
    color: 'text-blue-700 bg-blue-50 border-blue-200',
    pillColor: 'bg-blue-50 hover:bg-blue-100 text-blue-700 border-blue-200',
  },
  field_selection: {
    label: 'Choose a field',
    icon: <Layers size={12} />,
    color: 'text-violet-700 bg-violet-50 border-violet-200',
    pillColor: 'bg-violet-50 hover:bg-violet-100 text-violet-700 border-violet-200',
  },
  value_selection: {
    label: 'Choose a value',
    icon: <Tag size={12} />,
    color: 'text-amber-700 bg-amber-50 border-amber-200',
    pillColor: 'bg-amber-50 hover:bg-amber-100 text-amber-700 border-amber-200',
  },
  metric_selection: {
    label: 'Choose a calculation',
    icon: <Calculator size={12} />,
    color: 'text-emerald-700 bg-emerald-50 border-emerald-200',
    pillColor: 'bg-emerald-50 hover:bg-emerald-100 text-emerald-700 border-emerald-200',
  },
  period_selection: {
    label: 'Choose a period',
    icon: <Calendar size={12} />,
    color: 'text-sky-700 bg-sky-50 border-sky-200',
    pillColor: 'bg-sky-50 hover:bg-sky-100 text-sky-700 border-sky-200',
  },
  dimension_selection: {
    label: 'Choose a grouping',
    icon: <BarChart2 size={12} />,
    color: 'text-indigo-700 bg-indigo-50 border-indigo-200',
    pillColor: 'bg-indigo-50 hover:bg-indigo-100 text-indigo-700 border-indigo-200',
  },
};

function ClarificationPanel({
  type, options, context, onSelect,
}: {
  type: ClarificationType;
  options: string[];
  context?: { table?: string; field?: string };
  onSelect: (value: string) => void;
}) {
  const [search, setSearch] = useState('');
  const cfg = clarificationConfig[type] ?? clarificationConfig.field_selection;

  const filtered = search.trim()
    ? options.filter(o => o.toLowerCase().includes(search.toLowerCase()))
    : options;

  const showSearch = options.length > 8;

  if (options.length === 0 && type === 'value_selection') {
    // No values could be fetched — show a free-text input hint
    return (
      <div className="mt-3 space-y-2">
        <div className={clsx('flex items-center gap-2 px-3 py-2 rounded-lg border text-xs font-semibold', cfg.color)}>
          {cfg.icon}
          <span>{cfg.label}</span>
          {context?.field && (
            <span className="font-mono text-[10px] opacity-70">({context.field})</span>
          )}
        </div>
        <p className="text-xs text-slate-400 italic">Type the value directly in your message below.</p>
      </div>
    );
  }

  if (options.length === 0) return null;

  return (
    <div className="mt-3 space-y-2">
      {/* Header badge */}
      <div className={clsx('inline-flex items-center gap-1.5 px-2.5 py-1 rounded-lg border text-xs font-semibold', cfg.color)}>
        {cfg.icon}
        <span>{cfg.label}</span>
        {context?.table && <span className="font-mono text-[10px] opacity-70">· {context.table}</span>}
        {context?.field && <span className="font-mono text-[10px] opacity-70">· {context.field}</span>}
      </div>

      {/* Search bar for long lists */}
      {showSearch && (
        <div className="relative">
          <Search size={11} className="absolute left-2.5 top-1/2 -translate-y-1/2 text-slate-400" />
          <input
            type="text"
            value={search}
            onChange={e => setSearch(e.target.value)}
            placeholder="Filter…"
            className="w-full pl-7 pr-3 py-1.5 text-xs bg-slate-50 border border-slate-200 rounded-lg focus:outline-none focus:ring-1 focus:ring-emerald-400"
          />
        </div>
      )}

      {/* Options pills — two-column grid for value_selection with many items */}
      <div className={clsx(
        'flex gap-1.5',
        type === 'value_selection' && options.length > 6
          ? 'flex-wrap max-h-48 overflow-y-auto pr-1'
          : 'flex-wrap'
      )}>
        {filtered.map((opt, j) => (
          <button
            key={j}
            onClick={() => onSelect(opt)}
            className={clsx(
              'flex items-center gap-1.5 px-3 py-1.5 border rounded-lg text-xs font-medium transition-colors',
              cfg.pillColor
            )}
          >
            <CheckSquare size={11} />
            {opt}
          </button>
        ))}
        {filtered.length === 0 && (
          <p className="text-xs text-slate-400 italic">No match for "{search}"</p>
        )}
      </div>

      {/* Footer hint: user can also type freely */}
      <p className="text-[10px] text-slate-400 italic">
        Click an option or type your answer below.
      </p>
    </div>
  );
}

// ── AI Chart Panel ──────────────────────────────────────────────────────────

const AI_CHART_COLORS = ['#10b981', '#3b82f6', '#f59e0b', '#ef4444', '#8b5cf6', '#ec4899', '#06b6d4'];

interface AiChartPanelState {
  open: boolean;
  sql: string;
  data: any[];
  loading: boolean;
  error: string | null;
  chartType: 'bar' | 'line' | 'area' | 'pie';
  xKey: string;
  yKeys: string[];
  title: string;
}

export function ChatPane() {
  const {
    chatHistory, addChatMessage, clearChatHistory,
    chatConversationId,
    chatSqlHistory, addChatSqlEntry,
    schema, setQueryResult, queryHistory, setQueryHistory,
    tableMetadata, chatPaneSize, setChatPaneSize,
    tableMappings, selectedTableMappings, setSelectedTableMappings,
    agentMaxSteps,
  } = useAppStore();

  const [filterOpen, setFilterOpen] = useState(false);
  const [historyOpen, setHistoryOpen] = useState(false);

  const [input, setInput] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [isAgentLoading, setIsAgentLoading] = useState(false);
  const [expandedSteps, setExpandedSteps] = useState<Record<number, boolean>>({});
  const [dismissedExecActions, setDismissedExecActions] = useState<Set<number>>(new Set());

  // CSV export state
  const [exportDialogOpen, setExportDialogOpen] = useState(false);
  const [exportSql, setExportSql] = useState('');
  const [exportPath, setExportPath] = useState('');
  const [isExporting, setIsExporting] = useState(false);
  const [exportResult, setExportResult] = useState<{ success: boolean; message: string } | null>(null);
  const [aiChart, setAiChart] = useState<AiChartPanelState>({
    open: false, sql: '', data: [], loading: false, error: null,
    chartType: 'bar', xKey: '', yKeys: [], title: '',
  });
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
          conversation_id: chatConversationId,
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
          clarification_context: data.context,
        });
      } else {
        addChatMessage({
          role: 'assistant',
          content: data.explanation || 'Here is the query I generated:',
          sql: data.sql,
          visual: data.suggestedVisual
        });
        if (data.sql) {
          addChatSqlEntry({
            question: text,
            sql: data.sql,
            ts: new Date().toISOString(),
          });
        }
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
          maxSteps: agentMaxSteps,
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

  const openAiChart = async (sql: string, title: string) => {
    setAiChart(prev => ({ ...prev, open: true, sql, title, data: [], loading: true, error: null }));
    try {
      const res = await fetch('/api/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query: sql }),
      });
      const d = await res.json();
      if (d.error) throw new Error(d.error);
      const data = d.data || [];
      const keys = data.length > 0 ? Object.keys(data[0]) : [];
      setAiChart(prev => ({
        ...prev, loading: false, data,
        xKey: keys[0] || '',
        yKeys: keys.slice(1, 4).filter(Boolean),
      }));
    } catch (e: any) {
      setAiChart(prev => ({ ...prev, loading: false, error: e.message }));
    }
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
      setExportResult({ success: true, message: `${data.row_count.toLocaleString()} rows exported → ${data.path}` });
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

  // Schema-based starters: one suggestion per known table
  const schemaTableNames = Object.keys(schema);
  const schemaSuggestions = schemaTableNames.slice(0, 6).map(t => {
    const mapping = tableMappings.find(m => m.table_name === t);
    const label = mapping?.mapping_name ?? t;
    return { label, table: t };
  });

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

  const renderAiChartPanel = () => {
    if (!aiChart.open) return null;
    const keys = aiChart.data.length > 0 ? Object.keys(aiChart.data[0]) : [];
    const numericKeys = keys.filter(k => {
      const v = aiChart.data[0]?.[k];
      return typeof v === 'number' || (!isNaN(Number(v)) && v !== '' && v !== null);
    });
    const yKeys = aiChart.yKeys.length > 0 ? aiChart.yKeys : (numericKeys.length > 0 ? [numericKeys[0]] : []);
    const xKey = aiChart.xKey || (keys[0] ?? '');
    return (
      <div className="w-80 shrink-0 bg-white border-l border-slate-200 flex flex-col h-full overflow-hidden">
        <div className="px-3 py-2.5 border-b border-slate-200 bg-gradient-to-r from-violet-600 to-purple-700 flex items-center justify-between shrink-0">
          <div className="flex items-center gap-2">
            <BarChart2 size={14} className="text-white" />
            <span className="text-sm font-semibold text-white">AI Chart</span>
          </div>
          <button onClick={() => setAiChart(prev => ({ ...prev, open: false }))} className="text-white/70 hover:text-white p-0.5">
            <X size={15} />
          </button>
        </div>
        <div className="p-3 border-b border-slate-100 space-y-2 shrink-0 overflow-y-auto" style={{ maxHeight: '180px' }}>
          <div>
            <label className="text-[10px] font-bold text-slate-500 uppercase tracking-wider block mb-1">Chart Type</label>
            <div className="flex gap-1">
              {(['bar', 'line', 'area', 'pie'] as const).map(t => (
                <button key={t} onClick={() => setAiChart(prev => ({ ...prev, chartType: t }))}
                  className={clsx("flex-1 py-1 rounded text-xs font-medium transition-colors capitalize", aiChart.chartType === t ? "bg-violet-600 text-white" : "bg-slate-100 text-slate-600 hover:bg-slate-200")}>
                  {t}
                </button>
              ))}
            </div>
          </div>
          {aiChart.chartType !== 'pie' && keys.length > 0 && (
            <div>
              <label className="text-[10px] font-bold text-slate-500 uppercase tracking-wider block mb-1">X Axis</label>
              <select value={aiChart.xKey} onChange={e => setAiChart(prev => ({ ...prev, xKey: e.target.value }))} className="w-full text-xs border border-slate-200 rounded px-2 py-1 bg-slate-50">
                {keys.map(k => <option key={k} value={k}>{k}</option>)}
              </select>
            </div>
          )}
          <div>
            <label className="text-[10px] font-bold text-slate-500 uppercase tracking-wider block mb-1">{aiChart.chartType === 'pie' ? 'Value' : 'Y Axis'}</label>
            <div className="flex flex-wrap gap-1">
              {(aiChart.chartType === 'pie' ? keys : numericKeys).map(k => (
                <button key={k} onClick={() => {
                  if (aiChart.chartType === 'pie') {
                    setAiChart(prev => ({ ...prev, yKeys: [k] }));
                  } else {
                    setAiChart(prev => ({ ...prev, yKeys: prev.yKeys.includes(k) ? prev.yKeys.filter(y => y !== k) : [...prev.yKeys, k] }));
                  }
                }}
                  className={clsx("text-[10px] px-1.5 py-0.5 rounded border transition-colors", aiChart.yKeys.includes(k) ? "bg-violet-100 border-violet-300 text-violet-700" : "bg-white border-slate-200 text-slate-500 hover:border-violet-300")}>
                  {k}
                </button>
              ))}
            </div>
          </div>
        </div>
        <div className="flex-1 min-h-0 overflow-auto p-2">
          {aiChart.loading ? (
            <div className="flex items-center justify-center h-32"><Loader2 className="animate-spin text-violet-500" size={20} /></div>
          ) : aiChart.error ? (
            <div className="text-xs text-red-600 p-2 bg-red-50 rounded">{aiChart.error}</div>
          ) : aiChart.data.length === 0 ? (
            <div className="text-xs text-slate-400 text-center py-8">No data</div>
          ) : aiChart.chartType === 'bar' ? (
            <ResponsiveContainer width="100%" height={200}>
              <BarChart data={aiChart.data}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                <XAxis dataKey={xKey} tick={{ fontSize: 9 }} />
                <YAxis tick={{ fontSize: 9 }} />
                <RechartsTooltip contentStyle={{ fontSize: 10 }} />
                <Legend wrapperStyle={{ fontSize: 10 }} />
                {yKeys.map((k, i) => <Bar key={k} dataKey={k} fill={AI_CHART_COLORS[i % AI_CHART_COLORS.length]} radius={[3,3,0,0]} />)}
              </BarChart>
            </ResponsiveContainer>
          ) : aiChart.chartType === 'line' ? (
            <ResponsiveContainer width="100%" height={200}>
              <RechartsLineChart data={aiChart.data}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                <XAxis dataKey={xKey} tick={{ fontSize: 9 }} />
                <YAxis tick={{ fontSize: 9 }} />
                <RechartsTooltip contentStyle={{ fontSize: 10 }} />
                <Legend wrapperStyle={{ fontSize: 10 }} />
                {yKeys.map((k, i) => <Line key={k} type="monotone" dataKey={k} stroke={AI_CHART_COLORS[i % AI_CHART_COLORS.length]} strokeWidth={2} dot={false} />)}
              </RechartsLineChart>
            </ResponsiveContainer>
          ) : aiChart.chartType === 'area' ? (
            <ResponsiveContainer width="100%" height={200}>
              <RechartsAreaChart data={aiChart.data}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                <XAxis dataKey={xKey} tick={{ fontSize: 9 }} />
                <YAxis tick={{ fontSize: 9 }} />
                <RechartsTooltip contentStyle={{ fontSize: 10 }} />
                <Legend wrapperStyle={{ fontSize: 10 }} />
                {yKeys.map((k, i) => <Area key={k} type="monotone" dataKey={k} stroke={AI_CHART_COLORS[i % AI_CHART_COLORS.length]} fill={AI_CHART_COLORS[i % AI_CHART_COLORS.length]} fillOpacity={0.2} strokeWidth={2} />)}
              </RechartsAreaChart>
            </ResponsiveContainer>
          ) : (() => {
            const valueKey = yKeys[0] || xKey;
            const nameKey = keys.find(k => k !== valueKey) || xKey;
            return (
              <ResponsiveContainer width="100%" height={200}>
                <RechartsPieChart>
                  <Pie data={aiChart.data} dataKey={valueKey} nameKey={nameKey} cx="50%" cy="50%" outerRadius={70} innerRadius={35} paddingAngle={2}>
                    {aiChart.data.map((_, idx) => <Cell key={idx} fill={AI_CHART_COLORS[idx % AI_CHART_COLORS.length]} />)}
                  </Pie>
                  <RechartsTooltip contentStyle={{ fontSize: 10 }} />
                  <Legend wrapperStyle={{ fontSize: 10 }} />
                </RechartsPieChart>
              </ResponsiveContainer>
            );
          })()}
        </div>
        {aiChart.data.length > 0 && (
          <div className="px-3 py-1.5 border-t border-slate-100 text-[10px] text-slate-400 shrink-0">{aiChart.data.length} rows loaded</div>
        )}
      </div>
    );
  };

  return (
    <div className="flex flex-col h-full bg-slate-50 border-r border-slate-200" style={aiChart.open ? { flexDirection: 'row', flexWrap: 'nowrap' } : {}}>
      <div className={clsx("flex flex-col min-w-0", aiChart.open ? "flex-1" : "h-full")}>
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
            {chatSqlHistory.length > 0 && (
              <button
                onClick={() => { setHistoryOpen(o => !o); setFilterOpen(false); }}
                className={clsx(
                  "p-1.5 rounded-lg transition-colors relative",
                  historyOpen
                    ? "text-emerald-600 bg-emerald-50 hover:bg-emerald-100"
                    : "text-slate-400 hover:text-slate-700 hover:bg-slate-100"
                )}
                title="Query history"
              >
                <History size={15} />
                <span className="absolute -top-1 -right-1 w-4 h-4 bg-emerald-500 text-white text-[10px] font-bold rounded-full flex items-center justify-center">
                  {chatSqlHistory.length}
                </span>
              </button>
            )}
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
                title="Clear conversation (new session)"
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

        {/* History panel */}
        <AnimatePresence>
          {historyOpen && chatSqlHistory.length > 0 && (
            <motion.div
              initial={{ height: 0, opacity: 0 }}
              animate={{ height: 'auto', opacity: 1 }}
              exit={{ height: 0, opacity: 0 }}
              className="overflow-hidden border-t border-slate-100"
            >
              <div className="px-4 py-3 max-h-72 overflow-y-auto">
                <div className="flex items-center justify-between mb-2">
                  <p className="text-xs font-semibold text-slate-500 uppercase tracking-wide flex items-center gap-1.5">
                    <History size={11} />
                    Recent SQL queries
                  </p>
                  <button
                    onClick={() => setHistoryOpen(false)}
                    className="text-xs text-slate-400 hover:text-slate-600"
                  >
                    <X size={12} />
                  </button>
                </div>
                <div className="space-y-2">
                  {chatSqlHistory.map((entry, idx) => (
                    <div key={idx} className="bg-slate-50 border border-slate-200 rounded-lg overflow-hidden">
                      <div className="flex items-center justify-between px-3 py-1.5 bg-slate-100 border-b border-slate-200">
                        <span className="text-[10px] text-slate-500 truncate flex-1 pr-2">{entry.question}</span>
                        <div className="flex gap-1 shrink-0">
                          <button
                            onClick={() => { handleSend(entry.question); setHistoryOpen(false); }}
                            className="flex items-center gap-1 text-[10px] bg-emerald-500 hover:bg-emerald-600 text-white px-2 py-0.5 rounded transition-colors"
                            title="Re-run this query"
                          >
                            <Play size={9} />
                            Re-run
                          </button>
                          <button
                            onClick={async () => {
                              try {
                                const res = await fetch('/api/query', {
                                  method: 'POST',
                                  headers: { 'Content-Type': 'application/json' },
                                  body: JSON.stringify({ query: entry.sql }),
                                });
                                const data = await res.json();
                                if (!data.error) setQueryResult(data.data);
                              } catch {}
                              setHistoryOpen(false);
                            }}
                            className="flex items-center gap-1 text-[10px] bg-blue-500 hover:bg-blue-600 text-white px-2 py-0.5 rounded transition-colors"
                            title="Execute SQL directly"
                          >
                            <Zap size={9} />
                            SQL
                          </button>
                        </div>
                      </div>
                      <pre className="px-3 py-1.5 text-[10px] font-mono text-emerald-700 overflow-x-auto whitespace-pre-wrap">
                        {entry.sql}
                      </pre>
                    </div>
                  ))}
                </div>
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
          <div className="flex flex-col items-center justify-center h-full text-center space-y-5 py-6">
            <div className="w-14 h-14 bg-emerald-100 rounded-full flex items-center justify-center text-emerald-500">
              <Bot size={28} />
            </div>
            <div>
              <h3 className="text-lg font-medium text-slate-800 mb-1">How can I help you analyze your data?</h3>
              <p className="text-slate-500 text-sm max-w-md mx-auto">
                Ask me anything in plain language — I will guide you step by step, propose choices, and generate the right SQL query.
              </p>
            </div>

            {/* Schema-based table starters */}
            {schemaSuggestions.length > 0 && (
              <div className="w-full max-w-lg">
                <p className="text-xs font-semibold text-slate-400 uppercase tracking-wide mb-2 flex items-center justify-center gap-1.5">
                  <Table2 size={11} /> Available tables
                </p>
                <div className="flex flex-wrap gap-2 justify-center">
                  {schemaSuggestions.map((s, i) => (
                    <button
                      key={i}
                      onClick={() => handleSend(`Tell me about the table ${s.table} and suggest some analyses`)}
                      className="bg-white border border-slate-200 px-3 py-1.5 rounded-full text-xs text-slate-600 hover:border-emerald-500 hover:text-emerald-600 transition-colors shadow-sm flex items-center gap-1.5"
                    >
                      <Database size={11} className="text-emerald-500" />
                      {s.label}
                    </button>
                  ))}
                </div>
              </div>
            )}

            {/* Quick idea starters */}
            <div className="w-full max-w-lg">
              <p className="text-xs font-semibold text-slate-400 uppercase tracking-wide mb-2 flex items-center justify-center gap-1.5">
                <Lightbulb size={11} /> Quick ideas
              </p>
              <div className="flex flex-wrap gap-2 justify-center">
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
                      Agent Analysis
                    </span>
                    {msg.agent_steps && (
                      <span className="text-[10px] text-indigo-400">{msg.agent_steps.length} step{msg.agent_steps.length > 1 ? 's' : ''}</span>
                    )}
                  </div>
                )}
                <MarkdownContent text={msg.content} isUser={msg.role === 'user'} />

                {/* Clarification options — rich per-type UI */}
                {msg.needs_clarification && (
                  <ClarificationPanel
                    type={msg.clarification_type ?? 'field_selection'}
                    options={msg.options ?? []}
                    context={msg.clarification_context}
                    onSelect={handleSend}
                  />
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
                          title="Export as CSV (pipe separator, max 1M rows)"
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
                        <button
                          onClick={() => openAiChart(msg.sql!, chatHistory[i - 1]?.content || 'Chart')}
                          className="flex items-center gap-1 text-xs bg-violet-600 hover:bg-violet-500 text-white px-2.5 py-1 rounded-md transition-colors"
                          title="Create AI chart from this query"
                        >
                          <BarChart2 size={11} />
                          AI Chart
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
                        Agent — {msg.agent_steps.length} analysis{msg.agent_steps.length > 1 ? 'es' : ''} performed
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
                              <span className="font-medium text-indigo-800">Step {step.step}</span>
                              <span className="text-indigo-500 truncate flex-1">{step.reasoning}</span>
                              <span className="text-indigo-400 shrink-0 flex items-center gap-1">
                                {step.type === 'search_knowledge'
                                  ? <BookOpen size={10} className="text-violet-400" />
                                  : step.type === 'export_csv'
                                    ? <Download size={10} className="text-amber-400" />
                                    : <Database size={10} />
                                }
                                {step.type === 'search_knowledge' ? 'KB' : step.type === 'export_csv' ? 'Export' : `${step.row_count} row${step.row_count !== 1 ? 's' : ''}`}
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
                                      <span className="text-[10px] font-mono text-violet-600">Knowledge base search</span>
                                    </div>
                                    <div className="p-2">
                                      <p className="text-[10px] font-semibold text-violet-600 mb-1">Query</p>
                                      <p className="text-[10px] text-violet-700 font-mono italic">{step.search_query}</p>
                                    </div>
                                  </div>
                                ) : step.type === 'export_csv' ? (
                                  <div className="bg-amber-50 border border-amber-200 rounded-lg overflow-hidden">
                                    <div className="px-2 py-1 bg-amber-100/60 border-b border-amber-200">
                                      <span className="text-[10px] font-mono text-amber-700 flex items-center gap-1">
                                        <Download size={9} /> CSV Export requested
                                      </span>
                                    </div>
                                    <div className="p-2 space-y-2">
                                      <div>
                                        <p className="text-[10px] font-semibold text-amber-700 mb-1">Export SQL</p>
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
                                        Confirm and export
                                      </button>
                                    </div>
                                  </div>
                                ) : (
                                  <div className="bg-slate-900 rounded-lg overflow-hidden">
                                    <div className="px-2 py-1 bg-slate-800/50 border-b border-slate-700">
                                      <span className="text-[10px] font-mono text-slate-400">SQL executed</span>
                                    </div>
                                    <pre className="p-2 text-[10px] font-mono text-emerald-400 overflow-x-auto whitespace-pre-wrap">
                                      {step.sql}
                                    </pre>
                                  </div>
                                )}
                                {step.type !== 'export_csv' && (
                                  <div className="bg-white border border-indigo-100 rounded-lg p-2">
                                    <p className="text-[10px] font-semibold text-slate-500 mb-1">Result</p>
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

                {/* Executive Summary action buttons — shown after agent analysis */}
                {msg.is_agent && msg.role === 'assistant' && (
                  <ExecSummaryActions
                    content={msg.content}
                    dismissed={dismissedExecActions.has(i)}
                    onDismiss={() => setDismissedExecActions(prev => new Set([...prev, i]))}
                  />
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
                <span className="text-xs font-medium text-indigo-700">Agent analyzing…</span>
              </div>
              <p className="text-[11px] text-indigo-500">
                The agent runs iterative queries on your ClickHouse data (up to {agentMaxSteps} steps).
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
            title="Send (quick chat)"
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
          title={`Multi-step in-depth analysis by the AI agent (up to ${agentMaxSteps} queries)`}
        >
          {isAgentLoading
            ? <Loader2 size={15} className="animate-spin" />
            : <Brain size={15} />
          }
          {isAgentLoading ? 'Agent running…' : 'Analyze with Agent'}
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
                <h3 className="text-sm font-bold text-slate-800">Export as CSV</h3>
              </div>
              <button onClick={() => setExportDialogOpen(false)} className="text-slate-400 hover:text-slate-600 p-1.5 hover:bg-slate-100 rounded-md">
                <X size={16} />
              </button>
            </div>
            <div className="p-5 space-y-4">
              <div className="bg-amber-50 border border-amber-200 rounded-xl p-3 text-xs text-amber-700 space-y-1">
                <p className="font-semibold">Format: CSV with pipe separator ( | )</p>
                <p>Limit: 1,000,000 rows maximum</p>
              </div>
              <div>
                <label className="block text-xs font-semibold text-slate-600 uppercase tracking-wide mb-1">
                  <FolderOpen size={12} className="inline mr-1" />
                  Directory / destination file
                </label>
                <input
                  type="text"
                  value={exportPath}
                  onChange={e => { setExportPath(e.target.value); setExportResult(null); }}
                  placeholder="/home/user/my_data/export.csv"
                  className="w-full bg-slate-50 border border-slate-200 rounded-lg px-3 py-2 text-sm font-mono focus:ring-2 focus:ring-amber-500/20 focus:border-amber-500 transition-all"
                />
                <p className="mt-1 text-xs text-slate-400">Absolute path on the server (e.g.: /home/user/export.csv)</p>
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
                  {exportResult?.success ? 'Close' : 'Cancel'}
                </button>
                {!exportResult?.success && (
                  <button
                    onClick={handleExportCsv}
                    disabled={isExporting || !exportPath.trim()}
                    className="flex items-center gap-2 bg-amber-500 hover:bg-amber-600 disabled:opacity-50 text-white px-4 py-2 rounded-lg text-sm font-medium transition-colors"
                  >
                    {isExporting ? <><Loader2 size={14} className="animate-spin" />Exporting…</> : <><Download size={14} />Export</>}
                  </button>
                )}
              </div>
            </div>
          </div>
        </div>
      )}
      </div>
      {renderAiChartPanel()}
    </div>
  );
}
