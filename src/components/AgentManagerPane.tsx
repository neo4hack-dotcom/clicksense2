import { useEffect, useMemo, useState } from 'react';
import {
  Activity, AlertTriangle, ArrowDown, ArrowUp, Bot, CalendarClock, CheckCircle2,
  Clock3, Layers, Play, Plus, Save, Square, Trash2, Wrench, Zap,
} from 'lucide-react';
import clsx from 'clsx';

interface ManagerAgentParam {
  name: string;
  label: string;
  type: 'string' | 'number' | 'select';
  default: string | number;
  description: string;
  options?: string[];
}

interface ManagerAgent {
  id: string;
  name: string;
  description: string;
  runtime: boolean;
  interactive_possible: boolean;
  parameters: ManagerAgentParam[];
}

interface ManagerSchedule {
  mode: 'disabled' | 'interval' | 'daily';
  interval_minutes: number;
  daily_time: string;
  timezone: string;
}

interface ManagerStep {
  id: string;
  order: number;
  agent_id: string;
  title: string;
  prompt: string;
  params: Record<string, unknown>;
  halt_on_error: boolean;
  timeout_seconds: number;
  auto_approve_questions: boolean;
  auto_reply_text: string;
  max_followups: number;
}

interface ManagerWorkflow {
  id: string;
  name: string;
  description: string;
  objective: string;
  default_input: string;
  enabled: boolean;
  schedule: ManagerSchedule;
  steps: ManagerStep[];
  created_at: string;
  updated_at: string;
  last_run_at?: string | null;
  next_run_at?: string | null;
}

interface ManagerRunSummary {
  id: string;
  workflow_id: string;
  workflow_name: string;
  trigger: string;
  status: string;
  summary: string;
  error?: string;
  stop_requested?: boolean;
  created_at?: string;
  started_at?: string;
  completed_at?: string;
  updated_at?: string;
  step_results_count?: number;
}

interface ManagerRunLog {
  seq: number;
  ts: string;
  level: string;
  kind: string;
  message: string;
}

interface ManagerRunStepResult {
  step_id: string;
  step_index: number;
  title: string;
  agent_id: string;
  status: string;
  started_at: string;
  ended_at: string;
  error?: string;
  output_preview?: string;
  raw_payload?: string;
}

interface ManagerRunDetail extends ManagerRunSummary {
  input?: string;
  logs?: ManagerRunLog[];
  step_results?: ManagerRunStepResult[];
}

interface WorkflowStepDraft {
  id: string;
  agent_id: string;
  title: string;
  prompt: string;
  params_text: string;
  halt_on_error: boolean;
  timeout_seconds: number;
  auto_approve_questions: boolean;
  auto_reply_text: string;
  max_followups: number;
}

interface WorkflowDraft {
  id?: string;
  name: string;
  description: string;
  objective: string;
  default_input: string;
  enabled: boolean;
  schedule: ManagerSchedule;
  steps: WorkflowStepDraft[];
}

const ACTIVE_RUN_STATUSES = new Set(['queued', 'running', 'stopping', 'pausing']);

const detectTimezone = (): string => {
  try {
    return Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC';
  } catch {
    return 'UTC';
  }
};

const createStepDraft = (agentId = 'ai-data-analyst'): WorkflowStepDraft => ({
  id: crypto.randomUUID(),
  agent_id: agentId,
  title: 'New step',
  prompt: '',
  params_text: '{}',
  halt_on_error: true,
  timeout_seconds: 420,
  auto_approve_questions: false,
  auto_reply_text: 'oui',
  max_followups: 2,
});

const createWorkflowDraft = (): WorkflowDraft => ({
  name: '',
  description: '',
  objective: '',
  default_input: '',
  enabled: false,
  schedule: {
    mode: 'disabled',
    interval_minutes: 60,
    daily_time: '09:00',
    timezone: detectTimezone(),
  },
  steps: [createStepDraft()],
});

function formatDateTime(value?: string | null): string {
  if (!value) return '—';
  try {
    return new Date(value).toLocaleString();
  } catch {
    return value;
  }
}

function runStatusClass(status: string): string {
  const s = status.toLowerCase();
  if (s === 'completed') return 'bg-emerald-50 text-emerald-700 border-emerald-200';
  if (s === 'failed') return 'bg-rose-50 text-rose-700 border-rose-200';
  if (s === 'stopped') return 'bg-amber-50 text-amber-700 border-amber-200';
  if (ACTIVE_RUN_STATUSES.has(s)) return 'bg-sky-50 text-sky-700 border-sky-200';
  return 'bg-slate-100 text-slate-700 border-slate-200';
}

function workflowToDraft(workflow: ManagerWorkflow): WorkflowDraft {
  return {
    id: workflow.id,
    name: workflow.name || '',
    description: workflow.description || '',
    objective: workflow.objective || '',
    default_input: workflow.default_input || '',
    enabled: !!workflow.enabled,
    schedule: {
      mode: workflow.schedule?.mode || 'disabled',
      interval_minutes: Number(workflow.schedule?.interval_minutes || 60),
      daily_time: workflow.schedule?.daily_time || '09:00',
      timezone: workflow.schedule?.timezone || detectTimezone(),
    },
    steps: (workflow.steps || []).map((step, idx) => ({
      id: step.id || crypto.randomUUID(),
      agent_id: step.agent_id || 'ai-data-analyst',
      title: step.title || `Step ${idx + 1}`,
      prompt: step.prompt || '',
      params_text: JSON.stringify(step.params || {}, null, 2),
      halt_on_error: step.halt_on_error !== false,
      timeout_seconds: Number(step.timeout_seconds || 420),
      auto_approve_questions: !!step.auto_approve_questions,
      auto_reply_text: step.auto_reply_text || 'oui',
      max_followups: Number(step.max_followups ?? 2),
    })),
  };
}

function stepDefaultsByAgent(agents: ManagerAgent[]): Record<string, Record<string, unknown>> {
  const out: Record<string, Record<string, unknown>> = {};
  agents.forEach(agent => {
    const params: Record<string, unknown> = {};
    (agent.parameters || []).forEach(p => {
      params[p.name] = p.default;
    });
    out[agent.id] = params;
  });
  return out;
}

export function AgentManagerPane() {
  const [agents, setAgents] = useState<ManagerAgent[]>([]);
  const [workflows, setWorkflows] = useState<ManagerWorkflow[]>([]);
  const [runs, setRuns] = useState<ManagerRunSummary[]>([]);
  const [selectedWorkflowId, setSelectedWorkflowId] = useState<string>('');
  const [selectedRunId, setSelectedRunId] = useState<string>('');
  const [runDetail, setRunDetail] = useState<ManagerRunDetail | null>(null);
  const [draft, setDraft] = useState<WorkflowDraft>(createWorkflowDraft());
  const [runInput, setRunInput] = useState('');
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [running, setRunning] = useState(false);
  const [busyStop, setBusyStop] = useState(false);
  const [error, setError] = useState('');
  const [info, setInfo] = useState('');

  const selectedWorkflow = useMemo(
    () => workflows.find(w => w.id === selectedWorkflowId) || null,
    [workflows, selectedWorkflowId],
  );

  const agentById = useMemo(() => {
    const map: Record<string, ManagerAgent> = {};
    agents.forEach(a => { map[a.id] = a; });
    return map;
  }, [agents]);

  const stepParamDefaults = useMemo(() => stepDefaultsByAgent(agents), [agents]);
  const activeRun = runDetail && ACTIVE_RUN_STATUSES.has((runDetail.status || '').toLowerCase());

  async function loadAgents() {
    const res = await fetch('/api/agent-manager/agents');
    const data = await res.json();
    setAgents(Array.isArray(data.agents) ? data.agents : []);
  }

  async function loadWorkflows() {
    const res = await fetch('/api/agent-manager/workflows');
    const data = await res.json();
    const items: ManagerWorkflow[] = Array.isArray(data.workflows) ? data.workflows : [];
    setWorkflows(items);

    if (!selectedWorkflowId && items.length > 0) {
      setSelectedWorkflowId(items[0].id);
      setDraft(workflowToDraft(items[0]));
      setRunInput(items[0].default_input || '');
    }

    if (selectedWorkflowId) {
      const found = items.find(w => w.id === selectedWorkflowId);
      if (!found) {
        setSelectedWorkflowId('');
        setDraft(createWorkflowDraft());
        setRunInput('');
      }
    }
  }

  async function loadRuns(workflowId?: string) {
    const wfId = workflowId || selectedWorkflowId;
    if (!wfId) {
      setRuns([]);
      setRunDetail(null);
      return;
    }
    const res = await fetch(`/api/agent-manager/runs?workflow_id=${encodeURIComponent(wfId)}&limit=40`);
    const data = await res.json();
    const items: ManagerRunSummary[] = Array.isArray(data.runs) ? data.runs : [];
    setRuns(items);

    if (!selectedRunId && items.length > 0) {
      setSelectedRunId(items[0].id);
      await loadRunDetail(items[0].id);
    } else if (selectedRunId) {
      const current = items.find(r => r.id === selectedRunId);
      if (!current) {
        setSelectedRunId(items[0]?.id || '');
        if (items[0]) await loadRunDetail(items[0].id);
        else setRunDetail(null);
      }
    }
  }

  async function loadRunDetail(runId: string) {
    if (!runId) {
      setRunDetail(null);
      return;
    }
    const res = await fetch(`/api/agent-manager/runs/${encodeURIComponent(runId)}`);
    const data = await res.json();
    if (data.run) setRunDetail(data.run);
  }

  async function refreshAll() {
    setError('');
    try {
      setLoading(true);
      await loadAgents();
      await loadWorkflows();
      await loadRuns();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load Agent Manager data.');
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    refreshAll();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    if (!selectedWorkflowId) return;
    const wf = workflows.find(w => w.id === selectedWorkflowId);
    if (wf) {
      setRunInput(wf.default_input || '');
    }
  }, [selectedWorkflowId, workflows]);

  useEffect(() => {
    if (!selectedWorkflowId) return;
    loadRuns(selectedWorkflowId);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedWorkflowId]);

  useEffect(() => {
    if (!selectedRunId) return;
    loadRunDetail(selectedRunId);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedRunId]);

  useEffect(() => {
    const shouldPoll = !!(runDetail && ACTIVE_RUN_STATUSES.has((runDetail.status || '').toLowerCase()));
    if (!shouldPoll || !selectedWorkflowId) return;
    const timer = window.setInterval(() => {
      loadRuns(selectedWorkflowId);
      if (selectedRunId) loadRunDetail(selectedRunId);
      loadWorkflows();
    }, 1800);
    return () => window.clearInterval(timer);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [runDetail?.status, selectedWorkflowId, selectedRunId]);

  function selectWorkflow(workflow: ManagerWorkflow) {
    setSelectedWorkflowId(workflow.id);
    setDraft(workflowToDraft(workflow));
    setRunInput(workflow.default_input || '');
    setSelectedRunId('');
    setRunDetail(null);
    setInfo('');
    setError('');
  }

  function createNewWorkflow() {
    setSelectedWorkflowId('');
    setDraft(createWorkflowDraft());
    setRunInput('');
    setSelectedRunId('');
    setRunDetail(null);
    setInfo('New workflow draft ready.');
    setError('');
  }

  function updateDraft<K extends keyof WorkflowDraft>(field: K, value: WorkflowDraft[K]) {
    setDraft(prev => ({ ...prev, [field]: value }));
  }

  function updateSchedule<K extends keyof ManagerSchedule>(field: K, value: ManagerSchedule[K]) {
    setDraft(prev => ({
      ...prev,
      schedule: { ...prev.schedule, [field]: value },
    }));
  }

  function updateStep(stepId: string, patch: Partial<WorkflowStepDraft>) {
    setDraft(prev => ({
      ...prev,
      steps: prev.steps.map(step => (step.id === stepId ? { ...step, ...patch } : step)),
    }));
  }

  function moveStep(stepId: string, direction: -1 | 1) {
    setDraft(prev => {
      const idx = prev.steps.findIndex(s => s.id === stepId);
      if (idx < 0) return prev;
      const nextIdx = idx + direction;
      if (nextIdx < 0 || nextIdx >= prev.steps.length) return prev;
      const clone = [...prev.steps];
      const [item] = clone.splice(idx, 1);
      clone.splice(nextIdx, 0, item);
      return { ...prev, steps: clone };
    });
  }

  function removeStep(stepId: string) {
    setDraft(prev => {
      if (prev.steps.length <= 1) return prev;
      return { ...prev, steps: prev.steps.filter(s => s.id !== stepId) };
    });
  }

  function addStep() {
    const defaultAgent = agents[0]?.id || 'ai-data-analyst';
    const newStep = createStepDraft(defaultAgent);
    if (stepParamDefaults[defaultAgent]) {
      newStep.params_text = JSON.stringify(stepParamDefaults[defaultAgent], null, 2);
    }
    setDraft(prev => ({
      ...prev,
      steps: [...prev.steps, newStep],
    }));
  }

  function buildWorkflowPayload() {
    const trimmedName = draft.name.trim();
    if (!trimmedName) {
      throw new Error('Workflow name is required.');
    }
    const stepsPayload = draft.steps.map((step, idx) => {
      let parsedParams: Record<string, unknown> = {};
      const raw = step.params_text.trim();
      if (raw) {
        try {
          const parsed = JSON.parse(raw);
          if (typeof parsed === 'object' && parsed && !Array.isArray(parsed)) {
            parsedParams = parsed as Record<string, unknown>;
          } else {
            throw new Error(`Step ${idx + 1}: params must be a JSON object.`);
          }
        } catch (e) {
          throw new Error(`Step ${idx + 1} (${step.title || step.agent_id}) has invalid JSON params.`);
        }
      }
      return {
        id: step.id,
        agent_id: step.agent_id,
        title: step.title,
        prompt: step.prompt,
        params: parsedParams,
        halt_on_error: step.halt_on_error,
        timeout_seconds: Number(step.timeout_seconds || 420),
        auto_approve_questions: step.auto_approve_questions,
        auto_reply_text: step.auto_reply_text,
        max_followups: Number(step.max_followups || 2),
      };
    });

    return {
      name: trimmedName,
      description: draft.description,
      objective: draft.objective,
      default_input: draft.default_input,
      enabled: draft.enabled,
      schedule: draft.schedule,
      steps: stepsPayload,
    };
  }

  async function saveWorkflow() {
    setError('');
    setInfo('');
    let payload: Record<string, unknown>;
    try {
      payload = buildWorkflowPayload();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Invalid workflow payload.');
      return;
    }

    setSaving(true);
    try {
      const endpoint = draft.id
        ? `/api/agent-manager/workflows/${encodeURIComponent(draft.id)}`
        : '/api/agent-manager/workflows';
      const method = draft.id ? 'PUT' : 'POST';
      const res = await fetch(endpoint, {
        method,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'Unable to save workflow.');
      const saved: ManagerWorkflow = data.workflow;
      setInfo(draft.id ? 'Workflow updated.' : 'Workflow created.');
      setSelectedWorkflowId(saved.id);
      setDraft(workflowToDraft(saved));
      setRunInput(saved.default_input || '');
      await loadWorkflows();
      await loadRuns(saved.id);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Workflow save failed.');
    } finally {
      setSaving(false);
    }
  }

  async function deleteWorkflow() {
    if (!draft.id) {
      createNewWorkflow();
      return;
    }
    const confirmDelete = window.confirm(`Delete workflow "${draft.name}"?`);
    if (!confirmDelete) return;
    setError('');
    setInfo('');
    try {
      const res = await fetch(`/api/agent-manager/workflows/${encodeURIComponent(draft.id)}`, {
        method: 'DELETE',
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'Unable to delete workflow.');
      setInfo('Workflow deleted.');
      await loadWorkflows();
      createNewWorkflow();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Delete failed.');
    }
  }

  async function runWorkflowNow() {
    if (!draft.id) {
      setError('Save workflow before running it.');
      return;
    }
    setRunning(true);
    setError('');
    setInfo('');
    try {
      const res = await fetch(`/api/agent-manager/workflows/${encodeURIComponent(draft.id)}/run`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          input: runInput,
          trigger: 'manual',
        }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'Run launch failed.');
      const runId = data.run_id || data.run?.id;
      setInfo(`Run started${runId ? ` (#${String(runId).slice(0, 8)})` : ''}.`);
      await loadRuns(draft.id);
      if (runId) {
        setSelectedRunId(runId);
        await loadRunDetail(runId);
      }
      await loadWorkflows();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Run failed to start.');
    } finally {
      setRunning(false);
    }
  }

  async function stopRun() {
    if (!runDetail?.id) return;
    setBusyStop(true);
    setError('');
    setInfo('');
    try {
      const res = await fetch(`/api/agent-manager/runs/${encodeURIComponent(runDetail.id)}/stop`, {
        method: 'POST',
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'Stop request failed.');
      setInfo('Stop requested.');
      await loadRunDetail(runDetail.id);
      await loadRuns(selectedWorkflowId);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Stop request failed.');
    } finally {
      setBusyStop(false);
    }
  }

  if (loading) {
    return (
      <div className="h-full w-full flex items-center justify-center bg-gradient-to-br from-slate-50 via-white to-sky-50">
        <div className="flex items-center gap-3 text-slate-600">
          <Activity className="animate-pulse" size={18} />
          Loading Agent Manager...
        </div>
      </div>
    );
  }

  return (
    <div className="h-full w-full flex overflow-hidden bg-gradient-to-br from-slate-50 via-white to-cyan-50/40">
      <aside className="w-80 border-r border-slate-200 bg-white/90 backdrop-blur-sm flex flex-col">
        <div className="p-4 border-b border-slate-200 bg-gradient-to-r from-slate-900 to-slate-800 text-white">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-[11px] uppercase tracking-[0.18em] text-slate-300">Agentic Orchestration</p>
              <h2 className="text-lg font-semibold">Agent Manager</h2>
            </div>
            <button
              onClick={createNewWorkflow}
              className="inline-flex items-center gap-1.5 rounded-lg bg-emerald-500 px-2.5 py-1.5 text-xs font-semibold text-white hover:bg-emerald-600 transition-colors"
            >
              <Plus size={14} />
              New
            </button>
          </div>
        </div>
        <div className="p-3 border-b border-slate-100">
          <button
            onClick={refreshAll}
            className="w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-xs font-medium text-slate-700 hover:bg-slate-50 transition-colors"
          >
            Refresh manager state
          </button>
        </div>
        <div className="flex-1 overflow-y-auto p-3 space-y-2">
          {workflows.length === 0 && (
            <div className="rounded-xl border border-dashed border-slate-300 p-4 text-xs text-slate-500">
              No workflow yet. Create your first orchestration flow.
            </div>
          )}
          {workflows.map(wf => (
            <button
              key={wf.id}
              onClick={() => selectWorkflow(wf)}
              className={clsx(
                'w-full text-left rounded-xl border px-3 py-3 transition-all',
                selectedWorkflowId === wf.id
                  ? 'border-emerald-300 bg-emerald-50 shadow-sm'
                  : 'border-slate-200 bg-white hover:border-slate-300 hover:bg-slate-50',
              )}
            >
              <div className="flex items-start justify-between gap-2">
                <p className="text-sm font-semibold text-slate-800 line-clamp-2">{wf.name}</p>
                <span
                  className={clsx(
                    'text-[10px] px-2 py-0.5 rounded-full border',
                    wf.enabled
                      ? 'bg-emerald-100 border-emerald-200 text-emerald-700'
                      : 'bg-slate-100 border-slate-200 text-slate-500',
                  )}
                >
                  {wf.enabled ? 'Scheduled' : 'Manual'}
                </span>
              </div>
              <p className="text-xs text-slate-500 mt-1 line-clamp-2">{wf.description || 'No description'}</p>
              <div className="mt-2 flex items-center justify-between text-[11px] text-slate-500">
                <span>{wf.steps?.length || 0} step(s)</span>
                <span>Next: {formatDateTime(wf.next_run_at)}</span>
              </div>
            </button>
          ))}
        </div>
      </aside>

      <main className="flex-1 min-w-0 flex overflow-hidden">
        <section className="flex-1 overflow-y-auto p-5">
          <div className="max-w-5xl mx-auto space-y-4">
            <div className="rounded-2xl border border-slate-200 bg-white shadow-sm overflow-hidden">
              <div className="px-5 py-4 border-b border-slate-100 flex flex-wrap items-center justify-between gap-3 bg-gradient-to-r from-white via-sky-50/60 to-cyan-50/70">
                <div>
                  <p className="text-[11px] font-semibold tracking-widest uppercase text-sky-600">Workflow Designer</p>
                  <h3 className="text-lg font-semibold text-slate-800">
                    {draft.id ? 'Edit Orchestration' : 'Create Orchestration'}
                  </h3>
                </div>
                <div className="flex items-center gap-2">
                  <button
                    onClick={saveWorkflow}
                    disabled={saving}
                    className="inline-flex items-center gap-1.5 rounded-lg bg-emerald-600 px-3 py-2 text-xs font-semibold text-white hover:bg-emerald-700 disabled:opacity-50"
                  >
                    <Save size={14} />
                    {saving ? 'Saving...' : 'Save'}
                  </button>
                  <button
                    onClick={deleteWorkflow}
                    className="inline-flex items-center gap-1.5 rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-xs font-semibold text-rose-700 hover:bg-rose-100"
                  >
                    <Trash2 size={14} />
                    Delete
                  </button>
                </div>
              </div>

              <div className="p-5 space-y-5">
                {error && (
                  <div className="rounded-xl border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700 flex items-start gap-2">
                    <AlertTriangle size={15} className="mt-0.5" />
                    <span>{error}</span>
                  </div>
                )}
                {info && (
                  <div className="rounded-xl border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-700 flex items-start gap-2">
                    <CheckCircle2 size={15} className="mt-0.5" />
                    <span>{info}</span>
                  </div>
                )}

                <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                  <label className="space-y-1.5">
                    <span className="text-xs font-semibold text-slate-700">Workflow name</span>
                    <input
                      value={draft.name}
                      onChange={e => updateDraft('name', e.target.value)}
                      className="w-full rounded-xl border border-slate-200 px-3 py-2 text-sm focus:ring-2 focus:ring-sky-400 focus:outline-none"
                      placeholder="Monthly churn diagnosis orchestrator"
                    />
                  </label>
                  <label className="space-y-1.5">
                    <span className="text-xs font-semibold text-slate-700">Default run input</span>
                    <input
                      value={draft.default_input}
                      onChange={e => updateDraft('default_input', e.target.value)}
                      className="w-full rounded-xl border border-slate-200 px-3 py-2 text-sm focus:ring-2 focus:ring-sky-400 focus:outline-none"
                      placeholder="Business question template used by scheduler"
                    />
                  </label>
                </div>

                <label className="space-y-1.5 block">
                  <span className="text-xs font-semibold text-slate-700">Business objective</span>
                  <textarea
                    value={draft.objective}
                    onChange={e => updateDraft('objective', e.target.value)}
                    rows={3}
                    className="w-full rounded-xl border border-slate-200 px-3 py-2 text-sm focus:ring-2 focus:ring-sky-400 focus:outline-none"
                    placeholder="What should this manager workflow solve end-to-end?"
                  />
                </label>

                <label className="space-y-1.5 block">
                  <span className="text-xs font-semibold text-slate-700">Description</span>
                  <textarea
                    value={draft.description}
                    onChange={e => updateDraft('description', e.target.value)}
                    rows={2}
                    className="w-full rounded-xl border border-slate-200 px-3 py-2 text-sm focus:ring-2 focus:ring-sky-400 focus:outline-none"
                    placeholder="Operational context, guardrails, expected deliverable."
                  />
                </label>

                <div className="rounded-xl border border-slate-200 bg-slate-50/70 p-4">
                  <div className="flex items-center justify-between gap-3 mb-3">
                    <div>
                      <p className="text-xs font-semibold text-slate-800 uppercase tracking-wider">Task scheduling</p>
                      <p className="text-xs text-slate-500 mt-0.5">
                        Auto-launch this workflow via backend scheduler.
                      </p>
                    </div>
                    <label className="inline-flex items-center gap-2 text-sm font-medium text-slate-700">
                      <input
                        type="checkbox"
                        checked={draft.enabled}
                        onChange={e => updateDraft('enabled', e.target.checked)}
                        className="rounded border-slate-300 text-emerald-600 focus:ring-emerald-500"
                      />
                      Enabled
                    </label>
                  </div>

                  <div className="grid grid-cols-1 md:grid-cols-4 gap-3">
                    <label className="space-y-1.5">
                      <span className="text-xs font-semibold text-slate-600">Mode</span>
                      <select
                        value={draft.schedule.mode}
                        onChange={e => updateSchedule('mode', e.target.value as ManagerSchedule['mode'])}
                        className="w-full rounded-lg border border-slate-200 px-2.5 py-2 text-sm bg-white"
                      >
                        <option value="disabled">Disabled</option>
                        <option value="interval">Every X minutes</option>
                        <option value="daily">Daily at time</option>
                      </select>
                    </label>

                    {draft.schedule.mode === 'interval' && (
                      <label className="space-y-1.5">
                        <span className="text-xs font-semibold text-slate-600">Interval (min)</span>
                        <input
                          type="number"
                          min={5}
                          max={10080}
                          value={draft.schedule.interval_minutes}
                          onChange={e => updateSchedule('interval_minutes', Number(e.target.value || 60))}
                          className="w-full rounded-lg border border-slate-200 px-2.5 py-2 text-sm bg-white"
                        />
                      </label>
                    )}

                    {draft.schedule.mode === 'daily' && (
                      <label className="space-y-1.5">
                        <span className="text-xs font-semibold text-slate-600">Daily time</span>
                        <input
                          type="time"
                          value={draft.schedule.daily_time}
                          onChange={e => updateSchedule('daily_time', e.target.value)}
                          className="w-full rounded-lg border border-slate-200 px-2.5 py-2 text-sm bg-white"
                        />
                      </label>
                    )}

                    <label className="space-y-1.5 md:col-span-2">
                      <span className="text-xs font-semibold text-slate-600">Timezone</span>
                      <input
                        value={draft.schedule.timezone}
                        onChange={e => updateSchedule('timezone', e.target.value)}
                        className="w-full rounded-lg border border-slate-200 px-2.5 py-2 text-sm bg-white"
                        placeholder="Europe/Paris"
                      />
                    </label>
                  </div>
                </div>

                <div className="rounded-2xl border border-slate-200 overflow-hidden">
                  <div className="px-4 py-3 bg-slate-900 text-white flex items-center justify-between">
                    <div className="flex items-center gap-2">
                      <Layers size={15} />
                      <span className="font-semibold text-sm">Execution graph</span>
                    </div>
                    <button
                      onClick={addStep}
                      className="inline-flex items-center gap-1 rounded-lg bg-emerald-500 px-2.5 py-1.5 text-xs font-semibold hover:bg-emerald-600"
                    >
                      <Plus size={13} />
                      Add step
                    </button>
                  </div>
                  <div className="p-4 bg-white space-y-3">
                    {draft.steps.map((step, idx) => {
                      const meta = agentById[step.agent_id];
                      return (
                        <div
                          key={step.id}
                          className="rounded-xl border border-slate-200 bg-slate-50 p-3"
                        >
                          <div className="flex flex-wrap items-center justify-between gap-2 mb-3">
                            <div className="flex items-center gap-2">
                              <span className="inline-flex h-6 w-6 items-center justify-center rounded-full bg-slate-800 text-white text-[11px] font-semibold">
                                {idx + 1}
                              </span>
                              <p className="text-sm font-semibold text-slate-800">{step.title || `Step ${idx + 1}`}</p>
                            </div>
                            <div className="flex items-center gap-1">
                              <button
                                onClick={() => moveStep(step.id, -1)}
                                className="rounded-md border border-slate-200 bg-white p-1.5 text-slate-600 hover:bg-slate-100"
                                title="Move up"
                              >
                                <ArrowUp size={13} />
                              </button>
                              <button
                                onClick={() => moveStep(step.id, 1)}
                                className="rounded-md border border-slate-200 bg-white p-1.5 text-slate-600 hover:bg-slate-100"
                                title="Move down"
                              >
                                <ArrowDown size={13} />
                              </button>
                              <button
                                onClick={() => removeStep(step.id)}
                                className="rounded-md border border-rose-200 bg-rose-50 p-1.5 text-rose-600 hover:bg-rose-100"
                                title="Delete step"
                              >
                                <Trash2 size={13} />
                              </button>
                            </div>
                          </div>

                          <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
                            <label className="space-y-1.5">
                              <span className="text-[11px] font-semibold text-slate-600">Agent</span>
                              <select
                                value={step.agent_id}
                                onChange={e => {
                                  const nextAgent = e.target.value;
                                  const defaults = stepParamDefaults[nextAgent] || {};
                                  updateStep(step.id, {
                                    agent_id: nextAgent,
                                    params_text: JSON.stringify(defaults, null, 2),
                                  });
                                }}
                                className="w-full rounded-lg border border-slate-200 bg-white px-2.5 py-2 text-sm"
                              >
                                {agents.map(agent => (
                                  <option key={agent.id} value={agent.id}>
                                    {agent.name}
                                  </option>
                                ))}
                              </select>
                            </label>
                            <label className="space-y-1.5">
                              <span className="text-[11px] font-semibold text-slate-600">Step title</span>
                              <input
                                value={step.title}
                                onChange={e => updateStep(step.id, { title: e.target.value })}
                                className="w-full rounded-lg border border-slate-200 bg-white px-2.5 py-2 text-sm"
                              />
                            </label>
                          </div>

                          <label className="space-y-1.5 block mt-3">
                            <span className="text-[11px] font-semibold text-slate-600">Instruction prompt</span>
                            <textarea
                              value={step.prompt}
                              onChange={e => updateStep(step.id, { prompt: e.target.value })}
                              rows={3}
                              className="w-full rounded-lg border border-slate-200 bg-white px-2.5 py-2 text-sm"
                              placeholder="Tell this agent exactly what this step should accomplish."
                            />
                          </label>

                          <label className="space-y-1.5 block mt-3">
                            <span className="text-[11px] font-semibold text-slate-600">
                              Agent params (JSON)
                            </span>
                            <textarea
                              value={step.params_text}
                              onChange={e => updateStep(step.id, { params_text: e.target.value })}
                              rows={4}
                              className="w-full rounded-lg border border-slate-200 bg-white px-2.5 py-2 text-xs font-mono"
                            />
                          </label>

                          <div className="mt-3 grid grid-cols-1 md:grid-cols-4 gap-3">
                            <label className="space-y-1.5">
                              <span className="text-[11px] font-semibold text-slate-600">Timeout (s)</span>
                              <input
                                type="number"
                                min={30}
                                max={3600}
                                value={step.timeout_seconds}
                                onChange={e => updateStep(step.id, { timeout_seconds: Number(e.target.value || 420) })}
                                className="w-full rounded-lg border border-slate-200 bg-white px-2.5 py-2 text-sm"
                              />
                            </label>
                            <label className="space-y-1.5">
                              <span className="text-[11px] font-semibold text-slate-600">Max auto followups</span>
                              <input
                                type="number"
                                min={0}
                                max={5}
                                value={step.max_followups}
                                onChange={e => updateStep(step.id, { max_followups: Number(e.target.value || 2) })}
                                className="w-full rounded-lg border border-slate-200 bg-white px-2.5 py-2 text-sm"
                              />
                            </label>
                            <label className="space-y-1.5 md:col-span-2">
                              <span className="text-[11px] font-semibold text-slate-600">Auto reply value</span>
                              <input
                                value={step.auto_reply_text}
                                onChange={e => updateStep(step.id, { auto_reply_text: e.target.value })}
                                className="w-full rounded-lg border border-slate-200 bg-white px-2.5 py-2 text-sm"
                              />
                            </label>
                          </div>

                          <div className="mt-3 flex flex-wrap items-center gap-4 text-xs">
                            <label className="inline-flex items-center gap-2 text-slate-700">
                              <input
                                type="checkbox"
                                checked={step.halt_on_error}
                                onChange={e => updateStep(step.id, { halt_on_error: e.target.checked })}
                                className="rounded border-slate-300 text-rose-600"
                              />
                              Halt on error
                            </label>
                            <label className="inline-flex items-center gap-2 text-slate-700">
                              <input
                                type="checkbox"
                                checked={step.auto_approve_questions}
                                onChange={e => updateStep(step.id, { auto_approve_questions: e.target.checked })}
                                className="rounded border-slate-300 text-sky-600"
                              />
                              Auto-answer interactive prompts
                            </label>
                            {meta && (
                              <span className="inline-flex items-center gap-1 text-slate-500">
                                <Bot size={12} />
                                {meta.runtime ? 'Runtime agent' : 'Synchronous agent'}
                              </span>
                            )}
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </div>

                <div className="rounded-xl border border-emerald-200 bg-emerald-50/80 p-4">
                  <div className="flex items-center justify-between gap-3 mb-2">
                    <div className="flex items-center gap-2">
                      <Zap size={14} className="text-emerald-600" />
                      <p className="text-sm font-semibold text-emerald-800">Run workflow now</p>
                    </div>
                    <button
                      onClick={runWorkflowNow}
                      disabled={!draft.id || running}
                      className="inline-flex items-center gap-1.5 rounded-lg bg-emerald-600 px-3 py-2 text-xs font-semibold text-white hover:bg-emerald-700 disabled:opacity-50"
                    >
                      <Play size={14} />
                      {running ? 'Launching...' : 'Run now'}
                    </button>
                  </div>
                  <textarea
                    value={runInput}
                    onChange={e => setRunInput(e.target.value)}
                    rows={2}
                    className="w-full rounded-lg border border-emerald-200 bg-white px-2.5 py-2 text-sm"
                    placeholder="Optional run-specific goal override."
                  />
                </div>
              </div>
            </div>
          </div>
        </section>

        <aside className="w-[430px] border-l border-slate-200 bg-white/90 backdrop-blur-sm flex flex-col overflow-hidden">
          <div className="p-4 border-b border-slate-200 bg-slate-900 text-white">
            <p className="text-[11px] uppercase tracking-widest text-slate-300">Runtime Monitor</p>
            <h3 className="text-lg font-semibold">Workflow Runs</h3>
          </div>

          <div className="p-3 border-b border-slate-100 max-h-56 overflow-y-auto space-y-2">
            {runs.length === 0 && (
              <div className="text-xs text-slate-500 border border-dashed border-slate-300 rounded-xl p-3">
                No run history for this workflow yet.
              </div>
            )}
            {runs.map(run => (
              <button
                key={run.id}
                onClick={() => {
                  setSelectedRunId(run.id);
                  loadRunDetail(run.id);
                }}
                className={clsx(
                  'w-full rounded-xl border px-3 py-2 text-left transition-colors',
                  selectedRunId === run.id
                    ? 'border-sky-300 bg-sky-50'
                    : 'border-slate-200 bg-white hover:bg-slate-50',
                )}
              >
                <div className="flex items-center justify-between gap-2">
                  <span className="text-xs font-semibold text-slate-800">#{run.id.slice(0, 8)}</span>
                  <span className={clsx('text-[10px] border rounded-full px-2 py-0.5', runStatusClass(run.status || ''))}>
                    {run.status}
                  </span>
                </div>
                <p className="text-[11px] text-slate-500 mt-1">{formatDateTime(run.created_at)}</p>
                <p className="text-xs text-slate-600 mt-1 line-clamp-2">{run.summary || 'No summary yet.'}</p>
              </button>
            ))}
          </div>

          <div className="flex-1 overflow-y-auto p-4 space-y-3">
            {!runDetail && (
              <div className="h-full flex items-center justify-center text-slate-500 text-sm">
                Select a run to inspect logs and outputs.
              </div>
            )}

            {runDetail && (
              <>
                <div className="rounded-xl border border-slate-200 p-3 bg-slate-50">
                  <div className="flex items-center justify-between gap-2">
                    <div className="text-sm font-semibold text-slate-800 flex items-center gap-2">
                      <CalendarClock size={14} />
                      Run #{runDetail.id.slice(0, 8)}
                    </div>
                    <span className={clsx('text-[10px] border rounded-full px-2 py-0.5', runStatusClass(runDetail.status || ''))}>
                      {runDetail.status}
                    </span>
                  </div>
                  <p className="text-xs text-slate-500 mt-1">Trigger: {runDetail.trigger || 'manual'}</p>
                  <p className="text-xs text-slate-500">Started: {formatDateTime(runDetail.started_at || runDetail.created_at)}</p>
                  <p className="text-xs text-slate-500">Completed: {formatDateTime(runDetail.completed_at)}</p>
                  {runDetail.error && (
                    <p className="mt-2 text-xs text-rose-600">{runDetail.error}</p>
                  )}
                  {activeRun && (
                    <button
                      onClick={stopRun}
                      disabled={busyStop}
                      className="mt-3 inline-flex items-center gap-1.5 rounded-lg border border-rose-200 bg-rose-50 px-2.5 py-1.5 text-xs font-semibold text-rose-700 hover:bg-rose-100 disabled:opacity-50"
                    >
                      <Square size={12} />
                      {busyStop ? 'Stopping...' : 'Stop run'}
                    </button>
                  )}
                </div>

                <div className="rounded-xl border border-slate-200">
                  <div className="px-3 py-2 border-b border-slate-100 bg-slate-50 flex items-center gap-2">
                    <Clock3 size={14} className="text-slate-500" />
                    <p className="text-xs font-semibold text-slate-700 uppercase tracking-wide">Live logs</p>
                  </div>
                  <div className="max-h-56 overflow-y-auto p-3 space-y-1.5 bg-white">
                    {(runDetail.logs || []).length === 0 && (
                      <p className="text-xs text-slate-500">No logs yet.</p>
                    )}
                    {(runDetail.logs || []).map(log => (
                      <div key={log.seq} className="text-xs border-b border-slate-100 pb-1.5">
                        <div className="flex items-center gap-2 text-[10px] text-slate-500">
                          <span>#{log.seq}</span>
                          <span>{formatDateTime(log.ts)}</span>
                          <span className="uppercase">{log.level}</span>
                          <span className="uppercase">{log.kind}</span>
                        </div>
                        <p className="text-slate-700 mt-0.5">{log.message}</p>
                      </div>
                    ))}
                  </div>
                </div>

                <div className="rounded-xl border border-slate-200">
                  <div className="px-3 py-2 border-b border-slate-100 bg-slate-50 flex items-center gap-2">
                    <Wrench size={14} className="text-slate-500" />
                    <p className="text-xs font-semibold text-slate-700 uppercase tracking-wide">Step outputs</p>
                  </div>
                  <div className="max-h-[40vh] overflow-y-auto p-3 space-y-2 bg-white">
                    {(runDetail.step_results || []).length === 0 && (
                      <p className="text-xs text-slate-500">No step result yet.</p>
                    )}
                    {(runDetail.step_results || []).map(step => (
                      <div key={`${step.step_id}-${step.step_index}`} className="rounded-lg border border-slate-200 p-2.5 bg-slate-50">
                        <div className="flex items-center justify-between gap-2">
                          <p className="text-xs font-semibold text-slate-800">
                            {step.step_index}. {step.title}
                          </p>
                          <span className={clsx('text-[10px] border rounded-full px-2 py-0.5', runStatusClass(step.status || ''))}>
                            {step.status}
                          </span>
                        </div>
                        <p className="text-[11px] text-slate-500 mt-0.5">{step.agent_id}</p>
                        {step.output_preview && (
                          <p className="text-xs text-slate-700 mt-1 whitespace-pre-wrap break-words">{step.output_preview}</p>
                        )}
                        {step.error && (
                          <p className="text-xs text-rose-600 mt-1">{step.error}</p>
                        )}
                      </div>
                    ))}
                  </div>
                </div>

                <div className="rounded-xl border border-slate-200 bg-slate-50 p-3 text-xs text-slate-600">
                  <p className="font-semibold text-slate-700 mb-1 flex items-center gap-1.5">
                    <Activity size={13} />
                    Best practices embedded
                  </p>
                  <ul className="space-y-1 list-disc pl-4">
                    <li>Sequential execution with per-step timeout and error policy.</li>
                    <li>Reusable existing agents with parameterized calls.</li>
                    <li>Automatic polling for runtime agents and stop control.</li>
                    <li>Built-in scheduler for interval/daily autonomous runs.</li>
                  </ul>
                </div>
              </>
            )}
          </div>
        </aside>
      </main>
    </div>
  );
}

