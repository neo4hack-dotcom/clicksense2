export type KnowledgeMode = 'kb_context_once' | 'kb_agentic' | 'schema_only' | 'minimal';

export interface KnowledgeModeOption {
  value: KnowledgeMode;
  label: string;
  description: string;
}

export const KNOWLEDGE_MODE_OPTIONS: KnowledgeModeOption[] = [
  {
    value: 'kb_context_once',
    label: 'KB + contexte initial (recommande)',
    description:
      'Recherche KB active et injection statique schema/metadata/KB une seule fois au debut du run (bon equilibre qualite/tokens).',
  },
  {
    value: 'kb_agentic',
    label: 'KB agentique (sans injection statique)',
    description:
      'Aucune injection statique dans les prompts; l agent peut faire des recherches KB ciblees quand necessaire.',
  },
  {
    value: 'schema_only',
    label: 'Schema uniquement (sans KB)',
    description:
      'Injection statique schema/metadata une seule fois, mais recherche KB desactivee.',
  },
  {
    value: 'minimal',
    label: 'Minimal (sans KB ni injection)',
    description:
      'Ni KB ni injection statique: mode ultra leger base sur la question et l historique compact seulement.',
  },
];

const KNOWLEDGE_MODE_SET = new Set(KNOWLEDGE_MODE_OPTIONS.map((o) => o.value));

export function normalizeKnowledgeMode(raw: unknown): KnowledgeMode {
  const value = String(raw || '').trim().toLowerCase();
  if (KNOWLEDGE_MODE_SET.has(value as KnowledgeMode)) {
    return value as KnowledgeMode;
  }
  if (value === 'context_once' || value === 'default' || value === 'standard') {
    return 'kb_context_once';
  }
  if (value === 'agentic' || value === 'knowledge_agent') {
    return 'kb_agentic';
  }
  if (value === 'schema_only_no_kb' || value === 'no_kb_schema') {
    return 'schema_only';
  }
  if (value === 'minimal_no_context' || value === 'no_kb_no_context') {
    return 'minimal';
  }
  return 'kb_context_once';
}

export function knowledgeModeToFlags(raw: unknown): {
  knowledge_mode: KnowledgeMode;
  use_knowledge_base: boolean;
  use_knowledge_agent: boolean;
} {
  const knowledge_mode = normalizeKnowledgeMode(raw);
  if (knowledge_mode === 'kb_agentic') {
    return { knowledge_mode, use_knowledge_base: true, use_knowledge_agent: true };
  }
  if (knowledge_mode === 'schema_only') {
    return { knowledge_mode, use_knowledge_base: false, use_knowledge_agent: false };
  }
  if (knowledge_mode === 'minimal') {
    return { knowledge_mode, use_knowledge_base: false, use_knowledge_agent: true };
  }
  return { knowledge_mode, use_knowledge_base: true, use_knowledge_agent: false };
}

export function flagsToKnowledgeMode(useKnowledgeBase: boolean, useKnowledgeAgent: boolean): KnowledgeMode {
  if (useKnowledgeBase && useKnowledgeAgent) return 'kb_agentic';
  if (useKnowledgeBase && !useKnowledgeAgent) return 'kb_context_once';
  if (!useKnowledgeBase && !useKnowledgeAgent) return 'schema_only';
  return 'minimal';
}

export function knowledgeModeLabel(raw: unknown): string {
  const mode = normalizeKnowledgeMode(raw);
  return KNOWLEDGE_MODE_OPTIONS.find((o) => o.value === mode)?.label || KNOWLEDGE_MODE_OPTIONS[0].label;
}

export function knowledgeModeDescription(raw: unknown): string {
  const mode = normalizeKnowledgeMode(raw);
  return KNOWLEDGE_MODE_OPTIONS.find((o) => o.value === mode)?.description || KNOWLEDGE_MODE_OPTIONS[0].description;
}
