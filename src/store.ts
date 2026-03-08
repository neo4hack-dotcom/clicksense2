import { create } from 'zustand';

export interface KnowledgeFolder {
  id: number;
  title: string;
  content: string;
  created_at: string;
  updated_at: string;
}

export interface TableMapping {
  table_name: string;
  mapping_name: string;
}

export interface RagConfig {
  esHost: string;
  esIndex: string;
  esUsername: string;
  esPassword: string;
  embeddingModel: string;
  topK: number;
  chunkSize: number;
}

export interface RagMessage {
  role: 'user' | 'assistant';
  content: string;
  sources?: { title: string; score: number; excerpt: string }[];
}

interface AppState {
  schema: Record<string, { name: string; type: string }[]>;
  setSchema: (schema: Record<string, { name: string; type: string }[]>) => void;

  tableMetadata: Record<string, { description: string; is_favorite: boolean }>;
  setTableMetadata: (meta: Record<string, { description: string; is_favorite: boolean }>) => void;

  selectedTable: string | null;
  setSelectedTable: (table: string | null) => void;

  queryResult: any[];
  setQueryResult: (data: any[]) => void;

  queryConfig: {
    dimensions: string[];
    measures: { column: string; agg: string }[];
    filters: { column: string; operator: string; value: string }[];
  };
  setQueryConfig: (config: any) => void;

  activeTab: 'chat' | 'builder' | 'dashboard' | 'settings' | 'knowledge' | 'rag' | 'data-quality';
  setActiveTab: (tab: 'chat' | 'builder' | 'dashboard' | 'settings' | 'knowledge' | 'rag' | 'data-quality') => void;

  chatHistory: {
    role: 'user' | 'assistant';
    content: string;
    sql?: string;
    visual?: string;
    needs_clarification?: boolean;
    question?: string;
    options?: string[];
    clarification_type?: 'field_selection' | 'table_selection';
    is_agent?: boolean;
    agent_steps?: {
      step: number;
      type?: 'query' | 'search_knowledge' | 'export_csv';
      reasoning: string;
      sql?: string;
      search_query?: string;
      suggested_path?: string;
      result_summary: string;
      row_count: number;
      ok: boolean;
    }[];
  }[];
  addChatMessage: (msg: {
    role: 'user' | 'assistant';
    content: string;
    sql?: string;
    visual?: string;
    needs_clarification?: boolean;
    question?: string;
    options?: string[];
    clarification_type?: 'field_selection' | 'table_selection';
    is_agent?: boolean;
    agent_steps?: {
      step: number;
      type?: 'query' | 'search_knowledge' | 'export_csv';
      reasoning: string;
      sql?: string;
      search_query?: string;
      suggested_path?: string;
      result_summary: string;
      row_count: number;
      ok: boolean;
    }[];
  }) => void;
  clearChatHistory: () => void;

  chatPaneSize: 'normal' | 'expanded' | 'minimized';
  setChatPaneSize: (size: 'normal' | 'expanded' | 'minimized') => void;

  savedQueries: any[];
  setSavedQueries: (queries: any[]) => void;

  queryHistory: any[];
  setQueryHistory: (history: any[]) => void;

  knowledgeFolders: KnowledgeFolder[];
  setKnowledgeFolders: (folders: KnowledgeFolder[]) => void;

  tableMappings: TableMapping[];
  setTableMappings: (mappings: TableMapping[]) => void;

  selectedTableMappings: string[];
  setSelectedTableMappings: (tables: string[]) => void;

  ragConfig: RagConfig;
  setRagConfig: (config: RagConfig) => void;

  ragHistory: RagMessage[];
  addRagMessage: (msg: RagMessage) => void;
  clearRagHistory: () => void;

  agentMaxSteps: number;
  setAgentMaxSteps: (steps: number) => void;
}

export const useAppStore = create<AppState>((set) => ({
  schema: {},
  setSchema: (schema) => set({ schema }),

  tableMetadata: {},
  setTableMetadata: (tableMetadata) => set({ tableMetadata }),

  selectedTable: null,
  setSelectedTable: (selectedTable) => set({ selectedTable }),

  queryResult: [],
  setQueryResult: (queryResult) => set({ queryResult }),

  queryConfig: { dimensions: [], measures: [], filters: [] },
  setQueryConfig: (queryConfig) => set({ queryConfig }),

  activeTab: 'chat',
  setActiveTab: (activeTab) => set({ activeTab }),

  chatHistory: [],
  addChatMessage: (msg) => set((state) => ({ chatHistory: [...state.chatHistory, msg] })),
  clearChatHistory: () => set({ chatHistory: [] }),

  chatPaneSize: 'normal',
  setChatPaneSize: (chatPaneSize) => set({ chatPaneSize }),

  savedQueries: [],
  setSavedQueries: (savedQueries) => set({ savedQueries }),

  queryHistory: [],
  setQueryHistory: (queryHistory) => set({ queryHistory }),

  knowledgeFolders: [],
  setKnowledgeFolders: (knowledgeFolders) => set({ knowledgeFolders }),

  tableMappings: [],
  setTableMappings: (tableMappings) => set({ tableMappings }),

  selectedTableMappings: [],
  setSelectedTableMappings: (selectedTableMappings) => set({ selectedTableMappings }),

  ragConfig: {
    esHost: 'http://localhost:9200',
    esIndex: 'clicksense_rag',
    esUsername: '',
    esPassword: '',
    embeddingModel: '',
    topK: 5,
    chunkSize: 500,
  },
  setRagConfig: (ragConfig) => set({ ragConfig }),

  ragHistory: [],
  addRagMessage: (msg) => set((state) => ({ ragHistory: [...state.ragHistory, msg] })),
  clearRagHistory: () => set({ ragHistory: [] }),

  agentMaxSteps: 10,
  setAgentMaxSteps: (agentMaxSteps) => set({ agentMaxSteps }),
}));
