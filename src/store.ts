import { create } from 'zustand';

interface AppState {
  currentUser: { id: number; name: string } | null;
  setCurrentUser: (user: { id: number; name: string } | null) => void;

  schema: Record<string, { name: string; type: string }[]>;
  setSchema: (schema: Record<string, { name: string; type: string }[]>) => void;

  tableMetadata: Record<string, { description: string; is_favorite: boolean }>;
  setTableMetadata: (meta: Record<string, { description: string; is_favorite: boolean }>) => void;

  // tableMappings: technical table name -> friendly business name
  tableMappings: Record<string, string>;
  setTableMappings: (mappings: Record<string, string>) => void;

  // selectedMappings: list of technical table names selected in the chat filter
  selectedMappings: string[];
  setSelectedMappings: (mappings: string[]) => void;

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
  
  activeTab: 'chat' | 'builder' | 'dashboard' | 'settings' | 'knowledge';
  setActiveTab: (tab: 'chat' | 'builder' | 'dashboard' | 'settings' | 'knowledge') => void;
  
  chatHistory: { role: 'user' | 'assistant'; content: string; sql?: string; visual?: string }[];
  addChatMessage: (msg: { role: 'user' | 'assistant'; content: string; sql?: string; visual?: string }) => void;
  clearChatHistory: () => void;

  savedQueries: any[];
  setSavedQueries: (queries: any[]) => void;

  queryHistory: any[];
  setQueryHistory: (history: any[]) => void;
}

export const useAppStore = create<AppState>((set) => ({
  currentUser: null,
  setCurrentUser: (currentUser) => set({ currentUser }),

  schema: {},
  setSchema: (schema) => set({ schema }),

  tableMetadata: {},
  setTableMetadata: (tableMetadata) => set({ tableMetadata }),

  tableMappings: {},
  setTableMappings: (tableMappings) => set({ tableMappings }),

  selectedMappings: [],
  setSelectedMappings: (selectedMappings) => set({ selectedMappings }),

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

  savedQueries: [],
  setSavedQueries: (savedQueries) => set({ savedQueries }),

  queryHistory: [],
  setQueryHistory: (queryHistory) => set({ queryHistory }),
}));
