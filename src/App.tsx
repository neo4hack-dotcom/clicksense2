import { useEffect } from 'react';
import { Sidebar } from './components/Sidebar';
import { ChatPane } from './components/ChatPane';
import { BuilderPane } from './components/BuilderPane';
import { SettingsPane } from './components/SettingsPane';
import { KnowledgeBasePane } from './components/KnowledgeBasePane';
import { DashboardPane } from './components/DashboardPane';
import { RagPane } from './components/RagPane';
import { DataQualityPane } from './components/DataQualityPane';
import { AgentsPane } from './components/AgentsPane';
import { useAppStore } from './store';
import { MessageSquare } from 'lucide-react';

export default function App() {
  const { activeTab, setSchema, setTableMetadata, chatPaneSize, setChatPaneSize, setTableMappings } = useAppStore();

  useEffect(() => {
    fetch('/api/schema')
      .then(res => res.json())
      .then(data => {
        if (data.schema) setSchema(data.schema);
      })
      .catch(err => console.error("Failed to fetch schema", err));

    fetch('/api/tables/metadata')
      .then(res => res.json())
      .then(data => setTableMetadata(data))
      .catch(err => console.error("Failed to fetch metadata", err));

    fetch('/api/table-mappings')
      .then(res => res.json())
      .then(data => setTableMappings(data))
      .catch(err => console.error("Failed to fetch table mappings", err));
  }, [setSchema, setTableMetadata, setTableMappings]);

  const chatWidthClass =
    chatPaneSize === 'expanded'
      ? 'w-2/3'
      : 'w-1/3 min-w-[350px] max-w-[500px]';

  return (
    <div className="flex h-screen w-full bg-slate-100 font-sans overflow-hidden">
      <Sidebar />
      <div className="flex-1 flex overflow-hidden relative">
        {activeTab === 'chat' && (
          <>
            {chatPaneSize !== 'minimized' && (
              <div className={`${chatWidthClass} h-full shadow-xl z-10 transition-all duration-300`}>
                <ChatPane />
              </div>
            )}
            <div className="flex-1 h-full bg-white z-0">
              <BuilderPane />
            </div>
            {chatPaneSize === 'minimized' && (
              <button
                onClick={() => setChatPaneSize('normal')}
                className="fixed bottom-6 left-72 z-50 flex items-center gap-2 bg-emerald-500 hover:bg-emerald-600 text-white px-4 py-3 rounded-full shadow-lg transition-all"
              >
                <MessageSquare size={18} />
                <span className="text-sm font-medium">Open AI Assistant</span>
              </button>
            )}
          </>
        )}
        {activeTab === 'builder' && (
          <div className="flex-1 h-full bg-white">
            <BuilderPane />
          </div>
        )}
        {activeTab === 'dashboard' && (
          <div className="flex-1 h-full bg-slate-50 overflow-y-auto">
            <DashboardPane />
          </div>
        )}
        {activeTab === 'settings' && (
          <div className="flex-1 h-full bg-slate-50 overflow-y-auto">
            <SettingsPane />
          </div>
        )}
        {activeTab === 'knowledge' && (
          <div className="flex-1 h-full bg-slate-50 overflow-y-auto">
            <KnowledgeBasePane />
          </div>
        )}
        {activeTab === 'rag' && (
          <div className="flex-1 h-full bg-slate-50 overflow-y-auto">
            <RagPane />
          </div>
        )}
        {activeTab === 'data-quality' && (
          <div className="flex-1 h-full bg-slate-50 overflow-hidden">
            <DataQualityPane />
          </div>
        )}
        {activeTab === 'agents' && (
          <div className="flex-1 h-full overflow-hidden">
            <AgentsPane />
          </div>
        )}
      </div>
    </div>
  );
}
