import { useEffect } from 'react';
import { Sidebar } from './components/Sidebar';
import { ChatPane } from './components/ChatPane';
import { BuilderPane } from './components/BuilderPane';
import { SettingsPane } from './components/SettingsPane';
import { KnowledgeBasePane } from './components/KnowledgeBasePane';
import { DashboardPane } from './components/DashboardPane';
import { useAppStore } from './store';

export default function App() {
  const { activeTab, setSchema, setTableMetadata } = useAppStore();

  useEffect(() => {
    // Fetch schema on load
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
  }, [setSchema, setTableMetadata]);

  return (
    <div className="flex h-screen w-full bg-slate-100 font-sans overflow-hidden">
      <Sidebar />
      <div className="flex-1 flex overflow-hidden">
        {activeTab === 'chat' && (
          <>
            <div className="w-1/3 min-w-[350px] max-w-[500px] h-full shadow-xl z-10">
              <ChatPane />
            </div>
            <div className="flex-1 h-full bg-white z-0">
              <BuilderPane />
            </div>
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
      </div>
    </div>
  );
}
