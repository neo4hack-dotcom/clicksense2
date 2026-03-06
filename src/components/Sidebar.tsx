import { MessageSquare, LayoutDashboard, Settings, BookOpen, Database, Grid, Layers, ShieldCheck } from 'lucide-react';
import { useAppStore } from '../store';
import clsx from 'clsx';

export function Sidebar() {
  const { activeTab, setActiveTab } = useAppStore();

  const navItems = [
    { id: 'chat', icon: MessageSquare, label: 'AI Assistant' },
    { id: 'builder', icon: LayoutDashboard, label: 'Visual Builder' },
    { id: 'dashboard', icon: Grid, label: 'My Dashboard' },
    { id: 'knowledge', icon: BookOpen, label: 'Knowledge Base' },
    { id: 'rag', icon: Layers, label: 'RAG' },
    { id: 'data-quality', icon: ShieldCheck, label: 'AI Data Quality' },
    { id: 'settings', icon: Settings, label: 'Settings' },
  ] as const;

  return (
    <div className="w-64 bg-slate-900 text-slate-300 flex flex-col h-screen border-r border-slate-800">
      <div className="p-4 flex items-center gap-3 border-b border-slate-800">
        <div className="bg-emerald-500 p-2 rounded-lg text-white">
          <Database size={20} />
        </div>
        <h1 className="font-bold text-white text-lg tracking-tight">ClickSense AI</h1>
      </div>

      <nav className="flex-1 p-4 space-y-2">
        {navItems.map((item) => (
          <button
            key={item.id}
            onClick={() => setActiveTab(item.id)}
            className={clsx(
              "w-full flex items-center gap-3 px-4 py-3 rounded-xl transition-all duration-200 text-sm font-medium",
              activeTab === item.id
                ? "bg-emerald-500/10 text-emerald-400"
                : "hover:bg-slate-800 hover:text-white"
            )}
          >
            <item.icon size={18} />
            {item.label}
          </button>
        ))}
      </nav>
    </div>
  );
}
