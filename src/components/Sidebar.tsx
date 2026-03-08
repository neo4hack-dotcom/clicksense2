import { MessageSquare, LayoutDashboard, Settings, BookOpen, Database, Grid, Layers, ShieldCheck, Bot, BarChart2, Wrench, Cpu } from 'lucide-react';
import { useAppStore } from '../store';
import clsx from 'clsx';

const navGroups = [
  {
    label: 'Agent assistant',
    icon: Bot,
    items: [
      { id: 'chat', icon: MessageSquare, label: 'AI Assistant' },
      { id: 'agents', icon: Cpu, label: 'Agents' },
      { id: 'data-quality', icon: ShieldCheck, label: 'AI Data Quality' },
      { id: 'rag', icon: Layers, label: 'RAG' },
    ],
  },
  {
    label: 'Big data tools',
    icon: BarChart2,
    items: [
      { id: 'builder', icon: LayoutDashboard, label: 'Visual Builder' },
      { id: 'dashboard', icon: Grid, label: 'My Dashboard' },
    ],
  },
  {
    label: 'Configuration',
    icon: Wrench,
    items: [
      { id: 'knowledge', icon: BookOpen, label: 'Knowledge Base' },
      { id: 'settings', icon: Settings, label: 'Settings' },
    ],
  },
] as const;

export function Sidebar() {
  const { activeTab, setActiveTab } = useAppStore();

  return (
    <div className="w-64 bg-slate-900 text-slate-300 flex flex-col h-screen border-r border-slate-800">
      <div className="p-4 flex items-center gap-3 border-b border-slate-800">
        <div className="bg-emerald-500 p-2 rounded-lg text-white">
          <Database size={20} />
        </div>
        <h1 className="font-bold text-white text-lg tracking-tight">ClickSense AI</h1>
      </div>

      <nav className="flex-1 p-4 space-y-5 overflow-y-auto">
        {navGroups.map((group) => (
          <div key={group.label}>
            <div className="flex items-center gap-2 px-2 mb-1.5">
              <group.icon size={12} className="text-slate-500" />
              <span className="text-[10px] font-semibold uppercase tracking-widest text-slate-500">
                {group.label}
              </span>
            </div>
            <div className="space-y-0.5">
              {group.items.map((item) => (
                <button
                  key={item.id}
                  onClick={() => setActiveTab(item.id)}
                  className={clsx(
                    "w-full flex items-center gap-3 px-4 py-2.5 rounded-xl transition-all duration-200 text-sm font-medium",
                    activeTab === item.id
                      ? "bg-emerald-500/10 text-emerald-400"
                      : "hover:bg-slate-800 hover:text-white"
                  )}
                >
                  <item.icon size={17} />
                  {item.label}
                </button>
              ))}
            </div>
          </div>
        ))}
      </nav>
    </div>
  );
}
