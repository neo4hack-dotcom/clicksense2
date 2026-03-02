import { useEffect, useState } from 'react';
import { MessageSquare, LayoutDashboard, Settings, BookOpen, Database, UserCircle, Grid } from 'lucide-react';
import { useAppStore } from '../store';
import clsx from 'clsx';

export function Sidebar() {
  const { activeTab, setActiveTab, currentUser, setCurrentUser } = useAppStore();
  const [users, setUsers] = useState<{id: number, name: string}[]>([]);

  useEffect(() => {
    fetch('/api/users')
      .then(res => res.json())
      .then(data => {
        setUsers(data);
        if (data.length > 0 && !currentUser) {
          setCurrentUser(data[0]);
        }
      });
  }, []);

  const navItems = [
    { id: 'chat', icon: MessageSquare, label: 'AI Assistant' },
    { id: 'builder', icon: LayoutDashboard, label: 'Visual Builder' },
    { id: 'dashboard', icon: Grid, label: 'My Dashboard' },
    { id: 'knowledge', icon: BookOpen, label: 'Knowledge Base' },
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
      
      <div className="p-4 border-b border-slate-800">
        <div className="flex items-center gap-2 mb-2 text-xs font-semibold text-slate-500 uppercase tracking-wider">
          <UserCircle size={14} />
          Current User
        </div>
        <select 
          className="w-full bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:border-emerald-500"
          value={currentUser?.id || ''}
          onChange={(e) => {
            const user = users.find(u => u.id === Number(e.target.value));
            if (user) setCurrentUser(user);
          }}
        >
          {users.map(u => (
            <option key={u.id} value={u.id}>{u.name}</option>
          ))}
        </select>
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
      
      <div className="p-4 text-xs text-slate-500 border-t border-slate-800">
        ClickSense AI v1.0
      </div>
    </div>
  );
}
