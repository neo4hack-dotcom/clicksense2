import { useState, useEffect } from 'react';
import { BookOpen, Save, CheckCircle2, Tags, Search, X } from 'lucide-react';
import { useAppStore } from '../store';
import clsx from 'clsx';

type Tab = 'context' | 'mapping';

function ContextTab() {
  const [knowledge, setKnowledge] = useState('');
  const [isSaving, setIsSaving] = useState(false);
  const [saved, setSaved] = useState(false);

  useEffect(() => {
    fetch('/api/config')
      .then(res => res.json())
      .then(data => setKnowledge(data.knowledgeBase || ''));
  }, []);

  const handleSave = async () => {
    setIsSaving(true);
    try {
      await fetch('/api/config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ knowledge }),
      });
      setSaved(true);
      setTimeout(() => setSaved(false), 3000);
    } catch {
      alert('Failed to save knowledge base');
    } finally {
      setIsSaving(false);
    }
  };

  return (
    <>
      <div className="flex-1 bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden flex flex-col">
        <div className="p-4 border-b border-slate-100 flex items-center gap-3 bg-slate-50">
          <div className="bg-amber-100 p-2 rounded-lg text-amber-600">
            <BookOpen size={20} />
          </div>
          <h3 className="text-sm font-semibold text-slate-800">Context Document</h3>
        </div>
        <textarea
          value={knowledge}
          onChange={(e) => setKnowledge(e.target.value)}
          placeholder="Example:
- 'Revenue' is calculated as sum(price * quantity)
- 'Active Users' means users who logged in within the last 30 days
- The 'users' table contains customer information.
- 'Churned' means status = 'inactive' AND last_login < now() - interval 30 day"
          className="flex-1 w-full p-6 text-sm text-slate-700 bg-transparent border-none focus:ring-0 resize-none font-mono leading-relaxed"
        />
      </div>
      <div className="flex justify-end pt-6 shrink-0">
        <button
          onClick={handleSave}
          disabled={isSaving}
          className="flex items-center gap-2 bg-slate-900 hover:bg-slate-800 text-white px-6 py-3 rounded-xl font-medium transition-all shadow-sm disabled:opacity-50"
        >
          {saved ? <CheckCircle2 size={18} className="text-emerald-400" /> : <Save size={18} />}
          {isSaving ? 'Saving...' : saved ? 'Saved Successfully' : 'Save Knowledge Base'}
        </button>
      </div>
    </>
  );
}

function MappingTab() {
  const { schema, tableMappings, setTableMappings } = useAppStore();
  const [localMappings, setLocalMappings] = useState<Record<string, string>>({});
  const [savingTable, setSavingTable] = useState<string | null>(null);
  const [savedTables, setSavedTables] = useState<Set<string>>(new Set());
  const [search, setSearch] = useState('');

  useEffect(() => {
    fetch('/api/table_mappings')
      .then(res => res.json())
      .then(data => {
        setLocalMappings(data);
        setTableMappings(data);
      });
  }, []);

  const tables = Object.keys(schema).sort();
  const filtered = tables.filter(t =>
    t.toLowerCase().includes(search.toLowerCase()) ||
    (localMappings[t] || '').toLowerCase().includes(search.toLowerCase())
  );

  const handleSave = async (tableName: string) => {
    setSavingTable(tableName);
    try {
      await fetch('/api/table_mappings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ table_name: tableName, mapping_name: localMappings[tableName] || '' }),
      });
      const updated = { ...localMappings };
      if (!updated[tableName]) delete updated[tableName];
      setTableMappings(updated);
      setSavedTables(prev => new Set(prev).add(tableName));
      setTimeout(() => setSavedTables(prev => { const s = new Set(prev); s.delete(tableName); return s; }), 2000);
    } catch {
      alert('Failed to save mapping');
    } finally {
      setSavingTable(null);
    }
  };

  const handleChange = (tableName: string, value: string) => {
    setLocalMappings(prev => ({ ...prev, [tableName]: value }));
  };

  const handleClear = async (tableName: string) => {
    const updated = { ...localMappings, [tableName]: '' };
    setLocalMappings(updated);
    await handleSave(tableName);
  };

  if (tables.length === 0) {
    return (
      <div className="flex-1 flex items-center justify-center text-slate-400 text-sm">
        No tables found. Make sure ClickHouse is connected and the schema is loaded.
      </div>
    );
  }

  const mappedCount = Object.values(localMappings).filter(v => v.trim()).length;

  return (
    <div className="flex-1 flex flex-col min-h-0">
      <div className="mb-4 flex items-center gap-3">
        <div className="relative flex-1">
          <Search size={16} className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" />
          <input
            type="text"
            value={search}
            onChange={e => setSearch(e.target.value)}
            placeholder="Search tables or business names..."
            className="w-full pl-9 pr-4 py-2 text-sm border border-slate-200 rounded-xl focus:outline-none focus:ring-2 focus:ring-emerald-500/20 focus:border-emerald-500"
          />
        </div>
        <span className="text-xs text-slate-500 whitespace-nowrap">
          {mappedCount} / {tables.length} mapped
        </span>
      </div>

      <div className="flex-1 overflow-y-auto bg-white rounded-2xl border border-slate-200 shadow-sm divide-y divide-slate-100">
        {filtered.map(tableName => {
          const mappingValue = localMappings[tableName] || '';
          const isMapped = Boolean(tableMappings[tableName]);
          const isSaving = savingTable === tableName;
          const isSaved = savedTables.has(tableName);

          return (
            <div key={tableName} className="flex items-center gap-3 px-4 py-3 hover:bg-slate-50 transition-colors">
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2">
                  <span className="text-xs font-mono text-slate-500 truncate">{tableName}</span>
                  {isMapped && (
                    <span className="text-[10px] bg-emerald-100 text-emerald-700 px-1.5 py-0.5 rounded-full font-medium shrink-0">
                      mapped
                    </span>
                  )}
                </div>
                <input
                  type="text"
                  value={mappingValue}
                  onChange={e => handleChange(tableName, e.target.value)}
                  onBlur={() => handleSave(tableName)}
                  onKeyDown={e => e.key === 'Enter' && handleSave(tableName)}
                  placeholder="Business name (e.g. Ventes des produits Maison)"
                  className="mt-1 w-full text-sm text-slate-800 bg-slate-50 border border-slate-200 rounded-lg px-3 py-1.5 focus:outline-none focus:ring-2 focus:ring-emerald-500/20 focus:border-emerald-500 placeholder:text-slate-300 transition-all"
                />
              </div>
              <div className="flex items-center gap-1 shrink-0">
                {mappingValue && (
                  <button
                    onClick={() => handleClear(tableName)}
                    className="p-1.5 text-slate-400 hover:text-red-500 hover:bg-red-50 rounded-lg transition-colors"
                    title="Clear mapping"
                  >
                    <X size={14} />
                  </button>
                )}
                <button
                  onClick={() => handleSave(tableName)}
                  disabled={isSaving}
                  className="p-1.5 text-slate-400 hover:text-emerald-600 hover:bg-emerald-50 rounded-lg transition-colors disabled:opacity-50"
                  title="Save mapping"
                >
                  {isSaved ? (
                    <CheckCircle2 size={14} className="text-emerald-500" />
                  ) : (
                    <Save size={14} />
                  )}
                </button>
              </div>
            </div>
          );
        })}
        {filtered.length === 0 && (
          <div className="py-8 text-center text-sm text-slate-400">No tables match your search.</div>
        )}
      </div>
    </div>
  );
}

export function KnowledgeBasePane() {
  const [activeTab, setActiveTab] = useState<Tab>('context');

  return (
    <div className="p-8 max-w-4xl mx-auto h-full flex flex-col">
      <div className="mb-6">
        <h2 className="text-2xl font-bold text-slate-900 tracking-tight">Functional Knowledge Base</h2>
        <p className="text-slate-500 mt-1 max-w-2xl">
          Inject business context and map ClickHouse tables to human-friendly names to help the AI generate better queries.
        </p>
      </div>

      {/* Sub-tabs */}
      <div className="flex gap-1 p-1 bg-slate-100 rounded-xl mb-6 shrink-0 w-fit">
        <button
          onClick={() => setActiveTab('context')}
          className={clsx(
            'flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-all',
            activeTab === 'context'
              ? 'bg-white text-slate-900 shadow-sm'
              : 'text-slate-500 hover:text-slate-700'
          )}
        >
          <BookOpen size={16} />
          Context Document
        </button>
        <button
          onClick={() => setActiveTab('mapping')}
          className={clsx(
            'flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-all',
            activeTab === 'mapping'
              ? 'bg-white text-slate-900 shadow-sm'
              : 'text-slate-500 hover:text-slate-700'
          )}
        >
          <Tags size={16} />
          Table Mapping
        </button>
      </div>

      {activeTab === 'context' ? <ContextTab /> : <MappingTab />}
    </div>
  );
}
