import { useState, useEffect } from 'react';
import { BookOpen, Plus, Save, Trash2, Edit3, CheckCircle2, FolderOpen, X, ChevronDown, ChevronRight, Table2, Search, GitFork, ArrowRight, RotateCcw } from 'lucide-react';
import { useAppStore, KnowledgeFolder, FkRelation } from '../store';
import { motion, AnimatePresence } from 'motion/react';

export function KnowledgeBasePane() {
  const { knowledgeFolders, setKnowledgeFolders, schema, tableMappings, setTableMappings, fkRelations, setFkRelations } = useAppStore();
  const [activeSubTab, setActiveSubTab] = useState<'folders' | 'table-mapping' | 'fk-relations'>('folders');

  // Folders state
  const [expandedIds, setExpandedIds] = useState<Set<number>>(new Set());
  const [editingFolder, setEditingFolder] = useState<KnowledgeFolder | null>(null);
  const [isCreating, setIsCreating] = useState(false);
  const [newFolder, setNewFolder] = useState({ title: '', content: '' });
  const [savingId, setSavingId] = useState<number | null>(null);
  const [savedId, setSavedId] = useState<number | null>(null);

  // Table mapping state
  const [tableSearch, setTableSearch] = useState('');
  const [editingMappings, setEditingMappings] = useState<Record<string, string>>({});
  const [savingTable, setSavingTable] = useState<string | null>(null);
  const [savedTable, setSavedTable] = useState<string | null>(null);

  // FK relations state
  const [deletingFkId, setDeletingFkId] = useState<number | null>(null);

  useEffect(() => {
    fetch('/api/knowledge/folders')
      .then(res => res.json())
      .then(data => setKnowledgeFolders(data));
    fetch('/api/table-mappings')
      .then(res => res.json())
      .then((data: { table_name: string; mapping_name: string }[]) => setTableMappings(data));
    fetch('/api/fk-relations')
      .then(res => res.json())
      .then((data: FkRelation[]) => setFkRelations(data))
      .catch(() => {});
  }, []);

  const toggleExpand = (id: number) => {
    setExpandedIds(prev => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const handleCreate = async () => {
    if (!newFolder.title.trim()) return;
    try {
      const res = await fetch('/api/knowledge/folders', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(newFolder),
      });
      const data = await res.json();
      setKnowledgeFolders([...knowledgeFolders, data]);
      setNewFolder({ title: '', content: '' });
      setIsCreating(false);
      setExpandedIds(prev => new Set([...prev, data.id]));
    } catch {
      alert('Failed to create folder');
    }
  };

  const handleUpdate = async (folder: KnowledgeFolder) => {
    setSavingId(folder.id);
    try {
      await fetch(`/api/knowledge/folders/${folder.id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ title: folder.title, content: folder.content }),
      });
      setKnowledgeFolders(knowledgeFolders.map(f => f.id === folder.id ? folder : f));
      setEditingFolder(null);
      setSavedId(folder.id);
      setTimeout(() => setSavedId(null), 2000);
    } catch {
      alert('Failed to save folder');
    } finally {
      setSavingId(null);
    }
  };

  const handleDelete = async (id: number) => {
    if (!confirm('Delete this folder?')) return;
    await fetch(`/api/knowledge/folders/${id}`, { method: 'DELETE' });
    setKnowledgeFolders(knowledgeFolders.filter(f => f.id !== id));
    setExpandedIds(prev => { const n = new Set(prev); n.delete(id); return n; });
  };

  // Table mapping helpers
  const getMappingName = (tableName: string) => {
    const saved = tableMappings.find(m => m.table_name === tableName);
    return saved?.mapping_name ?? '';
  };

  const getEditingValue = (tableName: string) => {
    if (tableName in editingMappings) return editingMappings[tableName];
    return getMappingName(tableName);
  };

  const handleSaveMapping = async (tableName: string) => {
    const mappingName = (editingMappings[tableName] ?? getMappingName(tableName)).trim();
    setSavingTable(tableName);
    try {
      await fetch('/api/table-mappings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ table_name: tableName, mapping_name: mappingName }),
      });
      if (mappingName) {
        const updated = tableMappings.filter(m => m.table_name !== tableName);
        setTableMappings([...updated, { table_name: tableName, mapping_name: mappingName }]);
      } else {
        setTableMappings(tableMappings.filter(m => m.table_name !== tableName));
      }
      setEditingMappings(prev => { const n = { ...prev }; delete n[tableName]; return n; });
      setSavedTable(tableName);
      setTimeout(() => setSavedTable(null), 2000);
    } catch {
      alert('Failed to save mapping');
    } finally {
      setSavingTable(null);
    }
  };

  const allTableNames = Object.keys(schema).sort();
  const filteredTables = tableSearch.trim()
    ? allTableNames.filter(t =>
        t.toLowerCase().includes(tableSearch.toLowerCase()) ||
        getMappingName(t).toLowerCase().includes(tableSearch.toLowerCase())
      )
    : allTableNames;

  return (
    <div className="p-8 max-w-4xl mx-auto">
      <div className="mb-6">
        <h2 className="text-2xl font-bold text-slate-900 tracking-tight">Knowledge Base</h2>
        <p className="text-slate-500 mt-1 max-w-2xl">
          Organize business context and define friendly names for your ClickHouse tables.
        </p>
      </div>

      {/* Sub-tab switcher */}
      <div className="flex gap-1 mb-6 bg-slate-100 rounded-xl p-1 w-fit">
        <button
          onClick={() => setActiveSubTab('folders')}
          className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
            activeSubTab === 'folders'
              ? 'bg-white text-slate-800 shadow-sm'
              : 'text-slate-500 hover:text-slate-700'
          }`}
        >
          <BookOpen size={15} />
          Folders
        </button>
        <button
          onClick={() => setActiveSubTab('table-mapping')}
          className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
            activeSubTab === 'table-mapping'
              ? 'bg-white text-slate-800 shadow-sm'
              : 'text-slate-500 hover:text-slate-700'
          }`}
        >
          <Table2 size={15} />
          Table Mapping
          {tableMappings.length > 0 && (
            <span className="bg-emerald-100 text-emerald-700 text-xs px-1.5 py-0.5 rounded-full font-semibold">
              {tableMappings.length}
            </span>
          )}
        </button>
        <button
          onClick={() => setActiveSubTab('fk-relations')}
          className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
            activeSubTab === 'fk-relations'
              ? 'bg-white text-slate-800 shadow-sm'
              : 'text-slate-500 hover:text-slate-700'
          }`}
        >
          <GitFork size={15} />
          FK Relations
          {fkRelations.length > 0 && (
            <span className="bg-violet-100 text-violet-700 text-xs px-1.5 py-0.5 rounded-full font-semibold">
              {fkRelations.length}
            </span>
          )}
        </button>
      </div>

      {/* ── Folders sub-tab ── */}
      {activeSubTab === 'folders' && (
      <div>
      <div className="mb-6 flex items-start justify-between">
        <div>
          <p className="text-slate-500 text-sm max-w-2xl">
            Each folder's title is used for similarity matching during AI queries — allowing fast, precise retrieval without reading full content.
          </p>
        </div>
        <button
          onClick={() => setIsCreating(true)}
          className="shrink-0 flex items-center gap-2 bg-emerald-500 hover:bg-emerald-600 text-white px-4 py-2.5 rounded-xl font-medium text-sm transition-colors shadow-sm"
        >
          <Plus size={16} />
          New Folder
        </button>
      </div>

      {/* Create folder form */}
      <AnimatePresence>
        {isCreating && (
          <motion.div
            initial={{ opacity: 0, y: -10 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -10 }}
            className="mb-6 bg-white rounded-2xl border-2 border-emerald-200 shadow-sm overflow-hidden"
          >
            <div className="p-4 border-b border-slate-100 flex items-center justify-between bg-emerald-50/50">
              <div className="flex items-center gap-2 text-emerald-700">
                <FolderOpen size={18} />
                <span className="font-semibold text-sm">New Folder</span>
              </div>
              <button onClick={() => setIsCreating(false)} className="text-slate-400 hover:text-slate-600">
                <X size={16} />
              </button>
            </div>
            <div className="p-4 space-y-3">
              <div>
                <label className="text-xs font-semibold text-slate-600 uppercase tracking-wide mb-1 block">
                  Title <span className="text-emerald-500">*</span>
                </label>
                <input
                  type="text"
                  value={newFolder.title}
                  onChange={e => setNewFolder({ ...newFolder, title: e.target.value })}
                  placeholder="Ex: Description of the orders table"
                  className="w-full bg-slate-50 border border-slate-200 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-emerald-500/20 focus:border-emerald-500 transition-all"
                  autoFocus
                />
                <p className="mt-1 text-xs text-slate-400">Used for similarity search — be descriptive (e.g., "Description of the orders table")</p>
              </div>
              <div>
                <label className="text-xs font-semibold text-slate-600 uppercase tracking-wide mb-1 block">Content</label>
                <textarea
                  value={newFolder.content}
                  onChange={e => setNewFolder({ ...newFolder, content: e.target.value })}
                  placeholder="Describe the table, its columns, business rules, relationships..."
                  rows={6}
                  className="w-full bg-slate-50 border border-slate-200 rounded-lg px-3 py-2 text-sm font-mono focus:ring-2 focus:ring-emerald-500/20 focus:border-emerald-500 transition-all resize-y"
                />
              </div>
              <div className="flex justify-end gap-2">
                <button onClick={() => setIsCreating(false)} className="px-4 py-2 text-sm text-slate-600 hover:bg-slate-100 rounded-lg transition-colors">
                  Cancel
                </button>
                <button
                  onClick={handleCreate}
                  disabled={!newFolder.title.trim()}
                  className="flex items-center gap-2 bg-emerald-500 hover:bg-emerald-600 disabled:opacity-50 text-white px-4 py-2 rounded-lg text-sm font-medium transition-colors"
                >
                  <Save size={14} />
                  Create Folder
                </button>
              </div>
            </div>
          </motion.div>
        )}
      </AnimatePresence>

      {/* Folder list */}
      {knowledgeFolders.length === 0 && !isCreating ? (
        <div className="flex flex-col items-center justify-center h-48 text-slate-400 bg-white rounded-2xl border border-dashed border-slate-200">
          <BookOpen size={36} className="mb-3 opacity-20" />
          <p className="text-sm">No folders yet. Create your first knowledge folder.</p>
        </div>
      ) : (
        <div className="space-y-3">
          {knowledgeFolders.map(folder => {
            const isExpanded = expandedIds.has(folder.id);
            const isEditing = editingFolder?.id === folder.id;

            return (
              <div key={folder.id} className="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden">
                {/* Folder header */}
                <div
                  className="p-4 flex items-center justify-between cursor-pointer hover:bg-slate-50 transition-colors"
                  onClick={() => !isEditing && toggleExpand(folder.id)}
                >
                  <div className="flex items-center gap-3 flex-1 min-w-0">
                    {isExpanded ? (
                      <ChevronDown size={16} className="text-slate-400 shrink-0" />
                    ) : (
                      <ChevronRight size={16} className="text-slate-400 shrink-0" />
                    )}
                    <div className="bg-amber-100 p-1.5 rounded-lg text-amber-600 shrink-0">
                      <FolderOpen size={15} />
                    </div>
                    {isEditing ? (
                      <input
                        type="text"
                        value={editingFolder.title}
                        onChange={e => setEditingFolder({ ...editingFolder, title: e.target.value })}
                        onClick={e => e.stopPropagation()}
                        className="flex-1 bg-slate-50 border border-slate-200 rounded-lg px-3 py-1 text-sm font-semibold focus:ring-2 focus:ring-emerald-500/20 focus:border-emerald-500"
                        autoFocus
                      />
                    ) : (
                      <span className="font-semibold text-slate-800 text-sm truncate">{folder.title}</span>
                    )}
                    {savedId === folder.id && (
                      <CheckCircle2 size={16} className="text-emerald-500 shrink-0" />
                    )}
                  </div>
                  <div className="flex items-center gap-1.5 ml-3 shrink-0" onClick={e => e.stopPropagation()}>
                    {isEditing ? (
                      <>
                        <button
                          onClick={() => handleUpdate(editingFolder!)}
                          disabled={savingId === folder.id}
                          className="flex items-center gap-1 bg-emerald-500 hover:bg-emerald-600 text-white px-3 py-1.5 rounded-lg text-xs font-medium transition-colors"
                        >
                          <Save size={12} />
                          {savingId === folder.id ? 'Saving...' : 'Save'}
                        </button>
                        <button
                          onClick={() => setEditingFolder(null)}
                          className="p-1.5 text-slate-400 hover:text-slate-600 hover:bg-slate-100 rounded-lg transition-colors"
                        >
                          <X size={14} />
                        </button>
                      </>
                    ) : (
                      <>
                        <button
                          onClick={() => { setEditingFolder({ ...folder }); setExpandedIds(prev => new Set([...prev, folder.id])); }}
                          className="p-1.5 text-slate-400 hover:text-blue-500 hover:bg-blue-50 rounded-lg transition-colors"
                          title="Edit folder"
                        >
                          <Edit3 size={14} />
                        </button>
                        <button
                          onClick={() => handleDelete(folder.id)}
                          className="p-1.5 text-slate-400 hover:text-red-500 hover:bg-red-50 rounded-lg transition-colors"
                          title="Delete folder"
                        >
                          <Trash2 size={14} />
                        </button>
                      </>
                    )}
                  </div>
                </div>

                {/* Folder content */}
                <AnimatePresence>
                  {isExpanded && (
                    <motion.div
                      initial={{ height: 0 }}
                      animate={{ height: 'auto' }}
                      exit={{ height: 0 }}
                      className="overflow-hidden"
                    >
                      <div className="border-t border-slate-100 p-4">
                        {isEditing ? (
                          <textarea
                            value={editingFolder.content}
                            onChange={e => setEditingFolder({ ...editingFolder, content: e.target.value })}
                            rows={10}
                            className="w-full bg-slate-50 border border-slate-200 rounded-lg px-3 py-2 text-sm font-mono focus:ring-2 focus:ring-emerald-500/20 focus:border-emerald-500 transition-all resize-y"
                            placeholder="Describe tables, columns, business rules..."
                          />
                        ) : (
                          <pre className="text-sm text-slate-600 font-mono whitespace-pre-wrap leading-relaxed">
                            {folder.content || <span className="text-slate-400 italic not-italic font-sans">No content yet. Click edit to add.</span>}
                          </pre>
                        )}
                        <div className="mt-2 text-xs text-slate-400">
                          Updated {new Date(folder.updated_at).toLocaleDateString()}
                        </div>
                      </div>
                    </motion.div>
                  )}
                </AnimatePresence>
              </div>
            );
          })}
        </div>
      )}
      </div>
      )}

      {/* ── FK Relations sub-tab ── */}
      {activeSubTab === 'fk-relations' && (
        <FkRelationsTab
          fkRelations={fkRelations}
          setFkRelations={setFkRelations}
          deletingFkId={deletingFkId}
          setDeletingFkId={setDeletingFkId}
        />
      )}

      {/* ── Table Mapping sub-tab ── */}
      {activeSubTab === 'table-mapping' && (
        <div>
          <div className="mb-4 flex items-center gap-3">
            <div className="relative flex-1 max-w-sm">
              <Search size={15} className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" />
              <input
                type="text"
                value={tableSearch}
                onChange={e => setTableSearch(e.target.value)}
                placeholder="Search tables or mapping names..."
                className="w-full pl-9 pr-3 py-2 bg-white border border-slate-200 rounded-xl text-sm focus:ring-2 focus:ring-emerald-500/20 focus:border-emerald-500 transition-all"
              />
            </div>
            <p className="text-sm text-slate-500">
              {filteredTables.length} table{filteredTables.length !== 1 ? 's' : ''}
              {tableMappings.length > 0 && (
                <span className="ml-2 text-emerald-600 font-medium">· {tableMappings.length} mapped</span>
              )}
            </p>
          </div>

          {allTableNames.length === 0 ? (
            <div className="flex flex-col items-center justify-center h-48 text-slate-400 bg-white rounded-2xl border border-dashed border-slate-200">
              <Table2 size={36} className="mb-3 opacity-20" />
              <p className="text-sm">No tables found. Make sure your ClickHouse connection is configured.</p>
            </div>
          ) : (
            <div className="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden">
              <div className="grid grid-cols-[1fr_1fr_auto] gap-0 text-xs font-semibold text-slate-500 uppercase tracking-wide px-4 py-2.5 bg-slate-50 border-b border-slate-100">
                <span>Technical Name</span>
                <span>Friendly Mapping Name</span>
                <span></span>
              </div>
              <div className="divide-y divide-slate-100">
                {filteredTables.map(tableName => {
                  const currentSavedName = getMappingName(tableName);
                  const currentEditValue = getEditingValue(tableName);
                  const isDirty = currentEditValue !== currentSavedName;
                  const isSaving = savingTable === tableName;
                  const isSaved = savedTable === tableName;

                  return (
                    <div key={tableName} className="grid grid-cols-[1fr_1fr_auto] gap-3 items-center px-4 py-2.5 hover:bg-slate-50 transition-colors">
                      <div className="flex items-center gap-2 min-w-0">
                        <div className="w-1.5 h-1.5 rounded-full bg-blue-400 shrink-0" />
                        <span className="font-mono text-sm text-slate-700 truncate" title={tableName}>{tableName}</span>
                      </div>
                      <input
                        type="text"
                        value={currentEditValue}
                        onChange={e => setEditingMappings(prev => ({ ...prev, [tableName]: e.target.value }))}
                        onKeyDown={e => { if (e.key === 'Enter') handleSaveMapping(tableName); }}
                        placeholder="Ex: Home product sales"
                        className="w-full bg-slate-50 border border-slate-200 rounded-lg px-3 py-1.5 text-sm focus:ring-2 focus:ring-emerald-500/20 focus:border-emerald-500 transition-all"
                      />
                      <div className="flex items-center gap-1.5 shrink-0 w-20 justify-end">
                        {isSaved && !isDirty ? (
                          <CheckCircle2 size={16} className="text-emerald-500" />
                        ) : isDirty ? (
                          <button
                            onClick={() => handleSaveMapping(tableName)}
                            disabled={isSaving}
                            className="flex items-center gap-1 bg-emerald-500 hover:bg-emerald-600 disabled:opacity-50 text-white px-2.5 py-1 rounded-lg text-xs font-medium transition-colors"
                          >
                            <Save size={11} />
                            {isSaving ? '...' : 'Save'}
                          </button>
                        ) : currentSavedName ? (
                          <button
                            onClick={() => {
                              setEditingMappings(prev => ({ ...prev, [tableName]: '' }));
                            }}
                            className="p-1 text-slate-300 hover:text-red-400 hover:bg-red-50 rounded-lg transition-colors"
                            title="Remove mapping"
                          >
                            <X size={14} />
                          </button>
                        ) : null}
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// ── FK Relations sub-tab ─────────────────────────────────────────────────────

interface FkRelationsTabProps {
  fkRelations: FkRelation[];
  setFkRelations: (relations: FkRelation[]) => void;
  deletingFkId: number | null;
  setDeletingFkId: (id: number | null) => void;
}

const DIRECTION_OPTIONS = [
  { value: 'A → B', label: 'A → B  (FK in Table A references Table B)' },
  { value: 'B → A', label: 'B → A  (FK in Table B references Table A)' },
  { value: 'A ↔ B', label: 'A ↔ B  (bidirectional / many-to-many)' },
];

function FkRelationsTab({ fkRelations, setFkRelations, deletingFkId, setDeletingFkId }: FkRelationsTabProps) {
  const { schema } = useAppStore();

  // Manual creation form state
  const [showForm, setShowForm] = useState(false);
  const [formTableA, setFormTableA] = useState('');
  const [formFieldA, setFormFieldA] = useState('');
  const [formTableB, setFormTableB] = useState('');
  const [formFieldB, setFormFieldB] = useState('');
  const [formDirection, setFormDirection] = useState('A → B');
  const [isSaving, setIsSaving] = useState(false);

  const allTables = Object.keys(schema).sort();
  const columnsA = formTableA ? (schema[formTableA] || []).map(c => c.name) : [];
  const columnsB = formTableB ? (schema[formTableB] || []).map(c => c.name) : [];

  const resetForm = () => {
    setFormTableA(''); setFormFieldA('');
    setFormTableB(''); setFormFieldB('');
    setFormDirection('A → B');
    setShowForm(false);
  };

  const handleCreate = async () => {
    if (!formTableA || !formFieldA || !formTableB || !formFieldB) return;
    setIsSaving(true);
    try {
      const res = await fetch('/api/fk-relations', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          table_a: formTableA,
          field_a: formFieldA,
          table_b: formTableB,
          field_b: formFieldB,
          direction: formDirection,
          llm_reason: '',
        }),
      });
      const data = await res.json();
      if (data.error) throw new Error(data.error);
      setFkRelations([...fkRelations, data]);
      resetForm();
    } catch (e: any) {
      alert(`Failed to create relation: ${e.message}`);
    } finally {
      setIsSaving(false);
    }
  };

  const handleDelete = async (id: number) => {
    if (!confirm('Delete this FK relationship?')) return;
    setDeletingFkId(id);
    try {
      await fetch(`/api/fk-relations/${id}`, { method: 'DELETE' });
      setFkRelations(fkRelations.filter(r => r.id !== id));
    } catch {
      alert('Error during deletion.');
    } finally {
      setDeletingFkId(null);
    }
  };

  const selectClass = "w-full bg-slate-50 border border-slate-200 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-violet-500/20 focus:border-violet-500 transition-all";

  return (
    <div>
      <div className="mb-4 flex items-start justify-between">
        <div>
          <p className="text-slate-500 text-sm max-w-2xl">
            Foreign key relationships identified by the <strong>Key Identifier Agent</strong> or added manually.
            These relationships are automatically injected into the AI context to improve SQL query generation (JOINs).
          </p>
        </div>
        <button
          onClick={() => setShowForm(v => !v)}
          className="shrink-0 flex items-center gap-2 bg-violet-500 hover:bg-violet-600 text-white px-4 py-2.5 rounded-xl font-medium text-sm transition-colors shadow-sm"
        >
          <Plus size={16} />
          Add manually
        </button>
      </div>

      {/* Manual creation form */}
      <AnimatePresence>
        {showForm && (
          <motion.div
            initial={{ opacity: 0, y: -10 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -10 }}
            className="mb-5 bg-white rounded-2xl border-2 border-violet-200 shadow-sm overflow-hidden"
          >
            <div className="p-4 border-b border-slate-100 flex items-center justify-between bg-violet-50/50">
              <div className="flex items-center gap-2 text-violet-700">
                <GitFork size={17} />
                <span className="font-semibold text-sm">New FK Relation</span>
              </div>
              <button onClick={resetForm} className="text-slate-400 hover:text-slate-600">
                <X size={16} />
              </button>
            </div>
            <div className="p-4 space-y-4">
              <div className="grid grid-cols-2 gap-4">
                {/* Table A */}
                <div className="space-y-2">
                  <label className="text-xs font-semibold text-slate-600 uppercase tracking-wide block">Table A <span className="text-violet-500">*</span></label>
                  <select value={formTableA} onChange={e => { setFormTableA(e.target.value); setFormFieldA(''); }} className={selectClass}>
                    <option value="">— Select table —</option>
                    {allTables.map(t => <option key={t} value={t}>{t}</option>)}
                  </select>
                </div>
                {/* Field A */}
                <div className="space-y-2">
                  <label className="text-xs font-semibold text-slate-600 uppercase tracking-wide block">Field A <span className="text-violet-500">*</span></label>
                  <select value={formFieldA} onChange={e => setFormFieldA(e.target.value)} className={selectClass} disabled={!formTableA}>
                    <option value="">— Select field —</option>
                    {columnsA.map(c => <option key={c} value={c}>{c}</option>)}
                  </select>
                </div>
                {/* Table B */}
                <div className="space-y-2">
                  <label className="text-xs font-semibold text-slate-600 uppercase tracking-wide block">Table B <span className="text-violet-500">*</span></label>
                  <select value={formTableB} onChange={e => { setFormTableB(e.target.value); setFormFieldB(''); }} className={selectClass}>
                    <option value="">— Select table —</option>
                    {allTables.map(t => <option key={t} value={t}>{t}</option>)}
                  </select>
                </div>
                {/* Field B */}
                <div className="space-y-2">
                  <label className="text-xs font-semibold text-slate-600 uppercase tracking-wide block">Field B <span className="text-violet-500">*</span></label>
                  <select value={formFieldB} onChange={e => setFormFieldB(e.target.value)} className={selectClass} disabled={!formTableB}>
                    <option value="">— Select field —</option>
                    {columnsB.map(c => <option key={c} value={c}>{c}</option>)}
                  </select>
                </div>
              </div>

              {/* Direction */}
              <div className="space-y-2">
                <label className="text-xs font-semibold text-slate-600 uppercase tracking-wide block">Relation direction</label>
                <div className="flex gap-3">
                  {DIRECTION_OPTIONS.map(opt => (
                    <label key={opt.value} className="flex items-center gap-2 cursor-pointer">
                      <input
                        type="radio"
                        name="fk-direction"
                        value={opt.value}
                        checked={formDirection === opt.value}
                        onChange={() => setFormDirection(opt.value)}
                        className="accent-violet-600"
                      />
                      <span className="text-sm text-slate-700">{opt.label}</span>
                    </label>
                  ))}
                </div>
              </div>

              {/* Preview */}
              {formTableA && formFieldA && formTableB && formFieldB && (
                <div className="flex items-center gap-2 p-3 bg-slate-50 rounded-xl border border-slate-100 text-sm font-mono">
                  <span className="text-slate-800 font-semibold">{formTableA}.<span className="text-violet-600">{formFieldA}</span></span>
                  <ArrowRight size={14} className="text-slate-400" />
                  <span className="text-slate-800 font-semibold">{formTableB}.<span className="text-emerald-600">{formFieldB}</span></span>
                  <span className="ml-2 text-xs text-slate-400 font-sans">({formDirection})</span>
                </div>
              )}

              <div className="flex justify-end gap-2">
                <button onClick={resetForm} className="px-4 py-2 text-sm text-slate-600 hover:bg-slate-100 rounded-lg transition-colors">Cancel</button>
                <button
                  onClick={handleCreate}
                  disabled={isSaving || !formTableA || !formFieldA || !formTableB || !formFieldB}
                  className="flex items-center gap-2 bg-violet-500 hover:bg-violet-600 disabled:opacity-50 text-white px-4 py-2 rounded-lg text-sm font-medium transition-colors"
                >
                  <Save size={14} />
                  {isSaving ? 'Saving...' : 'Add relation'}
                </button>
              </div>
            </div>
          </motion.div>
        )}
      </AnimatePresence>

      {fkRelations.length === 0 ? (
        <div className="flex flex-col items-center justify-center h-48 text-slate-400 bg-white rounded-2xl border border-dashed border-slate-200">
          <GitFork size={36} className="mb-3 opacity-20" />
          <p className="text-sm font-medium">No FK relationships registered.</p>
          <p className="text-xs mt-1 text-slate-400">Run the <strong>Key Identifier Agent</strong> or add one manually.</p>
        </div>
      ) : (
        <div className="space-y-2">
          <AnimatePresence>
            {fkRelations.map(rel => (
              <motion.div
                key={rel.id}
                initial={{ opacity: 0, y: -8 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, x: -20 }}
                className="bg-white rounded-xl border border-slate-200 shadow-sm p-4 flex items-start gap-4"
              >
                {/* Icon */}
                <div className="p-2 bg-violet-50 rounded-lg shrink-0 mt-0.5">
                  <GitFork size={16} className="text-violet-500" />
                </div>

                {/* Relation detail */}
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2 flex-wrap">
                    <span className="font-mono text-sm font-semibold text-slate-800">
                      {rel.table_a}.<span className="text-violet-600">{rel.field_a}</span>
                    </span>
                    <ArrowRight size={14} className="text-slate-400 shrink-0" />
                    <span className="font-mono text-sm font-semibold text-slate-800">
                      {rel.table_b}.<span className="text-emerald-600">{rel.field_b}</span>
                    </span>
                  </div>
                  {rel.direction && (
                    <p className="text-xs text-slate-500 mt-1 font-mono">{rel.direction}</p>
                  )}
                  {rel.llm_reason && (
                    <p className="text-xs text-slate-400 mt-1 italic">{rel.llm_reason}</p>
                  )}
                  <p className="text-[10px] text-slate-400 mt-1.5">
                    Added on {new Date(rel.created_at).toLocaleDateString()}
                  </p>
                </div>

                {/* Delete */}
                <button
                  onClick={() => handleDelete(rel.id)}
                  disabled={deletingFkId === rel.id}
                  className="p-1.5 text-slate-300 hover:text-red-400 hover:bg-red-50 rounded-lg transition-colors shrink-0 disabled:opacity-50"
                  title="Delete this relationship"
                >
                  {deletingFkId === rel.id ? <RotateCcw size={14} className="animate-spin" /> : <Trash2 size={14} />}
                </button>
              </motion.div>
            ))}
          </AnimatePresence>
        </div>
      )}

      <div className="mt-4 p-3 bg-violet-50 border border-violet-100 rounded-xl text-xs text-violet-700">
        <strong>How does it work?</strong> The Key Identifier Agent analyzes your tables, samples up to 5 values per candidate field,
        then uses the local LLM to identify FK↔PK matches. You can also add relationships manually above.
        All relations are automatically injected into the AI context to improve SQL JOIN generation.
      </div>
    </div>
  );
}
