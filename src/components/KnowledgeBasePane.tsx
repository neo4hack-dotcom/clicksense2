import { useState, useEffect } from 'react';
import { BookOpen, Save, CheckCircle2 } from 'lucide-react';

export function KnowledgeBasePane() {
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
    } catch (error) {
      alert('Failed to save knowledge base');
    } finally {
      setIsSaving(false);
    }
  };

  return (
    <div className="p-8 max-w-4xl mx-auto h-full flex flex-col">
      <div className="mb-8">
        <h2 className="text-2xl font-bold text-slate-900 tracking-tight">Functional Knowledge Base</h2>
        <p className="text-slate-500 mt-1 max-w-2xl">
          Inject business context here. The AI Assistant will use this information to better understand your queries, map business terms to database columns, and generate more accurate SQL.
        </p>
      </div>

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
    </div>
  );
}
