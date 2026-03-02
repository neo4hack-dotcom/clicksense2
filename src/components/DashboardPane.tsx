import { useEffect, useState } from 'react';
import { useAppStore } from '../store';
import { Trash2, RefreshCw, BarChart2, Table, LineChart, PieChart } from 'lucide-react';
import { 
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip as RechartsTooltip, Legend, ResponsiveContainer,
  LineChart as RechartsLineChart, Line, PieChart as RechartsPieChart, Pie, Cell
} from 'recharts';

export function DashboardPane() {
  const { currentUser, savedQueries, setSavedQueries } = useAppStore();
  const [results, setResults] = useState<Record<number, any[]>>({});
  const [loading, setLoading] = useState<Record<number, boolean>>({});

  useEffect(() => {
    if (currentUser) {
      fetch(`/api/saved_queries/${currentUser.id}`)
        .then(res => res.json())
        .then(data => {
          setSavedQueries(data);
          data.forEach((q: any) => executeQuery(q));
        });
    }
  }, [currentUser]);

  const executeQuery = async (query: any) => {
    setLoading(prev => ({ ...prev, [query.id]: true }));
    try {
      const res = await fetch('/api/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query: query.sql }),
      });
      const data = await res.json();
      if (!data.error) {
        setResults(prev => ({ ...prev, [query.id]: data.data }));
      }
    } catch (e) {
      console.error(e);
    } finally {
      setLoading(prev => ({ ...prev, [query.id]: false }));
    }
  };

  const deleteQuery = async (id: number) => {
    await fetch(`/api/saved_queries/${id}`, { method: 'DELETE' });
    setSavedQueries(savedQueries.filter(q => q.id !== id));
  };

  const COLORS = ['#10b981', '#3b82f6', '#f59e0b', '#ef4444', '#8b5cf6'];

  const renderVisual = (query: any) => {
    const data = results[query.id];
    if (loading[query.id]) return <div className="flex items-center justify-center h-full text-slate-400">Loading...</div>;
    if (!data || data.length === 0) return <div className="flex items-center justify-center h-full text-slate-400">No data</div>;

    const keys = Object.keys(data[0]);
    const dimKey = query.config?.dimensions?.[0]?.name || keys[0];
    const measureKeys = keys.filter(k => k !== dimKey);

    switch (query.visual_type) {
      case 'bar':
        return (
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={data}>
              <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e2e8f0" />
              <XAxis dataKey={dimKey} axisLine={false} tickLine={false} tick={{fill: '#64748b', fontSize: 10}} />
              <YAxis axisLine={false} tickLine={false} tick={{fill: '#64748b', fontSize: 10}} />
              <RechartsTooltip cursor={{fill: '#f1f5f9'}} contentStyle={{borderRadius: '8px', border: 'none', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)'}} />
              {measureKeys.map((key, i) => (
                <Bar key={key} dataKey={key} fill={COLORS[i % COLORS.length]} radius={[4, 4, 0, 0]} />
              ))}
            </BarChart>
          </ResponsiveContainer>
        );
      case 'line':
        return (
          <ResponsiveContainer width="100%" height="100%">
            <RechartsLineChart data={data}>
              <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e2e8f0" />
              <XAxis dataKey={dimKey} axisLine={false} tickLine={false} tick={{fill: '#64748b', fontSize: 10}} />
              <YAxis axisLine={false} tickLine={false} tick={{fill: '#64748b', fontSize: 10}} />
              <RechartsTooltip contentStyle={{borderRadius: '8px', border: 'none', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)'}} />
              {measureKeys.map((key, i) => (
                <Line key={key} type="monotone" dataKey={key} stroke={COLORS[i % COLORS.length]} strokeWidth={2} dot={false} />
              ))}
            </RechartsLineChart>
          </ResponsiveContainer>
        );
      case 'pie':
        return (
          <ResponsiveContainer width="100%" height="100%">
            <RechartsPieChart>
              <Pie
                data={data}
                dataKey={measureKeys[0]}
                nameKey={dimKey}
                cx="50%"
                cy="50%"
                outerRadius={80}
                innerRadius={40}
                paddingAngle={2}
              >
                {data.map((entry: any, index: number) => (
                  <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                ))}
              </Pie>
              <RechartsTooltip contentStyle={{borderRadius: '8px', border: 'none', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)'}} />
            </RechartsPieChart>
          </ResponsiveContainer>
        );
      case 'table':
      default:
        return (
          <div className="overflow-auto h-full">
            <table className="w-full text-xs text-left">
              <thead className="text-slate-500 uppercase bg-slate-50 sticky top-0">
                <tr>
                  {keys.map((key) => (
                    <th key={key} className="px-4 py-2 font-semibold">{key}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {data.map((row: any, i: number) => (
                  <tr key={i} className="border-b border-slate-100">
                    {keys.map((key) => (
                      <td key={key} className="px-4 py-2 text-slate-700 whitespace-nowrap">
                        {typeof row[key] === 'number' ? row[key].toLocaleString() : String(row[key])}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        );
    }
  };

  return (
    <div className="p-8 h-full overflow-y-auto bg-slate-50">
      <div className="mb-8">
        <h2 className="text-2xl font-bold text-slate-900 tracking-tight">My Dashboard</h2>
        <p className="text-slate-500 mt-1">Your saved queries and visualizations.</p>
      </div>

      {savedQueries.length === 0 ? (
        <div className="flex flex-col items-center justify-center h-64 text-slate-400 bg-white rounded-2xl border border-slate-200 border-dashed">
          <BarChart2 size={48} className="mb-4 opacity-20" />
          <p>No saved queries yet. Build one in the Visual Builder or ask the AI Assistant!</p>
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-6">
          {savedQueries.map(query => (
            <div key={query.id} className="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden flex flex-col h-80">
              <div className="p-4 border-b border-slate-100 flex items-center justify-between bg-slate-50/50">
                <h3 className="font-semibold text-slate-800 truncate pr-4">{query.name}</h3>
                <div className="flex items-center gap-2 shrink-0">
                  <button onClick={() => executeQuery(query)} className="text-slate-400 hover:text-emerald-500 transition-colors">
                    <RefreshCw size={16} className={loading[query.id] ? "animate-spin" : ""} />
                  </button>
                  <button onClick={() => deleteQuery(query.id)} className="text-slate-400 hover:text-red-500 transition-colors">
                    <Trash2 size={16} />
                  </button>
                </div>
              </div>
              <div className="flex-1 p-4 overflow-hidden">
                {renderVisual(query)}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
