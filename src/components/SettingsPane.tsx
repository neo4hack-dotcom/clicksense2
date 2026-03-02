import { useState, useEffect } from 'react';
import { Save, Database, Cpu, CheckCircle2, RefreshCw } from 'lucide-react';

export function SettingsPane() {
  const [config, setConfig] = useState({
    clickhouse: { host: '', username: '', password: '', database: '' },
    llm: { provider: 'ollama', model: '', ollamaUrl: '', httpUrl: '', apiKey: '' }
  });
  const [isSaving, setIsSaving] = useState(false);
  const [saved, setSaved] = useState(false);
  const [models, setModels] = useState<string[]>([]);
  const [isLoadingModels, setIsLoadingModels] = useState(false);
  const [isTestingClickhouse, setIsTestingClickhouse] = useState(false);
  const [clickhouseTestResult, setClickhouseTestResult] = useState<'idle' | 'success' | 'error'>('idle');
  const [isTestingLlm, setIsTestingLlm] = useState(false);
  const [llmTestResult, setLlmTestResult] = useState<'idle' | 'success' | 'error'>('idle');

  useEffect(() => {
    fetch('/api/config')
      .then(res => res.json())
      .then(data => {
        setConfig({
          clickhouse: data.clickhouseConfig,
          llm: data.llmConfig
        });
      });
  }, []);

  const handleSave = async () => {
    setIsSaving(true);
    try {
      await fetch('/api/config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ clickhouse: config.clickhouse, llm: config.llm }),
      });
      setSaved(true);
      setTimeout(() => setSaved(false), 3000);
    } catch (error) {
      alert('Failed to save settings');
    } finally {
      setIsSaving(false);
    }
  };

  const testClickhouse = async () => {
    setIsTestingClickhouse(true);
    setClickhouseTestResult('idle');
    try {
      const res = await fetch('/api/clickhouse/test', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(config.clickhouse),
      });
      const data = await res.json();
      if (data.error) throw new Error(data.error);
      setClickhouseTestResult('success');
    } catch (e: any) {
      setClickhouseTestResult('error');
      alert(`ClickHouse Connection Failed: ${e.message}`);
    } finally {
      setIsTestingClickhouse(false);
    }
  };

  const testLlm = async () => {
    setIsTestingLlm(true);
    setLlmTestResult('idle');
    try {
      const res = await fetch('/api/llm/test', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(config.llm),
      });
      const data = await res.json();
      if (data.error) throw new Error(data.error);
      setLlmTestResult('success');
    } catch (e: any) {
      setLlmTestResult('error');
      alert(`LLM Connection Failed: ${e.message}`);
    } finally {
      setIsTestingLlm(false);
    }
  };

  const fetchModels = async () => {
    setIsLoadingModels(true);
    try {
      // Save current config first so backend uses the latest URL/Key
      await fetch('/api/config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ llm: config.llm }),
      });
      
      const res = await fetch('/api/llm/models');
      const data = await res.json();
      
      if (data.error) throw new Error(data.error);
      
      setModels(data.models || []);
      if (data.models && data.models.length > 0 && !config.llm.model) {
        setConfig(prev => ({...prev, llm: {...prev.llm, model: data.models[0]}}));
      }
    } catch (e: any) {
      alert(`Failed to fetch models: ${e.message}`);
    } finally {
      setIsLoadingModels(false);
    }
  };

  return (
    <div className="p-8 max-w-4xl mx-auto space-y-8">
      <div>
        <h2 className="text-2xl font-bold text-slate-900 tracking-tight">Configuration</h2>
        <p className="text-slate-500 mt-1">Manage your database connections and AI models.</p>
      </div>

      <div className="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden">
        <div className="p-6 border-b border-slate-100 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="bg-blue-100 p-2 rounded-lg text-blue-600">
              <Database size={20} />
            </div>
            <h3 className="text-lg font-semibold text-slate-800">ClickHouse Connection</h3>
          </div>
          <button
            onClick={testClickhouse}
            disabled={isTestingClickhouse}
            className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors border flex items-center gap-2 ${
              clickhouseTestResult === 'success' ? 'bg-emerald-50 text-emerald-700 border-emerald-200' :
              clickhouseTestResult === 'error' ? 'bg-red-50 text-red-700 border-red-200' :
              'bg-slate-50 text-slate-700 border-slate-200 hover:bg-slate-100'
            }`}
          >
            {isTestingClickhouse ? (
              <><RefreshCw size={16} className="animate-spin" /> Testing...</>
            ) : clickhouseTestResult === 'success' ? (
              <><CheckCircle2 size={16} /> Connected</>
            ) : (
              'Test Connection'
            )}
          </button>
        </div>
        <div className="p-6 grid grid-cols-2 gap-6">
          <div className="space-y-2">
            <label className="text-sm font-medium text-slate-700">Host URL</label>
            <input 
              type="text" 
              value={config.clickhouse.host}
              onChange={e => setConfig({...config, clickhouse: {...config.clickhouse, host: e.target.value}})}
              className="w-full bg-slate-50 border border-slate-200 rounded-lg px-4 py-2.5 text-sm focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all"
              placeholder="http://localhost:8123"
            />
          </div>
          <div className="space-y-2">
            <label className="text-sm font-medium text-slate-700">Database</label>
            <input 
              type="text" 
              value={config.clickhouse.database}
              onChange={e => setConfig({...config, clickhouse: {...config.clickhouse, database: e.target.value}})}
              className="w-full bg-slate-50 border border-slate-200 rounded-lg px-4 py-2.5 text-sm focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all"
              placeholder="default"
            />
          </div>
          <div className="space-y-2">
            <label className="text-sm font-medium text-slate-700">Username</label>
            <input 
              type="text" 
              value={config.clickhouse.username}
              onChange={e => setConfig({...config, clickhouse: {...config.clickhouse, username: e.target.value}})}
              className="w-full bg-slate-50 border border-slate-200 rounded-lg px-4 py-2.5 text-sm focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all"
              placeholder="default"
            />
          </div>
          <div className="space-y-2">
            <label className="text-sm font-medium text-slate-700">Password</label>
            <input 
              type="password" 
              value={config.clickhouse.password}
              onChange={e => setConfig({...config, clickhouse: {...config.clickhouse, password: e.target.value}})}
              className="w-full bg-slate-50 border border-slate-200 rounded-lg px-4 py-2.5 text-sm focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all"
              placeholder="••••••••"
            />
          </div>
        </div>
      </div>

      <div className="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden">
        <div className="p-6 border-b border-slate-100 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="bg-emerald-100 p-2 rounded-lg text-emerald-600">
              <Cpu size={20} />
            </div>
            <h3 className="text-lg font-semibold text-slate-800">LLM Configuration</h3>
          </div>
          <button
            onClick={testLlm}
            disabled={isTestingLlm}
            className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors border flex items-center gap-2 ${
              llmTestResult === 'success' ? 'bg-emerald-50 text-emerald-700 border-emerald-200' :
              llmTestResult === 'error' ? 'bg-red-50 text-red-700 border-red-200' :
              'bg-slate-50 text-slate-700 border-slate-200 hover:bg-slate-100'
            }`}
          >
            {isTestingLlm ? (
              <><RefreshCw size={16} className="animate-spin" /> Testing...</>
            ) : llmTestResult === 'success' ? (
              <><CheckCircle2 size={16} /> Connected</>
            ) : (
              'Test Connection'
            )}
          </button>
        </div>
        <div className="p-6 space-y-6">
          <div className="space-y-2">
            <label className="text-sm font-medium text-slate-700">Provider</label>
            <select 
              value={config.llm.provider}
              onChange={e => setConfig({...config, llm: {...config.llm, provider: e.target.value}})}
              className="w-full bg-slate-50 border border-slate-200 rounded-lg px-4 py-2.5 text-sm focus:ring-2 focus:ring-emerald-500/20 focus:border-emerald-500 transition-all"
            >
              <option value="ollama">Ollama (Local)</option>
              <option value="http">Custom HTTP (OpenAI Compatible)</option>
            </select>
          </div>
          
          {config.llm.provider === 'ollama' && (
            <div className="space-y-2 animate-in fade-in slide-in-from-top-2">
              <label className="text-sm font-medium text-slate-700">Ollama URL</label>
              <input 
                type="text" 
                value={config.llm.ollamaUrl || ''}
                onChange={e => setConfig({...config, llm: {...config.llm, ollamaUrl: e.target.value}})}
                className="w-full bg-slate-50 border border-slate-200 rounded-lg px-4 py-2.5 text-sm focus:ring-2 focus:ring-emerald-500/20 focus:border-emerald-500 transition-all"
                placeholder="http://localhost:11434"
              />
            </div>
          )}

          {config.llm.provider === 'http' && (
            <div className="space-y-4 animate-in fade-in slide-in-from-top-2">
              <div className="space-y-2">
                <label className="text-sm font-medium text-slate-700">HTTP Endpoint URL</label>
                <input 
                  type="text" 
                  value={config.llm.httpUrl || ''}
                  onChange={e => setConfig({...config, llm: {...config.llm, httpUrl: e.target.value}})}
                  className="w-full bg-slate-50 border border-slate-200 rounded-lg px-4 py-2.5 text-sm focus:ring-2 focus:ring-emerald-500/20 focus:border-emerald-500 transition-all"
                  placeholder="http://localhost:1234"
                />
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium text-slate-700">API Key (Optional)</label>
                <input 
                  type="password" 
                  value={config.llm.apiKey || ''}
                  onChange={e => setConfig({...config, llm: {...config.llm, apiKey: e.target.value}})}
                  className="w-full bg-slate-50 border border-slate-200 rounded-lg px-4 py-2.5 text-sm focus:ring-2 focus:ring-emerald-500/20 focus:border-emerald-500 transition-all"
                  placeholder="sk-..."
                />
              </div>
            </div>
          )}
          
          <div className="space-y-2">
            <label className="text-sm font-medium text-slate-700">Model Name</label>
            <div className="flex gap-2">
              <input 
                list="model-list"
                type="text" 
                value={config.llm.model || ''}
                onChange={e => setConfig({...config, llm: {...config.llm, model: e.target.value}})}
                className="w-full bg-slate-50 border border-slate-200 rounded-lg px-4 py-2.5 text-sm focus:ring-2 focus:ring-emerald-500/20 focus:border-emerald-500 transition-all"
                placeholder={config.llm.provider === 'ollama' ? 'llama3' : 'gpt-3.5-turbo'}
              />
              <datalist id="model-list">
                {models.map(m => <option key={m} value={m} />)}
              </datalist>
              <button
                onClick={fetchModels}
                disabled={isLoadingModels}
                className="shrink-0 flex items-center gap-2 bg-slate-100 hover:bg-slate-200 text-slate-700 px-4 py-2.5 rounded-lg text-sm font-medium transition-colors border border-slate-200"
              >
                <RefreshCw size={16} className={isLoadingModels ? "animate-spin" : ""} />
                Refresh
              </button>
            </div>
          </div>
        </div>
      </div>

      <div className="flex justify-end pt-4">
        <button 
          onClick={handleSave}
          disabled={isSaving}
          className="flex items-center gap-2 bg-slate-900 hover:bg-slate-800 text-white px-6 py-3 rounded-xl font-medium transition-all shadow-sm disabled:opacity-50"
        >
          {saved ? <CheckCircle2 size={18} className="text-emerald-400" /> : <Save size={18} />}
          {isSaving ? 'Saving...' : saved ? 'Saved Successfully' : 'Save Configuration'}
        </button>
      </div>
    </div>
  );
}
