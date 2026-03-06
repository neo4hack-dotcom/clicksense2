import { useState, useEffect } from 'react';
import { Save, Database, Cpu, CheckCircle2, RefreshCw, Search, AlertCircle, Layers } from 'lucide-react';
import { useAppStore } from '../store';

export function SettingsPane() {
  const { ragConfig, setRagConfig } = useAppStore();

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
  const [isTestingEs, setIsTestingEs] = useState(false);
  const [esTestResult, setEsTestResult] = useState<'idle' | 'success' | 'error'>('idle');
  const [isTestingEmbedding, setIsTestingEmbedding] = useState(false);
  const [embeddingTestResult, setEmbeddingTestResult] = useState<'idle' | 'success' | 'error'>('idle');
  const [embeddingTestDims, setEmbeddingTestDims] = useState<number | null>(null);
  const [isSavingRag, setIsSavingRag] = useState(false);
  const [ragSaved, setRagSaved] = useState(false);
  const [embeddingModels, setEmbeddingModels] = useState<string[]>([]);
  const [isLoadingEmbeddingModels, setIsLoadingEmbeddingModels] = useState(false);
  const [localRag, setLocalRag] = useState({
    embeddingUsername: '',
    embeddingPassword: '',
    ...ragConfig,
  });

  useEffect(() => {
    fetch('/api/config')
      .then(res => res.json())
      .then(data => {
        setConfig({
          clickhouse: data.clickhouseConfig,
          llm: data.llmConfig
        });
      });
    fetch('/api/rag/config')
      .then(res => res.json())
      .then(data => {
        if (data && !data.error) {
          setLocalRag(data);
          setRagConfig(data);
        }
      })
      .catch(() => {});
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
    } catch {
      alert('Failed to save settings');
    } finally {
      setIsSaving(false);
    }
  };

  const handleSaveRag = async () => {
    setIsSavingRag(true);
    try {
      await fetch('/api/rag/config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(localRag),
      });
      setRagConfig(localRag);
      setRagSaved(true);
      setTimeout(() => setRagSaved(false), 3000);
    } catch {
      alert('Failed to save RAG config');
    } finally {
      setIsSavingRag(false);
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

  const testElasticsearch = async () => {
    setIsTestingEs(true);
    setEsTestResult('idle');
    try {
      const res = await fetch('/api/rag/test', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(localRag),
      });
      const data = await res.json();
      if (data.error) throw new Error(data.error);
      setEsTestResult('success');
    } catch (e: any) {
      setEsTestResult('error');
      alert(`Elasticsearch connection failed: ${e.message}`);
    } finally {
      setIsTestingEs(false);
    }
  };

  const testEmbedding = async () => {
    setIsTestingEmbedding(true);
    setEmbeddingTestResult('idle');
    setEmbeddingTestDims(null);
    try {
      const res = await fetch('/api/rag/test-embedding', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(localRag),
      });
      const data = await res.json();
      if (data.error) throw new Error(data.error);
      setEmbeddingTestResult('success');
      setEmbeddingTestDims(data.dims ?? null);
    } catch (e: any) {
      setEmbeddingTestResult('error');
      alert(`Embedding model connection failed: ${e.message}`);
    } finally {
      setIsTestingEmbedding(false);
    }
  };

  const fetchModels = async () => {
    setIsLoadingModels(true);
    try {
      await fetch('/api/config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ llm: config.llm }),
      });
      const res = await fetch('/api/llm/models');
      const data = await res.json();
      if (data.error) throw new Error(data.error);
      setModels(data.models || []);
      if (data.models?.length > 0 && !config.llm.model) {
        setConfig(prev => ({ ...prev, llm: { ...prev.llm, model: data.models[0] } }));
      }
    } catch (e: any) {
      alert(`Failed to fetch models: ${e.message}`);
    } finally {
      setIsLoadingModels(false);
    }
  };

  const fetchEmbeddingModels = async () => {
    setIsLoadingEmbeddingModels(true);
    try {
      const res = await fetch('/api/rag/embedding-models', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(localRag),
      });
      const data = await res.json();
      if (data.error) throw new Error(data.error);
      setEmbeddingModels(data.models || []);
    } catch (e: any) {
      alert(`Failed to fetch embedding models: ${e.message}`);
    } finally {
      setIsLoadingEmbeddingModels(false);
    }
  };

  const testBtnClass = (result: 'idle' | 'success' | 'error') =>
    `px-4 py-2 rounded-lg text-sm font-medium transition-colors border flex items-center gap-2 ${result === 'success' ? 'bg-emerald-50 text-emerald-700 border-emerald-200' : result === 'error' ? 'bg-red-50 text-red-700 border-red-200' : 'bg-slate-50 text-slate-700 border-slate-200 hover:bg-slate-100'}`;

  const inputClass = "w-full bg-slate-50 border border-slate-200 rounded-lg px-4 py-2.5 text-sm focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all";
  const labelClass = "text-sm font-medium text-slate-700";

  return (
    <div className="p-8 max-w-4xl mx-auto space-y-8">
      <div>
        <h2 className="text-2xl font-bold text-slate-900 tracking-tight">Configuration</h2>
        <p className="text-slate-500 mt-1">Manage your database connections, AI models and RAG settings.</p>
      </div>

      {/* ClickHouse */}
      <div className="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden">
        <div className="p-6 border-b border-slate-100 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="bg-blue-100 p-2 rounded-lg text-blue-600"><Database size={20} /></div>
            <h3 className="text-lg font-semibold text-slate-800">ClickHouse Connection</h3>
          </div>
          <button onClick={testClickhouse} disabled={isTestingClickhouse} className={testBtnClass(clickhouseTestResult)}>
            {isTestingClickhouse ? <><RefreshCw size={16} className="animate-spin" /> Testing...</> : clickhouseTestResult === 'success' ? <><CheckCircle2 size={16} /> Connected</> : clickhouseTestResult === 'error' ? <><AlertCircle size={16} /> Failed</> : 'Test Connection'}
          </button>
        </div>
        <div className="p-6 grid grid-cols-2 gap-6">
          <div className="space-y-2">
            <label className={labelClass}>Host URL</label>
            <input type="text" value={config.clickhouse.host} onChange={e => setConfig({ ...config, clickhouse: { ...config.clickhouse, host: e.target.value } })} className={inputClass} placeholder="http://localhost:8123" />
          </div>
          <div className="space-y-2">
            <label className={labelClass}>Database</label>
            <input type="text" value={config.clickhouse.database} onChange={e => setConfig({ ...config, clickhouse: { ...config.clickhouse, database: e.target.value } })} className={inputClass} placeholder="default" />
          </div>
          <div className="space-y-2">
            <label className={labelClass}>Username</label>
            <input type="text" value={config.clickhouse.username} onChange={e => setConfig({ ...config, clickhouse: { ...config.clickhouse, username: e.target.value } })} className={inputClass} placeholder="default" />
          </div>
          <div className="space-y-2">
            <label className={labelClass}>Password</label>
            <input type="password" value={config.clickhouse.password} onChange={e => setConfig({ ...config, clickhouse: { ...config.clickhouse, password: e.target.value } })} className={inputClass} placeholder="••••••••" />
          </div>
        </div>
      </div>

      {/* LLM */}
      <div className="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden">
        <div className="p-6 border-b border-slate-100 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="bg-emerald-100 p-2 rounded-lg text-emerald-600"><Cpu size={20} /></div>
            <h3 className="text-lg font-semibold text-slate-800">LLM Configuration</h3>
          </div>
          <button onClick={testLlm} disabled={isTestingLlm} className={testBtnClass(llmTestResult)}>
            {isTestingLlm ? <><RefreshCw size={16} className="animate-spin" /> Testing...</> : llmTestResult === 'success' ? <><CheckCircle2 size={16} /> Connected</> : llmTestResult === 'error' ? <><AlertCircle size={16} /> Failed</> : 'Test Connection'}
          </button>
        </div>
        <div className="p-6 space-y-6">
          <div className="space-y-2">
            <label className={labelClass}>Provider</label>
            <select value={config.llm.provider} onChange={e => setConfig({ ...config, llm: { ...config.llm, provider: e.target.value } })} className={inputClass}>
              <option value="ollama">Ollama (Local)</option>
              <option value="http">Custom HTTP (OpenAI Compatible)</option>
            </select>
          </div>
          {config.llm.provider === 'ollama' && (
            <div className="space-y-2">
              <label className={labelClass}>Ollama URL</label>
              <input type="text" value={config.llm.ollamaUrl || ''} onChange={e => setConfig({ ...config, llm: { ...config.llm, ollamaUrl: e.target.value } })} className={inputClass} placeholder="http://localhost:11434" />
            </div>
          )}
          {config.llm.provider === 'http' && (
            <div className="space-y-4">
              <div className="space-y-2">
                <label className={labelClass}>HTTP Endpoint URL</label>
                <input type="text" value={config.llm.httpUrl || ''} onChange={e => setConfig({ ...config, llm: { ...config.llm, httpUrl: e.target.value } })} className={inputClass} placeholder="http://localhost:1234" />
              </div>
              <div className="space-y-2">
                <label className={labelClass}>API Key (Optional)</label>
                <input type="password" value={config.llm.apiKey || ''} onChange={e => setConfig({ ...config, llm: { ...config.llm, apiKey: e.target.value } })} className={inputClass} placeholder="sk-..." />
              </div>
            </div>
          )}
          <div className="space-y-2">
            <label className={labelClass}>Model Name</label>
            <div className="flex gap-2">
              <input list="model-list" type="text" value={config.llm.model || ''} onChange={e => setConfig({ ...config, llm: { ...config.llm, model: e.target.value } })} className={inputClass} placeholder={config.llm.provider === 'ollama' ? 'llama3' : 'gpt-3.5-turbo'} />
              <datalist id="model-list">{models.map(m => <option key={m} value={m} />)}</datalist>
              <button onClick={fetchModels} disabled={isLoadingModels} className="shrink-0 flex items-center gap-2 bg-slate-100 hover:bg-slate-200 text-slate-700 px-4 py-2.5 rounded-lg text-sm font-medium transition-colors border border-slate-200">
                <RefreshCw size={16} className={isLoadingModels ? "animate-spin" : ""} /> Refresh
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* Elasticsearch Connection */}
      <div className="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden">
        <div className="p-6 border-b border-slate-100 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="bg-purple-100 p-2 rounded-lg text-purple-600"><Search size={20} /></div>
            <div>
              <h3 className="text-lg font-semibold text-slate-800">Elasticsearch Connection</h3>
              <p className="text-xs text-slate-500 mt-0.5">Used for vector similarity search in the RAG module</p>
            </div>
          </div>
          <button onClick={testElasticsearch} disabled={isTestingEs} className={testBtnClass(esTestResult)}>
            {isTestingEs ? <><RefreshCw size={16} className="animate-spin" /> Testing...</> : esTestResult === 'success' ? <><CheckCircle2 size={16} /> Connected</> : esTestResult === 'error' ? <><AlertCircle size={16} /> Failed</> : 'Test Connection'}
          </button>
        </div>
        <div className="p-6 grid grid-cols-2 gap-6">
          <div className="space-y-2">
            <label className={labelClass}>Host URL</label>
            <input type="text" value={localRag.esHost} onChange={e => setLocalRag({ ...localRag, esHost: e.target.value })} className={inputClass} placeholder="http://localhost:9200" />
          </div>
          <div className="space-y-2">
            <label className={labelClass}>Index Name</label>
            <input type="text" value={localRag.esIndex} onChange={e => setLocalRag({ ...localRag, esIndex: e.target.value })} className={inputClass} placeholder="clicksense_rag" />
          </div>
          <div className="space-y-2">
            <label className={labelClass}>Username</label>
            <input type="text" value={localRag.esUsername} onChange={e => setLocalRag({ ...localRag, esUsername: e.target.value })} className={inputClass} placeholder="elastic" />
          </div>
          <div className="space-y-2">
            <label className={labelClass}>Password</label>
            <input type="password" value={localRag.esPassword} onChange={e => setLocalRag({ ...localRag, esPassword: e.target.value })} className={inputClass} placeholder="••••••••" />
          </div>
          <div className="space-y-2">
            <label className={labelClass}>Top-K Results</label>
            <input type="number" min={1} max={20} value={localRag.topK} onChange={e => setLocalRag({ ...localRag, topK: Number(e.target.value) })} className={inputClass} />
            <p className="text-xs text-slate-400">Number of knowledge fragments returned per query</p>
          </div>
          <div className="space-y-2">
            <label className={labelClass}>Chunk Size (chars)</label>
            <input type="number" min={100} max={2000} value={localRag.chunkSize} onChange={e => setLocalRag({ ...localRag, chunkSize: Number(e.target.value) })} className={inputClass} />
            <p className="text-xs text-slate-400">Size of text chunks when indexing folder content</p>
          </div>
        </div>
      </div>

      {/* Embedding Model */}
      <div className="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden">
        <div className="p-6 border-b border-slate-100 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="bg-indigo-100 p-2 rounded-lg text-indigo-600"><Layers size={20} /></div>
            <div>
              <h3 className="text-lg font-semibold text-slate-800">Embedding Model</h3>
              <p className="text-xs text-slate-500 mt-0.5">Model used to vectorise documents and queries</p>
            </div>
          </div>
          <div className="flex items-center gap-2">
            <button onClick={testEmbedding} disabled={isTestingEmbedding} className={testBtnClass(embeddingTestResult)}>
              {isTestingEmbedding
                ? <><RefreshCw size={16} className="animate-spin" /> Testing...</>
                : embeddingTestResult === 'success'
                  ? <><CheckCircle2 size={16} /> Connected{embeddingTestDims ? ` (${embeddingTestDims}d)` : ''}</>
                  : embeddingTestResult === 'error'
                    ? <><AlertCircle size={16} /> Failed</>
                    : 'Test Embedding'}
            </button>
          </div>
        </div>
        <div className="p-6 grid grid-cols-2 gap-6">
          <div className="space-y-2">
            <label className={labelClass}>Provider</label>
            <select value={localRag.embeddingProvider} onChange={e => setLocalRag({ ...localRag, embeddingProvider: e.target.value as 'ollama' | 'http' | 'huggingface' })} className={inputClass}>
              <option value="ollama">Ollama (Local)</option>
              <option value="http">Custom HTTP (OpenAI Compatible)</option>
              <option value="huggingface">HuggingFace TEI (BGE, E5…)</option>
            </select>
          </div>
          <div className="space-y-2">
            <label className={labelClass}>Server URL</label>
            <input
              type="text"
              value={localRag.embeddingUrl}
              onChange={e => setLocalRag({ ...localRag, embeddingUrl: e.target.value })}
              className={inputClass}
              placeholder={
                localRag.embeddingProvider === 'ollama'
                  ? 'http://localhost:11434'
                  : localRag.embeddingProvider === 'huggingface'
                    ? 'http://localhost:8080'
                    : 'http://localhost:1234'
              }
            />
          </div>
          <div className="space-y-2">
            <label className={labelClass}>Username</label>
            <input type="text" value={localRag.embeddingUsername} onChange={e => setLocalRag({ ...localRag, embeddingUsername: e.target.value })} className={inputClass} placeholder="(optional)" />
          </div>
          <div className="space-y-2">
            <label className={labelClass}>Password</label>
            <input type="password" value={localRag.embeddingPassword} onChange={e => setLocalRag({ ...localRag, embeddingPassword: e.target.value })} className={inputClass} placeholder="••••••••" />
          </div>
          {(localRag.embeddingProvider === 'http' || localRag.embeddingProvider === 'huggingface') && (
            <div className="space-y-2 col-span-2">
              <label className={labelClass}>API Key (optional)</label>
              <input type="password" value={localRag.embeddingApiKey} onChange={e => setLocalRag({ ...localRag, embeddingApiKey: e.target.value })} className={inputClass} placeholder="sk-..." />
            </div>
          )}
          <div className="space-y-2 col-span-2">
            <label className={labelClass}>Embedding Model</label>
            <div className="flex gap-2">
              <input
                list="emb-model-list"
                type="text"
                value={localRag.embeddingModel}
                onChange={e => setLocalRag({ ...localRag, embeddingModel: e.target.value })}
                className={inputClass}
                placeholder={
                  localRag.embeddingProvider === 'ollama'
                    ? 'bge-m3, nomic-embed-text, mxbai-embed-large'
                    : localRag.embeddingProvider === 'huggingface'
                      ? 'BAAI/bge-m3, BAAI/bge-large-en-v1.5'
                      : 'text-embedding-ada-002'
                }
              />
              <datalist id="emb-model-list">{embeddingModels.map(m => <option key={m} value={m} />)}</datalist>
              <button onClick={fetchEmbeddingModels} disabled={isLoadingEmbeddingModels} className="shrink-0 flex items-center gap-2 bg-slate-100 hover:bg-slate-200 text-slate-700 px-4 py-2.5 rounded-lg text-sm font-medium transition-colors border border-slate-200">
                <RefreshCw size={16} className={isLoadingEmbeddingModels ? "animate-spin" : ""} /> Refresh
              </button>
            </div>
            <p className="text-xs text-slate-400">
              {localRag.embeddingProvider === 'huggingface'
                ? 'BGE via HuggingFace TEI: BAAI/bge-m3, BAAI/bge-large-en-v1.5, BAAI/bge-small-en-v1.5'
                : 'Recommended: bge-m3, nomic-embed-text, mxbai-embed-large, all-minilm'}
            </p>
          </div>
        </div>
        <div className="px-6 pb-6 flex justify-end">
          <button onClick={handleSaveRag} disabled={isSavingRag} className="flex items-center gap-2 bg-indigo-600 hover:bg-indigo-700 text-white px-5 py-2.5 rounded-xl font-medium text-sm transition-all shadow-sm disabled:opacity-50">
            {ragSaved ? <><CheckCircle2 size={16} className="text-white" /> Saved</> : <><Save size={16} /> Save RAG Config</>}
          </button>
        </div>
      </div>

      <div className="flex justify-end pt-4">
        <button onClick={handleSave} disabled={isSaving} className="flex items-center gap-2 bg-slate-900 hover:bg-slate-800 text-white px-6 py-3 rounded-xl font-medium transition-all shadow-sm disabled:opacity-50">
          {saved ? <CheckCircle2 size={18} className="text-emerald-400" /> : <Save size={18} />}
          {isSaving ? 'Saving...' : saved ? 'Saved Successfully' : 'Save Configuration'}
        </button>
      </div>
    </div>
  );
}
