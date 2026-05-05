'use client';

import { useState, useEffect } from 'react';

export default function LumeStudio() {
  const [query, setQuery] = useState('{\n  "action": "stats"\n}');
  const [result, setResult] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [collections, setCollections] = useState<any[]>([]);
  const [status, setStatus] = useState<'connected' | 'disconnected'>('disconnected');
  const [host, setHost] = useState('127.0.0.1');
  const [port, setPort] = useState('7070');
  const [username, setUsername] = useState('admin');
  const [password, setPassword] = useState('password');
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [authError, setAuthError] = useState('');

  // Ping the server to check status and get collections on load
  useEffect(() => {
    checkConnection();
  }, []);

  const checkConnection = async () => {
    try {
      // 1. Ping (and Authenticate)
      const pingRes = await fetch('/api/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action: 'ping', host, port, username, password })
      });
      const pingData = await pingRes.json();
      
      if (pingData.status === 'ok') {
        setStatus('connected');
        setIsAuthenticated(true);
        setAuthError('');
        
        // 2. Fetch collections
        const colRes = await fetch('/api/query', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ action: 'listCollections', host, port, username, password })
        });
        const colData = await colRes.json();
        if (colData.collections) {
          setCollections(colData.collections);
        }
      } else {
        if (pingData.error?.includes('Unauthorized') || pingData.error?.includes('credentials')) {
            setIsAuthenticated(false);
            setAuthError(pingData.error);
        } else {
            setStatus('disconnected');
            setIsAuthenticated(false);
        }
      }
    } catch (e) {
      setStatus('disconnected');
      setIsAuthenticated(false);
    }
  };

  const executeQuery = async () => {
    try {
      setLoading(true);
      const parsedQuery = JSON.parse(query);
      
      const res = await fetch('/api/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ...parsedQuery, host, port, username, password })
      });
      
      const data = await res.json();
      setResult(data);
      
      // If we created a collection or index, refresh collections
      if (parsedQuery.action === 'insert' || parsedQuery.action === 'createCollection') {
        checkConnection();
      }
    } catch (err: any) {
      setResult({ status: 'error', error: err.message || 'Invalid JSON format' });
    } finally {
      setLoading(false);
    }
  };

  const syntaxHighlight = (json: any) => {
    if (!json) return '';
    let str = JSON.stringify(json, null, 2);
    str = str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    return str.replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g, function (match) {
        let cls = 'number';
        if (/^"/.test(match)) {
            if (/:$/.test(match)) {
                cls = 'key-string';
            } else {
                cls = 'string';
            }
        } else if (/true|false/.test(match)) {
            cls = 'boolean';
        } else if (/null/.test(match)) {
            cls = 'null';
        }
        return '<span class="' + cls + '">' + match + '</span>';
    });
  }

  const loadExample = (type: string) => {
    const examples: Record<string, string> = {
      'insert': '{\n  "action": "insert",\n  "collection": "users",\n  "document": {\n    "name": "Alice",\n    "role": "admin"\n  }\n}',
      'find': '{\n  "action": "find",\n  "collection": "users",\n  "query": {\n    "role": "admin"\n  }\n}',
      'stats': '{\n  "action": "stats"\n}',
    };
    setQuery(examples[type] || '');
  };

  if (!isAuthenticated && status !== 'connected') {
    return (
      <div className="container" style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100vh' }}>
        <div className="panel animated" style={{ width: '400px', padding: '2rem' }}>
          <div className="logo" style={{ justifyContent: 'center', marginBottom: '2rem', fontSize: '2rem' }}>
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" style={{width: '36px', height: '36px'}}>
              <path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"></path>
              <polyline points="3.27 6.96 12 12.01 20.73 6.96"></polyline>
              <line x1="12" y1="22.08" x2="12" y2="12"></line>
            </svg>
            LumeDB
          </div>
          <h3 style={{ textAlign: 'center', marginBottom: '1.5rem', color: 'var(--text-secondary)' }}>Login to Studio</h3>
          
          {authError && <div style={{ color: 'var(--error)', background: 'rgba(239, 68, 68, 0.1)', padding: '0.75rem', borderRadius: '8px', marginBottom: '1rem', fontSize: '0.85rem', textAlign: 'center' }}>{authError}</div>}
          
          <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
            <input 
              type="text" 
              placeholder="Username" 
              value={username}
              onChange={e => setUsername(e.target.value)}
              style={{ background: 'var(--bg-secondary)', border: '1px solid var(--border)', color: 'white', padding: '0.8rem', borderRadius: '8px', width: '100%' }}
            />
            <input 
              type="password" 
              placeholder="Password" 
              value={password}
              onChange={e => setPassword(e.target.value)}
              style={{ background: 'var(--bg-secondary)', border: '1px solid var(--border)', color: 'white', padding: '0.8rem', borderRadius: '8px', width: '100%' }}
            />
            <div style={{ display: 'flex', gap: '0.5rem' }}>
              <input type="text" value={host} onChange={e => setHost(e.target.value)} placeholder="Host" style={{ background: 'var(--bg-secondary)', border: '1px solid var(--border)', color: 'white', padding: '0.8rem', borderRadius: '8px', width: '70%' }} />
              <input type="text" value={port} onChange={e => setPort(e.target.value)} placeholder="Port" style={{ background: 'var(--bg-secondary)', border: '1px solid var(--border)', color: 'white', padding: '0.8rem', borderRadius: '8px', width: '30%' }} />
            </div>
            <button className="btn" onClick={checkConnection} style={{ width: '100%', marginTop: '0.5rem', padding: '1rem' }}>
              Connect & Authenticate
            </button>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="container">
      <header>
        <div className="logo">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" style={{width: '28px', height: '28px'}}>
            <path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"></path>
            <polyline points="3.27 6.96 12 12.01 20.73 6.96"></polyline>
            <line x1="12" y1="22.08" x2="12" y2="12"></line>
          </svg>
          LumeDB Studio
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
          <input 
            type="text" 
            value={host} 
            onChange={e => setHost(e.target.value)} 
            placeholder="Host (e.g. 127.0.0.1)"
            style={{ background: 'var(--bg-secondary)', border: '1px solid var(--border)', color: 'white', padding: '0.4rem 0.8rem', borderRadius: '6px', fontSize: '0.8rem', width: '130px' }}
          />
          <span style={{ color: 'var(--text-secondary)' }}>:</span>
          <input 
            type="text" 
            value={port} 
            onChange={e => setPort(e.target.value)} 
            placeholder="Port"
            style={{ background: 'var(--bg-secondary)', border: '1px solid var(--border)', color: 'white', padding: '0.4rem 0.8rem', borderRadius: '6px', fontSize: '0.8rem', width: '70px' }}
          />
          <button 
            onClick={checkConnection}
            style={{ background: 'transparent', border: '1px solid var(--border)', color: 'white', padding: '0.4rem 0.8rem', borderRadius: '6px', cursor: 'pointer', fontSize: '0.8rem', marginLeft: '0.5rem' }}
          >
            Connect
          </button>
          <button 
            onClick={() => setIsAuthenticated(false)}
            style={{ background: 'rgba(239, 68, 68, 0.1)', border: '1px solid rgba(239, 68, 68, 0.2)', color: 'var(--error)', padding: '0.4rem 0.8rem', borderRadius: '6px', cursor: 'pointer', fontSize: '0.8rem', marginLeft: '0.5rem' }}
          >
            Logout
          </button>
          <div className={`status-badge ${status}`} style={{ marginLeft: '0.5rem' }}>
            <div className="status-dot"></div>
            {status === 'connected' ? 'Connected' : 'Disconnected'}
          </div>
        </div>
      </header>

      <div className="main-grid">
        {/* Sidebar */}
        <div className="panel animated" style={{ animationDelay: '0.1s' }}>
          <div className="panel-header">Database Schema</div>
          <div className="panel-body">
            <h4 style={{ color: 'var(--text-primary)', marginBottom: '0.5rem', fontSize: '0.9rem' }}>Collections</h4>
            {collections.length === 0 ? (
              <p style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>No collections found. Run an insert to create one.</p>
            ) : (
              <ul className="collection-list">
                {collections.map(c => (
                  <li key={c.name} className="collection-item" onClick={() => loadExample('find')}>
                    <span className="collection-name">{c.name}</span>
                    <span className="collection-badge">{c.docCount} docs</span>
                  </li>
                ))}
              </ul>
            )}

            <h4 style={{ color: 'var(--text-primary)', margin: '1.5rem 0 0.5rem 0', fontSize: '0.9rem' }}>Quick Snippets</h4>
            <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap' }}>
              <button onClick={() => loadExample('insert')} style={{ background: 'rgba(255,255,255,0.05)', border: '1px solid var(--border)', color: 'var(--text-secondary)', padding: '4px 8px', borderRadius: '4px', cursor: 'pointer', fontSize: '0.8rem' }}>Insert</button>
              <button onClick={() => loadExample('find')} style={{ background: 'rgba(255,255,255,0.05)', border: '1px solid var(--border)', color: 'var(--text-secondary)', padding: '4px 8px', borderRadius: '4px', cursor: 'pointer', fontSize: '0.8rem' }}>Find</button>
              <button onClick={() => loadExample('stats')} style={{ background: 'rgba(255,255,255,0.05)', border: '1px solid var(--border)', color: 'var(--text-secondary)', padding: '4px 8px', borderRadius: '4px', cursor: 'pointer', fontSize: '0.8rem' }}>Stats</button>
            </div>
          </div>
        </div>

        {/* Main Editor */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: '2rem', minHeight: 0 }}>
          <div className="panel animated" style={{ animationDelay: '0.2s', flex: '0 0 auto' }}>
            <div className="panel-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <span>Query Editor (JSON)</span>
              <button 
                className="btn" 
                onClick={executeQuery}
                disabled={loading}
                style={{ padding: '0.4rem 1rem', fontSize: '0.8rem' }}
              >
                {loading ? 'Running...' : 'Run Query ▶'}
              </button>
            </div>
            <div className="panel-body" style={{ padding: 0 }}>
              <textarea 
                className="query-editor" 
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                spellCheck={false}
                style={{ border: 'none', borderRadius: 0, minHeight: '200px' }}
              />
            </div>
          </div>

          <div className="panel animated" style={{ animationDelay: '0.3s', flex: 1 }}>
            <div className="panel-header">Results</div>
            <div className="panel-body" style={{ padding: 0, background: 'var(--bg-secondary)' }}>
              {result ? (
                <div 
                  className="result-view" 
                  dangerouslySetInnerHTML={{ __html: syntaxHighlight(result) }}
                />
              ) : (
                <div style={{ padding: '2rem', color: 'var(--text-secondary)', fontSize: '0.9rem', textAlign: 'center', opacity: 0.5 }}>
                  Run a query to see results
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
