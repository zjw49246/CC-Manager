import { useState } from 'react';
import { setToken } from '../api/client';
import { getApiBase, getServerUrl, setServerUrl } from '../config/server';
import { ChevronDown, ChevronRight, Bot } from '../components/icons';

interface LoginPageProps {
  onLogin: () => void;
}

export function LoginPage({ onLogin }: LoginPageProps) {
  const [mode, setMode] = useState<'token' | 'email' | 'register'>('email');
  const [token, setTokenValue] = useState('');
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [name, setName] = useState('');
  const [code, setCode] = useState('');
  const [bootstrapToken, setBootstrapToken] = useState('');
  const [codeSent, setCodeSent] = useState(false);
  const [codeCooldown, setCodeCooldown] = useState(0);
  const [serverUrl, setServerUrlValue] = useState(getServerUrl());
  const [showServer, setShowServer] = useState(false);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);

  const base = () => serverUrl.replace(/\/+$/, '') || getApiBase();

  const handleSendCode = async () => {
    if (!email || codeCooldown > 0) return;
    setError('');
    try {
      const res = await fetch(`${base()}/api/auth/send-code`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email }),
      });
      if (res.ok) {
        setCodeSent(true);
        setCodeCooldown(60);
        const timer = setInterval(() => {
          setCodeCooldown(prev => {
            if (prev <= 1) { clearInterval(timer); return 0; }
            return prev - 1;
          });
        }, 1000);
      } else {
        const data = await res.json();
        setError(data.detail || 'Failed to send code');
      }
    } catch {
      setError('Connection failed');
    }
  };

  const handleTokenLogin = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setError('');
    if (serverUrl !== getServerUrl()) setServerUrl(serverUrl);
    try {
      const res = await fetch(`${base()}/api/auth/login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ token }),
      });
      if (res.ok) {
        setToken(token);
        // Load user info for token login
        try {
          const meRes = await fetch(`${base()}/api/auth/me`, {
            headers: { 'Authorization': `Bearer ${token}` },
          });
          if (meRes.ok) {
            const meData = await meRes.json();
            if (meData.user) {
              localStorage.setItem('cc_user', JSON.stringify(meData.user));
            } else if (meData.role) {
              localStorage.setItem('cc_user', JSON.stringify({
                name: meData.auth_type === 'none' ? 'Local Admin' : 'Admin',
                role: meData.role,
              }));
            }
          }
        } catch { /* user profile enrichment is optional */ }
        onLogin();
      } else {
        setError('Invalid token');
      }
    } catch {
      setError('Connection failed');
    } finally {
      setLoading(false);
    }
  };

  const handleEmailLogin = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setError('');
    if (serverUrl !== getServerUrl()) setServerUrl(serverUrl);
    try {
      const res = await fetch(`${base()}/api/auth/login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email, password }),
      });
      const data = await res.json();
      if (res.ok && data.token) {
        setToken(data.token);
        localStorage.setItem('cc_user', JSON.stringify(data.user));
        onLogin();
      } else {
        setError(data.detail || 'Login failed');
      }
    } catch {
      setError('Connection failed');
    } finally {
      setLoading(false);
    }
  };

  const handleRegister = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setError('');
    if (serverUrl !== getServerUrl()) setServerUrl(serverUrl);
    try {
      const res = await fetch(`${base()}/api/auth/register`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          email,
          name,
          password,
          code,
          bootstrap_token: bootstrapToken,
        }),
      });
      const data = await res.json();
      if (res.ok && data.token) {
        setToken(data.token);
        localStorage.setItem('cc_user', JSON.stringify(data.user));
        onLogin();
      } else {
        setError(data.detail || 'Registration failed');
      }
    } catch {
      setError('Connection failed');
    } finally {
      setLoading(false);
    }
  };

  const inputCls = 'w-full bg-gray-700 text-foreground rounded-lg px-3 py-2.5 text-sm border border-gray-600/60 placeholder-gray-500 focus:outline-none focus:border-indigo-500 focus:ring-2 focus:ring-indigo-500/25 transition-colors';
  const primaryBtnCls = 'w-full bg-indigo-600 hover:bg-indigo-500 text-white py-2.5 rounded-lg text-sm font-medium shadow-lg shadow-indigo-600/20 transition-colors disabled:opacity-50';

  return (
    <div className="relative min-h-screen bg-gray-950 flex items-center justify-center p-4 overflow-hidden">
      {/* 品牌辉光背景 */}
      <div className="pointer-events-none absolute -top-32 left-1/2 -translate-x-1/2 h-96 w-[36rem] rounded-full bg-indigo-600/15 blur-3xl" />
      <div className="relative w-full max-w-sm space-y-5">
        <div className="flex flex-col items-center gap-3">
          <div className="flex h-12 w-12 items-center justify-center rounded-2xl bg-gradient-to-br from-indigo-500 to-indigo-700 text-white shadow-lg shadow-indigo-600/30">
            <Bot size={26} />
          </div>
          <div className="text-center">
            <h1 className="text-foreground text-xl font-semibold tracking-tight">Claude Code Manager</h1>
            <p className="text-gray-500 text-xs mt-1">调度并行 Claude Code 实例的控制台</p>
          </div>
        </div>
        <div className="bg-gray-800 border border-gray-700/70 rounded-2xl p-6 space-y-4 shadow-xl shadow-black/10">

        {/* Mode tabs */}
        <div className="flex gap-1 bg-gray-900/50 rounded-lg p-1">
          {([['email', 'Email'], ['token', 'Token'], ['register', 'Register']] as const).map(([key, label]) => (
            <button
              key={key}
              type="button"
              onClick={() => { setMode(key); setError(''); }}
              className={`flex-1 py-1.5 text-xs font-medium rounded-md transition-colors ${
                mode === key ? 'bg-indigo-600 text-white shadow-sm' : 'text-gray-400 hover:text-gray-300'
              }`}
            >
              {label}
            </button>
          ))}
        </div>

        {/* Token login */}
        {mode === 'token' && (
          <form onSubmit={handleTokenLogin} className="space-y-3">
            <input
              type="password"
              className={inputCls}
              placeholder="Access Token"
              value={token}
              onChange={(e) => setTokenValue(e.target.value)}
              autoFocus
              required
            />
            <button type="submit" disabled={loading}
              className={primaryBtnCls}>
              {loading ? 'Verifying...' : 'Login'}
            </button>
          </form>
        )}

        {/* Email login */}
        {mode === 'email' && (
          <form onSubmit={handleEmailLogin} className="space-y-3">
            <input
              type="email"
              className={inputCls}
              placeholder="Email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              autoFocus
              required
            />
            <input
              type="password"
              className={inputCls}
              placeholder="Password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              required
            />
            <button type="submit" disabled={loading}
              className={primaryBtnCls}>
              {loading ? 'Logging in...' : 'Login'}
            </button>
          </form>
        )}

        {/* Register */}
        {mode === 'register' && (
          <form onSubmit={handleRegister} className="space-y-3">
            <input
              type="text"
              className={inputCls}
              placeholder="Name"
              value={name}
              onChange={(e) => setName(e.target.value)}
              autoFocus
              required
            />
            <div className="flex gap-2">
              <input
                type="email"
                className={`${inputCls} flex-1`}
                placeholder="Email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                required
              />
              <button
                type="button"
                onClick={handleSendCode}
                disabled={!email || codeCooldown > 0}
                className="shrink-0 bg-gray-600 hover:bg-gray-500 text-foreground px-3 py-2 rounded-lg text-xs font-medium disabled:opacity-50 whitespace-nowrap transition-colors"
              >
                {codeCooldown > 0 ? `${codeCooldown}s` : codeSent ? 'Resend' : 'Send Code'}
              </button>
            </div>
            <input
              type="text"
              className={inputCls}
              placeholder="Verification Code"
              value={code}
              onChange={(e) => setCode(e.target.value)}
              required
            />
            <input
              type="password"
              className={inputCls}
              placeholder="Password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              required
            />
            <input
              type="password"
              className={inputCls}
              placeholder="Bootstrap Token (first admin only)"
              value={bootstrapToken}
              onChange={(e) => setBootstrapToken(e.target.value)}
              autoComplete="off"
            />
            <button type="submit" disabled={loading || !codeSent}
              className={primaryBtnCls}>
              {loading ? 'Registering...' : 'Register'}
            </button>
          </form>
        )}

        {/* Server URL toggle */}
        <div>
          <button
            type="button"
            onClick={() => setShowServer(!showServer)}
            className="flex items-center gap-1 text-xs text-gray-500 hover:text-gray-300 transition-colors"
          >
            {showServer ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
            Server URL
          </button>
          {showServer && (
            <input
              type="url"
              className={`${inputCls} mt-1`}
              placeholder="https://your-server.com"
              value={serverUrl}
              onChange={(e) => setServerUrlValue(e.target.value)}
            />
          )}
        </div>

        {error && <p className="text-red-400 text-xs text-center">{error}</p>}
        </div>
      </div>
    </div>
  );
}
