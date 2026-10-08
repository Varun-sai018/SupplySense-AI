import React from 'react';
import { Activity, RefreshCw, Radio, Database, Menu } from 'lucide-react';

export default function Navbar({ 
  isHealthy, 
  autoRefresh, 
  toggleAutoRefresh, 
  lastUpdated, 
  onManualRefresh, 
  refreshing,
  toggleSidebar
}) {
  return (
    <header className="sticky top-0 z-40 bg-slate-900/90 backdrop-blur-md border-b border-slate-800 px-4 lg:px-6 py-3 flex items-center justify-between">
      {/* Brand & Subtitle */}
      <div className="flex items-center gap-3">
        <button
          onClick={toggleSidebar}
          className="lg:hidden p-2 rounded-lg text-slate-400 hover:text-white hover:bg-slate-800 transition-colors"
          aria-label="Toggle Navigation"
        >
          <Menu className="w-5 h-5" />
        </button>

        <div className="flex items-center gap-2.5">
          <div className="h-9 w-9 rounded-xl bg-gradient-to-tr from-sky-600 to-indigo-600 flex items-center justify-center shadow-lg shadow-sky-500/20 text-white font-black text-lg">
            S
          </div>
          <div>
            <div className="flex items-center gap-2">
              <h1 className="text-base lg:text-lg font-bold text-white tracking-tight">
                SupplySense AI
              </h1>
              <span className="hidden sm:inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-semibold bg-sky-500/10 text-sky-400 border border-sky-500/20">
                CAPSTONE
              </span>
            </div>
            <p className="text-[11px] text-slate-400 hidden sm:block">
              Event-Conditioned Data Pipeline Orchestrator
            </p>
          </div>
        </div>
      </div>

      {/* Right Controls: Health Badge & Auto Refresh */}
      <div className="flex items-center gap-3">
        {/* Backend Health Badge */}
        <div 
          className={`flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium border transition-colors ${
            isHealthy 
              ? 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20' 
              : 'bg-rose-500/10 text-rose-400 border-rose-500/20'
          }`}
          title={isHealthy ? 'FastAPI & MySQL Connected' : 'FastAPI or MySQL Unreachable'}
        >
          <span className={`w-2 h-2 rounded-full ${isHealthy ? 'bg-emerald-400 animate-pulse' : 'bg-rose-400'}`} />
          <span className="hidden md:inline">{isHealthy ? 'System Healthy' : 'Backend Offline'}</span>
          <span className="md:hidden">{isHealthy ? 'Healthy' : 'Offline'}</span>
        </div>

        {/* Live Auto-Refresh Controller */}
        <div className="flex items-center gap-2 bg-slate-800/80 border border-slate-700/80 rounded-xl px-2.5 py-1">
          <button
            onClick={toggleAutoRefresh}
            className={`flex items-center gap-1.5 text-xs font-semibold px-2 py-0.5 rounded-md transition-all ${
              autoRefresh 
                ? 'bg-sky-500/20 text-sky-400 border border-sky-500/30' 
                : 'text-slate-400 hover:text-slate-200'
            }`}
            title="Toggle 5-second automatic telemetry polling"
          >
            <Radio className={`w-3.5 h-3.5 ${autoRefresh ? 'text-sky-400 animate-pulse' : 'text-slate-500'}`} />
            <span>LIVE</span>
            <span className="text-[10px] uppercase opacity-75">({autoRefresh ? 'ON' : 'OFF'})</span>
          </button>

          {lastUpdated && (
            <span className="text-[11px] text-slate-400 font-mono hidden xl:inline">
              Updated: {lastUpdated}
            </span>
          )}

          <button
            onClick={onManualRefresh}
            disabled={refreshing}
            className={`p-1 text-slate-400 hover:text-white hover:bg-slate-700 rounded-md transition-all ${
              refreshing ? 'animate-spin text-sky-400' : ''
            }`}
            title="Refresh now"
          >
            <RefreshCw className="w-3.5 h-3.5" />
          </button>
        </div>
      </div>
    </header>
  );
}
