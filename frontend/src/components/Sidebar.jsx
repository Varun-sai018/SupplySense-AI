import React from 'react';
import { NavLink } from 'react-router-dom';
import { 
  LayoutDashboard, 
  PlayCircle, 
  Database, 
  GitBranch, 
  TrendingUp, 
  X,
  Workflow
} from 'lucide-react';

const navItems = [
  { to: '/', label: 'Dashboard', icon: LayoutDashboard },
  { to: '/executions', label: 'Executions', icon: PlayCircle },
  { to: '/datasets', label: 'Datasets', icon: Database },
  { to: '/dependencies', label: 'Dependencies', icon: GitBranch },
  { to: '/forecasts', label: 'Forecasts', icon: TrendingUp },
];

export default function Sidebar({ isOpen, onClose }) {
  return (
    <>
      {/* Mobile Backdrop */}
      {isOpen && (
        <div 
          className="fixed inset-0 z-40 bg-slate-950/70 backdrop-blur-sm lg:hidden"
          onClick={onClose}
        />
      )}

      {/* Sidebar container */}
      <aside className={`
        fixed top-0 bottom-0 left-0 z-50 w-64 bg-slate-900 border-r border-slate-800 flex flex-col transition-transform duration-200 ease-in-out
        lg:translate-x-0 lg:static lg:z-0
        ${isOpen ? 'translate-x-0' : '-translate-x-full'}
      `}>
        {/* Top brand section (mobile close visible) */}
        <div className="p-5 border-b border-slate-800 flex items-center justify-between">
          <div className="flex items-center gap-2.5">
            <Workflow className="w-5 h-5 text-sky-400" />
            <span className="font-bold text-sm tracking-wide text-white uppercase">Telemetry Plane</span>
          </div>
          <button 
            onClick={onClose}
            className="lg:hidden p-1.5 rounded-lg text-slate-400 hover:text-white hover:bg-slate-800"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Navigation links */}
        <nav className="flex-1 p-4 space-y-1.5 overflow-y-auto">
          {navItems.map((item) => {
            const Icon = item.icon;
            return (
              <NavLink
                key={item.to}
                to={item.to}
                onClick={() => onClose && onClose()}
                className={({ isActive }) => `
                  flex items-center gap-3 px-3.5 py-2.5 rounded-xl text-xs font-semibold tracking-wide transition-all
                  ${isActive 
                    ? 'bg-sky-500/15 text-sky-400 border border-sky-500/30 shadow-sm shadow-sky-500/10' 
                    : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800/60'}
                `}
              >
                <Icon className="w-4 h-4 flex-shrink-0" />
                <span>{item.label}</span>
              </NavLink>
            );
          })}
        </nav>

        {/* Pipeline Architecture Legend Footer */}
        <div className="p-4 border-t border-slate-800/80 bg-slate-950/40">
          <div className="text-[11px] text-slate-400 font-medium mb-1">Architecture Flow:</div>
          <div className="text-[10px] font-mono text-slate-400 leading-tight flex flex-wrap items-center gap-1">
            <span className="text-sky-400">DATA</span>
            <span>→</span>
            <span className="text-indigo-400">EVENT</span>
            <span>→</span>
            <span className="text-purple-400">DEPENDENCY</span>
            <span>→</span>
            <span className="text-emerald-400">PIPELINE</span>
            <span>→</span>
            <span className="text-amber-400">FORECAST</span>
          </div>
          <p className="mt-3 text-[10px] text-slate-400 leading-normal">
            Phases 1–12 Integrated Capstone
          </p>
        </div>
      </aside>
    </>
  );
}
