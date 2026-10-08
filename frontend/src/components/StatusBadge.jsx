import React from 'react';

/**
 * Semantic status badge for pipeline executions, datasets, and health states.
 * Centralizes all semantic status colors across the application.
 */
export default function StatusBadge({ status, size = 'md' }) {
  if (!status) return null;

  const normalized = String(status).toUpperCase();

  let style = 'bg-slate-700 text-slate-300 border-slate-600';
  let dotColor = 'bg-slate-400';

  if (['COMPLETED', 'READY', 'TRIGGER', 'HEALTHY', 'CONNECTED'].includes(normalized)) {
    style = 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20';
    dotColor = 'bg-emerald-400';
  } else if (['RUNNING', 'WAITING'].includes(normalized)) {
    style = 'bg-sky-500/10 text-sky-400 border-sky-500/20';
    dotColor = 'bg-sky-400 animate-pulse';
  } else if (['BLOCK', 'RETRYING'].includes(normalized)) {
    style = 'bg-amber-500/10 text-amber-400 border-amber-500/20';
    dotColor = 'bg-amber-400';
  } else if (['FAILED', 'UNHEALTHY', 'DISCONNECTED', 'ERROR'].includes(normalized)) {
    style = 'bg-rose-500/10 text-rose-400 border-rose-500/20';
    dotColor = 'bg-rose-400';
  }

  const sizeClasses = size === 'sm' 
    ? 'px-2 py-0.5 text-xs' 
    : 'px-2.5 py-1 text-xs font-medium';

  return (
    <span className={`inline-flex items-center gap-1.5 rounded-full border ${sizeClasses} ${style}`}>
      <span className={`h-1.5 w-1.5 rounded-full ${dotColor}`} />
      <span>{normalized}</span>
    </span>
  );
}
