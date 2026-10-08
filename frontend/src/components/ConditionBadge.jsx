import React from 'react';

/**
 * Condition badge for ALL / ANY / QUORUM dependency condition types.
 */
export default function ConditionBadge({ condition }) {
  if (!condition) return null;

  const normalized = String(condition).toUpperCase();

  let style = 'bg-slate-700/50 text-slate-300 border-slate-600';
  let desc = '';

  if (normalized === 'ALL') {
    style = 'bg-indigo-500/10 text-indigo-400 border-indigo-500/20';
    desc = 'All dependencies required';
  } else if (normalized === 'ANY') {
    style = 'bg-purple-500/10 text-purple-400 border-purple-500/20';
    desc = 'Any 1 dependency required';
  } else if (normalized === 'QUORUM') {
    style = 'bg-cyan-500/10 text-cyan-400 border-cyan-500/20';
    desc = 'Quorum threshold required';
  }

  return (
    <span 
      title={desc}
      className={`inline-flex items-center px-2.5 py-1 rounded-md text-xs font-semibold tracking-wide border ${style}`}
    >
      {normalized}
    </span>
  );
}
