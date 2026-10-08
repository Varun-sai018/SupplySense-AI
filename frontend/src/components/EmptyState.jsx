import React from 'react';
import { Database } from 'lucide-react';

export default function EmptyState({ 
  icon: Icon = Database, 
  title = 'No records found', 
  description = 'No operational data is currently available for this view.' 
}) {
  return (
    <div className="flex flex-col items-center justify-center py-16 px-4 text-center rounded-xl border border-dashed border-slate-700 bg-slate-800/20">
      <div className="p-3 bg-slate-800 rounded-xl text-slate-400 mb-3 border border-slate-700/60">
        <Icon className="w-6 h-6" />
      </div>
      <h3 className="text-base font-semibold text-slate-200">{title}</h3>
      <p className="mt-1 text-sm text-slate-400 max-w-sm">{description}</p>
    </div>
  );
}
