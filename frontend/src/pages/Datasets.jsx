import React, { useState, useEffect } from 'react';
import { Database, Clock, RefreshCw, AlertCircle, Info, Hash, ArrowUpDown } from 'lucide-react';
import { getDatasets } from '../api/client';
import StatusBadge from '../components/StatusBadge';
import LoadingSpinner from '../components/LoadingSpinner';
import EmptyState from '../components/EmptyState';

export default function Datasets({ refreshTrigger }) {
  const [datasets, setDatasets] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    let isMounted = true;
    setLoading(true);

    getDatasets()
      .then((data) => {
        if (isMounted) {
          setDatasets(data || []);
          setError(null);
        }
      })
      .catch((err) => {
        if (isMounted) {
          setError(err.response?.data?.detail || err.message || 'Failed to retrieve dataset statuses');
        }
      })
      .finally(() => {
        if (isMounted) setLoading(false);
      });

    return () => {
      isMounted = false;
    };
  }, [refreshTrigger]);

  return (
    <div className="space-y-6">
      {/* Page Header */}
      <div>
        <h2 className="text-2xl font-bold text-white tracking-tight">Dataset Synchronization & CDC State</h2>
        <p className="text-xs sm:text-sm text-slate-400 mt-1">
          Tracking upstream data arrivals, Debezium CDC change streams, micro-batches, and version state across tables.
        </p>
      </div>

      {/* Explanatory Banner */}
      <div className="bg-sky-500/10 border border-sky-500/20 rounded-xl p-4 flex items-start gap-3 text-sky-200">
        <Info className="w-5 h-5 flex-shrink-0 text-sky-400 mt-0.5" />
        <div className="text-xs leading-relaxed">
          <span className="font-semibold text-sky-300">Core Architecture Principle: </span>
          Dataset readiness is determined by the event-conditioned dependency engine. Datasets switch to{' '}
          <span className="font-mono text-emerald-300 font-bold">READY</span> upon receiving validated ingestion events, and automatically reset to{' '}
          <span className="font-mono text-sky-300 font-bold">WAITING</span> when downstream pipeline executions are triggered.
        </div>
      </div>

      {/* Error Banner */}
      {error && (
        <div className="p-4 rounded-xl bg-rose-500/10 border border-rose-500/20 text-rose-300 flex items-center gap-3">
          <AlertCircle className="w-5 h-5 flex-shrink-0 text-rose-400" />
          <p className="text-sm">{error}</p>
        </div>
      )}

      {/* Dataset Cards Grid */}
      {loading ? (
        <LoadingSpinner message="Auditing dataset versions and CDC events..." />
      ) : datasets.length === 0 ? (
        <EmptyState title="No datasets configured" description="No dataset metadata records found in MySQL." />
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-5">
          {datasets.map((d) => (
            <div 
              key={d.dataset_id}
              className="bg-slate-800/80 border border-slate-700/70 rounded-2xl p-5 flex flex-col justify-between shadow-sm hover:border-slate-600 transition-all"
            >
              <div>
                {/* Header */}
                <div className="flex items-center justify-between pb-3 border-b border-slate-700/60">
                  <div className="flex items-center gap-2.5">
                    <div className="p-2 bg-indigo-500/10 border border-indigo-500/20 rounded-lg text-indigo-400">
                      <Database className="w-4 h-4" />
                    </div>
                    <div>
                      <h3 className="text-base font-bold text-white">{d.dataset_name}</h3>
                      <span className="font-mono text-[11px] text-slate-400">{d.table_name}</span>
                    </div>
                  </div>
                  <StatusBadge status={d.status} size="sm" />
                </div>

                {/* Metrics */}
                <div className="mt-4 grid grid-cols-2 gap-3">
                  <div className="bg-slate-900/60 p-2.5 rounded-xl border border-slate-800 text-xs">
                    <span className="text-[11px] text-slate-400 block">Version</span>
                    <span className="font-mono text-base font-bold text-sky-400 mt-0.5 block">
                      v{d.current_version}
                    </span>
                  </div>
                  <div className="bg-slate-900/60 p-2.5 rounded-xl border border-slate-800 text-xs">
                    <span className="text-[11px] text-slate-400 block">Total Rows</span>
                    <span className="font-mono text-base font-bold text-slate-200 mt-0.5 block">
                      {d.row_count ? d.row_count.toLocaleString() : '0'}
                    </span>
                  </div>
                </div>

                {/* Metadata Details */}
                <div className="mt-4 space-y-2 text-xs border-t border-slate-700/40 pt-3">
                  <div className="flex justify-between items-center text-slate-300">
                    <span className="text-slate-400">Source Type:</span>
                    <span className="font-semibold px-2 py-0.5 rounded bg-slate-700/50 text-slate-200 text-[11px]">
                      {d.source_type}
                    </span>
                  </div>
                  <div className="flex justify-between items-center text-slate-300">
                    <span className="text-slate-400">Last Updated:</span>
                    <span className="font-mono text-slate-300 text-[11px]">
                      {d.last_updated_at || '—'}
                    </span>
                  </div>
                </div>

                {/* Latest Event Stream Details */}
                <div className="mt-3 p-3 bg-slate-900/80 rounded-xl border border-slate-800 text-xs space-y-1.5">
                  <div className="flex items-center justify-between text-slate-400 text-[11px] font-semibold uppercase tracking-wider">
                    <span>Latest Ingested Event</span>
                    {d.latest_event_id && (
                      <span className="text-sky-400 font-mono">#{d.latest_event_id}</span>
                    )}
                  </div>
                  <div className="flex justify-between text-slate-300">
                    <span className="text-slate-400">Event Type:</span>
                    <span className="font-mono text-slate-200">{d.latest_event_type || '—'}</span>
                  </div>
                  <div className="flex justify-between text-slate-300">
                    <span className="text-slate-400">Rows Changed:</span>
                    <span className="font-mono font-bold text-emerald-400">
                      {d.latest_rows_changed !== null ? `+${d.latest_rows_changed}` : '—'}
                    </span>
                  </div>
                  {d.latest_batch_id && (
                    <div className="flex justify-between text-slate-300">
                      <span className="text-slate-400">Micro-Batch ID:</span>
                      <span className="font-mono text-slate-300 text-[10px] truncate max-w-[150px]">
                        {d.latest_batch_id}
                      </span>
                    </div>
                  )}
                </div>
              </div>

              {/* Status Footer */}
              <div className="mt-4 pt-3 border-t border-slate-700/50 flex items-center justify-between text-[11px] text-slate-400">
                <span>Evaluation State</span>
                <span className={`font-semibold ${d.status === 'READY' ? 'text-emerald-400' : 'text-sky-400'}`}>
                  {d.status === 'READY' ? '● Satisfies Dependency' : '○ Awaiting Next Batch'}
                </span>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
