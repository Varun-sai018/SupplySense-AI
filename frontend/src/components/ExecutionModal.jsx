import React, { useEffect, useState } from 'react';
import { X, Clock, PlayCircle, CheckCircle2, AlertTriangle, Layers, FileCode2, Cpu, RefreshCw, AlertCircle } from 'lucide-react';
import { getExecution } from '../api/client';
import StatusBadge from './StatusBadge';
import ConditionBadge from './ConditionBadge';
import LoadingSpinner from './LoadingSpinner';

export default function ExecutionModal({ executionId, onClose }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    let isMounted = true;
    if (!executionId) return;

    setLoading(true);
    setError(null);

    getExecution(executionId)
      .then((res) => {
        if (isMounted) setData(res);
      })
      .catch((err) => {
        if (isMounted) {
          setError(err.response?.data?.detail || 'Failed to load execution telemetry');
        }
      })
      .finally(() => {
        if (isMounted) setLoading(false);
      });

    return () => {
      isMounted = false;
    };
  }, [executionId]);

  if (!executionId) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-slate-950/80 backdrop-blur-sm animate-in fade-in duration-150">
      <div 
        className="bg-slate-900 border border-slate-700/80 rounded-2xl w-full max-w-3xl max-h-[90vh] flex flex-col shadow-2xl overflow-hidden"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Modal Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-slate-800 bg-slate-800/50">
          <div className="flex items-center gap-3">
            <div className="p-2 bg-sky-500/10 border border-sky-500/20 rounded-lg text-sky-400">
              <PlayCircle className="w-5 h-5" />
            </div>
            <div>
              <div className="flex items-center gap-2">
                <h2 className="text-lg font-bold text-white">Execution #{executionId}</h2>
                {data && <StatusBadge status={data.status} size="sm" />}
              </div>
              <p className="text-xs text-slate-400">
                {data?.pipeline_name || 'Pipeline Execution Telemetry Audit'}
              </p>
            </div>
          </div>
          <button
            onClick={onClose}
            className="p-1.5 rounded-lg text-slate-400 hover:text-white hover:bg-slate-800 transition-colors"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Modal Body */}
        <div className="overflow-y-auto p-6 space-y-6">
          {loading && <LoadingSpinner message="Auditing execution telemetry & artifacts..." />}

          {error && (
            <div className="p-4 rounded-xl border border-rose-500/20 bg-rose-500/10 text-rose-300 flex items-start gap-3">
              <AlertCircle className="w-5 h-5 flex-shrink-0 mt-0.5 text-rose-400" />
              <div>
                <h4 className="text-sm font-semibold">Execution Audit Failed</h4>
                <p className="text-xs text-rose-200/80 mt-1">{error}</p>
              </div>
            </div>
          )}

          {data && !loading && (
            <>
              {/* Core Execution Summary Grid */}
              <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
                <div className="bg-slate-800/60 p-3.5 rounded-xl border border-slate-700/50">
                  <span className="text-[11px] font-medium text-slate-400 uppercase tracking-wider block">Duration</span>
                  <span className="text-base font-bold text-white mt-1 block">
                    {data.duration_seconds !== null ? `${data.duration_seconds}s` : '—'}
                  </span>
                </div>
                <div className="bg-slate-800/60 p-3.5 rounded-xl border border-slate-700/50">
                  <span className="text-[11px] font-medium text-slate-400 uppercase tracking-wider block">Retries</span>
                  <span className="text-base font-bold text-white mt-1 block">
                    {data.retry_count} / {data.max_retries}
                  </span>
                </div>
                <div className="bg-slate-800/60 p-3.5 rounded-xl border border-slate-700/50">
                  <span className="text-[11px] font-medium text-slate-400 uppercase tracking-wider block">Trigger Event</span>
                  <span className="text-base font-bold text-sky-400 mt-1 block">
                    {data.triggering_event_id ? `#${data.triggering_event_id}` : 'None'}
                  </span>
                </div>
                <div className="bg-slate-800/60 p-3.5 rounded-xl border border-slate-700/50">
                  <span className="text-[11px] font-medium text-slate-400 uppercase tracking-wider block">Decision ID</span>
                  <span className="text-base font-bold text-indigo-400 mt-1 block">
                    {data.decision_id ? `#${data.decision_id}` : '—'}
                  </span>
                </div>
              </div>

              {/* Timing Metadata */}
              <div className="bg-slate-800/40 border border-slate-700/40 rounded-xl p-4 text-xs space-y-2">
                <div className="flex justify-between items-center text-slate-300">
                  <span className="text-slate-400 flex items-center gap-1.5">
                    <Clock className="w-3.5 h-3.5 text-slate-500" /> Started At:
                  </span>
                  <span className="font-mono text-slate-200">{data.started_at || '—'}</span>
                </div>
                <div className="flex justify-between items-center text-slate-300">
                  <span className="text-slate-400 flex items-center gap-1.5">
                    <Clock className="w-3.5 h-3.5 text-slate-500" /> Completed At:
                  </span>
                  <span className="font-mono text-slate-200">{data.completed_at || '—'}</span>
                </div>
                {data.next_retry_at && (
                  <div className="flex justify-between items-center text-amber-300">
                    <span className="text-amber-400 flex items-center gap-1.5">
                      <RefreshCw className="w-3.5 h-3.5" /> Next Retry Scheduled:
                    </span>
                    <span className="font-mono">{data.next_retry_at}</span>
                  </div>
                )}
                {data.retry_error_type && (
                  <div className="flex justify-between items-center text-rose-300">
                    <span className="text-rose-400 flex items-center gap-1.5">
                      <AlertTriangle className="w-3.5 h-3.5" /> Error Classification:
                    </span>
                    <span className="font-mono font-semibold">{data.retry_error_type}</span>
                  </div>
                )}
                {data.error_message && (
                  <div className="mt-2 pt-2 border-t border-slate-700/60">
                    <span className="text-rose-400 font-semibold block mb-1">Failure Trace / Message:</span>
                    <pre className="p-2.5 bg-slate-950 rounded-lg text-rose-300 text-xs font-mono overflow-x-auto whitespace-pre-wrap">
                      {data.error_message}
                    </pre>
                  </div>
                )}
              </div>

              {/* Section 1: Triggering Dependency Decision */}
              {data.decision && (
                <div className="bg-slate-800/40 border border-slate-700/60 rounded-xl p-4">
                  <div className="flex items-center justify-between mb-3">
                    <div className="flex items-center gap-2">
                      <Layers className="w-4 h-4 text-indigo-400" />
                      <h3 className="text-sm font-semibold text-white">Dependency Engine Evaluation</h3>
                    </div>
                    <div className="flex items-center gap-2">
                      <ConditionBadge condition={data.decision.condition_type} />
                      <StatusBadge status={data.decision.decision} size="sm" />
                    </div>
                  </div>
                  <div className="bg-slate-900/60 rounded-lg p-3 border border-slate-800 text-xs space-y-1.5">
                    <div className="flex justify-between">
                      <span className="text-slate-400">Condition Rule:</span>
                      <span className="font-semibold text-slate-200">{data.decision.condition_type}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-slate-400">Satisfied Dependencies:</span>
                      <span className="font-semibold text-slate-200">
                        {data.decision.ready_count} / {data.decision.total_required} READY
                      </span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-slate-400">Evaluation Reason:</span>
                      <span className="font-semibold text-sky-300">{data.decision.reason}</span>
                    </div>
                  </div>
                </div>
              )}

              {/* Section 2: Machine Learning Forecast Telemetry */}
              {data.forecast && (
                <div className="bg-slate-800/40 border border-slate-700/60 rounded-xl p-4">
                  <div className="flex items-center gap-2 mb-3">
                    <Cpu className="w-4 h-4 text-emerald-400" />
                    <h3 className="text-sm font-semibold text-white">Demand Forecast Results</h3>
                  </div>
                  <div className="grid grid-cols-2 sm:grid-cols-3 gap-2.5 text-xs">
                    <div className="bg-slate-900/60 p-2.5 rounded-lg border border-slate-800">
                      <span className="text-slate-400 block">Model Engine</span>
                      <span className="font-semibold text-slate-200 mt-0.5 block">
                        {data.forecast.model_name} ({data.forecast.model_version})
                      </span>
                    </div>
                    <div className="bg-slate-900/60 p-2.5 rounded-lg border border-slate-800">
                      <span className="text-slate-400 block">Forecast Rows</span>
                      <span className="font-semibold text-slate-200 mt-0.5 block">{data.forecast.row_count}</span>
                    </div>
                    <div className="bg-slate-900/60 p-2.5 rounded-lg border border-slate-800">
                      <span className="text-slate-400 block">Categories</span>
                      <span className="font-semibold text-slate-200 mt-0.5 block">{data.forecast.category_count}</span>
                    </div>
                    <div className="bg-slate-900/60 p-2.5 rounded-lg border border-slate-800">
                      <span className="text-slate-400 block">Horizon Start</span>
                      <span className="font-semibold text-slate-200 mt-0.5 block font-mono">{data.forecast.min_forecast_week || '—'}</span>
                    </div>
                    <div className="bg-slate-900/60 p-2.5 rounded-lg border border-slate-800">
                      <span className="text-slate-400 block">Horizon End</span>
                      <span className="font-semibold text-slate-200 mt-0.5 block font-mono">{data.forecast.max_forecast_week || '—'}</span>
                    </div>
                    <div className="bg-slate-900/60 p-2.5 rounded-lg border border-slate-800">
                      <span className="text-slate-400 block">Avg Demand</span>
                      <span className="font-semibold text-emerald-400 mt-0.5 block">
                        {data.forecast.avg_predicted_demand !== null ? data.forecast.avg_predicted_demand : '—'}
                      </span>
                    </div>
                  </div>
                </div>
              )}

              {/* Section 3: Scoped Execution Artifacts (Phase 10) */}
              <div className="bg-slate-800/40 border border-slate-700/60 rounded-xl p-4">
                <div className="flex items-center justify-between mb-3">
                  <div className="flex items-center gap-2">
                    <FileCode2 className="w-4 h-4 text-sky-400" />
                    <h3 className="text-sm font-semibold text-white">Execution-Scoped Artifacts</h3>
                  </div>
                  {data.artifacts?.exists ? (
                    <span className="text-xs text-emerald-400 bg-emerald-500/10 border border-emerald-500/20 px-2 py-0.5 rounded-md flex items-center gap-1 font-medium">
                      <CheckCircle2 className="w-3.5 h-3.5" /> Isolated on Disk
                    </span>
                  ) : (
                    <span className="text-xs text-slate-400 bg-slate-800 border border-slate-700 px-2 py-0.5 rounded-md">
                      Not Preserved
                    </span>
                  )}
                </div>

                <div className="text-xs text-slate-400 space-y-2">
                  <p className="font-mono text-[11px] bg-slate-950 p-2 rounded-lg border border-slate-800 text-slate-300 break-all">
                    {data.output_location || 'ml/results/executions/' + executionId}
                  </p>

                  {data.artifacts?.scoped_files?.length > 0 ? (
                    <div className="mt-3">
                      <span className="text-[11px] font-semibold uppercase tracking-wider text-slate-400 block mb-2">
                        Preserved Execution Artifacts ({data.artifacts.scoped_files.length} files):
                      </span>
                      <ul className="grid grid-cols-1 sm:grid-cols-2 gap-1.5 font-mono text-xs">
                        {data.artifacts.scoped_files.map((file, idx) => (
                          <li key={idx} className="flex items-center gap-2 bg-slate-900/60 px-2.5 py-1.5 rounded-md border border-slate-800 text-slate-300">
                            <span className="text-sky-500">📄</span>
                            <span className="truncate">{file}</span>
                          </li>
                        ))}
                      </ul>
                    </div>
                  ) : (
                    <p className="text-xs text-slate-500 italic mt-2">
                      No discrete artifact files recorded in scoped directory.
                    </p>
                  )}
                </div>
              </div>
            </>
          )}
        </div>

        {/* Modal Footer */}
        <div className="px-6 py-3.5 border-t border-slate-800 bg-slate-800/50 flex justify-end">
          <button
            onClick={onClose}
            className="px-4 py-2 text-xs font-semibold rounded-lg bg-slate-800 text-slate-200 hover:bg-slate-700 border border-slate-700 transition-colors"
          >
            Close Audit
          </button>
        </div>
      </div>
    </div>
  );
}
