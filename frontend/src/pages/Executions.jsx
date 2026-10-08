import React, { useState, useEffect } from 'react';
import { PlayCircle, Filter, Eye, AlertCircle, RefreshCw } from 'lucide-react';
import { getExecutions } from '../api/client';
import StatusBadge from '../components/StatusBadge';
import LoadingSpinner from '../components/LoadingSpinner';
import EmptyState from '../components/EmptyState';
import ExecutionModal from '../components/ExecutionModal';

export default function Executions({ refreshTrigger }) {
  const [executions, setExecutions] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [statusFilter, setStatusFilter] = useState('ALL');
  const [limit, setLimit] = useState(20);
  const [selectedExecId, setSelectedExecId] = useState(null);

  useEffect(() => {
    let isMounted = true;
    setLoading(true);

    const params = { limit };
    if (statusFilter !== 'ALL') {
      params.status = statusFilter;
    }

    getExecutions(params)
      .then((data) => {
        if (isMounted) {
          setExecutions(data || []);
          setError(null);
        }
      })
      .catch((err) => {
        if (isMounted) {
          setError(err.response?.data?.detail || err.message || 'Failed to fetch pipeline executions');
        }
      })
      .finally(() => {
        if (isMounted) setLoading(false);
      });

    return () => {
      isMounted = false;
    };
  }, [statusFilter, limit, refreshTrigger]);

  return (
    <div className="space-y-6">
      {/* Header & Controls */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div>
          <h2 className="text-2xl font-bold text-white tracking-tight">Pipeline Executions</h2>
          <p className="text-xs sm:text-sm text-slate-400 mt-1">
            Audit trail of triggered pipeline runs, idempotent recovery, retries, and isolated artifact locations.
          </p>
        </div>

        {/* Filter Controls */}
        <div className="flex flex-wrap items-center gap-3">
          {/* Status Filter */}
          <div className="flex items-center gap-2 bg-slate-800/80 border border-slate-700/80 rounded-xl px-3 py-1.5 text-xs">
            <Filter className="w-3.5 h-3.5 text-slate-400" />
            <span className="text-slate-400 font-medium">Status:</span>
            <select
              value={statusFilter}
              onChange={(e) => setStatusFilter(e.target.value)}
              className="bg-transparent text-white font-semibold focus:outline-none cursor-pointer"
            >
              <option value="ALL" className="bg-slate-900 text-white">All Statuses</option>
              <option value="RUNNING" className="bg-slate-900 text-white">RUNNING</option>
              <option value="COMPLETED" className="bg-slate-900 text-white">COMPLETED</option>
              <option value="FAILED" className="bg-slate-900 text-white">FAILED</option>
            </select>
          </div>

          {/* Limit Selector */}
          <div className="flex items-center gap-2 bg-slate-800/80 border border-slate-700/80 rounded-xl px-3 py-1.5 text-xs">
            <span className="text-slate-400 font-medium">Rows:</span>
            <select
              value={limit}
              onChange={(e) => setLimit(Number(e.target.value))}
              className="bg-transparent text-white font-semibold focus:outline-none cursor-pointer"
            >
              <option value={20} className="bg-slate-900 text-white">20</option>
              <option value={50} className="bg-slate-900 text-white">50</option>
              <option value={100} className="bg-slate-900 text-white">100</option>
            </select>
          </div>
        </div>
      </div>

      {/* Error Banner */}
      {error && (
        <div className="p-4 rounded-xl bg-rose-500/10 border border-rose-500/20 text-rose-300 flex items-center gap-3">
          <AlertCircle className="w-5 h-5 flex-shrink-0 text-rose-400" />
          <p className="text-sm">{error}</p>
        </div>
      )}

      {/* Main Table Container */}
      <div className="bg-slate-800/80 border border-slate-700/70 rounded-2xl overflow-hidden shadow-sm">
        {loading ? (
          <LoadingSpinner message="Querying execution registry..." />
        ) : executions.length === 0 ? (
          <div className="p-8">
            <EmptyState 
              title="No executions found" 
              description={statusFilter !== 'ALL' 
                ? `No pipeline executions found with status '${statusFilter}'.` 
                : 'No pipeline executions have run yet.'} 
            />
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-left text-xs">
              <thead className="bg-slate-900/60 text-slate-400 uppercase font-semibold border-b border-slate-700/60 tracking-wider">
                <tr>
                  <th className="py-3.5 px-4">Execution</th>
                  <th className="py-3.5 px-4">Pipeline</th>
                  <th className="py-3.5 px-4">Status</th>
                  <th className="py-3.5 px-4">Retries</th>
                  <th className="py-3.5 px-4">Duration</th>
                  <th className="py-3.5 px-4 hidden md:table-cell">Started</th>
                  <th className="py-3.5 px-4 hidden lg:table-cell">Completed</th>
                  <th className="py-3.5 px-4 text-right">Audit</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-700/40 font-mono">
                {executions.map((exec) => (
                  <tr 
                    key={exec.execution_id}
                    onClick={() => setSelectedExecId(exec.execution_id)}
                    className="hover:bg-slate-700/40 transition-colors cursor-pointer group"
                  >
                    <td className="py-3.5 px-4 font-bold text-sky-400">
                      #{exec.execution_id}
                    </td>
                    <td className="py-3.5 px-4 font-sans font-medium text-slate-200">
                      {exec.pipeline_name}
                    </td>
                    <td className="py-3.5 px-4">
                      <StatusBadge status={exec.status} size="sm" />
                    </td>
                    <td className="py-3.5 px-4 text-slate-300">
                      <span className={exec.retry_count > 0 ? 'text-amber-400 font-bold' : 'text-slate-400'}>
                        {exec.retry_count}
                      </span>
                      <span className="text-slate-500"> / {exec.max_retries}</span>
                    </td>
                    <td className="py-3.5 px-4 text-slate-200">
                      {exec.duration_seconds !== null ? `${exec.duration_seconds}s` : '—'}
                    </td>
                    <td className="py-3.5 px-4 text-slate-400 hidden md:table-cell text-[11px]">
                      {exec.started_at || '—'}
                    </td>
                    <td className="py-3.5 px-4 text-slate-400 hidden lg:table-cell text-[11px]">
                      {exec.completed_at || '—'}
                    </td>
                    <td className="py-3.5 px-4 text-right">
                      <button
                        onClick={(e) => {
                          e.stopPropagation();
                          setSelectedExecId(exec.execution_id);
                        }}
                        className="inline-flex items-center gap-1 text-[11px] font-sans font-semibold text-slate-400 group-hover:text-sky-400 px-2.5 py-1 rounded-md bg-slate-800 group-hover:bg-sky-500/10 border border-slate-700 group-hover:border-sky-500/20 transition-all"
                      >
                        <Eye className="w-3.5 h-3.5" />
                        <span>Inspect</span>
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        <div className="px-5 py-3 border-t border-slate-700/60 bg-slate-900/40 text-xs text-slate-400 flex items-center justify-between font-sans">
          <span>Showing {executions.length} executions</span>
          <span className="text-slate-500 hidden sm:inline">Click any row to inspect decision rules & scoped artifact files</span>
        </div>
      </div>

      {/* Execution Audit Modal */}
      {selectedExecId && (
        <ExecutionModal
          executionId={selectedExecId}
          onClose={() => setSelectedExecId(null)}
        />
      )}
    </div>
  );
}
