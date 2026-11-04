import React, { useState, useEffect } from 'react';
import { PlayCircle, Filter, Eye, AlertCircle, RefreshCw, ChevronLeft, ChevronRight, Layers } from 'lucide-react';
import { getExecutions } from '../api/client';
import StatusBadge from '../components/StatusBadge';
import LoadingSpinner from '../components/LoadingSpinner';
import EmptyState from '../components/EmptyState';
import ExecutionModal from '../components/ExecutionModal';

export default function Executions({ refreshTrigger }) {
  const [executions, setExecutions] = useState([]);
  const [totalCount, setTotalCount] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  
  // Filters & Pagination State
  const [scopeFilter, setScopeFilter] = useState('OPERATIONAL');
  const [statusFilter, setStatusFilter] = useState('ALL');
  const [page, setPage] = useState(1);
  const [limit, setLimit] = useState(20);
  const [selectedExecId, setSelectedExecId] = useState(null);

  useEffect(() => {
    let isMounted = true;
    setLoading(true);

    const offset = (page - 1) * limit;
    const params = {
      scope: scopeFilter,
      limit,
      offset
    };
    if (statusFilter !== 'ALL') {
      params.status = statusFilter;
    }

    getExecutions(params)
      .then((data) => {
        if (isMounted) {
          if (data && Array.isArray(data.items)) {
            setExecutions(data.items);
            setTotalCount(data.total_count ?? data.items.length);
          } else if (Array.isArray(data)) {
            setExecutions(data);
            setTotalCount(data.length);
          } else {
            setExecutions([]);
            setTotalCount(0);
          }
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
  }, [scopeFilter, statusFilter, page, limit, refreshTrigger]);

  const handleScopeChange = (e) => {
    setScopeFilter(e.target.value);
    setPage(1);
  };

  const handleStatusChange = (e) => {
    setStatusFilter(e.target.value);
    setPage(1);
  };

  const handleLimitChange = (e) => {
    setLimit(Number(e.target.value));
    setPage(1);
  };

  const totalPages = Math.max(1, Math.ceil(totalCount / limit));
  const startItem = totalCount === 0 ? 0 : (page - 1) * limit + 1;
  const endItem = Math.min(page * limit, totalCount);

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
          {/* Scope Filter */}
          <div className="flex items-center gap-2 bg-slate-800/80 border border-slate-700/80 rounded-xl px-3 py-1.5 text-xs">
            <Layers className="w-3.5 h-3.5 text-sky-400" />
            <span className="text-slate-400 font-medium">Scope:</span>
            <select
              value={scopeFilter}
              onChange={handleScopeChange}
              className="bg-transparent text-white font-semibold focus:outline-none cursor-pointer"
            >
              <option value="OPERATIONAL" className="bg-slate-900 text-white">Operational</option>
              <option value="SIMULATED" className="bg-slate-900 text-white">Simulated</option>
              <option value="ALL" className="bg-slate-900 text-white">All (Unfiltered)</option>
            </select>
          </div>

          {/* Status Filter */}
          <div className="flex items-center gap-2 bg-slate-800/80 border border-slate-700/80 rounded-xl px-3 py-1.5 text-xs">
            <Filter className="w-3.5 h-3.5 text-slate-400" />
            <span className="text-slate-400 font-medium">Status:</span>
            <select
              value={statusFilter}
              onChange={handleStatusChange}
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
            <span className="text-slate-400 font-medium">Page Size:</span>
            <select
              value={limit}
              onChange={handleLimitChange}
              className="bg-transparent text-white font-semibold focus:outline-none cursor-pointer"
            >
              <option value={10} className="bg-slate-900 text-white">10</option>
              <option value={20} className="bg-slate-900 text-white">20</option>
              <option value={50} className="bg-slate-900 text-white">50</option>
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
                ? `No ${scopeFilter.toLowerCase()} pipeline executions found with status '${statusFilter}'.` 
                : `No ${scopeFilter.toLowerCase()} pipeline executions recorded.`} 
            />
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-left text-xs">
              <thead className="bg-slate-900/60 text-slate-400 uppercase font-semibold border-b border-slate-700/60 tracking-wider">
                <tr>
                  <th className="py-3.5 px-4">Execution</th>
                  <th className="py-3.5 px-4">Classification</th>
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
                {executions.map((exec) => {
                  const isSimulated = exec.pipeline_name?.startsWith('SIM-') || exec.scope === 'SIMULATED';
                  return (
                    <tr 
                      key={exec.execution_id}
                      onClick={() => setSelectedExecId(exec.execution_id)}
                      className="hover:bg-slate-700/40 transition-colors cursor-pointer group"
                    >
                      <td className="py-3.5 px-4 font-bold text-sky-400">
                        #{exec.execution_id}
                      </td>
                      <td className="py-3.5 px-4">
                        <span
                          className={`text-[10px] px-2 py-0.5 rounded font-bold uppercase tracking-wide border ${
                            isSimulated
                              ? 'bg-amber-500/10 text-amber-400 border-amber-500/20'
                              : 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20'
                          }`}
                        >
                          {isSimulated ? 'SIMULATED' : 'OPERATIONAL'}
                        </span>
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
                );
              })}
            </tbody>
            </table>
          </div>
        )}

        {/* Pagination Toolbar */}
        <div className="px-5 py-3 border-t border-slate-700/60 bg-slate-900/40 text-xs text-slate-400 flex flex-col sm:flex-row items-center justify-between gap-3 font-sans">
          <span>
            Showing <strong className="text-slate-200 font-mono">{startItem}</strong> to{' '}
            <strong className="text-slate-200 font-mono">{endItem}</strong> of{' '}
            <strong className="text-slate-200 font-mono">{totalCount}</strong> executions
          </span>

          <div className="flex items-center gap-2">
            <button
              onClick={() => setPage((p) => Math.max(1, p - 1))}
              disabled={page <= 1 || loading}
              className="px-2.5 py-1 rounded-lg border border-slate-700 bg-slate-800 text-slate-300 hover:bg-slate-700 disabled:opacity-40 disabled:cursor-not-allowed flex items-center gap-1 transition-colors"
            >
              <ChevronLeft className="w-3.5 h-3.5" />
              <span>Previous</span>
            </button>

            <span className="px-2 text-slate-400 text-xs font-mono">
              Page <span className="text-white font-bold">{page}</span> of {totalPages}
            </span>

            <button
              onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
              disabled={page >= totalPages || loading}
              className="px-2.5 py-1 rounded-lg border border-slate-700 bg-slate-800 text-slate-300 hover:bg-slate-700 disabled:opacity-40 disabled:cursor-not-allowed flex items-center gap-1 transition-colors"
            >
              <span>Next</span>
              <ChevronRight className="w-3.5 h-3.5" />
            </button>
          </div>
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
