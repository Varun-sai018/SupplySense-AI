import React, { useState, useEffect } from 'react';
import { TrendingUp, Cpu, Eye, AlertCircle, BarChart3, Calendar, Layers } from 'lucide-react';
import { getForecasts } from '../api/client';
import LoadingSpinner from '../components/LoadingSpinner';
import EmptyState from '../components/EmptyState';
import ExecutionModal from '../components/ExecutionModal';
import { 
  ResponsiveContainer, 
  BarChart, 
  Bar, 
  XAxis, 
  YAxis, 
  Tooltip 
} from 'recharts';

export default function Forecasts({ refreshTrigger }) {
  const [forecasts, setForecasts] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [limit, setLimit] = useState(20);
  const [selectedExecId, setSelectedExecId] = useState(null);

  useEffect(() => {
    let isMounted = true;
    setLoading(true);

    getForecasts({ limit })
      .then((data) => {
        if (isMounted) {
          setForecasts(data || []);
          setError(null);
        }
      })
      .catch((err) => {
        if (isMounted) {
          setError(err.response?.data?.detail || err.message || 'Failed to fetch ML forecast summaries');
        }
      })
      .finally(() => {
        if (isMounted) setLoading(false);
      });

    return () => {
      isMounted = false;
    };
  }, [limit, refreshTrigger]);

  // Chart data: Average Predicted Demand by Execution
  const chartData = [...forecasts]
    .reverse()
    .map((f) => ({
      name: `#${f.execution_id}`,
      avgDemand: f.average_predicted_demand !== null ? f.average_predicted_demand : 0,
      categories: f.category_count,
      model: `${f.model_name} (${f.model_version})`
    }));

  return (
    <div className="space-y-6">
      {/* Page Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div>
          <h2 className="text-2xl font-bold text-white tracking-tight">Machine Learning Forecasts</h2>
          <p className="text-xs sm:text-sm text-slate-400 mt-1">
            Downstream XGBoost demand forecast outputs persisted to MySQL and isolated in execution-scoped directories.
          </p>
        </div>

        {/* Row count limit selector */}
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

      {/* Error Banner */}
      {error && (
        <div className="p-4 rounded-xl bg-rose-500/10 border border-rose-500/20 text-rose-300 flex items-center gap-3">
          <AlertCircle className="w-5 h-5 flex-shrink-0 text-rose-400" />
          <p className="text-sm">{error}</p>
        </div>
      )}

      {/* Demand Forecast Chart */}
      {chartData.length > 0 && (
        <div className="bg-slate-800/80 border border-slate-700/70 rounded-2xl p-5 shadow-sm">
          <div className="flex items-center justify-between mb-2">
            <h3 className="text-sm font-bold text-white uppercase tracking-wider flex items-center gap-2">
              <BarChart3 className="w-4 h-4 text-emerald-400" />
              <span>Average Predicted Demand Across Pipeline Runs (Units/Week)</span>
            </h3>
            <span className="text-xs text-slate-400 font-mono">
              Model: XGBoost (xgboost-v1)
            </span>
          </div>
          <p className="text-xs text-slate-400 mb-4">
            Aggregated demand forecasts generated during automated pipeline execution.
          </p>

          <div className="h-56">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={chartData} margin={{ top: 10, right: 10, left: -15, bottom: 0 }}>
                <XAxis 
                  dataKey="name" 
                  tick={{ fill: '#94a3b8', fontSize: 11 }}
                  stroke="#334155"
                />
                <YAxis 
                  tick={{ fill: '#94a3b8', fontSize: 11 }}
                  stroke="#334155"
                />
                <Tooltip 
                  contentStyle={{ backgroundColor: '#0f172a', borderColor: '#334155', borderRadius: '0.75rem', fontSize: '12px' }}
                  itemStyle={{ color: '#10b981' }}
                  formatter={(val) => [`${val} units/week`, 'Avg Predicted Demand']}
                />
                <Bar dataKey="avgDemand" fill="#10b981" radius={[6, 6, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}

      {/* Forecast History Table */}
      <div className="bg-slate-800/80 border border-slate-700/70 rounded-2xl overflow-hidden shadow-sm">
        {loading ? (
          <LoadingSpinner message="Loading forecast telemetry..." />
        ) : forecasts.length === 0 ? (
          <div className="p-8">
            <EmptyState 
              title="No forecasts available" 
              description="Forecast results will be logged here once the Demand Forecast Pipeline executes." 
            />
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-left text-xs">
              <thead className="bg-slate-900/60 text-slate-400 uppercase font-semibold border-b border-slate-700/60 tracking-wider">
                <tr>
                  <th className="py-3.5 px-4">Execution</th>
                  <th className="py-3.5 px-4">Model Engine</th>
                  <th className="py-3.5 px-4">Rows</th>
                  <th className="py-3.5 px-4">Categories</th>
                  <th className="py-3.5 px-4">Horizon Period</th>
                  <th className="py-3.5 px-4">Avg Demand</th>
                  <th className="py-3.5 px-4 hidden lg:table-cell">Created At</th>
                  <th className="py-3.5 px-4 text-right">Inspect</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-700/40 font-mono">
                {forecasts.map((fc) => (
                  <tr 
                    key={fc.execution_id}
                    onClick={() => setSelectedExecId(fc.execution_id)}
                    className="hover:bg-slate-700/40 transition-colors cursor-pointer group"
                  >
                    <td className="py-3.5 px-4 font-bold text-sky-400">
                      #{fc.execution_id}
                    </td>
                    <td className="py-3.5 px-4 font-sans font-medium text-slate-200">
                      <span>{fc.model_name} </span>
                      <span className="text-slate-400 font-mono text-[11px]">({fc.model_version})</span>
                    </td>
                    <td className="py-3.5 px-4 text-slate-300">
                      {fc.row_count}
                    </td>
                    <td className="py-3.5 px-4 text-slate-300">
                      {fc.category_count}
                    </td>
                    <td className="py-3.5 px-4 text-slate-300 text-[11px]">
                      {fc.min_forecast_week} → {fc.max_forecast_week}
                    </td>
                    <td className="py-3.5 px-4 font-bold text-emerald-400">
                      {fc.average_predicted_demand !== null ? fc.average_predicted_demand : '—'}
                    </td>
                    <td className="py-3.5 px-4 text-slate-400 hidden lg:table-cell text-[11px]">
                      {fc.created_at || '—'}
                    </td>
                    <td className="py-3.5 px-4 text-right">
                      <button
                        onClick={(e) => {
                          e.stopPropagation();
                          setSelectedExecId(fc.execution_id);
                        }}
                        className="inline-flex items-center gap-1 text-[11px] font-sans font-semibold text-slate-400 group-hover:text-emerald-400 px-2.5 py-1 rounded-md bg-slate-800 group-hover:bg-emerald-500/10 border border-slate-700 group-hover:border-emerald-500/20 transition-all"
                      >
                        <Eye className="w-3.5 h-3.5" />
                        <span>Audit</span>
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        <div className="px-5 py-3 border-t border-slate-700/60 bg-slate-900/40 text-xs text-slate-400 flex items-center justify-between font-sans">
          <span>Showing {forecasts.length} forecast execution records</span>
          <span className="text-slate-500 hidden sm:inline">Persisted directly in MySQL forecast_results</span>
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
