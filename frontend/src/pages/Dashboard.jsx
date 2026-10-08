import React, { useState, useEffect } from 'react';
import { 
  Activity, 
  CheckCircle2, 
  XCircle, 
  Percent, 
  Database, 
  Clock, 
  Cpu, 
  PlayCircle,
  TrendingUp,
  AlertCircle
} from 'lucide-react';
import { getSummary, getExecutions } from '../api/client';
import MetricCard from '../components/MetricCard';
import StatusBadge from '../components/StatusBadge';
import LoadingSpinner from '../components/LoadingSpinner';
import EmptyState from '../components/EmptyState';
import ExecutionModal from '../components/ExecutionModal';
import { 
  ResponsiveContainer, 
  PieChart, 
  Pie, 
  Cell, 
  Tooltip, 
  Legend,
  BarChart,
  Bar,
  XAxis,
  YAxis
} from 'recharts';

export default function Dashboard({ refreshTrigger }) {
  const [summary, setSummary] = useState(null);
  const [recentExecs, setRecentExecs] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [selectedExecId, setSelectedExecId] = useState(null);

  useEffect(() => {
    let isMounted = true;

    async function loadData() {
      try {
        const [sumData, execsData] = await Promise.all([
          getSummary(),
          getExecutions({ limit: 8 }),
        ]);

        if (isMounted) {
          setSummary(sumData);
          setRecentExecs(execsData || []);
          setError(null);
        }
      } catch (err) {
        if (isMounted) {
          setError(err.response?.data?.detail || err.message || 'Unable to connect to SupplySense AI backend.');
        }
      } finally {
        if (isMounted) setLoading(false);
      }
    }

    loadData();
    return () => {
      isMounted = false;
    };
  }, [refreshTrigger]);

  if (loading && !summary) {
    return <LoadingSpinner message="Aggregating operational pipeline metrics..." />;
  }

  if (error && !summary) {
    return (
      <div className="p-6 rounded-2xl bg-rose-500/10 border border-rose-500/20 text-rose-300 flex items-start gap-3">
        <AlertCircle className="w-5 h-5 flex-shrink-0 mt-0.5 text-rose-400" />
        <div>
          <h3 className="text-base font-semibold">Observability Service Unreachable</h3>
          <p className="text-sm text-rose-200/80 mt-1">{error}</p>
          <p className="text-xs text-rose-300/70 mt-2">
            Verify that FastAPI is running on <code className="font-mono bg-rose-950/60 px-1.5 py-0.5 rounded">http://127.0.0.1:8000</code> using:
            <br />
            <code className="font-mono text-[11px] block mt-1">python -m uvicorn services.observability.main:app --port 8000</code>
          </p>
        </div>
      </div>
    );
  }

  const execCounts = summary?.executions || {};
  const datasets = summary?.datasets || {};
  const latestExec = summary?.latest_execution;
  const latestForecast = summary?.latest_forecast;

  // Chart Data: Status Distribution
  const pieData = [
    { name: 'Completed', value: execCounts.completed || 0, color: '#10b981' },
    { name: 'Failed', value: execCounts.failed || 0, color: '#f43f5e' },
    { name: 'Running', value: execCounts.running || 0, color: '#38bdf8' },
  ].filter(d => d.value > 0);

  // Chart Data: Recent Executions Duration
  const durationData = [...recentExecs]
    .reverse()
    .map(e => ({
      id: `#${e.execution_id}`,
      duration: e.duration_seconds !== null ? e.duration_seconds : 0,
      status: e.status
    }));

  return (
    <div className="space-y-6">
      {/* Page Title & Context */}
      <div>
        <h2 className="text-2xl font-bold text-white tracking-tight">Executive Telemetry Overview</h2>
        <p className="text-xs sm:text-sm text-slate-400 mt-1">
          Real-time metrics aggregating event detection, dependency satisfaction, automated execution, and ML forecasts.
        </p>
      </div>

      {/* Top 6 KPI Metric Cards */}
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3.5">
        <MetricCard
          title="Total Runs"
          value={execCounts.total ?? 0}
          subtitle={`${execCounts.retried_total || 0} retried runs`}
          icon={Activity}
          color="sky"
        />
        <MetricCard
          title="Completed"
          value={execCounts.completed ?? 0}
          subtitle="Successfully executed"
          icon={CheckCircle2}
          color="emerald"
        />
        <MetricCard
          title="Failed"
          value={execCounts.failed ?? 0}
          subtitle={`${execCounts.retrying || 0} currently retrying`}
          icon={XCircle}
          color="rose"
        />
        <MetricCard
          title="Success Rate"
          value={`${summary?.success_rate ?? 0}%`}
          subtitle={`Failure: ${summary?.failure_rate ?? 0}%`}
          icon={Percent}
          color="indigo"
        />
        <MetricCard
          title="Datasets Ready"
          value={`${datasets.ready ?? 0} / ${datasets.total ?? 0}`}
          subtitle={`${datasets.waiting ?? 0} waiting sync`}
          icon={Database}
          color="purple"
        />
        <MetricCard
          title="Avg Duration"
          value={summary?.average_duration_seconds !== null ? `${summary.average_duration_seconds}s` : '—'}
          subtitle="Pipeline latency"
          icon={Clock}
          color="amber"
        />
      </div>

      {/* Highlights: Latest Execution & Latest Forecast */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-5">
        {/* Latest Execution Card */}
        <div className="bg-slate-800/80 border border-slate-700/70 rounded-2xl p-5 flex flex-col justify-between">
          <div>
            <div className="flex items-center justify-between pb-3 border-b border-slate-700/60">
              <div className="flex items-center gap-2">
                <PlayCircle className="w-5 h-5 text-sky-400" />
                <h3 className="text-sm font-bold text-white uppercase tracking-wider">Latest Pipeline Execution</h3>
              </div>
              {latestExec && <StatusBadge status={latestExec.status} size="sm" />}
            </div>

            {latestExec ? (
              <div className="mt-4 space-y-2.5 text-xs">
                <div className="flex justify-between items-center py-1">
                  <span className="text-slate-400">Execution Identifier</span>
                  <button 
                    onClick={() => setSelectedExecId(latestExec.execution_id)}
                    className="font-mono font-bold text-sky-400 hover:underline"
                  >
                    #{latestExec.execution_id}
                  </button>
                </div>
                <div className="flex justify-between items-center py-1">
                  <span className="text-slate-400">Pipeline Name</span>
                  <span className="font-semibold text-slate-200">{latestExec.pipeline_name}</span>
                </div>
                <div className="flex justify-between items-center py-1">
                  <span className="text-slate-400">Duration</span>
                  <span className="font-semibold text-white">
                    {latestExec.duration_seconds !== null ? `${latestExec.duration_seconds} seconds` : '—'}
                  </span>
                </div>
                <div className="flex justify-between items-center py-1">
                  <span className="text-slate-400">Started Timestamp</span>
                  <span className="font-mono text-slate-300">{latestExec.started_at || '—'}</span>
                </div>
                <div className="flex justify-between items-center py-1">
                  <span className="text-slate-400">Completed Timestamp</span>
                  <span className="font-mono text-slate-300">{latestExec.completed_at || '—'}</span>
                </div>
                <div className="flex justify-between items-center py-1">
                  <span className="text-slate-400">Primary Artifact</span>
                  <span className="font-mono text-[11px] text-slate-400 truncate max-w-[240px]">
                    {latestExec.output_location || '—'}
                  </span>
                </div>
              </div>
            ) : (
              <p className="mt-6 text-xs text-slate-500 italic">No execution recorded yet.</p>
            )}
          </div>

          {latestExec && (
            <div className="mt-4 pt-3 border-t border-slate-700/50 flex justify-end">
              <button
                onClick={() => setSelectedExecId(latestExec.execution_id)}
                className="text-xs font-semibold text-sky-400 hover:text-sky-300 flex items-center gap-1"
              >
                Inspect Telemetry & Artifacts →
              </button>
            </div>
          )}
        </div>

        {/* Latest Forecast Card */}
        <div className="bg-slate-800/80 border border-slate-700/70 rounded-2xl p-5 flex flex-col justify-between">
          <div>
            <div className="flex items-center justify-between pb-3 border-b border-slate-700/60">
              <div className="flex items-center gap-2">
                <Cpu className="w-5 h-5 text-emerald-400" />
                <h3 className="text-sm font-bold text-white uppercase tracking-wider">Latest ML Forecast Model</h3>
              </div>
              <span className="text-xs px-2 py-0.5 rounded-md font-semibold bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">
                XGBoost Regressor
              </span>
            </div>

            {latestForecast ? (
              <div className="mt-4 space-y-2.5 text-xs">
                <div className="flex justify-between items-center py-1">
                  <span className="text-slate-400">Model Engine</span>
                  <span className="font-semibold text-white">
                    {latestForecast.model_name} <span className="text-slate-400 font-mono">({latestForecast.model_version})</span>
                  </span>
                </div>
                <div className="flex justify-between items-center py-1">
                  <span className="text-slate-400">Forecast Predictions</span>
                  <span className="font-semibold text-emerald-400">{latestForecast.row_count} rows</span>
                </div>
                <div className="flex justify-between items-center py-1">
                  <span className="text-slate-400">Category Coverage</span>
                  <span className="font-semibold text-slate-200">{latestForecast.category_count} product categories</span>
                </div>
                <div className="flex justify-between items-center py-1">
                  <span className="text-slate-400">Forecast Horizon</span>
                  <span className="font-mono text-slate-300">
                    {latestForecast.min_forecast_week} → {latestForecast.max_forecast_week}
                  </span>
                </div>
                <div className="flex justify-between items-center py-1">
                  <span className="text-slate-400">Average Predicted Demand</span>
                  <span className="font-mono font-bold text-emerald-400">
                    {latestForecast.avg_predicted_demand !== null ? `${latestForecast.avg_predicted_demand} units/week` : '—'}
                  </span>
                </div>
                <div className="flex justify-between items-center py-1">
                  <span className="text-slate-400">Generated Timestamp</span>
                  <span className="font-mono text-slate-300">{latestForecast.created_at || '—'}</span>
                </div>
              </div>
            ) : (
              <p className="mt-6 text-xs text-slate-500 italic">No forecast generated yet.</p>
            )}
          </div>

          {latestForecast?.execution_id && (
            <div className="mt-4 pt-3 border-t border-slate-700/50 flex justify-end">
              <button
                onClick={() => setSelectedExecId(latestForecast.execution_id)}
                className="text-xs font-semibold text-emerald-400 hover:text-emerald-300 flex items-center gap-1"
              >
                Inspect Forecast Execution #{latestForecast.execution_id} →
              </button>
            </div>
          )}
        </div>
      </div>

      {/* Recharts Visualizations */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-5">
        {/* Status Distribution Pie */}
        <div className="bg-slate-800/80 border border-slate-700/70 rounded-2xl p-5">
          <h3 className="text-sm font-bold text-white uppercase tracking-wider mb-2">
            Execution Lifecycle Distribution
          </h3>
          <p className="text-xs text-slate-400 mb-4">Ratio of Completed vs Failed executions.</p>

          <div className="h-56">
            {pieData.length > 0 ? (
              <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                  <Pie
                    data={pieData}
                    cx="50%"
                    cy="50%"
                    innerRadius={55}
                    outerRadius={80}
                    paddingAngle={4}
                    dataKey="value"
                  >
                    {pieData.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={entry.color} />
                    ))}
                  </Pie>
                  <Tooltip 
                    contentStyle={{ backgroundColor: '#0f172a', borderColor: '#334155', borderRadius: '0.75rem', fontSize: '12px' }}
                    itemStyle={{ color: '#f8fafc' }}
                  />
                  <Legend 
                    verticalAlign="bottom" 
                    height={36} 
                    iconType="circle"
                    formatter={(val) => <span className="text-xs text-slate-300">{val}</span>}
                  />
                </PieChart>
              </ResponsiveContainer>
            ) : (
              <EmptyState title="No execution data" description="Run a pipeline to populate metrics." />
            )}
          </div>
        </div>

        {/* Recent Executions Duration Bar */}
        <div className="bg-slate-800/80 border border-slate-700/70 rounded-2xl p-5 lg:col-span-2">
          <h3 className="text-sm font-bold text-white uppercase tracking-wider mb-2">
            Recent Pipeline Latencies (Seconds)
          </h3>
          <p className="text-xs text-slate-400 mb-4">Elapsed runtime for recent pipeline executions.</p>

          <div className="h-56">
            {durationData.length > 0 ? (
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={durationData} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
                  <XAxis 
                    dataKey="id" 
                    tick={{ fill: '#94a3b8', fontSize: 11 }}
                    stroke="#334155"
                  />
                  <YAxis 
                    tick={{ fill: '#94a3b8', fontSize: 11 }}
                    stroke="#334155"
                  />
                  <Tooltip 
                    contentStyle={{ backgroundColor: '#0f172a', borderColor: '#334155', borderRadius: '0.75rem', fontSize: '12px' }}
                    itemStyle={{ color: '#38bdf8' }}
                    formatter={(value) => [`${value}s`, 'Duration']}
                  />
                  <Bar dataKey="duration" fill="#0284c7" radius={[6, 6, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            ) : (
              <EmptyState title="No latency data" description="Executions will appear here after triggering." />
            )}
          </div>
        </div>
      </div>

      {/* Selected Execution Modal */}
      {selectedExecId && (
        <ExecutionModal
          executionId={selectedExecId}
          onClose={() => setSelectedExecId(null)}
        />
      )}
    </div>
  );
}
