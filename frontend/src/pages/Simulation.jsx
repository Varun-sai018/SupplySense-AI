import React, { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import {
  FlaskConical,
  Activity,
  Database,
  GitBranch,
  PlayCircle,
  CheckCircle2,
  ShieldAlert,
  Clock,
  Hash,
  Filter,
  ChevronRight,
  RefreshCw,
  AlertTriangle,
  Info,
  Layers,
  BookOpen,
  Sparkles,
  TrendingUp,
  Package,
  Truck,
  ArrowRight,
} from 'lucide-react';
import StatusBadge from '../components/StatusBadge';
import LoadingSpinner from '../components/LoadingSpinner';
import EmptyState from '../components/EmptyState';
import SimulationControlPanel from '../components/SimulationControlPanel';
import OlistReplayControlPanel from '../components/OlistReplayControlPanel';
import {
  getSimulatedDatasets,
  getSimulatedExecutions,
  getSimulatedDecisions,
  runE2ESimulation,
  getSimRuns,
} from '../api/client';

// ─── Helpers ─────────────────────────────────────────────────────────────────

function fmt(ts) {
  if (!ts) return '—';
  try {
    return new Date(ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
  } catch { return ts; }
}

function fmtDateTime(ts) {
  if (!ts) return '—';
  try {
    return new Date(ts).toLocaleString([], {
      month: 'short', day: '2-digit',
      hour: '2-digit', minute: '2-digit', second: '2-digit',
    });
  } catch { return ts; }
}

function elapsed(started) {
  if (!started) return null;
  const secs = Math.floor((Date.now() - new Date(started).getTime()) / 1000);
  if (secs < 60) return `${secs}s`;
  if (secs < 3600) return `${Math.floor(secs / 60)}m ${secs % 60}s`;
  return `${Math.floor(secs / 3600)}h ${Math.floor((secs % 3600) / 60)}m`;
}

const SCENARIO_COLOR = {
  S1_ALL:        'text-sky-400 bg-sky-500/10 border-sky-500/25',
  S2_ANY:        'text-emerald-400 bg-emerald-500/10 border-emerald-500/25',
  S3_QUORUM:     'text-purple-400 bg-purple-500/10 border-purple-500/25',
  S4_AST:        'text-amber-400 bg-amber-500/10 border-amber-500/25',
  S5_CONTINUOUS: 'text-indigo-400 bg-indigo-500/10 border-indigo-500/25',
};

function ScenarioBadge({ scenarioId }) {
  if (!scenarioId) return <span className="text-slate-500 text-xs">—</span>;
  const cls = SCENARIO_COLOR[scenarioId] || 'text-slate-400 bg-slate-700/50 border-slate-600';
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded-full border text-[10px] font-mono font-semibold ${cls}`}>
      {scenarioId}
    </span>
  );
}

// ─── Section Wrapper ─────────────────────────────────────────────────────────

function Section({ icon: Icon, title, children, badge, action }) {
  return (
    <div className="rounded-xl border border-slate-700/60 bg-slate-800/40 overflow-hidden">
      <div className="flex items-center gap-2.5 px-5 py-3.5 border-b border-slate-700/40 bg-slate-800/60">
        <Icon className="w-4 h-4 text-sky-400 flex-shrink-0" />
        <h3 className="text-sm font-semibold text-slate-200">{title}</h3>
        {badge && <span className="ml-auto text-xs font-mono text-slate-400">{badge}</span>}
        {action && <div className="ml-auto">{action}</div>}
      </div>
      <div className="p-5">{children}</div>
    </div>
  );
}

// ─── Live Overview ────────────────────────────────────────────────────────────

function StatRow({ label, value, mono }) {
  return (
    <div className="flex items-center justify-between py-1.5 border-b border-slate-700/30 last:border-0">
      <span className="text-xs text-slate-400">{label}</span>
      <span className={`text-xs font-medium text-slate-200 ${mono ? 'font-mono' : ''}`}>{value ?? '—'}</span>
    </div>
  );
}

function LiveOverview({ status }) {
  const [tick, setTick] = useState(0);
  // Keep elapsed time ticking for running state
  useEffect(() => {
    if (status?.state !== 'RUNNING') return;
    const id = setInterval(() => setTick((t) => t + 1), 1000);
    return () => clearInterval(id);
  }, [status?.state]);

  const stateVal = status?.state || 'IDLE';

  return (
    <div className="space-y-0.5">
      <StatRow label="Engine State" value={<StatusBadge status={stateVal} />} />
      <StatRow label="Execution Mode" value={status?.mode ?? 'STANDBY / IDLE'} />
      <StatRow
        label="Active Scenario"
        value={
          status?.active_scenario_id ? (
            <ScenarioBadge scenarioId={status.active_scenario_id} />
          ) : (
            <span className="text-slate-500 font-mono text-xs">NONE</span>
          )
        }
      />
      <StatRow label="Events Emitted" value={status?.emitted_event_count ?? 0} mono />
      <StatRow label="Last Event" value={fmt(status?.last_event_timestamp)} mono />
      <StatRow label="Event Interval" value={status?.event_interval_seconds ? `${status.event_interval_seconds}s` : '7s (default)'} />
      <StatRow label="Started At" value={status?.started_at ? fmtDateTime(status.started_at) : '—'} mono />
      <StatRow label="Elapsed" value={status?.started_at ? elapsed(status.started_at) : '—'} />
      {status?.simulation_id && (
        <div className="pt-2">
          <div className="text-[10px] text-slate-500 mb-0.5">Simulation ID</div>
          <div className="text-[10px] font-mono text-slate-400 break-all">{status.simulation_id}</div>
        </div>
      )}
      {status?.error_message && (
        <div className="mt-3 flex items-start gap-2 px-3 py-2.5 rounded-lg bg-rose-500/10 border border-rose-500/30 text-xs text-rose-300">
          <AlertTriangle className="w-3.5 h-3.5 flex-shrink-0 mt-0.5" />
          <span className="font-mono break-all">{status.error_message}</span>
        </div>
      )}
    </div>
  );
}

// ─── Scenario Architecture Guide (S1–S5) ──────────────────────────────────────

function ScenarioArchitectureGuide() {
  const [expanded, setExpanded] = useState(false);

  const scenarios = [
    {
      id: 'S1_ALL',
      name: 'Conjunctive Dependency (ALL)',
      type: 'ALL',
      rule: 'SIM_Raw_Feed == READY AND SIM_Catalog_Feed == READY AND SIM_Pricing_Feed == READY',
      desc: 'Triggers SIM-ALL-Pipeline only when ALL 3 feeder datasets reach READY state. Any single WAITING dataset blocks execution.',
    },
    {
      id: 'S2_ANY',
      name: 'Disjunctive Dependency (ANY)',
      type: 'ANY',
      rule: 'SIM_Primary_Feed == READY OR SIM_Backup_Feed == READY',
      desc: 'Triggers SIM-ANY-Pipeline immediately when AT LEAST ONE upstream feeder arrives (Primary or Backup failover).',
    },
    {
      id: 'S3_QUORUM',
      name: 'Quorum Threshold (M-of-N)',
      type: 'QUORUM',
      rule: 'COUNT(READY) >= 3 of 4 regional nodes (Node A, B, C, D)',
      desc: 'Requires a quorum threshold of at least 3 out of 4 regional edge nodes before triggering SIM-QUORUM-Pipeline.',
    },
    {
      id: 'S4_AST',
      name: 'Structured Boolean AST',
      type: 'AST',
      rule: "AND(SIM_Inventory_Feed == 'READY', SIM_Supplier_Feed == 'READY')",
      desc: 'Evaluates an Abstract Syntax Tree expression parser for complex nested boolean conditions across inventory and supplier feeds.',
    },
    {
      id: 'S5_CONTINUOUS',
      name: 'Multi-Scenario Continuous Rotation',
      type: 'ROTATION',
      rule: 'S1_ALL → S2_ANY → S3_QUORUM → S4_AST (One complete rotation)',
      desc: 'Sequentially exercises all 4 scenario dependency patterns in automated succession with bounded completion.',
    },
  ];

  return (
    <div className="rounded-xl border border-slate-700/60 bg-slate-800/40 overflow-hidden">
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full flex items-center justify-between px-5 py-3.5 bg-slate-800/60 border-b border-slate-700/40 text-left hover:bg-slate-800/80 transition-colors cursor-pointer"
      >
        <div className="flex items-center gap-2.5">
          <BookOpen className="w-4 h-4 text-sky-400 flex-shrink-0" />
          <h3 className="text-sm font-semibold text-white">Scenario Architecture Guide (S1–S5)</h3>
          <span className="text-[11px] px-2 py-0.5 rounded-full bg-slate-700/50 text-slate-300 font-mono">
            5 Scenarios
          </span>
        </div>
        <div className="flex items-center gap-2 text-xs text-slate-400">
          <span>{expanded ? 'Collapse Guide' : 'Expand Guide'}</span>
          <ChevronRight className={`w-4 h-4 transition-transform ${expanded ? 'rotate-90' : ''}`} />
        </div>
      </button>

      {expanded && (
        <div className="p-5 grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {scenarios.map((s) => (
            <div key={s.id} className="p-3.5 rounded-xl bg-slate-900/60 border border-slate-700/60 space-y-2">
              <div className="flex items-center justify-between">
                <ScenarioBadge scenarioId={s.id} />
                <span className="text-[10px] font-mono font-bold text-slate-400 px-1.5 py-0.5 rounded bg-slate-800 border border-slate-700">
                  {s.type}
                </span>
              </div>
              <h4 className="text-xs font-bold text-white">{s.name}</h4>
              <p className="text-[11px] text-slate-400 leading-relaxed">{s.desc}</p>
              <div className="pt-1.5 border-t border-slate-800 text-[10px] font-mono text-slate-300 truncate" title={s.rule}>
                <span className="text-slate-500">Condition: </span>{s.rule}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ─── Step Progress Indicator ──────────────────────────────────────────────────

// ─── End-to-End Simulation Runner (Phase F3) ──────────────────────────────────

function E2ESimulationPanel({ onRunComplete }) {
  const [batchSize, setBatchSize] = useState(200);
  const [running, setRunning] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);
  const [history, setHistory] = useState([]);

  const fetchHistory = useCallback(async () => {
    try {
      const data = await getSimRuns({ limit: 5 });
      setHistory(data || []);
    } catch {
      // non-fatal
    }
  }, []);

  useEffect(() => {
    fetchHistory();
  }, [fetchHistory]);

  const handleExecute = async () => {
    try {
      setRunning(true);
      setError(null);
      const res = await runE2ESimulation(batchSize);
      setResult(res);
      await fetchHistory();
      if (onRunComplete) onRunComplete(res);
    } catch (err) {
      setError(err.response?.data?.detail || err.message || 'Simulation execution failed');
    } finally {
      setRunning(false);
    }
  };

  return (
    <div className="rounded-xl border border-sky-500/30 bg-gradient-to-br from-slate-900/90 via-slate-800/60 to-sky-950/20 overflow-hidden shadow-lg">
      <div className="flex items-center justify-between px-5 py-4 border-b border-sky-500/20 bg-slate-800/80">
        <div className="flex items-center gap-3">
          <div className="p-2 rounded-xl bg-sky-500/15 border border-sky-500/30">
            <Sparkles className="w-5 h-5 text-sky-400 animate-pulse" />
          </div>
          <div>
            <h2 className="text-sm font-bold text-white tracking-wide flex items-center gap-2">
              End-to-End Data Simulation & Execution Engine
              <span className="text-[10px] font-mono font-bold px-2 py-0.5 rounded-full bg-emerald-500/15 text-emerald-400 border border-emerald-500/30">
                LIVE PIPELINES
              </span>
            </h2>
            <p className="text-xs text-slate-400 mt-0.5">
              Simulate synthetic supply-chain data → Evaluate dependencies → Trigger XGBoost Forecast & Inventory Reorder
            </p>
          </div>
        </div>
      </div>

      <div className="p-5 space-y-5">
        {/* Controls row */}
        <div className="flex flex-wrap items-center justify-between gap-4 p-4 rounded-xl bg-slate-900/60 border border-slate-700/60">
          <div className="space-y-1.5">
            <div className="text-xs font-semibold text-slate-300">Synthetic Orders Volume</div>
            <div className="flex items-center gap-2">
              {[
                { val: 100, label: '100 Orders (Quick)' },
                { val: 200, label: '200 Orders (Standard)' },
                { val: 500, label: '500 Orders (Deep Batch)' },
              ].map((opt) => (
                <button
                  key={opt.val}
                  type="button"
                  disabled={running}
                  onClick={() => setBatchSize(opt.val)}
                  className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-all cursor-pointer ${
                    batchSize === opt.val
                      ? 'bg-sky-500 text-white font-semibold shadow-md shadow-sky-500/20'
                      : 'bg-slate-800 text-slate-400 hover:text-white border border-slate-700'
                  }`}
                >
                  {opt.label}
                </button>
              ))}
            </div>
          </div>

          <button
            type="button"
            disabled={running}
            onClick={handleExecute}
            className={`flex items-center gap-2 px-5 py-2.5 rounded-xl font-semibold text-xs tracking-wide transition-all shadow-lg cursor-pointer ${
              running
                ? 'bg-slate-700 text-slate-400 cursor-not-allowed'
                : 'bg-gradient-to-r from-sky-500 to-indigo-600 hover:from-sky-400 hover:to-indigo-500 text-white shadow-sky-500/25 active:scale-98'
            }`}
          >
            {running ? (
              <>
                <RefreshCw className="w-4 h-4 animate-spin text-sky-300" />
                <span>Simulating & Executing Pipelines…</span>
              </>
            ) : (
              <>
                <Sparkles className="w-4 h-4" />
                <span>Run Full E2E Simulation</span>
              </>
            )}
          </button>
        </div>

        {error && (
          <div className="p-3.5 rounded-xl bg-rose-500/10 border border-rose-500/30 text-rose-300 text-xs flex items-center gap-2">
            <AlertTriangle className="w-4 h-4 text-rose-400 flex-shrink-0" />
            <span>{error}</span>
          </div>
        )}

        {/* Live Result Card */}
        {result && (
          <div className="p-4 rounded-xl bg-sky-950/30 border border-sky-500/30 space-y-3">
            <div className="flex items-center justify-between">
              <span className="text-xs font-bold text-sky-300 flex items-center gap-1.5">
                <CheckCircle2 className="w-4 h-4 text-emerald-400" />
                Simulation Run Completed Successfully ({result.simulation_run_id})
              </span>
              <span className="text-[10px] font-mono text-slate-400">
                Trigger: {result.forecast_decision} / {result.reorder_decision}
              </span>
            </div>

            <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
              <div className="p-3 rounded-lg bg-slate-900/60 border border-slate-700/50">
                <div className="text-[10px] text-slate-400 uppercase font-semibold">Orders Generated</div>
                <div className="text-lg font-bold font-mono text-white mt-0.5">{result.orders_generated}</div>
                <div className="text-[10px] text-slate-500">sim_orders table</div>
              </div>
              <div className="p-3 rounded-lg bg-slate-900/60 border border-slate-700/50">
                <div className="text-[10px] text-slate-400 uppercase font-semibold">Low-Stock Alerts</div>
                <div className="text-lg font-bold font-mono text-amber-400 mt-0.5">{result.low_stock_count} / {result.sku_count}</div>
                <div className="text-[10px] text-slate-500">sim_inventory_snapshots</div>
              </div>
              <div className="p-3 rounded-lg bg-slate-900/60 border border-slate-700/50">
                <div className="text-[10px] text-slate-400 uppercase font-semibold">Forecast Predictions</div>
                <div className="text-lg font-bold font-mono text-sky-400 mt-0.5">{result.forecast_count}</div>
                <div className="text-[10px] text-slate-500">XGBoost inference</div>
              </div>
              <div className="p-3 rounded-lg bg-slate-900/60 border border-slate-700/50">
                <div className="text-[10px] text-slate-400 uppercase font-semibold">Replenishments</div>
                <div className="text-lg font-bold font-mono text-emerald-400 mt-0.5">{result.replenishment_count}</div>
                <div className="text-[10px] text-slate-500">sim_replenishment_orders</div>
              </div>
            </div>

            <div className="flex flex-wrap items-center gap-3 pt-2 text-xs">
              <span className="text-slate-400">Reflected in Dashboard:</span>
              <span className="inline-flex items-center gap-1 px-2.5 py-1 rounded-md bg-emerald-500/10 text-emerald-400 border border-emerald-500/20 font-medium">
                <TrendingUp className="w-3.5 h-3.5" /> Forecasts Tab updated
              </span>
              <span className="inline-flex items-center gap-1 px-2.5 py-1 rounded-md bg-emerald-500/10 text-emerald-400 border border-emerald-500/20 font-medium">
                <Truck className="w-3.5 h-3.5" /> Inventory & Replenishments updated
              </span>
            </div>
          </div>
        )}

        {/* History Table */}
        {history.length > 0 && (
          <div className="space-y-2">
            <div className="text-xs font-semibold text-slate-400">Recent Simulation Runs</div>
            <div className="overflow-x-auto">
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b border-slate-700/50 text-slate-400">
                    <th className="text-left py-1.5 px-3">Run ID</th>
                    <th className="text-left py-1.5 px-3">Orders</th>
                    <th className="text-left py-1.5 px-3">Low-Stock</th>
                    <th className="text-left py-1.5 px-3">Forecasts</th>
                    <th className="text-left py-1.5 px-3">Replenishments</th>
                    <th className="text-left py-1.5 px-3">Status</th>
                    <th className="text-left py-1.5 px-3">Time</th>
                  </tr>
                </thead>
                <tbody>
                  {history.map((h) => (
                    <tr key={h.simulation_run_id} className="border-b border-slate-800 hover:bg-slate-800/40 font-mono">
                      <td className="py-2 px-3 text-sky-400">{h.simulation_run_id}</td>
                      <td className="py-2 px-3 text-slate-300">{h.orders_generated}</td>
                      <td className="py-2 px-3 text-amber-400">{h.low_stock_count}</td>
                      <td className="py-2 px-3 text-sky-300">{h.forecast_count}</td>
                      <td className="py-2 px-3 text-emerald-400">{h.replenishment_count}</td>
                      <td className="py-2 px-3">
                        <span className="px-1.5 py-0.5 rounded text-[10px] font-bold bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">
                          {h.status}
                        </span>
                      </td>
                      <td className="py-2 px-3 text-slate-500">{fmtDateTime(h.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

// ─── Step Progress Indicator ──────────────────────────────────────────────────

function StepProgress({ stepEvent }) {
  if (!stepEvent?.data?.action === 'STEP_EMITTED') return null;
  const d = stepEvent?.data || {};
  if (d.step_index == null || !d.total_steps) return null;

  const current = (d.step_index ?? 0) + 1;
  const total = d.total_steps ?? 1;
  const pct = Math.round((current / total) * 100);

  return (
    <div className="px-5 py-3 bg-sky-500/5 border-b border-sky-500/15 space-y-1.5">
      <div className="flex items-center justify-between text-xs">
        <span className="text-sky-300 font-medium flex items-center gap-1.5">
          <Activity className="w-3 h-3 animate-pulse" />
          Step {current} / {total} — {d.dataset_name ?? '—'}
        </span>
        <ScenarioBadge scenarioId={d.scenario_id} />
      </div>
      <div className="h-1 bg-slate-700 rounded-full overflow-hidden">
        <div
          className="h-full bg-sky-500 rounded-full transition-all duration-500"
          style={{ width: `${pct}%` }}
        />
      </div>
      <div className="flex justify-between text-[10px] text-slate-500">
        <span>Expected: <span className="font-mono">{d.expected_decision ?? '—'}</span></span>
        <span>{d.emitted_count ?? 0} events total</span>
      </div>
    </div>
  );
}

// ─── Dataset Readiness Grid ───────────────────────────────────────────────────

function DatasetGrid({ datasets, loading }) {
  if (loading) return <LoadingSpinner message="Loading simulated datasets…" />;
  if (!datasets.length) return (
    <EmptyState
      icon={Database}
      title="No simulated datasets"
      description="Simulated datasets are seeded during Phase 1. Run seed_sim_deps to initialise them."
    />
  );

  // Group by pipeline from name prefix
  const rows = datasets.map((d) => ({
    ...d,
    pipeline: d.dataset_name.startsWith('SIM_Regional')
      ? 'SIM-QUORUM-Pipeline'
      : d.dataset_name.startsWith('SIM_Raw') || d.dataset_name.startsWith('SIM_Catalog') || d.dataset_name.startsWith('SIM_Pricing')
        ? 'SIM-ALL-Pipeline'
        : d.dataset_name.startsWith('SIM_Primary') || d.dataset_name.startsWith('SIM_Backup')
          ? 'SIM-ANY-Pipeline'
          : d.dataset_name.startsWith('SIM_Inventory') || d.dataset_name.startsWith('SIM_Supplier')
            ? 'SIM-AST-Pipeline'
            : '—',
  }));

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between px-1 text-[11px] text-slate-400">
        <span className="flex items-center gap-1.5">
          <span className="w-2 h-2 rounded-full bg-amber-400 animate-pulse" />
          Virtual Simulation Feeds — 0 physical table rows (In-memory dependency evaluation)
        </span>
        <span className="font-mono text-slate-500">{rows.length} Feeds Registered</span>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-slate-700/50">
              {['Dataset Feed', 'Provenance', 'Status', 'Version', 'Pipeline', 'Last Updated'].map((h) => (
                <th key={h} className="text-left py-2 px-3 text-slate-400 font-medium whitespace-nowrap">{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((d) => (
              <tr key={d.dataset_id} className="border-b border-slate-700/20 hover:bg-slate-700/10 transition-colors">
                <td className="py-2.5 px-3 font-mono text-slate-200 whitespace-nowrap">{d.dataset_name}</td>
                <td className="py-2.5 px-3">
                  <span className="inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-bold uppercase tracking-wide bg-amber-500/10 text-amber-400 border border-amber-500/20">
                    SIMULATED
                  </span>
                </td>
                <td className="py-2.5 px-3"><StatusBadge status={d.status} size="sm" /></td>
                <td className="py-2.5 px-3 font-mono text-slate-400">v{d.current_version ?? 1}</td>
                <td className="py-2.5 px-3 text-slate-400">{d.pipeline}</td>
                <td className="py-2.5 px-3 font-mono text-slate-500">{fmtDateTime(d.last_updated_at)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ─── Decision Timeline ────────────────────────────────────────────────────────

const DECISION_FILTERS = ['ALL', 'TRIGGER', 'BLOCK'];

function DecisionTimeline({ decisions, loading, liveDecisions }) {
  const [filter, setFilter] = useState('ALL');
  const [scenarioFilter, setScenarioFilter] = useState('ALL');

  // Merge REST + live SSE decisions, deduplicate by decision_id/reason
  const merged = useMemo(() => {
    const map = new Map();
    [...(decisions || [])].forEach((d) => map.set(d.decision_id, d));
    liveDecisions.forEach((d) => {
      const key = `live-${d.timestamp || d.data?.timestamp}`;
      if (!map.has(key)) map.set(key, { ...d, _live: true });
    });
    return Array.from(map.values()).sort((a, b) => {
      const ta = new Date(a.evaluated_at || a.timestamp || 0).getTime();
      const tb = new Date(b.evaluated_at || b.timestamp || 0).getTime();
      return tb - ta;
    });
  }, [decisions, liveDecisions]);

  const filtered = useMemo(() => {
    return merged.filter((d) => {
      const dec = (d.decision || d.data?.decision || '').toUpperCase();
      const scen = d.scenario_id || d.data?.scenario_id || '';
      if (filter !== 'ALL' && dec !== filter) return false;
      if (scenarioFilter !== 'ALL' && scen !== scenarioFilter) return false;
      return true;
    });
  }, [merged, filter, scenarioFilter]);

  if (loading && !merged.length) return <LoadingSpinner message="Loading decisions…" />;
  if (!merged.length) return (
    <EmptyState
      icon={GitBranch}
      title="No decisions recorded"
      description="Dependency decisions will appear here in real-time as simulation events are emitted."
    />
  );

  return (
    <div className="space-y-3">
      {/* Filters */}
      <div className="flex flex-wrap items-center gap-2">
        <div className="flex items-center gap-1 rounded-lg border border-slate-700/60 bg-slate-900/40 p-0.5 text-xs">
          {DECISION_FILTERS.map((f) => (
            <button
              key={f}
              onClick={() => setFilter(f)}
              className={`px-2.5 py-1 rounded-md font-medium transition-all cursor-pointer ${
                filter === f
                  ? 'bg-sky-500/20 text-sky-400 font-semibold'
                  : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              {f}
            </button>
          ))}
        </div>

        <select
          value={scenarioFilter}
          onChange={(e) => setScenarioFilter(e.target.value)}
          className="rounded-lg border border-slate-700/60 bg-slate-900/60 px-2.5 py-1 text-xs text-slate-300 font-mono focus:outline-none cursor-pointer"
        >
          <option value="ALL">All Scenarios</option>
          {['S1_ALL', 'S2_ANY', 'S3_QUORUM', 'S4_AST', 'S5_CONTINUOUS'].map((s) => (
            <option key={s} value={s}>{s}</option>
          ))}
        </select>

        <span className="ml-auto text-[11px] text-slate-500 font-mono">
          {filtered.length} of {merged.length}
        </span>
      </div>

      {/* Decision list */}
      <div className="space-y-1.5 max-h-[380px] overflow-y-auto pr-1">
        {filtered.map((d, i) => {
          const dec = d.decision || d.data?.decision || '—';
          const pipeline = d.pipeline_name || d.data?.pipeline_name || '—';
          const scenario = d.scenario_id || d.data?.scenario_id;
          const stepIdx = d.step_index ?? d.data?.step_index;
          const dataset = d.dataset_name || d.data?.dataset_name || '—';
          const reason = d.reason || d.data?.reason || '—';
          const ts = d.evaluated_at || d.timestamp;
          const isLive = d._live;

          return (
            <div
              key={d.decision_id || `live-${i}`}
              className="flex items-center justify-between gap-3 p-3 rounded-lg bg-slate-900/40 border border-slate-700/30 hover:border-slate-700/60 transition-colors text-xs"
            >
              <div className="flex-1 min-w-0 space-y-0.5">
                <div className="flex items-center gap-2 flex-wrap">
                  <StatusBadge status={dec} size="sm" />
                  <span className="text-[10px] font-mono text-slate-300">{pipeline}</span>
                  {scenario && <ScenarioBadge scenarioId={scenario} />}
                  {stepIdx != null && (
                    <span className="text-[10px] text-slate-500">step {Number(stepIdx) + 1}</span>
                  )}
                  {isLive && (
                    <span className="text-[9px] px-1.5 py-0.5 rounded bg-sky-500/20 text-sky-400 border border-sky-500/30 font-semibold">
                      LIVE
                    </span>
                  )}
                </div>
                <div className="text-[10px] text-slate-400 font-mono truncate" title={dataset}>
                  Dataset: {dataset}
                </div>
                {reason !== '—' && (
                  <div className="text-[10px] text-slate-500 truncate" title={reason}>
                    {reason}
                  </div>
                )}
              </div>
              <span className="text-[10px] text-slate-500 font-mono flex-shrink-0">{fmt(ts)}</span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

// ─── Simulated Execution History ──────────────────────────────────────────────

function ExecutionHistory({ executions, loading }) {
  if (loading) return <LoadingSpinner message="Loading simulated executions…" />;
  if (!executions.length) return (
    <EmptyState
      icon={PlayCircle}
      title="No simulated executions"
      description="Pipeline executions triggered by simulated TRIGGER decisions will appear here."
    />
  );

  return (
    <div className="space-y-3">
      {/* Simulation Transparency Disclaimer */}
      <div className="p-3.5 rounded-xl bg-amber-500/10 border border-amber-500/30 text-amber-200 text-xs flex items-start gap-3">
        <AlertTriangle className="w-4 h-4 text-amber-400 flex-shrink-0 mt-0.5" />
        <div className="leading-relaxed">
          <span className="font-bold text-amber-300 uppercase tracking-wide mr-1.5">Simulation Boundary Notice:</span>
          Simulated executions evaluate event-conditioned dependency rules in a synthetic environment. They do not execute actual Airflow DAGs, ML model training, or XGBoost inference pipelines.
        </div>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-slate-700/50">
              {['ID', 'Scope', 'Pipeline', 'Status', 'Simulation ID', 'Started', 'Duration', 'Output'].map((h) => (
                <th key={h} className="text-left py-2 px-3 text-slate-400 font-medium whitespace-nowrap">{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {executions.map((e) => {
              const dur = e.duration_seconds
                ? `${Math.round(e.duration_seconds)}s`
                : e.started_at && !e.completed_at
                  ? elapsed(e.started_at)
                  : '—';
              return (
                <tr key={e.execution_id} className="border-b border-slate-700/20 hover:bg-slate-700/10 transition-colors">
                  <td className="py-2.5 px-3 font-mono text-slate-400">#{e.execution_id}</td>
                  <td className="py-2.5 px-3">
                    <span className="text-[10px] px-1.5 py-0.5 rounded font-bold uppercase bg-amber-500/10 text-amber-400 border border-amber-500/20">
                      SIMULATED
                    </span>
                  </td>
                  <td className="py-2.5 px-3 text-slate-200 whitespace-nowrap max-w-[200px] truncate" title={e.pipeline_name}>
                    <span className="inline-flex items-center gap-1.5">
                      <span className="text-[9px] px-1 py-0.5 rounded bg-sky-500/15 text-sky-400 border border-sky-500/20 font-mono">SIM</span>
                      {e.pipeline_name}
                    </span>
                  </td>
                  <td className="py-2.5 px-3"><StatusBadge status={e.status} size="sm" /></td>
                  <td className="py-2.5 px-3 font-mono text-slate-500 text-[10px] max-w-[140px] truncate" title={e.simulation_id}>
                    {e.simulation_id ? e.simulation_id.slice(0, 12) + '…' : '—'}
                  </td>
                  <td className="py-2.5 px-3 font-mono text-slate-400 whitespace-nowrap">{fmtDateTime(e.started_at)}</td>
                  <td className="py-2.5 px-3 font-mono text-slate-400">{dur}</td>
                  <td className="py-2.5 px-3 font-mono text-slate-500 text-[10px] max-w-[180px] truncate" title={e.output_location}>
                    {e.output_location ?? '—'}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ─── Main Page ────────────────────────────────────────────────────────────────

export default function Simulation({ refreshTrigger, sseStatus, sseEvents, latestEvent }) {
  const [simStatus, setSimStatus] = useState(null);
  const [datasets, setDatasets] = useState([]);
  const [datasetsLoading, setDatasetsLoading] = useState(true);
  const [executions, setExecutions] = useState([]);
  const [executionsLoading, setExecutionsLoading] = useState(true);
  const [decisions, setDecisions] = useState([]);
  const [decisionsLoading, setDecisionsLoading] = useState(true);
  const [lastRefreshed, setLastRefreshed] = useState(null);

  // SSE-derived live decisions (RULE_DECISION_EVALUATED for SIM- pipelines)
  const [liveDecisions, setLiveDecisions] = useState([]);
  // Latest STEP_EMITTED event
  const [stepEvent, setStepEvent] = useState(null);

  const fetchAll = useCallback(async () => {
    setDatasetsLoading(true);
    setExecutionsLoading(true);
    setDecisionsLoading(true);

    try {
      const [dsets, execs, decs] = await Promise.all([
        getSimulatedDatasets().catch(() => []),
        getSimulatedExecutions({ limit: 50 }).catch(() => []),
        getSimulatedDecisions({ limit: 80 }).catch(() => []),
      ]);
      setDatasets(dsets || []);
      setExecutions(execs || []);
      setDecisions(decs || []);
    } catch {
      // non-fatal
    }

    setDatasetsLoading(false);
    setExecutionsLoading(false);
    setDecisionsLoading(false);
    setLastRefreshed(new Date().toLocaleTimeString());
  }, []);

  useEffect(() => { fetchAll(); }, [fetchAll, refreshTrigger]);

  // ── React to SSE ───────────────────────────────────────────────────────
  useEffect(() => {
    if (!latestEvent) return;
    const { event_type, data } = latestEvent;

    // Dataset readiness changed → refresh grid
    if (event_type === 'DATASET_READINESS_CHANGED' && data?.source_type === 'SIMULATED') {
      setDatasets((prev) =>
        prev.map((d) =>
          d.dataset_name === data.dataset_name
            ? { ...d, status: data.status, last_updated_at: data.timestamp }
            : d
        )
      );
    }

    // Step emitted → update step progress indicator
    if (event_type === 'SIMULATION_STATUS_CHANGED' && data?.action === 'STEP_EMITTED') {
      setStepEvent(latestEvent);
    }

    // Clear step progress on STOPPED/IDLE
    if (event_type === 'SIMULATION_STATUS_CHANGED' && ['STOPPED', 'PAUSED'].includes(data?.action)) {
      setStepEvent(null);
    }

    // New decision for simulated pipeline
    if (event_type === 'RULE_DECISION_EVALUATED' && data?.pipeline_name?.startsWith('SIM-')) {
      setLiveDecisions((prev) => [latestEvent, ...prev].slice(0, 80));
    }

    // Execution triggered → refresh execution list
    if (
      (event_type === 'PIPELINE_STATUS_CHANGED' || event_type === 'RULE_DECISION_EVALUATED') &&
      data?.pipeline_name?.startsWith('SIM-') &&
      data?.decision === 'TRIGGER'
    ) {
      getSimulatedExecutions({ limit: 50 }).then(setExecutions).catch(() => {});
    }
  }, [latestEvent]);

  return (
    <div className="space-y-6">
      {/* ── Page Header ─────────────────────────────────────────────────── */}
      <div className="flex items-start justify-between gap-4">
        <div className="flex items-center gap-3">
          <div className="p-2 rounded-xl bg-sky-500/10 border border-sky-500/20">
            <FlaskConical className="w-5 h-5 text-sky-400" />
          </div>
          <div>
            <h1 className="text-xl font-bold text-white tracking-tight">Simulation Lab</h1>
            <p className="text-xs text-slate-400 mt-0.5">
              Real-time supply-chain dependency simulation — isolated from operational data
            </p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          {lastRefreshed && (
            <span className="text-[10px] text-slate-500 font-mono">updated {lastRefreshed}</span>
          )}
          <button
            onClick={fetchAll}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-slate-700 bg-slate-800 text-xs text-slate-300 hover:text-white hover:border-slate-600 transition-all cursor-pointer"
          >
            <RefreshCw className="w-3 h-3" />
            Refresh
          </button>
        </div>
      </div>

      {/* ── Safety Notice ────────────────────────────────────────────────── */}
      <div className="flex items-start gap-2.5 px-4 py-3 rounded-xl bg-slate-800/50 border border-slate-700/50 text-xs text-slate-400">
        <Info className="w-4 h-4 text-sky-400 flex-shrink-0 mt-0.5" />
        <span>
          Simulation events are <span className="text-slate-200 font-medium">isolated from operational data</span>.
          Inventory, Olist records, replenishments, and protected executions (#36, #99586, #99653) are never modified.
          Only <span className="font-mono text-sky-400">SIM-*</span> pipelines and <span className="font-mono text-sky-400">source_type = 'SIMULATED'</span> datasets are affected.
        </span>
      </div>

      {/* ── Olist Historical Order Replay Simulator ──────────────────────── */}
      <OlistReplayControlPanel refreshTrigger={refreshTrigger} />

      {/* ── End-to-End Simulation Runner (Phase F3) ───────────────────────── */}
      <E2ESimulationPanel onRunComplete={fetchAll} />

      {/* ── Top Grid: Control + Overview ─────────────────────────────────── */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Control Panel */}
        <Section icon={Layers} title="Simulation Controls">
          <SimulationControlPanel
            latestEvent={latestEvent}
            sseStatus={sseStatus}
            onStatusChange={setSimStatus}
          />
        </Section>

        {/* Live Overview */}
        <Section
          icon={Activity}
          title="Live Overview"
          action={
            stepEvent?.data?.action === 'STEP_EMITTED' && simStatus?.state === 'RUNNING'
              ? null : undefined
          }
        >
          {/* Step progress bar (shown only when RUNNING + STEP_EMITTED) */}
          {stepEvent?.data?.action === 'STEP_EMITTED' && simStatus?.state === 'RUNNING' && (
            <div className="-mx-5 -mt-5 mb-4">
              <StepProgress stepEvent={stepEvent} />
            </div>
          )}
          <LiveOverview status={simStatus} />
        </Section>
      </div>

      {/* ── Scenario Architecture Guide ─────────────────────────────────── */}
      <ScenarioArchitectureGuide />

      {/* ── Dataset Readiness Grid ────────────────────────────────────────── */}
      <Section
        icon={Database}
        title="Simulated Dataset Readiness"
        badge={`${datasets.length} datasets`}
      >
        <DatasetGrid datasets={datasets} loading={datasetsLoading} />
      </Section>

      {/* ── Decision Timeline ─────────────────────────────────────────────── */}
      <Section
        icon={GitBranch}
        title="Dependency Decision Timeline"
        badge={decisions.length + liveDecisions.length > 0 ? `${decisions.length + liveDecisions.length} records` : undefined}
      >
        <DecisionTimeline
          decisions={decisions}
          loading={decisionsLoading}
          liveDecisions={liveDecisions}
        />
      </Section>

      {/* ── Execution History ─────────────────────────────────────────────── */}
      <Section
        icon={PlayCircle}
        title="Simulated Pipeline Executions"
        badge={executions.length > 0 ? `${executions.length} executions` : undefined}
      >
        <ExecutionHistory executions={executions} loading={executionsLoading} />
      </Section>
    </div>
  );
}
