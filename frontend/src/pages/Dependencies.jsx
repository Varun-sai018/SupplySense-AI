import React, { useState, useEffect, useMemo, useRef } from 'react';
import { 
  GitBranch, 
  CheckCircle2, 
  ShieldAlert, 
  Layers, 
  ArrowRight, 
  Info, 
  AlertCircle, 
  Database,
  Sliders,
  Clock,
  Zap,
  Filter,
  ShoppingCart,
  Package,
  CreditCard,
  RefreshCw,
  Check,
  Activity,
  Radio,
  Eye,
  Code2,
  Terminal,
  Copy,
  X,
  Play,
  Pause,
  TrendingUp,
  Box,
  Send
} from 'lucide-react';
import { getDependencies, getDependencyDecisions, getDependencyEvents } from '../api/client';
import StatusBadge from '../components/StatusBadge';
import ConditionBadge from '../components/ConditionBadge';
import LoadingSpinner from '../components/LoadingSpinner';
import EmptyState from '../components/EmptyState';

// Verified business rules configured in SupplySense AI
const CONFIGURED_BUSINESS_RULES = [
  {
    id: 'rule_a',
    name: 'Rule A: High-Velocity Cart Abandonment Engine',
    event_topics: ['CART_ABANDONMENT', 'CART_DROP_OFF'],
    condition_type: 'EXPRESSION / AST',
    ast_expression: 'AND(dropoff_rate > 0.25, peak_hour == true)',
    evaluation_scope: 'Real-time user session dropoff rate vs timezone peak trading windows: 18:00 - 22:00 (Simulated BRT / UTC-3)',
    action: 'Dispatches personalized customer re-engagement webhook',
    target_pipeline: 'Rule A: High-Velocity Cart Abandonment Engine',
    icon: ShoppingCart,
    color: 'emerald'
  },
  {
    id: 'rule_b',
    name: 'Rule B: Automated Inventory Reorder Pipeline',
    event_topics: ['INVENTORY_REORDER', 'INVENTORY_CHECK'],
    condition_type: 'QUORUM / AST',
    ast_expression: 'AND(inventory_level <= safe_threshold, supplier_status == Online)',
    evaluation_scope: 'Current SKU stock level vs safety buffer + active supplier health (Quorum threshold)',
    action: 'Dispatches supplier reorder API and dispatches Inventory Reorder Pipeline',
    target_pipeline: 'Inventory Reorder Pipeline',
    icon: Package,
    color: 'sky'
  },
  {
    id: 'rule_c',
    name: 'Rule C: Payment Failure Spike Detection',
    event_topics: ['PAYMENT_SPIKE', 'PAYMENT_FAILURE_SPIKE'],
    condition_type: 'EXPRESSION / AST',
    ast_expression: 'success_rate < 0.92 (rolling 50 tx window)',
    evaluation_scope: 'Rolling 50-transaction window detecting sudden gateway or payment drop-offs',
    action: 'Dispatches high-priority payment security & checkout incident alert',
    target_pipeline: 'Rule C: Payment Failure Spike Detection',
    icon: CreditCard,
    color: 'amber'
  }
];

// Helper to determine dataset classification badge styling
function getDatasetStyle(datasetName = '') {
  const name = datasetName.toLowerCase();
  if (name.includes('sp') || name.includes('rj') || name.includes('mg') || name.includes('rs')) {
    return {
      category: 'REGIONAL HUB',
      badgeClass: 'bg-emerald-500/10 text-emerald-400 border-emerald-500/30',
      icon: Box
    };
  }
  if (name.includes('boleto') || name.includes('credit') || name.includes('voucher') || name.includes('payment')) {
    return {
      category: 'PAYMENT RAIL',
      badgeClass: 'bg-indigo-500/10 text-indigo-400 border-indigo-500/30',
      icon: CreditCard
    };
  }
  if (name.includes('operational')) {
    return {
      category: 'OPERATIONAL',
      badgeClass: 'bg-amber-500/10 text-amber-400 border-amber-500/30',
      icon: Activity
    };
  }
  return {
    category: 'OLIST CORE',
    badgeClass: 'bg-sky-500/10 text-sky-400 border-sky-500/30',
    icon: Database
  };
}

export default function Dependencies({ refreshTrigger }) {
  const [dependencies, setDependencies] = useState([]);
  const [decisions, setDecisions] = useState([]);
  const [events, setEvents] = useState([]);
  const [eventsTotalCount, setEventsTotalCount] = useState(0);

  const [loading, setLoading] = useState(true);
  const [decisionsLoading, setDecisionsLoading] = useState(true);
  const [eventsLoading, setEventsLoading] = useState(true);
  const [error, setError] = useState(null);

  // Filters
  const [scopeFilter, setScopeFilter] = useState('OPERATIONAL'); // 'OPERATIONAL' | 'SIMULATED' | 'ALL'
  const [pipelineFilter, setPipelineFilter] = useState('ALL');
  const [decisionFilter, setDecisionFilter] = useState('ALL'); // 'ALL' | 'TRIGGER' | 'BLOCK'
  
  // Events filter
  const [eventDatasetFilter, setEventDatasetFilter] = useState('ALL');
  const [eventStatusFilter, setEventStatusFilter] = useState('ALL');
  const [autoRefresh, setAutoRefresh] = useState(true);

  // Modal inspection
  const [selectedEventModal, setSelectedEventModal] = useState(null);
  const [copiedPayload, setCopiedPayload] = useState(false);

  const decisionsRef = useRef(null);
  const eventsRef = useRef(null);

  // 1. Fetch pipeline dependencies (only display spinner on initial load to prevent jarring UI flashes)
  useEffect(() => {
    let isMounted = true;
    if (dependencies.length === 0) {
      setLoading(true);
    }

    getDependencies()
      .then((data) => {
        if (isMounted) {
          setDependencies(data || []);
          setError(null);
        }
      })
      .catch((err) => {
        if (isMounted && dependencies.length === 0) {
          setError(err.response?.data?.detail || err.message || 'Failed to retrieve dependency evaluation states');
        }
      })
      .finally(() => {
        if (isMounted) setLoading(false);
      });

    return () => {
      isMounted = false;
    };
  }, [refreshTrigger]);

  // 2. Fetch decisions audit log with scope and pipeline filters (silent background in-place updates)
  useEffect(() => {
    let isMounted = true;
    if (decisions.length === 0) {
      setDecisionsLoading(true);
    }

    const params = {
      limit: 50,
      scope: scopeFilter
    };
    if (pipelineFilter !== 'ALL') {
      params.pipeline_name = pipelineFilter;
    }

    getDependencyDecisions(params)
      .then((data) => {
        if (isMounted) {
          const list = Array.isArray(data) ? data : (data?.items || []);
          setDecisions(list);
        }
      })
      .catch(() => {
        if (isMounted && decisions.length === 0) {
          setDecisions([]);
        }
      })
      .finally(() => {
        if (isMounted) setDecisionsLoading(false);
      });

    return () => {
      isMounted = false;
    };
  }, [refreshTrigger, scopeFilter, pipelineFilter]);

  // 3. Fetch incoming events arriving at Dependency Engine (smooth in-place updates)
  const fetchIncomingEvents = (silent = true) => {
    if (!silent && events.length === 0) setEventsLoading(true);

    const params = {
      limit: 20
    };
    if (eventDatasetFilter !== 'ALL') params.dataset_name = eventDatasetFilter;
    if (eventStatusFilter !== 'ALL') params.event_status = eventStatusFilter;

    getDependencyEvents(params)
      .then((data) => {
        const items = data?.items || [];
        setEvents(items);
        setEventsTotalCount(data?.total_count ?? items.length);
      })
      .catch((err) => {
        console.error('Failed to load dependency events:', err);
      })
      .finally(() => {
        if (!silent) setEventsLoading(false);
      });
  };

  useEffect(() => {
    // Only show full spinner on very first mount; subsequent updates update quietly in place
    fetchIncomingEvents(events.length === 0 ? false : true);
  }, [refreshTrigger, eventDatasetFilter, eventStatusFilter]);

  // 4. Live Auto-Polling Interval (calm 6s interval, pauses when inspecting payload modal)
  useEffect(() => {
    if (!autoRefresh || selectedEventModal) return;

    const intervalId = setInterval(() => {
      fetchIncomingEvents(true);
      // Silently re-fetch dependencies & decisions so cards update in real time without flashing
      getDependencies().then((data) => { 
        if (data) {
          setDependencies(data);
          setLoading(false);
        }
      }).catch(() => {});
      const pParams = { limit: 50, scope: scopeFilter };
      if (pipelineFilter !== 'ALL') pParams.pipeline_name = pipelineFilter;
      getDependencyDecisions(pParams).then((data) => {
        if (data) setDecisions(Array.isArray(data) ? data : (data.items || []));
      }).catch(() => {});
    }, 6000);

    return () => clearInterval(intervalId);
  }, [autoRefresh, selectedEventModal, scopeFilter, pipelineFilter, eventDatasetFilter, eventStatusFilter]);

  // Filtered decisions list
  const filteredDecisions = useMemo(() => {
    if (decisionFilter === 'ALL') return decisions;
    return decisions.filter((d) => d.decision === decisionFilter);
  }, [decisions, decisionFilter]);

  // Copy payload helper
  const handleCopyJson = (obj) => {
    navigator.clipboard.writeText(JSON.stringify(obj, null, 2));
    setCopiedPayload(true);
    setTimeout(() => setCopiedPayload(false), 2000);
  };

  return (
    <div className="space-y-6">
      {/* Page Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div>
          <h2 className="text-2xl font-bold text-white tracking-tight flex items-center gap-2.5">
            <span>Event-Conditioned Dependency Engine</span>
            <span className="flex items-center gap-1.5 px-2.5 py-0.5 rounded-full bg-emerald-500/10 border border-emerald-500/30 text-emerald-400 text-xs font-mono font-medium">
              <span className="w-2 h-2 rounded-full bg-emerald-400 animate-pulse"></span>
              LIVE ENGINE ACTIVE
            </span>
          </h2>
          <p className="text-xs sm:text-sm text-slate-400 mt-1">
            Intercepting Kafka & CDC event arrivals, evaluating Multi-Table conditions (ALL, QUORUM, ANY), and executing downstream transformations.
          </p>
        </div>

        {/* Live Auto-Refresh Controller */}
        <div className="flex items-center gap-2.5">
          <button
            onClick={() => setAutoRefresh(!autoRefresh)}
            className={`flex items-center gap-2 px-3 py-1.5 rounded-xl text-xs font-semibold border transition-all cursor-pointer ${
              autoRefresh 
                ? 'bg-emerald-500/10 border-emerald-500/30 text-emerald-300 hover:bg-emerald-500/20' 
                : 'bg-slate-800 border-slate-700 text-slate-400 hover:text-white'
            }`}
            title={autoRefresh ? 'Pause live auto-polling' : 'Enable live auto-polling (every 3.5s)'}
          >
            {autoRefresh ? <Pause className="w-3.5 h-3.5 text-emerald-400" /> : <Play className="w-3.5 h-3.5 text-slate-400" />}
            <span>Auto-Refresh: {autoRefresh ? 'ON (3.5s)' : 'PAUSED'}</span>
          </button>

          <button
            onClick={() => fetchIncomingEvents(false)}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-xl bg-slate-800/90 border border-slate-700 hover:border-slate-600 text-xs font-semibold text-sky-400 hover:text-white transition-all cursor-pointer"
            title="Poll immediate event updates"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${eventsLoading ? 'animate-spin' : ''}`} />
            <span>Poll Now</span>
          </button>
        </div>
      </div>

      {/* Core Research Contribution Callout Banner */}
      <div className="bg-gradient-to-r from-sky-950/60 to-indigo-950/60 border border-sky-500/30 rounded-2xl p-5 shadow-lg shadow-sky-950/20">
        <div className="flex items-start gap-3.5">
          <div className="p-2.5 bg-sky-500/20 border border-sky-500/30 rounded-xl text-sky-400 flex-shrink-0">
            <GitBranch className="w-5 h-5" />
          </div>
          <div>
            <h3 className="text-sm font-bold text-white uppercase tracking-wider">
              Core Capstone Research Contribution: Event-Conditioned Orchestration
            </h3>
            <p className="mt-1 text-xs sm:text-sm text-slate-300 leading-relaxed italic">
              "Traditional orchestration systems trigger pipelines on rigid, clock-based schedules (cron), often training ML models on stale or desynchronized data. SupplySense AI dynamically intercepts upstream events and evaluates condition rules (ALL, ANY, QUORUM, AST) to trigger execution only when verified dependencies are satisfied."
            </p>
          </div>
        </div>
      </div>

      {/* Safety & Idempotency Telemetry Bar */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="p-4 bg-slate-800/80 border border-slate-700/70 rounded-xl flex items-center gap-3">
          <div className="p-2 rounded-lg bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">
            <CheckCircle2 className="w-5 h-5" />
          </div>
          <div>
            <div className="text-xs font-bold text-white">Idempotency Protection</div>
            <div className="text-[11px] text-slate-400">Duplicate event IDs skipped via SQL unique constraint</div>
          </div>
        </div>

        <div className="p-4 bg-slate-800/80 border border-slate-700/70 rounded-xl flex items-center gap-3">
          <div className="p-2 rounded-lg bg-sky-500/10 text-sky-400 border border-sky-500/20">
            <Clock className="w-5 h-5" />
          </div>
          <div>
            <div className="text-xs font-bold text-white">Pipeline Cooldown Enforcement</div>
            <div className="text-[11px] text-slate-400">Guarantees safety window between consecutive runs</div>
          </div>
        </div>

        <div className="p-4 bg-slate-800/80 border border-slate-700/70 rounded-xl flex items-center gap-3">
          <div className="p-2 rounded-lg bg-indigo-500/10 text-indigo-400 border border-indigo-500/20">
            <RefreshCw className="w-5 h-5" />
          </div>
          <div>
            <div className="text-xs font-bold text-white">Automated Cycle Reset</div>
            <div className="text-[11px] text-slate-400">Datasets atomically reset to WAITING upon TRIGGER</div>
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

      {/* ========================================================================= */}
      {/* 1. REAL-TIME INCOMING EVENTS STREAM (LIVE EVENT BUS) */}
      {/* ========================================================================= */}
      <div ref={eventsRef} className="bg-slate-800/80 border border-slate-700/70 rounded-2xl overflow-hidden shadow-sm">
        {/* Stream Header */}
        <div className="px-5 py-4 border-b border-slate-700/60 bg-slate-800/60 flex flex-col lg:flex-row lg:items-center justify-between gap-3">
          <div>
            <div className="flex items-center gap-2">
              <Radio className="w-4 h-4 text-emerald-400 animate-pulse" />
              <h3 className="text-sm font-bold text-white uppercase tracking-wider">
                Live Ingestion Stream & Incoming Events Feed
              </h3>
              <span className="text-[10px] font-mono font-bold px-2 py-0.5 rounded-full bg-sky-500/10 text-sky-400 border border-sky-500/30">
                KAFKA TOPIC: [dataset-events]
              </span>
            </div>
            <p className="text-xs text-slate-400 mt-1">
              Events streamed chronologically from real Brazilian E-Commerce data (Part 2 Simulation), arriving at the engine for condition evaluation.
            </p>
          </div>

          <div className="flex flex-wrap items-center gap-2.5">
            {/* Dataset Filter */}
            <div className="flex items-center gap-2 bg-slate-900/80 border border-slate-700 rounded-xl px-2.5 py-1 text-xs">
              <Database className="w-3.5 h-3.5 text-sky-400" />
              <span className="text-slate-400 font-medium">Dataset:</span>
              <select
                value={eventDatasetFilter}
                onChange={(e) => setEventDatasetFilter(e.target.value)}
                className="bg-transparent text-white font-semibold focus:outline-none cursor-pointer max-w-[150px] truncate"
              >
                <option value="ALL" className="bg-slate-900 text-white">All Datasets ({eventsTotalCount})</option>
                <option value="Olist_Orders_SP" className="bg-slate-900 text-white">Olist_Orders_SP (Quorum)</option>
                <option value="Olist_Orders_RJ" className="bg-slate-900 text-white">Olist_Orders_RJ (Quorum)</option>
                <option value="Olist_Orders_MG" className="bg-slate-900 text-white">Olist_Orders_MG (Quorum)</option>
                <option value="Olist_Orders_RS" className="bg-slate-900 text-white">Olist_Orders_RS (Quorum)</option>
                <option value="Olist_Payments_Boleto" className="bg-slate-900 text-white">Olist_Payments_Boleto (ANY)</option>
                <option value="Olist_Payments_CreditCard" className="bg-slate-900 text-white">Olist_Payments_CreditCard (ANY)</option>
                <option value="Olist_Payments_Voucher" className="bg-slate-900 text-white">Olist_Payments_Voucher (ANY)</option>
                <option value="Olist_Orders" className="bg-slate-900 text-white">Olist_Orders (Core ALL)</option>
                <option value="Olist_Order_Items" className="bg-slate-900 text-white">Olist_Order_Items (Core ALL)</option>
                <option value="Olist_Payments" className="bg-slate-900 text-white">Olist_Payments (Core ALL)</option>
                <option value="Operational Orders" className="bg-slate-900 text-white">Operational Orders (Store POS)</option>
                <option value="Operational Demand Features" className="bg-slate-900 text-white">Operational Demand Features</option>
              </select>
            </div>

            {/* Status Filter */}
            <div className="flex items-center gap-2 bg-slate-900/80 border border-slate-700 rounded-xl px-2.5 py-1 text-xs">
              <Filter className="w-3.5 h-3.5 text-indigo-400" />
              <span className="text-slate-400 font-medium">Status:</span>
              <select
                value={eventStatusFilter}
                onChange={(e) => setEventStatusFilter(e.target.value)}
                className="bg-transparent text-white font-semibold focus:outline-none cursor-pointer"
              >
                <option value="ALL" className="bg-slate-900 text-white">All Statuses</option>
                <option value="COMPLETED" className="bg-slate-900 text-white">COMPLETED</option>
                <option value="NEW" className="bg-slate-900 text-white">NEW (Pending Claim)</option>
                <option value="PROCESSING" className="bg-slate-900 text-white">PROCESSING</option>
              </select>
            </div>
          </div>
        </div>

        {/* Events Feed Content */}
        {eventsLoading && events.length === 0 ? (
          <div className="p-8">
            <LoadingSpinner message="Connecting to Dependency Engine event stream..." />
          </div>
        ) : events.length === 0 ? (
          <div className="p-8">
            <EmptyState
              title="No incoming events found"
              description="No recent stream events matching the filter criteria. Backend automatic simulation generates events every ~4 seconds."
            />
          </div>
        ) : (
          <div className="divide-y divide-slate-700/50">
            {events.map((evt, idx) => {
              const style = getDatasetStyle(evt.dataset_name);
              const DatasetIcon = style.icon;
              const dec = evt.linked_decision;
              const isTrigger = dec?.decision === 'TRIGGER';
              const isBlock = dec?.decision === 'BLOCK';
              const isLatest = idx === 0;

              return (
                <div 
                  key={evt.event_id}
                  className={`p-4 transition-all hover:bg-slate-750/50 ${
                    isLatest ? 'bg-sky-500/[0.03]' : 'bg-transparent'
                  }`}
                >
                  <div className="flex flex-col lg:flex-row lg:items-center justify-between gap-3.5">
                    {/* Left: Event ID, Dataset Tag, Event Type */}
                    <div className="flex items-start sm:items-center gap-3">
                      <div className="flex flex-col items-center justify-center w-14 py-1.5 rounded-xl bg-slate-900/90 border border-slate-700 text-center font-mono flex-shrink-0">
                        <span className="text-[10px] text-slate-400 uppercase font-semibold">EVENT</span>
                        <span className="text-sm font-black text-sky-400">#{evt.event_id}</span>
                      </div>

                      <div className="space-y-1">
                        <div className="flex flex-wrap items-center gap-2">
                          <span className={`inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-lg border text-xs font-bold ${style.badgeClass}`}>
                            <DatasetIcon className="w-3.5 h-3.5" />
                            <span>{evt.dataset_name}</span>
                          </span>

                          <span className="text-[10px] font-mono px-2 py-0.5 rounded bg-slate-900 border border-slate-700/80 text-slate-300">
                            {evt.event_type}
                          </span>

                          <span className="text-[10px] font-mono text-slate-400 flex items-center gap-1">
                            <Clock className="w-3 h-3 text-slate-500" />
                            <span>{evt.event_time}</span>
                          </span>
                        </div>

                        {/* Realistic Business Summary */}
                        <p className="text-xs text-slate-300 font-medium leading-relaxed">
                          {evt.realistic_context?.summary || `${evt.rows_changed} rows changed in version v${evt.dataset_version}.`}
                        </p>
                      </div>
                    </div>

                    {/* Middle: Target Pipeline & Condition Rule */}
                    <div className="flex items-center gap-2 bg-slate-900/80 border border-slate-700/80 px-3 py-1.5 rounded-xl text-xs flex-shrink-0">
                      <div className="flex items-center gap-1.5 text-slate-400">
                        <ArrowRight className="w-3.5 h-3.5 text-indigo-400" />
                        <span className="font-semibold text-slate-200 truncate max-w-[200px]">
                          {evt.pipeline_name}
                        </span>
                      </div>
                      <ConditionBadge condition={evt.condition_type} />
                    </div>

                    {/* Right: Decision Evaluation Outcome & Inspect Button */}
                    <div className="flex items-center justify-between sm:justify-end gap-3 flex-shrink-0">
                      {dec ? (
                        <div className="text-right">
                          <span className={`inline-flex items-center gap-1 px-2.5 py-1 rounded-lg text-xs font-bold border ${
                            isTrigger
                              ? 'bg-emerald-500/15 border-emerald-500/30 text-emerald-300'
                              : 'bg-amber-500/15 border-amber-500/30 text-amber-300'
                          }`}>
                            {isTrigger ? <Check className="w-3.5 h-3.5" /> : <ShieldAlert className="w-3.5 h-3.5" />}
                            <span>{dec.decision}: {dec.ready_count}/{dec.total_required} READY</span>
                          </span>
                          <div className="text-[10px] font-mono text-slate-400 mt-0.5 truncate max-w-[180px]" title={dec.reason}>
                            {dec.reason}
                          </div>
                        </div>
                      ) : (
                        <span className="inline-flex items-center gap-1 px-2.5 py-1 rounded-lg text-xs font-mono bg-slate-900 border border-slate-700 text-slate-400">
                          <Clock className="w-3 h-3 text-slate-500" />
                          <span>Status: {evt.event_status}</span>
                        </span>
                      )}

                      <button
                        onClick={() => setSelectedEventModal(evt)}
                        className="flex items-center gap-1.5 px-3 py-1.5 rounded-xl bg-slate-900 hover:bg-slate-700 border border-slate-700 hover:border-slate-600 text-xs font-semibold text-sky-400 hover:text-white transition-all cursor-pointer"
                        title="Inspect realistic JSON payload & Kafka transport metadata"
                      >
                        <Eye className="w-3.5 h-3.5" />
                        <span>Inspect Payload</span>
                      </button>
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        )}

        {/* Stream Footer */}
        <div className="px-5 py-3 border-t border-slate-700/60 bg-slate-900/40 text-xs text-slate-400 flex flex-col sm:flex-row sm:items-center justify-between gap-2 font-sans">
          <div className="flex items-center gap-2">
            <span className="w-2 h-2 rounded-full bg-emerald-400"></span>
            <span>Displaying <strong>{events.length}</strong> of <strong>{eventsTotalCount}</strong> incoming events in live buffer.</span>
          </div>
          <span className="text-slate-500 text-[11px] font-mono">
            Directly consumed via MySQL dataset_events &amp; Apache Kafka
          </span>
        </div>
      </div>

      {/* ========================================================================= */}
      {/* 2. PRIMARY PIPELINE DEPENDENCY EVALUATION (Configured Rules) */}
      {/* ========================================================================= */}
      <div>
        <div className="flex items-center justify-between mb-3">
          <h3 className="text-base font-bold text-white tracking-wide flex items-center gap-2">
            <Layers className="w-5 h-5 text-sky-400" />
            <span>Configured Pipeline Dependencies &amp; Readiness States</span>
          </h3>
          <span className="text-[11px] font-mono text-slate-400">
            {dependencies.length} Authoritative Pipelines
          </span>
        </div>

        {loading && dependencies.length === 0 ? (
          <LoadingSpinner message="Evaluating active pipeline dependencies..." />
        ) : dependencies.length === 0 ? (
          <EmptyState title="No dependencies found" description="No pipeline dependency rules configured." />
        ) : (
          <div className="space-y-4">
            {dependencies.map((dep) => {
              const dec = dep.latest_decision;
              const isTrigger = dec?.decision === 'TRIGGER';

              return (
                <div 
                  key={dep.dependency_id}
                  className="bg-slate-800/80 border border-slate-700/70 rounded-2xl p-6 shadow-sm hover:border-slate-600 transition-all"
                >
                  {/* Pipeline Title & Condition Rule */}
                  <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 pb-4 border-b border-slate-700/60">
                    <div className="flex items-center gap-3">
                      <div className="p-2.5 bg-sky-500/10 border border-sky-500/20 rounded-xl text-sky-400">
                        <Layers className="w-5 h-5" />
                      </div>
                      <div>
                        <h3 className="text-lg font-bold text-white">{dep.pipeline_name}</h3>
                        <p className="text-xs text-slate-400 font-mono">Dependency ID #{dep.dependency_id}</p>
                      </div>
                    </div>

                    <div className="flex items-center gap-2.5">
                      <span className="text-xs text-slate-400 font-medium">Evaluation Rule:</span>
                      <ConditionBadge condition={dep.condition_type} />
                      {dep.required_count && (
                        <span className="text-xs px-2 py-0.5 rounded bg-slate-900 border border-slate-700 text-slate-300 font-mono">
                          Threshold: {dep.required_count}
                        </span>
                      )}
                    </div>
                  </div>

                  {/* Configured Datasets Visual Flow */}
                  <div className="mt-5">
                    <span className="text-xs font-semibold text-slate-400 uppercase tracking-wider block mb-2.5">
                      Configured Upstream Datasets:
                    </span>
                    <div className="flex flex-wrap items-center gap-2">
                      {dep.configured_datasets?.map((dsName, idx) => {
                        const style = getDatasetStyle(dsName);
                        const DIcon = style.icon;
                        return (
                          <div 
                            key={idx}
                            className="flex items-center gap-2 px-3 py-1.5 rounded-xl bg-slate-900 border border-slate-700/80 text-xs text-slate-200"
                          >
                            <DIcon className="w-3.5 h-3.5 text-sky-400" />
                            <span className="font-semibold">{dsName}</span>
                          </div>
                        );
                      })}
                    </div>
                  </div>

                  {/* Latest Decision Box */}
                  <div className="mt-6">
                    <span className="text-xs font-semibold text-slate-400 uppercase tracking-wider block mb-2.5">
                      Latest Evaluated Decision Audit:
                    </span>

                    {dec ? (
                      <div className={`p-4 rounded-xl border transition-all ${
                        isTrigger 
                          ? 'bg-emerald-500/10 border-emerald-500/30' 
                          : 'bg-amber-500/10 border-amber-500/30'
                      }`}>
                        <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
                          <div className="flex items-center gap-3.5">
                            {isTrigger ? (
                              <div className="p-2 rounded-xl bg-emerald-500/20 text-emerald-400">
                                <CheckCircle2 className="w-6 h-6" />
                              </div>
                            ) : (
                              <div className="p-2 rounded-xl bg-amber-500/20 text-amber-400">
                                <ShieldAlert className="w-6 h-6" />
                              </div>
                            )}

                            <div>
                              <div className="flex items-center gap-2.5">
                                <span className={`text-base font-black tracking-wide ${
                                  isTrigger ? 'text-emerald-400' : 'text-amber-400'
                                }`}>
                                  DECISION: {dec.decision}
                                </span>
                                <span className="text-xs font-mono px-2 py-0.5 rounded bg-slate-900/80 text-slate-300">
                                  {dec.ready_count} / {dec.total_required} READY
                                </span>
                              </div>
                              <p className="text-xs font-medium text-slate-300 mt-1">
                                {dec.reason}
                              </p>
                            </div>
                          </div>

                          <div className="text-right text-[11px] font-mono text-slate-400">
                            <div>Evaluated At: {dec.evaluated_at || dec.triggered_at || '—'}</div>
                            <div className="text-slate-500">Decision ID: #{dec.decision_id}</div>
                          </div>
                        </div>
                      </div>
                    ) : (
                      <div className="p-4 rounded-xl bg-slate-900/60 border border-slate-800 text-xs text-slate-400 italic">
                        No decision evaluation has occurred for this pipeline yet.
                      </div>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>

      {/* ========================================================================= */}
      {/* 3. SPECIALIZED BUSINESS RULES ARCHITECTURE (Rules A, B, C) */}
      {/* ========================================================================= */}
      <div>
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-2 mb-3">
          <h3 className="text-base font-bold text-white tracking-wide flex items-center gap-2">
            <Sliders className="w-5 h-5 text-indigo-400" />
            <span>Configured Specialized Business Rules (AST &amp; Quorum Engines)</span>
          </h3>
          <span className="text-[10px] px-2 py-0.5 rounded font-mono font-bold uppercase tracking-wider bg-slate-800 text-slate-400 border border-slate-700">
            Engine Architecture Reference
          </span>
        </div>

        <div className="mb-3.5 p-3 rounded-xl bg-slate-900/60 border border-slate-800 text-xs text-slate-400 flex items-start gap-2.5">
          <Info className="w-4 h-4 text-sky-400 flex-shrink-0 mt-0.5" />
          <div>
            <span className="font-semibold text-slate-300">Rule Definition Registry:</span> AST expressions are validated dynamically against incoming CDC and e-commerce streams. Executed actions are recorded in <code className="text-sky-300 font-mono">business_rule_actions</code>.
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {CONFIGURED_BUSINESS_RULES.map((rule) => {
            const Icon = rule.icon;
            return (
              <div 
                key={rule.id}
                className="bg-slate-800/80 border border-slate-700/70 rounded-2xl p-5 flex flex-col justify-between space-y-4 hover:border-slate-600 transition-all"
              >
                <div className="space-y-3">
                  <div className="flex items-center justify-between">
                    <div className="p-2 rounded-xl bg-slate-900 border border-slate-700 text-indigo-400">
                      <Icon className="w-5 h-5" />
                    </div>
                    <div className="flex items-center gap-1.5">
                      <span className="text-[10px] font-bold uppercase tracking-wider px-2 py-0.5 rounded bg-slate-800 text-slate-400 border border-slate-700 font-mono">
                        STATIC SPEC
                      </span>
                      <span className="text-[10px] font-bold uppercase tracking-wider px-2 py-0.5 rounded bg-indigo-500/10 text-indigo-400 border border-indigo-500/20 font-mono">
                        {rule.condition_type}
                      </span>
                    </div>
                  </div>

                  <div>
                    <h4 className="text-sm font-bold text-white leading-snug">{rule.name}</h4>
                    <p className="text-xs text-slate-400 mt-1">{rule.evaluation_scope}</p>
                  </div>

                  <div className="p-2.5 bg-slate-900/80 rounded-xl border border-slate-700/60 text-xs font-mono text-slate-300">
                    <div className="text-[10px] text-slate-500 uppercase font-sans font-semibold mb-1">AST Condition:</div>
                    <code className="text-emerald-400">{rule.ast_expression}</code>
                  </div>
                </div>

                <div className="pt-3 border-t border-slate-700/60 text-[11px] text-slate-400 space-y-1.5">
                  <div className="flex items-center gap-1.5">
                    <Zap className="w-3.5 h-3.5 text-amber-400 flex-shrink-0" />
                    <span className="truncate"><strong className="text-slate-300">Action:</strong> {rule.action}</span>
                  </div>
                  <div className="flex items-center gap-1.5 text-slate-500">
                    <span>Events:</span>
                    <span className="font-mono text-slate-400">{rule.event_topics.join(', ')}</span>
                  </div>
                  <button
                    onClick={() => {
                      setPipelineFilter(rule.target_pipeline);
                      if (scopeFilter === 'SIMULATED') setScopeFilter('OPERATIONAL');
                      decisionsRef.current?.scrollIntoView({ behavior: 'smooth' });
                    }}
                    className="w-full mt-2 inline-flex items-center justify-between px-3 py-1.5 rounded-xl bg-slate-900 border border-slate-700/80 text-xs font-semibold text-sky-400 hover:text-sky-300 hover:border-slate-600 transition-colors cursor-pointer"
                  >
                    <span>View Evaluated Decisions</span>
                    <ArrowRight className="w-3.5 h-3.5" />
                  </button>
                </div>
              </div>
            );
          })}
        </div>
      </div>

      {/* ========================================================================= */}
      {/* 4. DECISION AUDIT TRAIL LOG */}
      {/* ========================================================================= */}
      <div ref={decisionsRef} className="bg-slate-800/80 border border-slate-700/70 rounded-2xl overflow-hidden shadow-sm">
        <div className="px-5 py-4 border-b border-slate-700/60 bg-slate-800/50 flex flex-col lg:flex-row lg:items-center justify-between gap-3">
          <div>
            <h3 className="text-sm font-bold text-white uppercase tracking-wider flex items-center gap-2">
              <ShieldAlert className="w-4 h-4 text-emerald-400" />
              <span>Dependency Engine Decision Audit Trail</span>
            </h3>
            <p className="text-xs text-slate-400 mt-0.5">
              Live chronological log of every BLOCK and TRIGGER decision evaluated by the engine with exact reasons.
            </p>
          </div>

          <div className="flex flex-wrap items-center gap-2.5">
            {/* Scope Filter Dropdown */}
            <div className="flex items-center gap-2 bg-slate-900/80 border border-slate-700 rounded-xl px-2.5 py-1 text-xs">
              <Layers className="w-3.5 h-3.5 text-sky-400" />
              <span className="text-slate-400 font-medium">Scope:</span>
              <select
                value={scopeFilter}
                onChange={(e) => setScopeFilter(e.target.value)}
                className="bg-transparent text-white font-semibold focus:outline-none cursor-pointer"
              >
                <option value="OPERATIONAL" className="bg-slate-900 text-white">Operational</option>
                <option value="SIMULATED" className="bg-slate-900 text-white">Simulated</option>
                <option value="ALL" className="bg-slate-900 text-white">All (Unfiltered)</option>
              </select>
            </div>

            {/* Pipeline Filter Dropdown */}
            <div className="flex items-center gap-2 bg-slate-900/80 border border-slate-700 rounded-xl px-2.5 py-1 text-xs">
              <Filter className="w-3.5 h-3.5 text-indigo-400" />
              <span className="text-slate-400 font-medium">Pipeline:</span>
              <select
                value={pipelineFilter}
                onChange={(e) => setPipelineFilter(e.target.value)}
                className="bg-transparent text-white font-semibold focus:outline-none cursor-pointer max-w-[170px] truncate"
              >
                <option value="ALL" className="bg-slate-900 text-white">All Pipelines &amp; Rules</option>
                <option value="Inventory Reorder Pipeline" className="bg-slate-900 text-white">Inventory Reorder Pipeline (Rule B)</option>
                <option value="Rule A: High-Velocity Cart Abandonment Engine" className="bg-slate-900 text-white">Rule A: Cart Abandonment</option>
                <option value="Rule C: Payment Failure Spike Detection" className="bg-slate-900 text-white">Rule C: Payment Spike</option>
                <option value="Olist Core Forecast Pipeline" className="bg-slate-900 text-white">Olist Core Forecast Pipeline (ALL)</option>
                <option value="Olist Regional Dispatch Pipeline" className="bg-slate-900 text-white">Olist Regional Dispatch Pipeline (QUORUM)</option>
                <option value="Olist Payment Settlement Pipeline" className="bg-slate-900 text-white">Olist Payment Settlement Pipeline (ANY)</option>
                <option value="Operational Demand Feature Pipeline" className="bg-slate-900 text-white">Operational Feature Pipeline</option>
                <option value="Operational Demand Forecast Pipeline" className="bg-slate-900 text-white">Operational Forecast Pipeline</option>
              </select>
            </div>

            {/* Active Pipeline Filter Badge if filtered */}
            {pipelineFilter !== 'ALL' && (
              <div className="flex items-center gap-1.5 px-2.5 py-1 rounded-xl bg-sky-500/10 border border-sky-500/30 text-sky-300 text-xs font-medium">
                <span className="truncate max-w-[200px]">Filtered: <strong className="text-white">{pipelineFilter}</strong></span>
                <button 
                  onClick={() => setPipelineFilter('ALL')}
                  className="hover:text-white ml-1 p-0.5 rounded cursor-pointer text-slate-400 hover:text-slate-200"
                  title="Clear pipeline filter"
                >
                  ✕
                </button>
              </div>
            )}

            {/* Decision Status Filter Tabs */}
            <div className="flex items-center gap-1.5 bg-slate-900/80 p-1 rounded-xl border border-slate-700">
              <button
                onClick={() => setDecisionFilter('ALL')}
                className={`text-xs px-2.5 py-1 rounded-lg font-medium transition-all cursor-pointer ${
                  decisionFilter === 'ALL'
                    ? 'bg-slate-800 text-white shadow-sm'
                    : 'text-slate-400 hover:text-white'
                }`}
              >
                All ({decisions.length})
              </button>
              <button
                onClick={() => setDecisionFilter('TRIGGER')}
                className={`text-xs px-2.5 py-1 rounded-lg font-medium transition-all cursor-pointer ${
                  decisionFilter === 'TRIGGER'
                    ? 'bg-emerald-500/20 text-emerald-300 border border-emerald-500/30'
                    : 'text-slate-400 hover:text-white'
                }`}
              >
                TRIGGER ({decisions.filter(d => d.decision === 'TRIGGER').length})
              </button>
              <button
                onClick={() => setDecisionFilter('BLOCK')}
                className={`text-xs px-2.5 py-1 rounded-lg font-medium transition-all cursor-pointer ${
                  decisionFilter === 'BLOCK'
                    ? 'bg-amber-500/20 text-amber-300 border border-amber-500/30'
                    : 'text-slate-400 hover:text-white'
                }`}
              >
                BLOCK ({decisions.filter(d => d.decision === 'BLOCK').length})
              </button>
            </div>
          </div>
        </div>

        {decisionsLoading && filteredDecisions.length === 0 ? (
          <LoadingSpinner message="Loading decision audit trail..." />
        ) : filteredDecisions.length === 0 ? (
          <div className="p-8">
            <EmptyState 
              title="No decision records found" 
              description={decisionFilter === 'ALL' ? "No pipeline decisions logged matching current filters." : `No ${decisionFilter} decisions found in audit history.`} 
            />
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-left text-xs">
              <thead className="bg-slate-900/60 text-slate-400 uppercase font-semibold border-b border-slate-700/60 tracking-wider">
                <tr>
                  <th className="py-3 px-4">Decision ID</th>
                  <th className="py-3 px-4">Classification</th>
                  <th className="py-3 px-4">Target Pipeline / Rule</th>
                  <th className="py-3 px-4">Condition Evaluated</th>
                  <th className="py-3 px-4">Decision</th>
                  <th className="py-3 px-4">Readiness</th>
                  <th className="py-3 px-4">Audit Reason</th>
                  <th className="py-3 px-4 text-right">Evaluated At</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-700/40 font-mono">
                {filteredDecisions.map((dec) => {
                  const isTrigger = dec.decision === 'TRIGGER';
                  return (
                    <tr key={dec.decision_id} className="hover:bg-slate-700/40 transition-colors">
                      <td className="py-3 px-4 font-bold text-sky-400">
                        #{dec.decision_id}
                      </td>
                      <td className="py-3 px-4">
                        <span className={`text-[10px] px-2 py-0.5 rounded font-bold uppercase tracking-wide border ${
                          dec.classification === 'SIMULATED'
                            ? 'bg-amber-500/10 text-amber-400 border-amber-500/20'
                            : dec.classification === 'UNCERTAIN'
                            ? 'bg-purple-500/10 text-purple-400 border-purple-500/20'
                            : 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20'
                        }`}>
                          {dec.classification || 'OPERATIONAL'}
                        </span>
                      </td>
                      <td className="py-3 px-4 font-sans font-medium text-slate-200">
                        {dec.pipeline_name}
                      </td>
                      <td className="py-3 px-4">
                        <ConditionBadge condition={dec.condition_type} />
                      </td>
                      <td className="py-3 px-4">
                        <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded text-[11px] font-bold ${
                          isTrigger 
                            ? 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/20' 
                            : 'bg-amber-500/10 text-amber-400 border border-amber-500/20'
                        }`}>
                          {isTrigger ? <Check className="w-3 h-3" /> : <ShieldAlert className="w-3 h-3" />}
                          {dec.decision}
                        </span>
                      </td>
                      <td className="py-3 px-4 text-slate-300">
                        {dec.ready_count !== null && dec.total_required !== null 
                          ? `${dec.ready_count} / ${dec.total_required} READY`
                          : '—'}
                      </td>
                      <td className="py-3 px-4 font-sans text-slate-300 max-w-xs truncate" title={dec.reason}>
                        {dec.reason || '—'}
                      </td>
                      <td className="py-3 px-4 text-right text-slate-400 text-[11px]">
                        {dec.evaluated_at || dec.triggered_at || '—'}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}

        <div className="px-5 py-3 border-t border-slate-700/60 bg-slate-900/40 text-xs text-slate-400 flex items-center justify-between font-sans">
          <span>Displaying <strong>{filteredDecisions.length}</strong> of <strong>{decisions.length}</strong> evaluated decisions (Scope: <strong>{scopeFilter}</strong>{pipelineFilter !== 'ALL' ? `, Pipeline: ${pipelineFilter}` : ''})</span>
          <span className="text-slate-500 hidden sm:inline">Persisted directly in MySQL pipeline_decisions</span>
        </div>
      </div>

      {/* ========================================================================= */}
      {/* 5. INTERACTIVE EVENT INSPECTOR MODAL */}
      {/* ========================================================================= */}
      {selectedEventModal && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-slate-950/80 backdrop-blur-sm animate-fade-in">
          <div className="bg-slate-900 border border-slate-700 rounded-2xl w-full max-w-2xl max-h-[90vh] flex flex-col shadow-2xl overflow-hidden">
            {/* Modal Header */}
            <div className="px-6 py-4 border-b border-slate-800 flex items-center justify-between bg-slate-900/80">
              <div className="flex items-center gap-3">
                <div className="p-2 rounded-xl bg-sky-500/10 text-sky-400 border border-sky-500/20">
                  <Code2 className="w-5 h-5" />
                </div>
                <div>
                  <h3 className="text-base font-bold text-white flex items-center gap-2">
                    <span>Event Payload &amp; Kafka Metadata</span>
                    <span className="text-xs font-mono font-bold px-2 py-0.5 rounded bg-sky-500/20 text-sky-300">
                      #{selectedEventModal.event_id}
                    </span>
                  </h3>
                  <p className="text-xs text-slate-400">
                    Ingested from real Olist e-commerce stream dataset <strong className="text-slate-300">{selectedEventModal.dataset_name}</strong>
                  </p>
                </div>
              </div>

              <button
                onClick={() => setSelectedEventModal(null)}
                className="p-1.5 rounded-lg text-slate-400 hover:text-white hover:bg-slate-800 transition-colors cursor-pointer"
                title="Close Modal"
              >
                <X className="w-5 h-5" />
              </button>
            </div>

            {/* Modal Body */}
            <div className="p-6 space-y-4 overflow-y-auto font-sans">
              {/* Event Attributes Grid */}
              <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
                <div className="p-3 rounded-xl bg-slate-800/80 border border-slate-700/60">
                  <div className="text-[10px] text-slate-400 uppercase font-semibold">Dataset Version</div>
                  <div className="text-sm font-bold text-white font-mono mt-0.5">v{selectedEventModal.dataset_version}</div>
                </div>

                <div className="p-3 rounded-xl bg-slate-800/80 border border-slate-700/60">
                  <div className="text-[10px] text-slate-400 uppercase font-semibold">Batch Volume</div>
                  <div className="text-sm font-bold text-white font-mono mt-0.5">{selectedEventModal.rows_changed} rows</div>
                </div>

                <div className="p-3 rounded-xl bg-slate-800/80 border border-slate-700/60">
                  <div className="text-[10px] text-slate-400 uppercase font-semibold">Lifecycle Status</div>
                  <div className="text-sm font-bold text-emerald-400 font-mono mt-0.5">{selectedEventModal.event_status}</div>
                </div>

                <div className="p-3 rounded-xl bg-slate-800/80 border border-slate-700/60">
                  <div className="text-[10px] text-slate-400 uppercase font-semibold">Target Rule</div>
                  <div className="text-sm font-bold text-sky-400 font-mono mt-0.5">{selectedEventModal.condition_type}</div>
                </div>
              </div>

              {/* Realistic Payload Breakdown */}
              <div className="p-4 rounded-xl bg-slate-950/70 border border-slate-800 space-y-2">
                <div className="text-xs font-bold text-white uppercase tracking-wider flex items-center gap-2">
                  <Terminal className="w-4 h-4 text-emerald-400" />
                  <span>Realistic E-Commerce Context:</span>
                </div>
                <p className="text-xs text-slate-300 leading-relaxed">
                  {selectedEventModal.realistic_context?.summary}
                </p>
                {selectedEventModal.realistic_context?.sample_data && (
                  <div className="mt-2 text-xs font-mono text-slate-400 grid grid-cols-1 sm:grid-cols-2 gap-2">
                    {Object.entries(selectedEventModal.realistic_context.sample_data).map(([k, v]) => (
                      <div key={k} className="p-2 rounded-lg bg-slate-900 border border-slate-800">
                        <span className="text-slate-500 font-semibold">{k}: </span>
                        <span className="text-slate-200">{Array.isArray(v) ? v.join(', ') : String(v)}</span>
                      </div>
                    ))}
                  </div>
                )}
              </div>

              {/* Engine Evaluation Trace */}
              {selectedEventModal.linked_decision ? (
                <div className={`p-4 rounded-xl border ${
                  selectedEventModal.linked_decision.decision === 'TRIGGER'
                    ? 'bg-emerald-500/10 border-emerald-500/30 text-emerald-300'
                    : 'bg-amber-500/10 border-amber-500/30 text-amber-300'
                }`}>
                  <div className="text-xs font-bold uppercase tracking-wider flex items-center gap-1.5">
                    <CheckCircle2 className="w-4 h-4" />
                    <span>Dependency Engine Evaluation Trace:</span>
                  </div>
                  <div className="text-xs font-semibold text-white mt-1">
                    Decision: {selectedEventModal.linked_decision.decision} ({selectedEventModal.linked_decision.ready_count} / {selectedEventModal.linked_decision.total_required} Datasets READY)
                  </div>
                  <p className="text-xs text-slate-300 mt-1">
                    Reason: {selectedEventModal.linked_decision.reason}
                  </p>
                </div>
              ) : (
                <div className="p-3 rounded-xl bg-slate-900/60 border border-slate-800 text-xs text-slate-400 italic">
                  Engine evaluation pending or scheduled for next poller cycle.
                </div>
              )}

              {/* Raw JSON Code Block */}
              <div className="relative">
                <div className="flex items-center justify-between pb-1.5 text-xs text-slate-400">
                  <span>Raw Kafka Payload JSON:</span>
                  <button
                    onClick={() => handleCopyJson(selectedEventModal)}
                    className="flex items-center gap-1 text-sky-400 hover:text-white transition-colors cursor-pointer"
                  >
                    {copiedPayload ? <Check className="w-3.5 h-3.5 text-emerald-400" /> : <Copy className="w-3.5 h-3.5" />}
                    <span>{copiedPayload ? 'Copied!' : 'Copy JSON'}</span>
                  </button>
                </div>
                <pre className="p-4 rounded-xl bg-slate-950 border border-slate-800 text-xs font-mono text-emerald-400 overflow-x-auto max-h-52">
                  {JSON.stringify(selectedEventModal, null, 2)}
                </pre>
              </div>
            </div>

            {/* Modal Footer */}
            <div className="px-6 py-3 border-t border-slate-800 bg-slate-900/80 flex items-center justify-between text-xs text-slate-400">
              <span>Source Change ID: <code className="text-sky-300 font-mono">{selectedEventModal.source_change_id}</code></span>
              <button
                onClick={() => setSelectedEventModal(null)}
                className="px-4 py-1.5 rounded-xl bg-slate-800 hover:bg-slate-700 text-white font-semibold transition-colors cursor-pointer"
              >
                Close
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
