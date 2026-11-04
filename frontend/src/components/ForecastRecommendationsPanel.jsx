import React, { useState, useEffect, useCallback, useMemo } from 'react';
import {
  TrendingUp,
  Package,
  Clock,
  AlertTriangle,
  CheckCircle2,
  HelpCircle,
  Filter,
  Layers,
  ShieldAlert,
  ShieldCheck,
  Info,
  RefreshCw,
  Search,
  ChevronDown,
  ChevronUp,
  Cpu,
  Calendar,
  AlertCircle,
  Send,
  UserCheck,
  ClipboardList
} from 'lucide-react';
import { getForecastRecommendations, submitApproval } from '../api/client';
import LoadingSpinner from './LoadingSpinner';
import EmptyState from './EmptyState';
import ReplenishmentApprovalsQueue from './ReplenishmentApprovalsQueue';

// ─── Status Badge Component ──────────────────────────────────────────────────

function RecommendationStatusBadge({ status }) {
  const configs = {
    REPLENISHMENT_RECOMMENDED: {
      label: 'REORDER RECOMMENDED',
      cls: 'bg-emerald-500/15 text-emerald-400 border-emerald-500/30',
      icon: <CheckCircle2 className="w-3.5 h-3.5" />
    },
    STOCK_SUFFICIENT: {
      label: 'STOCK SUFFICIENT',
      cls: 'bg-sky-500/15 text-sky-400 border-sky-500/30',
      icon: <ShieldCheck className="w-3.5 h-3.5" />
    },
    NEEDS_REVIEW: {
      label: 'NEEDS REVIEW',
      cls: 'bg-amber-500/15 text-amber-400 border-amber-500/30',
      icon: <AlertTriangle className="w-3.5 h-3.5" />
    },
    INSUFFICIENT_DATA: {
      label: 'INSUFFICIENT DATA',
      cls: 'bg-slate-700/40 text-slate-400 border-slate-600',
      icon: <HelpCircle className="w-3.5 h-3.5" />
    }
  };

  const current = configs[status] || configs.INSUFFICIENT_DATA;
  return (
    <span className={`inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-[11px] font-bold uppercase tracking-wider border ${current.cls}`}>
      {current.icon}
      {current.label}
    </span>
  );
}

// ─── Provenance Tag ──────────────────────────────────────────────────────────

function ProvenanceTag({ provenance }) {
  const labels = {
    SOLE_OPERATIONAL_SKU_FOR_CATEGORY: 'Direct 1:1 SKU',
    AMBIGUOUS_MULTI_SKU: 'Multi-SKU Category Ambiguity',
    CATEGORY_AGGREGATE: 'Category Aggregate Ledger',
    UNMAPPED: 'Unmapped SKU'
  };

  const isWarn = provenance === 'AMBIGUOUS_MULTI_SKU' || provenance === 'UNMAPPED';
  return (
    <span className={`text-[10px] font-mono px-2 py-0.5 rounded border ${
      isWarn
        ? 'bg-amber-500/10 text-amber-300 border-amber-500/20'
        : 'bg-slate-800 text-slate-300 border-slate-700'
    }`}>
      {labels[provenance] || provenance}
    </span>
  );
}

// ─── Main Component ──────────────────────────────────────────────────────────

export default function ForecastRecommendationsPanel({ executionId, refreshTrigger }) {
  // Navigation: 'RECOMMENDATIONS' vs 'APPROVAL_QUEUE'
  const [activeTab, setActiveTab] = useState('RECOMMENDATIONS');

  // Simulated Actor Identity State (Role & ID)
  const [actorRole, setActorRole] = useState('OPERATOR');
  const [actorId, setActorId] = useState('op-alex-rivera');

  // Change default actor ID when switching roles
  const handleRoleChange = (newRole) => {
    setActorRole(newRole);
    if (newRole === 'OPERATOR') setActorId('op-alex-rivera');
    else if (newRole === 'MANAGER') setActorId('mgr-sarah-chen');
    else if (newRole === 'ADMIN') setActorId('admin-j-smith');
  };

  // Recommendations Data State
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState(null);

  // Filters
  const [scopeFilter, setScopeFilter] = useState('ALL');
  const [statusFilter, setStatusFilter] = useState('ALL');
  const [horizonWeeks, setHorizonWeeks] = useState(4);
  const [searchQuery, setSearchQuery] = useState('');
  const [expandedId, setExpandedId] = useState(null);

  // Submission Modal State
  const [submissionTarget, setSubmissionTarget] = useState(null);
  const [submissionQuantity, setSubmissionQuantity] = useState(10);
  const [submissionNotes, setSubmissionNotes] = useState('');
  const [submittingModal, setSubmittingModal] = useState(false);
  const [submitSuccessMsg, setSubmitSuccessMsg] = useState(null);
  const [submitErrorMsg, setSubmitErrorMsg] = useState(null);
  const [queueRefreshCounter, setQueueRefreshCounter] = useState(0);

  const loadRecommendations = useCallback(async (isManual = false) => {
    try {
      if (isManual) setRefreshing(true);
      else setLoading(true);
      setError(null);

      const params = {
        execution_id: executionId || undefined,
        planning_horizon_weeks: horizonWeeks,
        limit: 100
      };

      if (scopeFilter !== 'ALL') params.scope_type = scopeFilter;
      if (statusFilter !== 'ALL') params.status = statusFilter;

      const res = await getForecastRecommendations(params);
      setData(res);
    } catch (err) {
      setError(err?.message || 'Failed to load forecast-driven replenishment recommendations.');
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, [executionId, horizonWeeks, scopeFilter, statusFilter]);

  useEffect(() => {
    loadRecommendations();
  }, [loadRecommendations, refreshTrigger]);

  const summary = data?.summary || {};
  const rawList = data?.recommendations || [];

  // Local client-side search query
  const filteredList = useMemo(() => {
    if (!searchQuery) return rawList;
    const q = searchQuery.toLowerCase();
    return rawList.filter(item => {
      const matchSku = item.sku?.toLowerCase().includes(q);
      const matchCat = item.category?.toLowerCase().includes(q);
      const matchEng = item.category_english?.toLowerCase().includes(q);
      return matchSku || matchCat || matchEng;
    });
  }, [rawList, searchQuery]);

  // Submission handler
  const handleOpenSubmitModal = (rec) => {
    setSubmissionTarget(rec);
    setSubmissionQuantity(rec.suggested_quantity > 0 ? rec.suggested_quantity : 10);
    setSubmissionNotes(`Generated from forecast model ${rec.model_name || 'execution'} horizon ${horizonWeeks}w`);
    setSubmitErrorMsg(null);
  };

  const handleConfirmSubmit = async () => {
    if (!submissionTarget || submittingModal) return;
    setSubmittingModal(true);
    setSubmitErrorMsg(null);

    try {
      const payload = {
        recommendation_id: submissionTarget.recommendation_id,
        sku: submissionTarget.sku,
        warehouse: submissionTarget.warehouse || 'SP-01',
        requested_quantity: Number(submissionQuantity),
        execution_id: submissionTarget.execution_id,
        planning_horizon_weeks: horizonWeeks,
        mapping_provenance: submissionTarget.mapping_provenance,
        idempotency_key: `APPR-${submissionTarget.execution_id}-${submissionTarget.sku}-${Date.now().toString(36)}`,
        operator_notes: submissionNotes.trim() || undefined
      };

      const headers = {
        'X-Actor-ID': actorId,
        'X-Actor-Role': actorRole
      };

      const res = await submitApproval(payload, headers);
      setSubmitSuccessMsg(`Replenishment request #${res.approval_id} submitted for ${submissionTarget.sku} (${submissionQuantity} units).`);
      setSubmissionTarget(null);
      setQueueRefreshCounter(c => c + 1);
    } catch (err) {
      const detail = err?.response?.data?.detail;
      const msg = typeof detail === 'string' ? detail : JSON.stringify(detail) || err.message;
      setSubmitErrorMsg(`Submission failed: ${msg}`);
    } finally {
      setSubmittingModal(false);
    }
  };

  return (
    <div className="bg-slate-800/80 border border-slate-700/70 rounded-2xl p-5 shadow-sm space-y-4">
      {/* ── Top Header & Tab Navigation ── */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 pb-3 border-b border-slate-700/60">
        <div>
          <div className="flex items-center gap-2">
            <TrendingUp className="w-4 h-4 text-emerald-400" />
            <h3 className="text-sm font-bold text-white uppercase tracking-wider">
              Replenishment Planning & Approval Workflow
            </h3>
            <span className="text-[10px] px-2 py-0.5 rounded font-mono font-bold bg-sky-500/10 text-sky-400 border border-sky-500/20">
              PHASE 3E HITL
            </span>
          </div>
          <p className="text-xs text-slate-400 mt-0.5">
            Forecast-conditioned replenishment recommendations governed by role-based operator review, validation, and idempotent dispatch.
          </p>
        </div>

        {/* Actor Role Simulation Controls */}
        <div className="flex items-center gap-2 bg-slate-900 border border-slate-700/80 px-3 py-1.5 rounded-xl text-xs">
          <UserCheck className="w-3.5 h-3.5 text-sky-400" />
          <span className="text-slate-400">Actor:</span>
          <select
            value={actorRole}
            onChange={e => handleRoleChange(e.target.value)}
            className="bg-transparent text-white font-semibold focus:outline-none cursor-pointer"
          >
            <option value="OPERATOR" className="bg-slate-900">OPERATOR (Submit Only)</option>
            <option value="MANAGER" className="bg-slate-900">MANAGER (Approve/Reject)</option>
            <option value="ADMIN" className="bg-slate-900">ADMIN (Approve/Reject)</option>
          </select>
          <span className="text-slate-500 font-mono text-[10px]">({actorId})</span>
        </div>
      </div>

      {/* ── Subtabs ── */}
      <div className="flex items-center justify-between gap-2 border-b border-slate-700/40 pb-2">
        <div className="flex items-center gap-1.5">
          <button
            onClick={() => setActiveTab('RECOMMENDATIONS')}
            className={`px-3 py-1.5 rounded-lg text-xs font-bold transition-all flex items-center gap-1.5 ${
              activeTab === 'RECOMMENDATIONS'
                ? 'bg-sky-500/20 text-sky-300 border border-sky-500/30'
                : 'text-slate-400 hover:text-white bg-slate-900/50'
            }`}
          >
            <TrendingUp className="w-3.5 h-3.5" />
            <span>Forecast Recommendations</span>
          </button>

          <button
            onClick={() => setActiveTab('APPROVAL_QUEUE')}
            className={`px-3 py-1.5 rounded-lg text-xs font-bold transition-all flex items-center gap-1.5 ${
              activeTab === 'APPROVAL_QUEUE'
                ? 'bg-sky-500/20 text-sky-300 border border-sky-500/30'
                : 'text-slate-400 hover:text-white bg-slate-900/50'
            }`}
          >
            <ClipboardList className="w-3.5 h-3.5" />
            <span>Approval Queue</span>
          </button>
        </div>

        {activeTab === 'RECOMMENDATIONS' && (
          <div className="flex items-center gap-2">
            {/* Horizon Selector */}
            <div className="flex items-center gap-1.5 text-xs text-slate-300 bg-slate-900 border border-slate-700 px-2.5 py-1 rounded-lg">
              <Calendar className="w-3.5 h-3.5 text-slate-400" />
              <span>Horizon:</span>
              <select
                value={horizonWeeks}
                onChange={e => setHorizonWeeks(Number(e.target.value))}
                className="bg-transparent text-white font-semibold focus:outline-none cursor-pointer"
              >
                <option value={2} className="bg-slate-900">2 Weeks</option>
                <option value={4} className="bg-slate-900">4 Weeks</option>
                <option value={8} className="bg-slate-900">8 Weeks</option>
                <option value={12} className="bg-slate-900">12 Weeks</option>
              </select>
            </div>

            <button
              onClick={() => loadRecommendations(true)}
              disabled={refreshing}
              className="p-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300 border border-slate-700 text-xs flex items-center gap-1 cursor-pointer transition-colors"
              title="Refresh recommendations"
            >
              <RefreshCw className={`w-3.5 h-3.5 ${refreshing ? 'animate-spin' : ''}`} />
            </button>
          </div>
        )}
      </div>

      {/* ── Submission Success Toast ── */}
      {submitSuccessMsg && (
        <div className="p-3 rounded-xl bg-emerald-500/10 border border-emerald-500/20 text-emerald-300 text-xs flex items-center justify-between">
          <div className="flex items-center gap-2">
            <CheckCircle2 className="w-4 h-4 flex-shrink-0" />
            <span>{submitSuccessMsg}</span>
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={() => { setActiveTab('APPROVAL_QUEUE'); setSubmitSuccessMsg(null); }}
              className="text-xs text-sky-400 underline font-semibold hover:text-sky-300 cursor-pointer"
            >
              View in Queue →
            </button>
            <button onClick={() => setSubmitSuccessMsg(null)} className="text-emerald-400 hover:text-white cursor-pointer font-bold ml-2">✕</button>
          </div>
        </div>
      )}

      {/* ────────────────────────────────────────────────────────────────────────
          TAB 1: RECOMMENDATIONS
      ──────────────────────────────────────────────────────────────────────── */}
      {activeTab === 'RECOMMENDATIONS' && (
        <>
          {/* ── Summary KPI Row ── */}
          <div className="grid grid-cols-2 sm:grid-cols-5 gap-3">
            <div className="bg-slate-900/60 rounded-xl p-3 border border-slate-700/50 text-center">
              <div className="text-xl font-bold font-mono text-white">{summary.total_recommendations ?? 0}</div>
              <div className="text-[11px] text-slate-400 mt-0.5">Evaluated Scopes</div>
            </div>
            <div className="bg-slate-900/60 rounded-xl p-3 border border-emerald-500/20 text-center">
              <div className="text-xl font-bold font-mono text-emerald-400">{summary.replenishment_recommended_count ?? 0}</div>
              <div className="text-[11px] text-slate-400 mt-0.5">Reorder Recommended</div>
            </div>
            <div className="bg-slate-900/60 rounded-xl p-3 border border-sky-500/20 text-center">
              <div className="text-xl font-bold font-mono text-sky-400">{summary.stock_sufficient_count ?? 0}</div>
              <div className="text-[11px] text-slate-400 mt-0.5">Stock Sufficient</div>
            </div>
            <div className="bg-slate-900/60 rounded-xl p-3 border border-amber-500/20 text-center">
              <div className="text-xl font-bold font-mono text-amber-400">{summary.needs_review_count ?? 0}</div>
              <div className="text-[11px] text-slate-400 mt-0.5">Needs Review</div>
            </div>
            <div className="bg-slate-900/60 rounded-xl p-3 border border-slate-700/50 text-center">
              <div className="text-xl font-bold font-mono text-slate-400">{summary.insufficient_data_count ?? 0}</div>
              <div className="text-[11px] text-slate-400 mt-0.5">Insufficient Data</div>
            </div>
          </div>

          {/* ── Filter Bar ── */}
          <div className="flex flex-wrap items-center justify-between gap-2.5 pt-1 text-xs">
            <div className="flex flex-wrap items-center gap-2">
              {/* Scope Filter */}
              <div className="flex items-center rounded-lg bg-slate-900 border border-slate-700 p-0.5">
                {['ALL', 'SKU', 'CATEGORY'].map(s => (
                  <button
                    key={s}
                    onClick={() => setScopeFilter(s)}
                    className={`px-2.5 py-1 rounded-md font-semibold transition-all ${
                      scopeFilter === s
                        ? 'bg-sky-500/20 text-sky-300 border border-sky-500/30'
                        : 'text-slate-400 hover:text-white'
                    }`}
                  >
                    {s === 'ALL' ? 'All Scopes' : s}
                  </button>
                ))}
              </div>

              {/* Status Dropdown */}
              <select
                value={statusFilter}
                onChange={e => setStatusFilter(e.target.value)}
                className="py-1.5 px-2.5 rounded-lg bg-slate-900 border border-slate-700 text-slate-200 text-xs focus:outline-none focus:border-sky-500"
              >
                <option value="ALL">All Statuses</option>
                <option value="REPLENISHMENT_RECOMMENDED">Reorder Recommended</option>
                <option value="STOCK_SUFFICIENT">Stock Sufficient</option>
                <option value="NEEDS_REVIEW">Needs Review</option>
                <option value="INSUFFICIENT_DATA">Insufficient Data</option>
              </select>
            </div>

            {/* Search */}
            <div className="relative">
              <Search className="w-3.5 h-3.5 text-slate-400 absolute left-2.5 top-1/2 -translate-y-1/2" />
              <input
                type="text"
                placeholder="Search SKU or Category..."
                value={searchQuery}
                onChange={e => setSearchQuery(e.target.value)}
                className="pl-8 pr-3 py-1.5 rounded-lg bg-slate-900 border border-slate-700 text-slate-200 placeholder-slate-500 text-xs focus:outline-none focus:border-sky-500"
              />
            </div>
          </div>

          {/* ── Content View ── */}
          {loading && !filteredList.length ? (
            <div className="py-12 flex flex-col items-center gap-3">
              <LoadingSpinner size="md" />
              <p className="text-xs text-slate-400">Correlating forecast demand with operational inventory snapshots...</p>
            </div>
          ) : error ? (
            <div className="p-4 rounded-xl bg-rose-500/10 border border-rose-500/20 text-rose-300 text-xs flex items-center gap-3">
              <AlertCircle className="w-4 h-4 flex-shrink-0" />
              <span>{error}</span>
            </div>
          ) : filteredList.length === 0 ? (
            <EmptyState
              title="No recommendations match current filters"
              description="Try broadening the scope, status filter, or search term."
              icon={TrendingUp}
            />
          ) : (
            <div className="space-y-3 max-h-[580px] overflow-y-auto pr-1">
              {filteredList.map(rec => {
                const isExpanded = expandedId === rec.recommendation_id;
                const isReorder = rec.status === 'REPLENISHMENT_RECOMMENDED';
                const isReview = rec.status === 'NEEDS_REVIEW';

                // Safety Invariant: Check if recommendation is approvable/submittable
                const isDirectSku = rec.mapping_provenance === 'SOLE_OPERATIONAL_SKU_FOR_CATEGORY' || (rec.scope_type === 'SKU' && rec.sku);
                const isBlocked = rec.mapping_provenance === 'AMBIGUOUS_MULTI_SKU' || rec.mapping_provenance === 'UNMAPPED' || rec.status === 'INSUFFICIENT_DATA';

                const borderCls = isReorder
                  ? 'border-emerald-500/30 bg-emerald-950/15 hover:border-emerald-500/50'
                  : isReview
                  ? 'border-amber-500/30 bg-amber-950/10 hover:border-amber-500/40'
                  : 'border-slate-700/60 bg-slate-900/60 hover:border-slate-600';

                return (
                  <div
                    key={rec.recommendation_id}
                    className={`p-4 rounded-xl border text-xs transition-all space-y-3 ${borderCls}`}
                  >
                    {/* Top Row: Scope, Identifier, Status, Reorder Qty */}
                    <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-2">
                      <div className="flex items-center gap-2 flex-wrap">
                        <span className={`text-[10px] font-bold px-2 py-0.5 rounded font-mono ${
                          rec.scope_type === 'SKU'
                            ? 'bg-sky-500/15 text-sky-400 border border-sky-500/30'
                            : 'bg-indigo-500/15 text-indigo-400 border border-indigo-500/30'
                        }`}>
                          {rec.scope_type}
                        </span>
                        <span className="font-mono font-bold text-white text-sm">
                          {rec.scope_type === 'SKU' ? rec.sku : rec.category}
                        </span>
                        {rec.category_english && rec.category_english !== rec.category && (
                          <span className="text-slate-400 text-xs">
                            ({rec.category_english})
                          </span>
                        )}
                        <ProvenanceTag provenance={rec.mapping_provenance} />
                      </div>

                      <div className="flex items-center gap-2.5">
                        {isReorder && rec.suggested_quantity > 0 && (
                          <div className="px-2.5 py-1 rounded-lg bg-emerald-500/20 text-emerald-300 border border-emerald-500/30 font-mono font-bold text-xs">
                            Suggested: +{rec.suggested_quantity} units
                          </div>
                        )}
                        <RecommendationStatusBadge status={rec.status} />
                      </div>
                    </div>

                    {/* Metrics Grid */}
                    <div className="grid grid-cols-2 sm:grid-cols-4 gap-2.5 pt-1 text-[11px] border-t border-slate-700/40">
                      <div className="p-2 rounded-lg bg-slate-800/60 border border-slate-700/40">
                        <span className="text-slate-400 text-[10px] uppercase font-semibold">Available Stock</span>
                        <div className="font-mono font-bold text-white text-sm mt-0.5">
                          {rec.current_stock?.available ?? 0}
                          <span className="text-[10px] font-normal text-slate-400 ml-1">
                            (On-hand: {rec.current_stock?.on_hand ?? 0}, Res: {rec.current_stock?.reserved ?? 0})
                          </span>
                        </div>
                      </div>

                      <div className="p-2 rounded-lg bg-slate-800/60 border border-slate-700/40">
                        <span className="text-slate-400 text-[10px] uppercase font-semibold">Forecast Demand</span>
                        <div className="font-mono font-bold text-white text-sm mt-0.5">
                          {rec.forecast_demand?.weekly_avg !== null ? `${rec.forecast_demand.weekly_avg} /wk` : '—'}
                          <span className="text-[10px] font-normal text-slate-400 ml-1">
                            ({horizonWeeks}w: {rec.forecast_demand?.horizon_total ?? '—'})
                          </span>
                        </div>
                      </div>

                      <div className="p-2 rounded-lg bg-slate-800/60 border border-slate-700/40">
                        <span className="text-slate-400 text-[10px] uppercase font-semibold">Stock Coverage</span>
                        <div className="font-mono font-bold text-white text-sm mt-0.5">
                          {rec.stock_coverage?.coverage_days !== null ? (
                            <span className={rec.stock_coverage.coverage_days < 7 ? 'text-amber-400' : 'text-emerald-400'}>
                              ~{rec.stock_coverage.coverage_days} days
                            </span>
                          ) : '—'}
                          <span className="text-[10px] font-normal text-slate-400 ml-1">
                            ({rec.stock_coverage?.coverage_weeks ?? '—'} wks)
                          </span>
                        </div>
                      </div>

                      <div className="p-2 rounded-lg bg-slate-800/60 border border-slate-700/40">
                        <span className="text-slate-400 text-[10px] uppercase font-semibold">Target vs Threshold</span>
                        <div className="font-mono font-bold text-white text-sm mt-0.5">
                          {rec.target_stock !== null ? rec.target_stock : '—'}
                          <span className="text-[10px] font-normal text-slate-400 ml-1">
                            (Thresh: {rec.current_stock?.reorder_threshold ?? 20})
                          </span>
                        </div>
                      </div>
                    </div>

                    {/* Explanation text */}
                    <div className="text-xs text-slate-300 leading-relaxed bg-slate-900/40 p-2.5 rounded-lg border border-slate-700/30">
                      <p>{rec.explanation}</p>
                    </div>

                    {/* Action Bar: Submit for Approval vs Blocked Banner */}
                    <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-2 pt-2 border-t border-slate-700/40">
                      <button
                        onClick={() => setExpandedId(isExpanded ? null : rec.recommendation_id)}
                        className="text-[11px] text-sky-400 hover:text-sky-300 font-semibold flex items-center gap-1 cursor-pointer transition-colors"
                      >
                        <span>{isExpanded ? 'Hide Calculation Assumptions' : 'View Audit & Lead-Time Assumptions'}</span>
                        {isExpanded ? <ChevronUp className="w-3.5 h-3.5" /> : <ChevronDown className="w-3.5 h-3.5" />}
                      </button>

                      {isBlocked ? (
                        <div className="flex items-center gap-1.5 text-[11px] text-amber-300 bg-amber-500/10 px-2.5 py-1 rounded-lg border border-amber-500/20">
                          <AlertTriangle className="w-3.5 h-3.5 text-amber-400 flex-shrink-0" />
                          <span>Category requires manual SKU allocation before approval submission.</span>
                        </div>
                      ) : isDirectSku ? (
                        <button
                          onClick={() => handleOpenSubmitModal(rec)}
                          className="px-3 py-1.5 rounded-lg bg-sky-600 hover:bg-sky-500 text-white font-semibold text-xs flex items-center gap-1.5 cursor-pointer transition-colors shadow-sm"
                        >
                          <Send className="w-3.5 h-3.5" />
                          <span>Submit for Approval</span>
                        </button>
                      ) : null}
                    </div>

                    {/* Expanded Details */}
                    {isExpanded && (
                      <div className="p-3 rounded-lg bg-slate-900/80 border border-slate-700/50 space-y-2 text-[11px] text-slate-300">
                        <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 pb-2 border-b border-slate-700/40">
                          <div>
                            <span className="text-slate-400">Forecast Execution:</span>{' '}
                            <strong className="text-white font-mono">#{rec.execution_id}</strong> ({rec.model_name} {rec.model_version})
                          </div>
                          <div>
                            <span className="text-slate-400">Forecast Period:</span>{' '}
                            <span className="font-mono text-slate-200">
                              {rec.forecast_period?.start_week || '—'} → {rec.forecast_period?.end_week || '—'}
                            </span>
                          </div>
                          <div>
                            <span className="text-slate-400">Supplier Sourcing:</span>{' '}
                            <span className="font-mono text-slate-200">
                              {rec.lead_time?.source_supplier_id || 'N/A'} ({rec.lead_time?.supplier_status || 'Unknown'})
                            </span>
                          </div>
                          <div>
                            <span className="text-slate-400">Supplier Lead Time:</span>{' '}
                            <span className="font-mono text-slate-200">
                              {rec.lead_time?.days ?? '—'} days ({rec.lead_time?.hours ?? '—'}h)
                            </span>
                          </div>
                          <div>
                            <span className="text-slate-400">Demand During Lead Time:</span>{' '}
                            <strong className="text-white font-mono">{rec.lead_time?.demand_during_lead_time ?? '—'} units</strong>
                          </div>
                          <div>
                            <span className="text-slate-400">Configured Safety Stock:</span>{' '}
                            <strong className="text-white font-mono">{rec.planning_assumptions?.safety_stock_units ?? 10} units</strong>
                          </div>
                        </div>

                        <div className="text-[10px] text-slate-400 flex items-center justify-between">
                          <span>Recommendation ID: <code className="text-slate-300">{rec.recommendation_id}</code></span>
                          <span>Evaluated: {new Date(rec.generated_at).toLocaleString()}</span>
                        </div>
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          )}
        </>
      )}

      {/* ────────────────────────────────────────────────────────────────────────
          TAB 2: APPROVAL QUEUE
      ──────────────────────────────────────────────────────────────────────── */}
      {activeTab === 'APPROVAL_QUEUE' && (
        <ReplenishmentApprovalsQueue
          actorRole={actorRole}
          actorId={actorId}
          refreshTrigger={queueRefreshCounter}
          onWorkflowComplete={() => {
            loadRecommendations(true);
          }}
        />
      )}

      {/* ── Submission Modal ── */}
      {submissionTarget && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/70 backdrop-blur-sm">
          <div className="bg-slate-900 border border-slate-700 rounded-2xl max-w-md w-full p-5 space-y-4 shadow-xl">
            <div className="flex items-center justify-between pb-3 border-b border-slate-800">
              <div className="flex items-center gap-2 text-sky-400 font-bold text-sm">
                <Send className="w-4 h-4" />
                <span>Submit Replenishment Approval</span>
              </div>
              <button
                onClick={() => setSubmissionTarget(null)}
                className="text-slate-400 hover:text-white cursor-pointer"
              >
                ✕
              </button>
            </div>

            {submitErrorMsg && (
              <div className="p-2.5 rounded-lg bg-rose-500/10 border border-rose-500/20 text-rose-300 text-xs">
                {submitErrorMsg}
              </div>
            )}

            <div className="text-xs text-slate-300 space-y-2">
              <p>
                Submitting approval request for SKU <code className="text-white font-bold">{submissionTarget.sku}</code> (Warehouse {submissionTarget.warehouse || 'SP-01'}).
              </p>
              <div className="p-2.5 rounded-lg bg-slate-800/80 border border-slate-700 text-[11px] space-y-1">
                <div>Available Stock: <strong className="text-white">{submissionTarget.current_stock?.available ?? 0} units</strong></div>
                <div>Model Weekly Demand: <strong className="text-white">{submissionTarget.forecast_demand?.weekly_avg ?? 0} units/wk</strong></div>
                <div>Target Horizon Stock: <strong className="text-white">{submissionTarget.target_stock ?? 0} units</strong></div>
              </div>
            </div>

            <div className="space-y-3">
              <div className="space-y-1">
                <label className="text-[11px] font-semibold text-slate-300">
                  Requested Replenishment Quantity <span className="text-sky-400">*</span>
                </label>
                <input
                  type="number"
                  min="1"
                  max="10000"
                  value={submissionQuantity}
                  onChange={e => setSubmissionQuantity(Math.max(1, parseInt(e.target.value) || 1))}
                  className="w-full px-3 py-1.5 rounded-lg bg-slate-800 border border-slate-700 text-slate-200 text-xs focus:outline-none focus:border-sky-500"
                />
              </div>

              <div className="space-y-1">
                <label className="text-[11px] font-semibold text-slate-300">
                  Operator Notes (Optional)
                </label>
                <textarea
                  rows={2}
                  value={submissionNotes}
                  onChange={e => setSubmissionNotes(e.target.value)}
                  placeholder="Additional context for reviewing manager..."
                  className="w-full px-3 py-1.5 rounded-lg bg-slate-800 border border-slate-700 text-slate-200 text-xs placeholder-slate-500 focus:outline-none focus:border-sky-500"
                />
              </div>
            </div>

            <div className="p-2 rounded-lg bg-sky-500/10 border border-sky-500/20 text-[10px] text-sky-300">
              Submitting records this request as <strong>PENDING_REVIEW</strong>. Stock will NOT be deducted or reserved until approved and received.
            </div>

            <div className="flex items-center justify-end gap-2 pt-2 border-t border-slate-800">
              <button
                type="button"
                onClick={() => setSubmissionTarget(null)}
                className="px-3 py-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300 text-xs font-semibold cursor-pointer"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={handleConfirmSubmit}
                disabled={submittingModal || submissionQuantity <= 0}
                className="px-4 py-1.5 rounded-lg bg-sky-600 hover:bg-sky-500 text-white text-xs font-bold cursor-pointer disabled:opacity-50 transition-colors"
              >
                {submittingModal ? 'Submitting...' : 'Submit Request'}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Safety Notice Footer */}
      <div className="pt-2 border-t border-slate-700/40 flex items-center gap-2 text-[11px] text-slate-400">
        <Info className="w-3.5 h-3.5 text-sky-400 flex-shrink-0" />
        <span>
          <strong>Human-in-the-Loop Governance:</strong> Recommendations are advisory. Requests require operator review,
          manager approval, and server-side stock revalidation before order dispatch.
        </span>
      </div>
    </div>
  );
}
