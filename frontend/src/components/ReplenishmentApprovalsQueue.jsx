import React, { useState, useEffect, useCallback } from 'react';
import {
  CheckCircle2,
  XCircle,
  AlertTriangle,
  Clock,
  ShieldCheck,
  ShieldAlert,
  Search,
  Filter,
  RefreshCw,
  FileText,
  User,
  Package,
  Layers,
  ArrowRight,
  Info,
  AlertCircle
} from 'lucide-react';
import { getApprovals, getApprovalDetail, approveReplenishment, rejectReplenishment } from '../api/client';
import LoadingSpinner from './LoadingSpinner';
import EmptyState from './EmptyState';

function StatusBadge({ status }) {
  const configs = {
    PENDING_REVIEW: {
      label: 'PENDING REVIEW',
      cls: 'bg-amber-500/15 text-amber-300 border-amber-500/30',
      icon: <Clock className="w-3.5 h-3.5" />
    },
    APPROVED: {
      label: 'APPROVED',
      cls: 'bg-emerald-500/15 text-emerald-300 border-emerald-500/30',
      icon: <CheckCircle2 className="w-3.5 h-3.5" />
    },
    DISPATCHED: {
      label: 'DISPATCHED',
      cls: 'bg-sky-500/15 text-sky-300 border-sky-500/30',
      icon: <ShieldCheck className="w-3.5 h-3.5" />
    },
    REJECTED: {
      label: 'REJECTED',
      cls: 'bg-rose-500/15 text-rose-300 border-rose-500/30',
      icon: <XCircle className="w-3.5 h-3.5" />
    },
    FAILED: {
      label: 'FAILED',
      cls: 'bg-red-900/30 text-red-300 border-red-700/50',
      icon: <ShieldAlert className="w-3.5 h-3.5" />
    }
  };

  const current = configs[status] || configs.PENDING_REVIEW;
  return (
    <span className={`inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-[11px] font-bold uppercase tracking-wider border ${current.cls}`}>
      {current.icon}
      {current.label}
    </span>
  );
}

export default function ReplenishmentApprovalsQueue({
  actorRole = 'MANAGER',
  actorId = 'mgr-sarah-chen',
  refreshTrigger = 0,
  onWorkflowComplete
}) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [statusFilter, setStatusFilter] = useState('ALL');
  const [skuFilter, setSkuFilter] = useState('');
  const [actionError, setActionError] = useState(null);
  const [actionSuccess, setActionSuccess] = useState(null);

  // Rejection modal state
  const [rejectionTarget, setRejectionTarget] = useState(null);
  const [rejectionReason, setRejectionReason] = useState('');
  const [submittingAction, setSubmittingAction] = useState(false);

  // Audit detail modal state
  const [auditTarget, setAuditTarget] = useState(null);
  const [auditLoading, setAuditLoading] = useState(false);

  const fetchApprovals = useCallback(async (isManual = false) => {
    try {
      if (isManual) setRefreshing(true);
      else setLoading(true);
      setActionError(null);

      const params = { limit: 100 };
      if (statusFilter !== 'ALL') params.status = statusFilter;
      if (skuFilter.trim()) params.sku = skuFilter.trim();

      const res = await getApprovals(params);
      setData(res);
    } catch (err) {
      setActionError(err?.response?.data?.detail || err?.message || 'Failed to load approvals queue.');
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, [statusFilter, skuFilter]);

  useEffect(() => {
    fetchApprovals();
  }, [fetchApprovals, refreshTrigger]);

  const handleApprove = async (approval) => {
    if (submittingAction) return;
    setSubmittingAction(true);
    setActionError(null);
    setActionSuccess(null);

    try {
      const headers = {
        'X-Actor-ID': actorId,
        'X-Actor-Role': actorRole
      };
      const res = await approveReplenishment(approval.approval_id, {}, headers);
      setActionSuccess(`Approval ${approval.approval_id} approved and dispatched! Order: ${res.approval?.replenishment_id || 'DISPATCHED'}`);
      fetchApprovals(true);
      if (onWorkflowComplete) onWorkflowComplete();
    } catch (err) {
      const detail = err?.response?.data?.detail;
      const msg = typeof detail === 'string' ? detail : JSON.stringify(detail) || err.message;
      setActionError(`Approval failed: ${msg}`);
    } finally {
      setSubmittingAction(false);
    }
  };

  const handleOpenReject = (approval) => {
    setRejectionTarget(approval);
    setRejectionReason('');
    setActionError(null);
  };

  const handleConfirmReject = async () => {
    if (!rejectionTarget || !rejectionReason.trim() || submittingAction) return;
    setSubmittingAction(true);
    setActionError(null);
    setActionSuccess(null);

    try {
      const headers = {
        'X-Actor-ID': actorId,
        'X-Actor-Role': actorRole
      };
      await rejectReplenishment(
        rejectionTarget.approval_id,
        { reason: rejectionReason.trim(), notes: `Rejected via dashboard by ${actorRole}` },
        headers
      );
      setActionSuccess(`Approval ${rejectionTarget.approval_id} rejected successfully.`);
      setRejectionTarget(null);
      setRejectionReason('');
      fetchApprovals(true);
      if (onWorkflowComplete) onWorkflowComplete();
    } catch (err) {
      const detail = err?.response?.data?.detail;
      const msg = typeof detail === 'string' ? detail : JSON.stringify(detail) || err.message;
      setActionError(`Rejection failed: ${msg}`);
    } finally {
      setSubmittingAction(false);
    }
  };

  const handleViewAudit = async (approvalId) => {
    try {
      setAuditLoading(true);
      setAuditTarget({ approval_id: approvalId, audit_trail: [] });
      const detail = await getApprovalDetail(approvalId);
      setAuditTarget(detail);
    } catch (err) {
      setActionError(`Failed to load audit history: ${err.message}`);
      setAuditTarget(null);
    } finally {
      setAuditLoading(false);
    }
  };

  const summary = data?.summary || {};
  const approvalsList = data?.approvals || [];
  const isManagerOrAdmin = actorRole === 'MANAGER' || actorRole === 'ADMIN';

  return (
    <div className="space-y-4">
      {/* ── Action Notifications ── */}
      {actionSuccess && (
        <div className="p-3 rounded-xl bg-emerald-500/10 border border-emerald-500/20 text-emerald-300 text-xs flex items-center justify-between">
          <div className="flex items-center gap-2">
            <CheckCircle2 className="w-4 h-4 flex-shrink-0" />
            <span>{actionSuccess}</span>
          </div>
          <button onClick={() => setActionSuccess(null)} className="text-emerald-400 hover:text-white cursor-pointer font-bold">✕</button>
        </div>
      )}

      {actionError && (
        <div className="p-3 rounded-xl bg-rose-500/10 border border-rose-500/20 text-rose-300 text-xs flex items-center justify-between">
          <div className="flex items-center gap-2">
            <AlertCircle className="w-4 h-4 flex-shrink-0" />
            <span>{actionError}</span>
          </div>
          <button onClick={() => setActionError(null)} className="text-rose-400 hover:text-white cursor-pointer font-bold">✕</button>
        </div>
      )}

      {/* ── KPI Cards ── */}
      <div className="grid grid-cols-2 sm:grid-cols-5 gap-3">
        <div className="bg-slate-900/60 rounded-xl p-3 border border-slate-700/50 text-center">
          <div className="text-xl font-bold font-mono text-white">{summary.total ?? 0}</div>
          <div className="text-[11px] text-slate-400 mt-0.5">Total Requests</div>
        </div>
        <div className="bg-slate-900/60 rounded-xl p-3 border border-amber-500/20 text-center">
          <div className="text-xl font-bold font-mono text-amber-400">{summary.pending_review ?? 0}</div>
          <div className="text-[11px] text-slate-400 mt-0.5">Pending Review</div>
        </div>
        <div className="bg-slate-900/60 rounded-xl p-3 border border-sky-500/20 text-center">
          <div className="text-xl font-bold font-mono text-sky-400">{summary.dispatched ?? 0}</div>
          <div className="text-[11px] text-slate-400 mt-0.5">Dispatched</div>
        </div>
        <div className="bg-slate-900/60 rounded-xl p-3 border border-rose-500/20 text-center">
          <div className="text-xl font-bold font-mono text-rose-400">{summary.rejected ?? 0}</div>
          <div className="text-[11px] text-slate-400 mt-0.5">Rejected</div>
        </div>
        <div className="bg-slate-900/60 rounded-xl p-3 border border-red-900/40 text-center">
          <div className="text-xl font-bold font-mono text-red-400">{summary.failed ?? 0}</div>
          <div className="text-[11px] text-slate-400 mt-0.5">Failed</div>
        </div>
      </div>

      {/* ── Filters & Search ── */}
      <div className="flex flex-wrap items-center justify-between gap-2.5 pt-1 text-xs">
        <div className="flex flex-wrap items-center gap-2">
          {/* Status Tabs */}
          <div className="flex items-center rounded-lg bg-slate-900 border border-slate-700 p-0.5">
            {['ALL', 'PENDING_REVIEW', 'DISPATCHED', 'REJECTED'].map(st => (
              <button
                key={st}
                onClick={() => setStatusFilter(st)}
                className={`px-2.5 py-1 rounded-md font-semibold transition-all ${
                  statusFilter === st
                    ? 'bg-sky-500/20 text-sky-300 border border-sky-500/30'
                    : 'text-slate-400 hover:text-white'
                }`}
              >
                {st === 'ALL' ? 'All Requests' : st.replace('_', ' ')}
              </button>
            ))}
          </div>
        </div>

        <div className="flex items-center gap-2">
          {/* SKU Filter */}
          <div className="relative">
            <Search className="w-3.5 h-3.5 text-slate-400 absolute left-2.5 top-1/2 -translate-y-1/2" />
            <input
              type="text"
              placeholder="Filter by SKU..."
              value={skuFilter}
              onChange={e => setSkuFilter(e.target.value)}
              className="pl-8 pr-3 py-1.5 rounded-lg bg-slate-900 border border-slate-700 text-slate-200 placeholder-slate-500 text-xs focus:outline-none focus:border-sky-500"
            />
          </div>

          <button
            onClick={() => fetchApprovals(true)}
            disabled={refreshing}
            className="p-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300 border border-slate-700 text-xs flex items-center gap-1 cursor-pointer transition-colors"
            title="Refresh approvals"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${refreshing ? 'animate-spin' : ''}`} />
          </button>
        </div>
      </div>

      {/* ── Approval Requests List ── */}
      {loading && !approvalsList.length ? (
        <div className="py-12 flex flex-col items-center gap-3">
          <LoadingSpinner size="md" />
          <p className="text-xs text-slate-400">Loading replenishment approvals queue...</p>
        </div>
      ) : approvalsList.length === 0 ? (
        <EmptyState
          title="No approval requests match filters"
          description="Submit a replenishment recommendation from the Recommendations tab to create an approval request."
          icon={CheckCircle2}
        />
      ) : (
        <div className="space-y-3 max-h-[580px] overflow-y-auto pr-1">
          {approvalsList.map(app => {
            const isPending = app.status === 'PENDING_REVIEW';
            const isDispatched = app.status === 'DISPATCHED';
            const isRejected = app.status === 'REJECTED';

            const borderCls = isPending
              ? 'border-amber-500/30 bg-amber-950/10 hover:border-amber-500/50'
              : isDispatched
              ? 'border-sky-500/30 bg-sky-950/10 hover:border-sky-500/50'
              : isRejected
              ? 'border-rose-500/20 bg-rose-950/10 hover:border-rose-500/30'
              : 'border-slate-700/60 bg-slate-900/60';

            return (
              <div
                key={app.approval_id}
                className={`p-4 rounded-xl border text-xs transition-all space-y-3 ${borderCls}`}
              >
                {/* Header Row */}
                <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-2">
                  <div className="flex items-center gap-2 flex-wrap">
                    <span className="font-mono font-bold text-white text-sm">
                      {app.sku}
                    </span>
                    <span className="text-[10px] font-mono px-2 py-0.5 rounded bg-slate-800 text-slate-300 border border-slate-700">
                      WH: {app.warehouse}
                    </span>
                    <span className="text-[10px] font-mono px-2 py-0.5 rounded bg-slate-800 text-slate-300 border border-slate-700">
                      Req #{app.approval_id}
                    </span>
                    <span className="text-[10px] font-mono text-slate-400">
                      Exec #{app.execution_id}
                    </span>
                  </div>

                  <div className="flex items-center gap-2.5">
                    <div className="px-2.5 py-1 rounded-lg bg-sky-500/20 text-sky-300 border border-sky-500/30 font-mono font-bold text-xs">
                      Qty: {app.requested_quantity} units
                    </div>
                    <StatusBadge status={app.status} />
                  </div>
                </div>

                {/* Details Grid */}
                <div className="grid grid-cols-2 sm:grid-cols-4 gap-2.5 pt-1 text-[11px] border-t border-slate-700/40">
                  <div className="p-2 rounded-lg bg-slate-800/60 border border-slate-700/40">
                    <span className="text-slate-400 text-[10px] uppercase font-semibold">Stock at Submission</span>
                    <div className="font-mono font-bold text-white text-xs mt-0.5">
                      Available: {app.stock_at_submission?.available ?? '—'}
                      <span className="text-[10px] font-normal text-slate-400 ml-1">
                        (On-hand: {app.stock_at_submission?.on_hand ?? '—'})
                      </span>
                    </div>
                    <span className="text-[10px] text-slate-500 font-mono">Inv v{app.stock_at_submission?.version ?? 1}</span>
                  </div>

                  <div className="p-2 rounded-lg bg-slate-800/60 border border-slate-700/40">
                    <span className="text-slate-400 text-[10px] uppercase font-semibold">Assigned Supplier</span>
                    <div className="font-mono font-bold text-white text-xs mt-0.5">
                      {app.supplier_id || 'SUP-001'}
                    </div>
                    <span className="text-[10px] text-emerald-400">Status: Online</span>
                  </div>

                  <div className="p-2 rounded-lg bg-slate-800/60 border border-slate-700/40">
                    <span className="text-slate-400 text-[10px] uppercase font-semibold">Requester</span>
                    <div className="font-mono font-bold text-white text-xs mt-0.5 flex items-center gap-1">
                      <User className="w-3 h-3 text-slate-400" />
                      {app.created_by_actor_id}
                    </div>
                    <span className="text-[10px] text-slate-400">Role: {app.created_by_role}</span>
                  </div>

                  <div className="p-2 rounded-lg bg-slate-800/60 border border-slate-700/40">
                    <span className="text-slate-400 text-[10px] uppercase font-semibold">Timeline</span>
                    <div className="text-[10px] text-slate-300 mt-0.5">
                      Created: {new Date(app.created_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                    </div>
                    {app.reviewed_at && (
                      <div className="text-[10px] text-slate-400">
                        Reviewed: {new Date(app.reviewed_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                      </div>
                    )}
                  </div>
                </div>

                {/* Status-specific banners */}
                {isDispatched && (
                  <div className="p-2.5 rounded-lg bg-sky-950/40 border border-sky-500/30 flex items-center justify-between text-xs text-sky-200">
                    <div className="flex items-center gap-2">
                      <ShieldCheck className="w-4 h-4 text-sky-400 flex-shrink-0" />
                      <span>
                        Order Dispatched to Supplier: <strong className="font-mono text-white">{app.replenishment_id}</strong>
                        {app.decision_id && <span className="text-sky-400 ml-2">(Decision #{app.decision_id})</span>}
                      </span>
                    </div>
                    <span className="text-[10px] text-slate-400 font-mono">Status: CONFIRMED</span>
                  </div>
                )}

                {isRejected && (
                  <div className="p-2.5 rounded-lg bg-rose-950/40 border border-rose-500/30 text-xs text-rose-200 space-y-1">
                    <div className="flex items-center gap-2 font-semibold">
                      <XCircle className="w-4 h-4 text-rose-400 flex-shrink-0" />
                      <span>Rejected by {app.reviewed_by_actor_id} ({app.reviewed_by_role}):</span>
                    </div>
                    <p className="pl-6 text-rose-300 italic">"{app.rejection_reason}"</p>
                  </div>
                )}

                {/* Operator / Manager Actions Bar */}
                <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-2 pt-2 border-t border-slate-700/40">
                  <div className="flex items-center gap-3">
                    <button
                      onClick={() => handleViewAudit(app.approval_id)}
                      className="text-[11px] text-sky-400 hover:text-sky-300 font-semibold flex items-center gap-1 cursor-pointer transition-colors"
                    >
                      <FileText className="w-3.5 h-3.5" />
                      <span>Audit Trail History</span>
                    </button>
                    <span className="text-[10px] text-slate-500 font-mono truncate max-w-xs" title={app.idempotency_key}>
                      Key: {app.idempotency_key}
                    </span>
                  </div>

                  {isPending && (
                    <div className="flex items-center gap-2">
                      {isManagerOrAdmin ? (
                        <>
                          <button
                            onClick={() => handleOpenReject(app)}
                            disabled={submittingAction}
                            className="px-3 py-1.5 rounded-lg bg-rose-500/20 hover:bg-rose-500/30 text-rose-300 border border-rose-500/30 font-semibold text-xs flex items-center gap-1.5 cursor-pointer transition-colors disabled:opacity-50"
                          >
                            <XCircle className="w-3.5 h-3.5" />
                            <span>Reject</span>
                          </button>
                          <button
                            onClick={() => handleApprove(app)}
                            disabled={submittingAction}
                            className="px-3 py-1.5 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-white font-semibold text-xs flex items-center gap-1.5 cursor-pointer transition-colors shadow-sm disabled:opacity-50"
                          >
                            <CheckCircle2 className="w-3.5 h-3.5" />
                            <span>Approve & Dispatch</span>
                          </button>
                        </>
                      ) : (
                        <div className="flex items-center gap-1 text-[11px] text-slate-400 italic bg-slate-800/80 px-2.5 py-1 rounded-lg border border-slate-700">
                          <ShieldAlert className="w-3.5 h-3.5 text-amber-400" />
                          <span>Manager or Admin role required to approve/reject</span>
                        </div>
                      )}
                    </div>
                  )}
                </div>
              </div>
            );
          })}
        </div>
      )}

      {/* ── Rejection Reason Modal ── */}
      {rejectionTarget && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/70 backdrop-blur-sm">
          <div className="bg-slate-900 border border-slate-700 rounded-2xl max-w-md w-full p-5 space-y-4 shadow-xl">
            <div className="flex items-center justify-between pb-3 border-b border-slate-800">
              <div className="flex items-center gap-2 text-rose-400 font-bold text-sm">
                <XCircle className="w-4 h-4" />
                <span>Reject Replenishment Approval</span>
              </div>
              <button
                onClick={() => setRejectionTarget(null)}
                className="text-slate-400 hover:text-white cursor-pointer"
              >
                ✕
              </button>
            </div>

            <div className="text-xs text-slate-300 space-y-2">
              <p>
                Rejecting request <strong>#{rejectionTarget.approval_id}</strong> for{' '}
                <code className="text-white font-bold">{rejectionTarget.sku}</code> ({rejectionTarget.requested_quantity} units).
              </p>
              <div className="p-2.5 rounded-lg bg-amber-500/10 border border-amber-500/20 text-amber-300 text-[11px]">
                A specific rejection reason is mandatory and will be permanently recorded in the audit trail.
              </div>
            </div>

            <div className="space-y-1.5">
              <label className="text-[11px] font-semibold text-slate-300">
                Rejection Reason <span className="text-rose-400">*</span>
              </label>
              <textarea
                rows={3}
                value={rejectionReason}
                onChange={e => setRejectionReason(e.target.value)}
                placeholder="e.g. Supplier lead time exceeds required horizon; warehouse undergoing maintenance..."
                className="w-full px-3 py-2 rounded-lg bg-slate-800 border border-slate-700 text-slate-200 text-xs placeholder-slate-500 focus:outline-none focus:border-rose-500"
              />
            </div>

            <div className="flex items-center justify-end gap-2 pt-2 border-t border-slate-800">
              <button
                type="button"
                onClick={() => setRejectionTarget(null)}
                className="px-3 py-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300 text-xs font-semibold cursor-pointer"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={handleConfirmReject}
                disabled={!rejectionReason.trim() || submittingAction}
                className="px-4 py-1.5 rounded-lg bg-rose-600 hover:bg-rose-500 text-white text-xs font-bold cursor-pointer disabled:opacity-50 transition-colors"
              >
                {submittingAction ? 'Rejecting...' : 'Confirm Rejection'}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* ── Audit Trail Modal / Drawer ── */}
      {auditTarget && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/70 backdrop-blur-sm">
          <div className="bg-slate-900 border border-slate-700 rounded-2xl max-w-2xl w-full p-5 space-y-4 shadow-xl max-h-[85vh] flex flex-col">
            <div className="flex items-center justify-between pb-3 border-b border-slate-800">
              <div className="flex items-center gap-2 text-sky-400 font-bold text-sm">
                <FileText className="w-4 h-4" />
                <span>Approval Audit Trail — Request #{auditTarget.approval_id}</span>
              </div>
              <button
                onClick={() => setAuditTarget(null)}
                className="text-slate-400 hover:text-white cursor-pointer"
              >
                ✕
              </button>
            </div>

            {auditLoading ? (
              <div className="py-8 flex justify-center">
                <LoadingSpinner size="md" />
              </div>
            ) : (
              <div className="flex-1 overflow-y-auto space-y-3 pr-1 text-xs">
                <div className="p-3 rounded-lg bg-slate-800/80 border border-slate-700 text-slate-300 grid grid-cols-2 gap-2 text-[11px]">
                  <div>SKU: <strong className="text-white font-mono">{auditTarget.sku}</strong></div>
                  <div>Warehouse: <strong className="text-white font-mono">{auditTarget.warehouse}</strong></div>
                  <div>Quantity: <strong className="text-white font-mono">{auditTarget.requested_quantity}</strong></div>
                  <div>Current Status: <StatusBadge status={auditTarget.status} /></div>
                </div>

                <h4 className="text-[11px] font-bold text-slate-400 uppercase tracking-wider pt-2">
                  Transition History ({auditTarget.audit_trail?.length || 0} events)
                </h4>

                <div className="space-y-2">
                  {(auditTarget.audit_trail || []).map((step, idx) => (
                    <div
                      key={step.audit_id || idx}
                      className="p-3 rounded-lg bg-slate-800/40 border border-slate-700/60 space-y-1.5"
                    >
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-2">
                          <span className="font-bold text-sky-400 font-mono">{step.action}</span>
                          <span className="text-slate-400 text-[10px]">
                            {step.previous_status ? `${step.previous_status} → ` : ''}{step.new_status}
                          </span>
                        </div>
                        <span className="text-[10px] text-slate-400 font-mono">
                          {new Date(step.created_at).toLocaleString()}
                        </span>
                      </div>

                      <div className="text-[11px] text-slate-300 flex items-center gap-2">
                        <User className="w-3 h-3 text-slate-400" />
                        <span>Actor: <strong className="text-white">{step.actor_id}</strong> ({step.actor_role})</span>
                      </div>

                      {step.notes && (
                        <p className="text-[11px] text-slate-400 italic bg-slate-900/40 p-1.5 rounded border border-slate-800">
                          {step.notes}
                        </p>
                      )}
                    </div>
                  ))}
                </div>
              </div>
            )}

            <div className="flex justify-end pt-2 border-t border-slate-800">
              <button
                type="button"
                onClick={() => setAuditTarget(null)}
                className="px-4 py-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300 text-xs font-semibold cursor-pointer"
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
