import React, { useState, useEffect, useCallback } from 'react';
import {
  AlertTriangle,
  AlertCircle,
  Info,
  RefreshCw,
  Radio,
  ShieldAlert,
  ShieldCheck,
  Activity,
  Filter,
} from 'lucide-react';
import { getAlerts, getEventHealth } from '../api/client';
import LoadingSpinner from './LoadingSpinner';
import EmptyState from './EmptyState';

// ─── Severity badge ────────────────────────────────────────────────────────────

function SeverityBadge({ severity }) {
  const cfg = {
    CRITICAL: {
      cls: 'bg-rose-500/15 text-rose-400 border-rose-500/30',
      icon: <AlertCircle className="w-3 h-3" />,
    },
    WARNING: {
      cls: 'bg-amber-500/15 text-amber-400 border-amber-500/30',
      icon: <AlertTriangle className="w-3 h-3" />,
    },
    INFO: {
      cls: 'bg-sky-500/15 text-sky-400 border-sky-500/30',
      icon: <Info className="w-3 h-3" />,
    },
  };
  const { cls, icon } = cfg[severity] || cfg.INFO;
  return (
    <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[11px] font-bold uppercase tracking-wide border ${cls}`}>
      {icon}
      {severity}
    </span>
  );
}

// ─── Category label ────────────────────────────────────────────────────────────

const CATEGORY_LABELS = {
  PIPELINE: 'Pipeline',
  SUPPLIER: 'Supplier',
  CATALOG: 'Catalog',
  REPLENISHMENT: 'Replenishment',
};

// ─── Event health pill ────────────────────────────────────────────────────────

function EventHealthPill({ health }) {
  if (!health) return null;
  const status = health.status || 'UNKNOWN';
  const clsMap = {
    HEALTHY: 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20',
    DEGRADED: 'bg-amber-500/10 text-amber-400 border-amber-500/20',
    UNKNOWN: 'bg-slate-700/50 text-slate-400 border-slate-600',
  };
  return (
    <span className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-semibold border ${clsMap[status] || clsMap.UNKNOWN}`}>
      <span className={`w-1.5 h-1.5 rounded-full ${status === 'HEALTHY' ? 'bg-emerald-400 animate-pulse' : status === 'DEGRADED' ? 'bg-amber-400' : 'bg-slate-400'}`} />
      Event Bus: {status}
    </span>
  );
}

// ─── Main component ────────────────────────────────────────────────────────────

const SEVERITIES = ['ALL', 'CRITICAL', 'WARNING', 'INFO'];
const CATEGORIES = ['ALL', 'PIPELINE', 'SUPPLIER', 'CATALOG', 'REPLENISHMENT'];

export default function AlertsPanel({ refreshTrigger }) {
  const [alertData, setAlertData] = useState(null);   // { summary, alerts }
  const [health, setHealth] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [refreshing, setRefreshing] = useState(false);

  const [severityFilter, setSeverityFilter] = useState('ALL');
  const [categoryFilter, setCategoryFilter] = useState('ALL');

  const loadData = useCallback(async (isManualRefresh = false) => {
    try {
      if (isManualRefresh) setRefreshing(true);
      else setLoading(true);
      setError(null);

      const params = { limit: 100, exclude_simulated: true };
      if (severityFilter !== 'ALL') params.severity = severityFilter;
      if (categoryFilter !== 'ALL') params.category = categoryFilter;

      const [alertRes, healthRes] = await Promise.all([
        getAlerts(params).catch(() => ({ summary: { total_alerts: 0, critical_count: 0, warning_count: 0, info_count: 0, by_category: {} }, alerts: [] })),
        getEventHealth().catch(() => null),
      ]);

      setAlertData(alertRes);
      setHealth(healthRes);
    } catch (err) {
      setError(err?.message || 'Failed to load operational alerts.');
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, [severityFilter, categoryFilter]);

  // Reload when filters change or parent triggers refresh
  useEffect(() => {
    loadData();
  }, [loadData, refreshTrigger]);

  // ── Render ────────────────────────────────────────────────────────────────

  const summary = alertData?.summary || {};
  const alerts = alertData?.alerts || [];

  return (
    <div className="bg-slate-800/80 border border-slate-700/70 rounded-2xl p-5 shadow-sm space-y-4">

      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 pb-3 border-b border-slate-700/60">
        <div>
          <div className="flex items-center gap-2">
            <ShieldAlert className="w-4 h-4 text-rose-400" />
            <h3 className="text-sm font-bold text-white uppercase tracking-wider">
              Operational Health &amp; Alerts
            </h3>
          </div>
          <p className="text-xs text-slate-400 mt-0.5">
            Real-time failure detection across suppliers, catalog, replenishments, and pipeline executions.
          </p>
        </div>

        <div className="flex items-center gap-2 flex-wrap">
          <EventHealthPill health={health} />
          <button
            onClick={() => loadData(true)}
            disabled={refreshing}
            className="p-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300 border border-slate-700 text-xs flex items-center gap-1 cursor-pointer transition-colors"
            title="Refresh alerts"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${refreshing ? 'animate-spin' : ''}`} />
          </button>
        </div>
      </div>

      {/* KPI Summary Row */}
      {summary.total_alerts !== undefined && (
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
          {[
            { label: 'Total Alerts', value: summary.total_alerts, cls: 'text-white' },
            { label: 'Critical', value: summary.critical_count, cls: 'text-rose-400' },
            { label: 'Warning', value: summary.warning_count, cls: 'text-amber-400' },
            { label: 'Info', value: summary.info_count, cls: 'text-sky-400' },
          ].map(({ label, value, cls }) => (
            <div key={label} className="bg-slate-900/60 rounded-xl p-3 border border-slate-700/50 text-center">
              <div className={`text-xl font-bold font-mono ${cls}`}>{value ?? 0}</div>
              <div className="text-[11px] text-slate-400 mt-0.5">{label}</div>
            </div>
          ))}
        </div>
      )}

      {/* Category breakdown */}
      {summary.by_category && Object.keys(summary.by_category).length > 0 && (
        <div className="flex flex-wrap gap-2 text-xs">
          {Object.entries(summary.by_category).map(([cat, count]) => (
            <span
              key={cat}
              className="px-2.5 py-1 rounded-full bg-slate-900/70 border border-slate-700 text-slate-300 font-medium"
            >
              {CATEGORY_LABELS[cat] || cat}: <span className="text-white font-bold">{count}</span>
            </span>
          ))}
        </div>
      )}

      {/* Event Journal Metrics */}
      {health?.journal && (
        <div className="bg-slate-900/50 border border-slate-700/50 rounded-xl p-3 text-xs flex flex-wrap gap-x-6 gap-y-1.5">
          <div className="flex items-center gap-1.5 text-slate-400">
            <Activity className="w-3.5 h-3.5 text-emerald-400" />
            <span>Event Journal:</span>
            <span className="text-white font-semibold font-mono">{health.journal.total_events ?? '—'} total</span>
          </div>
          {health.journal.events_last_24h !== undefined && (
            <div className="text-slate-400">
              24h: <span className="text-white font-semibold font-mono">{health.journal.events_last_24h}</span>
            </div>
          )}
          {health.journal.latest_timestamp && (
            <div className="text-slate-400">
              Latest: <span className="text-slate-300 font-mono">{new Date(health.journal.latest_timestamp).toLocaleTimeString()}</span>
            </div>
          )}
          {health.sse && (
            <div className="text-slate-400">
              SSE Subscribers: <span className="text-white font-semibold font-mono">{health.sse.active_subscribers ?? 0}</span>
            </div>
          )}
        </div>
      )}

      {/* Filters */}
      <div className="flex flex-wrap items-center gap-2 text-xs pt-1">
        <Filter className="w-3.5 h-3.5 text-slate-400" />
        <select
          value={severityFilter}
          onChange={e => setSeverityFilter(e.target.value)}
          className="py-1.5 px-2.5 rounded-lg bg-slate-900 border border-slate-700 text-slate-200 text-xs focus:outline-none focus:border-rose-500"
        >
          {SEVERITIES.map(s => <option key={s} value={s}>{s === 'ALL' ? 'All Severities' : s}</option>)}
        </select>
        <select
          value={categoryFilter}
          onChange={e => setCategoryFilter(e.target.value)}
          className="py-1.5 px-2.5 rounded-lg bg-slate-900 border border-slate-700 text-slate-200 text-xs focus:outline-none focus:border-rose-500"
        >
          {CATEGORIES.map(c => <option key={c} value={c}>{c === 'ALL' ? 'All Categories' : (CATEGORY_LABELS[c] || c)}</option>)}
        </select>
      </div>

      {/* Alert List */}
      {loading && !alerts.length ? (
        <div className="py-10 flex flex-col items-center gap-3">
          <LoadingSpinner size="md" />
          <p className="text-xs text-slate-400">Querying operational alert conditions…</p>
        </div>
      ) : error ? (
        <div className="p-4 rounded-xl bg-rose-500/10 border border-rose-500/20 text-rose-300 text-xs flex items-center gap-3">
          <AlertCircle className="w-4 h-4 flex-shrink-0" />
          {error}
        </div>
      ) : alerts.length === 0 ? (
        <EmptyState
          title="No alerts detected"
          description="All suppliers, catalog entries, replenishments, and pipeline executions are within normal operating parameters."
          icon={ShieldCheck}
        />
      ) : (
        <div className="space-y-2 max-h-96 overflow-y-auto pr-1">
          {alerts.map(alert => (
            <div
              key={alert.alert_id}
              className={`p-3 rounded-xl border text-xs flex flex-col sm:flex-row sm:items-center justify-between gap-2 transition-all ${
                alert.severity === 'CRITICAL'
                  ? 'bg-rose-950/25 border-rose-500/25 hover:border-rose-500/40'
                  : alert.severity === 'WARNING'
                  ? 'bg-amber-950/20 border-amber-500/20 hover:border-amber-500/35'
                  : 'bg-slate-900/50 border-slate-700/50'
              }`}
            >
              <div className="flex items-start gap-3 flex-1 min-w-0">
                {/* Severity dot */}
                <span className={`mt-0.5 w-2 h-2 rounded-full flex-shrink-0 ${
                  alert.severity === 'CRITICAL' ? 'bg-rose-400' : alert.severity === 'WARNING' ? 'bg-amber-400' : 'bg-sky-400'
                }`} />
                <div className="min-w-0">
                  <div className="flex flex-wrap items-center gap-2 mb-0.5">
                    <SeverityBadge severity={alert.severity} />
                    <span className="text-[11px] px-2 py-0.5 rounded bg-slate-800 text-slate-400 border border-slate-700 uppercase font-semibold tracking-wide">
                      {CATEGORY_LABELS[alert.category] || alert.category}
                    </span>
                    <span className="font-semibold text-white">{alert.title}</span>
                  </div>
                  <p className="text-slate-400 text-[11px] leading-relaxed truncate">{alert.condition}</p>
                  {alert.identifiers && Object.keys(alert.identifiers).length > 0 && (
                    <div className="flex flex-wrap gap-1.5 mt-1">
                      {Object.entries(alert.identifiers).map(([k, v]) => (
                        <span key={k} className="font-mono text-[10px] px-1.5 py-0.5 rounded bg-slate-800/80 text-slate-300 border border-slate-700">
                          {k}: {v}
                        </span>
                      ))}
                    </div>
                  )}
                </div>
              </div>
              <div className="flex-shrink-0 text-right">
                <div className="font-mono text-[10px] text-slate-500">
                  {alert.timestamp ? new Date(alert.timestamp).toLocaleString() : '—'}
                </div>
                {alert.actionable_link?.tab && (
                  <div className="text-[10px] text-sky-400 mt-1 font-medium">→ {alert.actionable_link.tab}</div>
                )}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
