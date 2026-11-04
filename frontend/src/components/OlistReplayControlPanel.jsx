import React, { useState, useEffect, useCallback } from 'react';
import {
  Play,
  Pause,
  Square,
  RefreshCw,
  Database,
  CheckCircle2,
  AlertTriangle,
  Clock,
  Layers,
  ArrowRight,
  TrendingUp,
  FileSpreadsheet
} from 'lucide-react';
import {
  startOlistReplay,
  pauseOlistReplay,
  resumeOlistReplay,
  stopOlistReplay,
  getOlistReplayStatus,
  previewOlistReplay
} from '../api/client';

export default function OlistReplayControlPanel({ refreshTrigger }) {
  const [statusData, setStatusData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [errorMsg, setErrorMsg] = useState(null);
  const [previewData, setPreviewData] = useState(null);

  // Form options
  const [windowStart, setWindowStart] = useState('2018-W36');
  const [windowEnd, setWindowEnd] = useState('2018-W42');
  const [batchSize, setBatchSize] = useState(25);
  const [intervalSec, setIntervalSec] = useState(5.0);
  const [autoFallback, setAutoFallback] = useState(true);

  const fetchStatus = useCallback(async () => {
    try {
      const data = await getOlistReplayStatus();
      setStatusData(data);
    } catch (err) {
      // non-critical
    }
  }, []);

  useEffect(() => {
    fetchStatus();
  }, [fetchStatus, refreshTrigger]);

  // Polling when RUNNING or PAUSED
  useEffect(() => {
    if (statusData?.status === 'RUNNING' || statusData?.status === 'PAUSED') {
      const timer = setInterval(() => {
        fetchStatus();
      }, 1500);
      return () => clearInterval(timer);
    }
  }, [statusData?.status, fetchStatus]);

  const handleStart = async () => {
    setLoading(true);
    setErrorMsg(null);
    try {
      const res = await startOlistReplay({
        window_start: windowStart,
        window_end: windowEnd,
        batch_size: parseInt(batchSize, 10),
        interval_seconds: parseFloat(intervalSec),
        auto_fallback: autoFallback
      });
      setStatusData(res);
    } catch (err) {
      setErrorMsg(err.response?.data?.detail || err.message || 'Failed to start replay');
    } finally {
      setLoading(false);
    }
  };

  const handlePause = async () => {
    setLoading(true);
    try {
      const res = await pauseOlistReplay();
      setStatusData(res);
    } catch (err) {
      setErrorMsg(err.response?.data?.detail || err.message);
    } finally {
      setLoading(false);
    }
  };

  const handleResume = async () => {
    setLoading(true);
    try {
      const res = await resumeOlistReplay();
      setStatusData(res);
    } catch (err) {
      setErrorMsg(err.response?.data?.detail || err.message);
    } finally {
      setLoading(false);
    }
  };

  const handleStop = async () => {
    setLoading(true);
    try {
      const res = await stopOlistReplay();
      setStatusData(res);
    } catch (err) {
      setErrorMsg(err.response?.data?.detail || err.message);
    } finally {
      setLoading(false);
    }
  };

  const handlePreview = async () => {
    setLoading(true);
    setErrorMsg(null);
    try {
      const res = await previewOlistReplay({
        window_start: windowStart,
        window_end: windowEnd,
        batch_size: parseInt(batchSize, 10),
        auto_fallback: autoFallback
      });
      setPreviewData(res);
    } catch (err) {
      setErrorMsg(err.response?.data?.detail || err.message);
    } finally {
      setLoading(false);
    }
  };

  const st = statusData?.status || 'IDLE';
  const progressPct = statusData?.progress_pct || 0;

  const getStatusColor = (s) => {
    switch (s) {
      case 'RUNNING': return 'bg-emerald-500/20 text-emerald-400 border-emerald-500/40';
      case 'PAUSED': return 'bg-amber-500/20 text-amber-400 border-amber-500/40';
      case 'STOPPED': return 'bg-rose-500/20 text-rose-400 border-rose-500/40';
      case 'COMPLETED': return 'bg-sky-500/20 text-sky-400 border-sky-500/40';
      default: return 'bg-slate-700/50 text-slate-300 border-slate-600';
    }
  };

  return (
    <div className="rounded-xl border border-sky-800/40 bg-slate-900/70 p-5 shadow-lg backdrop-blur-sm">
      <div className="flex flex-wrap items-center justify-between gap-4 border-b border-slate-800 pb-4 mb-4">
        <div className="flex items-center gap-3">
          <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-sky-500/10 border border-sky-500/30 text-sky-400">
            <FileSpreadsheet className="h-5 w-5" />
          </div>
          <div>
            <div className="flex items-center gap-2">
              <h3 className="text-base font-semibold text-slate-100">Olist Historical Order Replay Simulator</h3>
              <span className={`px-2.5 py-0.5 text-xs font-mono font-medium rounded-full border ${getStatusColor(st)}`}>
                {st}
              </span>
            </div>
            <p className="text-xs text-slate-400 mt-0.5">
              Streams historical orders from <code className="text-sky-300">Olist/</code> into <code className="text-sky-300">POST /api/observability/orders</code> with 8-week operational timestamp mapping.
            </p>
          </div>
        </div>

        {/* Action Controls */}
        <div className="flex items-center gap-2">
          {st === 'IDLE' || st === 'STOPPED' || st === 'COMPLETED' ? (
            <button
              onClick={handleStart}
              disabled={loading}
              className="flex items-center gap-1.5 px-3.5 py-1.5 text-xs font-medium rounded-lg bg-emerald-600 hover:bg-emerald-500 text-white shadow-sm transition disabled:opacity-50"
            >
              <Play className="h-3.5 w-3.5 fill-current" />
              Start Replay
            </button>
          ) : null}

          {st === 'RUNNING' ? (
            <button
              onClick={handlePause}
              disabled={loading}
              className="flex items-center gap-1.5 px-3.5 py-1.5 text-xs font-medium rounded-lg bg-amber-600 hover:bg-amber-500 text-white shadow-sm transition disabled:opacity-50"
            >
              <Pause className="h-3.5 w-3.5 fill-current" />
              Pause
            </button>
          ) : null}

          {st === 'PAUSED' ? (
            <button
              onClick={handleResume}
              disabled={loading}
              className="flex items-center gap-1.5 px-3.5 py-1.5 text-xs font-medium rounded-lg bg-emerald-600 hover:bg-emerald-500 text-white shadow-sm transition disabled:opacity-50"
            >
              <Play className="h-3.5 w-3.5 fill-current" />
              Resume
            </button>
          ) : null}

          {st === 'RUNNING' || st === 'PAUSED' ? (
            <button
              onClick={handleStop}
              disabled={loading}
              className="flex items-center gap-1.5 px-3.5 py-1.5 text-xs font-medium rounded-lg bg-rose-600 hover:bg-rose-500 text-white shadow-sm transition disabled:opacity-50"
            >
              <Square className="h-3.5 w-3.5 fill-current" />
              Stop
            </button>
          ) : null}

          <button
            onClick={fetchStatus}
            disabled={loading}
            title="Refresh status"
            className="p-1.5 text-slate-400 hover:text-slate-200 rounded-lg bg-slate-800 hover:bg-slate-700 border border-slate-700 transition"
          >
            <RefreshCw className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
          </button>
        </div>
      </div>

      {errorMsg && (
        <div className="mb-4 flex items-center gap-2 rounded-lg border border-rose-500/30 bg-rose-500/10 p-3 text-xs text-rose-300">
          <AlertTriangle className="h-4 w-4 flex-shrink-0" />
          <span>{errorMsg}</span>
        </div>
      )}

      {/* Progress Bar */}
      <div className="mb-5 space-y-1.5">
        <div className="flex justify-between text-xs text-slate-400 font-mono">
          <span>Simulation Progress</span>
          <span className="text-slate-200 font-semibold">{progressPct}%</span>
        </div>
        <div className="h-2 w-full overflow-hidden rounded-full bg-slate-800 border border-slate-700/60">
          <div
            className="h-full bg-gradient-to-r from-sky-500 to-emerald-400 transition-all duration-300"
            style={{ width: `${Math.min(100, Math.max(0, progressPct))}%` }}
          />
        </div>
      </div>

      {/* 10 Required Simulator Display Metrics */}
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3 mb-4">
        {/* 1. Total eligible orders */}
        <div className="rounded-lg border border-slate-800 bg-slate-800/40 p-3">
          <div className="text-[11px] text-slate-400">Total Eligible Orders</div>
          <div className="text-lg font-bold font-mono text-slate-100 mt-1">
            {statusData?.total_eligible_orders?.toLocaleString() ?? '—'}
          </div>
          <div className="text-[10px] text-slate-500 mt-0.5">Olist source pool</div>
        </div>

        {/* 2. Processed orders */}
        <div className="rounded-lg border border-slate-800 bg-slate-800/40 p-3">
          <div className="text-[11px] text-slate-400">Processed Orders</div>
          <div className="text-lg font-bold font-mono text-emerald-400 mt-1">
            {statusData?.processed_orders?.toLocaleString() ?? 0}
          </div>
          <div className="text-[10px] text-emerald-500/70 mt-0.5">Ingested to pipeline</div>
        </div>

        {/* 3. Remaining orders */}
        <div className="rounded-lg border border-slate-800 bg-slate-800/40 p-3">
          <div className="text-[11px] text-slate-400">Remaining Orders</div>
          <div className="text-lg font-bold font-mono text-amber-400 mt-1">
            {statusData?.remaining_orders?.toLocaleString() ?? '—'}
          </div>
          <div className="text-[10px] text-amber-500/70 mt-0.5">Pending stream</div>
        </div>

        {/* 4. Current batch / total batches */}
        <div className="rounded-lg border border-slate-800 bg-slate-800/40 p-3">
          <div className="text-[11px] text-slate-400">Current Batch</div>
          <div className="text-lg font-bold font-mono text-sky-400 mt-1">
            {statusData?.current_batch ?? 0} <span className="text-xs text-slate-500 font-normal">/ {statusData?.total_batches ?? 0}</span>
          </div>
          <div className="text-[10px] text-slate-500 mt-0.5">Batch execution</div>
        </div>

        {/* 5. Successful batches */}
        <div className="rounded-lg border border-slate-800 bg-slate-800/40 p-3">
          <div className="text-[11px] text-slate-400">Successful Batches</div>
          <div className="text-lg font-bold font-mono text-emerald-400 mt-1">
            {statusData?.successful_batches ?? 0}
          </div>
          <div className="text-[10px] text-emerald-500/70 mt-0.5">200 OK / Ingested</div>
        </div>

        {/* 6. Failed batches */}
        <div className="rounded-lg border border-slate-800 bg-slate-800/40 p-3">
          <div className="text-[11px] text-slate-400">Failed Batches</div>
          <div className="text-lg font-bold font-mono text-rose-400 mt-1">
            {statusData?.failed_batches ?? 0}
          </div>
          <div className="text-[10px] text-rose-500/70 mt-0.5">Rejections/errors</div>
        </div>
      </div>

      {/* Configuration & Meta Bar */}
      <div className="flex flex-wrap items-center justify-between gap-4 rounded-lg bg-slate-950/40 p-3 border border-slate-800/60 text-xs">
        <div className="flex flex-wrap items-center gap-4">
          <div className="flex items-center gap-1.5 text-slate-300">
            <span className="text-slate-500">Replay Window:</span>
            <span className="font-mono text-sky-300">{statusData?.metadata?.window_active || `${windowStart}..${windowEnd}`}</span>
            {statusData?.metadata?.auto_fallback_applied && (
              <span className="px-1.5 py-0.2 rounded bg-amber-500/10 text-amber-400 text-[10px] border border-amber-500/30">
                Fallback Active
              </span>
            )}
          </div>
          <div className="flex items-center gap-1.5 text-slate-300">
            <span className="text-slate-500">Batch Size:</span>
            <span className="font-mono text-slate-200">{statusData?.metadata?.batch_size || batchSize} orders</span>
          </div>
          <div className="flex items-center gap-1.5 text-slate-300">
            <span className="text-slate-500">Interval:</span>
            <span className="font-mono text-slate-200">{statusData?.interval_seconds || intervalSec}s</span>
          </div>
          <div className="flex items-center gap-1.5 text-slate-300">
            <span className="text-slate-500">Source:</span>
            <span className="font-mono text-purple-300">SIMULATED</span>
          </div>
        </div>

        <div className="flex items-center gap-2">
          <button
            onClick={handlePreview}
            disabled={loading || st === 'RUNNING'}
            className="px-2.5 py-1 text-[11px] rounded bg-slate-800 hover:bg-slate-700 text-slate-300 border border-slate-700 transition"
          >
            Preview Window
          </button>
        </div>
      </div>

      {previewData && (
        <div className="mt-3 rounded-lg border border-sky-800/30 bg-sky-950/20 p-3 text-xs">
          <div className="flex items-center justify-between font-semibold text-sky-300 mb-2">
            <span>Window Preview Summary</span>
            <button onClick={() => setPreviewData(null)} className="text-slate-400 hover:text-slate-200">×</button>
          </div>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 text-slate-300 mb-2">
            <div>Window: <span className="font-mono text-sky-200">{previewData.metadata.window_active}</span></div>
            <div>Eligible Orders: <span className="font-mono text-emerald-300">{previewData.metadata.total_eligible_orders.toLocaleString()}</span></div>
            <div>Total Items: <span className="font-mono text-slate-200">{previewData.metadata.total_line_items.toLocaleString()}</span></div>
            <div>Batches: <span className="font-mono text-purple-300">{previewData.metadata.total_batches}</span></div>
          </div>
          {previewData.sample_batch?.sample_orders?.length > 0 && (
            <div className="font-mono text-[11px] text-slate-400 bg-slate-900/60 p-2 rounded border border-slate-800">
              Sample line item: SKU={previewData.sample_batch.sample_orders[0].sku}, Qty={previewData.sample_batch.sample_orders[0].quantity}, Price=R${previewData.sample_batch.sample_orders[0].unit_price}, SimTime={previewData.sample_batch.sample_orders[0].event_time.slice(0, 19)}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
