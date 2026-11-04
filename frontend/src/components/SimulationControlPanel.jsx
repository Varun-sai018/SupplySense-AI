import React, { useState, useEffect, useCallback } from 'react';
import {
  Play,
  Pause,
  Square,
  RefreshCw,
  AlertTriangle,
  CheckCircle2,
  Loader2,
  ChevronDown,
  Zap,
} from 'lucide-react';
import {
  getSimulatorStatus,
  getSimulatorScenarios,
  startSimulator,
  pauseSimulator,
  resumeSimulator,
  stopSimulator,
} from '../api/client';

const IDLE_STATES = ['IDLE'];
const RUNNING_STATES = ['RUNNING'];
const PAUSED_STATES = ['PAUSED'];
const ACTIVE_STATES = ['RUNNING', 'PAUSED', 'STOPPING'];

const INTERVAL_OPTIONS = [5, 6, 7, 8, 9, 10];

const STATE_STYLES = {
  IDLE: 'text-slate-400',
  RUNNING: 'text-emerald-400',
  PAUSED: 'text-amber-400',
  STOPPING: 'text-sky-400',
  ERROR: 'text-rose-400',
};

const STATE_DOT = {
  IDLE: 'bg-slate-500',
  RUNNING: 'bg-emerald-400 animate-pulse',
  PAUSED: 'bg-amber-400',
  STOPPING: 'bg-sky-400 animate-pulse',
  ERROR: 'bg-rose-400 animate-pulse',
};

/**
 * SimulationControlPanel — Phase 3 Simulation Engine control surface.
 *
 * - Fetches real engine state from GET /api/simulator/status on mount and on refresh.
 * - Reacts to incoming SIMULATION_STATUS_CHANGED SSE events to stay in sync.
 * - Never shows RUNNING until the backend confirms it.
 * - Prevents invalid actions based on current engine state.
 * - Requires confirmation before stopping an active simulation.
 */
export default function SimulationControlPanel({ latestEvent, sseStatus, onStatusChange }) {
  const [status, setStatus] = useState(null);          // API status snapshot
  const [scenarios, setScenarios] = useState([]);      // Available scenarios
  const [mode, setMode] = useState('CONTINUOUS');      // Selected mode
  const [interval, setIntervalVal] = useState(7);      // Selected interval
  const [loading, setLoading] = useState(false);       // Action in-flight
  const [initialising, setInitialising] = useState(true);
  const [feedback, setFeedback] = useState(null);      // { type: 'success'|'error', msg }
  const [confirmStop, setConfirmStop] = useState(false);

  const engineState = status?.state ?? 'IDLE';

  // ─── Fetch current status ────────────────────────────────────────────────
  const refreshStatus = useCallback(async () => {
    try {
      const s = await getSimulatorStatus();
      setStatus(s);
      if (onStatusChange) onStatusChange(s);
    } catch {
      // Non-critical; status remains as-is
    }
  }, [onStatusChange]);

  // ─── Status polling fallback when SSE is unavailable or disconnected ────
  useEffect(() => {
    // If SSE is actively CONNECTED, pause HTTP polling and rely on SSE events
    if (sseStatus === 'CONNECTED') {
      return;
    }

    // Polling is active as a fallback whenever SSE is NOT CONNECTED (CONNECTING, RECONNECTING, OFFLINE, or unavailable)
    const pollTimer = setInterval(() => {
      refreshStatus();
    }, 2000);

    return () => {
      clearInterval(pollTimer);
    };
  }, [sseStatus, refreshStatus]);

  // ─── Fetch scenario catalogue on mount ──────────────────────────────────
  useEffect(() => {
    let alive = true;
    (async () => {
      try {
        const { scenarios: list } = await getSimulatorScenarios();
        if (alive) setScenarios(list || []);
      } catch {
        // non-critical
      }
      // Also grab initial status
      try {
        const s = await getSimulatorStatus();
        if (alive) {
          setStatus(s);
          if (onStatusChange) onStatusChange(s);
        }
      } catch {
        // non-critical
      }
      if (alive) setInitialising(false);
    })();
    return () => { alive = false; };
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // ─── React to SSE SIMULATION_STATUS_CHANGED events ──────────────────────
  useEffect(() => {
    if (!latestEvent) return;
    if (
      latestEvent.event_type === 'SIMULATION_STATUS_CHANGED' ||
      latestEvent.event_type === 'DATASET_READINESS_CHANGED'
    ) {
      refreshStatus();
    }
  }, [latestEvent, refreshStatus]);

  // ─── Clear feedback after 4 s ────────────────────────────────────────────
  useEffect(() => {
    if (!feedback) return;
    const t = setTimeout(() => setFeedback(null), 4000);
    return () => clearTimeout(t);
  }, [feedback]);

  // ─── Action helpers ──────────────────────────────────────────────────────
  const withAction = useCallback(async (fn, successMsg) => {
    setLoading(true);
    setFeedback(null);
    try {
      const result = await fn();
      setStatus(result);
      if (onStatusChange) onStatusChange(result);
      setFeedback({ type: 'success', msg: successMsg });
    } catch (err) {
      const detail = err?.response?.data?.detail || err?.message || 'Action failed';
      setFeedback({ type: 'error', msg: detail });
    } finally {
      setLoading(false);
    }
  }, [onStatusChange]);

  const handleStart = () => withAction(
    () => startSimulator({ mode, event_interval_seconds: interval }),
    `Simulation started — ${mode}`
  );

  const handlePause = () => withAction(
    () => pauseSimulator(),
    'Simulation paused'
  );

  const handleResume = () => withAction(
    () => resumeSimulator(),
    'Simulation resumed'
  );

  const handleStop = () => {
    if (!confirmStop && ACTIVE_STATES.includes(engineState)) {
      setConfirmStop(true);
      return;
    }
    setConfirmStop(false);
    withAction(() => stopSimulator(), 'Simulation stopped and datasets reset to WAITING');
  };

  // ─── Derived flags ───────────────────────────────────────────────────────
  const canStart   = IDLE_STATES.includes(engineState) && !loading;
  const canPause   = RUNNING_STATES.includes(engineState) && !loading;
  const canResume  = PAUSED_STATES.includes(engineState) && !loading;
  const canStop    = ACTIVE_STATES.includes(engineState) && !loading;

  if (initialising) {
    return (
      <div className="flex items-center gap-3 py-6 px-4 text-slate-400">
        <Loader2 className="w-5 h-5 animate-spin text-sky-400" />
        <span className="text-sm">Connecting to simulation engine…</span>
      </div>
    );
  }

  // ─── Build mode options: CONTINUOUS + each scenario ─────────────────────
  const modeOptions = [
    { value: 'CONTINUOUS', label: 'CONTINUOUS — Rotate S1→S2→S3→S4' },
    ...scenarios.map((s) => ({
      value: s.scenario_id,
      label: `${s.scenario_id} — ${s.pipeline_name} (${s.condition_type})`,
    })),
  ];

  return (
    <div className="space-y-4">
      {/* ── Engine State Indicator ────────────────────────────────────── */}
      <div className="flex items-center gap-3 pb-3 border-b border-slate-700/50">
        <span className={`h-2.5 w-2.5 rounded-full flex-shrink-0 ${STATE_DOT[engineState] || 'bg-slate-500'}`} />
        <span className={`text-sm font-semibold tracking-wide ${STATE_STYLES[engineState] || 'text-slate-400'}`}>
          {engineState}
        </span>
        {sseStatus === 'CONNECTED' ? (
          <span className="text-[10px] px-2 py-0.5 rounded font-mono font-semibold bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">
            SSE Active (Polling Paused)
          </span>
        ) : (
          <span className="text-[10px] px-2 py-0.5 rounded font-mono font-semibold bg-amber-500/10 text-amber-300 border border-amber-500/20">
            Fallback Polling (2s)
          </span>
        )}
        {status?.simulation_id && (
          <span className="ml-auto text-[10px] font-mono text-slate-500 truncate max-w-[180px]">
            {status.simulation_id}
          </span>
        )}
        <button
          onClick={refreshStatus}
          disabled={loading}
          className="ml-2 p-1 rounded text-slate-500 hover:text-slate-300 disabled:opacity-40 transition-colors"
          title="Refresh status"
        >
          <RefreshCw className="w-3.5 h-3.5" />
        </button>
      </div>

      {/* ── Configuration (only editable when IDLE) ──────────────────── */}
      <div className={`space-y-3 transition-opacity ${!canStart ? 'opacity-50 pointer-events-none' : ''}`}>
        {/* Mode Selector */}
        <div>
          <label className="block text-xs font-medium text-slate-400 mb-1.5">Simulation Mode</label>
          <div className="relative">
            <select
              value={mode}
              onChange={(e) => setMode(e.target.value)}
              disabled={!canStart}
              className="w-full appearance-none bg-slate-800 border border-slate-700 text-slate-200 text-xs rounded-lg px-3 py-2.5 pr-8 focus:outline-none focus:ring-2 focus:ring-sky-500/50 focus:border-sky-500/50 transition-colors"
            >
              {modeOptions.map((o) => (
                <option key={o.value} value={o.value}>{o.label}</option>
              ))}
            </select>
            <ChevronDown className="absolute right-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-slate-500 pointer-events-none" />
          </div>
        </div>

        {/* Interval Selector */}
        <div>
          <label className="block text-xs font-medium text-slate-400 mb-1.5">
            Event Interval: <span className="text-sky-400">{interval}s</span>
          </label>
          <input
            type="range"
            min={5}
            max={10}
            step={1}
            value={interval}
            onChange={(e) => setIntervalVal(Number(e.target.value))}
            disabled={!canStart}
            className="w-full accent-sky-500 h-1.5 rounded-full"
          />
          <div className="flex justify-between text-[10px] text-slate-500 mt-1">
            {INTERVAL_OPTIONS.map((v) => <span key={v}>{v}s</span>)}
          </div>
        </div>
      </div>

      {/* ── Action Buttons ────────────────────────────────────────────── */}
      <div className="grid grid-cols-2 gap-2 pt-1">
        {/* Start */}
        <button
          onClick={handleStart}
          disabled={!canStart}
          className="flex items-center justify-center gap-2 px-4 py-2.5 rounded-lg text-xs font-semibold
            bg-emerald-500/15 text-emerald-400 border border-emerald-500/25
            hover:bg-emerald-500/25 hover:border-emerald-500/40
            disabled:opacity-40 disabled:pointer-events-none
            transition-all"
        >
          {loading && canStart ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Play className="w-3.5 h-3.5" />}
          Start
        </button>

        {/* Pause */}
        <button
          onClick={handlePause}
          disabled={!canPause}
          className="flex items-center justify-center gap-2 px-4 py-2.5 rounded-lg text-xs font-semibold
            bg-amber-500/15 text-amber-400 border border-amber-500/25
            hover:bg-amber-500/25 hover:border-amber-500/40
            disabled:opacity-40 disabled:pointer-events-none
            transition-all"
        >
          <Pause className="w-3.5 h-3.5" />
          Pause
        </button>

        {/* Resume */}
        <button
          onClick={handleResume}
          disabled={!canResume}
          className="flex items-center justify-center gap-2 px-4 py-2.5 rounded-lg text-xs font-semibold
            bg-sky-500/15 text-sky-400 border border-sky-500/25
            hover:bg-sky-500/25 hover:border-sky-500/40
            disabled:opacity-40 disabled:pointer-events-none
            transition-all"
        >
          <Play className="w-3.5 h-3.5" />
          Resume
        </button>

        {/* Stop */}
        <button
          onClick={handleStop}
          disabled={!canStop}
          className={`flex items-center justify-center gap-2 px-4 py-2.5 rounded-lg text-xs font-semibold
            ${confirmStop
              ? 'bg-rose-500/30 text-rose-300 border border-rose-500/60 animate-pulse'
              : 'bg-rose-500/15 text-rose-400 border border-rose-500/25 hover:bg-rose-500/25 hover:border-rose-500/40'
            }
            disabled:opacity-40 disabled:pointer-events-none
            transition-all`}
        >
          <Square className="w-3.5 h-3.5" />
          {confirmStop ? 'Confirm Stop' : 'Stop'}
        </button>
      </div>

      {/* Cancel confirm */}
      {confirmStop && (
        <button
          onClick={() => setConfirmStop(false)}
          className="w-full text-xs text-slate-500 hover:text-slate-300 py-1 transition-colors"
        >
          Cancel
        </button>
      )}

      {/* ── Feedback Banner ───────────────────────────────────────────── */}
      {feedback && (
        <div className={`flex items-start gap-2 px-3 py-2.5 rounded-lg border text-xs font-medium
          ${feedback.type === 'success'
            ? 'bg-emerald-500/10 border-emerald-500/30 text-emerald-300'
            : 'bg-rose-500/10 border-rose-500/30 text-rose-300'
          }`}
        >
          {feedback.type === 'success'
            ? <CheckCircle2 className="w-3.5 h-3.5 flex-shrink-0 mt-0.5" />
            : <AlertTriangle className="w-3.5 h-3.5 flex-shrink-0 mt-0.5" />
          }
          <span>{feedback.msg}</span>
        </div>
      )}

      {/* Error message from engine */}
      {engineState === 'ERROR' && status?.error_message && (
        <div className="flex items-start gap-2 px-3 py-2.5 rounded-lg border border-rose-500/30 bg-rose-500/10 text-xs text-rose-300">
          <Zap className="w-3.5 h-3.5 flex-shrink-0 mt-0.5" />
          <span className="font-mono break-all">{status.error_message}</span>
        </div>
      )}
    </div>
  );
}
