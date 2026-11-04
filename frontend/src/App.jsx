import React, { useState, useEffect, useCallback, useRef } from 'react';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { getHealth } from './api/client';
import useObservabilityEvents from './hooks/useObservabilityEvents';
import Navbar from './components/Navbar';
import Sidebar from './components/Sidebar';
import Dashboard from './pages/Dashboard';
import Executions from './pages/Executions';
import Datasets from './pages/Datasets';
import Dependencies from './pages/Dependencies';
import Forecasts from './pages/Forecasts';
import Simulation from './pages/Simulation';

export default function App() {
  const [isHealthy, setIsHealthy] = useState(false);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [lastUpdated, setLastUpdated] = useState('');
  const [refreshing, setRefreshing] = useState(false);
  const [sidebarOpen, setSidebarOpen] = useState(false);

  // Phase D: Targeted Invalidation Triggers
  const [globalTrigger, setGlobalTrigger] = useState(0);
  const [inventoryTrigger, setInventoryTrigger] = useState(0);
  const [executionsTrigger, setExecutionsTrigger] = useState(0);
  const [datasetsTrigger, setDatasetsTrigger] = useState(0);
  const [dependenciesTrigger, setDependenciesTrigger] = useState(0);
  const [simulationTrigger, setSimulationTrigger] = useState(0);

  // Coalesce global refresh across all domains
  const triggerAll = useCallback(() => {
    setGlobalTrigger((c) => c + 1);
    setInventoryTrigger((c) => c + 1);
    setExecutionsTrigger((c) => c + 1);
    setDatasetsTrigger((c) => c + 1);
    setDependenciesTrigger((c) => c + 1);
    setSimulationTrigger((c) => c + 1);
    setLastUpdated(new Date().toLocaleTimeString());
  }, []);

  // Health check & trigger update
  const checkHealthAndTrigger = useCallback(async () => {
    try {
      setRefreshing(true);
      const res = await getHealth();
      setIsHealthy(res?.status === 'healthy' && res?.database === 'connected');
      triggerAll();
    } catch {
      setIsHealthy(false);
    } finally {
      setRefreshing(false);
    }
  }, [triggerAll]);

  // Debounced targeted event dispatcher to coalesce rapid SSE bursts
  const pendingTargetsRef = useRef(new Set());
  const debounceTimerRef = useRef(null);

  const dispatchTargetedEvents = useCallback(() => {
    if (debounceTimerRef.current) {
      clearTimeout(debounceTimerRef.current);
    }
    debounceTimerRef.current = setTimeout(() => {
      const targets = new Set(pendingTargetsRef.current);
      pendingTargetsRef.current.clear();

      if (targets.has('ALL') || targets.size === 0) {
        triggerAll();
        return;
      }

      setLastUpdated(new Date().toLocaleTimeString());
      if (targets.has('INVENTORY')) setInventoryTrigger((c) => c + 1);
      if (targets.has('EXECUTIONS')) setExecutionsTrigger((c) => c + 1);
      if (targets.has('DATASETS')) setDatasetsTrigger((c) => c + 1);
      if (targets.has('DEPENDENCIES')) setDependenciesTrigger((c) => c + 1);
      if (targets.has('SIMULATION')) setSimulationTrigger((c) => c + 1);
    }, 2000); // 2s debounce to prevent rapid re-rendering during stream bursts
  }, [triggerAll]);

  const handleIncomingEvent = useCallback((payload) => {
    if (!payload) return;
    const eventType = payload.event_type || '';
    const entity = (payload.entity || '').toUpperCase();

    if (
      eventType === 'INVENTORY_REORDER_STATE_CHANGED' ||
      eventType === 'DELIVERY_COMPLETED' ||
      entity === 'INVENTORY'
    ) {
      pendingTargetsRef.current.add('INVENTORY');
    } else if (
      eventType === 'OPERATIONAL_ORDER_INGESTED' ||
      entity === 'OPERATIONAL_ORDERS'
    ) {
      pendingTargetsRef.current.add('DATASETS');
      pendingTargetsRef.current.add('EXECUTIONS');
    } else if (
      eventType === 'OPERATIONAL_FEATURES_GENERATED' ||
      entity === 'FEATURE_STORE'
    ) {
      pendingTargetsRef.current.add('DATASETS');
      pendingTargetsRef.current.add('EXECUTIONS');
    } else if (
      eventType === 'OPERATIONAL_FORECAST_GENERATED' ||
      entity === 'FORECASTING'
    ) {
      pendingTargetsRef.current.add('DATASETS');
      pendingTargetsRef.current.add('EXECUTIONS');
      pendingTargetsRef.current.add('ALL');
    } else if (
      eventType === 'PIPELINE_STATUS_CHANGED' ||
      entity === 'PIPELINE'
    ) {
      pendingTargetsRef.current.add('EXECUTIONS');
    } else if (
      eventType === 'DATASET_READINESS_CHANGED' ||
      entity === 'DATASET'
    ) {
      pendingTargetsRef.current.add('DATASETS');
      pendingTargetsRef.current.add('SIMULATION');
    } else if (
      eventType === 'RULE_DECISION_EVALUATED' ||
      entity === 'RULE'
    ) {
      pendingTargetsRef.current.add('DEPENDENCIES');
      pendingTargetsRef.current.add('EXECUTIONS');
    } else if (
      eventType === 'SIMULATION_STATUS_CHANGED' ||
      entity === 'SIMULATION'
    ) {
      pendingTargetsRef.current.add('SIMULATION');
      pendingTargetsRef.current.add('DATASETS');
    } else if (
      eventType === 'REST_SNAPSHOT_REQUIRED'
    ) {
      pendingTargetsRef.current.add('ALL');
    } else {
      pendingTargetsRef.current.add('ALL');
    }

    dispatchTargetedEvents();
  }, [dispatchTargetedEvents]);

  useEffect(() => {
    return () => {
      if (debounceTimerRef.current) {
        clearTimeout(debounceTimerRef.current);
      }
    };
  }, []);

  // SSE Live State Synchronization hook
  const { connectionStatus: sseStatus, activeAlerts, latestEvent, events: sseEvents } = useObservabilityEvents({
    onEvent: handleIncomingEvent,
    onSnapshotRequired: () => {
      // Re-sync full REST snapshot
      checkHealthAndTrigger();
    }
  });

  // Initial check on mount
  useEffect(() => {
    checkHealthAndTrigger();
  }, [checkHealthAndTrigger]);

  // Periodic Auto-refresh interval (15 seconds fallback when not triggered by SSE)
  useEffect(() => {
    if (!autoRefresh) return;

    const intervalId = setInterval(() => {
      checkHealthAndTrigger();
    }, 15000);

    return () => clearInterval(intervalId);
  }, [autoRefresh, checkHealthAndTrigger]);

  const toggleAutoRefresh = () => {
    setAutoRefresh((prev) => !prev);
  };

  return (
    <BrowserRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
      <div className="min-h-screen bg-slate-950 text-slate-100 flex flex-col font-sans">
        {/* Global Top Navbar */}
        <Navbar
          isHealthy={isHealthy}
          autoRefresh={autoRefresh}
          toggleAutoRefresh={toggleAutoRefresh}
          lastUpdated={lastUpdated}
          onManualRefresh={checkHealthAndTrigger}
          refreshing={refreshing}
          toggleSidebar={() => setSidebarOpen((prev) => !prev)}
          sseStatus={sseStatus}
        />

        {/* Main Layout Area */}
        <div className="flex-1 flex overflow-hidden">
          {/* Sidebar Navigation */}
          <Sidebar
            isOpen={sidebarOpen}
            onClose={() => setSidebarOpen(false)}
          />

          {/* Main View Container */}
          <main className="flex-1 overflow-y-auto p-4 sm:p-6 lg:p-8 bg-slate-950/60">
            <div className="max-w-7xl mx-auto space-y-4">
              {/* Real-time Gateway Alert Banner */}
              {activeAlerts && activeAlerts.length > 0 && (
                <div className="space-y-2">
                  {activeAlerts.map((alert, idx) => (
                    <div
                      key={alert.event_id || idx}
                      className="p-4 rounded-xl bg-rose-500/15 border border-rose-500/40 text-rose-200 flex items-start justify-between shadow-lg shadow-rose-950/30 animate-pulse"
                    >
                      <div className="flex items-center gap-3">
                        <span className="flex h-3 w-3 relative">
                          <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-rose-400 opacity-75"></span>
                          <span className="relative inline-flex rounded-full h-3 w-3 bg-rose-500"></span>
                        </span>
                        <div>
                          <div className="text-xs font-bold uppercase tracking-wider text-rose-300">
                            {alert.entity} — {alert.severity} Incident
                          </div>
                          <div className="text-sm font-semibold text-white mt-0.5">
                            {alert.data?.message || 'Payment success rate degraded below operational threshold.'}
                          </div>
                          <div className="text-[11px] text-rose-300/80 font-mono mt-1">
                            Success Rate: {alert.data?.success_rate ? `${(alert.data.success_rate * 100).toFixed(1)}%` : 'Degraded'} | Event ID: {alert.event_id}
                          </div>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              )}

              <Routes>
                <Route
                  path="/"
                  element={
                    <Dashboard
                      refreshTrigger={globalTrigger}
                      inventoryTrigger={inventoryTrigger}
                      executionsTrigger={executionsTrigger}
                      sseStatus={sseStatus}
                      sseEvents={sseEvents}
                      latestEvent={latestEvent}
                    />
                  }
                />
                <Route
                  path="/inventory"
                  element={
                    <Dashboard
                      defaultTab="inventory"
                      refreshTrigger={inventoryTrigger}
                      inventoryTrigger={inventoryTrigger}
                      executionsTrigger={executionsTrigger}
                      sseStatus={sseStatus}
                      sseEvents={sseEvents}
                      latestEvent={latestEvent}
                    />
                  }
                />
                <Route path="/executions" element={<Executions refreshTrigger={executionsTrigger} />} />
                <Route path="/datasets" element={<Datasets refreshTrigger={datasetsTrigger} />} />
                <Route path="/dependencies" element={<Dependencies refreshTrigger={dependenciesTrigger} />} />
                <Route path="/forecasts" element={<Forecasts refreshTrigger={globalTrigger} />} />
                <Route
                  path="/simulation"
                  element={
                    <Simulation
                      refreshTrigger={simulationTrigger}
                      sseStatus={sseStatus}
                      sseEvents={sseEvents}
                      latestEvent={latestEvent}
                    />
                  }
                />
                <Route path="*" element={<Navigate to="/" replace />} />
              </Routes>
            </div>
          </main>
        </div>

      </div>
    </BrowserRouter>
  );
}
