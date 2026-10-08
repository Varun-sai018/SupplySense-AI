import React, { useState, useEffect, useCallback } from 'react';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { getHealth } from './api/client';
import Navbar from './components/Navbar';
import Sidebar from './components/Sidebar';
import Dashboard from './pages/Dashboard';
import Executions from './pages/Executions';
import Datasets from './pages/Datasets';
import Dependencies from './pages/Dependencies';
import Forecasts from './pages/Forecasts';

export default function App() {
  const [isHealthy, setIsHealthy] = useState(false);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [refreshTrigger, setRefreshTrigger] = useState(0);
  const [lastUpdated, setLastUpdated] = useState('');
  const [refreshing, setRefreshing] = useState(false);
  const [sidebarOpen, setSidebarOpen] = useState(false);

  // Health check & trigger update
  const checkHealthAndTrigger = useCallback(async () => {
    try {
      setRefreshing(true);
      const res = await getHealth();
      setIsHealthy(res?.status === 'healthy' && res?.database === 'connected');
      const now = new Date();
      setLastUpdated(now.toLocaleTimeString());
      setRefreshTrigger((prev) => prev + 1);
    } catch {
      setIsHealthy(false);
    } finally {
      setRefreshing(false);
    }
  }, []);

  // Initial check on mount
  useEffect(() => {
    checkHealthAndTrigger();
  }, [checkHealthAndTrigger]);

  // Periodic Auto-refresh interval (5 seconds)
  useEffect(() => {
    if (!autoRefresh) return;

    const intervalId = setInterval(() => {
      checkHealthAndTrigger();
    }, 5000);

    return () => clearInterval(intervalId);
  }, [autoRefresh, checkHealthAndTrigger]);

  const toggleAutoRefresh = () => {
    setAutoRefresh((prev) => !prev);
  };

  return (
    <BrowserRouter>
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
            <div className="max-w-7xl mx-auto">
              <Routes>
                <Route path="/" element={<Dashboard refreshTrigger={refreshTrigger} />} />
                <Route path="/executions" element={<Executions refreshTrigger={refreshTrigger} />} />
                <Route path="/datasets" element={<Datasets refreshTrigger={refreshTrigger} />} />
                <Route path="/dependencies" element={<Dependencies refreshTrigger={refreshTrigger} />} />
                <Route path="/forecasts" element={<Forecasts refreshTrigger={refreshTrigger} />} />
                <Route path="*" element={<Navigate to="/" replace />} />
              </Routes>
            </div>
          </main>
        </div>
      </div>
    </BrowserRouter>
  );
}
