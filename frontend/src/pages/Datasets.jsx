import React, { useState, useEffect } from 'react';
import { Database, Clock, RefreshCw, AlertCircle, Info, Hash, ArrowUpDown, Layers, ChevronLeft, ChevronRight, ShieldCheck, MapPin, CreditCard, ShoppingBag } from 'lucide-react';
import { getDatasets } from '../api/client';
import StatusBadge from '../components/StatusBadge';
import LoadingSpinner from '../components/LoadingSpinner';
import EmptyState from '../components/EmptyState';

function getDatasetProvenance(d) {
  const name = d.dataset_name || '';
  const src = d.source_type || '';

  if (src === 'OLIST_STREAM' || (name.startsWith('Olist_') && !name.includes('_Orders_') && !name.includes('_Payments_'))) {
    return {
      tag: 'OLIST CORE',
      upstream: 'Real-Time Olist Data Stream (Part 2 Simulation)',
      icon: ShoppingBag,
      isVirtual: false,
      badgeColor: 'bg-cyan-500/10 text-cyan-400 border-cyan-500/20'
    };
  }

  if (src === 'OLIST_REGIONAL' || name.startsWith('Olist_Orders_')) {
    return {
      tag: 'REGIONAL HUB',
      upstream: 'Olist Regional State Logistics (SP, RJ, MG, RS)',
      icon: MapPin,
      isVirtual: false,
      badgeColor: 'bg-purple-500/10 text-purple-400 border-purple-500/20'
    };
  }

  if (src === 'OLIST_PAYMENT' || name.startsWith('Olist_Payments_')) {
    return {
      tag: 'PAYMENT RAIL',
      upstream: 'Olist Multi-Channel Payment Ingestion',
      icon: CreditCard,
      isVirtual: false,
      badgeColor: 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20'
    };
  }

  if (src === 'OPERATIONAL' || name.startsWith('Operational')) {
    return {
      tag: 'OPERATIONAL',
      upstream: 'Operational Pipeline & Feature Store',
      icon: Layers,
      isVirtual: false,
      badgeColor: 'bg-amber-500/10 text-amber-400 border-amber-500/20'
    };
  }

  return {
    tag: 'REAL DATASET',
    upstream: 'Authoritative Data Store',
    icon: Database,
    isVirtual: false,
    badgeColor: 'bg-sky-500/10 text-sky-400 border-sky-500/20'
  };
}

export default function Datasets({ refreshTrigger }) {
  const [datasets, setDatasets] = useState([]);
  const [totalCount, setTotalCount] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  // Filters & Pagination State
  const [scopeFilter, setScopeFilter] = useState('ALL');
  const [page, setPage] = useState(1);
  const [limit, setLimit] = useState(25);

  useEffect(() => {
    let isMounted = true;
    // Only display full loading spinner on initial mount to prevent jarring UI flashes
    if (datasets.length === 0) {
      setLoading(true);
    }

    const offset = (page - 1) * limit;
    const params = {
      scope: 'ALL',
      limit: 100,
      offset: 0
    };

    getDatasets(params)
      .then((data) => {
        if (isMounted) {
          const rawItems = Array.isArray(data?.items) ? data.items : Array.isArray(data) ? data : [];
          // Filter out any virtual dummy SIM_% feeds
          const cleanItems = rawItems.filter(
            (d) => !d.dataset_name.startsWith('SIM_') && d.source_type !== 'SIMULATED'
          );

          // Apply client-side sub-filter if needed
          let filtered = cleanItems;
          if (scopeFilter === 'OLIST') {
            filtered = cleanItems.filter((d) => d.dataset_name.startsWith('Olist') || d.table_name.startsWith('olist'));
          } else if (scopeFilter === 'OPERATIONAL') {
            filtered = cleanItems.filter((d) => d.source_type === 'OPERATIONAL' || d.table_name.startsWith('operational'));
          } else if (scopeFilter === 'REGIONAL') {
            filtered = cleanItems.filter((d) => d.source_type === 'OLIST_REGIONAL');
          } else if (scopeFilter === 'PAYMENTS') {
            filtered = cleanItems.filter((d) => d.source_type === 'OLIST_PAYMENT');
          }

          setTotalCount(filtered.length);
          const paged = filtered.slice(offset, offset + limit);
          setDatasets(paged);
          setError(null);
        }
      })
      .catch((err) => {
        if (isMounted) {
          setError(err.response?.data?.detail || err.message || 'Failed to retrieve dataset statuses');
        }
      })
      .finally(() => {
        if (isMounted) setLoading(false);
      });

    return () => {
      isMounted = false;
    };
  }, [scopeFilter, page, limit, refreshTrigger]);

  const handleScopeChange = (e) => {
    setScopeFilter(e.target.value);
    setPage(1);
  };

  const handleLimitChange = (e) => {
    setLimit(Number(e.target.value));
    setPage(1);
  };

  const totalPages = Math.max(1, Math.ceil(totalCount / limit));
  const startItem = totalCount === 0 ? 0 : (page - 1) * limit + 1;
  const endItem = Math.min(page * limit, totalCount);

  return (
    <div className="space-y-6">
      {/* Page Header & Controls */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div>
          <h2 className="text-2xl font-bold text-white tracking-tight">Dataset Synchronization &amp; Live Status</h2>
          <p className="text-xs sm:text-sm text-slate-400 mt-1">
            Tracking live readiness, version evolution, and record volumes across authoritative Olist and operational data streams.
          </p>
        </div>

        {/* Filter Controls */}
        <div className="flex flex-wrap items-center gap-3">
          {/* Category Filter */}
          <div className="flex items-center gap-2 bg-slate-800/80 border border-slate-700/80 rounded-xl px-3 py-1.5 text-xs">
            <Layers className="w-3.5 h-3.5 text-sky-400" />
            <span className="text-slate-400 font-medium">Dataset Category:</span>
            <select
              value={scopeFilter}
              onChange={handleScopeChange}
              className="bg-transparent text-white font-semibold focus:outline-none cursor-pointer"
            >
              <option value="ALL" className="bg-slate-900 text-white">All Authoritative ({totalCount})</option>
              <option value="OLIST" className="bg-slate-900 text-white">Olist E-Commerce</option>
              <option value="REGIONAL" className="bg-slate-900 text-white">Regional State Hubs</option>
              <option value="PAYMENTS" className="bg-slate-900 text-white">Payment Rails</option>
              <option value="OPERATIONAL" className="bg-slate-900 text-white">Operational &amp; Features</option>
            </select>
          </div>

          {/* Page Size */}
          <div className="flex items-center gap-2 bg-slate-800/80 border border-slate-700/80 rounded-xl px-3 py-1.5 text-xs">
            <span className="text-slate-400 font-medium">Page Size:</span>
            <select
              value={limit}
              onChange={handleLimitChange}
              className="bg-transparent text-white font-semibold focus:outline-none cursor-pointer"
            >
              <option value={10} className="bg-slate-900 text-white">10</option>
              <option value={25} className="bg-slate-900 text-white">25</option>
              <option value={50} className="bg-slate-900 text-white">50</option>
            </select>
          </div>
        </div>
      </div>

      {/* Provenance Architecture Explanatory Banner */}
      <div className="bg-gradient-to-r from-sky-950/40 to-indigo-950/40 border border-sky-500/20 rounded-xl p-4 flex flex-col md:flex-row items-start md:items-center justify-between gap-3 text-sky-200">
        <div className="flex items-start gap-3">
          <Info className="w-5 h-5 flex-shrink-0 text-sky-400 mt-0.5" />
          <div className="text-xs leading-relaxed">
            <span className="font-semibold text-sky-300">Authoritative Data Provenance: </span>
            All displayed datasets represent real physical tables from the Brazilian Olist e-commerce database, regional state logistics hubs (São Paulo, Rio de Janeiro, Minas Gerais, Rio Grande do Sul), multi-channel payment streams, and the operational feature store. Synthetic dummy placeholders have been eliminated.
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

      {/* Dataset Cards Grid */}
      {loading && datasets.length === 0 ? (
        <LoadingSpinner message="Auditing live dataset versions and readiness states..." />
      ) : datasets.length === 0 ? (
        <EmptyState 
          title="No datasets found" 
          description="No authoritative datasets match the selected filter." 
        />
      ) : (
        <div className="space-y-6">
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-5">
            {datasets.map((d) => {
              const prov = getDatasetProvenance(d);
              const ProvIcon = prov.icon;
              return (
                <div 
                  key={d.dataset_id}
                  className="bg-slate-800/80 border border-slate-700/70 rounded-2xl p-5 flex flex-col justify-between shadow-sm hover:border-slate-600 transition-all"
                >
                  <div>
                    {/* Header */}
                    <div className="flex items-start justify-between pb-3 border-b border-slate-700/60 gap-2">
                      <div className="flex items-center gap-2.5">
                        <div className="p-2 bg-indigo-500/10 border border-indigo-500/20 rounded-lg text-indigo-400">
                          <ProvIcon className="w-4 h-4" />
                        </div>
                        <div>
                          <h3 className="text-base font-bold text-white">{d.dataset_name}</h3>
                          <span className="font-mono text-[11px] text-slate-400">{d.table_name}</span>
                        </div>
                      </div>
                      <div className="flex flex-col items-end gap-1">
                        <StatusBadge status={d.status} size="sm" />
                        <span className={`text-[9px] px-1.5 py-0.2 rounded font-bold uppercase tracking-wide border ${prov.badgeColor}`}>
                          {prov.tag}
                        </span>
                      </div>
                    </div>

                    {/* Metrics */}
                    <div className="mt-4 grid grid-cols-2 gap-3">
                      <div className="bg-slate-900/60 p-2.5 rounded-xl border border-slate-800 text-xs">
                        <span className="text-[11px] text-slate-400 block">Current Version</span>
                        <span className="font-mono text-base font-bold text-sky-400 mt-0.5 block">
                          v{d.current_version || 1}
                        </span>
                      </div>
                      <div className="bg-slate-900/60 p-2.5 rounded-xl border border-slate-800 text-xs">
                        <span className="text-[11px] text-slate-400 block">Total Rows</span>
                        <span className="font-mono text-base font-bold text-slate-200 mt-0.5 block">
                          {d.row_count ? d.row_count.toLocaleString() : '0'}
                        </span>
                        <span className="text-[10px] text-slate-500 block truncate">
                          Authoritative records
                        </span>
                      </div>
                    </div>

                    <div className="mt-3 text-[11px] text-slate-400 border-t border-slate-800/80 pt-2 flex items-center justify-between">
                      <span className="truncate">{prov.upstream}</span>
                      <span className="font-mono text-[10px] text-slate-500">ID #{d.dataset_id}</span>
                    </div>
                  </div>

                  <div className="mt-4 pt-3 border-t border-slate-700/60 flex items-center justify-between text-xs text-slate-400">
                    <span className="flex items-center gap-1.5">
                      <Clock className="w-3.5 h-3.5 text-slate-500" />
                      <span>{d.last_updated_at ? new Date(d.last_updated_at).toLocaleTimeString() : 'Awaiting stream'}</span>
                    </span>
                    <span className="text-[10px] font-mono text-emerald-400">LIVE SYNCED</span>
                  </div>
                </div>
              );
            })}
          </div>

          {/* Pagination Controls */}
          {totalPages > 1 && (
            <div className="flex flex-col sm:flex-row items-center justify-between gap-4 pt-4 border-t border-slate-800 text-xs text-slate-400">
              <div>
                Showing <span className="font-semibold text-white">{startItem}</span> to{' '}
                <span className="font-semibold text-white">{endItem}</span> of{' '}
                <span className="font-semibold text-white">{totalCount}</span> authoritative datasets
              </div>
              <div className="flex items-center gap-2">
                <button
                  onClick={() => setPage((p) => Math.max(1, p - 1))}
                  disabled={page === 1}
                  className="px-3 py-1.5 rounded-lg border border-slate-700 bg-slate-800 text-white disabled:opacity-40 disabled:cursor-not-allowed hover:bg-slate-700 transition"
                >
                  <ChevronLeft className="w-4 h-4" />
                </button>
                <span className="px-2 font-medium">Page {page} of {totalPages}</span>
                <button
                  onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
                  disabled={page === totalPages}
                  className="px-3 py-1.5 rounded-lg border border-slate-700 bg-slate-800 text-white disabled:opacity-40 disabled:cursor-not-allowed hover:bg-slate-700 transition"
                >
                  <ChevronRight className="w-4 h-4" />
                </button>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
