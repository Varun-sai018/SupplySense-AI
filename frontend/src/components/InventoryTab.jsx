import React, { useState, useEffect, useCallback, useMemo } from 'react';
import {
  Package,
  AlertTriangle,
  CheckCircle2,
  Clock,
  Truck,
  RefreshCw,
  Search,
  Filter,
  Radio,
  Building2,
  ChevronDown,
  ChevronUp,
  ChevronLeft,
  ChevronRight,
  Layers,
  ArrowRight,
  ShieldCheck,
  ShieldAlert,
  Info,
  AlertCircle,
  X
} from 'lucide-react';
import {
  getOperationalInventory,
  getOperationalSuppliers,
  getReplenishments,
  getOperationalSummary,
  getRecentEvents,
  getSimInventory,
  getSimReplenishments,
} from '../api/client';
import { Sparkles } from 'lucide-react';
import MetricCard from './MetricCard';
import StatusBadge from './StatusBadge';
import LoadingSpinner from './LoadingSpinner';
import EmptyState from './EmptyState';
import AlertsPanel from './AlertsPanel';

export default function InventoryTab({ refreshTrigger, sseStatus, sseEvents, latestEvent }) {
  // Scope State: 'OPERATIONAL' | 'SIMULATED'
  const [inventoryScope, setInventoryScope] = useState('OPERATIONAL');
  const [simInventory, setSimInventory] = useState([]);
  const [simReplenishments, setSimReplenishments] = useState([]);
  const [simLoading, setSimLoading] = useState(false);

  // 1. Data States
  const [summary, setSummary] = useState(null);
  const [inventory, setInventory] = useState([]);
  const [invTotal, setInvTotal] = useState(0);
  const [lowStockCount, setLowStockCount] = useState(0);
  const [suppliers, setSuppliers] = useState([]);
  const [replenishments, setReplenishments] = useState([]);
  const [eventsList, setEventsList] = useState([]);

  // 2. Loading & Error States
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState(null);

  // 3. Filter & Pagination States
  const [searchTerm, setSearchTerm] = useState('');
  const [warehouseFilter, setWarehouseFilter] = useState('ALL');
  const [lowStockOnly, setLowStockOnly] = useState(false);
  const [invPage, setInvPage] = useState(1);
  const [invPageSize, setInvPageSize] = useState(20);
  const [invLoading, setInvLoading] = useState(false);
  const [availableWarehouses, setAvailableWarehouses] = useState(['ALL', 'WH-SP-01']);
  const [replenishmentStatusFilter, setReplenishmentStatusFilter] = useState('ALL');
  const [expandedSupplier, setExpandedSupplier] = useState(null);

  // Operational Replenishment State (Simulated delivery removed for data isolation)


  // Fetch simulated inventory and replenishments when scope is SIMULATED
  useEffect(() => {
    if (inventoryScope !== 'SIMULATED') return;
    setSimLoading(true);
    Promise.all([
      getSimInventory({ limit: 50 }).catch(() => []),
      getSimReplenishments({ limit: 50 }).catch(() => []),
    ])
      .then(([invData, repData]) => {
        setSimInventory(invData || []);
        setSimReplenishments(repData || []);
      })
      .finally(() => setSimLoading(false));
  }, [inventoryScope, refreshTrigger]);

  // Fetch paginated inventory from backend
  const loadInventory = useCallback(async () => {
    try {
      setInvLoading(true);
      const params = {
        limit: invPageSize,
        offset: (invPage - 1) * invPageSize,
        low_stock_only: lowStockOnly
      };
      if (warehouseFilter !== 'ALL') {
        params.warehouse_id = warehouseFilter;
      }
      if (searchTerm.trim()) {
        params.search = searchTerm.trim();
      }

      const invRes = await getOperationalInventory(params);
      if (invRes) {
        setInventory(invRes.items || []);
        setInvTotal(invRes.total_count ?? (invRes.items || []).length);
        setLowStockCount(invRes.low_stock_count ?? 0);
        if (invRes.warehouses?.length) {
          setAvailableWarehouses(['ALL', ...invRes.warehouses]);
        } else if (invRes.items?.length) {
          setAvailableWarehouses(prev => {
            const set = new Set(prev);
            invRes.items.forEach(i => {
              if (i.warehouse_id) set.add(i.warehouse_id);
            });
            return Array.from(set);
          });
        }
      }
    } catch (err) {
      // Keep existing inventory on network failure
    } finally {
      setInvLoading(false);
    }
  }, [invPage, invPageSize, warehouseFilter, lowStockOnly, searchTerm]);

  // Load inventory when pagination or filter changes
  useEffect(() => {
    loadInventory();
  }, [loadInventory, refreshTrigger]);

  // Fetch all other operational data from REST APIs
  const loadData = useCallback(async (isManualRefresh = false) => {
    try {
      if (isManualRefresh) setRefreshing(true);
      else setLoading(true);
      setError(null);

      const [sumRes, supRes, repRes, evtRes] = await Promise.all([
        getOperationalSummary().catch(() => null),
        getOperationalSuppliers({ limit: 50 }).catch(() => ({ items: [], total_count: 0 })),
        getReplenishments({ limit: 50 }).catch(() => ({ items: [], total_count: 0 })),
        getRecentEvents({ limit: 30 }).catch(() => [])
      ]);

      if (sumRes) setSummary(sumRes);
      if (supRes) {
        setSuppliers(supRes.items || []);
      }
      if (repRes) {
        setReplenishments(repRes.items || []);
      }
      if (Array.isArray(evtRes)) {
        // Filter to operational inventory, delivery, and system notification events
        const opEvents = evtRes.filter(e => 
          ['INVENTORY_REORDER_STATE_CHANGED', 'DELIVERY_COMPLETED', 'SYSTEM_NOTIFICATION'].includes(e.event_type) ||
          e.entity === 'INVENTORY'
        );
        setEventsList(opEvents);
      }
    } catch (err) {
      setError(err?.message || 'Failed to load operational telemetry from server.');
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, []);

  // Initial load and parent refreshTrigger listener
  useEffect(() => {
    loadData();
  }, [loadData, refreshTrigger]);

  // Merge real-time SSE events dynamically without duplication
  useEffect(() => {
    if (!latestEvent) return;
    const isOperational = ['INVENTORY_REORDER_STATE_CHANGED', 'DELIVERY_COMPLETED'].includes(latestEvent.event_type) || latestEvent.entity === 'INVENTORY';
    const isDiagnostic = latestEvent.event_type === 'SYSTEM_NOTIFICATION';

    if (isOperational || isDiagnostic) {
      setEventsList(prev => {
        const id = latestEvent.event_id;
        if (id && prev.some(e => e.event_id === id)) {
          return prev;
        }
        return [latestEvent, ...prev].slice(0, 50);
      });

      // Automatically refresh inventory and replenishment state when order state changes or delivery completes
      if (isOperational) {
        loadData(true);
        loadInventory();
      }
    }
  }, [latestEvent, loadData, loadInventory]);

  // Filtered replenishment rows
  const filteredReplenishments = useMemo(() => {
    if (replenishmentStatusFilter === 'ALL') return replenishments;
    return replenishments.filter(r => r.status === replenishmentStatusFilter);
  }, [replenishments, replenishmentStatusFilter]);

  if (loading && !inventory.length) {
    return (
      <div className="py-20 flex flex-col items-center justify-center">
        <LoadingSpinner size="lg" />
        <p className="text-xs text-slate-400 mt-3 font-medium">Querying authoritative operational inventory tables...</p>
      </div>
    );
  }

  if (error && !inventory.length) {
    return (
      <div className="p-6 rounded-2xl bg-rose-500/10 border border-rose-500/20 text-rose-300 flex flex-col items-start gap-4">
        <div className="flex items-center gap-3">
          <AlertTriangle className="w-5 h-5 text-rose-400" />
          <h3 className="text-sm font-bold uppercase tracking-wider text-white">Operational Telemetry Error</h3>
        </div>
        <p className="text-xs text-rose-200/80">{error}</p>
        <button
          onClick={() => loadData(true)}
          className="px-4 py-2 rounded-xl bg-rose-500/20 hover:bg-rose-500/30 text-rose-100 border border-rose-500/30 text-xs font-semibold flex items-center gap-2 cursor-pointer transition-colors"
        >
          <RefreshCw className="w-3.5 h-3.5" /> Retry Connection
        </button>
      </div>
    );
  }

  const invSummary = summary?.inventory || {};
  const repSummary = summary?.replenishments || {};
  const supSummary = summary?.suppliers || {};

  return (
    <div className="space-y-6">
      {/* Scope Switcher: Operational vs Simulated */}
      <div className="flex flex-wrap items-center justify-between gap-3 p-3.5 rounded-2xl bg-slate-800/60 border border-slate-700/60">
        <div className="flex items-center gap-1.5 p-1 bg-slate-900/80 rounded-xl border border-slate-700/50">
          <button
            type="button"
            onClick={() => setInventoryScope('OPERATIONAL')}
            className={`px-3.5 py-1.5 rounded-lg text-xs font-semibold transition-all cursor-pointer ${inventoryScope === 'OPERATIONAL' ? 'bg-sky-500 text-white shadow-md shadow-sky-500/20' : 'text-slate-400 hover:text-white'}`}
          >
            Operational Inventory (Authoritative)
          </button>
          <button
            type="button"
            onClick={() => setInventoryScope('SIMULATED')}
            className={`flex items-center gap-1.5 px-3.5 py-1.5 rounded-lg text-xs font-semibold transition-all cursor-pointer ${inventoryScope === 'SIMULATED' ? 'bg-amber-500 text-slate-900 shadow-md shadow-amber-500/20 font-bold' : 'text-slate-400 hover:text-amber-300'}`}
          >
            <Sparkles className="w-3.5 h-3.5" />
            Simulated Inventory (E2E Run)
          </button>
        </div>
        <div className="text-[11px] font-mono text-slate-400">
          {inventoryScope === 'OPERATIONAL'
            ? 'Authoritative Ledger: operational_inventory (106 items)'
            : 'Simulated Snapshots: sim_inventory_snapshots (Warehouse SIM-WH-01)'}
        </div>
      </div>

      {inventoryScope === 'SIMULATED' ? (
        /* SIMULATED VIEW */
        <div className="space-y-6">
          {/* Simulated Notice */}
          <div className="flex items-start gap-3 p-4 rounded-xl bg-amber-500/10 border border-amber-500/30 text-amber-200 text-xs">
            <Sparkles className="w-4 h-4 text-amber-400 flex-shrink-0 mt-0.5" />
            <div>
              <span className="font-bold text-amber-300 uppercase tracking-wide mr-1.5">Simulated Inventory & Replenishment:</span>
              This view reflects the state generated by your latest End-to-End simulation run. Low-stock conditions were detected by the Dependency Engine, which triggered the <strong>SIM-REORDER-Pipeline</strong> to automatically generate simulated supplier replenishment orders. Real operational inventory is untouched.
            </div>
          </div>

          {/* Simulated Inventory Table */}
          <div className="bg-slate-800/80 border border-slate-700/70 rounded-2xl p-5 shadow-sm space-y-4">
            <div className="flex items-center justify-between pb-3 border-b border-slate-700/60">
              <div>
                <h3 className="text-sm font-bold text-white uppercase tracking-wider flex items-center gap-2">
                  <Package className="w-4 h-4 text-amber-400" />
                  Simulated Stock Snapshots
                  <span className="text-[10px] px-2 py-0.5 rounded font-bold uppercase bg-amber-500/15 text-amber-400 border border-amber-500/30">
                    SIM-WH-01
                  </span>
                </h3>
                <p className="text-xs text-slate-400 mt-0.5">
                  Tracked items with real-time below-threshold indicators.
                </p>
              </div>
              <span className="text-xs font-mono text-slate-400">{simInventory.length} Snapshots</span>
            </div>

            {simLoading ? (
              <LoadingSpinner message="Loading simulated inventory..." />
            ) : simInventory.length === 0 ? (
              <EmptyState
                title="No simulated inventory found"
                description="Run an End-to-End simulation in the Simulation Lab to generate simulated stock snapshots."
              />
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-left text-xs">
                  <thead>
                    <tr className="border-b border-slate-700/50 text-slate-400 font-semibold">
                      <th className="pb-2.5 px-3">SKU</th>
                      <th className="pb-2.5 px-3">Warehouse</th>
                      <th className="pb-2.5 px-3 text-right">On Hand</th>
                      <th className="pb-2.5 px-3 text-right">Reorder Point</th>
                      <th className="pb-2.5 px-3 text-right">Target Stock</th>
                      <th className="pb-2.5 px-3 text-center">Status</th>
                    </tr>
                  </thead>
                  <tbody>
                    {simInventory.map((item) => (
                      <tr
                        key={item.snapshot_id}
                        className={`border-b border-slate-800 hover:bg-slate-800/50 font-mono transition-colors ${item.is_below_threshold ? 'bg-rose-950/20' : ''}`}
                      >
                        <td className="py-2.5 px-3 text-slate-200 font-medium">{item.sku}</td>
                        <td className="py-2.5 px-3 text-slate-400">{item.warehouse_id}</td>
                        <td className={`py-2.5 px-3 text-right font-bold ${item.is_below_threshold ? 'text-rose-400' : 'text-emerald-400'}`}>

                          {item.on_hand_quantity}
                        </td>
                        <td className="py-2.5 px-3 text-right text-slate-400">{item.reorder_threshold}</td>
                        <td className="py-2.5 px-3 text-right text-slate-300">{item.reorder_target_qty}</td>
                        <td className="py-2.5 px-3 text-center">
                          {item.is_below_threshold ? (
                            <span className="px-2 py-0.5 rounded text-[10px] font-bold bg-rose-500/15 text-rose-400 border border-rose-500/30">
                              LOW STOCK
                            </span>
                          ) : (
                            <span className="px-2 py-0.5 rounded text-[10px] font-bold bg-emerald-500/15 text-emerald-400 border border-emerald-500/30">
                              OPTIMAL
                            </span>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>

          {/* Simulated Replenishment Orders Table */}
          <div className="bg-slate-800/80 border border-slate-700/70 rounded-2xl p-5 shadow-sm space-y-4">
            <div className="flex items-center justify-between pb-3 border-b border-slate-700/60">
              <div>
                <h3 className="text-sm font-bold text-white uppercase tracking-wider flex items-center gap-2">
                  <Truck className="w-4 h-4 text-emerald-400" />
                  Simulated Replenishment Orders (Triggered by SIM-REORDER-Pipeline)
                </h3>
                <p className="text-xs text-slate-400 mt-0.5">
                  Orders automatically dispatched to online suppliers during simulation runs.
                </p>
              </div>
              <span className="text-xs font-mono text-slate-400">{simReplenishments.length} Orders</span>
            </div>

            {simReplenishments.length === 0 ? (
              <EmptyState
                title="No simulated replenishment orders"
                description="Replenishment orders will be created automatically when low-stock SKUs are detected in an E2E simulation run."
              />
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-left text-xs">
                  <thead>
                    <tr className="border-b border-slate-700/50 text-slate-400 font-semibold">
                      <th className="pb-2.5 px-3">Order ID</th>
                      <th className="pb-2.5 px-3">SKU</th>
                      <th className="pb-2.5 px-3">Supplier</th>
                      <th className="pb-2.5 px-3 text-right">Quantity</th>
                      <th className="pb-2.5 px-3 text-center">Status</th>
                      <th className="pb-2.5 px-3 text-right">Dispatched</th>
                    </tr>
                  </thead>
                  <tbody>
                    {simReplenishments.map((order) => (
                      <tr key={order.sim_replenishment_id} className="border-b border-slate-800 hover:bg-slate-800/50 font-mono">
                        <td className="py-2.5 px-3 text-sky-400 font-medium">{order.sim_replenishment_id}</td>
                        <td className="py-2.5 px-3 text-slate-200">{order.sku}</td>
                        <td className="py-2.5 px-3 text-indigo-300">{order.supplier_id}</td>
                        <td className="py-2.5 px-3 text-right font-bold text-emerald-400">{order.quantity}</td>
                        <td className="py-2.5 px-3 text-center">
                          <span className="px-2 py-0.5 rounded text-[10px] font-bold bg-emerald-500/15 text-emerald-400 border border-emerald-500/30">
                            {order.status}
                          </span>
                        </td>
                        <td className="py-2.5 px-3 text-right text-slate-500">
                          {order.dispatched_at ? new Date(order.dispatched_at).toLocaleTimeString() : '—'}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>
      ) : (
        /* OPERATIONAL VIEW */
        <>
      {/* --------------------------------------------------------------------- */}
      {/* 1. Operational Summary KPI Cards                                      */}
      {/* --------------------------------------------------------------------- */}
      <div>
        <div className="flex items-center justify-between pb-3">
          <div className="flex items-center gap-2">
            <Package className="w-4 h-4 text-sky-400" />
            <h3 className="text-sm font-bold text-white uppercase tracking-wider">
              Operational Inventory & Replenishment Summary
            </h3>
          </div>
          <div className="flex items-center gap-2">
            <span className="text-[10px] px-2 py-0.5 rounded font-bold uppercase tracking-wide border bg-emerald-500/10 text-emerald-400 border-emerald-500/20">
              PERSISTED STATE
            </span>
            <button
              onClick={() => loadData(true)}
              disabled={refreshing}
              className="p-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300 border border-slate-700 text-xs flex items-center gap-1 cursor-pointer transition-colors"
              title="Refresh telemetry"
            >
              <RefreshCw className={`w-3.5 h-3.5 ${refreshing ? 'animate-spin' : ''}`} />
            </button>
          </div>
        </div>

        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-3.5">
          <MetricCard
            title="Tracked SKUs"
            value={invSummary.total_skus ?? invTotal}
            subtitle={`${invSummary.total_on_hand ?? 0} physical units on hand`}
            icon={Package}
            color="sky"
          />
          <MetricCard
            title="Low-Stock SKUs"
            value={invSummary.low_stock_skus ?? lowStockCount}
            subtitle={lowStockCount > 0 ? "Requires Rule B replenishment" : "All SKUs above safe threshold"}
            icon={AlertTriangle}
            color={lowStockCount > 0 ? "rose" : "emerald"}
          />
          <MetricCard
            title="In-Flight Orders"
            value={repSummary.active_replenishments ?? 0}
            subtitle="Dispatched, Confirmed, or In-Transit"
            icon={Truck}
            color="amber"
          />
          <MetricCard
            title="Delivered Orders"
            value={repSummary.delivered_replenishments ?? 0}
            subtitle="Restocked & inventoried"
            icon={CheckCircle2}
            color="emerald"
          />
          <MetricCard
            title="Suppliers Online"
            value={`${supSummary.online_suppliers ?? 0} / ${supSummary.total_suppliers ?? 0}`}
            subtitle={supSummary.offline_suppliers > 0 ? `${supSummary.offline_suppliers} offline supplier(s)` : "All suppliers operational"}
            icon={Building2}
            color={supSummary.offline_suppliers > 0 ? "rose" : "indigo"}
          />
        </div>
      </div>

      {/* --------------------------------------------------------------------- */}
      {/* 2. Live Operational Inventory Table                                    */}
      {/* --------------------------------------------------------------------- */}
      <div className="bg-slate-800/80 border border-slate-700/70 rounded-2xl p-5 shadow-sm space-y-4">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 pb-3 border-b border-slate-700/60">
          <div>
            <div className="flex items-center gap-2">
              <Package className="w-4 h-4 text-sky-400" />
              <h3 className="text-sm font-bold text-white uppercase tracking-wider">
                Authoritative Inventory Ledger
              </h3>
            </div>
            <p className="text-xs text-slate-400 mt-0.5">
              Source: <span className="font-mono text-slate-300">operational_inventory</span>. Real-time availability computed as <code className="text-sky-300">on_hand - reserved</code>.
            </p>
          </div>

          {/* Table Filters */}
          <div className="flex flex-wrap items-center gap-2 text-xs">
            {/* Search Input */}
            <div className="relative">
              <Search className="w-3.5 h-3.5 text-slate-400 absolute left-2.5 top-1/2 -translate-y-1/2" />
              <input
                type="text"
                placeholder="Search SKU..."
                value={searchTerm}
                onChange={e => {
                  setSearchTerm(e.target.value);
                  setInvPage(1);
                }}
                className="pl-8 pr-3 py-1.5 rounded-lg bg-slate-900 border border-slate-700 text-slate-200 placeholder-slate-500 text-xs focus:outline-none focus:border-sky-500"
              />
            </div>

            {/* Warehouse Filter */}
            <select
              value={warehouseFilter}
              onChange={e => {
                setWarehouseFilter(e.target.value);
                setInvPage(1);
              }}
              className="py-1.5 px-2.5 rounded-lg bg-slate-900 border border-slate-700 text-slate-200 text-xs focus:outline-none focus:border-sky-500"
            >
              {availableWarehouses.map(w => (
                <option key={w} value={w}>
                  {w === 'ALL' ? 'All Warehouses' : w}
                </option>
              ))}
            </select>

            {/* Low-Stock Toggle */}
            <label className="flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg bg-slate-900 border border-slate-700 text-slate-300 cursor-pointer select-none">
              <input
                type="checkbox"
                checked={lowStockOnly}
                onChange={e => {
                  setLowStockOnly(e.target.checked);
                  setInvPage(1);
                }}
                className="rounded border-slate-700 text-sky-500 focus:ring-0 cursor-pointer"
              />
              <span>Low-Stock Only</span>
            </label>
          </div>
        </div>

        {/* Table View */}
        {inventory.length === 0 ? (
          <EmptyState
            title="No inventory records matched"
            description="Try adjusting your warehouse filter or search term."
            icon={Package}
          />
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-left text-xs">
              <thead>
                <tr className="border-b border-slate-700/60 text-slate-400 font-semibold uppercase tracking-wider">
                  <th className="py-2.5 px-3">SKU Identifier</th>
                  <th className="py-2.5 px-3">Warehouse</th>
                  <th className="py-2.5 px-3 text-right">On-Hand</th>
                  <th className="py-2.5 px-3 text-right">Reserved</th>
                  <th className="py-2.5 px-3 text-right">Available</th>
                  <th className="py-2.5 px-3 text-right">Threshold</th>
                  <th className="py-2.5 px-3 text-center">Status</th>
                  <th className="py-2.5 px-3 text-center">Version</th>
                  <th className="py-2.5 px-3">Last Updated</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-700/30">
                {inventory.map(item => (
                  <tr key={`${item.sku}-${item.warehouse_id}`} className="hover:bg-slate-700/20 transition-colors">
                    <td className="py-2.5 px-3 font-mono font-medium text-slate-200">{item.sku}</td>
                    <td className="py-2.5 px-3 text-slate-300 font-medium">{item.warehouse_id}</td>
                    <td className="py-2.5 px-3 text-right font-mono text-slate-300">{item.on_hand_quantity}</td>
                    <td className="py-2.5 px-3 text-right font-mono text-slate-400">{item.reserved_quantity}</td>
                    <td className="py-2.5 px-3 text-right font-mono font-bold text-white">
                      <span className={item.is_low_stock ? 'text-rose-400' : 'text-emerald-400'}>
                        {item.available_quantity}
                      </span>
                    </td>
                    <td className="py-2.5 px-3 text-right font-mono text-slate-400">{item.reorder_threshold}</td>
                    <td className="py-2.5 px-3 text-center">
                      {item.is_low_stock ? (
                        <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[11px] font-semibold bg-rose-500/10 text-rose-400 border border-rose-500/20">
                          <span className="w-1.5 h-1.5 rounded-full bg-rose-400 animate-pulse" />
                          LOW STOCK
                        </span>
                      ) : (
                        <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[11px] font-semibold bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">
                          <span className="w-1.5 h-1.5 rounded-full bg-emerald-400" />
                          OPTIMAL
                        </span>
                      )}
                    </td>
                    <td className="py-2.5 px-3 text-center font-mono text-slate-400 text-[11px]">
                      v{item.version}
                    </td>
                    <td className="py-2.5 px-3 text-slate-400 text-[11px] whitespace-nowrap">
                      {item.last_updated_at ? new Date(item.last_updated_at).toLocaleString() : '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {/* Inventory Pagination Toolbar */}
        <div className="pt-3 border-t border-slate-700/60 text-xs text-slate-400 flex flex-col sm:flex-row items-center justify-between gap-3 font-sans">
          <span>
            Showing <strong className="text-slate-200 font-mono">{invTotal === 0 ? 0 : (invPage - 1) * invPageSize + 1}</strong> to{' '}
            <strong className="text-slate-200 font-mono">{Math.min(invPage * invPageSize, invTotal)}</strong> of{' '}
            <strong className="text-slate-200 font-mono">{invTotal}</strong> SKU records
          </span>

          <div className="flex items-center gap-3">
            <div className="flex items-center gap-1.5">
              <span className="text-slate-400 text-[11px]">Rows:</span>
              <select
                value={invPageSize}
                onChange={(e) => {
                  setInvPageSize(Number(e.target.value));
                  setInvPage(1);
                }}
                className="py-1 px-2 rounded-lg bg-slate-900 border border-slate-700 text-slate-200 text-xs focus:outline-none"
              >
                <option value={10}>10</option>
                <option value={20}>20</option>
                <option value={50}>50</option>
              </select>
            </div>

            <div className="flex items-center gap-2">
              <button
                onClick={() => setInvPage((p) => Math.max(1, p - 1))}
                disabled={invPage <= 1 || invLoading}
                className="px-2.5 py-1 rounded-lg border border-slate-700 bg-slate-900 text-slate-300 hover:bg-slate-700 disabled:opacity-40 disabled:cursor-not-allowed flex items-center gap-1 transition-colors"
              >
                <ChevronLeft className="w-3.5 h-3.5" />
                <span>Previous</span>
              </button>

              <span className="px-2 text-slate-400 text-xs font-mono">
                Page <span className="text-white font-bold">{invPage}</span> of {Math.max(1, Math.ceil(invTotal / invPageSize))}
              </span>

              <button
                onClick={() => setInvPage((p) => Math.min(Math.max(1, Math.ceil(invTotal / invPageSize)), p + 1))}
                disabled={invPage >= Math.max(1, Math.ceil(invTotal / invPageSize)) || invLoading}
                className="px-2.5 py-1 rounded-lg border border-slate-700 bg-slate-900 text-slate-300 hover:bg-slate-700 disabled:opacity-40 disabled:cursor-not-allowed flex items-center gap-1 transition-colors"
              >
                <span>Next</span>
                <ChevronRight className="w-3.5 h-3.5" />
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* --------------------------------------------------------------------- */}
      {/* 3. Supplier Connectivity & SKU Availability Matrix                    */}
      {/* --------------------------------------------------------------------- */}
      <div className="bg-slate-800/80 border border-slate-700/70 rounded-2xl p-5 shadow-sm space-y-4">
        <div className="pb-3 border-b border-slate-700/60">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <Building2 className="w-4 h-4 text-indigo-400" />
              <h3 className="text-sm font-bold text-white uppercase tracking-wider">
                Supplier Health & Catalog Availability
              </h3>
            </div>
            <span className="text-[10px] text-slate-400 font-medium">
              Source: <code className="text-slate-300">operational_suppliers</code> &amp; <code className="text-slate-300">supplier_catalog_availability</code>
            </span>
          </div>
          <p className="text-xs text-slate-400 mt-1">
            Authoritative supplier connectivity status is strictly distinguished from SKU-level availability constraints.
          </p>
        </div>

        {suppliers.length === 0 ? (
          <EmptyState
            title="No suppliers found"
            description="Operational supplier records have not been seeded or configured."
            icon={Building2}
          />
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {suppliers.map(sup => {
              const isExpanded = expandedSupplier === sup.supplier_id;
              const isOnline = sup.status === 'Online';
              const isDegraded = sup.status === 'Degraded';

              return (
                <div
                  key={sup.supplier_id}
                  className={`rounded-xl border p-4 transition-all flex flex-col justify-between ${
                    isOnline
                      ? 'bg-slate-900/60 border-slate-700 hover:border-slate-600'
                      : isDegraded
                      ? 'bg-amber-950/20 border-amber-500/30'
                      : 'bg-rose-950/20 border-rose-500/30'
                  }`}
                >
                  <div className="space-y-3">
                    {/* Header */}
                    <div className="flex items-start justify-between">
                      <div>
                        <h4 className="text-sm font-bold text-white tracking-tight">{sup.supplier_name}</h4>
                        <span className="text-[11px] font-mono text-slate-400">{sup.supplier_id}</span>
                      </div>
                      <StatusBadge status={sup.status} size="sm" />
                    </div>

                    {/* Metrics */}
                    <div className="grid grid-cols-2 gap-2 text-xs py-2 border-y border-slate-700/40">
                      <div>
                        <span className="text-slate-400 text-[11px]">Reliability Score</span>
                        <div className="font-semibold text-slate-200 mt-0.5">
                          {(sup.reliability_score * 100).toFixed(1)}%
                        </div>
                      </div>
                      <div>
                        <span className="text-slate-400 text-[11px]">Avg Lead Time</span>
                        <div className="font-semibold text-slate-200 mt-0.5">
                          {sup.average_lead_time_days} days
                        </div>
                      </div>
                    </div>

                    {/* Last Health Check */}
                    <div className="text-[11px] text-slate-400 flex items-center justify-between">
                      <span>Last Health Check</span>
                      <span className="font-mono text-slate-300">
                        {sup.last_health_check_at ? new Date(sup.last_health_check_at).toLocaleTimeString() : '—'}
                      </span>
                    </div>

                    {/* Catalog Details Dropdown */}
                    {sup.catalog_items && sup.catalog_items.length > 0 && (
                      <div className="pt-2">
                        <button
                          onClick={() => setExpandedSupplier(isExpanded ? null : sup.supplier_id)}
                          className="w-full text-left text-xs text-sky-400 hover:text-sky-300 font-medium flex items-center justify-between py-1 px-2 rounded-lg bg-slate-800/80 cursor-pointer"
                        >
                          <span>Catalog Availability ({sup.catalog_items.length} SKUs)</span>
                          {isExpanded ? <ChevronUp className="w-3.5 h-3.5" /> : <ChevronDown className="w-3.5 h-3.5" />}
                        </button>

                        {isExpanded && (
                          <div className="mt-2 space-y-1.5 max-h-48 overflow-y-auto pr-1">
                            {sup.catalog_items.map(cat => (
                              <div
                                key={cat.sku}
                                className="p-2 rounded-lg bg-slate-800/50 border border-slate-700/50 text-[11px] flex items-center justify-between"
                              >
                                <div>
                                  <div className="font-mono text-slate-200 font-medium">{cat.sku}</div>
                                  <div className="text-slate-400 text-[10px]">
                                    Cost: ${cat.unit_reorder_cost.toFixed(2)} | Min: {cat.min_reorder_quantity}
                                  </div>
                                </div>
                                <StatusBadge status={cat.supply_availability} size="sm" />
                              </div>
                            ))}
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
      </div>

      {/* --------------------------------------------------------------------- */}
      {/* 4. Replenishment Orders Lifecycle                                     */}
      {/* --------------------------------------------------------------------- */}
      <div className="bg-slate-800/80 border border-slate-700/70 rounded-2xl p-5 shadow-sm space-y-4">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 pb-3 border-b border-slate-700/60">
          <div>
            <div className="flex items-center gap-2">
              <Truck className="w-4 h-4 text-emerald-400" />
              <h3 className="text-sm font-bold text-white uppercase tracking-wider">
                Replenishment Orders Lifecycle
              </h3>
            </div>
            <p className="text-xs text-slate-400 mt-0.5">
              Source: <span className="font-mono text-slate-300">replenishment_orders</span>. Links automated decisions &amp; actions to delivery tracking.
            </p>
          </div>

          {/* Status Filter */}
          <div className="flex items-center gap-2 text-xs">
            <span className="text-slate-400">Status:</span>
            <select
              value={replenishmentStatusFilter}
              onChange={e => setReplenishmentStatusFilter(e.target.value)}
              className="py-1.5 px-2.5 rounded-lg bg-slate-900 border border-slate-700 text-slate-200 text-xs focus:outline-none focus:border-sky-500"
            >
              <option value="ALL">All Statuses</option>
              <option value="CONFIRMED">CONFIRMED</option>
              <option value="DISPATCHED">DISPATCHED</option>
              <option value="IN_TRANSIT">IN_TRANSIT</option>
              <option value="DELIVERED">DELIVERED</option>
              <option value="CANCELLED">CANCELLED</option>
              <option value="FAILED">FAILED</option>
            </select>
          </div>
        </div>

        {filteredReplenishments.length === 0 ? (
          <EmptyState
            title="No replenishment orders found"
            description="No replenishment orders match the selected lifecycle filter."
            icon={Truck}
          />
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-left text-xs">
              <thead>
                <tr className="border-b border-slate-700/60 text-slate-400 font-semibold uppercase tracking-wider">
                  <th className="py-2.5 px-3">Order ID</th>
                  <th className="py-2.5 px-3">SKU</th>
                  <th className="py-2.5 px-3">Supplier</th>
                  <th className="py-2.5 px-3 text-right">Quantity</th>
                  <th className="py-2.5 px-3 text-center">Status</th>
                  <th className="py-2.5 px-3">Dispatched</th>
                  <th className="py-2.5 px-3">Estimated Delivery</th>
                  <th className="py-2.5 px-3">Delivered At</th>
                  <th className="py-2.5 px-3">Ledger Trace</th>
                  <th className="py-2.5 px-3 text-right">Tracking / State</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-700/30">
                {filteredReplenishments.map(order => (
                  <tr key={order.replenishment_id} className="hover:bg-slate-700/20 transition-colors">
                    <td className="py-2.5 px-3 font-mono font-bold text-sky-400 whitespace-nowrap">
                      {order.replenishment_id}
                    </td>
                    <td className="py-2.5 px-3 font-mono text-slate-300">{order.sku}</td>
                    <td className="py-2.5 px-3 text-slate-300">{order.supplier_id}</td>
                    <td className="py-2.5 px-3 text-right font-mono font-semibold text-white">
                      +{order.quantity}
                    </td>
                    <td className="py-2.5 px-3 text-center whitespace-nowrap">
                      <StatusBadge status={order.status} size="sm" />
                    </td>
                    <td className="py-2.5 px-3 text-slate-400 text-[11px] whitespace-nowrap">
                      {order.dispatched_at ? new Date(order.dispatched_at).toLocaleDateString() : '—'}
                    </td>
                    <td className="py-2.5 px-3 text-slate-400 text-[11px] whitespace-nowrap">
                      {order.estimated_delivery_at ? new Date(order.estimated_delivery_at).toLocaleString() : '—'}
                    </td>
                    <td className="py-2.5 px-3 text-[11px] whitespace-nowrap">
                      {order.delivered_at ? (
                        <span className="font-semibold text-emerald-400">
                          {new Date(order.delivered_at).toLocaleString()}
                        </span>
                      ) : (
                        <span className="text-slate-500 italic">In-Flight</span>
                      )}
                    </td>
                    <td className="py-2.5 px-3 font-mono text-[10px] text-slate-400 whitespace-nowrap">
                      Act #{order.action_id} | Dec #{order.decision_id}
                    </td>
                    <td className="py-2.5 px-3 text-right whitespace-nowrap">
                      {['CONFIRMED', 'DISPATCHED', 'IN_TRANSIT'].includes(order.status) ? (
                        <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-lg text-xs font-medium bg-amber-950/60 text-amber-300 border border-amber-800/40">
                          <Clock className="w-3.5 h-3.5 text-amber-400 animate-pulse" />
                          <span>In-Flight</span>
                        </span>
                      ) : order.status === 'DELIVERED' ? (
                        <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-lg text-xs font-medium bg-emerald-950/60 text-emerald-300 border border-emerald-800/40">
                          <CheckCircle2 className="w-3.5 h-3.5 text-emerald-400" />
                          <span>Delivered</span>
                        </span>
                      ) : (
                        <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium bg-slate-800/60 text-slate-400 border border-slate-700/40">
                          {order.status || '—'}
                        </span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* --------------------------------------------------------------------- */}
      {/* 5. Real-Time Operational Event Feed (SSE)                             */}
      {/* --------------------------------------------------------------------- */}
      <div className="bg-slate-800/80 border border-slate-700/70 rounded-2xl p-5 shadow-sm space-y-4">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 pb-3 border-b border-slate-700/60">
          <div>
            <div className="flex items-center gap-2">
              <Radio className="w-4 h-4 text-emerald-400 animate-pulse" />
              <h3 className="text-sm font-bold text-white uppercase tracking-wider">
                Live Operational Event Feed
              </h3>
            </div>
            <p className="text-xs text-slate-400 mt-0.5">
              Live Server-Sent Events (SSE) telemetry stream capturing <code className="text-sky-300">INVENTORY_REORDER_STATE_CHANGED</code> and <code className="text-emerald-300">DELIVERY_COMPLETED</code>.
            </p>
          </div>

          {/* SSE Status Pill */}
          <div className="flex items-center gap-2">
            <div className="flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-semibold border bg-slate-900 border-slate-700">
              <span
                className={`w-2 h-2 rounded-full ${
                  sseStatus === 'CONNECTED' || sseStatus === 'LIVE'
                    ? 'bg-emerald-400 animate-pulse'
                    : sseStatus === 'CONNECTING'
                    ? 'bg-sky-400 animate-pulse'
                    : sseStatus === 'RECONNECTING'
                    ? 'bg-amber-400 animate-ping'
                    : 'bg-rose-400'
                }`}
              />
              <span className="text-slate-300">SSE: {sseStatus || 'CONNECTING'}</span>
            </div>
          </div>
        </div>

        {eventsList.length === 0 ? (
          <EmptyState
            title="No operational events captured yet"
            description="Events will appear automatically in real time when reorders are dispatched or deliveries completed."
            icon={Radio}
          />
        ) : (
          <div className="space-y-2.5 max-h-80 overflow-y-auto pr-1">
            {eventsList.map((evt, idx) => {
              const isDelivery = evt.event_type === 'DELIVERY_COMPLETED';
              const isReorder = evt.event_type === 'INVENTORY_REORDER_STATE_CHANGED';

              return (
                <div
                  key={evt.event_id || idx}
                  className={`p-3 rounded-xl border text-xs flex flex-col sm:flex-row items-start sm:items-center justify-between gap-2 transition-all ${
                    isDelivery
                      ? 'bg-emerald-500/10 border-emerald-500/30 text-emerald-200'
                      : isReorder
                      ? 'bg-sky-500/10 border-sky-500/30 text-sky-200'
                      : 'bg-slate-900/60 border-slate-700/60 text-slate-300'
                  }`}
                >
                  <div className="flex items-center gap-3">
                    <span
                      className={`w-2 h-2 rounded-full flex-shrink-0 ${
                        isDelivery ? 'bg-emerald-400' : isReorder ? 'bg-sky-400' : 'bg-slate-400'
                      }`}
                    />
                    <div>
                      <div className="flex items-center gap-2">
                        <span className="font-bold tracking-wide uppercase text-[11px]">
                          {evt.event_type}
                        </span>
                        {evt.correlation_id && (
                          <span className="font-mono text-[10px] px-1.5 py-0.5 rounded bg-slate-800 text-slate-300 border border-slate-700">
                            {evt.correlation_id}
                          </span>
                        )}
                      </div>
                      <div className="text-[11px] text-slate-400 mt-0.5">
                        {evt.data?.sku && <span>SKU: <strong className="text-white font-mono">{evt.data.sku}</strong> </span>}
                        {evt.data?.quantity && <span>Qty: <strong className="text-white">+{evt.data.quantity}</strong> </span>}
                        {evt.data?.warehouse_id && <span>({evt.data.warehouse_id}) </span>}
                        {evt.data?.lifecycle_state && <span>State: <strong className="text-white">{evt.data.lifecycle_state}</strong> </span>}
                      </div>
                    </div>
                  </div>

                  <div className="font-mono text-[10px] text-slate-400 sm:text-right flex-shrink-0">
                    {evt.timestamp ? new Date(evt.timestamp).toLocaleTimeString() : '—'}
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>

      {/* --------------------------------------------------------------------- */}
      {/* 6. Operational Health & Alerts                                        */}
      {/* --------------------------------------------------------------------- */}
      <AlertsPanel refreshTrigger={refreshTrigger} />
        </>
      )}
    </div>
  );
}
