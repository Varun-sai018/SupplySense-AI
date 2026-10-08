import React, { useState, useEffect } from 'react';
import { GitBranch, CheckCircle2, ShieldAlert, Layers, ArrowRight, Info, AlertCircle, Database } from 'lucide-react';
import { getDependencies } from '../api/client';
import StatusBadge from '../components/StatusBadge';
import ConditionBadge from '../components/ConditionBadge';
import LoadingSpinner from '../components/LoadingSpinner';
import EmptyState from '../components/EmptyState';

export default function Dependencies({ refreshTrigger }) {
  const [dependencies, setDependencies] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    let isMounted = true;
    setLoading(true);

    getDependencies()
      .then((data) => {
        if (isMounted) {
          setDependencies(data || []);
          setError(null);
        }
      })
      .catch((err) => {
        if (isMounted) {
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

  return (
    <div className="space-y-6">
      {/* Page Header */}
      <div>
        <h2 className="text-2xl font-bold text-white tracking-tight">Event-Conditioned Dependency Engine</h2>
        <p className="text-xs sm:text-sm text-slate-400 mt-1">
          Evaluating Multi-Table synchronization conditions (ALL / ANY / QUORUM) to orchestrate downstream ML pipelines.
        </p>
      </div>

      {/* Core Research Contribution Callout Banner */}
      <div className="bg-gradient-to-r from-sky-950/60 to-indigo-950/60 border border-sky-500/30 rounded-2xl p-5 shadow-lg shadow-sky-950/20">
        <div className="flex items-start gap-3.5">
          <div className="p-2.5 bg-sky-500/20 border border-sky-500/30 rounded-xl text-sky-400 flex-shrink-0">
            <GitBranch className="w-5 h-5" />
          </div>
          <div>
            <h3 className="text-sm font-bold text-white uppercase tracking-wider">
              Core Capstone Research Contribution
            </h3>
            <p className="mt-1 text-xs sm:text-sm text-slate-300 leading-relaxed italic">
              "Traditional orchestration systems trigger pipelines on rigid, clock-based schedules (cron), often training ML models on stale or desynchronized data. SupplySense AI dynamically intercepts upstream events and evaluates condition rules (ALL, ANY, QUORUM) to trigger execution only when verified dependencies are satisfied."
            </p>
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

      {/* Dependency Rules List */}
      {loading ? (
        <LoadingSpinner message="Evaluating active pipeline dependencies..." />
      ) : dependencies.length === 0 ? (
        <EmptyState title="No dependencies found" description="No pipeline dependency rules configured." />
      ) : (
        <div className="space-y-5">
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
                  </div>
                </div>

                {/* Configured Datasets Visual Flow */}
                <div className="mt-5">
                  <span className="text-xs font-semibold text-slate-400 uppercase tracking-wider block mb-2.5">
                    Configured Upstream Datasets:
                  </span>
                  <div className="flex flex-wrap items-center gap-2">
                    {dep.configured_datasets?.map((dsName, idx) => (
                      <div 
                        key={idx}
                        className="flex items-center gap-2 px-3 py-1.5 rounded-xl bg-slate-900 border border-slate-700/80 text-xs text-slate-200"
                      >
                        <Database className="w-3.5 h-3.5 text-sky-400" />
                        <span className="font-semibold">{dsName}</span>
                      </div>
                    ))}
                  </div>
                </div>

                {/* Latest Decision Box: Heavy Visual Distinction (TRIGGER vs BLOCK) */}
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
  );
}
