import React, { useState, useEffect, useMemo, useCallback, useRef } from 'react';
import {
  Activity,
  Terminal,
  Sliders,
  Upload,
  Download,
  RefreshCw,
  Search,
  Filter,
  Check,
  X,
  AlertTriangle,
  Play,
  Pause,
  Layers,
  Cpu,
  Database,
  Grid,
  List,
  Code,
  Copy,
  ChevronDown,
  ChevronUp,
  ArrowUpDown,
  TrendingUp,
  Maximize2,
  Minimize2,
  Edit3,
  FileText,
  Save,
  Clock,
  Zap,
  Info,
  SlidersHorizontal,
  Table,
  CheckCircle2
} from 'lucide-react';
import {
  ResponsiveContainer,
  LineChart,
  Line,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ReferenceArea,
  ReferenceLine
} from 'recharts';

// ---------------------------------------------------------------------------
// Baseline Historical & Projected Data Generator (Olist Real Catalog Horizon)
// ---------------------------------------------------------------------------
const CATEGORY_METADATA = [
  { id: 'beleza_saude', label: 'Health & Beauty', color: '#10b981', baseDemand: 135.9, stdDev: 14.2 },
  { id: 'cama_mesa_banho', label: 'Bed, Bath & Table', color: '#38bdf8', baseDemand: 130.0, stdDev: 12.8 },
  { id: 'utilidades_domesticas', label: 'Housewares', color: '#f59e0b', baseDemand: 104.1, stdDev: 11.5 },
  { id: 'esporte_lazer', label: 'Sports & Leisure', color: '#a855f7', baseDemand: 96.5, stdDev: 13.1 },
  { id: 'relogios_presentes', label: 'Watches & Gifts', color: '#ec4899', baseDemand: 95.1, stdDev: 10.4 },
  { id: 'informatica_acessorios', label: 'Computers Accessories', color: '#06b6d4', baseDemand: 92.7, stdDev: 15.0 },
  { id: 'moveis_decoracao', label: 'Furniture & Decor', color: '#6366f1', baseDemand: 82.1, stdDev: 9.8 },
];

const WEEKS_TIMELINE = [
  { week: '2018-W22', date: '2018-05-28', label: 'W01 (May 28)' },
  { week: '2018-W23', date: '2018-06-04', label: 'W02 (Jun 04)' },
  { week: '2018-W24', date: '2018-06-11', label: 'W03 (Jun 11)' },
  { week: '2018-W25', date: '2018-06-18', label: 'W04 (Jun 18)' },
  { week: '2018-W26', date: '2018-06-25', label: 'W05 (Jun 25)' },
  { week: '2018-W27', date: '2018-07-02', label: 'W06 (Jul 02)' },
  { week: '2018-W28', date: '2018-07-09', label: 'W07 (Jul 09)' },
  { week: '2018-W29', date: '2018-07-16', label: 'W08 (Jul 16)' },
  { week: '2018-W30', date: '2018-07-23', label: 'W09 (Jul 23)' },
  { week: '2018-W31', date: '2018-07-30', label: 'W10 (Jul 30)' },
  { week: '2018-W32', date: '2018-08-06', label: 'W11 (Aug 06)' },
  { week: '2018-W33', date: '2018-08-13', label: 'W12 (Aug 13)' },
  { week: '2018-W34', date: '2018-08-20', label: 'W13 (Aug 20)' },
  { week: '2018-W35', date: '2018-08-27', label: 'W14 (Aug 27)' },
];

function generateBaselineRecords() {
  const records = [];
  let recordId = 1001;

  WEEKS_TIMELINE.forEach((w, wIdx) => {
    CATEGORY_METADATA.forEach((cat) => {
      // Deterministic pseudo-variation matching Olist seasonal holdout
      const wave = Math.sin((wIdx + 1) * 0.45);
      const actualVal = Math.round((cat.baseDemand * (1 + wave * 0.18) + (wIdx * 1.5)) * 10) / 10;
      const forecastVal = Math.round((cat.baseDemand * (1 + wave * 0.15) + (wIdx * 1.2)) * 10) / 10;
      const variance = Math.round((actualVal - forecastVal) * 10) / 10;
      const variancePct = Math.round((variance / Math.max(1, actualVal)) * 1000) / 10;

      let status = 'NOMINAL';
      if (Math.abs(variancePct) > 18.0) status = 'ANOMALY';
      else if (Math.abs(variancePct) > 8.0) status = 'ELEVATED';

      records.push({
        id: `REC-${recordId++}`,
        period: w.week,
        date: w.date,
        periodLabel: w.label,
        category: cat.id,
        categoryLabel: cat.label,
        actual: actualVal,
        forecast: forecastVal,
        variance,
        variancePct,
        status,
        color: cat.color,
        stdDev: cat.stdDev,
        isCustom: false,
      });
    });
  });

  return records;
}

export default function Forecasts() {
  // Master Workstation State
  const [records, setRecords] = useState(() => generateBaselineRecords());
  const [streamActive, setStreamActive] = useState(true);
  const [streamRate, setStreamRate] = useState(142);
  const [streamLatency, setStreamLatency] = useState(28);
  const [clockUtc, setClockUtc] = useState(new Date().toISOString().substring(11, 19));

  // Left Panel Controls State
  const [aggregation, setAggregation] = useState('weekly'); // 'hourly' | 'daily' | 'weekly'
  const [confidenceInterval, setConfidenceInterval] = useState(95); // 80, 90, 95, 99
  const [movingAverageWindow, setMovingAverageWindow] = useState(5); // 1 - 14
  const [dampingFactor, setDampingFactor] = useState(1.0); // 0.5 - 2.0
  const [importModalOpen, setImportModalOpen] = useState(false);
  const [importPayloadText, setImportPayloadText] = useState('');
  const [importError, setImportError] = useState(null);
  const [showRecalibrateFlash, setShowRecalibrateFlash] = useState(false);

  // Center Panel Visualization State
  const [vizEngine, setVizEngine] = useState('lines'); // 'lines' | 'area' | 'heatmap'
  const [selectedCategory, setSelectedCategory] = useState('ALL');
  const [showActualOverlay, setShowActualOverlay] = useState(true);
  const [showConfidenceBounds, setShowConfidenceBounds] = useState(true);
  const [showMovingAverage, setShowMovingAverage] = useState(true);
  const [selectedHeatmapCell, setSelectedHeatmapCell] = useState(null);

  // Right Panel Operations Grid State
  const [gridMode, setGridMode] = useState('table'); // 'table' | 'json' | 'logs'
  const [regexFilter, setRegexFilter] = useState('');
  const [regexError, setRegexError] = useState(null);
  const [sortField, setSortField] = useState('period');
  const [sortOrder, setSortOrder] = useState('asc'); // 'asc' | 'desc'
  const [editingCell, setEditingCell] = useState(null); // { id, field }
  const [editValue, setEditValue] = useState('');
  const [copyFeedback, setCopyFeedback] = useState(false);

  // Execution Log Console Messages
  const [logs, setLogs] = useState([
    { id: 1, time: '19:40:02', level: 'INFO', src: 'SYS-INIT', msg: 'Analyst workstation initialized with XGBoost v1.0 engine.' },
    { id: 2, time: '19:40:05', level: 'INFO', src: 'KAFKA', msg: 'Partition consumer bound to topic [dataset-events]. Offset: 184920.' },
    { id: 3, time: '19:40:11', level: 'INFO', src: 'DEP-ENG', msg: 'Triad Condition ALL satisfied for Olist core orders & items.' },
    { id: 4, time: '19:40:18', level: 'DEBUG', src: 'INFERENCE', msg: 'Calculated 14-week horizon for 70 categories. R² = 0.8806.' },
    { id: 5, time: '19:40:24', level: 'WARN', src: 'VARIANCE', msg: 'Category [esporte_lazer] volatility delta +16.3% over W08.' },
  ]);

  // Live Simulated Stream Ticker
  useEffect(() => {
    const timer = setInterval(() => {
      setClockUtc(new Date().toISOString().substring(11, 19));
      if (streamActive) {
        // Jitter telemetry rates slightly
        setStreamRate(Math.floor(130 + Math.random() * 30));
        setStreamLatency(Math.floor(22 + Math.random() * 12));
      }
    }, 1000);
    return () => clearInterval(timer);
  }, [streamActive]);

  // Periodic Log Stream Simulation
  useEffect(() => {
    if (!streamActive) return;
    const logTimer = setInterval(() => {
      const components = ['KAFKA-CONSUMER', 'DEP-ENGINE', 'XGBOOST-V1', 'FEATURE-STORE', 'ANOMALY-DETECTOR'];
      const comp = components[Math.floor(Math.random() * components.length)];
      const randCat = CATEGORY_METADATA[Math.floor(Math.random() * CATEGORY_METADATA.length)].label;
      const msgs = [
        `Ingested batch #${Math.floor(10000 + Math.random() * 5000)} (payload: ${(Math.random() * 16 + 8).toFixed(1)}KB)`,
        `Evaluated lag-7 feature matrix for [${randCat}] in ${Math.floor(8 + Math.random() * 20)}ms`,
        `Variance threshold nominal across regional SP/RJ state distribution`,
        `Model weights verified: Objective reg:squarederror, depth=6`,
      ];
      const newLog = {
        id: Date.now(),
        time: new Date().toISOString().substring(11, 19),
        level: Math.random() > 0.85 ? 'WARN' : 'INFO',
        src: comp,
        msg: msgs[Math.floor(Math.random() * msgs.length)]
      };
      setLogs(prev => [...prev.slice(-45), newLog]);
    }, 4500);
    return () => clearInterval(logTimer);
  }, [streamActive]);

  // ---------------------------------------------------------------------------
  // Calculations: Confidence Bounds & Moving Average
  // ---------------------------------------------------------------------------
  const zScore = useMemo(() => {
    switch (confidenceInterval) {
      case 80: return 1.282;
      case 90: return 1.645;
      case 99: return 2.576;
      case 95:
      default: return 1.960;
    }
  }, [confidenceInterval]);

  // Compute Active Time Series for Charts
  const chartTimelineData = useMemo(() => {
    const timeBuckets = {};

    WEEKS_TIMELINE.forEach(w => {
      timeBuckets[w.week] = {
        week: w.week,
        periodLabel: w.label,
        totalActual: 0,
        totalForecast: 0,
        categoryCount: 0,
      };
      CATEGORY_METADATA.forEach(cat => {
        timeBuckets[w.week][cat.id] = 0;
        timeBuckets[w.week][`${cat.id}_actual`] = 0;
      });
    });

    // Populate from active records (taking into account inline cell edits)
    records.forEach(r => {
      if (timeBuckets[r.period]) {
        timeBuckets[r.period][r.category] = Number(r.forecast) || 0;
        timeBuckets[r.period][`${r.category}_actual`] = Number(r.actual) || 0;

        if (selectedCategory === 'ALL' || r.category === selectedCategory) {
          timeBuckets[r.period].totalActual += Number(r.actual) || 0;
          timeBuckets[r.period].totalForecast += Number(r.forecast) || 0;
          timeBuckets[r.period].categoryCount += 1;
        }
      }
    });

    const timelineArray = Object.values(timeBuckets);

    // Apply moving average and uncertainty intervals
    return timelineArray.map((point, idx, arr) => {
      // Calculate moving average over previous `movingAverageWindow` steps
      const startIdx = Math.max(0, idx - movingAverageWindow + 1);
      const windowPoints = arr.slice(startIdx, idx + 1);
      const maSum = windowPoints.reduce((acc, curr) => acc + curr.totalForecast, 0);
      const movingAvg = Math.round((maSum / windowPoints.length) * 10) / 10;

      // Uncertainty bound expansion based on CI zScore & dampingFactor
      const uncertaintySigma = (point.totalForecast * 0.08 * dampingFactor) * zScore;
      const upperBound = Math.round((point.totalForecast + uncertaintySigma) * 10) / 10;
      const lowerBound = Math.max(0, Math.round((point.totalForecast - uncertaintySigma) * 10) / 10);
      const boundSpan = Math.round((upperBound - lowerBound) * 10) / 10;

      return {
        ...point,
        movingAvg,
        upperBound,
        lowerBound,
        boundSpan,
        uncertaintySigma: Math.round(uncertaintySigma * 10) / 10
      };
    });
  }, [records, selectedCategory, movingAverageWindow, zScore, dampingFactor]);

  // Heatmap Matrix Data (Category x Week)
  const heatmapMatrix = useMemo(() => {
    return CATEGORY_METADATA.map(cat => {
      const cells = WEEKS_TIMELINE.map(w => {
        const match = records.find(r => r.period === w.week && r.category === cat.id);
        const actual = match ? Number(match.actual) : cat.baseDemand;
        const forecast = match ? Number(match.forecast) : cat.baseDemand;
        const variance = Math.round((actual - forecast) * 10) / 10;
        const variancePct = Math.round((variance / Math.max(1, actual)) * 1000) / 10;
        const absPct = Math.abs(variancePct);

        let intensityClass = 'bg-emerald-500/20 text-emerald-300 border-emerald-500/30';
        if (absPct > 18.0) intensityClass = 'bg-rose-500/30 text-rose-200 border-rose-500/50';
        else if (absPct > 8.0) intensityClass = 'bg-amber-500/25 text-amber-200 border-amber-500/40';

        return {
          id: match?.id || `${cat.id}-${w.week}`,
          period: w.week,
          weekLabel: w.label,
          category: cat.id,
          categoryLabel: cat.label,
          actual,
          forecast,
          variance,
          variancePct,
          absPct,
          intensityClass
        };
      });

      return {
        category: cat.id,
        categoryLabel: cat.label,
        color: cat.color,
        cells
      };
    });
  }, [records]);

  // Filtered & Sorted Operations Grid Records
  const gridRecords = useMemo(() => {
    let list = [...records];

    // Regex or substring filter
    if (regexFilter.trim()) {
      try {
        const rx = new RegExp(regexFilter.trim(), 'i');
        setRegexError(null);
        list = list.filter(r =>
          rx.test(r.period) ||
          rx.test(r.categoryLabel) ||
          rx.test(r.category) ||
          rx.test(r.status) ||
          rx.test(String(r.actual)) ||
          rx.test(String(r.forecast))
        );
      } catch (err) {
        setRegexError('Invalid regex syntax');
      }
    } else {
      setRegexError(null);
    }

    // Sorting
    list.sort((a, b) => {
      let vA = a[sortField];
      let vB = b[sortField];
      if (typeof vA === 'string') {
        return sortOrder === 'asc' ? vA.localeCompare(vB) : vB.localeCompare(vA);
      }
      return sortOrder === 'asc' ? Number(vA) - Number(vB) : Number(vB) - Number(vA);
    });

    return list;
  }, [records, regexFilter, sortField, sortOrder]);

  // Handle Sort Click
  const handleSort = (field) => {
    if (sortField === field) {
      setSortOrder(prev => (prev === 'asc' ? 'desc' : 'asc'));
    } else {
      setSortField(field);
      setSortOrder('asc');
    }
  };

  // Inline Cell Editing Handler
  const handleStartEdit = (id, field, initialVal) => {
    setEditingCell({ id, field });
    setEditValue(String(initialVal));
  };

  const handleCommitEdit = (id, field) => {
    const numVal = parseFloat(editValue);
    if (!isNaN(numVal)) {
      setRecords(prev => prev.map(r => {
        if (r.id === id) {
          const updated = { ...r, [field]: Math.round(numVal * 10) / 10, isCustom: true };
          // Recalculate variance dynamically
          const actual = field === 'actual' ? updated.actual : r.actual;
          const forecast = field === 'forecast' ? updated.forecast : r.forecast;
          const variance = Math.round((actual - forecast) * 10) / 10;
          const variancePct = Math.round((variance / Math.max(1, actual)) * 1000) / 10;
          let status = 'NOMINAL';
          if (Math.abs(variancePct) > 18.0) status = 'ANOMALY';
          else if (Math.abs(variancePct) > 8.0) status = 'ELEVATED';

          return { ...updated, variance, variancePct, status };
        }
        return r;
      }));
    }
    setEditingCell(null);
    setEditValue('');
  };

  // Preset Scenario Loaders
  const loadScenario = (type) => {
    if (type === 'stress') {
      setRecords(prev => prev.map((r, i) => ({
        ...r,
        actual: Math.round(r.actual * (1 + (i % 3 === 0 ? 0.35 : -0.2)) * 10) / 10,
        status: i % 2 === 0 ? 'ANOMALY' : 'ELEVATED',
        isCustom: true
      })));
    } else if (type === 'surge') {
      setRecords(prev => prev.map(r => ({
        ...r,
        forecast: Math.round(r.forecast * 1.45 * 10) / 10,
        actual: Math.round(r.actual * 1.5 * 10) / 10,
        status: 'NOMINAL',
        isCustom: true
      })));
    } else {
      // Reset to baseline Olist stream
      setRecords(generateBaselineRecords());
    }
    triggerRecalibrateFlash();
  };

  // Export Workstation Report (JSON download)
  const handleExportReport = () => {
    const report = {
      title: 'SupplySense Analyst Workstation Calibration Report',
      timestamp: new Date().toISOString(),
      metadata: {
        modelEngine: 'XGBoost Regressor (xgboost-v1)',
        objective: 'reg:squarederror',
        varianceExplainedR2: 0.8806,
        confidenceInterval: `${confidenceInterval}% (z=${zScore})`,
        movingAverageWindow: `${movingAverageWindow} periods`,
        aggregation: aggregation,
        totalRecords: records.length,
        anomaliesDetected: records.filter(r => r.status === 'ANOMALY').length
      },
      categoryCoverage: CATEGORY_METADATA.map(c => ({
        id: c.id,
        label: c.label,
        avgDemand: c.baseDemand
      })),
      calibratedRecords: records
    };

    const blob = new Blob([JSON.stringify(report, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `supplysense-analyst-report-${Date.now()}.json`;
    a.click();
    URL.revokeObjectURL(url);
  };

  // Custom Raw Payload Importer
  const handleImportPayload = () => {
    try {
      setImportError(null);
      const parsed = JSON.parse(importPayloadText);
      const incomingList = Array.isArray(parsed) ? parsed : (parsed.records || parsed.calibratedRecords || []);
      if (incomingList.length === 0) {
        throw new Error('Payload contains no valid record items array.');
      }

      const formatted = incomingList.map((item, idx) => ({
        id: item.id || `CUSTOM-${idx + 1}`,
        period: item.period || '2018-W22',
        date: item.date || '2018-05-28',
        periodLabel: item.periodLabel || item.period || 'W01',
        category: item.category || 'general',
        categoryLabel: item.categoryLabel || item.category || 'General Category',
        actual: Number(item.actual) || 100,
        forecast: Number(item.forecast) || 100,
        variance: Number(item.variance) || 0,
        variancePct: Number(item.variancePct) || 0,
        status: item.status || 'NOMINAL',
        color: item.color || '#38bdf8',
        stdDev: Number(item.stdDev) || 10,
        isCustom: true
      }));

      setRecords(formatted);
      setImportModalOpen(false);
      setImportPayloadText('');
      triggerRecalibrateFlash();
    } catch (err) {
      setImportError(`Import Parse Error: ${err.message}`);
    }
  };

  const triggerRecalibrateFlash = () => {
    setShowRecalibrateFlash(true);
    setTimeout(() => setShowRecalibrateFlash(false), 800);
  };

  const copyJsonToClipboard = () => {
    navigator.clipboard.writeText(JSON.stringify(records, null, 2));
    setCopyFeedback(true);
    setTimeout(() => setCopyFeedback(false), 1500);
  };

  return (
    <div className="space-y-3 font-sans pb-10">
      {/* ===================================================================== */}
      {/* TOP BANNER: Real-Time Data Stream Status Ticker & Workstation HUD */}
      {/* ===================================================================== */}
      <div className="bg-slate-950 border border-slate-800 rounded-xl px-4 py-2.5 shadow-xl shadow-black/40 flex flex-wrap items-center justify-between gap-3 text-xs">
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-2">
            <span className="relative flex h-2.5 w-2.5">
              {streamActive && (
                <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
              )}
              <span className={`relative inline-flex rounded-full h-2.5 w-2.5 ${streamActive ? 'bg-emerald-500' : 'bg-amber-500'}`}></span>
            </span>
            <span className="font-mono font-bold uppercase tracking-wider text-white">
              DATA STREAM STATUS: <span className={streamActive ? 'text-emerald-400' : 'text-amber-400'}>{streamActive ? 'ACTIVE // INGESTING' : 'PAUSED'}</span>
            </span>
          </div>

          <div className="hidden sm:flex items-center gap-2 border-l border-slate-800 pl-3 font-mono text-[11px] text-slate-400">
            <span>FEED: <strong className="text-sky-300">olist.stream.v2 (Kafka partitioned)</strong></span>
            <span className="text-slate-600">|</span>
            <span>INGESTION: <strong className="text-emerald-300">{streamRate} evt/s</strong></span>
            <span className="text-slate-600">|</span>
            <span>LATENCY: <strong className="text-indigo-300">{streamLatency}ms</strong></span>
            <span className="text-slate-600">|</span>
            <span>BUFFER: <strong className="text-slate-200">99.8%</strong></span>
          </div>
        </div>

        {/* Stream Actions */}
        <div className="flex items-center gap-2 font-mono text-[11px]">
          <span className="text-slate-500 hidden md:inline">UTC: {clockUtc}</span>
          <button
            onClick={() => setStreamActive(prev => !prev)}
            className={`px-2.5 py-1 rounded border flex items-center gap-1.5 transition-colors cursor-pointer ${
              streamActive
                ? 'bg-amber-500/10 text-amber-300 border-amber-500/30 hover:bg-amber-500/20'
                : 'bg-emerald-500/10 text-emerald-300 border-emerald-500/30 hover:bg-emerald-500/20'
            }`}
          >
            {streamActive ? <Pause className="w-3 h-3" /> : <Play className="w-3 h-3" />}
            <span>{streamActive ? 'Pause Stream' : 'Resume Stream'}</span>
          </button>

          <button
            onClick={() => loadScenario('baseline')}
            className="px-2.5 py-1 rounded bg-slate-900 hover:bg-slate-800 text-slate-300 border border-slate-700 flex items-center gap-1.5 transition-colors cursor-pointer"
            title="Reset workstation to clean baseline Olist partition"
          >
            <RefreshCw className="w-3 h-3 text-sky-400" />
            <span>Reset Baseline</span>
          </button>
        </div>
      </div>

      {/* ===================================================================== */}
      {/* 3-COLUMN ANALYST WORKSTATION LAYOUT */}
      {/* ===================================================================== */}
      <div className="grid grid-cols-1 lg:grid-cols-12 gap-3.5">
        
        {/* =================================================================== */}
        {/* COLUMN 1: LEFT CONTROLS PANEL (Real-Time Config & Tuning) */}
        {/* =================================================================== */}
        <div className="lg:col-span-3 space-y-3.5">
          {/* Panel Header */}
          <div className="bg-slate-950 border border-slate-800 rounded-xl p-3.5 shadow-md space-y-3">
            <div className="flex items-center justify-between pb-2 border-b border-slate-800">
              <div className="flex items-center gap-2">
                <Sliders className="w-4 h-4 text-sky-400" />
                <h3 className="text-xs font-bold font-mono uppercase tracking-wider text-white">
                  1. Workstation Controls
                </h3>
              </div>
              <span className="text-[10px] font-mono px-1.5 py-0.5 rounded bg-sky-500/10 text-sky-400 border border-sky-500/30">
                ACTIVE
              </span>
            </div>

            {/* Ingestion & Data Importer */}
            <div className="space-y-2">
              <label className="text-[11px] font-mono font-semibold text-slate-400 uppercase tracking-wide block">
                Data Stream Importer (JSON / CSV)
              </label>
              <div className="grid grid-cols-2 gap-2">
                <button
                  onClick={() => setImportModalOpen(true)}
                  className="px-2.5 py-2 rounded-lg bg-slate-900 hover:bg-slate-800 border border-slate-700/80 text-sky-300 font-mono text-[11px] flex items-center justify-center gap-1.5 transition-colors cursor-pointer"
                >
                  <Upload className="w-3.5 h-3.5 text-sky-400" />
                  <span>Paste Payload</span>
                </button>

                <button
                  onClick={handleExportReport}
                  className="px-2.5 py-2 rounded-lg bg-slate-900 hover:bg-slate-800 border border-slate-700/80 text-emerald-300 font-mono text-[11px] flex items-center justify-center gap-1.5 transition-colors cursor-pointer"
                >
                  <Download className="w-3.5 h-3.5 text-emerald-400" />
                  <span>Export Report</span>
                </button>
              </div>

              {/* Quick Scenario Injectors */}
              <div className="pt-2 border-t border-slate-800/80">
                <span className="text-[10px] font-mono text-slate-500 block mb-1.5 uppercase">
                  Inject Test Scenarios:
                </span>
                <div className="flex flex-wrap gap-1.5">
                  <button
                    onClick={() => loadScenario('baseline')}
                    className="px-2 py-1 rounded bg-slate-900 hover:bg-slate-800 text-slate-300 border border-slate-800 text-[10px] font-mono cursor-pointer"
                  >
                    Olist Base (14W)
                  </button>
                  <button
                    onClick={() => loadScenario('surge')}
                    className="px-2 py-1 rounded bg-slate-900 hover:bg-slate-800 text-emerald-300 border border-emerald-500/30 text-[10px] font-mono cursor-pointer"
                  >
                    Q4 Holiday Surge
                  </button>
                  <button
                    onClick={() => loadScenario('stress')}
                    className="px-2 py-1 rounded bg-slate-900 hover:bg-slate-800 text-rose-300 border border-rose-500/30 text-[10px] font-mono cursor-pointer"
                  >
                    Supply Chain Stress
                  </button>
                </div>
              </div>
            </div>

            {/* Aggregation Toggles */}
            <div className="pt-2 border-t border-slate-800/80 space-y-2">
              <label className="text-[11px] font-mono font-semibold text-slate-400 uppercase tracking-wide block">
                Temporal Bucket Aggregation
              </label>
              <div className="grid grid-cols-3 gap-1 bg-slate-900 p-1 rounded-lg border border-slate-800">
                {[
                  { id: 'hourly', label: '1H', desc: 'Hourly' },
                  { id: 'daily',  label: '24D', desc: 'Daily' },
                  { id: 'weekly', label: '7W', desc: 'Weekly' },
                ].map(agg => (
                  <button
                    key={agg.id}
                    onClick={() => setAggregation(agg.id)}
                    className={`py-1 text-center font-mono text-xs rounded transition-all cursor-pointer ${
                      aggregation === agg.id
                        ? 'bg-sky-500 text-slate-950 font-bold shadow-sm'
                        : 'text-slate-400 hover:text-white'
                    }`}
                  >
                    {agg.label}
                  </button>
                ))}
              </div>
              <span className="text-[10px] font-mono text-slate-500 block">
                Active Resolution: <strong className="text-slate-300 uppercase">{aggregation}</strong> intervals (98 partitions)
              </span>
            </div>

            {/* Model Parameter Tuning Card */}
            <div className="pt-2 border-t border-slate-800/80 space-y-3">
              <div className="flex items-center justify-between">
                <label className="text-[11px] font-mono font-semibold text-slate-400 uppercase tracking-wide">
                  Model Parameter Tuning
                </label>
                <Cpu className="w-3.5 h-3.5 text-indigo-400" />
              </div>

              {/* Slider 1: Confidence Interval */}
              <div className="space-y-1">
                <div className="flex justify-between text-[11px] font-mono">
                  <span className="text-slate-400">Confidence Interval (CI):</span>
                  <span className="font-bold text-sky-400">{confidenceInterval}% (z={zScore})</span>
                </div>
                <input
                  type="range"
                  min="80"
                  max="99"
                  step="5"
                  value={confidenceInterval}
                  onChange={(e) => setConfidenceInterval(Number(e.target.value))}
                  className="w-full accent-sky-400 cursor-pointer h-1 bg-slate-800 rounded-lg"
                />
                <div className="flex justify-between text-[9px] font-mono text-slate-600">
                  <span>80%</span>
                  <span>90%</span>
                  <span>95% (Nominal)</span>
                  <span>99%</span>
                </div>
              </div>

              {/* Slider 2: Moving Average Smoothing Window */}
              <div className="space-y-1">
                <div className="flex justify-between text-[11px] font-mono">
                  <span className="text-slate-400">MA Smoothing Window:</span>
                  <span className="font-bold text-amber-400">{movingAverageWindow} Periods</span>
                </div>
                <input
                  type="range"
                  min="1"
                  max="10"
                  step="1"
                  value={movingAverageWindow}
                  onChange={(e) => setMovingAverageWindow(Number(e.target.value))}
                  className="w-full accent-amber-400 cursor-pointer h-1 bg-slate-800 rounded-lg"
                />
                <div className="flex justify-between text-[9px] font-mono text-slate-600">
                  <span>1 (Raw)</span>
                  <span>5 (Standard)</span>
                  <span>10 (Smoothed)</span>
                </div>
              </div>

              {/* Slider 3: Damping Factor */}
              <div className="space-y-1">
                <div className="flex justify-between text-[11px] font-mono">
                  <span className="text-slate-400">Variance Multiplier:</span>
                  <span className="font-bold text-purple-400">{dampingFactor.toFixed(1)}x</span>
                </div>
                <input
                  type="range"
                  min="0.5"
                  max="2.0"
                  step="0.1"
                  value={dampingFactor}
                  onChange={(e) => setDampingFactor(Number(e.target.value))}
                  className="w-full accent-purple-400 cursor-pointer h-1 bg-slate-800 rounded-lg"
                />
              </div>

              {/* Recalibrate Trigger */}
              <button
                onClick={triggerRecalibrateFlash}
                className={`w-full py-2 rounded-lg font-mono text-xs font-bold uppercase transition-all flex items-center justify-center gap-2 cursor-pointer ${
                  showRecalibrateFlash
                    ? 'bg-emerald-500 text-slate-950 shadow-lg shadow-emerald-500/40'
                    : 'bg-indigo-600/30 hover:bg-indigo-600/40 border border-indigo-500/50 text-indigo-200'
                }`}
              >
                <Zap className="w-3.5 h-3.5" />
                <span>{showRecalibrateFlash ? 'Model Recalibrated!' : 'Recalibrate Engine'}</span>
              </button>
            </div>

            {/* Quick Engine Telemetry */}
            <div className="p-2.5 rounded-lg bg-slate-900 border border-slate-800 font-mono text-[10px] space-y-1 text-slate-400">
              <div className="flex justify-between">
                <span>Algorithm:</span>
                <span className="text-slate-200">XGBoost Regressor</span>
              </div>
              <div className="flex justify-between">
                <span>Variance Explained:</span>
                <span className="text-emerald-400 font-bold">R² = 0.8806</span>
              </div>
              <div className="flex justify-between">
                <span>Holdout Error:</span>
                <span className="text-sky-300">MAPE 14.8% | RMSE 3.42</span>
              </div>
              <div className="flex justify-between">
                <span>Custom Records:</span>
                <span className="text-amber-400">{records.filter(r => r.isCustom).length} Edited</span>
              </div>
            </div>
          </div>
        </div>

        {/* =================================================================== */}
        {/* COLUMN 2: CENTER VISUALIZATION CANVAS (Real-Time Interactive Charts) */}
        {/* =================================================================== */}
        <div className="lg:col-span-6 space-y-3.5">
          <div className="bg-slate-950 border border-slate-800 rounded-xl p-4 shadow-md space-y-3">
            {/* Canvas Header & Engine Switcher */}
            <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 pb-3 border-b border-slate-800">
              <div className="flex items-center gap-2">
                <Activity className="w-4 h-4 text-emerald-400" />
                <h3 className="text-xs font-bold font-mono uppercase tracking-wider text-white">
                  2. Interactive Forecasting Canvas
                </h3>
              </div>

              {/* View Engines Tabs */}
              <div className="flex items-center gap-1 bg-slate-900 p-1 rounded-lg border border-slate-800 font-mono text-xs">
                <button
                  onClick={() => setVizEngine('lines')}
                  className={`px-2.5 py-1 rounded transition-colors cursor-pointer ${
                    vizEngine === 'lines'
                      ? 'bg-sky-500 text-slate-950 font-bold'
                      : 'text-slate-400 hover:text-white'
                  }`}
                >
                  Multi-Series Line
                </button>
                <button
                  onClick={() => setVizEngine('area')}
                  className={`px-2.5 py-1 rounded transition-colors cursor-pointer ${
                    vizEngine === 'area'
                      ? 'bg-sky-500 text-slate-950 font-bold'
                      : 'text-slate-400 hover:text-white'
                  }`}
                >
                  Area Envelope
                </button>
                <button
                  onClick={() => setVizEngine('heatmap')}
                  className={`px-2.5 py-1 rounded transition-colors cursor-pointer ${
                    vizEngine === 'heatmap'
                      ? 'bg-sky-500 text-slate-950 font-bold'
                      : 'text-slate-400 hover:text-white'
                  }`}
                >
                  Variance Matrix
                </button>
              </div>
            </div>

            {/* Filter & Overlay Toggles Sub-Bar */}
            <div className="flex flex-wrap items-center justify-between gap-2 text-xs font-mono">
              <div className="flex items-center gap-2">
                <span className="text-slate-400 text-[11px]">Filter Sector:</span>
                <select
                  value={selectedCategory}
                  onChange={(e) => setSelectedCategory(e.target.value)}
                  className="bg-slate-900 border border-slate-700 rounded px-2 py-1 text-sky-300 text-[11px] focus:outline-none focus:border-sky-500 cursor-pointer"
                >
                  <option value="ALL">All 7 Top Categories (Aggregated)</option>
                  {CATEGORY_METADATA.map(c => (
                    <option key={c.id} value={c.id}>{c.label}</option>
                  ))}
                </select>
              </div>

              {/* Overlays */}
              {vizEngine !== 'heatmap' && (
                <div className="flex items-center gap-3 text-[11px]">
                  <label className="flex items-center gap-1.5 text-slate-300 cursor-pointer">
                    <input
                      type="checkbox"
                      checked={showActualOverlay}
                      onChange={(e) => setShowActualOverlay(e.target.checked)}
                      className="accent-sky-400 rounded"
                    />
                    <span>Actuals</span>
                  </label>

                  <label className="flex items-center gap-1.5 text-slate-300 cursor-pointer">
                    <input
                      type="checkbox"
                      checked={showConfidenceBounds}
                      onChange={(e) => setShowConfidenceBounds(e.target.checked)}
                      className="accent-purple-400 rounded"
                    />
                    <span>CI Bounds</span>
                  </label>

                  <label className="flex items-center gap-1.5 text-slate-300 cursor-pointer">
                    <input
                      type="checkbox"
                      checked={showMovingAverage}
                      onChange={(e) => setShowMovingAverage(e.target.checked)}
                      className="accent-amber-400 rounded"
                    />
                    <span>Moving Avg</span>
                  </label>
                </div>
              )}
            </div>

            {/* =============================================================== */}
            {/* ENGINE 1: MULTI-SERIES LINE CHART */}
            {/* =============================================================== */}
            {vizEngine === 'lines' && (
              <div className="h-80 w-full pt-2">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={chartTimelineData} margin={{ top: 10, right: 15, left: 0, bottom: 20 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" opacity={0.6} />
                    <XAxis
                      dataKey="periodLabel"
                      tick={{ fill: '#94a3b8', fontSize: 10, fontFamily: 'monospace' }}
                      stroke="#334155"
                      minTickGap={15}
                    />
                    <YAxis
                      tick={{ fill: '#94a3b8', fontSize: 10, fontFamily: 'monospace' }}
                      stroke="#334155"
                      label={{ value: 'Projected Demand (Units)', angle: -90, position: 'insideLeft', fill: '#64748b', fontSize: 10, dy: 60 }}
                    />
                    <Tooltip
                      contentStyle={{ backgroundColor: '#020617', borderColor: '#334155', borderRadius: '0.5rem', fontSize: '11px', fontFamily: 'monospace' }}
                      itemStyle={{ color: '#f8fafc' }}
                    />
                    <Legend
                      verticalAlign="bottom"
                      height={32}
                      iconType="plainline"
                      formatter={(val) => <span className="text-[10px] font-mono text-slate-300">{val}</span>}
                    />

                    {/* Forecast Baseline Line */}
                    <Line
                      type="monotone"
                      dataKey="totalForecast"
                      name="Projected Forecast (Median)"
                      stroke="#10b981"
                      strokeWidth={2.5}
                      dot={{ r: 2 }}
                    />

                    {/* Historical Actuals Line */}
                    {showActualOverlay && (
                      <Line
                        type="monotone"
                        dataKey="totalActual"
                        name="Historical Actual Demand"
                        stroke="#38bdf8"
                        strokeWidth={1.5}
                        strokeDasharray="4 4"
                        dot={false}
                      />
                    )}

                    {/* Upper & Lower Confidence Interval Bounds */}
                    {showConfidenceBounds && (
                      <>
                        <Line
                          type="monotone"
                          dataKey="upperBound"
                          name={`Upper CI (+${confidenceInterval}%)`}
                          stroke="#c084fc"
                          strokeWidth={1}
                          strokeDasharray="2 2"
                          dot={false}
                        />
                        <Line
                          type="monotone"
                          dataKey="lowerBound"
                          name={`Lower CI (-${confidenceInterval}%)`}
                          stroke="#c084fc"
                          strokeWidth={1}
                          strokeDasharray="2 2"
                          dot={false}
                        />
                      </>
                    )}

                    {/* Moving Average Line */}
                    {showMovingAverage && (
                      <Line
                        type="monotone"
                        dataKey="movingAvg"
                        name={`SMA (${movingAverageWindow}P)`}
                        stroke="#f59e0b"
                        strokeWidth={1.8}
                        dot={false}
                      />
                    )}
                  </LineChart>
                </ResponsiveContainer>
              </div>
            )}

            {/* =============================================================== */}
            {/* ENGINE 2: AREA ACCUMULATION CHART */}
            {/* =============================================================== */}
            {vizEngine === 'area' && (
              <div className="h-80 w-full pt-2">
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart data={chartTimelineData} margin={{ top: 10, right: 15, left: 0, bottom: 20 }}>
                    <defs>
                      <linearGradient id="colorForecast" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor="#10b981" stopOpacity={0.4} />
                        <stop offset="95%" stopColor="#10b981" stopOpacity={0.0} />
                      </linearGradient>
                      <linearGradient id="colorActual" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor="#38bdf8" stopOpacity={0.3} />
                        <stop offset="95%" stopColor="#38bdf8" stopOpacity={0.0} />
                      </linearGradient>
                    </defs>
                    <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" opacity={0.6} />
                    <XAxis
                      dataKey="periodLabel"
                      tick={{ fill: '#94a3b8', fontSize: 10, fontFamily: 'monospace' }}
                      stroke="#334155"
                      minTickGap={15}
                    />
                    <YAxis
                      tick={{ fill: '#94a3b8', fontSize: 10, fontFamily: 'monospace' }}
                      stroke="#334155"
                      label={{ value: 'Demand Volume Envelope', angle: -90, position: 'insideLeft', fill: '#64748b', fontSize: 10, dy: 65 }}
                    />
                    <Tooltip
                      contentStyle={{ backgroundColor: '#020617', borderColor: '#334155', borderRadius: '0.5rem', fontSize: '11px', fontFamily: 'monospace' }}
                    />
                    <Legend verticalAlign="bottom" height={32} />

                    <Area
                      type="monotone"
                      dataKey="totalForecast"
                      name="Forecast Demand"
                      stroke="#10b981"
                      fillOpacity={1}
                      fill="url(#colorForecast)"
                    />
                    {showActualOverlay && (
                      <Area
                        type="monotone"
                        dataKey="totalActual"
                        name="Actual Demand"
                        stroke="#38bdf8"
                        fillOpacity={1}
                        fill="url(#colorActual)"
                      />
                    )}
                    {showConfidenceBounds && (
                      <Area
                        type="monotone"
                        dataKey="upperBound"
                        name="Upper Uncertainty"
                        stroke="#a855f7"
                        fill="none"
                        strokeDasharray="3 3"
                      />
                    )}
                  </AreaChart>
                </ResponsiveContainer>
              </div>
            )}

            {/* =============================================================== */}
            {/* ENGINE 3: HEATMAP MATRIX (Prediction Variance Over Time) */}
            {/* =============================================================== */}
            {vizEngine === 'heatmap' && (
              <div className="space-y-3 pt-1">
                <div className="flex items-center justify-between text-[11px] font-mono text-slate-400">
                  <span>Rows = Product Sectors | Columns = Horizon Weeks | Intensity = Prediction Variance (% Δ)</span>
                  <div className="flex items-center gap-3">
                    <span className="flex items-center gap-1 text-emerald-400"><span className="w-2.5 h-2.5 rounded bg-emerald-500/30 border border-emerald-500/50"></span> &lt; 8%</span>
                    <span className="flex items-center gap-1 text-amber-400"><span className="w-2.5 h-2.5 rounded bg-amber-500/30 border border-amber-500/50"></span> 8–18%</span>
                    <span className="flex items-center gap-1 text-rose-400"><span className="w-2.5 h-2.5 rounded bg-rose-500/30 border border-rose-500/50"></span> &gt; 18%</span>
                  </div>
                </div>

                <div className="overflow-x-auto border border-slate-800 rounded-lg p-2 bg-slate-900/60">
                  <table className="w-full text-[10px] font-mono border-collapse">
                    <thead>
                      <tr>
                        <th className="py-1 px-2 text-left text-slate-400 border-b border-slate-800">Category</th>
                        {WEEKS_TIMELINE.map(w => (
                          <th key={w.week} className="py-1 px-1.5 text-center text-slate-400 border-b border-slate-800">
                            {w.week.replace('2018-', '')}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {heatmapMatrix.map(row => (
                        <tr key={row.category} className="hover:bg-slate-800/40 transition-colors">
                          <td className="py-1.5 px-2 font-bold text-slate-200 border-b border-slate-800/60 whitespace-nowrap">
                            <span className="w-2 h-2 rounded-full inline-block mr-1.5" style={{ backgroundColor: row.color }}></span>
                            {row.categoryLabel}
                          </td>
                          {row.cells.map(cell => (
                            <td
                              key={cell.period}
                              onClick={() => setSelectedHeatmapCell(cell)}
                              className="p-1 border-b border-slate-800/60 text-center cursor-pointer"
                            >
                              <div
                                className={`py-1 px-1 rounded border text-[9px] font-bold transition-transform hover:scale-110 ${cell.intensityClass}`}
                                title={`${cell.categoryLabel} ${cell.weekLabel}: Actual ${cell.actual} vs Forecast ${cell.forecast} (Δ ${cell.variancePct}%)`}
                              >
                                {cell.variancePct > 0 ? `+${cell.variancePct}%` : `${cell.variancePct}%`}
                              </div>
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>

                {/* Heatmap Cell Inspector Modal/Drawer */}
                {selectedHeatmapCell && (
                  <div className="p-3 bg-slate-900 border border-slate-800 rounded-lg flex items-center justify-between text-xs font-mono">
                    <div className="flex items-center gap-3">
                      <span className="font-bold text-white">{selectedHeatmapCell.categoryLabel}</span>
                      <span className="text-slate-400">{selectedHeatmapCell.weekLabel}</span>
                      <span className="text-emerald-400">Forecast: {selectedHeatmapCell.forecast}u</span>
                      <span className="text-sky-300">Actual: {selectedHeatmapCell.actual}u</span>
                      <span className={selectedHeatmapCell.absPct > 18 ? 'text-rose-400 font-bold' : 'text-amber-400'}>
                        Residual Δ: {selectedHeatmapCell.variance} units ({selectedHeatmapCell.variancePct}%)
                      </span>
                    </div>
                    <button
                      onClick={() => setSelectedHeatmapCell(null)}
                      className="text-slate-400 hover:text-white"
                    >
                      <X className="w-4 h-4" />
                    </button>
                  </div>
                )}
              </div>
            )}
          </div>
        </div>

        {/* =================================================================== */}
        {/* COLUMN 3: RIGHT OPERATIONS GRID (Live Data Stream & Inline Editing) */}
        {/* =================================================================== */}
        <div className="lg:col-span-3 space-y-3.5">
          <div className="bg-slate-950 border border-slate-800 rounded-xl p-3.5 shadow-md space-y-3">
            {/* Header & Mode Switcher */}
            <div className="flex items-center justify-between pb-2 border-b border-slate-800">
              <div className="flex items-center gap-2">
                <Database className="w-4 h-4 text-purple-400" />
                <h3 className="text-xs font-bold font-mono uppercase tracking-wider text-white">
                  3. Operations Grid
                </h3>
              </div>

              {/* View Mode Toggle: Table vs JSON vs Logs */}
              <div className="flex items-center gap-1 bg-slate-900 p-0.5 rounded border border-slate-800 font-mono text-[10px]">
                <button
                  onClick={() => setGridMode('table')}
                  className={`px-2 py-0.5 rounded transition-colors cursor-pointer ${
                    gridMode === 'table' ? 'bg-purple-600 text-white font-bold' : 'text-slate-400 hover:text-white'
                  }`}
                >
                  Table
                </button>
                <button
                  onClick={() => setGridMode('json')}
                  className={`px-2 py-0.5 rounded transition-colors cursor-pointer ${
                    gridMode === 'json' ? 'bg-purple-600 text-white font-bold' : 'text-slate-400 hover:text-white'
                  }`}
                >
                  JSON
                </button>
                <button
                  onClick={() => setGridMode('logs')}
                  className={`px-2 py-0.5 rounded transition-colors cursor-pointer ${
                    gridMode === 'logs' ? 'bg-purple-600 text-white font-bold' : 'text-slate-400 hover:text-white'
                  }`}
                >
                  Terminal
                </button>
              </div>
            </div>

            {/* Quick Regex Filter & Row Count */}
            {gridMode === 'table' && (
              <div className="space-y-1">
                <div className="relative">
                  <Search className="w-3.5 h-3.5 text-slate-500 absolute left-2.5 top-2" />
                  <input
                    type="text"
                    placeholder="Quick regex e.g. /(health|bed)/ or >100..."
                    value={regexFilter}
                    onChange={(e) => setRegexFilter(e.target.value)}
                    className="w-full bg-slate-900 border border-slate-800 rounded pl-8 pr-2.5 py-1 text-xs font-mono text-white placeholder-slate-600 focus:outline-none focus:border-purple-500"
                  />
                </div>
                <div className="flex justify-between text-[10px] font-mono text-slate-500">
                  <span>Match: <strong className="text-slate-300">{gridRecords.length}</strong> / {records.length} records</span>
                  {regexError && <span className="text-rose-400">{regexError}</span>}
                  <span className="text-slate-500">Click cells to edit inline</span>
                </div>
              </div>
            )}

            {/* =============================================================== */}
            {/* VIEW A: INTERACTIVE OPERATIONS TABLE WITH INLINE EDITING */}
            {/* =============================================================== */}
            {gridMode === 'table' && (
              <div className="overflow-x-auto max-h-[380px] border border-slate-800/80 rounded-lg">
                <table className="w-full text-[11px] font-mono">
                  <thead className="bg-slate-900 text-slate-400 uppercase text-[9px] font-bold sticky top-0 z-10 border-b border-slate-800">
                    <tr>
                      <th className="py-2 px-2 text-left cursor-pointer hover:text-white" onClick={() => handleSort('period')}>
                        Period <ArrowUpDown className="w-2.5 h-2.5 inline" />
                      </th>
                      <th className="py-2 px-2 text-left cursor-pointer hover:text-white" onClick={() => handleSort('category')}>
                        Sector
                      </th>
                      <th className="py-2 px-2 text-right cursor-pointer hover:text-white" onClick={() => handleSort('forecast')}>
                        Forecast
                      </th>
                      <th className="py-2 px-2 text-right cursor-pointer hover:text-white" onClick={() => handleSort('variancePct')}>
                        Δ %
                      </th>
                      <th className="py-2 px-2 text-center">Status</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-800/60 bg-slate-950">
                    {gridRecords.slice(0, 35).map((row) => (
                      <tr key={row.id} className="hover:bg-slate-900/50 transition-colors">
                        <td className="py-1.5 px-2 text-slate-400 whitespace-nowrap">
                          {row.period.replace('2018-', '')}
                        </td>
                        <td className="py-1.5 px-2 text-slate-200 truncate max-w-[90px]" title={row.categoryLabel}>
                          {row.categoryLabel}
                        </td>

                        {/* Editable Forecast Cell */}
                        <td className="py-1.5 px-2 text-right font-bold">
                          {editingCell?.id === row.id && editingCell?.field === 'forecast' ? (
                            <input
                              type="number"
                              autoFocus
                              value={editValue}
                              onChange={(e) => setEditValue(e.target.value)}
                              onBlur={() => handleCommitEdit(row.id, 'forecast')}
                              onKeyDown={(e) => e.key === 'Enter' && handleCommitEdit(row.id, 'forecast')}
                              className="w-16 bg-slate-900 border border-sky-400 rounded px-1 text-right text-sky-300 text-xs font-mono outline-none"
                            />
                          ) : (
                            <span
                              onClick={() => handleStartEdit(row.id, 'forecast', row.forecast)}
                              className="text-emerald-400 hover:text-sky-300 hover:underline cursor-pointer flex items-center justify-end gap-1"
                              title="Click to edit value inline"
                            >
                              {row.forecast}
                              <Edit3 className="w-2.5 h-2.5 opacity-40" />
                            </span>
                          )}
                        </td>

                        {/* Variance Delta % */}
                        <td className={`py-1.5 px-2 text-right font-bold ${
                          row.status === 'ANOMALY' ? 'text-rose-400' : row.status === 'ELEVATED' ? 'text-amber-400' : 'text-slate-400'
                        }`}>
                          {row.variancePct > 0 ? `+${row.variancePct}%` : `${row.variancePct}%`}
                        </td>

                        {/* Status Badge */}
                        <td className="py-1.5 px-2 text-center">
                          <span className={`text-[8px] font-bold px-1.5 py-0.2 rounded uppercase ${
                            row.status === 'ANOMALY'
                              ? 'bg-rose-500/20 text-rose-300 border border-rose-500/30'
                              : row.status === 'ELEVATED'
                              ? 'bg-amber-500/20 text-amber-300 border border-amber-500/30'
                              : 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/20'
                          }`}>
                            {row.status}
                          </span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            {/* =============================================================== */}
            {/* VIEW B: RAW JSON TREE VIEWER */}
            {/* =============================================================== */}
            {gridMode === 'json' && (
              <div className="space-y-2">
                <div className="flex justify-between items-center text-[10px] font-mono text-slate-400">
                  <span>RAW JSON TREE ({records.length} records)</span>
                  <button
                    onClick={copyJsonToClipboard}
                    className="px-2 py-0.5 rounded bg-slate-900 hover:bg-slate-800 text-sky-300 border border-slate-700 flex items-center gap-1 cursor-pointer"
                  >
                    {copyFeedback ? <Check className="w-3 h-3 text-emerald-400" /> : <Copy className="w-3 h-3" />}
                    <span>{copyFeedback ? 'Copied!' : 'Copy JSON'}</span>
                  </button>
                </div>
                <pre className="p-2.5 bg-slate-900/90 border border-slate-800 rounded-lg text-[10px] font-mono text-emerald-300 max-h-[380px] overflow-auto leading-relaxed select-all">
                  {JSON.stringify(records.slice(0, 15), null, 2)}
                </pre>
                <span className="text-[10px] font-mono text-slate-500 block">
                  * Truncated to first 15 records in viewer. Export contains all {records.length}.
                </span>
              </div>
            )}

            {/* =============================================================== */}
            {/* VIEW C: EXECUTION LOG TERMINAL */}
            {/* =============================================================== */}
            {gridMode === 'logs' && (
              <div className="space-y-2">
                <div className="flex justify-between items-center text-[10px] font-mono text-slate-400">
                  <span className="flex items-center gap-1.5 text-slate-300">
                    <Terminal className="w-3.5 h-3.5 text-emerald-400" />
                    <span>SYS_EXEC // TELEMETRY TERMINAL</span>
                  </span>
                  <button
                    onClick={() => setLogs([])}
                    className="text-slate-500 hover:text-slate-300 text-[10px]"
                  >
                    Clear
                  </button>
                </div>

                <div className="p-2.5 bg-black/90 border border-slate-800 rounded-lg text-[10px] font-mono max-h-[380px] overflow-y-auto space-y-1 text-slate-300">
                  <div className="text-slate-600 mb-1">analyst@supplysense-node:~$ tail -f /var/log/inference.log</div>
                  {logs.map((lg) => (
                    <div key={lg.id} className="leading-tight">
                      <span className="text-slate-500">[{lg.time}]</span>{' '}
                      <span className={
                        lg.level === 'WARN' ? 'text-amber-400 font-bold' : lg.level === 'DEBUG' ? 'text-purple-400' : 'text-sky-400'
                      }>
                        [{lg.level}]
                      </span>{' '}
                      <span className="text-slate-400">&lt;{lg.src}&gt;</span>{' '}
                      <span className={lg.level === 'WARN' ? 'text-amber-200' : 'text-slate-200'}>{lg.msg}</span>
                    </div>
                  ))}
                  <div className="text-emerald-500 animate-pulse pt-1">_</div>
                </div>
              </div>
            )}
          </div>
        </div>

      </div>

      {/* ===================================================================== */}
      {/* RAW PAYLOAD IMPORT MODAL */}
      {/* ===================================================================== */}
      {importModalOpen && (
        <div className="fixed inset-0 z-50 bg-black/80 backdrop-blur-sm flex items-center justify-center p-4">
          <div className="bg-slate-900 border border-slate-700 rounded-2xl max-w-xl w-full p-5 space-y-4 shadow-2xl font-mono text-xs">
            <div className="flex justify-between items-center pb-2 border-b border-slate-800">
              <div className="flex items-center gap-2">
                <Upload className="w-4 h-4 text-sky-400" />
                <h3 className="font-bold text-white uppercase">Raw Stream Payload Importer</h3>
              </div>
              <button onClick={() => setImportModalOpen(false)} className="text-slate-400 hover:text-white">
                <X className="w-4 h-4" />
              </button>
            </div>

            <p className="text-[11px] text-slate-400 font-sans">
              Paste arbitrary JSON stream records. Required properties per object: <code className="text-sky-300">period</code>, <code className="text-sky-300">category</code>, <code className="text-sky-300">forecast</code>, <code className="text-sky-300">actual</code>.
            </p>

            <textarea
              rows={8}
              value={importPayloadText}
              onChange={(e) => setImportPayloadText(e.target.value)}
              placeholder='[ { "period": "2018-W22", "category": "health_beauty", "forecast": 140, "actual": 135 } ]'
              className="w-full bg-slate-950 border border-slate-700 rounded-lg p-3 text-xs text-sky-300 font-mono focus:outline-none focus:border-sky-500"
            />

            {importError && (
              <div className="p-2 rounded bg-rose-500/10 border border-rose-500/30 text-rose-300 text-[11px]">
                {importError}
              </div>
            )}

            <div className="flex justify-end gap-2 pt-2 border-t border-slate-800">
              <button
                onClick={() => setImportModalOpen(false)}
                className="px-3 py-1.5 rounded bg-slate-800 hover:bg-slate-700 text-slate-300 cursor-pointer"
              >
                Cancel
              </button>
              <button
                onClick={handleImportPayload}
                className="px-4 py-1.5 rounded bg-sky-500 hover:bg-sky-400 text-slate-950 font-bold cursor-pointer"
              >
                Parse &amp; Ingest Records
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
