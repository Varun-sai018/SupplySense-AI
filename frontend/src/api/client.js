import axios from 'axios';

/**
 * Axios instance configured to use Vite proxy.
 * Routes starting with /api and /health are proxied to http://127.0.0.1:8000.
 */
const api = axios.create({
  baseURL: '',
  timeout: 25000,
  headers: {
    'Accept': 'application/json',
  },
});

export const getHealth = async (config = {}) => {
  const response = await api.get('/health', config);
  return response.data;
};

export const getSummary = async (config = {}) => {
  const response = await api.get('/api/observability/summary', config);
  return response.data;
};

export const getExecutions = async (params = {}, config = {}) => {
  const response = await api.get('/api/observability/executions', { params, ...config });
  return response.data;
};

export const getExecution = async (executionId, config = {}) => {
  const response = await api.get(`/api/observability/executions/${executionId}`, config);
  return response.data;
};

export const getDatasets = async (params = {}, config = {}) => {
  const response = await api.get('/api/observability/datasets', { params, ...config });
  return response.data;
};

export const getDependencies = async (config = {}) => {
  const response = await api.get('/api/observability/dependencies', config);
  return response.data;
};

export const getDependencyDecisions = async (params = {}, config = {}) => {
  const response = await api.get('/api/observability/dependencies/decisions', { params, ...config });
  return response.data;
};

export const getDependencyEvents = async (params = {}, config = {}) => {
  const response = await api.get('/api/observability/dependencies/events', { params, ...config });
  return response.data;
};

export const getForecasts = async (params = {}, config = {}) => {
  const response = await api.get('/api/observability/forecasts', { params, ...config });
  return response.data;
};

export const getForecastCategoryTrends = async (executionId, params = {}, config = {}) => {
  const response = await api.get(`/api/observability/forecasts/${executionId}/category-trends`, { params, ...config });
  return response.data;
};

export const getModelComparison = async (executionId, config = {}) => {
  const response = await api.get(`/api/observability/executions/${executionId}/model-comparison`, config);
  return response.data;
};

export const getForecastProducts = async (executionId, params = {}, config = {}) => {
  const response = await api.get(`/api/observability/forecasts/${executionId}/products`, { params, ...config });
  return response.data;
};

export const getProductDetail = async (productId, params = {}, config = {}) => {
  const response = await api.get(`/api/observability/products/${productId}/details`, { params, ...config });
  return response.data;
};

export const getAnalystMetrics = async (params = {}, config = {}) => {
  const response = await api.get('/api/observability/analyst/metrics', { params, ...config });
  return response.data;
};

export const getOperationalInventory = async (params = {}, config = {}) => {
  const response = await api.get('/api/observability/inventory', { params, ...config });
  return response.data;
};

export const getOperationalSuppliers = async (params = {}, config = {}) => {
  const response = await api.get('/api/observability/suppliers', { params, ...config });
  return response.data;
};

export const getReplenishments = async (params = {}, config = {}) => {
  const response = await api.get('/api/observability/replenishments', { params, ...config });
  return response.data;
};

export const deliverReplenishment = async (replenishmentId, config = {}) => {
  const response = await api.post(`/api/observability/replenishments/${encodeURIComponent(replenishmentId)}/deliver`, {}, config);
  return response.data;
};

export const getOperationalSummary = async (config = {}) => {
  const response = await api.get('/api/observability/operational-summary', config);
  return response.data;
};

export const getRecentEvents = async (params = {}, config = {}) => {
  const response = await api.get('/api/observability/events/recent', { params, ...config });
  return response.data;
};

export const getAlerts = async (params = {}, config = {}) => {
  const response = await api.get('/api/observability/alerts', { params, ...config });
  return response.data;
};

export const getEventHealth = async (config = {}) => {
  const response = await api.get('/api/observability/event-health', config);
  return response.data;
};

export const getForecastRecommendations = async (params = {}, config = {}) => {
  const response = await api.get('/api/observability/forecast-recommendations', { params, ...config });
  return response.data;
};

export const getForecastRecommendationDetail = async (recommendationId, config = {}) => {
  const response = await api.get(`/api/observability/forecast-recommendations/${recommendationId}`, config);
  return response.data;
};

export const submitApproval = async (data, headers = {}) => {
  const response = await api.post('/api/observability/replenishment-approvals', data, { headers });
  return response.data;
};

export const getApprovals = async (params = {}, config = {}) => {
  const response = await api.get('/api/observability/replenishment-approvals', { params, ...config });
  return response.data;
};

export const getApprovalDetail = async (approvalId, config = {}) => {
  const response = await api.get(`/api/observability/replenishment-approvals/${approvalId}`, config);
  return response.data;
};

export const approveReplenishment = async (approvalId, data = {}, headers = {}) => {
  const response = await api.post(`/api/observability/replenishment-approvals/${approvalId}/approve`, data, { headers });
  return response.data;
};

export const rejectReplenishment = async (approvalId, data, headers = {}) => {
  const response = await api.post(`/api/observability/replenishment-approvals/${approvalId}/reject`, data, { headers });
  return response.data;
};

// ─── Simulator API ────────────────────────────────────────────────────────────

export const getSimulatorStatus = async (config = {}) => {
  const response = await api.get('/api/simulator/status', config);
  return response.data;
};

export const getSimulatorScenarios = async (config = {}) => {
  const response = await api.get('/api/simulator/scenarios', config);
  return response.data;
};

export const startSimulator = async (data = {}, config = {}) => {
  const response = await api.post('/api/simulator/start', data, config);
  return response.data;
};

export const pauseSimulator = async (config = {}) => {
  const response = await api.post('/api/simulator/pause', {}, config);
  return response.data;
};

export const resumeSimulator = async (config = {}) => {
  const response = await api.post('/api/simulator/resume', {}, config);
  return response.data;
};

export const stopSimulator = async (config = {}) => {
  const response = await api.post('/api/simulator/stop', {}, config);
  return response.data;
};

export const getSimulatedDatasets = async (config = {}) => {
  const response = await api.get('/api/observability/datasets', config);
  // Filter client-side to return only SIMULATED datasets
  const all = response.data || [];
  return all.filter((d) => d.source_type === 'SIMULATED');
};

export const getSimulatedExecutions = async (params = {}, config = {}) => {
  const response = await api.get('/api/observability/executions', { params: { limit: 100, ...params }, ...config });
  const all = response.data || [];
  // Simulated pipelines are prefixed with SIM-
  return all.filter((e) => e.pipeline_name && e.pipeline_name.startsWith('SIM-'));
};

export const getSimulatedDecisions = async (params = {}, config = {}) => {
  const response = await api.get('/api/observability/dependencies/decisions', { params: { limit: 100, scope: 'SIMULATED', ...params }, ...config });
  const all = Array.isArray(response.data) ? response.data : (response.data?.items || []);
  // Filter to only decisions from simulated pipelines
  return all.filter((d) => d.pipeline_name && d.pipeline_name.startsWith('SIM-'));
};

// ─── Phase F3: True End-to-End Simulation APIs ───────────────────────────────

export const runE2ESimulation = async (batchSize = 200, config = {}) => {
  const response = await api.post('/api/simulator/run-e2e', { batch_size: batchSize }, config);
  return response.data;
};

export const getSimRuns = async (params = {}, config = {}) => {
  const response = await api.get('/api/simulator/runs', { params, ...config });
  return response.data;
};

export const getSimRunDetail = async (runId, config = {}) => {
  const response = await api.get(`/api/simulator/runs/${runId}`, config);
  return response.data;
};

export const getSimForecasts = async (params = {}, config = {}) => {
  const response = await api.get('/api/observability/sim-forecasts', { params, ...config });
  return response.data;
};

export const getSimForecastCategoryTrends = async (executionId, params = {}, config = {}) => {
  const response = await api.get(`/api/observability/sim-forecasts/${executionId}/category-trends`, { params, ...config });
  return response.data;
};

export const getSimReplenishments = async (params = {}, config = {}) => {
  const response = await api.get('/api/observability/sim-replenishments', { params, ...config });
  return response.data;
};

export const getSimInventory = async (params = {}, config = {}) => {
  const response = await api.get('/api/observability/sim-inventory', { params, ...config });
  return response.data;
};

// ─── Olist Historical Order Replay Simulator APIs ───────────────────────────

export const startOlistReplay = async (data = {}, config = {}) => {
  const response = await api.post('/api/simulator/replay/start', data, config);
  return response.data;
};

export const pauseOlistReplay = async (config = {}) => {
  const response = await api.post('/api/simulator/replay/pause', {}, config);
  return response.data;
};

export const resumeOlistReplay = async (config = {}) => {
  const response = await api.post('/api/simulator/replay/resume', {}, config);
  return response.data;
};

export const stopOlistReplay = async (config = {}) => {
  const response = await api.post('/api/simulator/replay/stop', {}, config);
  return response.data;
};

export const getOlistReplayStatus = async (config = {}) => {
  const response = await api.get('/api/simulator/replay/status', config);
  return response.data;
};

export const previewOlistReplay = async (params = {}, config = {}) => {
  const response = await api.get('/api/simulator/replay/preview', { params, ...config });
  return response.data;
};

export default {
  getHealth,
  getSummary,
  getExecutions,
  getExecution,
  getDatasets,
  getDependencies,
  getDependencyEvents,
  getDependencyDecisions,
  getForecasts,
  getForecastCategoryTrends,
  getModelComparison,
  getAnalystMetrics,
  getOperationalInventory,
  getOperationalSuppliers,
  getReplenishments,
  deliverReplenishment,
  getOperationalSummary,
  getRecentEvents,
  getAlerts,
  getEventHealth,
  getForecastRecommendations,
  getForecastRecommendationDetail,
  submitApproval,
  getApprovals,
  getApprovalDetail,
  approveReplenishment,
  rejectReplenishment,
  getSimulatorStatus,
  getSimulatorScenarios,
  startSimulator,
  pauseSimulator,
  resumeSimulator,
  stopSimulator,
  getSimulatedDatasets,
  getSimulatedExecutions,
  getSimulatedDecisions,
  runE2ESimulation,
  getSimRuns,
  getSimRunDetail,
  getSimForecasts,
  getSimForecastCategoryTrends,
  getSimReplenishments,
  getSimInventory,
  startOlistReplay,
  pauseOlistReplay,
  resumeOlistReplay,
  stopOlistReplay,
  getOlistReplayStatus,
  previewOlistReplay,
};
