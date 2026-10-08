import axios from 'axios';

/**
 * Axios instance configured to use Vite proxy.
 * Routes starting with /api and /health are proxied to http://127.0.0.1:8000.
 */
const api = axios.create({
  baseURL: '',
  timeout: 10000,
  headers: {
    'Accept': 'application/json',
  },
});

export const getHealth = async () => {
  const response = await api.get('/health');
  return response.data;
};

export const getSummary = async () => {
  const response = await api.get('/api/observability/summary');
  return response.data;
};

export const getExecutions = async (params = {}) => {
  const response = await api.get('/api/observability/executions', { params });
  return response.data;
};

export const getExecution = async (executionId) => {
  const response = await api.get(`/api/observability/executions/${executionId}`);
  return response.data;
};

export const getDatasets = async () => {
  const response = await api.get('/api/observability/datasets');
  return response.data;
};

export const getDependencies = async () => {
  const response = await api.get('/api/observability/dependencies');
  return response.data;
};

export const getForecasts = async (params = {}) => {
  const response = await api.get('/api/observability/forecasts', { params });
  return response.data;
};

export default {
  getHealth,
  getSummary,
  getExecutions,
  getExecution,
  getDatasets,
  getDependencies,
  getForecasts,
};
