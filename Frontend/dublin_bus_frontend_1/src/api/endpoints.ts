import apiClient from './apiClient';
import { PredictionResponse, RouteInfo } from '../types';

export const api = {
  // Health check
  getHealth: () => apiClient.get('/health'),

  // Get all routes
  getRoutes: () => apiClient.get<{ total_routes: number; routes: RouteInfo[] }>('/routes'),

  // Get predictions for latest rows
  getPredictionsLatest: (limit: number = 10, writeDb: boolean = false) =>
    apiClient.get<PredictionResponse>('/predict/latest', {
      params: { limit, write_db: writeDb },
    }),

  // Get predictions for specific route
  getPredictionsByRoute: (routeShortName: string, limit: number = 10, writeDb: boolean = false) =>
    apiClient.get<PredictionResponse>(`/predict/route/${routeShortName}`, {
      params: { limit, write_db: writeDb },
    }),
};
