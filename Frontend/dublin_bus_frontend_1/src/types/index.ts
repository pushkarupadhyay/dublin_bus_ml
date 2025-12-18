export interface Vehicle {
  entity_id: string;
  trip_id: string;
  route_id: string;
  vehicle_id: string;
  latitude: number;
  longitude: number;
  bearing?: number;
  speed?: number;
  timestamp: string;
  status?: 'ON_TIME' | 'DELAYED' | 'EARLY';
}

export interface Stop {
  stop_id: string;
  stop_name: string;
  latitude: number;
  longitude: number;
  distance?: number;
}

export interface Arrival {
  route_id: string;
  route_short_name: string;
  destination: string;
  scheduled_time: string;
  predicted_time: string;
  delay: number;
  status: 'DUE' | 'LATE' | 'ON_TIME';
}

export interface Prediction {
  id: number;
  route_id: string;
  vehicle_id: string;
  stop_sequence: number;
  latitude: number;
  longitude: number;
  pred_arrival_delay: number;
  pred_std: number;
  actual_arrival_delay?: number;
}

export interface PredictionResponse {
  model_version: string;
  scored_at_utc: string;
  route_info?: {
    route_id: string;
    route_short_name: string;
    route_long_name: string;
  };
  rows: Prediction[];
}

export interface RouteInfo {
  route_id: string;
  route_short_name: string;
  route_long_name: string;
}
