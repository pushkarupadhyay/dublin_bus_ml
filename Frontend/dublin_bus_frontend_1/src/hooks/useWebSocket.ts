import { useState, useEffect, useRef } from 'react';
import { Vehicle } from '../types';

export const useVehiclesWebSocket = () => {
  const [vehicles, setVehicles] = useState<Vehicle[]>([]);
  const [isConnected, setIsConnected] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimeoutRef = useRef<NodeJS.Timeout | undefined>(undefined);

  useEffect(() => {
    const connect = () => {
      try {
        // Use your vehicle producer as WebSocket server
        // For now, we'll poll the API every 5 seconds
        const interval = setInterval(async () => {
          try {
            // Fetch from MongoDB or your vehicles endpoint
            const response = await fetch('http://localhost:5000/api/vehicles/live');
            const data = await response.json();
            setVehicles(data.vehicles || []);
            setIsConnected(true);
            setError(null);
          } catch (err) {
            console.error('Error fetching vehicles:', err);
            setError('Failed to fetch live vehicles');
            setIsConnected(false);
          }
        }, 5000);

        return () => clearInterval(interval);
      } catch (err) {
        setError('Connection failed');
        setIsConnected(false);
      }
    };

    connect();

    return () => {
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
      }
    };
  }, []);

  return { vehicles, isConnected, error };
};
