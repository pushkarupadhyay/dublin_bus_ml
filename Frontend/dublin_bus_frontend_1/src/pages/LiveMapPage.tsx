import React, { useState, useEffect } from 'react';
import { Container, Badge, Button, Spinner, Alert } from 'react-bootstrap';
import { MapContainer, TileLayer, Marker, Popup, useMap } from 'react-leaflet';
import { useNavigate } from 'react-router-dom';
import L from 'leaflet';
import { Vehicle } from '../types';
import { FaBus, FaArrowLeft } from 'react-icons/fa';

/*Custom Bus Icon */
const busIcon = new L.Icon({
  iconUrl:
    'data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSIyNCIgaGVpZ2h0PSIyNCIgdmlld0JveD0iMCAwIDI0IDI0IiBmaWxsPSIjMTBiOTgxIj48Y2lyY2xlIGN4PSIxMiIgY3k9IjEyIiByPSIxMCIvPjx0ZXh0IHg9IjUwJSIgeT0iNTAlIiBmb250LXNpemU9IjE0IiBmaWxsPSJ3aGl0ZSIgdGV4dC1hbmNob3I9Im1pZGRsZSIgZHk9Ii4zZW0iPkI8L3RleHQ+PC9zdmc+',
  iconSize: [32, 32],
  iconAnchor: [16, 16],
  popupAnchor: [0, -16],
});

/*Map Controller
 Auto-fit */
const MapController: React.FC<{ vehicles: Vehicle[] }> = ({ vehicles }) => {
  const map = useMap();
  const [hasCentered, setHasCentered] = useState(false);

  useEffect(() => {
    if (!hasCentered && vehicles.length > 0) {
      const bounds = L.latLngBounds(
        vehicles.map((v) => [v.latitude, v.longitude])
      );

      map.fitBounds(bounds, { padding: [50, 50] });
      setHasCentered(true); // Prevent future auto-zoom
    }
  }, [vehicles, map, hasCentered]);

  return null;
};

/*Live Map Page */
const LiveMapPage: React.FC = () => {
  const navigate = useNavigate();
  const [vehicles, setVehicles] = useState<Vehicle[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [lastUpdate, setLastUpdate] = useState<Date>(new Date());

  /* Fetch Vehicles at 10s */
  useEffect(() => {
    const fetchVehicles = async () => {
      try {
        setLoading(true);
        const response = await fetch('http://localhost:5000/api/vehicles/all');
        const data = await response.json();

        setVehicles(data.vehicles || []);
        setLastUpdate(new Date());
        setError(null);
      } catch (err) {
        console.error(err);
        setError('Failed to fetch live vehicles');
      } finally {
        setLoading(false);
      }
    };

    fetchVehicles();
    const interval = setInterval(fetchVehicles, 10000);

    return () => clearInterval(interval);
  }, []);

  return (
    <div style={{ backgroundColor: '#1a1d2e', minHeight: '100vh' }}>
      {/* header */}
      <div className="bg-dark py-3 border-bottom border-secondary">
        <Container>
          <div className="d-flex justify-content-between align-items-center">
            <div className="d-flex align-items-center">
              <Button
                variant="link"
                className="text-white p-0 me-3"
                onClick={() => navigate(-1)}
              >
                <FaArrowLeft size={20} />
              </Button>
              <h4 className="mb-0 text-white">Live Bus Map</h4>
            </div>

            <div className="d-flex align-items-center gap-3">
              <Badge bg="success" className="d-flex align-items-center gap-2">
                <span
                  className="rounded-circle bg-white"
                  style={{ width: '8px', height: '8px' }}
                />
                Live
              </Badge>
              <span className="text-white fw-bold">
                {vehicles.length} buses
              </span>
            </div>
          </div>
        </Container>
      </div>

      {/* map */}
      <div style={{ height: 'calc(100vh - 120px)' }}>
        {loading && vehicles.length === 0 ? (
          <div className="d-flex justify-content-center align-items-center h-100">
            <div className="text-center">
              <Spinner animation="border" variant="light" />
              <p className="text-white mt-3">Loading live vehicles...</p>
            </div>
          </div>
        ) : error ? (
          <Container className="py-5">
            <Alert variant="danger">{error}</Alert>
          </Container>
        ) : (
          <MapContainer
            center={[53.3498, -6.2603]}
            zoom={12}
            scrollWheelZoom={true}
            style={{ height: '100%', width: '100%' }}
          >
            <TileLayer
              url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
              attribution="&copy; OpenStreetMap"
            />

            <MapController vehicles={vehicles} />

            {vehicles.map((vehicle) => (
              <Marker
                key={vehicle.vehicle_id}
                position={[vehicle.latitude, vehicle.longitude]}
                icon={busIcon}
              >
                <Popup>
                  <div style={{ minWidth: '200px' }}>
                    <h6 className="mb-2">
                      <FaBus className="me-2" />
                      Vehicle {vehicle.vehicle_id}
                    </h6>
                    <p className="mb-1">
                      <strong>Route:</strong> {vehicle.route_id}
                    </p>
                    <p className="mb-1">
                      <strong>Trip:</strong> {vehicle.trip_id}
                    </p>
                    {vehicle.speed && (
                      <p className="mb-1">
                        <strong>Speed:</strong> {vehicle.speed} km/h
                      </p>
                    )}
                    <Button
                      variant="primary"
                      size="sm"
                      className="w-100 mt-2"
                      onClick={() =>
                        navigate(`/vehicle/${vehicle.vehicle_id}`)
                      }
                    >
                      Track Vehicle
                    </Button>
                  </div>
                </Popup>
              </Marker>
            ))}
          </MapContainer>
        )}
      </div>

      {/*footer file*/}
      <div className="bg-dark py-2 border-top border-secondary">
        <Container>
          <div className="d-flex justify-content-between align-items-center">
            <small className="text-muted">
              Last updated: {lastUpdate.toLocaleTimeString()}
            </small>
            <small className="text-muted">Waiting for updates...</small>
          </div>
        </Container>
      </div>
    </div>
  );
};

export default LiveMapPage;
