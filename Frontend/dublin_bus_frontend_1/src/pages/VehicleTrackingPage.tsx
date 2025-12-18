import React, { useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { Container, Row, Col, Card, Badge, Button, ListGroup, Spinner } from 'react-bootstrap';
import { MapContainer, TileLayer, Marker, Polyline } from 'react-leaflet';
import { FaArrowLeft, FaBus } from 'react-icons/fa';
import L from 'leaflet';
import { Vehicle } from '../types';

const vehicleIcon = new L.Icon({
  iconUrl: 'data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSIzMiIgaGVpZ2h0PSIzMiIgdmlld0JveD0iMCAwIDMyIDMyIj48Y2lyY2xlIGN4PSIxNiIgY3k9IjE2IiByPSIxNCIgZmlsbD0iIzEwYjk4MSIvPjx0ZXh0IHg9IjUwJSIgeT0iNTAlIiBmb250LXNpemU9IjE4IiBmaWxsPSJ3aGl0ZSIgdGV4dC1hbmNob3I9Im1pZGRsZSIgZHk9Ii4zZW0iPkI8L3RleHQ+PC9zdmc+',
  iconSize: [40, 40],
  iconAnchor: [20, 20],
});

interface VehicleDetails extends Vehicle {
  bearing?: number;
  speed?: number;
  route_name?: string;
  destination?: string;
}

const VehicleTrackingPage: React.FC = () => {
  const { vehicleId } = useParams<{ vehicleId: string }>();
  const navigate = useNavigate();
  const [vehicle, setVehicle] = useState<VehicleDetails | null>(null);
  const [loading, setLoading] = useState(true);
  const [vehicleHistory, setVehicleHistory] = useState<[number, number][]>([]);

  useEffect(() => {
    const fetchVehicleData = async () => {
      try {
        setLoading(true);
        // Mock data - replace with actual API
        const mockVehicle: VehicleDetails = {
          entity_id: '5240_1273',
          trip_id: '5240_1273',
          route_id: '5240_119662',
          vehicle_id: vehicleId || '5',
          latitude: 53.34972,
          longitude: -6.24571,
          speed: 25,
          bearing: 180,
          timestamp: new Date().toISOString(),
          status: 'ON_TIME',
          route_name: 'Route 220',
          destination: 'City Centre',
        };
        setVehicle(mockVehicle);
        setVehicleHistory([
          [53.3497, -6.2457],
          [53.3495, -6.2460],
          [53.3493, -6.2463],
        ]);
        setLoading(false);
      } catch (error) {
        console.error('Error fetching vehicle:', error);
        setLoading(false);
      }
    };

    fetchVehicleData();
    const interval = setInterval(fetchVehicleData, 5000);

    return () => clearInterval(interval);
  }, [vehicleId]);

  if (loading || !vehicle) {
    return (
      <div className="d-flex justify-content-center align-items-center" style={{ height: '100vh', backgroundColor: '#1a1d2e' }}>
        <Spinner animation="border" variant="light" />
      </div>
    );
  }

  return (
    <div style={{ backgroundColor: '#1a1d2e', minHeight: '100vh' }}>
      {/* Header */}
      <div className="bg-dark py-3 border-bottom border-secondary">
        <Container>
          <div className="d-flex align-items-center">
            <Button variant="link" className="text-white p-0 me-3" onClick={() => navigate(-1)}>
              <FaArrowLeft size={20} />
            </Button>
            <div>
              <h4 className="mb-0 text-white">Live Tracking</h4>
              <small className="text-muted">Trip: {vehicle.trip_id}</small>
              <br />
              <small className="text-muted">Route: {vehicle.route_id}</small>
            </div>
            <Badge bg="success" className="ms-auto">
              Live
            </Badge>
          </div>
        </Container>
      </div>

      <Container className="py-4">
        <Row>
          {/* Map Section */}
          <Col lg={8} className="mb-4">
            <Card bg="dark" text="white" className="shadow-lg border-0">
              <Card.Body className="p-0">
                <div style={{ height: '400px', borderRadius: '15px', overflow: 'hidden' }}>
                  <MapContainer
                    center={[vehicle.latitude, vehicle.longitude]}
                    zoom={15}
                    style={{ height: '100%', width: '100%' }}
                  >
                    <TileLayer url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png" />
                    <Marker position={[vehicle.latitude, vehicle.longitude]} icon={vehicleIcon} />
                    {vehicleHistory.length > 1 && (
                      <Polyline positions={vehicleHistory} pathOptions={{ color: '#10b981', weight: 3 }} />
                    )}
                  </MapContainer>
                </div>
              </Card.Body>
            </Card>
          </Col>

          {/* Vehicle Status */}
          <Col lg={4} className="mb-4">
            <Card bg="dark" text="white" className="shadow-lg border-0">
              <Card.Header className="bg-transparent border-bottom border-secondary">
                <h5 className="mb-0">
                  <FaBus className="me-2" />
                  VEHICLE STATUS
                </h5>
              </Card.Header>
              <Card.Body>
                <ListGroup variant="flush" className="bg-transparent">
                  <ListGroup.Item className="bg-transparent text-white border-secondary d-flex justify-content-between">
                    <span className="text-muted">Vehicle ID</span>
                    <strong>{vehicle.vehicle_id}</strong>
                  </ListGroup.Item>
                  <ListGroup.Item className="bg-transparent text-white border-secondary d-flex justify-content-between">
                    <span className="text-muted">Route</span>
                    <strong>{vehicle.route_id}</strong>
                  </ListGroup.Item>
                  <ListGroup.Item className="bg-transparent text-white border-secondary d-flex justify-content-between">
                    <span className="text-muted">Speed</span>
                    <strong>{vehicle.speed ? `${vehicle.speed} km/h` : 'N/A'}</strong>
                  </ListGroup.Item>
                  <ListGroup.Item className="bg-transparent text-white border-secondary d-flex justify-content-between">
                    <span className="text-muted">Position</span>
                    <strong>{vehicle.latitude.toFixed(5)}, {vehicle.longitude.toFixed(5)}</strong>
                  </ListGroup.Item>
                  <ListGroup.Item className="bg-transparent text-white border-secondary d-flex justify-content-between">
                    <span className="text-muted">Bearing</span>
                    <strong>{vehicle.bearing ? `${vehicle.bearing}°` : 'N/A'}</strong>
                  </ListGroup.Item>
                  <ListGroup.Item className="bg-transparent text-white border-secondary d-flex justify-content-between">
                    <span className="text-muted">Last Update</span>
                    <strong>{new Date(vehicle.timestamp).toLocaleTimeString()}</strong>
                  </ListGroup.Item>
                </ListGroup>
              </Card.Body>
            </Card>
          </Col>
        </Row>

        {/* ETA Predictions Section */}
        <Row>
          <Col>
            <Card bg="dark" text="white" className="shadow-lg border-0">
              <Card.Body className="text-center py-5">
                <p className="text-muted mb-0">
                  No stop selected. Go back and select an arrival to see ETA predictions.
                </p>
              </Card.Body>
            </Card>
          </Col>
        </Row>
      </Container>
    </div>
  );
};

export default VehicleTrackingPage;
