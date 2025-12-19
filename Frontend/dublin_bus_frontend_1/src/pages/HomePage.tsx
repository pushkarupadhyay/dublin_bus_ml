import React, { useState, useEffect } from 'react';
import { Container, Row, Col, Button, Form, Card, ListGroup, Spinner, Badge } from 'react-bootstrap';
import { useNavigate } from 'react-router-dom';
import { MapContainer, TileLayer, Marker, Popup, Circle } from 'react-leaflet';
import { FaMapMarkedAlt, FaSearch, FaBus } from 'react-icons/fa';
import L from 'leaflet';
import axios from 'axios';
import { Stop } from '../types';

// Fix Leaflet default icon issue
delete (L.Icon.Default.prototype as any)._getIconUrl;
L.Icon.Default.mergeOptions({
  iconRetinaUrl: require('leaflet/dist/images/marker-icon-2x.png'),
  iconUrl: require('leaflet/dist/images/marker-icon.png'),
  shadowUrl: require('leaflet/dist/images/marker-shadow.png'),
});

// API Base URL
const API_BASE = process.env.REACT_APP_API_BASE_URL || 'http://localhost:8888';

interface RouteSearchResult {
  route_id: string;
  route_short_name: string;
  route_long_name: string;
  active_vehicles: number;
}

const HomePage: React.FC = () => {
  const navigate = useNavigate();
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState<RouteSearchResult[]>([]);
  const [showSearchResults, setShowSearchResults] = useState(false);
  const [searching, setSearching] = useState(false);
  
  const [nearbyStops, setNearbyStops] = useState<Stop[]>([]);
  const [userLocation, setUserLocation] = useState<[number, number]>([53.3498, -6.2603]);
  const [loadingLocation, setLoadingLocation] = useState(true);
  const [loadingStops, setLoadingStops] = useState(false);

  useEffect(() => {
    // Get user location
    if (navigator.geolocation) {
      navigator.geolocation.getCurrentPosition(
        (position) => {
          const lat = position.coords.latitude;
          const lng = position.coords.longitude;
          setUserLocation([lat, lng]);
          setLoadingLocation(false);
          // Fetch nearby stops from API
          fetchNearbyStops(lat, lng);
        },
        (error) => {
          console.error('Error getting location:', error);
          setLoadingLocation(false);
          // Use default Dublin location if geolocation fails
          fetchNearbyStops(53.3498, -6.2603);
        }
      );
    } else {
      setLoadingLocation(false);
      fetchNearbyStops(53.3498, -6.2603);
    }
  }, []);

  const fetchNearbyStops = async (lat: number, lng: number) => {
    setLoadingStops(true);
    try {
      const response = await axios.get(`${API_BASE}/stops/nearby`, {
        params: {
          lat: lat,
          lng: lng,
          radius: 500
        }
      });

      if (response.data.success) {
        setNearbyStops(response.data.stops);
        console.log(`✅ Found ${response.data.stops.length} nearby stops`);
      }
    } catch (error) {
      console.error('❌ Error fetching nearby stops:', error);
      // Fallback to empty array if API fails
      setNearbyStops([]);
    } finally {
      setLoadingStops(false);
    }
  };

  // Search for routes by route_short_name
  const handleSearchInput = async (query: string) => {
    setSearchQuery(query);

    if (query.trim().length < 1) {
      setSearchResults([]);
      setShowSearchResults(false);
      return;
    }

    setSearching(true);
    setShowSearchResults(true);

    try {
      const response = await axios.get(`${API_BASE}/routes/search`, {
        params: { query: query.trim() }
      });

      if (response.data.success) {
        setSearchResults(response.data.routes);
        console.log(`✅ Found ${response.data.routes.length} routes for "${query}"`);
      }
    } catch (error) {
      console.error('❌ Error searching routes:', error);
      setSearchResults([]);
    } finally {
      setSearching(false);
    }
  };

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault();
    if (searchQuery.trim()) {
      // Navigate to live map with route filter
      navigate(`/live-map?route=${searchQuery.trim()}`);
    }
  };

  const handleSelectRoute = (route: RouteSearchResult) => {
    setShowSearchResults(false);
    setSearchQuery('');
    // Navigate to live map filtered by this route
    navigate(`/live-map?route=${route.route_short_name}`);
  };

  return (
    <div style={{ backgroundColor: '#1a1d2e', minHeight: '100vh', paddingTop: '30px' }}>
      <Container>
        {/* Hero Section */}
        <Row className="text-center mb-5">
          <Col>
            <h1 className="display-4 fw-bold text-white mb-3">Dublin Bus</h1>
            <p className="lead text-secondary mb-4">Real-time arrivals and tracking</p>
            <Button
              variant="primary"
              size="lg"
              className="rounded-pill px-5 py-3"
              onClick={() => navigate('/live-map')}
            >
              <FaMapMarkedAlt className="me-2" />
              View Live Bus Map
            </Button>
          </Col>
        </Row>

        {/* Search Section */}
        <Row className="justify-content-center mb-5">
          <Col md={8}>
            <Card bg="dark" text="white" className="shadow-lg border-0">
              <Card.Body className="p-4">
                <Form onSubmit={handleSearch}>
                  <Form.Group>
                    <div className="position-relative">
                      <FaSearch 
                        className="position-absolute" 
                        style={{ left: '15px', top: '50%', transform: 'translateY(-50%)', color: '#6c757d', zIndex: 10 }} 
                      />
                      <Form.Control
                        type="text"
                        placeholder="Search by route (e.g., 46A, 123, 145)..."
                        value={searchQuery}
                        onChange={(e) => handleSearchInput(e.target.value)}
                        size="lg"
                        className="ps-5"
                        style={{ 
                          backgroundColor: '#2d3250', 
                          border: 'none', 
                          color: '#fff',
                          borderRadius: '50px'
                        }}
                      />
                      {searching && (
                        <Spinner
                          animation="border"
                          size="sm"
                          className="position-absolute"
                          style={{
                            right: '15px',
                            top: '50%',
                            transform: 'translateY(-50%)',
                            color: '#6366f1'
                          }}
                        />
                      )}
                    </div>
                  </Form.Group>
                </Form>

                {/* Search Results Dropdown */}
                {showSearchResults && (
                  <div className="mt-3">
                    {searchResults.length > 0 ? (
                      <ListGroup style={{ maxHeight: '300px', overflowY: 'auto' }}>
                        {searchResults.map((route) => (
                          <ListGroup.Item
                            key={route.route_id}
                            action
                            onClick={() => handleSelectRoute(route)}
                            className="d-flex justify-content-between align-items-center"
                            style={{
                              backgroundColor: '#2d3250',
                              border: '1px solid #3d4260',
                              color: '#fff',
                              cursor: 'pointer'
                            }}
                          >
                            <div className="d-flex align-items-center">
                              <div
                                className="rounded d-flex align-items-center justify-content-center me-3"
                                style={{
                                  width: '45px',
                                  height: '45px',
                                  backgroundColor: '#10b981',
                                  fontWeight: 'bold',
                                  fontSize: '0.9rem'
                                }}
                              >
                                {route.route_short_name}
                              </div>
                              <div>
                                <strong>{route.route_long_name}</strong>
                                <br />
                                <small className="text-muted">
                                  <FaBus className="me-1" />
                                  {route.active_vehicles} active bus{route.active_vehicles !== 1 ? 'es' : ''}
                                </small>
                              </div>
                            </div>
                            <Badge bg="success">Live</Badge>
                          </ListGroup.Item>
                        ))}
                      </ListGroup>
                    ) : searching ? (
                      <div className="text-center text-muted py-3">
                        <Spinner animation="border" size="sm" className="me-2" />
                        Searching...
                      </div>
                    ) : (
                      <div className="text-center text-muted py-3">
                        No routes found for "{searchQuery}"
                      </div>
                    )}
                  </div>
                )}
              </Card.Body>
            </Card>
          </Col>
        </Row>

        {/* Map Section */}
        <Row className="mb-4">
          <Col>
            <Card bg="dark" text="white" className="shadow-lg border-0">
              <Card.Body className="p-0">
                <div style={{ height: '500px', borderRadius: '15px', overflow: 'hidden' }}>
                  {loadingLocation ? (
                    <div className="d-flex justify-content-center align-items-center h-100 bg-secondary">
                      <Spinner animation="border" variant="light" />
                    </div>
                  ) : (
                    <MapContainer
                      center={userLocation}
                      zoom={14}
                      style={{ height: '100%', width: '100%' }}
                    >
                      <TileLayer
                        url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
                        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
                      />
                      <Circle center={userLocation} radius={500} pathOptions={{ color: '#6366f1', fillColor: '#6366f1', fillOpacity: 0.2 }} />
                      <Marker position={userLocation}>
                        <Popup>Your Location</Popup>
                      </Marker>
                      {nearbyStops.map((stop) => (
                        <Marker key={stop.stop_id} position={[stop.latitude, stop.longitude]}>
                          <Popup>
                            <strong>{stop.stop_name}</strong>
                            <br />
                            Stop ID: {stop.stop_id}
                            <br />
                            {stop.distance}m away
                          </Popup>
                        </Marker>
                      ))}
                    </MapContainer>
                  )}
                </div>
              </Card.Body>
            </Card>
          </Col>
        </Row>

        {/* Stops Near You */}
        <Row>
          <Col>
            <Card bg="dark" text="white" className="shadow-lg border-0">
              <Card.Header className="bg-transparent border-bottom border-secondary">
                <h5 className="mb-0">📍 Stops Near By You (within 500m)</h5>
              </Card.Header>
              <Card.Body>
                {loadingStops ? (
                  <div className="text-center py-4">
                    <Spinner animation="border" variant="light" />
                    <p className="text-muted mt-2">Finding nearby stops...</p>
                  </div>
                ) : nearbyStops.length > 0 ? (
                  nearbyStops.map((stop) => (
                    <Card
                      key={stop.stop_id}
                      bg="secondary"
                      text="white"
                      className="mb-3 cursor-pointer hover-shadow"
                      onClick={() => navigate(`/stops/${stop.stop_id}`)}
                      style={{ cursor: 'pointer', transition: 'transform 0.2s' }}
                      onMouseEnter={(e) => e.currentTarget.style.transform = 'scale(1.02)'}
                      onMouseLeave={(e) => e.currentTarget.style.transform = 'scale(1)'}
                    >
                      <Card.Body className="d-flex justify-content-between align-items-center">
                        <div>
                          <h6 className="mb-1">{stop.stop_name}</h6>
                          <small className="text-muted">Stop ID: {stop.stop_id}</small>
                        </div>
                        <div className="text-end">
                          <span className="badge bg-primary">{stop.distance}m</span>
                          <br />
                          <small className="text-muted">away</small>
                        </div>
                      </Card.Body>
                    </Card>
                  ))
                ) : (
                  <div className="text-center text-muted py-4">
                    <p>No stops found within 500m</p>
                    <small>Try moving closer to a bus stop</small>
                  </div>
                )}
              </Card.Body>
            </Card>
          </Col>
        </Row>
      </Container>
    </div>
  );
};

export default HomePage;
