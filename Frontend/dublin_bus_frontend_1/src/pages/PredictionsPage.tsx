import React, { useState, useEffect } from 'react';
import { Container, Row, Col, Card, Form, Button, Badge, Spinner, Alert } from 'react-bootstrap';
import { FaBus, FaChartLine } from 'react-icons/fa';
import { api } from '../api/endpoints';
import { PredictionResponse, RouteInfo } from '../types';

const PredictionsPage: React.FC = () => {
  const [routes, setRoutes] = useState<RouteInfo[]>([]);
  const [selectedRoute, setSelectedRoute] = useState<string>('');
  const [predictions, setPredictions] = useState<PredictionResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchRoutes = async () => {
      try {
        const response = await api.getRoutes();
        setRoutes(response.data.routes);
      } catch (err) {
        console.error('Error fetching routes:', err);
      }
    };

    fetchRoutes();
  }, []);

  const handlePredict = async () => {
    if (!selectedRoute) {
      setError('Please select a route');
      return;
    }

    try {
      setLoading(true);
      setError(null);
      const response = await api.getPredictionsByRoute(selectedRoute, 10, false);
      setPredictions(response.data);
      setLoading(false);
    } catch (err) {
      console.error('Error fetching predictions:', err);
      setError('Failed to fetch predictions. Please try again.');
      setLoading(false);
    }
  };

  const getDelayColor = (delay: number) => {
    if (delay < 60) return 'success';
    if (delay < 300) return 'warning';
    return 'danger';
  };

  return (
    <div style={{ backgroundColor: '#1a1d2e', minHeight: '100vh', paddingTop: '30px' }}>
      <Container>
        <Row className="mb-4">
          <Col>
            <h2 className="text-white mb-3">
              <FaBus className="me-3" />
              Delay Predictions
            </h2>
            <p className="text-muted">
              Select a route to see real-time delay predictions powered by our CatBoost ML model
            </p>
          </Col>
        </Row>

        {/* Route Selection */}
        <Row className="mb-4">
          <Col md={8}>
            <Card bg="dark" text="white" className="shadow-lg border-0">
              <Card.Body>
                <Form>
                  <Row>
                    <Col md={8}>
                      <Form.Group>
                        <Form.Label>Select Route</Form.Label>
                        <Form.Select
                          value={selectedRoute}
                          onChange={(e) => setSelectedRoute(e.target.value)}
                          style={{ backgroundColor: '#2d3250', color: '#fff', border: 'none' }}
                        >
                          <option value="">-- Choose a route --</option>
                          {routes.map((route) => (
                            <option key={route.route_id} value={route.route_short_name}>
                              {route.route_short_name} - {route.route_long_name}
                            </option>
                          ))}
                        </Form.Select>
                      </Form.Group>
                    </Col>
                    <Col md={4} className="d-flex align-items-end">
                      <Button
                        variant="primary"
                        className="w-100"
                        onClick={handlePredict}
                        disabled={loading || !selectedRoute}
                      >
                        {loading ? (
                          <>
                            <Spinner animation="border" size="sm" className="me-2" />
                            Predicting...
                          </>
                        ) : (
                          <>
                            <FaChartLine className="me-2" />
                            Get Predictions
                          </>
                        )}
                      </Button>
                    </Col>
                  </Row>
                </Form>
              </Card.Body>
            </Card>
          </Col>
          <Col md={4}>
            <Card bg="dark" text="white" className="shadow-lg border-0">
              <Card.Body>
                <h6 className="text-muted mb-2">Model Info</h6>
                {predictions && (
                  <>
                    <p className="mb-1">
                      <strong>Version:</strong> {predictions.model_version}
                    </p>
                    <p className="mb-0">
                      <strong>Results:</strong> {predictions.rows.length} predictions
                    </p>
                  </>
                )}
              </Card.Body>
            </Card>
          </Col>
        </Row>

        {/* Error Alert */}
        {error && (
          <Row className="mb-4">
            <Col>
              <Alert variant="danger" dismissible onClose={() => setError(null)}>
                {error}
              </Alert>
            </Col>
          </Row>
        )}

        {/* Predictions Results */}
        {predictions && predictions.rows.length > 0 && (
          <Row>
            <Col>
              <Card bg="dark" text="white" className="shadow-lg border-0">
                <Card.Header className="bg-transparent border-bottom border-secondary">
                  <h5 className="mb-0">
                    {predictions.route_info?.route_short_name} - {predictions.route_info?.route_long_name}
                  </h5>
                  <small className="text-muted">
                    Scored at: {new Date(predictions.scored_at_utc).toLocaleString()}
                  </small>
                </Card.Header>
                <Card.Body className="p-0">
                  <div className="table-responsive">
                    <table className="table table-dark table-hover mb-0">
                      <thead>
                        <tr>
                          <th>Stop Sequence</th>
                          <th>Vehicle ID</th>
                          <th>Predicted Delay</th>
                          <th>Uncertainty (±)</th>
                          <th>Actual Delay</th>
                          <th>Status</th>
                        </tr>
                      </thead>
                      <tbody>
                        {predictions.rows.map((pred) => (
                          <tr key={pred.id}>
                            <td className="fw-bold">#{pred.stop_sequence}</td>
                            <td>{pred.vehicle_id}</td>
                            <td>
                              <Badge bg={getDelayColor(pred.pred_arrival_delay)}>
                                {Math.round(pred.pred_arrival_delay)} sec
                              </Badge>
                            </td>
                            <td className="text-warning">
                              ±{Math.round(pred.pred_std)} sec
                            </td>
                            <td>
                              {pred.actual_arrival_delay !== null && pred.actual_arrival_delay !== undefined
                                ? `${Math.round(pred.actual_arrival_delay)} sec`
                                : 'N/A'}
                            </td>
                            <td>
                              {pred.pred_arrival_delay < 60 ? (
                                <Badge bg="success">On Time</Badge>
                              ) : pred.pred_arrival_delay < 300 ? (
                                <Badge bg="warning" text="dark">Delayed</Badge>
                              ) : (
                                <Badge bg="danger">Very Late</Badge>
                              )}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </Card.Body>
              </Card>
            </Col>
          </Row>
        )}
      </Container>
    </div>
  );
};

export default PredictionsPage;
