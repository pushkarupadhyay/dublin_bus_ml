import React, { useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { Container, Row, Col, Card, Badge, Button, Spinner } from 'react-bootstrap';
import { FaArrowLeft, FaClock, FaRedo } from 'react-icons/fa';
import { Arrival } from '../types';

const StopArrivalsPage: React.FC = () => {
  const { stopId } = useParams<{ stopId: string }>();
  const navigate = useNavigate();
  const [arrivals, setArrivals] = useState<Arrival[]>([]);
  const [loading, setLoading] = useState(true);
  const [lastUpdate, setLastUpdate] = useState<Date>(new Date());

  const fetchArrivals = async () => {
    try {
      setLoading(true);
      // Mock data - replace with actual API
      const mockArrivals: Arrival[] = [
        {
          route_id: 'E1',
          route_short_name: 'E1',
          destination: 'Northwood',
          scheduled_time: '13:48',
          predicted_time: '13:48',
          delay: 0,
          status: 'DUE',
        },
        {
          route_id: 'E1',
          route_short_name: 'E1',
          destination: 'Northwood',
          scheduled_time: '13:48',
          predicted_time: '13:48',
          delay: 0,
          status: 'DUE',
        },
        {
          route_id: 'E1',
          route_short_name: 'E1',
          destination: 'Northwood',
          scheduled_time: '13:49',
          predicted_time: '13:49',
          delay: 0,
          status: 'DUE',
        },
        {
          route_id: 'E2',
          route_short_name: 'E2',
          destination: 'Harristown',
          scheduled_time: '13:49',
          predicted_time: '13:50',
          delay: 60,
          status: 'LATE',
        },
      ];
      setArrivals(mockArrivals);
      setLastUpdate(new Date());
      setLoading(false);
    } catch (error) {
      console.error('Error fetching arrivals:', error);
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchArrivals();
    const interval = setInterval(fetchArrivals, 10000); // Refresh every 15s

    return () => clearInterval(interval);
  }, [stopId]);

  const getStatusBadge = (status: string, delay: number) => {
    if (status === 'DUE') {
      return <Badge bg="success" className="fs-5 px-3 py-2">Due</Badge>;
    } else if (delay > 0) {
      return <Badge bg="warning" text="dark" className="fs-6 px-3 py-2">{Math.floor(delay / 60)} min</Badge>;
    } else {
      return <Badge bg="success" className="fs-6 px-3 py-2">On Time</Badge>;
    }
  };

  return (
    <div style={{ backgroundColor: '#1a1d2e', minHeight: '100vh' }}>
      {/* Header */}
      <div className="bg-dark py-3 border-bottom border-secondary">
        <Container>
          <div className="d-flex justify-content-between align-items-center">
            <div className="d-flex align-items-center">
              <Button variant="link" className="text-white p-0 me-3" onClick={() => navigate(-1)}>
                <FaArrowLeft size={20} />
              </Button>
              <div>
                <h4 className="mb-0 text-white">Arrivals</h4>
                <small className="text-muted">Stop ID: {stopId}</small>
              </div>
            </div>
            <Button variant="outline-light" size="sm" onClick={fetchArrivals}>
              <FaRedo className="me-2" />
              Refresh
            </Button>
          </div>
        </Container>
      </div>

      {/* Last Updated */}
      <div className="bg-dark border-bottom border-secondary py-2">
        <Container>
          <small className="text-muted">
            Last updated: {lastUpdate.toLocaleTimeString()}
          </small>
        </Container>
      </div>

      <Container className="py-4">
        {loading ? (
          <div className="text-center py-5">
            <Spinner animation="border" variant="light" />
          </div>
        ) : arrivals.length === 0 ? (
          <Card bg="dark" text="white" className="shadow-lg border-0">
            <Card.Body className="text-center py-5">
              <p className="mb-0">No upcoming arrivals at this stop.</p>
            </Card.Body>
          </Card>
        ) : (
          <Row>
            {arrivals.map((arrival, index) => (
              <Col key={index} xs={12} className="mb-3">
                <Card 
                  bg="dark" 
                  text="white" 
                  className="shadow-lg border-0 hover-shadow" 
                  style={{ cursor: 'pointer', transition: 'transform 0.2s' }}
                  onMouseEnter={(e) => e.currentTarget.style.transform = 'scale(1.02)'}
                  onMouseLeave={(e) => e.currentTarget.style.transform = 'scale(1)'}
                >
                  <Card.Body className="p-4">
                    <Row className="align-items-center">
                      <Col xs={2}>
                        <div 
                          className="rounded d-flex align-items-center justify-content-center fw-bold" 
                          style={{ 
                            width: '60px', 
                            height: '60px', 
                            backgroundColor: '#6366f1',
                            fontSize: '1.5rem'
                          }}
                        >
                          {arrival.route_short_name}
                        </div>
                      </Col>
                      <Col xs={6}>
                        <h5 className="mb-1">{arrival.destination}</h5>
                        <small className="text-muted d-flex align-items-center">
                          <FaClock className="me-2" />
                          ETA: {arrival.predicted_time}
                        </small>
                        {arrival.delay > 0 && (
                          <small className="text-warning">
                            ({Math.floor(arrival.delay / 60)} min delay)
                          </small>
                        )}
                      </Col>
                      <Col xs={4} className="text-end">
                        {getStatusBadge(arrival.status, arrival.delay)}
                        <div className="mt-2">
                          <Badge bg="secondary" className="text-uppercase">
                            schedule
                          </Badge>
                        </div>
                      </Col>
                    </Row>
                  </Card.Body>
                </Card>
              </Col>
            ))}
          </Row>
        )}
      </Container>
    </div>
  );
};

export default StopArrivalsPage;
