import React, { useState, useCallback, useEffect, useRef } from 'react';
import { Form, ListGroup, Spinner, Badge } from 'react-bootstrap';
import { FaSearch, FaBus, FaTimes } from 'react-icons/fa';
import axios from 'axios';
import debounce from 'lodash.debounce';

interface SearchResult {
  vehicle_id: string;
  route_short_name: string;
  route_long_name: string;
  trip_headsign: string;
  latitude: number;
  longitude: number;
  last_seen: string;
}

interface VehicleSearchProps {
  onSelectVehicle: (vehicle: SearchResult) => void;
}

const VehicleSearch: React.FC<VehicleSearchProps> = ({ onSelectVehicle }) => {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState<SearchResult[]>([]);
  const [loading, setLoading] = useState(false);
  const [showResults, setShowResults] = useState(false);
  const searchRef = useRef<HTMLDivElement>(null);

  const API_BASE = process.env.REACT_APP_API_BASE_URL || 'http://localhost:8888';

  // Debounced search function
  const searchVehicles = useCallback(
    debounce(async (searchQuery: string) => {
      if (searchQuery.length < 2) {
        setResults([]);
        setLoading(false);
        return;
      }

      try {
        setLoading(true);
        const response = await axios.get(`${API_BASE}/vehicles/search`, {
          params: { query: searchQuery }
        });

        if (response.data.success) {
          setResults(response.data.results);
          setShowResults(true);
        }
      } catch (error) {
        console.error('Search error:', error);
        setResults([]);
      } finally {
        setLoading(false);
      }
    }, 500),
    []
  );

  useEffect(() => {
    searchVehicles(query);
  }, [query, searchVehicles]);

  // Click outside to close
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (searchRef.current && !searchRef.current.contains(event.target as Node)) {
        setShowResults(false);
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  const handleSelect = (vehicle: SearchResult) => {
    setQuery(vehicle.route_short_name);
    setShowResults(false);
    onSelectVehicle(vehicle);
  };

  const clearSearch = () => {
    setQuery('');
    setResults([]);
    setShowResults(false);
  };

  return (
    <div ref={searchRef} className="position-relative" style={{ maxWidth: '600px' }}>
      <Form.Group className="mb-0">
        <div className="position-relative">
          <FaSearch
            className="position-absolute"
            style={{
              left: '15px',
              top: '50%',
              transform: 'translateY(-50%)',
              color: '#6c757d',
              zIndex: 10
            }}
          />
          <Form.Control
            type="text"
            placeholder="Search by route (e.g., DK05) or vehicle ID..."
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onFocus={() => results.length > 0 && setShowResults(true)}
            style={{
              paddingLeft: '45px',
              paddingRight: '45px',
              backgroundColor: '#2d3250',
              color: '#fff',
              border: 'none',
              borderRadius: '25px',
              fontSize: '1rem'
            }}
          />
          {query && (
            <button
              onClick={clearSearch}
              className="btn btn-link position-absolute"
              style={{
                right: '10px',
                top: '50%',
                transform: 'translateY(-50%)',
                color: '#6c757d',
                zIndex: 10
              }}
            >
              <FaTimes />
            </button>
          )}
          {loading && (
            <Spinner
              animation="border"
              size="sm"
              className="position-absolute"
              style={{
                right: '15px',
                top: '50%',
                transform: 'translateY(-50%)'
              }}
            />
          )}
        </div>
      </Form.Group>

      {showResults && results.length > 0 && (
        <ListGroup
          className="position-absolute w-100 mt-2"
          style={{
            maxHeight: '400px',
            overflowY: 'auto',
            zIndex: 1000,
            backgroundColor: '#2d3250',
            borderRadius: '15px',
            boxShadow: '0 4px 6px rgba(0,0,0,0.3)'
          }}
        >
          {results.map((vehicle, index) => (
            <ListGroup.Item
              key={index}
              action
              onClick={() => handleSelect(vehicle)}
              className="bg-dark text-white border-secondary"
              style={{ cursor: 'pointer' }}
            >
              <div className="d-flex align-items-center justify-content-between">
                <div className="d-flex align-items-center">
                  <div
                    className="rounded d-flex align-items-center justify-content-center me-3"
                    style={{
                      width: '50px',
                      height: '50px',
                      backgroundColor: '#6366f1',
                      fontWeight: 'bold'
                    }}
                  >
                    {vehicle.route_short_name}
                  </div>
                  <div>
                    <h6 className="mb-1">{vehicle.route_long_name}</h6>
                    <small className="text-muted">
                      <FaBus className="me-1" />
                      Vehicle: {vehicle.vehicle_id} • {vehicle.trip_headsign}
                    </small>
                  </div>
                </div>
                <Badge bg="success">Live</Badge>
              </div>
            </ListGroup.Item>
          ))}
        </ListGroup>
      )}

      {showResults && query.length >= 2 && results.length === 0 && !loading && (
        <div
          className="position-absolute w-100 mt-2 p-3 text-center text-muted"
          style={{
            backgroundColor: '#2d3250',
            borderRadius: '15px'
          }}
        >
          No vehicles found for "{query}"
        </div>
      )}
    </div>
  );
};

export default VehicleSearch;
