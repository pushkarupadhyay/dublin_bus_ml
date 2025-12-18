import React from 'react';
import { Link } from 'react-router-dom';
import { Navbar, Nav, Container } from 'react-bootstrap';
import { FaBus, FaMapMarkedAlt, FaChartLine } from 'react-icons/fa';

const Navigation: React.FC = () => {
  return (
    <Navbar bg="dark" variant="dark" expand="lg" sticky="top" className="shadow-sm">
      <Container>
        <Navbar.Brand as={Link} to="/">
          <FaBus className="me-2" size={24} />
          Dublin Bus Tracker
        </Navbar.Brand>
        <Navbar.Toggle aria-controls="basic-navbar-nav" />
        <Navbar.Collapse id="basic-navbar-nav">
          <Nav className="ms-auto">
            <Nav.Link as={Link} to="/">
              Home
            </Nav.Link>
            <Nav.Link as={Link} to="/live-map">
              <FaMapMarkedAlt className="me-1" />
              Live Map
            </Nav.Link>
            <Nav.Link as={Link} to="/predictions">
              <FaBus className="me-1" />
              Predictions
            </Nav.Link>
            <Nav.Link as={Link} to="/analytics">
              <FaChartLine className="me-1" />
              Analytics
            </Nav.Link>
          </Nav>
        </Navbar.Collapse>
      </Container>
    </Navbar>
  );
};

export default Navigation;
