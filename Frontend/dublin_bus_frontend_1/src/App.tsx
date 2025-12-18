import React from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import Navigation from './components/common/Navbar';
import HomePage from './pages/HomePage';
import LiveMapPage from './pages/LiveMapPage';
import VehicleTrackingPage from './pages/VehicleTrackingPage';
import StopArrivalsPage from './pages/StopArrivalsPage';
import PredictionsPage from './pages/PredictionsPage';
import AnalyticsPage from './pages/AnalyticsPage';

const App: React.FC = () => {
  return (
    <Router>
      <div className="App">
        <Navigation />
        <Routes>
          <Route path="/" element={<HomePage />} />
          <Route path="/live-map" element={<LiveMapPage />} />
          <Route path="/vehicle/:vehicleId" element={<VehicleTrackingPage />} />
          <Route path="/stop/:stopId" element={<StopArrivalsPage />} />
          <Route path="/predictions" element={<PredictionsPage />} />
          <Route path="/analytics" element={<AnalyticsPage />} />
        </Routes>
      </div>
    </Router>
  );
};

export default App;
