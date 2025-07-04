
import React from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import LegislatorList from './components/LegislatorList';
import LegislatorProfile from './components/LegislatorProfile';

function App() {
  return (
    <Router>
      <Routes>
        <Route path="/" element={<LegislatorList />} />
        <Route path="/legislators/:id" element={<LegislatorProfile />} />
      </Routes>
    </Router>
  );
}

export default App;
