import React, { useEffect, useState } from 'react';
import LegislatorCard from './components/LegislatorCard';

function App() {
  const [legislators, setLegislators] = useState([]);

  useEffect(() => {
    fetch(`${import.meta.env.VITE_API_URL}/api/legislators`)
      .then(res => res.json())
      .then(data => setLegislators(data));
  }, []);

  return (
    <div>
      <h1>Congressional Accountability</h1>
      {legislators.map(leg => (
        <LegislatorCard key={leg.id} legislator={leg} />
      ))}
    </div>
  );
}

export default App;
