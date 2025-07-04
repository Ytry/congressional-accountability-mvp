import React, { useEffect, useState } from 'react';
import LegislatorCard from './components/LegislatorCard';

function App() {
  const [legislators, setLegislators] = useState([]);

  useEffect(() => {
    fetch('http://localhost:3000/api/legislators')
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
