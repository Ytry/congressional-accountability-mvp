
import React, { useEffect, useState } from 'react';
import LegislatorCard from './LegislatorCard';

function LegislatorList() {
  const [legislators, setLegislators] = useState([]);

  useEffect(() => {
    fetch(`${import.meta.env.VITE_API_URL}/api/legislators`)
      .then(res => res.json())
      .then(data => setLegislators(data));
  }, []);

  return (
    <div>
      {legislators.map(leg => (
        <LegislatorCard key={leg.id} legislator={leg} />
      ))}
    </div>
  );
}

export default LegislatorList;
