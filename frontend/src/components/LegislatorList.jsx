import React, { useEffect, useState } from 'react';
import LegislatorCard from './LegislatorCard';

function LegislatorList() {
  const [legislators, setLegislators] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch(`${import.meta.env.VITE_API_URL}/api/legislators`)
      .then(res => {
        if (!res.ok) throw new Error(`API error: ${res.status}`);
        return res.json();
      })
      .then(data => setLegislators(data))
      .catch(err => console.error("Fetch error:", err))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <div>Loading legislators...</div>;
  if (legislators.length === 0) return <div>No legislators found.</div>;

  return (
    <div>
      {legislators.map(leg => (
        <LegislatorCard key={leg.id} legislator={leg} />
      ))}
    </div>
  );
}

export default LegislatorList;
