import React from 'react';

function LegislatorCard({ legislator }) {
  return (
    <div style={{ border: '1px solid black', padding: '1rem', marginBottom: '1rem' }}>
      <h2>{legislator.full_name}</h2>
      <p>{legislator.party} - {legislator.state}</p>
      <p>District: {legislator.district}</p>
    </div>
  );
}

export default LegislatorCard;