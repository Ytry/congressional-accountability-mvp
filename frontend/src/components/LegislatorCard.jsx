
import React from 'react';
import { Link } from 'react-router-dom';

function LegislatorCard({ legislator }) {
  return (
    <div style={{ border: '1px solid #ccc', padding: '1rem', margin: '1rem 0' }}>
      <h3>{legislator.full_name}</h3>
      <p>{legislator.party} - {legislator.state}</p>
      <p>District: {legislator.district}</p>
      <Link to={`/legislators/${legislator.id}`}>View Profile</Link>
    </div>
  );
}

export default LegislatorCard;
