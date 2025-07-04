
import React, { useEffect, useState } from 'react';
import { useParams } from 'react-router-dom';

function LegislatorProfile() {
  const { id } = useParams();
  const [legislator, setLegislator] = useState(null);

  useEffect(() => {
    fetch(`${import.meta.env.VITE_API_URL}/api/legislators/${id}`)
      .then(res => res.json())
      .then(data => setLegislator(data));
  }, [id]);

  if (!legislator) return <p>Loading profile...</p>;

  return (
    <div>
      <h2>{legislator.full_name}</h2>
      <p>{legislator.party} - {legislator.state} District {legislator.district}</p>
      <p><strong>Committees:</strong> {legislator.committees?.join(", ")}</p>
      <p><strong>Leadership Roles:</strong> {legislator.leadership_roles?.join(", ")}</p>
      <p><strong>Top Donors:</strong> {legislator.campaign_donors?.join(", ")}</p>
    </div>
  );
}

export default LegislatorProfile;
