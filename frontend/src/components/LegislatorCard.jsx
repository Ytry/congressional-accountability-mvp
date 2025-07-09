// src/components/LegislatorCard.jsx
import React from 'react';
import { Link } from 'react-router-dom';

export default function LegislatorCard({ legislator }) {
  // Use bioguide_id to build the URL param 'bioguideId'
  const { bioguide_id, full_name, party, state, portrait_url } = legislator;

  return (
    <Link
      to={`/legislators/${bioguide_id}`}
      className="block border rounded-lg overflow-hidden hover:shadow-lg transition-shadow"
    >
      {portrait_url && (
        <img
          src={portrait_url}
          alt={full_name}
          className="w-full h-48 object-cover"
        />
      )}
      <div className="p-4">
        <h3 className="text-lg font-semibold">{full_name}</h3>
        <p className="text-sm text-gray-600">
          {party} &middot; {state}
        </p>
      </div>
    </Link>
  );
}
