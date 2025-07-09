// src/components/LegislatorCard.jsx
import React from 'react';
import { Link } from 'react-router-dom';

export default function LegislatorCard({ legislator }) {
  const { bioguide_id, full_name, party, state, portrait_url } = legislator;

  // Use a reliable image host (theunitedstates.io) if the stored URL is broken
  const imgSrc = portrait_url && portrait_url.startsWith('http')
    ? portrait_url
    : `https://theunitedstates.io/images/congress/225x275/${bioguide_id}.jpg`;

  return (
    <Link
      to={`/legislators/${bioguide_id}`}
      className="block border rounded-lg overflow-hidden hover:shadow-lg transition-shadow"
    >
      <img
        src={imgSrc}
        alt={full_name}
        className="w-full h-48 object-cover bg-gray-100"
        onError={e => {
          // fallback to a placeholder on load error
          e.currentTarget.onerror = null;
          e.currentTarget.src = '/placeholder-portrait.png';
        }}
      />
      <div className="p-4">
        <h3 className="text-lg font-semibold">{full_name}</h3>
        <p className="text-sm text-gray-600">
          {party} · {state}
        </p>
      </div>
    </Link>
  );
}
