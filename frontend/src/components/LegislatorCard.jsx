// src/components/LegislatorCard.jsx
import React, { useState } from 'react';
import PropTypes from 'prop-types';
import { Link } from 'react-router-dom';

export default function LegislatorCard({ legislator, size = '225x275' }) {
  const { bioguide_id: id, full_name: name, party, state, portrait_url } = legislator;
  const [loaded, setLoaded] = useState(false);

  // 1) Try your API’s URL if it’s HTTPS
  // 2) Else fetch from the official GitHub-Pages host at the chosen size
  const fallback = `https://unitedstates.github.io/images/congress/${size}/${id}.jpg`;
  const src      = portrait_url?.startsWith('https') ? portrait_url : fallback;

  return (
    <Link to={`/legislators/${id}`} className="group block ...">
      <div className="relative w-full h-0 pb-[100%] bg-gray-100">
        {!loaded && <div className="absolute inset-0 animate-pulse bg-gray-200" />}
        <img
          src={src}
          alt={name}
          className={`absolute inset-0 w-full h-full object-cover transition-opacity ${
            loaded ? 'opacity-100' : 'opacity-0'
          }`}
          onLoad={() => setLoaded(true)}
          onError={e => {
            e.currentTarget.onerror = null;
            // final fallback: local placeholder
            e.currentTarget.src = '/placeholder-portrait.png';
          }}
        />
      </div>
      <div className="p-4">
        <h3 className="text-lg font-semibold">{name}</h3>
        <p className="text-sm text-gray-600">{party} · {state}</p>
      </div>
    </Link>
  );
}

LegislatorCard.propTypes = {
  legislator: PropTypes.shape({
    bioguide_id: PropTypes.string.isRequired,
    full_name:   PropTypes.string.isRequired,
    party:       PropTypes.string,
    state:       PropTypes.string,
    portrait_url: PropTypes.string,
  }).isRequired,
  size: PropTypes.oneOf(['225x275', '450x550', 'original']),
};
