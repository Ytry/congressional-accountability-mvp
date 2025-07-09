// src/components/LegislatorCard.jsx
import React, { useState } from 'react';
import PropTypes from 'prop-types';
import { Link } from 'react-router-dom';

export default function LegislatorCard({ legislator }) {
  const {
    bioguide_id: id,
    full_name: name,
    party,
    state,
    portrait_url,
  } = legislator;

  const [imgLoaded, setImgLoaded] = useState(false);

  // Official Bioguide photo as fallback
  const officialSrc = `https://bioguide.congress.gov/bioguide/photo/${id.charAt(0)}/${id}.jpg`;
  // Use provided URL if valid HTTPS, else fallback to official
  const src = portrait_url?.startsWith('https') ? portrait_url : officialSrc;

  return (
    <Link
      to={`/legislators/${id}`}
      className="group block bg-white border border-gray-200 rounded-lg overflow-hidden shadow hover:shadow-md transition-shadow duration-200"
    >
      {/* Image container with aspect ratio and placeholder */}
      <div className="relative w-full h-0 pb-[100%] bg-gray-100">
        {/* Skeleton while loading */}
        {!imgLoaded && <div className="absolute inset-0 animate-pulse bg-gray-200" />}
        <img
          src={src}
          alt={name}
          className={`absolute inset-0 w-full h-full object-cover transition-opacity duration-300 ${
            imgLoaded ? 'opacity-100' : 'opacity-0'
          }`}
          onLoad={() => setImgLoaded(true)}
          onError={(e) => {
            e.currentTarget.onerror = null;
            e.currentTarget.src = '/placeholder-portrait.png';
          }}
        />
      </div>

      {/* Legislator info */}
      <div className="p-4">
        <h3 className="text-lg font-semibold text-gray-900 group-hover:text-blue-600 transition-colors duration-200">
          {name}
        </h3>
        <p className="mt-1 text-sm text-gray-600">
          {party} &middot; {state}
        </p>
      </div>
    </Link>
  );
}

LegislatorCard.propTypes = {
  legislator: PropTypes.shape({
    bioguide_id: PropTypes.string.isRequired,
    full_name: PropTypes.string.isRequired,
    party: PropTypes.string,
    state: PropTypes.string,
    portrait_url: PropTypes.string,
  }).isRequired,
};
