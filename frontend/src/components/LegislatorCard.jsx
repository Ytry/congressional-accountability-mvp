// src/components/LegislatorCard.jsx
import React, { useState } from 'react'
import PropTypes from 'prop-types'
import { Link } from 'react-router-dom'
import placeholder from '../frontend/public/placeholder-portrait.png'

/**
 * Displays a single legislator card with a fast, cached image from the CDN,
 * plus a local placeholder fallback on error.
 */
export default function LegislatorCard({ legislator, size = '225x275' }) {
  const { bioguide_id: id, full_name: name, party, state } = legislator
  const [src, setSrc] = useState(
    `https://cdn.jsdelivr.net/gh/unitedstates/congress-legislators@main/images/congress/${size}/${id}.jpg`
  )
  const [loaded, setLoaded] = useState(false)

  return (
    <Link
      to={`/legislators/${id}`}
      className="group block bg-white border border-gray-200 rounded-lg overflow-hidden shadow hover:shadow-md transition-shadow duration-200"
    >
      <div className="relative w-full h-0 pb-[100%] bg-gray-100">
        {!loaded && <div className="absolute inset-0 animate-pulse bg-gray-200" />}
        <img
          src={src}
          loading="lazy"
          alt={name}
          className={`absolute inset-0 w-full h-full object-cover transition-opacity duration-300 ${
            loaded ? 'opacity-100' : 'opacity-0'
          }`}
          onLoad={() => setLoaded(true)}
          onError={() => {
            setSrc(placeholder)
            setLoaded(true)
          }}
        />
      </div>

      <div className="p-4">
        <h3 className="text-lg font-semibold text-gray-900 group-hover:text-blue-600 transition-colors duration-200">
          {name}
        </h3>
        <p className="mt-1 text-sm text-gray-600">
          {party} &middot; {state}
        </p>
      </div>
    </Link>
  )
}

LegislatorCard.propTypes = {
  legislator: PropTypes.shape({
    bioguide_id: PropTypes.string.isRequired,
    full_name:   PropTypes.string.isRequired,
    party:       PropTypes.string,
    state:       PropTypes.string,
  }).isRequired,
  size: PropTypes.oneOf(['225x275', '450x550', 'original']),
}
