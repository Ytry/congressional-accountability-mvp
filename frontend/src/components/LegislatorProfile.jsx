// src/components/LegislatorProfile.jsx
import React, { useState, useEffect } from 'react'
import { useParams, Link } from 'react-router-dom'
import axios from 'axios'
import placeholder from '../assets/placeholder.png'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5000'

export default function LegislatorProfile() {
  const { bioguide_id: id } = useParams()
  const [legislator, setLegislator] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  useEffect(() => {
    console.log('[Profile] useParams bioguideId =', id)
    if (!id) {
      console.error('[Profile] No legislator ID found in route params')
      setError('Invalid legislator ID')
      setLoading(false)
      return
    }

    setLoading(true)
    axios
      .get(`${API_URL}/api/legislators/${id}`)
      .then(({ data }) => {
        console.log('[Profile] Loaded data for legislator:', data)
        if (!data || Object.keys(data).length === 0) {
          throw new Error('Empty legislator data')
        }
        setLegislator(data)
        setError('')
      })
      .catch(err => {
        console.error('[Profile] Error fetching legislator:', err)
        setError(
          err.response?.status === 404
            ? 'Legislator not found.'
            : 'Failed to load legislator data.'
        )
      })
      .finally(() => setLoading(false))
  }, [id])

  if (loading) {
    return <div className="p-6 text-gray-500">Loading...</div>
  }

  if (error) {
    return (
      <div className="p-6 text-center">
        <h2 className="text-2xl font-bold text-red-600 mb-4">Error</h2>
        <p className="text-lg">{error}</p>
        <Link to="/legislators" className="text-blue-600 hover:underline mt-4 block">
          ← Back to all legislators
        </Link>
      </div>
    )
  }

  if (!legislator || !legislator.first_name || !legislator.last_name) {
    console.warn('[Profile] Incomplete legislator data:', legislator)
    return (
      <div className="p-6 text-center">
        <h2 className="text-2xl font-bold text-yellow-600 mb-4">Incomplete Data</h2>
        <p className="text-lg">The legislator's information appears to be incomplete.</p>
        <Link to="/legislators" className="text-blue-600 hover:underline mt-4 block">
          ← Back to all legislators
        </Link>
      </div>
    )
  }

  const {
    first_name,
    last_name,
    party,
    state,
    chamber,
    district,
    start_year,
    end_year,
    portrait_url,
    bio = '',
    service_history = [],
    committees = [],
    leadership_positions = [],
    sponsored_bills = [],
    finance_summary = {},
    recent_votes = [],
  } = legislator

  return (
    <div className="space-y-8 p-6">
      <Link to="/legislators" className="text-blue-600 hover:underline">
        ← Back to all legislators
      </Link>

      <header className="flex flex-col md:flex-row items-center md:items-start space-y-4 md:space-y-0 md:space-x-6">
        <img
          src={portrait_url ? `${API_URL}${portrait_url}` : placeholder}
          alt={`${first_name} ${last_name}`}
          className="rounded-full w-40 h-40 object-cover shadow"
          onError={e => {
            e.currentTarget.onerror = null
            e.currentTarget.src = placeholder
          }}
        />
        <div>
          <h1 className="text-3xl font-bold">
            {first_name} {last_name}
          </h1>
          <p className="text-lg text-gray-600">
            {party} · {state}
            {chamber === 'house' && district ? ` · District ${district}` : ''}
          </p>
          <p className="text-sm text-gray-500 mt-1">
            Serving {start_year}{end_year ? `–${end_year}` : '–present'} in the {chamber?.toUpperCase()}
          </p>
        </div>
      </header>

      <section className="space-y-6">
        <h2 className="text-2xl font-semibold">About</h2>
        <p>{bio || 'No biographical summary available.'}</p>

        {service_history.length > 0 && (
          <>
            <h3 className="text-xl font-semibold">Service History</h3>
            <ul className="list-disc list-inside">
              {service_history.map((term, i) => (
                <li key={i}>
                  {term.chamber?.charAt(0).toUpperCase() + term.chamber?.slice(1)} (
                  {term.start_date}{term.end_date ? `–${term.end_date}` : '–present'})
                </li>
              ))}
            </ul>
          </>
        )}

        {committees.length > 0 && (
          <>
            <h3 className="text-xl font-semibold">Committee Assignments</h3>
            <table className="w-full text-left border-collapse">
              <thead>
                <tr>
                  <th className="border-b px-2 py-1">Committee</th>
                  <th className="border-b px-2 py-1">Role</th>
                  <th className="border-b px-2 py-1">Since</th>
                </tr>
              </thead>
              <tbody>
                {committees.map(c => (
                  <tr key={c.committee_id}>
                    <td className="px-2 py-1">{c.name}</td>
                    <td className="px-2 py-1">{c.role}</td>
                    <td className="px-2 py-1">{c.since || ''}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </>
        )}

        {/* ... rest of profile sections unchanged ... */}
      </section>
        {Array.isArray(leadership_positions) && leadership_positions.length > 0 && (
          <>
            <h3 className="text-xl font-semibold">Leadership Positions</h3>
            <ul className="list-disc list-inside">
              {leadership_positions.map((pos, i) => (
                <li key={i}>
                  {pos.title} ({pos.start_date}
                  {pos.end_date ? `–${pos.end_date}` : '–present'})
                </li>
              ))}
            </ul>
          </>
        )}
      </section>

      <section className="space-y-4">
        <h2 className="text-2xl font-semibold">Sponsored Bills</h2>
        {Array.isArray(sponsored_bills) && sponsored_bills.length ? (
          <ul className="list-disc list-inside">
            {sponsored_bills.map(b => (
              <li key={b.bill_id}>
                <Link to={`/bills/${b.bill_id}`} className="text-blue-600 hover:underline">
                  {b.bill_id}: {b.title}
                </Link>{' '}
                ({b.date}, {b.status})
              </li>
            ))}
          </ul>
        ) : (
          <p>No sponsored bills found.</p>
        )}
      </section>

      <section className="space-y-4">
        <h2 className="text-2xl font-semibold">Campaign Finance</h2>
        {finance_summary.total_contributions ? (
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div className="border rounded p-4 shadow-sm">
              <h3 className="font-semibold">Total Contributions</h3>
              <p className="text-xl">
                ${finance_summary.total_contributions.toLocaleString()}
              </p>
              <p className="text-sm text-gray-600">Cycle: {finance_summary.cycle}</p>
            </div>
            <div className="border rounded p-4 shadow-sm">
              <h3 className="font-semibold">Top Industries</h3>
              <ul className="list-disc list-inside">
                {finance_summary.top_industries.map((ind, i) => (
                  <li key={i}>
                    {ind.industry}: ${ind.amount.toLocaleString()}
                  </li>
                ))}
              </ul>
            </div>
          </div>
        ) : (
          <p>No finance data available.</p>
        )}
      </section>

      <section className="space-y-4">
        <h2 className="text-2xl font-semibold">Recent Votes</h2>
        {Array.isArray(recent_votes) && recent_votes.length ? (
          <table className="w-full text-left border-collapse">
            <thead>
              <tr>
                <th className="border-b px-2 py-1">Date</th>
                <th className="border-b px-2 py-1">Bill</th>
                <th className="border-b px-2 py-1">Position</th>
              </tr>
            </thead>
            <tbody>
              {recent_votes.map(v => (
                <tr key={v.vote_id}>
                  <td className="px-2 py-1">{v.date}</td>
                  <td className="px-2 py-1">{v.bill}</td>
                  <td
                    className={`px-2 py-1 font-semibold ${
                      v.position === 'Yea'
                        ? 'text-green-600'
                        : v.position === 'Nay'
                        ? 'text-red-600'
                        : 'text-gray-600'
                    }`}
                  >
                    {v.position}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <p>No recent votes recorded.</p>
        )}
      </section>
    </div>
  );
}
