// src/components/LegislatorProfile.jsx
import React, { useState, useEffect } from 'react';
import { useParams, Link } from 'react-router-dom';
import axios from 'axios';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5000';

export default function LegislatorProfile() {
  const { bioguideId } = useParams();
  const [legislator, setLegislator] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  useEffect(() => {
    setLoading(true);
    axios
      .get(`${API_URL}/api/legislators/${bioguideId}`)
      .then(({ data }) => {
        setLegislator(data);
        setError('');
      })
      .catch(err => {
        console.error(err);
        setError(
          err.response?.status === 404
            ? 'Legislator not found.'
            : 'Failed to load legislator data.'
        );
      })
      .finally(() => setLoading(false));
  }, [bioguideId]);

  if (loading) {
    return (
      <div className="p-4 animate-pulse">
        <div className="h-6 bg-gray-300 mb-4 rounded w-1/3"></div>
        <div className="h-64 bg-gray-200 rounded mb-6"></div>
        <div className="space-y-2">
          {[...Array(6)].map((_, i) => (
            <div key={i} className="h-4 bg-gray-200 rounded"></div>
          ))}
        </div>
      </div>
    );
  }

  if (error) {
    return <p className="p-4 text-red-600">{error}</p>;
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
    service_history = [],
    committees = [],
    leadership_positions = [],
    sponsored_bills = [],
    finance_summary = {},
    recent_votes = [],
  } = legislator;

  return (
    <div className="space-y-8 p-6">
      {/* Back link */}
      <Link to="/" className="text-blue-600 hover:underline">
        ← Back to all legislators
      </Link>

      {/* Header */}
      <div className="flex flex-col md:flex-row items-center md:items-start space-y-4 md:space-y-0 md:space-x-6">
        <img
          src={portrait_url}
          alt={`${first_name} ${last_name}`}
          className="rounded-full w-40 h-40 object-cover shadow"
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
            Serving {start_year}
            {end_year ? `–${end_year}` : '–present'} in the {chamber.toUpperCase()}
          </p>
        </div>
      </div>

      {/* About & Map */}
      <section className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div className="lg:col-span-2 space-y-4">
          <h2 className="text-2xl font-semibold">About</h2>
          <p>
            {/* Placeholder: actual bio field if available */}
            {legislator.bio ||
              'No biographical summary available for this legislator.'}
          </p>

          {/* Service history */}
          {service_history.length > 0 && (
            <div>
              <h3 className="text-xl font-semibold">Service History</h3>
              <ul className="list-disc list-inside space-y-1">
                {service_history.map((term, i) => (
                  <li key={i}>
                    {term.chamber.charAt(0).toUpperCase() +
                      term.chamber.slice(1)}{' '}
                    ({term.start_date}
                    {term.end_date ? `–${term.end_date}` : '–present'})
                  </li>
                ))}
              </ul>
            </div>
          )}

          {/* Committees */}
          {committees.length > 0 && (
            <div>
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
                      <td className="px-2 py-1">{c.from}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {/* Leadership */}
          {leadership_positions.length > 0 && (
            <div>
              <h3 className="text-xl font-semibold">Leadership Positions</h3>
              <ul className="list-disc list-inside space-y-1">
                {leadership_positions.map((pos, i) => (
                  <li key={i}>
                    {pos.title} ({pos.start_date}
                    {pos.end_date ? `–${pos.end_date}` : '–present'})
                  </li>
                ))}
              </ul>
            </div>
          )}
        </div>

        {/* District Map Placeholder */}
        <div className="h-64 bg-gray-100 rounded shadow flex items-center justify-center">
          Map of District {district || '—'} (coming soon)
        </div>
      </section>

      {/* Sponsored Bills */}
      <section className="space-y-2">
        <h2 className="text-2xl font-semibold">Sponsored Bills</h2>
        {sponsored_bills.length > 0 ? (
          <table className="w-full text-left border-collapse">
            <thead>
              <tr>
                <th className="border-b px-2 py-1">Bill</th>
                <th className="border-b px-2 py-1">Date</th>
                <th className="border-b px-2 py-1">Status</th>
                <th className="border-b px-2 py-1">Cosponsors</th>
              </tr>
            </thead>
            <tbody>
              {sponsored_bills.map(b => (
                <tr key={b.bill_id}>
                  <td className="px-2 py-1">
                    <Link
                      to={`/bills/${b.bill_id}`}
                      className="text-blue-600 hover:underline"
                    >
                      {b.bill_id}: {b.title}
                    </Link>
                  </td>
                  <td className="px-2 py-1">{b.date}</td>
                  <td className="px-2 py-1">{b.status}</td>
                  <td className="px-2 py-1">{b.cosponsors_count}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <p>No sponsored bills found.</p>
        )}
      </section>

      {/* Finance Summary */}
      <section className="space-y-2">
        <h2 className="text-2xl font-semibold">Campaign Finance</h2>
        {finance_summary.total_contributions ? (
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div className="border rounded p-4 shadow-sm">
              <h3 className="font-semibold">Total Contributions</h3>
              <p className="text-xl">
                ${finance_summary.total_contributions.toLocaleString()}
              </p>
              <p className="text-sm text-gray-600">
                Cycle: {finance_summary.cycle}
              </p>
            </div>
            <div className="border rounded p-4 shadow-sm">
              <h3 className="font-semibold">Top Industries</h3>
              <ul className="list-disc list-inside space-y-1">
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

      {/* Recent Votes */}
      <section className="space-y-2">
        <h2 className="text-2xl font-semibold">Recent Votes</h2>
        {recent_votes.length > 0 ? (
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
