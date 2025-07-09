import React, { useState, useEffect, useContext } from 'react';
import { ApiContext } from '../App';
import { Link } from 'react-router-dom';

export default function BillsList() {
  const API_URL = useContext(ApiContext);
  const [bills, setBills] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  useEffect(() => {
    fetch(`${API_URL}/api/bills`)
      .then(r => {
        if (!r.ok) throw new Error(`Status ${r.status}`);
        return r.json();
      })
      .then(data => setBills(data))
      .catch(err => setError(err.message))
      .finally(() => setLoading(false));
  }, [API_URL]);

  if (loading) return <p>Loading bills…</p>;
  if (error)   return <p className="text-red-600">Error: {error}</p>;

  return (
    <div className="space-y-4">
      <h2 className="text-2xl font-semibold">Bills</h2>
      {bills.length === 0 ? (
        <p>No bills found.</p>
      ) : (
        <ul className="list-disc list-inside">
          {bills.map(b => (
            <li key={b.bill_id}>
              <Link
                to={`/bills/${b.bill_id}`}
                className="text-blue-600 hover:underline"
              >
                {b.bill_id}: {b.title}
              </Link>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
