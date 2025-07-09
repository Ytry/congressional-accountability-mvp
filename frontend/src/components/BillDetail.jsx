import React, { useState, useEffect, useContext } from 'react';
import { useParams, Link } from 'react-router-dom';
import { ApiContext } from '../App';

export default function BillDetail() {
  const { billId } = useParams();
  const API_URL = useContext(ApiContext);
  const [bill, setBill] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  useEffect(() => {
    fetch(`${API_URL}/api/bills/${billId}`)
      .then(r => {
        if (!r.ok) throw new Error(`Status ${r.status}`);
        return r.json();
      })
      .then(data => setBill(data))
      .catch(err => setError(err.message))
      .finally(() => setLoading(false));
  }, [API_URL, billId]);

  if (loading) return <p>Loading bill…</p>;
  if (error)   return <p className="text-red-600">Error: {error}</p>;
  if (!bill)   return <p>Bill not found.</p>;

  return (
    <div className="space-y-4">
      <Link to="/bills" className="text-blue-600 hover:underline">
        ← Back to Bills
      </Link>
      <h2 className="text-2xl font-bold">{bill.bill_id}: {bill.title}</h2>
      <p>{bill.summary}</p>
      {/* TODO: flesh out sponsors, status, actions, etc */}
    </div>
  );
}
