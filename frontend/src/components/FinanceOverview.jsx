import React, { useState, useEffect, useContext } from 'react';
import { ApiContext } from '../App';

export default function FinanceOverview() {
  const API_URL = useContext(ApiContext);
  const [summary, setSummary] = useState(null);

  useEffect(() => {
    fetch(`${API_URL}/api/finance/summary`)
      .then(r => r.ok ? r.json() : Promise.reject(`Status ${r.status}`))
      .then(setSummary)
      .catch(console.error);
  }, [API_URL]);

  return (
    <div className="space-y-4">
      <h2 className="text-2xl font-semibold">Finance Dashboard</h2>
      {summary ? (
        <pre className="bg-gray-100 p-4 rounded">{JSON.stringify(summary, null, 2)}</pre>
      ) : (
        <p>Loading finance data…</p>
      )}
    </div>
  );
}
