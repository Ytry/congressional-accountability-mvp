import React, { useState, useEffect, useContext } from 'react'
import { ApiContext } from '../App'
import LegislatorCard from './LegislatorCard'

// Custom debounce hook
function useDebounce(value, delay = 500) {
  const [debouncedValue, setDebouncedValue] = useState(value)
  useEffect(() => {
    const handler = setTimeout(() => setDebouncedValue(value), delay)
    return () => clearTimeout(handler)
  }, [value, delay])
  return debouncedValue
}

const PAGE_SIZE = 24
const PARTIES = [
  { label: 'All Parties', value: '' },
  { label: 'Democrat', value: 'D' },
  { label: 'Republican', value: 'R' },
  { label: 'Independent', value: 'I' },
]
const STATES = [
  '', 'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','IA','ID','IL','IN',
  'KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM',
  'NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA',
  'WV','WI','WY','DC',
]

export default function LegislatorList() {
  const API_URL = useContext(ApiContext)
  const [legislators, setLegislators] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [page, setPage] = useState(1)

  // Filters
  const [query, setQuery] = useState('')
  const debouncedQuery = useDebounce(query, 500)
  const [party, setParty] = useState('')
  const [stateFilter, setStateFilter] = useState('')

  useEffect(() => {
    async function fetchLegislators() {
      setLoading(true)
      setError('')
      try {
        const params = new URLSearchParams()
        params.set('page', page)
        params.set('pageSize', PAGE_SIZE)
        if (debouncedQuery) params.set('query', debouncedQuery)
        if (party) params.set('party', party)
        if (stateFilter) params.set('state', stateFilter)

        const res = await fetch(`${API_URL}/api/legislators?${params.toString()}`)
        if (!res.ok) throw new Error(`Server responded ${res.status}`)
        const data = await res.json()
        setLegislators(data)
      } catch (err) {
        setError(err.message)
      } finally {
        setLoading(false)
      }
    }
    fetchLegislators()
  }, [API_URL, page, debouncedQuery, party, stateFilter])

  const handleReset = () => {
    setQuery('')
    setParty('')
    setStateFilter('')
    setPage(1)
  }

  return (
    <div className="space-y-6">
      {/* Search & Filters */}
      <div className="flex flex-wrap gap-3 items-end">
        <div className="flex-1 min-w-[200px]">
          <label className="block text-sm font-medium mb-1">Search Name</label>
          <input
            type="text"
            value={query}
            onChange={e => setQuery(e.target.value)}
            placeholder="e.g. Smith"
            className="w-full border rounded px-3 py-2 focus:outline-none focus:ring-2 focus:ring-blue-400"
          />
        </div>

        <div>
          <label className="block text-sm font-medium mb-1">Party</label>
          <select
            value={party}
            onChange={e => setParty(e.target.value)}
            className="border rounded px-3 py-2 focus:outline-none focus:ring-2 focus:ring-blue-400"
          >
            {PARTIES.map(p => (
              <option key={p.value} value={p.value}>
                {p.label}
              </option>
            ))}
          </select>
        </div>

        <div>
          <label className="block text-sm font-medium mb-1">State</label>
          <select
            value={stateFilter}
            onChange={e => setStateFilter(e.target.value)}
            className="border rounded px-3 py-2 focus:outline-none focus:ring-2 focus:ring-blue-400"
          >
            {STATES.map(s => (
              <option key={s} value={s}>
                {s || 'All States'}
              </option>
            ))}
          </select>
        </div>

        <div className="flex gap-2">
          <button
            onClick={() => setPage(1)}
            className="bg-blue-600 text-white px-4 py-2 rounded hover:bg-blue-700"
          >
            Apply
          </button>
          <button
            onClick={handleReset}
            className="border border-gray-300 px-4 py-2 rounded hover:bg-gray-100"
          >
            Reset
          </button>
        </div>
      </div>

      {/* Results */}
      {loading && (
        <p className="text-center text-gray-500">Loading legislators…</p>
      )}
      {error && (
        <p className="text-center text-red-600">Error: {error}</p>
      )}
      {!loading && !error && legislators.length === 0 && (
        <p className="text-center text-gray-600">No legislators found.</p>
      )}

      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-6">
        {legislators.map(leg => (
          <LegislatorCard key={leg.bioguide_id} legislator={leg} />
        ))}
      </div>

      {/* Pagination */}
      <div className="flex justify-center items-center gap-4 py-4">
        <button
          onClick={() => setPage(p => Math.max(1, p - 1))}
          disabled={page === 1}
          className={`px-4 py-2 rounded border ${
            page === 1
              ? 'opacity-50 cursor-not-allowed'
              : 'hover:bg-gray-100'
          }`}
        >
          ← Prev
        </button>
        <span className="text-sm text-gray-700">
          Page {page}
        </span>
        <button
          onClick={() => setPage(p => p + 1)}
          disabled={legislators.length < PAGE_SIZE}
          className={`px-4 py-2 rounded border ${
            legislators.length < PAGE_SIZE
              ? 'opacity-50 cursor-not-allowed'
              : 'hover:bg-gray-100'
          }`}
        >
          Next →
        </button>
      </div>
    </div>
  )
}
