// src/App.jsx
import React, { Suspense, lazy, createContext } from 'react';
import { Routes, Route, NavLink, Navigate, Outlet } from 'react-router-dom';

// Lazy‐loaded pages for code‐splitting
const LegislatorList    = lazy(() => import('./components/LegislatorList'));
const LegislatorProfile = lazy(() => import('./components/LegislatorProfile'));
const BillsList         = lazy(() => import('./components/BillsList'));
const BillDetail        = lazy(() => import('./components/BillDetail'));
const FinanceOverview   = lazy(() => import('./components/FinanceOverview'));

// Create an API‐URL context for child components to consume
export const ApiContext = createContext(import.meta.env.VITE_API_URL);

// Error boundary to catch render errors in children
class ErrorBoundary extends React.Component {
  state = { hasError: false, error: null };
  static getDerivedStateFromError(err) {
    return { hasError: true, error: err };
  }
  componentDidCatch(err, info) {
    console.error('ErrorBoundary caught', err, info);
  }
  render() {
    if (this.state.hasError) {
      return (
        <div className="p-6 text-center">
          <h2 className="text-2xl font-semibold text-red-600">
            Something went wrong.
          </h2>
          <pre className="mt-4 text-sm text-gray-700">
            {this.state.error?.message}
          </pre>
        </div>
      );
    }
    return this.props.children;
  }
}

// Main layout: header, nav, outlet, footer
function Layout() {
  const linkClasses = ({ isActive }) =>
    isActive
      ? 'text-blue-600 border-b-2 border-blue-600 pb-1'
      : 'text-gray-700 hover:text-blue-600 pb-1';

  return (
    <div className="min-h-screen flex flex-col">
      <header className="bg-white shadow">
        <div className="container mx-auto px-4 py-4 flex flex-wrap items-center justify-between">
          <h1 className="text-2xl font-bold">Congressional Accountability</h1>
          <nav className="space-x-6 mt-2 sm:mt-0">
            <NavLink to="/legislators" className={linkClasses}>
              Legislators
            </NavLink>
            <NavLink to="/bills" className={linkClasses}>
              Bills
            </NavLink>
            <NavLink to="/finance" className={linkClasses}>
              Finance
            </NavLink>
          </nav>
        </div>
      </header>

      <main className="flex-1 container mx-auto px-4 py-6">
        <ErrorBoundary>
          <Suspense fallback={<p>Loading…</p>}>
            <Outlet />
          </Suspense>
        </ErrorBoundary>
      </main>

      <footer className="bg-gray-100 text-center py-4 text-sm text-gray-600">
        Data refreshed nightly · © {new Date().getFullYear()}
      </footer>
    </div>
  );
}

// Fallback for unmatched routes
function NotFound() {
  return (
    <div className="text-center py-16">
      <h2 className="text-3xl font-semibold">404: Page Not Found</h2>
      <NavLink to="/legislators" className="text-blue-600 hover:underline mt-4 inline-block">
        ← Back to Legislators
      </NavLink>
    </div>
  );
}

export default function App() {
  const apiBase = import.meta.env.VITE_API_URL || '';

  return (
    <ApiContext.Provider value={apiBase}>
      <Routes>
        <Route path="/" element={<Layout />}>  
          {/* Redirect root → /legislators */}
          <Route index element={<Navigate to="legislators" replace />} />

          {/* Legislator pages */}
          <Route path="legislators" element={<LegislatorList />} />
          <Route path="legislators/:bioguide_id" element={<LegislatorProfile />} />

          {/* Bills pages */}
          <Route path="bills" element={<BillsList />} />
          <Route path="bills/:billId" element={<BillDetail />} />

          {/* Finance pages */}
          <Route path="finance" element={<FinanceOverview />} />

          {/* Catch-all */}
          <Route path="*" element={<NotFound />} />
        </Route>
      </Routes>
    </ApiContext.Provider>
  );
}
