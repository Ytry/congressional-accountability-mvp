// Main React App entry
import React, { useEffect, useState, useMemo, Suspense, lazy, useRef } from 'react';
import { BrowserRouter as Router, Route, Routes, useParams, useLocation, Link, useNavigate } from 'react-router-dom';
import './App.css';

const LiveRegion = ({ message }) => (
  <div aria-live="polite" aria-atomic="true" className="sr-only" style={{ position: 'absolute', left: '-9999px' }}>
    {message}
  </div>
);

const Dashboard = React.memo(() => (
  <main className="content" role="main">
    <h2>Welcome</h2>
    <p>Select a data category from the menu to explore congressional records.</p>
  </main>
));

const Header = React.memo(() => {
  const [open, setOpen] = useState(false);
  return (
    <header className="header" role="banner">
      <a href="#main-content" className="skip-to-content">Skip to main content</a>
      <h1>Congressional Accountability</h1>
      <button className="menu-toggle" aria-label="Toggle navigation menu" onClick={() => setOpen(!open)}>
        ☰
      </button>
      <nav className={`nav-links${open ? ' active' : ''}`} aria-label="Main navigation">
        <Link to="/" onClick={() => setOpen(false)}>Dashboard</Link>
        <Link to="/votes/Yea" onClick={() => setOpen(false)}>Votes</Link>
        <Link to="/bills/category/Health" onClick={() => setOpen(false)}>Bills</Link>
        <Link to="/audit/2025-07-03" onClick={() => setOpen(false)}>Audit Logs</Link>
      </nav>
    </header>
  );
});

const Breadcrumbs = React.memo(() => {
  const location = useLocation();
  const crumbs = useMemo(() => location.pathname.split('/').filter(Boolean), [location]);
  return (
    <nav className="breadcrumbs" aria-label="Breadcrumb">
      <Link to="/">Home</Link>
      {crumbs.map((crumb, i) => {
        const path = `/${crumbs.slice(0, i + 1).join('/')}`;
        return (
          <span key={i}>
            {' > '}<Link to={path}>{decodeURIComponent(crumb)}</Link>
          </span>
        );
      })}
    </nav>
  );
});

const Tabs = React.memo(({ tabs, activeTab }) => (
  <div className="tabs" role="tablist">
    {tabs.map(tab => (
      <Link key={tab.path} role="tab" aria-selected={tab.path === activeTab} tabIndex={0} to={tab.path} className={tab.path === activeTab ? 'tab active' : 'tab'}>
        {tab.label}
      </Link>
    ))}
  </div>
));

const VoteTypeView = React.memo(() => {
  const { type } = useParams();
  const [votes, setVotes] = useState([]);
  const [sort, setSort] = useState('recent');
  const [announce, setAnnounce] = useState('');

  useEffect(() => {
    const timeout = setTimeout(() => {
      fetch(`/api/votes?type=${type}&sort=${sort}`)
        .then(res => res.json())
        .then(data => {
          setVotes(data);
          setAnnounce(`${data.length} votes loaded for type ${type}`);
        });
    }, 300);
    return () => clearTimeout(timeout);
  }, [type, sort]);

  const tabs = useMemo(() => [
    { path: '/votes/Yea', label: 'Yea' },
    { path: '/votes/Nay', label: 'Nay' },
    { path: '/votes/Present', label: 'Present' },
    { path: '/votes/NotVoting', label: 'Not Voting' },
  ], []);

  return (
    <main className="content" id="main-content" role="main">
      <LiveRegion message={announce} />
      <Tabs tabs={tabs} activeTab={`/votes/${type}`} />
      <label htmlFor="vote-sort">Sort by:</label>
      <select id="vote-sort" aria-label="Sort votes" value={sort} onChange={e => setSort(e.target.value)}>
        <option value="recent">Most Recent</option>
        <option value="oldest">Oldest First</option>
      </select>
      <h2>Votes: {type}</h2>
      <ul className="list">
        {votes.map((v, i) => (
          <li key={i}><strong>{v.bill_number}</strong>: {v.vote_description} — <em>{v.vote_result}</em></li>
        ))}
      </ul>
    </main>
  );
});

const BillsByCategory = () => {
  const { category } = useParams();
  const [bills, setBills] = useState([]);
  const [announce, setAnnounce] = useState('');

  useEffect(() => {
    fetch(`/api/bills/category/${category}`)
      .then(res => res.json())
      .then(data => {
        setBills(data);
        setAnnounce(`${data.length} bills loaded in category ${category}`);
      });
  }, [category]);

  return (
    <main className="content" id="main-content" role="main">
      <LiveRegion message={announce} />
      <h2>Bills in: {category}</h2>
      <ul className="list">
        {bills.map((b, i) => (
          <li key={i}><strong>{b.bill_number}</strong>: {b.title} — {b.status}</li>
        ))}
      </ul>
    </main>
  );
};

const AuditByDate = () => {
  const { date } = useParams();
  const [logs, setLogs] = useState([]);
  const [announce, setAnnounce] = useState('');

  useEffect(() => {
    fetch(`/api/audit/${date}`)
      .then(res => res.json())
      .then(data => {
        setLogs(data);
        setAnnounce(`${data.length} audit log entries for ${date}`);
      });
  }, [date]);

  return (
    <main className="content" id="main-content" role="main">
      <LiveRegion message={announce} />
      <h2>Audit Logs for {date}</h2>
      <ul className="list">
        {logs.map((log, i) => (
          <li key={i}><strong>{log.timestamp}</strong>: {log.action}</li>
        ))}
      </ul>
    </main>
  );
};

function App() {
  return (
    <Router>
      <Header />
      <Breadcrumbs />
      <Suspense fallback={<div className="content" role="main">Loading...</div>}>
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/votes/:type" element={<VoteTypeView />} />
          <Route path="/bills/category/:category" element={<BillsByCategory />} />
          <Route path="/audit/:date" element={<AuditByDate />} />
        </Routes>
      </Suspense>
    </Router>
  );
}

export default App;
