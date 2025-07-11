// src/backend/server.js

require('dotenv').config();
const express = require('express');
const cors    = require('cors');
const fs      = require('fs');
const path    = require('path');
const app     = express();

// ── Compute the true path to the shared portraits folder ────────────────────
// __dirname === /opt/render/project/src/backend
const PORTRAITS_DIR = path.join(__dirname, '..', 'public', 'portraits');

// ── DEBUG: List what’s actually in that folder ─────────────────────────────
app.get('/debug/portraits', (req, res) => {
  console.debug(`[/debug/portraits] Looking in ${PORTRAITS_DIR}`);
  try {
    const files = fs.readdirSync(PORTRAITS_DIR);
    console.debug(`[/debug/portraits] Found:`, files);
    return res.json(files);
  } catch (err) {
    console.error(`[/debug/portraits] Error reading folder:`, err);
    return res.status(500).json({ error: err.message });
  }
});

// ── Serve headshots from that same folder at /portraits/:id.jpg ───────────
app.use('/portraits', express.static(PORTRAITS_DIR));

// ── CORS setup ─────────────────────────────────────────────────────────────
const allowedOrigins = [];
if (process.env.FRONTEND_ORIGIN)    allowedOrigins.push(process.env.FRONTEND_ORIGIN);
if (process.env.ADDITIONAL_ORIGINS) allowedOrigins.push(...process.env.ADDITIONAL_ORIGINS.split(','));

app.use(cors({
  origin: (origin, cb) => {
    if (!origin) return cb(null, true);               // allow server‐to‐server or same‐origin
    if (allowedOrigins.includes(origin)) return cb(null, true);
    if (/\.vercel\.app$/.test(origin))  return cb(null, true);
    if (/\.onrender\.com$/.test(origin)) return cb(null, true);
    console.warn(`Blocked CORS from ${origin}`);
    return cb(new Error('Not allowed by CORS'), false);
  },
  credentials: true,
}));

// ── JSON parsing ───────────────────────────────────────────────────────────
app.use(express.json());

// ── Mount your API routes ──────────────────────────────────────────────────
app.use('/api/etl',        require('./routes/etl'));
app.use('/api/legislators',require('./routes/legislators'));
app.use('/api/votes',      require('./routes/votes'));
app.use('/api/bills',      require('./routes/bills'));
app.use('/api/finance',    require('./routes/finance'));

// ── Health check ───────────────────────────────────────────────────────────
app.get('/', (req, res) => res.send('Congressional Accountability API is running'));

// ── Start server ───────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`API listening on port ${PORT}`);
});
