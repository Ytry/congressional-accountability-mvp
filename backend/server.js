// server.js

require('dotenv').config();
const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const app = express();

// ── DEBUG: list what's in public/portraits ────────────────────────────────
app.get('/debug/portraits', (req, res) => {
  const portraitsDir = path.join(process.cwd(), 'public', 'portraits');
  console.log(`[/debug/portraits] Reading directory: ${portraitsDir}`);
  try {
    const files = fs.readdirSync(portraitsDir);
    console.log(`[/debug/portraits] Found files:`, files);
    return res.json(files);
  } catch (err) {
    console.error('[/debug/portraits] Error reading portraits directory:', err);
    return res.status(500).json({ error: err.message });
  }
});

// ── Serve headshots from public/portraits via GET /portraits/:filename.jpg ──
app.use(
  '/portraits',
  express.static(path.join(process.cwd(), 'public', 'portraits'))
);

// ── CORS setup ─────────────────────────────────────────────────────────────
const allowedOrigins = [];
if (process.env.FRONTEND_ORIGIN) {
  allowedOrigins.push(process.env.FRONTEND_ORIGIN);
}
if (process.env.ADDITIONAL_ORIGINS) {
  allowedOrigins.push(...process.env.ADDITIONAL_ORIGINS.split(','));
}
app.use(
  cors({
    origin: (origin, callback) => {
      if (!origin) return callback(null, true);
      if (allowedOrigins.includes(origin)) return callback(null, true);
      if (/^https:\/\/[a-z0-9-]+\.vercel\.app$/.test(origin)) return callback(null, true);
      if (/^https:\/\/[a-z0-9-]+\.onrender\.com$/.test(origin)) return callback(null, true);
      console.warn(`Blocked CORS request from origin: ${origin}`);
      return callback(new Error('Not allowed by CORS'), false);
    },
    credentials: true,
  })
);

// ── JSON parsing ───────────────────────────────────────────────────────────
app.use(express.json());

// ── API routes ─────────────────────────────────────────────────────────────
app.use('/api/etl',        require('./routes/etl'));
app.use('/api/legislators', require('./routes/legislators'));
app.use('/api/votes',       require('./routes/votes'));
app.use('/api/bills',       require('./routes/bills'));
app.use('/api/finance',     require('./routes/finance'));

// ── Health check ───────────────────────────────────────────────────────────
app.get('/', (req, res) => {
  res.send('Congressional Accountability API is running');
});

// ── Start server ───────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
