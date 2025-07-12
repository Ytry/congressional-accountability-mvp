require('dotenv').config();
const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const logger = require('./utils/logger');
const correlationId = require('./utils/correlation');
const expressWinston = require('express-winston');

const app = express();

// ── Compute the true path to the shared portraits folder ────────────────────
// __dirname === /opt/render/project/src/backend
const PORTRAITS_DIR = path.join(__dirname, '..', 'portraits');

// ── Correlation middleware for centralized logging ─────────────────────────
app.use(correlationId);

// ── Express-Winston HTTP request logging ──────────────────────────────────
app.use(expressWinston.logger({
  winstonInstance: logger,
  meta: true,
  msg: 'HTTP {{req.method}} {{req.url}}',
  expressFormat: false,
  colorize: false,
  dynamicMeta: (req, res) => ({
    correlationId: req.correlationId,
    method: req.method,
    url: req.originalUrl,
    statusCode: res.statusCode
  })
}));

// ── CORS setup with logging ───────────────────────────────────────────────
const allowedOrigins = [];
if (process.env.FRONTEND_ORIGIN)    allowedOrigins.push(process.env.FRONTEND_ORIGIN);
if (process.env.ADDITIONAL_ORIGINS) allowedOrigins.push(...process.env.ADDITIONAL_ORIGINS.split(','));

app.use(cors({
  origin: (origin, cb) => {
    if (!origin) return cb(null, true);
    if (allowedOrigins.includes(origin)) return cb(null, true);
    if (/\.vercel\.app$/.test(origin))  return cb(null, true);
    if (/\.onrender\.com$/.test(origin)) return cb(null, true);
    logger.warn(`Blocked CORS from ${origin}`);
    return cb(new Error('Not allowed by CORS'), false);
  },
  credentials: true,
}));

// ── JSON parsing ───────────────────────────────────────────────────────────
app.use(express.json());

// ── Debug: List what's in the portraits folder ────────────────────────────
app.get('/debug/portraits', (req, res) => {
  req.logger.debug(`Looking in ${PORTRAITS_DIR}`);
  try {
    const files = fs.readdirSync(PORTRAITS_DIR);
    req.logger.debug('Found portraits', { files });
    return res.json(files);
  } catch (err) {
    req.logger.error('Error reading portraits directory', { message: err.message, stack: err.stack });
    return res.status(500).json({ error: err.message });
  }
});

// ── Serve headshots from portraits folder ─────────────────────────────────
app.use('/portraits', express.static(PORTRAITS_DIR));

// ── Mount API routes ──────────────────────────────────────────────────────
app.use('/api/etl',        require('./routes/etl'));
app.use('/api/legislators',require('./routes/legislators'));
app.use('/api/votes',      require('./routes/votes'));
app.use('/api/bills',      require('./routes/bills'));
app.use('/api/finance',    require('./routes/finance'));

// ── Health check ──────────────────────────────────────────────────────────
app.get('/', (req, res) => {
  req.logger.info('Health check', { path: '/' });
  res.send('Congressional Accountability API is running');
});

// ── Express-Winston error logging ─────────────────────────────────────────
app.use(expressWinston.errorLogger({
  winstonInstance: logger,
  meta: false,
  msg: 'ERROR {{err.message}}',
  dynamicMeta: (req, res, err) => ({
    correlationId: req.correlationId,
    stack: err.stack
  })
}));

// ── Global error handler ──────────────────────────────────────────────────
app.use((err, req, res, next) => {
  logger.error('Unhandled exception in request pipeline', {
    message: err.message,
    stack: err.stack,
    correlationId: req.correlationId
  });
  res.status(500).json({ error: 'Internal Server Error' });
});

// ── Start server ──────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  logger.info(`API listening on port ${PORT}`);
});
