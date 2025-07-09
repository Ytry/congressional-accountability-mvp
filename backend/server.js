// server.js
// Load environment variables from .env
require('dotenv').config();

const express = require('express');
const cors = require('cors');
const app = express();

// Build a whitelist from your environment vars
const allowedOrigins = [];
if (process.env.FRONTEND_ORIGIN) {
  allowedOrigins.push(process.env.FRONTEND_ORIGIN);
}
if (process.env.ADDITIONAL_ORIGINS) {
  allowedOrigins.push(...process.env.ADDITIONAL_ORIGINS.split(','));
}

// CORS configuration: dynamic origin check with patterns
app.use(
  cors({
    origin: (origin, callback) => {
      // allow same-origin or tools like curl (no origin)
      if (!origin) return callback(null, true);

      // 1) Exact matches from env
      if (allowedOrigins.includes(origin)) {
        return callback(null, true);
      }

      // 2) Preview or production Vercel domains (*.vercel.app)
      if (/^https:\/\/[A-Za-z0-9-]+\.vercel\.app$/.test(origin)) {
        return callback(null, true);
      }

      // 3) Render domains (if hosting on Render *.onrender.com)
      if (/^https:\/\/[A-Za-z0-9-]+\.onrender\.com$/.test(origin)) {
        return callback(null, true);
      }

      console.warn(`Blocked CORS request from origin: ${origin}`);
      return callback(new Error('Not allowed by CORS'), false);
    },
    credentials: true,
  })
);

// JSON parsing
app.use(express.json());

// Route modules
const legislatorsRoute = require('./routes/legislators');
const votesRoute       = require('./routes/votes');
const billsRoute       = require('./routes/bills');
const financeRoute     = require('./routes/finance');
const etlRoutes        = require('./routes/etl');

// Mount endpoints
app.use('/api/etl', etlRoutes);
app.use('/api/legislators', legislatorsRoute);
app.use('/api/votes',       votesRoute);
app.use('/api/bills',       billsRoute);
app.use('/api/finance',     financeRoute);

// Health check
app.get('/', (req, res) => {
  res.send('Congressional Accountability API is running');
});

// Start server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
