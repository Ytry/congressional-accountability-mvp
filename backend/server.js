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
  // e.g. add comma-separated list: "https://preview1.vercel.app,https://preview2.vercel.app"
  allowedOrigins.push(...process.env.ADDITIONAL_ORIGINS.split(','));
}

// CORS configuration: dynamic origin check
app.use(
  cors({
    origin: (origin, callback) => {
      // allow requests with no origin (like mobile apps or curl)
      if (!origin) return callback(null, true);
      if (allowedOrigins.includes(origin)) {
        return callback(null, true);
      }
      console.warn(`Blocked CORS request from origin: ${origin}`);
      return callback(new Error('Not allowed by CORS'));
    },
    credentials: true,
  })
);

// Middleware for JSON parsing
app.use(express.json());

// Route modules
const legislatorsRoute = require('./routes/legislators');
const votesRoute       = require('./routes/votes');
const billsRoute       = require('./routes/bills');
const financeRoute     = require('./routes/finance');
const etlRoutes        = require('./routes/etl');

// Mount ETL endpoint
app.use('/api/etl', etlRoutes);

// Mount API endpoints
app.use('/api/legislators', legislatorsRoute);
app.use('/api/votes',       votesRoute);
app.use('/api/bills',       billsRoute);
app.use('/api/finance',     financeRoute);

// Health check / root
app.get('/', (req, res) => {
  res.send('Congressional Accountability API is running');
});

// Start server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
