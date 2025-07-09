// server.js
// Load environment variables from .env (ensure FRONTEND_ORIGIN is set)
require('dotenv').config();

const express = require('express');
const cors = require('cors');
const app = express();

// CORS configuration: allow only the front-end origin
app.use(
  cors({
    origin: process.env.FRONTEND_ORIGIN, // e.g. https://your-frontend-domain.com
    credentials: true,                  // if you need to send cookies/auth
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
