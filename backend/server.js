// server.js

require('dotenv').config();
const express = require('express');
const cors = require('cors');
const app = express();

// Define CORS allowed origins from env
const allowedOrigins = [];
if (process.env.FRONTEND_ORIGIN) {
  allowedOrigins.push(process.env.FRONTEND_ORIGIN);
}
if (process.env.ADDITIONAL_ORIGINS) {
  allowedOrigins.push(...process.env.ADDITIONAL_ORIGINS.split(','));
}

// Dynamic CORS middleware
app.use(
  cors({
    origin: (origin, callback) => {
      if (!origin) return callback(null, true); // allow server-to-server or same-origin

      // Allow listed origins from env
      if (allowedOrigins.includes(origin)) {
        return callback(null, true);
      }

      // Allow Vercel preview/production domains
      if (/^https:\/\/[a-z0-9-]+\.vercel\.app$/.test(origin)) {
        return callback(null, true);
      }

      // Allow Render domains
      if (/^https:\/\/[a-z0-9-]+\.onrender\.com$/.test(origin)) {
        return callback(null, true);
      }

      console.warn(`Blocked CORS request from origin: ${origin}`);
      return callback(new Error('Not allowed by CORS'), false);
    },
    credentials: true,
  })
);

// Enable JSON parsing
app.use(express.json());

// Mount route modules
const legislatorsRoute = require('./routes/legislators');
const votesRoute       = require('./routes/votes');
const billsRoute       = require('./routes/bills');
const financeRoute     = require('./routes/finance');
const etlRoutes        = require('./routes/etl');

app.use('/api/etl',        etlRoutes);
app.use('/api/legislators', legislatorsRoute);
app.use('/api/votes',       votesRoute);
app.use('/api/bills',       billsRoute);
app.use('/api/finance',     financeRoute);

// Health check route
app.get('/', (req, res) => {
  res.send('Congressional Accountability API is running');
});

// Start server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
