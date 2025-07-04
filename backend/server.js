const express = require('express');
const cors = require('cors');
const app = express();

// Middleware
app.use(cors());
app.use(express.json());

// Routes
const legislatorsRoute = require('./routes/legislators');
const votesRoute = require('./routes/votes');
const billsRoute = require('./routes/bills');
const financeRoute = require('./routes/finance');
const etlRoutes = require('./routes/etl');
app.use('/api/etl', etlRoutes);

app.use('/api/legislators', legislatorsRoute);
app.use('/api/votes', votesRoute);
app.use('/api/bills', billsRoute);
app.use('/api/finance', financeRoute);

// Root route
app.get('/', (req, res) => {
  res.send('Congressional Accountability API is running');
});

// Port binding for Render
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
