const express = require('express');
const router = express.Router();
const runLegislatorETL = require('../etl/legislators_etl');

router.post('/legislators', async (req, res) => {
  try {
    await runLegislatorETL();
    res.status(200).json({ message: 'Legislator ETL completed.' });
  } catch (error) {
    console.error('ETL Error:', error);
    res.status(500).json({ error: 'ETL failed' });
  }
});

module.exports = router;
