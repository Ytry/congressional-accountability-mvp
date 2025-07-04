const express = require('express');
const router = express.Router();
const { spawn } = require('child_process');
const path = require('path');

router.post('/legislators', (req, res) => {
  const scriptPath = path.join(__dirname, '../../etl/legislators_etl.py');
  const python = spawn('python3', [scriptPath]);

  let output = '';
  let errorOutput = '';

  python.stdout.on('data', (data) => {
    output += data.toString();
  });

  python.stderr.on('data', (data) => {
    errorOutput += data.toString();
  });

  python.on('close', (code) => {
    if (code === 0) {
      res.status(200).json({ message: 'ETL script completed successfully', output });
    } else {
      res.status(500).json({ message: 'ETL script failed', error: errorOutput });
    }
  });
});

module.exports = router;
