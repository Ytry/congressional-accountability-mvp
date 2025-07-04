const express = require('express');
const router = express.Router();

// Placeholder: Return sample campaign finance data
router.get('/', (req, res) => {
  res.json([
    {
      legislator_id: 1,
      total_received: 2500000,
      top_contributors: [
        { name: "ACME Corp", amount: 500000 },
        { name: "Teachers Union", amount: 350000 }
      ],
      top_industries: [
        { name: "Education", amount: 700000 },
        { name: "Tech", amount: 300000 }
      ]
    }
  ]);
});

module.exports = router;
