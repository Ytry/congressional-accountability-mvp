const express = require('express');
const router = express.Router();

// Placeholder: Return sample sponsored bills
router.get('/', (req, res) => {
  res.json([
    {
      bill_number: "H.R.1234",
      title: "Education for All Act",
      status: "Introduced",
      policy_area: "Education",
      date_introduced: "2023-03-15"
    }
  ]);
});

module.exports = router;
