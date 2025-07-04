const express = require('express');
const router = express.Router();

// Placeholder: Return sample vote data
router.get('/', (req, res) => {
  res.json([
    {
      vote_id: "house-118-1-123",
      bill_number: "H.R.1",
      question_text: "On Passage",
      vote_description: "Fiscal Responsibility Act of 2023",
      vote_result: "Agreed to",
      position: "Yea",
      date: "2023-05-31",
      tally_yea: 314,
      tally_nay: 117,
      tally_present: 2,
      tally_not_voting: 0,
      is_key_vote: true
    }
  ]);
});

module.exports = router;
