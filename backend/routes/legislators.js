const express = require('express');
const router = express.Router();

// Placeholder: Return sample legislator data
router.get('/', (req, res) => {
  res.json([
    {
      id: 1,
      bioguide_id: "A000360",
      full_name: "Rep. Jane Doe",
      party: "I",
      state: "XY",
      district: 99,
      chamber: "House",
      portrait_url: "https://example.com/portrait.jpg",
      official_website_url: "https://example.com",
      office_contact: {
        address: "123 Constitution Ave, DC",
        phone: "202-555-0123"
      },
      bio_snapshot: "Former educator and city council member.",
    }
  ]);
});

module.exports = router;
