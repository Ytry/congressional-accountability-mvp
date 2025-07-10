const express = require('express');
const router = express.Router();
const db = require('../db');

// GET all legislators with pagination
router.get('/', async (req, res) => {
  // Parse pagination params with defaults
  const page = parseInt(req.query.page, 10) || 1;
  const pageSize = parseInt(req.query.pageSize, 10) || 24;
  const offset = (page - 1) * pageSize;

  try {
    // Total count for pagination metadata
    const countRes = await db.query('SELECT COUNT(*) FROM legislators');
    const totalCount = parseInt(countRes.rows[0].count, 10);

    // Page slice of legislators
    const result = await db.query(
      `
      SELECT
        bioguide_id,
        full_name,
        party,
        state,
        district,
        chamber,
        portrait_url,
        official_website_url,
        office_contact,
        bio_snapshot
      FROM legislators
      ORDER BY state, district NULLS LAST
      LIMIT $1 OFFSET $2
      `,
      [pageSize, offset]
    );

    // Include pagination metadata
    res.set('X-Total-Count', totalCount);
    return res.status(200).json({
      items: result.rows,
      totalCount,
      page,
      pageSize
    });
  } catch (err) {
    console.error('Error fetching legislators with pagination:', err);
    return res.status(500).json({ error: 'Failed to fetch legislators' });
  }
});

// GET a single legislator by BioGuide ID
router.get('/:bioguide_id', async (req, res) => {
  const { bioguide_id } = req.params;

  try {
    const legislatorResult = await db.query(
      `
      SELECT *
      FROM legislators
      WHERE bioguide_id = $1
      `,
      [bioguide_id]
    );

    if (legislatorResult.rows.length === 0) {
      return res.status(404).json({ error: 'Legislator not found' });
    }

    const legislator = legislatorResult.rows[0];

    // Fetch committees
    const committeesResult = await db.query(
      `
      SELECT committee_name, role
      FROM committee_assignments
      WHERE bioguide_id = $1
      `,
      [bioguide_id]
    );
    legislator.committees = committeesResult.rows;

    // Fetch votes
    const votesResult = await db.query(
      `
      SELECT
        v.bill_number,
        v.question_text,
        v.vote_result,
        v.vote_description,
        v.date,
        mv.member_vote_position
      FROM member_votes mv
      JOIN votes v ON mv.vote_id = v.vote_id
      WHERE mv.bioguide_id = $1
      ORDER BY v.date DESC
      LIMIT 20
      `,
      [bioguide_id]
    );
    legislator.voting_record = votesResult.rows;

    // Fetch sponsored bills
    const billsResult = await db.query(
      `
      SELECT bill_number, title, latest_status, introduction_date
      FROM bills
      WHERE sponsor_bioguide_id = $1
      ORDER BY introduction_date DESC
      LIMIT 10
      `,
      [bioguide_id]
    );
    legislator.sponsored_bills = billsResult.rows;

    // Fetch campaign finance summary
    const financeResult = await db.query(
      `
      SELECT *
      FROM campaign_finance
      WHERE bioguide_id = $1
      `,
      [bioguide_id]
    );
    legislator.campaign_finance = financeResult.rows[0] || null;

    return res.status(200).json(legislator);

  } catch (err) {
    console.error('Error fetching legislator profile:', err);
    return res.status(500).json({ error: 'Failed to fetch legislator profile' });
  }
});

module.exports = router;
