const express = require('express');
const router = express.Router();
const db = require('../db');

// GET all legislators
router.get('/', async (req, res) => {
  try {
    const result = await db.query(`
      SELECT
        l.bioguide_id,
        l.full_name,
        l.party,
        l.state,
        l.district,
        l.chamber,
        l.portrait_url,
        l.official_website_url,
        l.office_contact,
        l.bio_snapshot
      FROM legislators l
      ORDER BY l.state, l.district NULLS LAST
    `);

    res.status(200).json(result.rows);
  } catch (err) {
    console.error('Error fetching legislators:', err);
    res.status(500).json({ error: 'Failed to fetch legislators' });
  }
});

// GET a single legislator by BioGuide ID
router.get('/:bioguide_id', async (req, res) => {
  const { bioguide_id } = req.params;

  try {
    const legislatorResult = await db.query(`
      SELECT *
      FROM legislators
      WHERE bioguide_id = $1
    `, [bioguide_id]);

    if (legislatorResult.rows.length === 0) {
      return res.status(404).json({ error: 'Legislator not found' });
    }

    const legislator = legislatorResult.rows[0];

    // Fetch committees
    const committeesResult = await db.query(`
      SELECT committee_name, role
      FROM committee_assignments
      WHERE bioguide_id = $1
    `, [bioguide_id]);

    legislator.committees = committeesResult.rows;

    // Fetch votes
    const votesResult = await db.query(`
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
    `, [bioguide_id]);

    legislator.voting_record = votesResult.rows;

    // Fetch sponsored bills
    const billsResult = await db.query(`
      SELECT bill_number, title, latest_status, introduction_date
      FROM bills
      WHERE sponsor_bioguide_id = $1
      ORDER BY introduction_date DESC
      LIMIT 10
    `, [bioguide_id]);

    legislator.sponsored_bills = billsResult.rows;

    // Fetch campaign finance summary
    const financeResult = await db.query(`
      SELECT *
      FROM campaign_finance
      WHERE bioguide_id = $1
    `, [bioguide_id]);

    legislator.campaign_finance = financeResult.rows[0] || null;

    res.status(200).json(legislator);

  } catch (err) {
    console.error('Error fetching legislator profile:', err);
    res.status(500).json({ error: 'Failed to fetch legislator profile' });
  }
});

module.exports = router;
