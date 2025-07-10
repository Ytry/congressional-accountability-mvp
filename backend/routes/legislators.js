// routes/legislators.js
const express = require('express')
const router  = express.Router()
const db      = require('../db')

// … your paginated GET '/' here …

// GET a single legislator by BioGuide ID
router.get('/:bioguide_id', async (req, res) => {
  const { bioguide_id } = req.params

  try {
    // 1) Core legislator row + rename bio_snapshot → bio
    const { rows } = await db.query(
      `
      SELECT
        l.bioguide_id,
        l.first_name,
        l.last_name,
        l.party,
        l.state,
        l.district,
        l.chamber,
        l.portrait_url,
        l.official_website_url,
        l.office_contact,
        l.bio_snapshot AS bio
      FROM legislators l
      WHERE l.bioguide_id = $1
      `,
      [bioguide_id]
    )
    if (!rows.length) {
      return res.status(404).json({ error: 'Legislator not found' })
    }
    const legislator = rows[0]

    // 2) Service history
    const svc = await db.query(
      `
      SELECT chamber, start_date, end_date
      FROM service_history
      WHERE bioguide_id = $1
      ORDER BY start_date DESC
      `,
      [bioguide_id]
    )
    legislator.service_history = svc.rows

    // 3) Committee assignments
    const comm = await db.query(
      `
      SELECT committee_id, name, role, from_date AS from, to_date AS to
      FROM committee_assignments
      WHERE bioguide_id = $1
      `,
      [bioguide_id]
    )
    legislator.committees = comm.rows

    // 4) Leadership roles
    const lead = await db.query(
      `
      SELECT title, start_date, end_date
      FROM leadership_roles
      WHERE bioguide_id = $1
      `,
      [bioguide_id]
    )
    legislator.leadership_positions = lead.rows

    // 5) Sponsored bills (bill_sponsorships)
    const bills = await db.query(
      `
      SELECT bill_id,
             title,
             latest_status AS status,
             introduction_date AS date
      FROM bill_sponsorships
      WHERE sponsor_bioguide_id = $1
      ORDER BY introduction_date DESC
      LIMIT 10
      `,
      [bioguide_id]
    )
    legislator.sponsored_bills = bills.rows

    // 6) Campaign finance
    const finance = await db.query(
      `
      SELECT cycle,
             total_contributions,
             top_industries
      FROM campaign_finance
      WHERE bioguide_id = $1
      `,
      [bioguide_id]
    )
    legislator.finance_summary = finance.rows[0] || {}

    // 7) Recent votes via vote_records → vote_sessions
    const votes = await db.query(
      `
      SELECT
        vs.date,
        vs.bill_number   AS bill,
        vr.position      AS position
      FROM vote_records vr
      JOIN vote_sessions vs
        ON vr.vote_session_id = vs.vote_session_id
      WHERE vr.bioguide_id = $1
      ORDER BY vs.date DESC
      LIMIT 20
      `,
      [bioguide_id]
    )
    legislator.recent_votes = votes.rows

    return res.json(legislator)
  } catch (err) {
    console.error('Error fetching legislator profile:', err)
    return res.status(500).json({ error: 'Failed to fetch legislator profile' })
  }
})

module.exports = router
