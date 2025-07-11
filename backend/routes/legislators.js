// routes/legislators.js
const express = require('express')
const router  = express.Router()
const db      = require('../db')

// ─── GET paginated list ─────────────────────────────────────────────────────
router.get('/', async (req, res) => {
  const page     = parseInt(req.query.page, 10) || 1
  const pageSize = parseInt(req.query.pageSize, 10) || 24
  const offset   = (page - 1) * pageSize

  try {
    const countRes   = await db.query('SELECT COUNT(*) FROM legislators')
    const totalCount = parseInt(countRes.rows[0].count, 10)

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
    )

    // ── OVERRIDE PORTRAIT URL to your local static endpoint ───────────────
    const items = result.rows.map(row => ({
      ...row,
      portrait_url: `/portraits/${row.bioguide_id}.jpg`
    }))

    res.set('X-Total-Count', totalCount)
    return res.json({
      items,
      totalCount,
      page,
      pageSize
    })
  } catch (err) {
    console.error('Error fetching paginated legislators:', err.stack || err.message)
    return res.status(500).json({ error: 'Failed to fetch legislators' })
  }
})

// ─── GET single legislator by bioguide_id ──────────────────────────────────
router.get('/:bioguide_id', async (req, res) => {
  const { bioguide_id } = req.params
  let legislator
  let legislatorId

  // 1) Core legislator row (grab integer PK as 'id')
  try {
    const coreRes = await db.query(
      `
      SELECT
        l.id                  AS id,
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
        l.bio_snapshot        AS bio
      FROM legislators l
      WHERE l.bioguide_id = $1
      `,
      [bioguide_id]
    )

    if (!coreRes.rows.length) {
      return res.status(404).json({ error: 'Legislator not found' })
    }

    legislatorId = coreRes.rows[0].id
    const { id, ...clientLeg } = coreRes.rows[0]
    legislator = clientLeg

    // ── OVERRIDE PORTRAIT URL to your local static endpoint ────────────────
    legislator.portrait_url = `/portraits/${bioguide_id}.jpg`
  } catch (err) {
    console.error(
      'Error running CORE legislator query [SELECT … FROM legislators WHERE bioguide_id = $1]:',
      err.stack || err.message
    )
    return res.status(500).json({ error: 'Failed to fetch core legislator info' })
  }

  // 2) Service history
  try {
    const svc = await db.query(
      `
      SELECT chamber, start_date, end_date
      FROM service_history
      WHERE legislator_id = $1
      ORDER BY start_date DESC
      `,
      [legislatorId]
    )
    legislator.service_history = svc.rows
  } catch (err) {
    console.error(
      'Error running SERVICE_HISTORY query [SELECT … FROM service_history WHERE legislator_id = $1]:',
      err.stack || err.message
    )
    return res.status(500).json({ error: 'Failed to fetch service history' })
  }

  // 3) Committee assignments
  try {
    const comm = await db.query(
      `
      SELECT
        id               AS committee_id,
        committee_name   AS name,
        role,
        congress,
        subcommittee_name
      FROM committee_assignments
      WHERE legislator_id = $1
      `,
      [legislatorId]
    )
    legislator.committees = comm.rows
  } catch (err) {
    console.error(
      'Error running COMMITTEE_ASSIGNMENTS query [SELECT … FROM committee_assignments WHERE legislator_id = $1]:',
      err.stack || err.message
    )
    return res.status(500).json({ error: 'Failed to fetch committee assignments' })
  }

  // 4) Leadership roles
  try {
    const lead = await db.query(
      `
      SELECT
        id        AS leadership_id,
        congress,
        role      AS title
      FROM leadership_roles
      WHERE legislator_id = $1
      `,
      [legislatorId]
    )
    legislator.leadership_positions = lead.rows
  } catch (err) {
    console.error(
      'Error running LEADERSHIP_ROLES query [SELECT … FROM leadership_roles WHERE legislator_id = $1]:',
      err.stack || err.message
    )
    return res.status(500).json({ error: 'Failed to fetch leadership roles' })
  }

  // 5) Sponsored bills
  try {
    const bills = await db.query(
      `
      SELECT
        bill_number        AS bill_id,
        title,
        sponsorship_type   AS type,
        status,
        date_introduced    AS date
      FROM bill_sponsorships
      WHERE legislator_id = $1
      ORDER BY date_introduced DESC
      LIMIT 10
      `,
      [legislatorId]
    )
    legislator.sponsored_bills = bills.rows
  } catch (err) {
    console.error(
      'Error running BILL_SPONSORSHIPS query [SELECT … FROM bill_sponsorships WHERE legislator_id = $1]:',
      err.stack || err.message
    )
    return res.status(500).json({ error: 'Failed to fetch sponsored bills' })
  }

  // 6) Campaign finance
  try {
    const finance = await db.query(
      `
      SELECT
        cycle,
        total_raised          AS total_contributions,
        industry_breakdown    AS top_industries
      FROM campaign_finance
      WHERE legislator_id = $1
      `,
      [legislatorId]
    )
    legislator.finance_summary = finance.rows[0] || {}
  } catch (err) {
    console.error(
      'Error running CAMPAIGN_FINANCE query [SELECT … FROM campaign_finance WHERE legislator_id = $1]:',
      err.stack || err.message
    )
    return res.status(500).json({ error: 'Failed to fetch finance summary' })
  }

  // 7) Recent votes
  try {
    const votes = await db.query(
      `
      SELECT
        vs.date,
        vs.bill_id    AS bill,
        vr.vote_cast  AS position,
        vr.vote_session_id
      FROM vote_records vr
      JOIN vote_sessions vs
        ON vr.vote_session_id = vs.id
      WHERE vr.legislator_id = $1
      ORDER BY vs.date DESC
      LIMIT 20
      `,
      [legislatorId]
    )
    legislator.recent_votes = votes.rows
  } catch (err) {
    console.error(
      'Error running RECENT_VOTES query [SELECT … FROM vote_records JOIN vote_sessions WHERE vr.legislator_id = $1]:',
      err.stack || err.message
    )
    return res.status(500).json({ error: 'Failed to fetch recent votes' })
  }

  // All queries succeeded; return assembled profile
  return res.json(legislator)
})

module.exports = router
