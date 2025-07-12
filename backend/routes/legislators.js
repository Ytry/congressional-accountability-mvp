const express = require('express');
const router = express.Router();
const db = require('../db');

// ─── GET paginated list of legislators ───────────────────────────────────────
router.get('/', async (req, res) => {
  const logger = req.logger;
  const page = parseInt(req.query.page, 10) || 1;
  const pageSize = parseInt(req.query.pageSize, 10) || 24;
  const offset = (page - 1) * pageSize;

  logger.info('Fetching paginated legislators', { page, pageSize });

  try {
    const countRes = await db.query('SELECT COUNT(*) FROM legislators');
    const totalCount = parseInt(countRes.rows[0].count, 10);
    logger.info('Total legislator count retrieved', { totalCount });

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

    const items = result.rows.map(row => ({
      ...row,
      portrait_url: `/portraits/${row.bioguide_id}.jpg`
    }));
    logger.info('Paginated legislators fetched', { returned: items.length });

    res.set('X-Total-Count', totalCount);
    return res.json({ items, totalCount, page, pageSize });
  } catch (err) {
    logger.error('Error fetching paginated legislators', { message: err.message, stack: err.stack });
    return res.status(500).json({ error: 'Failed to fetch legislators' });
  }
});

// ─── GET full profile for a single legislator ───────────────────────────────
router.get('/:bioguide_id', async (req, res) => {
  const logger = req.logger;
  const { bioguide_id } = req.params;
  logger.info('Fetching legislator profile', { bioguide_id });

  let legislator;
  let legislatorId;

  try {
    const coreRes = await db.query(
      `
      SELECT
        id,
        bioguide_id,
        first_name,
        last_name,
        party,
        state,
        district,
        chamber,
        portrait_url,
        official_website_url,
        office_contact,
        bio_snapshot AS bio
      FROM legislators
      WHERE bioguide_id = $1
      `,
      [bioguide_id]
    );

    if (!coreRes.rows.length) {
      logger.warn('Legislator not found', { bioguide_id });
      return res.status(404).json({ error: 'Legislator not found' });
    }

    legislatorId = coreRes.rows[0].id;
    const { id, ...clientLeg } = coreRes.rows[0];
    legislator = { ...clientLeg, portrait_url: `/portraits/${bioguide_id}.jpg` };
    logger.info('Core legislator data fetched', { legislatorId });
  } catch (err) {
    logger.error('Error fetching core legislator data', { bioguide_id, message: err.message, stack: err.stack });
    return res.status(500).json({ error: 'Failed to fetch core legislator info' });
  }

  // Helper to run subqueries and attach results
  async function fetchSection(query, params, sectionName) {
    try {
      const resDb = await db.query(query, params);
      logger.info(`Fetched ${sectionName}`, { count: resDb.rows.length });
      return resDb.rows;
    } catch (err) {
      logger.error(`Error fetching ${sectionName}`, { legislatorId, sectionName, message: err.message, stack: err.stack });
      throw new Error(`Failed to fetch ${sectionName}`);
    }
  }

  try {
    legislator.service_history = await fetchSection(
      `SELECT chamber, start_date, end_date FROM service_history WHERE legislator_id = $1 ORDER BY start_date DESC`,
      [legislatorId],
      'service_history'
    );

    legislator.committees = await fetchSection(
      `SELECT id AS committee_id, committee_name AS name, role, congress, subcommittee_name FROM committee_assignments WHERE legislator_id = $1`,
      [legislatorId],
      'committee_assignments'
    );

    legislator.leadership_positions = await fetchSection(
      `SELECT id AS leadership_id, congress, role AS title FROM leadership_roles WHERE legislator_id = $1`,
      [legislatorId],
      'leadership_roles'
    );

    legislator.sponsored_bills = await fetchSection(
      `SELECT bill_number AS bill_id, title, sponsorship_type AS type, status, date_introduced AS date FROM bill_sponsorships WHERE legislator_id = $1 ORDER BY date_introduced DESC LIMIT 10`,
      [legislatorId],
      'sponsored_bills'
    );

    legislator.finance_summary = (await fetchSection(
      `SELECT cycle, total_raised AS total_contributions, industry_breakdown AS top_industries FROM campaign_finance WHERE legislator_id = $1`,
      [legislatorId],
      'campaign_finance'
    ))[0] || {};

    legislator.recent_votes = await fetchSection(
      `SELECT vs.date, vs.bill_id AS bill, vr.vote_cast AS position, vr.vote_session_id FROM vote_records vr JOIN vote_sessions vs ON vr.vote_session_id = vs.id WHERE vr.legislator_id = $1 ORDER BY vs.date DESC LIMIT 20`,
      [legislatorId],
      'recent_votes'
    );
  } catch (subErr) {
    return res.status(500).json({ error: subErr.message });
  }

  logger.info('Assembled full legislator profile', { legislatorId });
  return res.json(legislator);
});

module.exports = router;
