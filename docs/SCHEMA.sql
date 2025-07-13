-- =============================================
-- DIGITAL CONGRESSIONAL ACCOUNTABILITY PLATFORM
-- FULL SCHEMA MIGRATION SCRIPT (PHASE 2–3 + FEC CANDIDATES)
-- =============================================

-- ========= DROP TABLES IF THEY EXIST =========

DROP TABLE IF EXISTS vote_records CASCADE;
DROP TABLE IF EXISTS vote_sessions CASCADE;
DROP TABLE IF EXISTS bill_sponsorships CASCADE;
DROP TABLE IF EXISTS election_history CASCADE;
DROP TABLE IF EXISTS campaign_finance CASCADE;
DROP TABLE IF EXISTS committee_assignments CASCADE;
DROP TABLE IF EXISTS leadership_roles CASCADE;
DROP TABLE IF EXISTS service_history CASCADE;
DROP TABLE IF EXISTS fec_candidates CASCADE;
DROP TABLE IF EXISTS legislators CASCADE;

-- ========= CREATE TABLES =========

-- ===== LEGISLATORS =====
CREATE TABLE legislators (
    id SERIAL PRIMARY KEY,
    bioguide_id TEXT UNIQUE NOT NULL,
    icpsr_id TEXT,
    first_name TEXT,
    last_name TEXT,
    full_name TEXT NOT NULL,
    gender TEXT,
    birthday DATE,
    party TEXT,
    state TEXT,
    district INT,
    chamber TEXT CHECK (chamber IN ('House', 'Senate')),
    portrait_url TEXT,
    official_website_url TEXT,
    office_contact JSONB,
    bio_snapshot TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ===== SERVICE HISTORY =====
CREATE TABLE service_history (
    id SERIAL PRIMARY KEY,
    legislator_id INT REFERENCES legislators(id) ON DELETE CASCADE,
    start_date DATE NOT NULL,
    end_date DATE,
    chamber TEXT,
    state TEXT,
    district INT,
    party TEXT,
    UNIQUE (legislator_id, start_date)
);

-- ===== LEADERSHIP ROLES =====
CREATE TABLE leadership_roles (
    id SERIAL PRIMARY KEY,
    legislator_id INT REFERENCES legislators(id) ON DELETE CASCADE,
    congress INT,
    role TEXT NOT NULL,
    UNIQUE (legislator_id, congress, role)
);

-- ===== COMMITTEE ASSIGNMENTS =====
CREATE TABLE committee_assignments (
    id SERIAL PRIMARY KEY,
    legislator_id INT REFERENCES legislators(id) ON DELETE CASCADE,
    congress INT,
    committee_name TEXT NOT NULL,
    subcommittee_name TEXT,
    role TEXT
);

-- Preserve COALESCE uniqueness via a functional unique index
CREATE UNIQUE INDEX uq_committee_assignments
  ON committee_assignments(
    legislator_id,
    congress,
    committee_name,
    COALESCE(subcommittee_name, '')
  );

-- ===== VOTE SESSIONS =====
CREATE TABLE vote_sessions (
    id SERIAL PRIMARY KEY,
    vote_id TEXT UNIQUE NOT NULL,
    congress INT NOT NULL,
    chamber TEXT CHECK (chamber IN ('house', 'senate')) NOT NULL,
    date DATE NOT NULL,
    question TEXT,
    description TEXT,
    result TEXT,
    key_vote BOOLEAN DEFAULT FALSE,
    bill_id TEXT
);

-- ===== VOTE RECORDS =====
CREATE TABLE vote_records (
    id SERIAL PRIMARY KEY,
    vote_session_id INT REFERENCES vote_sessions(id) ON DELETE CASCADE,
    legislator_id INT REFERENCES legislators(id) ON DELETE CASCADE,
    vote_cast TEXT CHECK (vote_cast IN ('Yea', 'Nay', 'Present', 'Not Voting', 'Absent', 'Unknown')) NOT NULL,
    UNIQUE (vote_session_id, legislator_id)
);

-- ===== CAMPAIGN FINANCE =====
CREATE TABLE campaign_finance (
    id SERIAL PRIMARY KEY,
    legislator_id INT REFERENCES legislators(id) ON DELETE CASCADE,
    cycle INT NOT NULL,
    total_raised NUMERIC,
    total_spent NUMERIC,
    top_donors JSONB,
    industry_breakdown JSONB,
    UNIQUE (legislator_id, cycle)
);

-- ===== ELECTION HISTORY =====
CREATE TABLE election_history (
    id SERIAL PRIMARY KEY,
    legislator_id INT REFERENCES legislators(id) ON DELETE CASCADE,
    cycle INT NOT NULL,
    state TEXT,
    district INT,
    won BOOLEAN,
    vote_percent NUMERIC,
    opponent TEXT,
    UNIQUE (legislator_id, cycle)
);

-- ===== BILL SPONSORSHIPS =====
CREATE TABLE bill_sponsorships (
    id SERIAL PRIMARY KEY,
    legislator_id INT REFERENCES legislators(id) ON DELETE CASCADE,
    bill_number TEXT NOT NULL,
    sponsorship_type TEXT CHECK (sponsorship_type IN ('Sponsor', 'Cosponsor')),
    title TEXT,
    status TEXT,
    policy_area TEXT,
    date_introduced DATE,
    UNIQUE (legislator_id, bill_number, sponsorship_type)
);

-- ===== FEC CANDIDATES =====
-- Maps FEC candidate IDs to Bioguide IDs for integration with OpenFEC data
CREATE TABLE fec_candidates (
    fec_id       VARCHAR PRIMARY KEY,
    bioguide_id  VARCHAR NOT NULL REFERENCES legislators(bioguide_id),
    name         TEXT,
    office       VARCHAR,
    state        CHAR(2),
    district     INT,
    cycle        SMALLINT,
    last_updated TIMESTAMPTZ
);

-- (Optional) Index to speed lookups by bioguide and cycle
CREATE INDEX idx_fec_candidates_bioguide_cycle
  ON fec_candidates(bioguide_id, cycle);
