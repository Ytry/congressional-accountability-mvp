-- =============================================
-- DIGITAL CONGRESSIONAL ACCOUNTABILITY PLATFORM
-- FULL SCHEMA MIGRATION SCRIPT
-- (Includes FEC candidates & expanded campaign_finance)
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
DROP TABLE IF EXISTS staff_members CASCADE;
DROP TABLE IF EXISTS floor_speeches CASCADE;
DROP TABLE IF EXISTS hearings CASCADE;
DROP TABLE IF EXISTS hearing_participants CASCADE;
DROP TABLE IF EXISTS official_letters CASCADE;
DROP TABLE IF EXISTS letter_cosigners CASCADE;
DROP TABLE IF EXISTS lobbying_filings CASCADE;
DROP TABLE IF EXISTS lobbying_targets CASCADE;
DROP TABLE IF EXISTS press_releases CASCADE;
DROP TABLE IF EXISTS tweets CASCADE;
DROP TABLE IF EXISTS facebook_posts CASCADE;
DROP TABLE IF EXISTS youtube_videos CASCADE;
DROP TABLE IF EXISTS travel_disclosures CASCADE;
DROP TABLE IF EXISTS caucuses CASCADE;
DROP TABLE IF EXISTS caucus_memberships CASCADE;
DROP TABLE IF EXISTS bill_opinions CASCADE;

-- ===== LEGISLATORS =====
CREATE TABLE legislators (
    id                   SERIAL PRIMARY KEY,
    bioguide_id          TEXT    UNIQUE     NOT NULL,
    icpsr_id             TEXT,
    first_name           TEXT,
    last_name            TEXT,
    full_name            TEXT                 NOT NULL,
    gender               TEXT,
    birthday             DATE,
    party                TEXT,
    state                TEXT,
    district             INT,
    chamber              TEXT    CHECK (chamber IN ('House','Senate')),
    portrait_url         TEXT,
    official_website_url TEXT,
    office_contact       JSONB,
    bio_snapshot         TEXT,
    created_at           TIMESTAMPTZ DEFAULT NOW()
);

-- ===== SERVICE HISTORY =====
CREATE TABLE service_history (
    id             SERIAL PRIMARY KEY,
    legislator_id  INT    REFERENCES legislators(id) ON DELETE CASCADE,
    start_date     DATE   NOT NULL,
    end_date       DATE,
    chamber        TEXT,
    state          TEXT,
    district       INT,
    party          TEXT,
    UNIQUE (legislator_id, start_date)
);

-- ===== LEADERSHIP ROLES =====
CREATE TABLE leadership_roles (
    id             SERIAL PRIMARY KEY,
    legislator_id  INT    REFERENCES legislators(id) ON DELETE CASCADE,
    congress       INT,
    role           TEXT   NOT NULL,
    UNIQUE (legislator_id, congress, role)
);

-- ===== COMMITTEE ASSIGNMENTS =====
CREATE TABLE committee_assignments (
    id                SERIAL PRIMARY KEY,
    legislator_id     INT    REFERENCES legislators(id) ON DELETE CASCADE,
    congress          INT,
    committee_name    TEXT   NOT NULL,
    subcommittee_name TEXT,
    role              TEXT
);
CREATE UNIQUE INDEX uq_committee_assignments
  ON committee_assignments(
    legislator_id,
    congress,
    committee_name,
    COALESCE(subcommittee_name, '')
  );

-- ===== VOTE SESSIONS =====
CREATE TABLE vote_sessions (
    id          SERIAL PRIMARY KEY,
    vote_id     TEXT   UNIQUE   NOT NULL,
    congress    INT    NOT NULL,
    chamber     TEXT   CHECK (chamber IN ('house','senate')) NOT NULL,
    date        DATE   NOT NULL,
    question    TEXT,
    description TEXT,
    result      TEXT,
    key_vote    BOOLEAN DEFAULT FALSE,
    bill_id     TEXT
);

-- ===== VOTE RECORDS =====
CREATE TABLE vote_records (
    id               SERIAL PRIMARY KEY,
    vote_session_id  INT    REFERENCES vote_sessions(id) ON DELETE CASCADE,
    legislator_id    INT    REFERENCES legislators(id)    ON DELETE CASCADE,
    vote_cast        TEXT   CHECK (vote_cast IN 
                     ('Yea','Nay','Present','Not Voting','Absent','Unknown')) NOT NULL,
    UNIQUE (vote_session_id, legislator_id)
);

-- ===== CAMPAIGN FINANCE =====
CREATE TABLE campaign_finance (
    id                     SERIAL PRIMARY KEY,
    legislator_id          INT     REFERENCES legislators(id) ON DELETE CASCADE,
    cycle                  INT     NOT NULL,
    total_raised           NUMERIC,
    total_spent            NUMERIC,
    other_federal_receipts NUMERIC,
    top_donors             JSONB,
    industry_breakdown     JSONB,
    top_spenders           JSONB,
    payee_breakdown        JSONB,
    UNIQUE (legislator_id, cycle)
);

-- ===== ELECTION HISTORY =====
CREATE TABLE election_history (
    id             SERIAL PRIMARY KEY,
    legislator_id  INT    REFERENCES legislators(id) ON DELETE CASCADE,
    cycle          INT    NOT NULL,
    state          TEXT,
    district       INT,
    won            BOOLEAN,
    vote_percent   NUMERIC,
    opponent       TEXT,
    UNIQUE (legislator_id, cycle)
);

-- ===== BILL SPONSORSHIPS =====
CREATE TABLE bill_sponsorships (
    id                SERIAL PRIMARY KEY,
    legislator_id     INT    REFERENCES legislators(id) ON DELETE CASCADE,
    bill_number       TEXT   NOT NULL,
    sponsorship_type  TEXT   CHECK (sponsorship_type IN ('Sponsor','Cosponsor')),
    title             TEXT,
    status            TEXT,
    policy_area       TEXT,
    date_introduced   DATE,
    UNIQUE (legislator_id, bill_number, sponsorship_type)
);

-- ===== STAFF MEMBERS =====
CREATE TABLE staff_members (
    staff_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    title TEXT,
    office TEXT,
    chamber TEXT,
    phone TEXT,
    email_opt TEXT,
    legislator_id INT REFERENCES legislators(id),
    committee_name TEXT,
    date_range DATERANGE
);

-- ===== FLOOR SPEECHES =====
CREATE TABLE floor_speeches (
    speech_id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    chamber TEXT CHECK (chamber IN ('House', 'Senate')),
    legislator_id INT REFERENCES legislators(id),
    text TEXT,
    context TEXT
);

-- ===== HEARINGS =====
CREATE TABLE hearings (
    hearing_id SERIAL PRIMARY KEY,
    committee_name TEXT NOT NULL,
    date DATE NOT NULL,
    topic TEXT,
    document_link TEXT
);

CREATE TABLE hearing_participants (
    id SERIAL PRIMARY KEY,
    hearing_id INT REFERENCES hearings(hearing_id) ON DELETE CASCADE,
    legislator_id INT REFERENCES legislators(id),
    role TEXT
);

-- ===== OFFICIAL LETTERS =====
CREATE TABLE official_letters (
    letter_id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    originator TEXT,
    target_agency TEXT,
    subject TEXT,
    url TEXT
);

CREATE TABLE letter_cosigners (
    id SERIAL PRIMARY KEY,
    letter_id INT REFERENCES official_letters(letter_id) ON DELETE CASCADE,
    legislator_id INT REFERENCES legislators(id)
);

-- ===== LOBBYING DISCLOSURES =====
CREATE TABLE lobbying_filings (
    filing_id TEXT PRIMARY KEY,
    date_received DATE,
    client TEXT,
    registrant TEXT,
    filing_type TEXT,
    period TEXT,
    issues TEXT,
    amount NUMERIC
);

CREATE TABLE lobbying_targets (
    id SERIAL PRIMARY KEY,
    filing_id TEXT REFERENCES lobbying_filings(filing_id) ON DELETE CASCADE,
    target_type TEXT,
    target_name TEXT
);

-- ===== PRESS RELEASES =====
CREATE TABLE press_releases (
    pr_id SERIAL PRIMARY KEY,
    legislator_id INT REFERENCES legislators(id),
    date DATE,
    title TEXT,
    content TEXT,
    url TEXT,
    mentions_bill TEXT
);

-- ===== SOCIAL MEDIA POSTS =====
CREATE TABLE tweets (
    tweet_id TEXT PRIMARY KEY,
    legislator_id INT REFERENCES legislators(id),
    datetime TIMESTAMPTZ,
    text TEXT,
    retweets INT,
    likes INT,
    reply_count INT,
    media_links TEXT[]
);

CREATE TABLE facebook_posts (
    post_id TEXT PRIMARY KEY,
    legislator_id INT REFERENCES legislators(id),
    datetime TIMESTAMPTZ,
    text TEXT,
    reactions INT,
    shares INT,
    comments_count INT,
    url TEXT
);

CREATE TABLE youtube_videos (
    video_id TEXT PRIMARY KEY,
    legislator_id INT REFERENCES legislators(id),
    upload_datetime TIMESTAMPTZ,
    title TEXT,
    description TEXT,
    view_count INT,
    like_count INT,
    url TEXT
);

-- ===== TRAVEL DISCLOSURES =====
CREATE TABLE travel_disclosures (
    travel_id SERIAL PRIMARY KEY,
    legislator_id INT REFERENCES legislators(id),
    sponsor TEXT,
    destination TEXT,
    start_date DATE,
    end_date DATE,
    purpose TEXT,
    cost NUMERIC
);

-- ===== CAUCUSES =====
CREATE TABLE caucuses (
    caucus_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    congress_number INT
);

CREATE TABLE caucus_memberships (
    id SERIAL PRIMARY KEY,
    caucus_id INT REFERENCES caucuses(caucus_id),
    legislator_id INT REFERENCES legislators(id),
    role_in_caucus TEXT,
    join_date DATE
);

-- ===== PUBLIC OPINION =====
CREATE TABLE bill_opinions (
    id SERIAL PRIMARY KEY,
    bill_number TEXT,
    source TEXT,
    date DATE,
    support_percent NUMERIC,
    oppose_percent NUMERIC,
    sample_size INT,
    details TEXT
);

-- ===== FEC CANDIDATES =====
CREATE TABLE fec_candidates (
    fec_id       VARCHAR NOT NULL,
    cycle        SMALLINT NOT NULL,
    bioguide_id  VARCHAR NOT NULL REFERENCES legislators(bioguide_id),
    name         TEXT,
    office       VARCHAR,
    state        CHAR(2),
    district     INT,
    last_updated TIMESTAMPTZ,
    PRIMARY KEY (fec_id, cycle)
);
CREATE INDEX idx_fec_candidates_bioguide_cycle
  ON fec_candidates(bioguide_id, cycle);
