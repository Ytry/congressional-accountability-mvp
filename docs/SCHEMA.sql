
/* ------------------------------------------------------------------
   CORE TABLE: legislators
------------------------------------------------------------------ */
DROP TABLE IF EXISTS legislators CASCADE;

CREATE TABLE legislators (
    id               SERIAL PRIMARY KEY,
    bioguide_id      VARCHAR(12)  NOT NULL UNIQUE,      -- <-- for ON CONFLICT
    full_name        TEXT         NOT NULL,
    party            CHAR(1)      CHECK (party IN ('R','D','I')),
    chamber          TEXT         CHECK (chamber IN ('House','Senate')),
    state            CHAR(2)      NOT NULL,
    district         INT,
    portrait_url     TEXT,
    official_website_url TEXT,
    office_contact   JSONB,                              -- address & phone live here
    bio_snapshot     TEXT,
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

/* Helpful, non-blocking indexes                                       */
CREATE INDEX idx_leg_state_dist  ON legislators (state, district);
CREATE INDEX idx_leg_party       ON legislators (party);

/* ------------------------------------------------------------------
   SERVICE HISTORY & ROLES
------------------------------------------------------------------ */
DROP TABLE IF EXISTS service_history CASCADE;
CREATE TABLE service_history (
    id            SERIAL PRIMARY KEY,
    legislator_id INT REFERENCES legislators(id) ON DELETE CASCADE,
    term_start    DATE NOT NULL,
    term_end      DATE,
    UNIQUE (legislator_id, term_start)               -- no duplicate rows
);

DROP TABLE IF EXISTS leadership_roles CASCADE;
CREATE TABLE leadership_roles (
    id            SERIAL PRIMARY KEY,
    legislator_id INT REFERENCES legislators(id) ON DELETE CASCADE,
    congress      INT,                               -- 118, 119, …
    role          TEXT NOT NULL,
    UNIQUE (legislator_id, congress, role)
);

/* ------------------------------------------------------------------
   COMMITTEE ASSIGNMENTS
------------------------------------------------------------------ */
DROP TABLE IF EXISTS committee_assignments CASCADE;
CREATE TABLE committee_assignments (
    id               SERIAL PRIMARY KEY,
    legislator_id    INT REFERENCES legislators(id) ON DELETE CASCADE,
    congress         INT,
    committee_name   TEXT NOT NULL,
    subcommittee_name TEXT,
    role             TEXT,                           -- Member | Chair | Ranking Member
    UNIQUE (legislator_id, congress, committee_name, COALESCE(subcommittee_name, ''))
);

/* ------------------------------------------------------------------
   INDIVIDUAL VOTES
------------------------------------------------------------------ */
DROP TABLE IF EXISTS votes CASCADE;
CREATE TABLE votes (
    id                  SERIAL PRIMARY KEY,
    legislator_id       INT REFERENCES legislators(id) ON DELETE CASCADE,
    vote_id             TEXT   NOT NULL,              -- “senate-118-1-329”
    bill_number         TEXT,
    question_text       TEXT,
    vote_description    TEXT,
    vote_result         TEXT,
    position            TEXT      CHECK (position IN ('Yea','Nay','Present','Not Voting')),
    vote_date           DATE,
    tally_yea           INT,
    tally_nay           INT,
    tally_present       INT,
    tally_not_voting    INT,
    is_key_vote         BOOLEAN DEFAULT FALSE,
    UNIQUE (legislator_id, vote_id)                   -- for ON CONFLICT update
);

/* ------------------------------------------------------------------
   CAMPAIGN FINANCE SNAPSHOT
------------------------------------------------------------------ */
DROP TABLE IF EXISTS campaign_finance CASCADE;
CREATE TABLE campaign_finance (
    id               SERIAL PRIMARY KEY,
    legislator_id    INT REFERENCES legislators(id) ON DELETE CASCADE,
    cycle            INT  NOT NULL,                  -- election cycle (e.g. 2024)
    total_received   NUMERIC,
    top_contributors JSONB,
    top_industries   JSONB,
    UNIQUE (legislator_id, cycle)
);

/* ------------------------------------------------------------------
   BILLS – SPONSORED & COSPONSORED
------------------------------------------------------------------ */
DROP TABLE IF EXISTS sponsored_bills CASCADE;
CREATE TABLE sponsored_bills (
    id               SERIAL PRIMARY KEY,
    legislator_id    INT REFERENCES legislators(id) ON DELETE CASCADE,
    bill_number      TEXT NOT NULL,
    title            TEXT,
    status           TEXT,
    policy_area      TEXT,
    date_introduced  DATE,
    UNIQUE (legislator_id, bill_number)
);

DROP TABLE IF EXISTS cosponsored_bills CASCADE;
CREATE TABLE cosponsored_bills (
    id               SERIAL PRIMARY KEY,
    legislator_id    INT REFERENCES legislators(id) ON DELETE CASCADE,
    bill_number      TEXT NOT NULL,
    title            TEXT,
    status           TEXT,
    policy_area      TEXT,
    date_introduced  DATE,
    UNIQUE (legislator_id, bill_number)
);
