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
DROP TABLE IF EXISTS interest_group_ratings CASCADE;
DROP TABLE IF EXISTS financial_disclosures CASCADE;
DROP TABLE IF EXISTS office_expenses CASCADE;

-- ===== LEGISLATORS =====
CREATE TABLE legislators (
    id                   SERIAL PRIMARY KEY,
    bioguide_id          TEXT    UNIQUE     NOT NULL,
    icpsr_id             TEXT,
    first_name           TEXT               NOT NULL,
    last_name            TEXT               NOT NULL,
    full_name            TEXT               NOT NULL,
    gender               TEXT,
    birthday             DATE,
    party                TEXT               NOT NULL,
    state                TEXT               NOT NULL,
    district             INT,
    chamber              TEXT    CHECK (chamber IN ('House','Senate')) NOT NULL,
    portrait_url         TEXT,
    official_website_url TEXT,
    office_contact       JSONB,
    bio_snapshot         TEXT,
    created_at           TIMESTAMPTZ DEFAULT NOW(),
    updated_at           TIMESTAMPTZ DEFAULT NOW(),
    source_url           TEXT
);
CREATE INDEX idx_legislators_bioguide_id ON legislators(bioguide_id);
CREATE INDEX idx_legislators_chamber ON legislators(chamber);
CREATE INDEX idx_legislators_state ON legislators(state);
CREATE INDEX idx_legislators_party ON legislators(party);
CREATE INDEX idx_legislators_updated_at ON legislators(updated_at);

-- ===== SERVICE HISTORY =====
CREATE TABLE service_history (
    id             SERIAL PRIMARY KEY,
    legislator_id  INT    REFERENCES legislators(id) ON DELETE CASCADE,
    start_date     DATE   NOT NULL,
    end_date       DATE,
    chamber        TEXT   NOT NULL,
    state          TEXT   NOT NULL,
    district       INT,
    party          TEXT   NOT NULL,
    created_at     TIMESTAMPTZ DEFAULT NOW(),
    updated_at     TIMESTAMPTZ DEFAULT NOW(),
    source_url     TEXT,
    UNIQUE (legislator_id, start_date),
    CHECK (end_date IS NULL OR end_date > start_date)
);
CREATE INDEX idx_service_history_legislator_id ON service_history(legislator_id);
CREATE INDEX idx_service_history_start_date ON service_history(start_date);
CREATE INDEX idx_service_history_end_date ON service_history(end_date);
CREATE INDEX idx_service_history_updated_at ON service_history(updated_at);

-- ===== LEADERSHIP ROLES =====
CREATE TABLE leadership_roles (
    id             SERIAL PRIMARY KEY,
    legislator_id  INT    REFERENCES legislators(id) ON DELETE CASCADE,
    congress       INT    NOT NULL,
    role           TEXT   NOT NULL CHECK (role IN ('Speaker', 'Majority Leader', 'Minority Leader', 'Majority Whip', 'Minority Whip', 'Conference Chair', 'Other')), -- Expanded enum based on common roles
    created_at     TIMESTAMPTZ DEFAULT NOW(),
    updated_at     TIMESTAMPTZ DEFAULT NOW(),
    source_url     TEXT,
    UNIQUE (legislator_id, congress, role)
);
CREATE INDEX idx_leadership_roles_legislator_id ON leadership_roles(legislator_id);
CREATE INDEX idx_leadership_roles_congress ON leadership_roles(congress);
CREATE INDEX idx_leadership_roles_updated_at ON leadership_roles(updated_at);

-- ===== COMMITTEE ASSIGNMENTS =====
CREATE TABLE committee_assignments (
    id                SERIAL PRIMARY KEY,
    legislator_id     INT    REFERENCES legislators(id) ON DELETE CASCADE,
    congress          INT    NOT NULL,
    committee_name    TEXT   NOT NULL,
    subcommittee_name TEXT,
    role              TEXT   CHECK (role IN ('Chair', 'Ranking Member', 'Member', 'Vice Chair', 'Other')), -- Expanded enum for roles
    created_at        TIMESTAMPTZ DEFAULT NOW(),
    updated_at        TIMESTAMPTZ DEFAULT NOW(),
    source_url        TEXT
);
CREATE UNIQUE INDEX uq_committee_assignments
  ON committee_assignments(
    legislator_id,
    congress,
    committee_name,
    COALESCE(subcommittee_name, '')
  );
CREATE INDEX idx_committee_assignments_legislator_id ON committee_assignments(legislator_id);
CREATE INDEX idx_committee_assignments_congress ON committee_assignments(congress);
CREATE INDEX idx_committee_assignments_committee_name ON committee_assignments(committee_name);
CREATE INDEX idx_committee_assignments_updated_at ON committee_assignments(updated_at);

-- ===== VOTE SESSIONS =====
CREATE TABLE vote_sessions (
    id              SERIAL PRIMARY KEY,
    vote_id         TEXT   UNIQUE   NOT NULL,
    congress        INT    NOT NULL,
    chamber         TEXT   CHECK (chamber IN ('house','senate')) NOT NULL,
    date            DATE   NOT NULL,
    question        TEXT,
    description     TEXT,
    result          TEXT,
    key_vote        BOOLEAN DEFAULT FALSE,
    bill_id         TEXT,
    tally_yea       INT    DEFAULT 0,
    tally_nay       INT    DEFAULT 0,
    tally_present   INT    DEFAULT 0,
    tally_not_voting INT   DEFAULT 0,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    source_url      TEXT
);
CREATE INDEX idx_vote_sessions_congress ON vote_sessions(congress);
CREATE INDEX idx_vote_sessions_chamber ON vote_sessions(chamber);
CREATE INDEX idx_vote_sessions_date ON vote_sessions(date);
CREATE INDEX idx_vote_sessions_bill_id ON vote_sessions(bill_id);
CREATE INDEX idx_vote_sessions_updated_at ON vote_sessions(updated_at);

-- ===== VOTE RECORDS =====
CREATE TABLE vote_records (
    id               SERIAL PRIMARY KEY,
    vote_session_id  INT    REFERENCES vote_sessions(id) ON DELETE CASCADE,
    legislator_id    INT    REFERENCES legislators(id)    ON DELETE CASCADE,
    vote_cast        TEXT   CHECK (vote_cast IN 
                     ('Yea','Nay','Present','Not Voting','Absent','Unknown')) NOT NULL,
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    updated_at       TIMESTAMPTZ DEFAULT NOW(),
    source_url       TEXT,
    UNIQUE (vote_session_id, legislator_id)
);
CREATE INDEX idx_vote_records_vote_session_id ON vote_records(vote_session_id);
CREATE INDEX idx_vote_records_legislator_id ON vote_records(legislator_id);
CREATE INDEX idx_vote_records_updated_at ON vote_records(updated_at);

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
    created_at             TIMESTAMPTZ DEFAULT NOW(),
    updated_at             TIMESTAMPTZ DEFAULT NOW(),
    source_url             TEXT,
    UNIQUE (legislator_id, cycle)
);
CREATE INDEX idx_campaign_finance_legislator_id ON campaign_finance(legislator_id);
CREATE INDEX idx_campaign_finance_cycle ON campaign_finance(cycle);
CREATE INDEX idx_campaign_finance_updated_at ON campaign_finance(updated_at);
CREATE INDEX gin_campaign_finance_top_donors ON campaign_finance USING GIN (top_donors);
CREATE INDEX gin_campaign_finance_industry_breakdown ON campaign_finance USING GIN (industry_breakdown);
CREATE INDEX gin_campaign_finance_top_spenders ON campaign_finance USING GIN (top_spenders);
CREATE INDEX gin_campaign_finance_payee_breakdown ON campaign_finance USING GIN (payee_breakdown);

-- ===== ELECTION HISTORY =====
CREATE TABLE election_history (
    id             SERIAL PRIMARY KEY,
    legislator_id  INT    REFERENCES legislators(id) ON DELETE CASCADE,
    cycle          INT    NOT NULL,
    state          TEXT   NOT NULL,
    district       INT,
    won            BOOLEAN,
    vote_percent   NUMERIC,
    opponent       TEXT,
    created_at     TIMESTAMPTZ DEFAULT NOW(),
    updated_at     TIMESTAMPTZ DEFAULT NOW(),
    source_url     TEXT,
    UNIQUE (legislator_id, cycle)
);
CREATE INDEX idx_election_history_legislator_id ON election_history(legislator_id);
CREATE INDEX idx_election_history_cycle ON election_history(cycle);
CREATE INDEX idx_election_history_updated_at ON election_history(updated_at);

-- ===== BILL SPONSORSHIPS =====
CREATE TABLE bill_sponsorships (
    id                SERIAL PRIMARY KEY,
    legislator_id     INT    REFERENCES legislators(id) ON DELETE CASCADE,
    bill_number       TEXT   NOT NULL,
    sponsorship_type  TEXT   CHECK (sponsorship_type IN ('Sponsor','Cosponsor')) NOT NULL,
    title             TEXT,
    status            TEXT,
    policy_area       TEXT,
    date_introduced   DATE,
    created_at        TIMESTAMPTZ DEFAULT NOW(),
    updated_at        TIMESTAMPTZ DEFAULT NOW(),
    source_url        TEXT,
    UNIQUE (legislator_id, bill_number, sponsorship_type)
);
CREATE INDEX idx_bill_sponsorships_legislator_id ON bill_sponsorships(legislator_id);
CREATE INDEX idx_bill_sponsorships_bill_number ON bill_sponsorships(bill_number);
CREATE INDEX idx_bill_sponsorships_date_introduced ON bill_sponsorships(date_introduced);
CREATE INDEX idx_bill_sponsorships_updated_at ON bill_sponsorships(updated_at);

-- ===== STAFF MEMBERS =====
CREATE TABLE staff_members (
    staff_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    title TEXT NOT NULL,
    office TEXT NOT NULL,
    chamber TEXT CHECK (chamber IN ('House', 'Senate')) NOT NULL,
    phone TEXT,
    email_opt TEXT,
    legislator_id INT REFERENCES legislators(id) ON DELETE CASCADE,
    committee_name TEXT,
    date_range DATERANGE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    source_url TEXT
);
CREATE INDEX idx_staff_members_legislator_id ON staff_members(legislator_id);
CREATE INDEX idx_staff_members_committee_name ON staff_members(committee_name);
CREATE INDEX idx_staff_members_date_range ON staff_members USING GIST (date_range);
CREATE INDEX idx_staff_members_updated_at ON staff_members(updated_at);

-- ===== FLOOR SPEECHES =====
CREATE TABLE floor_speeches (
    speech_id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    chamber TEXT CHECK (chamber IN ('House', 'Senate')) NOT NULL,
    legislator_id INT REFERENCES legislators(id) ON DELETE CASCADE,
    text TEXT,
    context TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    source_url TEXT
);
CREATE INDEX idx_floor_speeches_legislator_id ON floor_speeches(legislator_id);
CREATE INDEX idx_floor_speeches_date ON floor_speeches(date);
CREATE INDEX idx_floor_speeches_updated_at ON floor_speeches(updated_at);
CREATE INDEX idx_floor_speeches_text_fts ON floor_speeches USING GIN (to_tsvector('english', text));

-- ===== HEARINGS =====
CREATE TABLE hearings (
    hearing_id SERIAL PRIMARY KEY,
    committee_name TEXT NOT NULL,
    date DATE NOT NULL,
    topic TEXT,
    document_link TEXT,
    transcript_text TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    source_url TEXT
);
CREATE INDEX idx_hearings_committee_name ON hearings(committee_name);
CREATE INDEX idx_hearings_date ON hearings(date);
CREATE INDEX idx_hearings_updated_at ON hearings(updated_at);
CREATE INDEX idx_hearings_transcript_text_fts ON hearings USING GIN (to_tsvector('english', transcript_text));

CREATE TABLE hearing_participants (
    id SERIAL PRIMARY KEY,
    hearing_id INT REFERENCES hearings(hearing_id) ON DELETE CASCADE,
    legislator_id INT REFERENCES legislators(id) ON DELETE CASCADE,
    role TEXT CHECK (role IN ('Chair', 'Ranking Member', 'Member', 'Witness', 'Other')), -- Expanded enum
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    source_url TEXT
);
CREATE INDEX idx_hearing_participants_hearing_id ON hearing_participants(hearing_id);
CREATE INDEX idx_hearing_participants_legislator_id ON hearing_participants(legislator_id);
CREATE INDEX idx_hearing_participants_updated_at ON hearing_participants(updated_at);

-- ===== OFFICIAL LETTERS =====
CREATE TABLE official_letters (
    letter_id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    originator TEXT NOT NULL,
    target_agency TEXT NOT NULL,
    subject TEXT,
    url TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    source_url TEXT
);
CREATE INDEX idx_official_letters_date ON official_letters(date);
CREATE INDEX idx_official_letters_target_agency ON official_letters(target_agency);
CREATE INDEX idx_official_letters_updated_at ON official_letters(updated_at);

CREATE TABLE letter_cosigners (
    id SERIAL PRIMARY KEY,
    letter_id INT REFERENCES official_letters(letter_id) ON DELETE CASCADE,
    legislator_id INT REFERENCES legislators(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    source_url TEXT
);
CREATE INDEX idx_letter_cosigners_letter_id ON letter_cosigners(letter_id);
CREATE INDEX idx_letter_cosigners_legislator_id ON letter_cosigners(legislator_id);
CREATE INDEX idx_letter_cosigners_updated_at ON letter_cosigners(updated_at);

-- ===== LOBBYING DISCLOSURES =====
CREATE TABLE lobbying_filings (
    filing_id TEXT PRIMARY KEY,
    date_received DATE NOT NULL,
    client TEXT NOT NULL,
    registrant TEXT NOT NULL,
    filing_type TEXT NOT NULL,
    period TEXT NOT NULL,
    issues JSONB, -- Changed to JSONB for multi-valued issues
    amount NUMERIC,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    source_url TEXT
);
CREATE INDEX idx_lobbying_filings_date_received ON lobbying_filings(date_received);
CREATE INDEX idx_lobbying_filings_updated_at ON lobbying_filings(updated_at);
CREATE INDEX gin_lobbying_filings_issues ON lobbying_filings USING GIN (issues);

CREATE TABLE lobbying_targets (
    id SERIAL PRIMARY KEY,
    filing_id TEXT REFERENCES lobbying_filings(filing_id) ON DELETE CASCADE,
    target_type TEXT NOT NULL,
    target_name TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    source_url TEXT
);
CREATE INDEX idx_lobbying_targets_filing_id ON lobbying_targets(filing_id);
CREATE INDEX idx_lobbying_targets_updated_at ON lobbying_targets(updated_at);

-- ===== PRESS RELEASES =====
CREATE TABLE press_releases (
    pr_id SERIAL PRIMARY KEY,
    legislator_id INT REFERENCES legislators(id) ON DELETE CASCADE,
    date DATE NOT NULL,
    title TEXT NOT NULL,
    content TEXT,
    url TEXT,
    mentions_bill TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    source_url TEXT
);
CREATE INDEX idx_press_releases_legislator_id ON press_releases(legislator_id);
CREATE INDEX idx_press_releases_date ON press_releases(date);
CREATE INDEX idx_press_releases_updated_at ON press_releases(updated_at);
CREATE INDEX idx_press_releases_content_fts ON press_releases USING GIN (to_tsvector('english', content));

-- ===== SOCIAL MEDIA POSTS =====
CREATE TABLE tweets (
    tweet_id TEXT PRIMARY KEY,
    legislator_id INT REFERENCES legislators(id) ON DELETE CASCADE,
    datetime TIMESTAMPTZ NOT NULL,
    text TEXT NOT NULL,
    retweets INT DEFAULT 0,
    likes INT DEFAULT 0,
    reply_count INT DEFAULT 0,
    media_links TEXT[],
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    source_url TEXT
);
CREATE INDEX idx_tweets_legislator_id ON tweets(legislator_id);
CREATE INDEX idx_tweets_datetime ON tweets(datetime);
CREATE INDEX idx_tweets_updated_at ON tweets(updated_at);
CREATE INDEX idx_tweets_text_fts ON tweets USING GIN (to_tsvector('english', text));

CREATE TABLE facebook_posts (
    post_id TEXT PRIMARY KEY,
    legislator_id INT REFERENCES legislators(id) ON DELETE CASCADE,
    datetime TIMESTAMPTZ NOT NULL,
    text TEXT NOT NULL,
    reactions INT DEFAULT 0,
    shares INT DEFAULT 0,
    comments_count INT DEFAULT 0,
    url TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    source_url TEXT
);
CREATE INDEX idx_facebook_posts_legislator_id ON facebook_posts(legislator_id);
CREATE INDEX idx_facebook_posts_datetime ON facebook_posts(datetime);
CREATE INDEX idx_facebook_posts_updated_at ON facebook_posts(updated_at);
CREATE INDEX idx_facebook_posts_text_fts ON facebook_posts USING GIN (to_tsvector('english', text));

CREATE TABLE youtube_videos (
    video_id TEXT PRIMARY KEY,
    legislator_id INT REFERENCES legislators(id) ON DELETE CASCADE,
    upload_datetime TIMESTAMPTZ NOT NULL,
    title TEXT NOT NULL,
    description TEXT,
    view_count INT DEFAULT 0,
    like_count INT DEFAULT 0,
    url TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    source_url TEXT
);
CREATE INDEX idx_youtube_videos_legislator_id ON youtube_videos(legislator_id);
CREATE INDEX idx_youtube_videos_upload_datetime ON youtube_videos(upload_datetime);
CREATE INDEX idx_youtube_videos_updated_at ON youtube_videos(updated_at);
CREATE INDEX idx_youtube_videos_description_fts ON youtube_videos USING GIN (to_tsvector('english', description));

-- ===== TRAVEL DISCLOSURES =====
CREATE TABLE travel_disclosures (
    travel_id SERIAL PRIMARY KEY,
    legislator_id INT REFERENCES legislators(id) ON DELETE CASCADE,
    sponsor TEXT NOT NULL,
    destination TEXT NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    purpose TEXT,
    cost NUMERIC,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    source_url TEXT,
    CHECK (end_date >= start_date)
);
CREATE INDEX idx_travel_disclosures_legislator_id ON travel_disclosures(legislator_id);
CREATE INDEX idx_travel_disclosures_start_date ON travel_disclosures(start_date);
CREATE INDEX idx_travel_disclosures_end_date ON travel_disclosures(end_date);
CREATE INDEX idx_travel_disclosures_updated_at ON travel_disclosures(updated_at);

-- ===== CAUCUSES =====
CREATE TABLE caucuses (
    caucus_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    congress_number INT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    source_url TEXT
);
CREATE INDEX idx_caucuses_congress_number ON caucuses(congress_number);
CREATE INDEX idx_caucuses_updated_at ON caucuses(updated_at);

CREATE TABLE caucus_memberships (
    id SERIAL PRIMARY KEY,
    caucus_id INT REFERENCES caucuses(caucus_id) ON DELETE CASCADE,
    legislator_id INT REFERENCES legislators(id) ON DELETE CASCADE,
    role_in_caucus TEXT CHECK (role_in_caucus IN ('Chair', 'Co-Chair', 'Member', 'Vice Chair', 'Other')), -- Expanded enum
    join_date DATE NOT NULL,
    end_date DATE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    source_url TEXT,
    CHECK (end_date IS NULL OR end_date > join_date)
);
CREATE INDEX idx_caucus_memberships_caucus_id ON caucus_memberships(caucus_id);
CREATE INDEX idx_caucus_memberships_legislator_id ON caucus_memberships(legislator_id);
CREATE INDEX idx_caucus_memberships_join_date ON caucus_memberships(join_date);
CREATE INDEX idx_caucus_memberships_end_date ON caucus_memberships(end_date);
CREATE INDEX idx_caucus_memberships_updated_at ON caucus_memberships(updated_at);

-- ===== PUBLIC OPINION =====
CREATE TABLE bill_opinions (
    id SERIAL PRIMARY KEY,
    bill_number TEXT NOT NULL,
    source TEXT NOT NULL,
    date DATE NOT NULL,
    support_percent NUMERIC,
    oppose_percent NUMERIC,
    sample_size INT,
    details TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    source_url TEXT
);
CREATE INDEX idx_bill_opinions_bill_number ON bill_opinions(bill_number);
CREATE INDEX idx_bill_opinions_date ON bill_opinions(date);
CREATE INDEX idx_bill_opinions_updated_at ON bill_opinions(updated_at);

-- ===== FEC CANDIDATES =====
CREATE TABLE fec_candidates (
    fec_id       VARCHAR(20) NOT NULL, -- Added length limit for consistency
    cycle        SMALLINT NOT NULL,
    bioguide_id  VARCHAR(20) NOT NULL REFERENCES legislators(bioguide_id),
    name         TEXT NOT NULL,
    office       VARCHAR(20) NOT NULL,
    state        CHAR(2) NOT NULL,
    district     INT,
    last_updated TIMESTAMPTZ NOT NULL,
    created_at   TIMESTAMPTZ DEFAULT NOW(),
    updated_at   TIMESTAMPTZ DEFAULT NOW(),
    source_url   TEXT,
    PRIMARY KEY (fec_id, cycle)
);
CREATE INDEX idx_fec_candidates_bioguide_cycle
  ON fec_candidates(bioguide_id, cycle);
CREATE INDEX idx_fec_candidates_updated_at ON fec_candidates(updated_at);

-- ===== INTEREST GROUP RATINGS =====
CREATE TABLE interest_group_ratings (
    id             SERIAL PRIMARY KEY,
    legislator_id  INT REFERENCES legislators(id) ON DELETE CASCADE,
    group_name     TEXT NOT NULL,
    score          NUMERIC,
    year           INT NOT NULL,
    created_at     TIMESTAMPTZ DEFAULT NOW(),
    updated_at     TIMESTAMPTZ DEFAULT NOW(),
    source_url     TEXT,
    UNIQUE (legislator_id, group_name, year)
);
CREATE INDEX idx_interest_group_ratings_legislator_id ON interest_group_ratings(legislator_id);
CREATE INDEX idx_interest_group_ratings_year ON interest_group_ratings(year);
CREATE INDEX idx_interest_group_ratings_updated_at ON interest_group_ratings(updated_at);

-- ===== FINANCIAL DISCLOSURES =====
CREATE TABLE financial_disclosures (
    id             SERIAL PRIMARY KEY,
    legislator_id  INT REFERENCES legislators(id) ON DELETE CASCADE,
    year           INT NOT NULL,
    assets         JSONB,
    liabilities    JSONB,
    income         JSONB,
    created_at     TIMESTAMPTZ DEFAULT NOW(),
    updated_at     TIMESTAMPTZ DEFAULT NOW(),
    source_url     TEXT,
    UNIQUE (legislator_id, year)
);
CREATE INDEX idx_financial_disclosures_legislator_id ON financial_disclosures(legislator_id);
CREATE INDEX idx_financial_disclosures_year ON financial_disclosures(year);
CREATE INDEX idx_financial_disclosures_updated_at ON financial_disclosures(updated_at);
CREATE INDEX gin_financial_disclosures_assets ON financial_disclosures USING GIN (assets);
CREATE INDEX gin_financial_disclosures_liabilities ON financial_disclosures USING GIN (liabilities);
CREATE INDEX gin_financial_disclosures_income ON financial_disclosures USING GIN (income);

-- ===== OFFICE EXPENSES =====
CREATE TABLE office_expenses (
    id             SERIAL PRIMARY KEY,
    legislator_id  INT REFERENCES legislators(id) ON DELETE CASCADE,
    quarter        TEXT NOT NULL, -- e.g., '2025-Q1'
    category       TEXT NOT NULL,
    amount         NUMERIC NOT NULL,
    created_at     TIMESTAMPTZ DEFAULT NOW(),
    updated_at     TIMESTAMPTZ DEFAULT NOW(),
    source_url     TEXT,
    UNIQUE (legislator_id, quarter, category)
);
CREATE INDEX idx_office_expenses_legislator_id ON office_expenses(legislator_id);
CREATE INDEX idx_office_expenses_quarter ON office_expenses(quarter);
CREATE INDEX idx_office_expenses_updated_at ON office_expenses(updated_at);
