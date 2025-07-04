
-- Updated SCHEMA.sql for Digital Congressional Accountability Platform

CREATE TABLE legislators (
    id SERIAL PRIMARY KEY,
    bioguide_id VARCHAR(12) UNIQUE NOT NULL,
    full_name TEXT,
    party VARCHAR(10),
    state CHAR(2),
    district INT,
    chamber VARCHAR(10),
    portrait_url TEXT,
    official_website_url TEXT,
    office_contact JSONB,
    bio_snapshot TEXT
);

CREATE TABLE service_history (
    id SERIAL PRIMARY KEY,
    legislator_id INT REFERENCES legislators(id),
    term_start DATE,
    term_end DATE
);

CREATE TABLE leadership_roles (
    id SERIAL PRIMARY KEY,
    legislator_id INT REFERENCES legislators(id),
    role TEXT
);

CREATE TABLE committee_assignments (
    id SERIAL PRIMARY KEY,
    legislator_id INT REFERENCES legislators(id),
    committee_name TEXT,
    subcommittee_name TEXT,
    role TEXT
);

CREATE TABLE votes (
    id SERIAL PRIMARY KEY,
    legislator_id INT REFERENCES legislators(id),
    vote_id VARCHAR(50),
    bill_number VARCHAR(20),
    question_text TEXT,
    vote_description TEXT,
    vote_result TEXT,
    position VARCHAR(10),
    date DATE,
    tally_yea INT,
    tally_nay INT,
    tally_present INT,
    tally_not_voting INT,
    is_key_vote BOOLEAN DEFAULT FALSE
);

CREATE TABLE campaign_finance (
    id SERIAL PRIMARY KEY,
    legislator_id INT REFERENCES legislators(id),
    total_received NUMERIC,
    top_contributors JSONB,
    top_industries JSONB
);

CREATE TABLE sponsored_bills (
    id SERIAL PRIMARY KEY,
    legislator_id INT REFERENCES legislators(id),
    bill_number VARCHAR(20),
    title TEXT,
    status TEXT,
    policy_area TEXT,
    date_introduced DATE
);

CREATE TABLE cosponsored_bills (
    id SERIAL PRIMARY KEY,
    legislator_id INT REFERENCES legislators(id),
    bill_number VARCHAR(20),
    title TEXT,
    status TEXT,
    policy_area TEXT,
    date_introduced DATE
);
