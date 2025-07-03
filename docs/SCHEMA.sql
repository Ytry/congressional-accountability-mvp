-- SCHEMA.sql

CREATE TABLE legislators (
    id SERIAL PRIMARY KEY,
    bioguide_id VARCHAR(12) UNIQUE NOT NULL,
    full_name TEXT,
    party VARCHAR(10),
    state CHAR(2),
    district INT,
    chamber VARCHAR(10)
);

CREATE TABLE votes (
    id SERIAL PRIMARY KEY,
    legislator_id INT REFERENCES legislators(id),
    vote_id VARCHAR(50),
    bill_number VARCHAR(20),
    description TEXT,
    position VARCHAR(10)
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
    status TEXT
);