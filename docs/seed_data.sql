-- Enhanced SEED_DATA.sql for Refactored Schema

-- Insert into normalized lookup tables
INSERT INTO parties (name) VALUES
  ('Democratic'),
  ('Republican'),
  ('Independent')
ON CONFLICT DO NOTHING;

INSERT INTO chambers (name) VALUES
  ('House'),
  ('Senate')
ON CONFLICT DO NOTHING;

-- Insert legislators
INSERT INTO legislators (bioguide_id, full_name, party_id, state, district, chamber_id, start_date, in_office)
VALUES
  ('A000360', 'John Smith', 1, 'CA', 12, 1, '2019-01-03', TRUE),
  ('B000574', 'Jane Doe', 2, 'TX', 7, 1, '2021-01-03', TRUE),
  ('C001111', 'Sam Rayburn', 3, 'VT', NULL, 2, '2015-01-03', TRUE);

-- Insert campaign finance
INSERT INTO campaign_finance (legislator_id, total_received, top_contributors, top_industries, cycle)
VALUES
  (1, 120000.00,
    '[{"name": "Company A", "amount": 50000}, {"name": "Company B", "amount": 40000}]',
    '[{"industry_name": "Healthcare", "total": 60000}, {"industry_name": "Tech", "total": 30000}]',
    '2024'
  ),
  (2, 90000.00,
    '[{"name": "Energy Co", "amount": 50000}]',
    '[{"industry_name": "Energy", "total": 50000}]',
    '2024'
  );

-- Insert votes
INSERT INTO votes (legislator_id, vote_id, bill_number, description, position, vote_date, chamber_id)
VALUES
  (1, 'RC001', 'HR123', 'Vote on healthcare reform', 'Yea', '2023-05-01', 1),
  (2, 'RC002', 'HR456', 'Vote on energy subsidies', 'Nay', '2023-06-15', 1);

-- Insert sponsored bills
INSERT INTO sponsored_bills (legislator_id, bill_number, title, status, introduced_date, passed_date)
VALUES
  (1, 'HR123', 'Healthcare Reform Act', 'Passed', '2023-04-15', '2023-06-01'),
  (2, 'HR456', 'Energy Fairness Act', 'Introduced', '2023-05-20', NULL);
