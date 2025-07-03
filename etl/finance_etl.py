# finance_etl.py
import psycopg2
import json

def run():
    conn = psycopg2.connect(dbname='congress', user='postgres', password='password', host='localhost')
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO campaign_finance (legislator_id, total_received, top_contributors, top_industries)
        VALUES (
            1,
            175000,
            %s,
            %s
        )
        ON CONFLICT (legislator_id) DO UPDATE SET
            total_received = EXCLUDED.total_received,
            top_contributors = EXCLUDED.top_contributors,
            top_industries = EXCLUDED.top_industries;
    """, (
        json.dumps([{"name": "Union A", "amount": 80000}, {"name": "Union B", "amount": 40000}]),
        json.dumps([{"industry_name": "Education", "total": 70000}, {"industry_name": "Labor", "total": 50000}])
    ))
    conn.commit()
    cur.close()
    conn.close()

if __name__ == '__main__':
    run()