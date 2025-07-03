# bills_etl.py
import psycopg2

def run():
    conn = psycopg2.connect(dbname='congress', user='postgres', password='password', host='localhost')
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO sponsored_bills (legislator_id, bill_number, title, status)
        VALUES
        (1, 'HR789', 'Education Reform Act', 'Passed'),
        (1, 'HR101', 'Tax Cut Act', 'Introduced')
        ON CONFLICT DO NOTHING
    """)
    conn.commit()
    cur.close()
    conn.close()

if __name__ == '__main__':
    run()