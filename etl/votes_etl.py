# votes_etl.py
import psycopg2

def run():
    conn = psycopg2.connect(dbname='congress', user='postgres', password='password', host='localhost')
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO votes (legislator_id, vote_id, bill_number, description, position)
        VALUES
        (1, 'RC003', 'HR789', 'Vote on education reform', 'Yea'),
        (1, 'RC004', 'HR101', 'Vote on tax cut', 'Nay')
        ON CONFLICT DO NOTHING
    """)
    conn.commit()
    cur.close()
    conn.close()

if __name__ == '__main__':
    run()