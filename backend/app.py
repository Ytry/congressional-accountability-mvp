from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from typing import List
import os
import psycopg2
from psycopg2.extras import RealDictCursor

# Database connection
DATABASE_URL = os.getenv("DATABASE_URL")

def get_db():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        yield conn
    finally:
        conn.close()

# Pydantic models
class VoteRecord(BaseModel):
    legislator_id: int
    vote_cast: str

class VoteSession(BaseModel):
    vote_id: str
    congress: int
    chamber: str
    date: str
    question: str
    description: str
    result: str
    bill_id: str = None
    key_vote: bool = False
    records: List[VoteRecord] = []

# FastAPI instance
app = FastAPI(
    title="Congressional Accountability API",
    description="Serve vote sessions and records from our Postgres database",
    version="0.1.0"
)

@app.get("/votes/{vote_id}", response_model=VoteSession)
def get_vote(vote_id: str, db=Depends(get_db)):
    cur = db.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        "SELECT id, vote_id, congress, chamber, date::text, question, description, result, key_vote, bill_id "
        "FROM vote_sessions WHERE vote_id = %s",
        (vote_id,)
    )
    session = cur.fetchone()
    if not session:
        raise HTTPException(status_code=404, detail="Vote session not found")
    cur.execute(
        "SELECT legislator_id, vote_cast FROM vote_records WHERE vote_session_id = %s",
        (session["id"],)
    )
    records = cur.fetchall()
    session["records"] = records
    return session

@app.get("/votes", response_model=List[VoteSession])
def list_votes(limit: int = 100, offset: int = 0, db=Depends(get_db)):
    cur = db.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        "SELECT id, vote_id, congress, chamber, date::text, question, description, result, key_vote, bill_id "
        "FROM vote_sessions ORDER BY date DESC LIMIT %s OFFSET %s",
        (limit, offset)
    )
    sessions = cur.fetchall()
    for session in sessions:
        cur.execute(
            "SELECT legislator_id, vote_cast FROM vote_records WHERE vote_session_id = %s",
            (session["id"],)
        )
        session["records"] = cur.fetchall()
    return sessions
