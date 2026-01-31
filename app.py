import os
import psycopg2
import streamlit as st

st.set_page_config(page_title="ME/CFS Mechanism Explorer", layout="wide")

st.title("ME/CFS + Long COVID Mechanism Explorer")
st.caption("Railway + Postgres ✅")

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    st.error("DATABASE_URL is not set in Railway Variables.")
    st.stop()

@st.cache_resource
def get_conn():
    # Some platforms return postgres:// but psycopg2 expects postgresql://
    url = DATABASE_URL.replace("postgres://", "postgresql://")
    return psycopg2.connect(url)

def init_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS papers (
            id TEXT PRIMARY KEY,
            title TEXT,
            year INTEGER,
            doi TEXT,
            work_type TEXT,
            journal TEXT,
            url TEXT,
            cited_by_count INTEGER,
            abstract TEXT,
            condition TEXT
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS paper_tags (
            paper_id TEXT REFERENCES papers(id) ON DELETE CASCADE,
            tag TEXT
        );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_papers_year ON papers(year);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_papers_condition ON papers(condition);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_tags_tag ON paper_tags(tag);")
    conn.commit()

conn = get_conn()
init_db(conn)

with conn.cursor() as cur:
    cur.execute("SELECT COUNT(*) FROM papers;")
    paper_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM paper_tags;")
    tag_count = cur.fetchone()[0]

st.success("Connected to Postgres and ensured tables exist.")
st.write(f"Papers in DB: **{paper_count}**")
st.write(f"Tags in DB: **{tag_count}**")
st.info("Next: we’ll add an ingestion script to pull papers from OpenAlex into this database.")
