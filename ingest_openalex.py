import os
import time
import requests
import psycopg2

OPENALEX_API_KEY = os.getenv("OPENALEX_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")

OPENALEX_BASE = "https://api.openalex.org/works"

TAG_RULES = {
    "mitochondrial dysfunction": ["mitochond", "oxidative phosphorylation", "electron transport chain", "atp", "tca cycle"],
    "immune dysregulation": ["cytokine", "interferon", "t cell", "b cell", "autoantibod", "inflamm"],
    "endothelial / microclot": ["endothel", "microclot", "fibrin", "platelet", "amyloid fibrin"],
    "viral persistence": ["persistent", "reservoir", "viral rna", "antigen", "reactivation", "herpesvirus", "ebv", "cmv"],
    "autonomic / pots": ["dysautonomia", "p o t s", "postural tachycardia", "orthostatic", "autonomic"],
    "metabolism": ["metabol", "lactate", "glycolysis", "fatty acid oxidation"],
    "neuroinflammation": ["microglia", "neuroinflamm", "brain fog", "gli", "cns"],
}

def normalize_db_url(url: str) -> str:
    return url.replace("postgres://", "postgresql://")

def connect_db():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL missing")
    return psycopg2.connect(normalize_db_url(DATABASE_URL))

def extract_abstract(work: dict) -> str | None:
    inv = work.get("abstract_inverted_index")
    if not inv:
        return work.get("abstract")
    positions = {}
    for word, pos_list in inv.items():
        for p in pos_list:
            positions[p] = word
    return " ".join(positions[p] for p in sorted(positions.keys()))

def tag_text(text: str) -> list[str]:
    if not text:
        return []
    t = text.lower()
    tags = []
    for tag, needles in TAG_RULES.items():
        if any(n in t for n in needles):
            tags.append(tag)
    return tags

def upsert_paper(conn, row: dict):
    abstract = extract_abstract(row)
    title = row.get("title") or ""
    year = row.get("publication_year")
    doi = row.get("doi")
    work_type = row.get("type")
    cited_by_count = row.get("cited_by_count")
    url = row.get("id")
    journal = None
    primary_location = row.get("primary_location") or {}
    source = primary_location.get("source") or {}
    journal = source.get("display_name")

    paper_id = row.get("id")

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO papers (id, title, year, doi, work_type, journal, url, cited_by_count, abstract, condition)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (id) DO UPDATE SET
              title = EXCLUDED.title,
              year = EXCLUDED.year,
              doi = EXCLUDED.doi,
              work_type = EXCLUDED.work_type,
              journal = EXCLUDED.journal,
              url = EXCLUDED.url,
              cited_by_count = EXCLUDED.cited_by_count,
              abstract = EXCLUDED.abstract,
              condition = EXCLUDED.condition
            """,
            (paper_id, title, year, doi, work_type, journal, url, cited_by_count, abstract, row.get("_condition")),
        )

        cur.execute("DELETE FROM paper_tags WHERE paper_id = %s", (paper_id,))
        combined_text = f"{title}\n\n{abstract or ''}"
        tags = tag_text(combined_text)
        for tag in tags:
            cur.execute("INSERT INTO paper_tags (paper_id, tag) VALUES (%s, %s)", (paper_id, tag))

def fetch_all(query: str, condition_label: str, max_pages: int = 5, per_page: int = 100):
    if not OPENALEX_API_KEY:
        raise RuntimeError("OPENALEX_API_KEY missing")

    headers = {"User-Agent": "mecfs-explorer/0.1"}
    params = {
        "search": query,
        "per-page": per_page,
        "page": 1,
        "api_key": OPENALEX_API_KEY,
    }

    all_rows = []
    for page in range(1, max_pages + 1):
        params["page"] = page
        r = requests.get(OPENALEX_BASE, params=params, headers=headers, timeout=60)
        r.raise_for_status()
        data = r.json()
        results = data.get("results", [])
        if not results:
            break
        for w in results:
            w["_condition"] = condition_label
        all_rows.extend(results)
        time.sleep(0.25)

    return all_rows

def main():
    conn = connect_db()
    conn.autocommit = False

    queries = [
        ("myalgic encephalomyelitis chronic fatigue syndrome", "ME/CFS"),
        ("long covid OR post-acute sequelae of sars-cov-2 OR PASC", "Long COVID"),
    ]

    total = 0
    for q, label in queries:
        rows = fetch_all(q, label, max_pages=5, per_page=100)
        for row in rows:
            upsert_paper(conn, row)
        conn.commit()
        total += len(rows)
        print(f"Ingested {len(rows)} works for {label}")

    print(f"Done. Total works processed: {total}")

if __name__ == "__main__":
    main()
