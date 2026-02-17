import os, re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
from dotenv import load_dotenv
from zoneinfo import ZoneInfo
from langdetect import detect, DetectorFactory
import psycopg
from psycopg.rows import dict_row
from typing import List, Dict, Any
from sentence_transformers import SentenceTransformer
import numpy as np
from pgvector.psycopg import register_vector

load_dotenv()
DSN = os.getenv("NEWS_DB_DSN_DOCKER")
if not DSN:
    raise RuntimeError("NEWS_DB_DSN not set. Check your .env")

print("Connecting with DSN:")


MODEL_NAME = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
_embedding_model: SentenceTransformer | None = None


def get_embedding_model() -> SentenceTransformer:
    """
    Loads and returns the sentence transformer model for embeddings once.
    Subsequent calls return the cached model.
    """
    global _embedding_model
    if _embedding_model is None:
        _embedding_model = SentenceTransformer(MODEL_NAME)
    return _embedding_model


def get_db_conn() -> psycopg.Connection:
    dsn = os.environ.get("NEWS_DB_DSN_DOCKER")
    if not dsn:
        raise RuntimeError("NEWS_DB_DSN is not set")

    conn = psycopg.connect(dsn, autocommit=False)
    register_vector(conn)  # to handle vector(384) type with list[float]
    return conn


def build_embedding_text(a: dict, max_chars: int = 1000) -> str:
    """
    Builds a text string for embedding from the article dict.
    """
    title = a.get("title") or ""
    category = a.get("category") or ""
    full_text = a.get("full_text") or ""
    summary = a.get("summary") or ""

    parts: list[str] = []

    if title:
        parts.append(title.strip())

    if category:
        parts.append(f"Κατηγορία: {category.strip()}.")

    body = full_text or summary
    body = (body or "").strip()
    if len(body) > max_chars:
        body = body[:max_chars]

    if body:
        parts.append(body)

    text = " ".join(parts)
    if not text:
        text = "(κενό άρθρο)"
    return text


def compute_article_embeddings(mapped_articles: list[dict]) -> list[list[float]]:
    """
    Takes a list of mapped article dicts (with 'title', 'category', 'full_text', 'summary'),
    builds embedding texts, and returns a list of embeddings as lists of floats.
    """
    model = get_embedding_model()

    texts = [
        build_embedding_text(a)
        for a in mapped_articles
    ]

    embs = model.encode(
        texts,
        batch_size=32,
        show_progress_bar=True,
        convert_to_numpy=True,
        normalize_embeddings=True,
    )

    return [e.tolist() for e in embs]




TRACKING_PREFIXES = ("utm_", "gclid", "fbclid")         # tracking prefixes in urls to remove for normalization 

def normalize_url(url: str) -> str:
    parts = list(urlsplit(url.strip()))
    parts[4] = ""  # remove fragment
    q = [(k, v) for k, v in parse_qsl(parts[3], keep_blank_values=True)
         if not k.lower().startswith(TRACKING_PREFIXES)]
    parts[3] = urlencode(q, doseq=True)
    return urlunsplit(parts)


def ensure_utc(dt, naive_policy="assume_utc", naive_tz="Europe/Athens"):
    """
    Normalize various date/time inputs to timezone-aware UTC datetime.
    naive_policy:
      - "assume_utc": treat naïve as already-UTC (attach tz=UTC)
      - "assume_local": treat naïve as local naive_tz, then convert to UTC
      - "reject": raise ValueError on naïve values
    """

    if dt is None:
        return None

    # datetime object
    if isinstance(dt, datetime):
        if dt.tzinfo:                   # if aware turn to UTC
            return dt.astimezone(timezone.utc)
        # if naive handle according to policy
        if naive_policy == "assume_utc":
            return dt.replace(tzinfo=timezone.utc)
        elif naive_policy == "assume_local":
            return dt.replace(tzinfo=ZoneInfo(naive_tz)).astimezone(timezone.utc)
        else:  # reject
            raise ValueError(f"Naive datetime with no tzinfo: {dt!r}")

    # Unix timestamp 
    if isinstance(dt, (int, float)):
        return datetime.fromtimestamp(dt, tz=timezone.utc)

    s = str(dt).strip()

    # ISO-8601 with timezone without ':'
    m = re.search(r"([+-]\d{2})(\d{2})$", s)
    if m:
        s = s[:-5] + f"{m.group(1)}:{m.group(2)}"

    # ISO-8601 
    try:
        d = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return d.astimezone(timezone.utc) if d.tzinfo else (
            d.replace(tzinfo=timezone.utc) if naive_policy == "assume_utc"
            else d.replace(tzinfo=ZoneInfo(naive_tz)).astimezone(timezone.utc)
            if naive_policy == "assume_local" else
            (_ for _ in ()).throw(ValueError(f"Naïve ISO datetime: {s!r}"))
        )
    except Exception:
        pass

    # RFC-2822 
    try:
        d = parsedate_to_datetime(s) 
        return d.astimezone(timezone.utc)
    except Exception:
        pass

    # usual datetime formats 
    for fmt in ("%a, %d %b %Y %H:%M:%S %z",
                "%d %b %Y %H:%M:%S %z",
                "%Y-%m-%d %H:%M:%S"):
        try:
            d = datetime.strptime(s, fmt)
            return d.astimezone(timezone.utc) if d.tzinfo else (
                d.replace(tzinfo=timezone.utc) if naive_policy == "assume_utc"
                else d.replace(tzinfo=ZoneInfo(naive_tz)).astimezone(timezone.utc)
                if naive_policy == "assume_local" else
                (_ for _ in ()).throw(ValueError(f"Naïve strptime datetime: {s!r}"))
            )
        except Exception:
            continue

    return None

DetectorFactory.seed = 0 

def guess_lang(title: str, content: str) -> str | None:
    text = (title or "") + " " + (content or "")
    text = text.strip()
    if len(text) < 25:   # cut off short texts
        return None
    try:
        code = detect(text)  #'en', 'el'
        return code
    except Exception:
        return None


SQL_UPSERT = """
INSERT INTO articles
 (title, url, summary, full_text, source, category, published_at, language, scraped_at, updated_at, embedding, image_url)
VALUES
 (%(title)s, %(url)s, %(summary)s, %(full_text)s, %(source)s, %(category)s, %(published_at)s, %(language)s, %(scraped_at)s, %(updated_at)s, %(embedding)s, %(image_url)s)
ON CONFLICT (url) DO UPDATE SET
  title        = EXCLUDED.title,
  summary      = EXCLUDED.summary,
  full_text    = EXCLUDED.full_text,
  source       = EXCLUDED.source,
  category     = EXCLUDED.category,
  published_at = EXCLUDED.published_at,
  language     = EXCLUDED.language,
  updated_at   = EXCLUDED.updated_at,
  embedding    = EXCLUDED.embedding,
  image_url    = EXCLUDED.image_url;
"""


def map_article(a: dict) -> dict:
    # mapping from raw article dict to the final dict we want to insert in the DB
    now = datetime.now(timezone.utc)
    return {
        "title":        a.get("title"),
        "url":          normalize_url(a.get("link") or ""),
        "summary":      a.get("summary"),
        "full_text":    a.get("full_text"),
        "source":       a.get("source"),
        "category":     a.get("category"),
        "published_at": ensure_utc(a.get("published")),
        "language":     guess_lang(a.get("title"), a.get("full_text") or a.get("summary")),
        "scraped_at":   now,
        "updated_at":   now,
        "image_url":    a.get("image_url"),
    }

def upsert_articles(
    conn: psycopg.Connection,
    articles: List[Dict[str, Any]],
) -> tuple[int, int, int]:
    """
        Takes a list of raw article dicts (as returned by scrape_rss or scrape_html),
        maps them, computes embeddings, and upserts them into the DB.
        Returns a tuple of (total_articles, inserts, updates).
    """

    if not articles:
        return 0, 0, 0

    # map articles to the final format for DB insertion
    mapped_articles: list[dict] = [map_article(a) for a in articles]

    total = len(mapped_articles)

    # finding existing URLs to determine inserts vs updates
    urls = [a["url"] for a in mapped_articles]

    with conn.cursor() as cur:
        cur.execute(
            "SELECT url FROM articles WHERE url = ANY(%s);",
            (urls,),
        )
        existing_urls = {row[0] for row in cur.fetchall()}

    inserts = 0
    updates = 0
    for a in mapped_articles:
        if a["url"] in existing_urls:
            updates += 1
        else:
            inserts += 1

    # calculate embeddings for all articles (both new and existing)
    embeddings = compute_article_embeddings(mapped_articles)

    # attach embeddings to the mapped articles
    for a, emb in zip(mapped_articles, embeddings):
        a["embedding"] = emb  

    # execure upsert for each article
    with conn.cursor() as cur:
        for a in mapped_articles:
            cur.execute(SQL_UPSERT, a)

    conn.commit()
    return total, inserts, updates
 
