import os
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
from dotenv import load_dotenv
from zoneinfo import ZoneInfo
from langdetect import detect, DetectorFactory

import psycopg
from psycopg.rows import dict_row

load_dotenv()

DSN = os.getenv("NEWS_DB_DSN")
if not DSN:
    raise RuntimeError("NEWS_DB_DSN not set. Check your .env")

print("Connecting with DSN:", DSN) 

# Κανονικοποιηση URLs με αφαιρεση παραμετρων παρακολουθησης
TRACKING_PREFIXES = ("utm_", "gclid", "fbclid")

def normalize_url(url: str) -> str:
    parts = list(urlsplit(url.strip()))
    parts[4] = ""  # αφαιρεση fragment
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

    # datetime αντικειμενο
    if isinstance(dt, datetime):
        if dt.tzinfo:                   # aware → μετατρεψε σε UTC
            return dt.astimezone(timezone.utc)
        # naive → εφαρμοσε policy
        if naive_policy == "assume_utc":
            return dt.replace(tzinfo=timezone.utc)
        elif naive_policy == "assume_local":
            return dt.replace(tzinfo=ZoneInfo(naive_tz)).astimezone(timezone.utc)
        else:  # απορριψε
            raise ValueError(f"Naive datetime with no tzinfo: {dt!r}")

    # Unix timestamp (int/float)
    if isinstance(dt, (int, float)):
        return datetime.fromtimestamp(dt, tz=timezone.utc)

    s = str(dt).strip()

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

    # Συνηθης φορμες ημερομηνιας/ωρας
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
    if len(text) < 25:   # προφύλαξη για πολύ μικρά κείμενα
        return None
    try:
        code = detect(text)  # π.χ. 'en', 'el'
        return code
    except Exception:
        return None


SQL_UPSERT = """
INSERT INTO articles
 (title, url, summary, full_text, source, category, published_at, language, scraped_at, updated_at)
VALUES
 (%(title)s, %(url)s, %(summary)s, %(full_text)s, %(source)s, %(category)s, %(published_at)s, %(language)s, %(scraped_at)s, %(updated_at)s)
ON CONFLICT (url) DO UPDATE SET
  title        = EXCLUDED.title,
  summary      = EXCLUDED.summary,
  full_text    = EXCLUDED.full_text,
  source       = EXCLUDED.source,
  category     = EXCLUDED.category,
  published_at = EXCLUDED.published_at,
  language     = EXCLUDED.language,
  updated_at   = EXCLUDED.updated_at;
"""


def map_article(a: dict) -> dict:
    # Αντιστοιχιση αρθρου σε φορμα για ΒΔ
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
    }

def upsert_articles(articles: list[dict]) -> int:
    if not articles:
        return 0
    payload = [map_article(a) for a in articles]
    with psycopg.connect(DSN) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.executemany(SQL_UPSERT, payload)
        conn.commit()
    return len(payload)
