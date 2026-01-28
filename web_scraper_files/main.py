import json
from pathlib import Path
from db_conn import upsert_articles
from typing import List, Dict, Iterable
from rss_scraper import scrape_rss
from html_scraper import scrape_html
from db_conn import get_db_conn

def _safe_str(x):
    """Best-effort stringification that tolerates None and non-string types."""
    if x is None:
        return ""
    return str(x)

def _join_categories(val):
    if not val:
        return ""
    if isinstance(val, (list, tuple, set)):
        return ", ".join(map(_safe_str, val))
    return _safe_str(val)

def save_articles_txt(articles, out_dir="outputs"):

    # Αποθηκευω τα αρθρα σε txt αρχειο 
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    out_path = Path(out_dir) / f"articles.txt"

    # Ταξινομηση πρωτα με source, μετα με published 
    def _sort_key(a):
        return (_safe_str(a.get("source")).lower(),
                _safe_str(a.get("published")))
    
    articles_sorted = sorted(articles, key=_sort_key)

    with open(out_path, "w", encoding="utf-8", newline="\n") as f:
        for i, a in enumerate(articles_sorted, 1):
            title       = _safe_str(a.get("title"))
            link        = _safe_str(a.get("link"))
            published   = _safe_str(a.get("published"))
            source      = _safe_str(a.get("source"))
            bucket      = _safe_str(a.get("category"))          
            rss_cats    = _join_categories(a.get("rss_categories"))
            summary     = _safe_str(a.get("summary"))
            full_text   = _safe_str(a.get("full_text"))  
            image_url   = _safe_str(a.get("image_url"))       

            f.write("="*88 + "\n")
            f.write(f"ARTICLE #{i}\n")
            f.write("="*88 + "\n")
            f.write(f"Source      : {source}\n")
            f.write(f"Bucket      : {bucket}\n")
            f.write(f"Title       : {title}\n")
            f.write(f"Link        : {link}\n")
            f.write(f"Published   : {published}\n")
            f.write(f"Image URL   : {image_url}\n")
            if rss_cats:
                f.write(f"RSS tags    : {rss_cats}\n")
            if summary:
                f.write("\n--- Summary ---\n")
                f.write(summary.strip() + "\n")
            if full_text:
                f.write("\n--- Full Text ---\n")
                f.write(full_text.strip() + "\n")
            f.write("\n\n")

    return str(out_path)


def run_scraper(config_path: str, only_sources: list[str] | None = None):
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    if only_sources is not None:
        config = {k: v for k, v in config.items() if k in only_sources}

    all_articles = []
    for source, cfg in config.items():
        method = cfg.get("method")
        if method == "rss":
            all_articles.extend(scrape_rss(source, cfg))
        elif method == "html":
            all_articles.extend(scrape_html(source, cfg))
        else:
            print(f"Skipping {source}: unknown method={method}") 
            
    return all_articles

if __name__ == "__main__":
    articles = run_scraper("scraper_config.json")
    txt_path = save_articles_txt(articles, out_dir="outputs")
    print(f"Saved articles to {txt_path}")
    conn = get_db_conn()
    total, inserts, updates = upsert_articles(conn, articles)
    print(f"Upserted {total} articles (new: {inserts}, updated: {updates})")
