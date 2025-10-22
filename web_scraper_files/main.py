import json
from pathlib import Path
from rss_scraper import scrape_rss
from html_scraper import scrape_html


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

    #Save all collected articles into a .txt file, one block per article.
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    out_path = Path(out_dir) / f"articles.txt"

    # Sort by source then published 
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

            f.write("="*88 + "\n")
            f.write(f"ARTICLE #{i}\n")
            f.write("="*88 + "\n")
            f.write(f"Source      : {source}\n")
            f.write(f"Bucket      : {bucket}\n")
            f.write(f"Title       : {title}\n")
            f.write(f"Link        : {link}\n")
            f.write(f"Published   : {published}\n")
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


def run_scraper(config_path: str):
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

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
    
    print(f"\n {len(articles)} articles collected!\n")
    for art in articles[:5]:
        print(f"📌 {art['title']}")
        print(f"🔗 {art['link']}")
        print(f"🕒 {art['published']}")
        print(f"🗂️ {art['category']}")
        print()

    out_file = save_articles_txt(articles, out_dir="outputs")
    print(f"📝 Exported TXT: {out_file}")
