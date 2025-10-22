import feedparser
import unicodedata
import time
from extractors import (
    fetch_url,
    clean_dom_in_root,
    extract_full_text_generic,
    postfilter_text_lines,
    REQUEST_SLEEP,          
)

# RSS SCRAPER
def scrape_rss(source_name: str, config: dict):
    """
    Reads one RSS feed and returns a list of article dicts.
    Respects:
      - config['rss_filters']: list[str]  (optional, broad allow-list)
      - config['caps']:       list[{"labels": [...], "max": int}] (optional, per-group quotas)
      - config['max_items']:  int (optional, flat cap across all kept items)
    Requires:
      - extract_categories(entry): returns raw category labels (list[str])
      - _norm(s): normalization helper (casefold + accent strip + space collapse)
      - match_group(entry_categories, caps): returns group index or None
    """
   

    rss_url    = config["rss"]
    allow_list  = config.get("rss_filters", []) or []
    block_list  = config.get("rss_exclude", []) or []
    caps        = config.get("caps", []) or []
    max_items   = int(config.get("max_items", 0) or 0)   

    # Κανονικοποιηση λιστων φιλτρων
    allow_norm = [_norm(x) for x in allow_list]

    block_norm = [_norm(x) for x in block_list]

    # Μετρηση ανα κατηγορια
    group_counts = [0] * len(caps)

    feed = feedparser.parse(rss_url)
    entries = list(feed.entries)

    # Ταξινομηση κατα χρονολογιας (νεοτερα πρωτα)
    def _pubkey(e):
        return getattr(e, "published_parsed", None) or 0
    try:
        entries.sort(key=_pubkey, reverse=True)
    except Exception:
        pass

    articles = []
    kept_total = 0

    for entry in entries:

        title = (entry.get("title") or "").strip()
        url   = entry.get("link")

        # τραβαω τις κατηγοριες 
        entry_categories = extract_categories(entry)          # ακατεργαστες
        ecs_norm = [_norm(c) for c in entry_categories]       # κανονικοποιημενες

        # πεταω αν ταιριαζει με block-list
        if block_norm and any(any(b in c for c in ecs_norm) for b in block_norm):
            continue

        # κραταω μονο αν ταιριαζει με allow-list 
        if allow_norm and not any(any(d in c for c in ecs_norm) for d in allow_norm):
            continue

        # ορια για καθε group  
        gi = match_group(entry_categories, caps) if caps else None

        if caps:
            if gi is None:
                continue
            quota = int(caps[gi].get("max", 0) or 0)
            if quota and group_counts[gi] >= quota:
                continue

        # χτιζω το αρθρο
        art = {
            "title":     entry.get("title"),
            "link":      entry.get("link"),
            "published": entry.get("published", ""),
            "source":    source_name,
            "category":  config["category"],     # bucket 
            "rss_categories": entry_categories,  
            "summary":   entry.get("summary", ""),
        }

        want_full = bool(config.get("fetch_full_text", False))

        if want_full and url:
            html = fetch_url(art["link"])
            if html:
                root_sel = config.get("content_root_selector")
                exclude = config.get("dom_exclude_selectors", []) or []
                html_clean = clean_dom_in_root(html, root_sel, exclude)
                txt = extract_full_text_generic(html_clean, label=f"{art['source']} | {title[:200]}")
                art["full_text"] = postfilter_text_lines(txt) if txt else ""
                time.sleep(REQUEST_SLEEP)


        articles.append(art)

        # ενημερωση μετρητων 
        if caps and gi is not None:
            group_counts[gi] += 1

        kept_total += 1
        if max_items and kept_total >= max_items:
            break

        # στοπ αν εχουν εξαντληθει ολα τα ορια των group 
        if caps:
            all_exhausted = True
            for idx, g in enumerate(caps):
                q = int(g.get("max", 0) or 0)
                if q == 0 or group_counts[idx] < q:
                    all_exhausted = False
                    break
            if all_exhausted:
                break

    return articles



def match_group(entry_categories, caps):
    if not caps:
        return None
    ecs = [_norm(c) for c in entry_categories]          # κανονικοποιημενες
    for gi, group in enumerate(caps):
        glabs = [_norm(x) for x in group.get("labels", [])]  
        if any(any(g in c for c in ecs) for g in glabs):     # ψαξιμο για sustring
            return gi
    return None


def _norm(s: str) -> str:
    s = (s or "").casefold()
    s = "".join(ch for ch in unicodedata.normalize("NFD", s)
                if unicodedata.category(ch) != "Mn")
    return " ".join(s.split())


def extract_categories(entry):
    cats = []
    for t in entry.get("tags", []):
        term = getattr(t, "term", None) if hasattr(t, "term") else (t.get("term") if isinstance(t, dict) else None)
        if term:
            cats.append(term)
    if entry.get("category"):
        cats.append(entry.get("category"))
    return cats  # ακατεργαστες κατηγοριες



