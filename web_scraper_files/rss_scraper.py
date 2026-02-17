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

    rss_url    = config["rss"]
    allow_list  = config.get("rss_filters", []) or []
    block_list  = config.get("rss_exclude", []) or []
    caps        = config.get("caps", []) or []
    max_items   = int(config.get("max_items", 0) or 0)   

    # normalized lists for efficient substring checks
    allow_norm = [_norm(x) for x in allow_list]

    block_norm = [_norm(x) for x in block_list]

    # counters for group caps
    group_counts = [0] * len(caps)

    feed = feedparser.parse(rss_url)
    entries = list(feed.entries)

    # sort by published date (newest first)
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

        # extract and normalize categories for filtering and grouping 
        entry_categories = extract_categories(entry)          
        ecs_norm = [_norm(c) for c in entry_categories]       

        
        if block_norm and any(any(b in c for c in ecs_norm) for b in block_norm):
            continue

        if allow_norm and not any(any(d in c for c in ecs_norm) for d in allow_norm):
            continue

        # cap group matching and quota check  
        gi = match_group(entry_categories, caps) if caps else None

        if caps:
            if gi is None:
                continue
            quota = int(caps[gi].get("max", 0) or 0)
            if quota and group_counts[gi] >= quota:
                continue

        # build article
        art = {
            "title":     entry.get("title"),
            "link":      entry.get("link"),
            "published": entry.get("published", ""),
            "source":    source_name,
            "category":  config["category"],     
            "rss_categories": entry_categories,  
            "summary":   entry.get("summary", ""),
            "image_url": extract_rss_image(entry),
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

        # update group count if caps are used 
        if caps and gi is not None:
            group_counts[gi] += 1

        kept_total += 1
        if max_items and kept_total >= max_items:
            break

        # stop if all caps groups are exhausted 
        if caps:
            all_exhausted = True
            for idx, g in enumerate(caps):
                q = int(g.get("max", 0) or 0)
                if q == 0 or group_counts[idx] < q:
                    all_exhausted = False
                    break
            if all_exhausted:
                break

        if source_name in ("bbc.com", "ign.com"):
            print(source_name, "IMG:", extract_rss_image(entry))    

    return articles



def match_group(entry_categories, caps):
    if not caps:
        return None
    ecs = [_norm(c) for c in entry_categories]          # normalized entry categories
    for gi, group in enumerate(caps):
        glabs = [_norm(x) for x in group.get("labels", [])]  
        if any(any(g in c for c in ecs) for g in glabs):     # search for substring match in any category
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
    return cats  # unnormalized categories 


def extract_rss_image(entry) -> str:
    # media:content 
    mc = entry.get("media_content")
    if mc:
        # feedparser typically parses this as list of dicts
        if isinstance(mc, list):
            for item in mc:
                if isinstance(item, dict) and item.get("url"):
                    return item["url"]
        elif isinstance(mc, dict) and mc.get("url"):
            return mc["url"]

    # media:thumbnail 
    mt = entry.get("media_thumbnail")
    if mt:
        if isinstance(mt, list):
            for item in mt:
                if isinstance(item, dict) and item.get("url"):
                    return item["url"]
                if isinstance(item, str) and item.strip().startswith("http"):
                    return item.strip()
        if isinstance(mt, str) and mt.strip().startswith("http"):
            return mt.strip()
        if isinstance(mt, dict) and mt.get("url"):
            return mt["url"]

    # enclosures (Atom/RSS)
    for l in (entry.get("links") or []):
        if not isinstance(l, dict):
            continue
        if l.get("rel") == "enclosure":
            t = (l.get("type") or "").lower()
            if t.startswith("image") and l.get("href"):
                return l["href"]

    # <img> in summary (fallback)
    summ = entry.get("summary") or ""
    if "<img" in summ:
        try:
            from bs4 import BeautifulSoup
            s = BeautifulSoup(summ, "lxml")
            img = s.select_one("img[src]")
            if img and img.get("src"):
                return img["src"]
        except Exception:
            pass

    return ""


