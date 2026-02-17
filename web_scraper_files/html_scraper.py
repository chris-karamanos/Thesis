import re, time
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from extractors import (
    fetch_url, fetch_html, extract_full_text_generic, clean_dom_in_root,
    postfilter_text_lines, extract_published_el, is_paywalled,
    extract_bleacherreport_body, REQUEST_SLEEP)


def _url_allowed(url: str, allow_pat: str | None, block_pat: str | None) -> bool:
    if block_pat and re.search(block_pat, url, re.I): return False
    if allow_pat: return bool(re.search(allow_pat, url, re.I))
    return True

def _norm_url(base, href):
    return urljoin(base, href) if href else None

def discover_article_links_html(config: dict) -> list[dict]:
    print("[HTML] Discovering article links...")
    listing_urls   = config.get("listing_urls", []) or []
    card_sel       = config.get("card_selector")
    link_sel       = config.get("link_selector", "a")
    next_sel       = config.get("next_page_selector")
    max_pages      = int(config.get("max_pages", 1) or 1)
    max_articles   = int(config.get("max_articles", 30) or 30)
    allow_pat      = config.get("allow_url_regex")
    block_pat      = config.get("block_url_regex")
    max_per_list   = int(config.get("max_per_listing", 0) or 0)
    scope_sel      = config.get("listing_scope_selector")
    paywall_sels   = config.get("paywall_selectors", [])
    paywall_phr    = config.get("paywall_phrases", [])
    url_cat_map    = config.get("listing_url_categories", {})
    
    seen: set[str] = set()
    out: list[dict] = []

    def candidate_ok(absu: str) -> bool:
        if not absu or absu in seen:
            print(f"[HTML]   Skipping seen/empty url: {absu}")
            return False
        # check allow/block regex
        if not _url_allowed(absu, allow_pat, block_pat):
            print(f"[HTML]   URL blocked by allow/block regex: {absu}")
            return False

        # if no paywall selectors/phrases, consider it ok without fetching
        if not paywall_sels and not paywall_phr:
            return True     
        # else fetch and check for paywall indicators
        html = fetch_url(absu)
        if not html:
            return False
        if is_paywalled(html, paywall_sels, paywall_phr):
            print(f"[HTML] paywalled (discovery), skipping: {absu}")
            return False
        return True
      
    def try_add_url(absu: str) -> bool:
        nonlocal taken_here
        if max_per_list and taken_here >= max_per_list:
            return False
        if len(out) >= max_articles:
            return False
        if candidate_ok(absu):
            seen.add(absu)
            out.append({
                "url": absu,
                "category": listing_cat
                    })
            taken_here += 1
            return True
        return False
    
    for start_url in listing_urls:
        print(f"[HTML] Starting listing URL: {start_url}")
        url, pages = start_url, 0
        taken_here = 0                                                          #  counter per url 
        listing_cat = url_cat_map.get(start_url, config.get("category", ""))    # category for this listing

        while url and pages < max_pages and len(out) < max_articles:
            html = fetch_html(url, config, is_listing=True)
            if not html:
                print(f"[HTML] Failed to fetch listing page: {url}")
                break
            soup = BeautifulSoup(html, "lxml")
            scope = soup.select_one(scope_sel) if scope_sel else soup
            cards = scope.select(card_sel) if card_sel else scope.find_all("article")
            
            print(f"[HTML] Found {len(cards)} article cards")

            if not cards:
                for a in scope.select("a[href]"):
                    if max_per_list and taken_here >= max_per_list: break
                    absu = _norm_url(url, a.get("href"))
                    if try_add_url(absu) and len(out) >= max_articles: break
            else:
                for card in cards:
                    if max_per_list and taken_here >= max_per_list:
                        break
                    a = card.select_one(link_sel) if link_sel else card.find("a")
                    if not a:
                        print(f"[HTML] No link found in card, skipping.")
                        continue
                    absu = _norm_url(url, a.get("href") if a else None)                   
                    if try_add_url(absu) and len(out) >= max_articles: break

            if max_per_list and taken_here >= max_per_list: break
            
            if next_sel:
                nxt = soup.select_one(next_sel)
                url = _norm_url(url, nxt.get("href") if nxt else None)
            else:
                url = None
            pages += 1
            time.sleep(REQUEST_SLEEP)
        print(f"[HTML] Took {taken_here} urls from: {start_url}")
    return out

def extract_meta_from_article_html(html: str, base_url: str | None = None) -> dict:
    soup = BeautifulSoup(html, "lxml")
    meta = {}
    title = (soup.find("meta", property="og:title") or {}).get("content")
    if not title and soup.title:
        title = soup.title.get_text(" ", strip=True)
    desc = (soup.find("meta", property="og:description") or {}).get("content")
    if not desc:
        md = soup.find("meta", attrs={"name": "description"})
        if md and md.get("content"):
            desc = md["content"]

    img = None
    tag = soup.find("meta", property="og:image")
    if tag and tag.get("content"):
        img = tag["content"].strip()

    if not img:
        tag = soup.find("meta", property="og:image:secure_url")
        if tag and tag.get("content"):
            img = tag["content"].strip()

    if not img:
        tag = soup.find("meta", attrs={"name": "twitter:image"})
        if tag and tag.get("content"):
            img = tag["content"].strip()

    # Normalize image URL if found
    if img and base_url:
        img = urljoin(base_url, img)        
    
    pub = None

    tag = soup.find("meta", property="article:published_time")
    if tag and tag.get("content"):
        pub = tag["content"]

    if not pub:
        # check for common <time> elements with datetime or data-time attributes  
        cand = soup.select_one("[data-testid*='post_date'], [id*='post_date'], [class*='post_date']")
        if cand:
            txt = cand.get_text(" ", strip=True)
            if txt:
                try:
                    from dateutil.parser import parse as dtparse
                    pub = dtparse(txt, fuzzy=True).isoformat()
                except Exception:
                    pub = txt        
            
    meta.update(title=title, summary=desc, published=pub or "", image_url=img or "")
    return meta


def scrape_html(source_name: str, config: dict) -> list[dict]:
    print(f"[HTML] Scraping: {source_name}")
    urls = discover_article_links_html(config)
    print(f"[HTML] {source_name}: discovered {len(urls)} urls")

    want_full = bool(config.get("fetch_full_text", False))
    articles = []
    for item in urls:
        url = item["url"]
        cat = item.get("category", config.get("category", ""))

        html_raw = fetch_html(url, config, is_listing=False)
        if not html_raw: 
            continue

        pub = extract_published_el(html_raw)
        meta = extract_meta_from_article_html(html_raw)
        title = meta.get("title") or ""

        if not meta.get("published"):
            meta["published"] = pub  # filled from common patterns

        art = {
            "title":     meta.get("title"),
            "link":      url,
            "published": meta.get("published", ""),
            "source":    source_name,
            "category":  cat,
            "summary":   meta.get("summary", "") or "",
            "rss_categories": [],
            "image_url": meta.get("image_url", "") or "",
        }
        if want_full:
            root_sel = config.get("content_root_selector")
            exclude  = config.get("dom_exclude_selectors", []) or []
            html_clean = clean_dom_in_root(html_raw, root_sel, exclude)
            if source_name == "bleacherreport.com":
                full_text = extract_bleacherreport_body(html_raw)
            else:
                full_text = extract_full_text_generic(html_clean, label=f"{art['source']} | {title[:200]}") or ""
            art["full_text"] = postfilter_text_lines(full_text) if full_text else ""
            time.sleep(REQUEST_SLEEP)

        articles.append(art)            

        max_items = int(config.get("max_items", 0) or 0)
        if max_items and len(articles) >= max_items:
            break

    return articles
