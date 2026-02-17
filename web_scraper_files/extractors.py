import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
from urllib.parse import urljoin
from playwright.sync_api import TimeoutError as PwTimeout
from playwright_stealth import stealth_sync


try:
    from playwright.sync_api import sync_playwright
    HAVE_PLAYWRIGHT = True
except Exception:
    HAVE_PLAYWRIGHT = False

try:
    import trafilatura
    HAVE_TRAFILATURA = True
except Exception:
    HAVE_TRAFILATURA = False

try:
    from readability import Document
    HAVE_READABILITY = True
except Exception:
    HAVE_READABILITY = False

HTTP_TIMEOUT = 15
HTTP_HEADERS = {
    "User-Agent": "NewsAggregator/1.0 (+research; contact: up1072518@ac.upatras.gr)"
}
REQUEST_SLEEP = 0.4   # pause between requests


def fetch_url(url: str) -> str | None:
    # returns HTML text or None on failure 
    try:
        r = requests.get(url, headers=HTTP_HEADERS, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            return None
        return r.text
    except Exception as e:
        return None
    

def extract_full_text_generic(html: str, label: str = "") -> str:
    # text extraction with multiple strategies

    tag = f" [{label}]" if label else ""
    # Trafilatura
    if HAVE_TRAFILATURA:
        try:
            txt = trafilatura.extract(html, include_comments=False, target_language=None)
            n = len((txt or "").strip())
            if txt and n > 120:
                print(f"[XTRACT] trafilatura ✓{tag}")
                return txt.strip()
        except Exception:
            pass

    # Readability-lxml
    if HAVE_READABILITY:
        try:
            doc = Document(html)
            main_html = doc.summary(html_partial=True)
            soup = BeautifulSoup(main_html, "lxml")
            text = "\n".join(p.get_text(" ", strip=True) for p in soup.find_all("p"))
            n = len(text.strip())
            if n > 120:
                print(f"[XTRACT] readability ✓{tag}")
                return text.strip()
        except Exception:
            pass

    # BS4
    soup = BeautifulSoup(html, "lxml")
    candidates = []
    for sel in [
        "article",
        "div[itemprop='articleBody']",
        "div.entry-content",
        "div.post-content",
        "div.article__content",
        "section.article-body",
        "div#article-body",
        ".single-article__content",
    ]:
        found = soup.select(sel)
        if found:
            candidates.extend(found)

    if not candidates:
        # worst case fallback: take all <p> in the page
        candidates = [soup] 
        ps = soup.find_all("p")
        text = "\n".join(p.get_text(" ", strip=True) for p in ps).strip()
        print(f"[XTRACT] bs4 ✓{tag}")
        return text

    def score(node):
        # penalty for nodes with many scripts/asides/navs/forms
        text = node.get_text(" ", strip=True)
        penalty = 50 * len(node.find_all(["script", "aside", "nav", "footer", "form"]))
        return len(text) - penalty

    best = max(candidates, key=score)
    text = "\n".join(p.get_text(" ", strip=True) for p in best.find_all("p"))
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text   


def clean_dom_in_root(html: str, root_sel: str | None, exclude_sels: list[str]) -> str:
    soup = BeautifulSoup(html, "lxml")
    root = soup.select_one(root_sel) if root_sel else soup
    if root is None:
        root = soup  

    # remove unwanted elements 
    for sel in exclude_sels or []:
        for tag in root.select(sel):
            tag.decompose()

    return str(root)


def postfilter_text_lines(text: str) -> str:
    lines = [ln.strip() for ln in text.splitlines()]
    keep = []
    for ln in lines:
        # remove embeds/links
        if re.search(r"(twitter\.com|pic\.twitter\.com|instagram\.com|youtu(\.be|be\.com))", ln, re.I):
            continue
        if re.search(r"https?://", ln, re.I):
            # remove links
            if len(ln) < 80:
                continue
        # remove short lines that are likely noise
        if len(ln) <= 3:
            continue
        keep.append(ln)
    return "\n".join(keep).strip()


GREEK_MONTHS = {
    "Ιανουαρίου": 1, "Ιαν": 1,
    "Φεβρουαρίου": 2, "Φεβ": 2,
    "Μαρτίου": 3, "Μαρ": 3,
    "Απριλίου": 4, "Απρ": 4,
    "Μαΐου": 5, "Μαϊου": 5, "Μαϊ": 5,
    "Ιουνίου": 6, "Ιουν": 6,
    "Ιουλίου": 7, "Ιουλ": 7,
    "Αυγούστου": 8, "Αυγ": 8,
    "Σεπτεμβρίου": 9, "Σεπ": 9,
    "Οκτωβρίου": 10, "Οκτ": 10,
    "Νοεμβρίου": 11, "Νοε": 11,
    "Δεκεμβρίου": 12, "Δεκ": 12,
}

GREEK_DATE_RE = re.compile(
    r"(\d{1,2})\s*[-\s]\s*"
    r"(Ιανουαρίου|Ιαν|Φεβρουαρίου|Φεβ|Μαρτίου|Μαρ|Απριλίου|Απρ|"
    r"Μαΐου|Μαϊου|Μαϊ|Ιουνίου|Ιουν|Ιουλίου|Ιουλ|Αυγούστου|Αυγ|"
    r"Σεπτεμβρίου|Σεπ|Οκτωβρίου|Οκτ|Νοεμβρίου|Νοε|Δεκεμβρίου|Δεκ)"
    r"\s*[-\s]\s*(\d{4})"
    r"(?:\s+(\d{2}):(\d{2}))?",
    re.IGNORECASE
)

def extract_published_el(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")

    # returns if <time datetime="..."> exists 
    t = soup.select_one("time[datetime]")
    if t:
        return t["datetime"]

    # check for meta tags:
    m = soup.select_one("meta[property='article:published_time'], meta[name='pubdate'], meta[name='publish-date']")
    if m and m.get("content"):
        return m["content"]  

    # regex for greek dates
    txt = soup.get_text(" ", strip=True)
    m = GREEK_DATE_RE.search(txt)

    if m:
        day, mon_gr, year, hh, mm = m.groups()
        month = GREEK_MONTHS.get(mon_gr)
        try:
            dt = datetime(int(year), int(month), int(day), int(hh), int(mm))
            return dt.isoformat()  # ISO format for consistency
        except Exception:
            pass

    return ""  


def is_paywalled(html: str, selectors: list[str] | None, phrases: list[str] | None) -> bool:
    soup = BeautifulSoup(html, "lxml")
    # DOM 
    for sel in selectors or []:
        if soup.select_one(sel):
            print(f"[PAY] paywall detected by selector: {sel}")
            return True
    if phrases:
        txt = soup.get_text(" ", strip=True)
        for p in phrases:
            if p and p.lower() in txt.lower():
                print(f"[PAY] paywall detected by phrase: {p}")
                return True
    return False   



def fetch_html(url: str, config: dict, *, is_listing: bool = False) -> str | None:
    """
    Fetches HTML content for a given URL using static requests first and optionally Playwright for dynamic content
    """
    # Static fetch
    html = fetch_url(url)
    if html:
        return html

    # If not HTML, try playwright 
    if config.get("use_playwright") == True:

        if HAVE_PLAYWRIGHT:
            if is_listing:
                # different default selector for listings
                wait_sel = config.get("listing_dynamic_wait_selector", "main, .site-main, body")
            else:
                wait_sel = config.get("dynamic_wait_selector", "[data-testid='article-body'] p")

            print(f"[DynamicFetch] Trying Playwright for {url} (is_listing={is_listing})")
            html_dyn = fetch_dynamic_url(url, wait_selector=wait_sel)
            if html_dyn:
                print("[DynamicFetch] Playwright fetch successful.")
                return html_dyn
            else:
                print("[DynamicFetch] Playwright returned empty HTML.")
        else:
            print("[DynamicFetch] Playwright not available; cannot fetch dynamically.")

    return None



def fetch_dynamic_url(
    url: str,
    wait_selector: str = "article",
    timeout_ms: int = 20000
) -> str | None:

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,                # HEADLESS only way it works in Docker
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-infobars",
                ]
            )
            context = browser.new_context(              # simulate typical user environment
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                locale="el-GR",
                viewport={"width": 1280, "height": 900},
            )

            page = context.new_page()
            stealth_sync(page)  # stealth mode

            print(f"[DynamicFetch] Opening page (cloudflare bypass mode): {url}")
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)

            # simulating user interaction to trigger lazy loading and bypass anti-bot
            page.mouse.move(200, 200)
            page.wait_for_timeout(500)
            page.mouse.wheel(0, 400)
            page.wait_for_timeout(500)

            # effort to bypass simple anti-bot checks by waiting for a key selector to appear
            try:
                page.wait_for_selector(wait_selector, timeout=timeout_ms)
            except PwTimeout:
                print(f"[DynamicFetch] WARNING: selector {wait_selector} did not appear, continuing anyway")

            html = page.content()
            browser.close()
            return html

    except Exception as e:
        print(f"[DynamicFetch] ERROR for {url}: {e}")
        return None

    

def extract_bleacherreport_body(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    root = soup.select_one("[data-testid='article-body']") or soup

    # remove right-rail, pinned video, recommendations
    killers = [
        "[data-analytics-module-id='side_rail']",
        "[id^='id/article/side_rail']",
        "[id^='id/article/siderail']",
        "[data-testid^='id/article/side_rail']",
        "[data-testid*='article_recommendations']",
        "[id*='article_recommendations']",
        "[id*='recommended_video']",
        "[data-testid*='recommended_video']",
        "[data-testid='VideoElement']",
        "[data-testid='headlines-header']",
        ".MuiCollapse-root [data-analytics-module-id]"
    ]
    for sel in killers:
        for n in root.select(sel):
            n.decompose()

    paras = []
    for p in root.select("p"):
        t = p.get_text(" ", strip=True)
        if t:
            paras.append(t)
    return "\n\n".join(paras).strip()    