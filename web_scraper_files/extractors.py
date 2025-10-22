import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
from urllib.parse import urljoin


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
REQUEST_SLEEP = 0.4   # παυση αναμεσα σε αιτηματα


def fetch_url(url: str) -> str | None:
    # Επιστρεφει την HTML ως string ή None σε αποτυχια 
    try:
        r = requests.get(url, headers=HTTP_HEADERS, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            return None
        return r.text
    except Exception as e:
        return None
    

def extract_full_text_generic(html: str, label: str = "") -> str:
    # εξορυξη κειμενου: trafilatura -> readability -> BS4 heuristic

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
        # στη χειροτερη περίπτωση, όλο το doc
        candidates = [soup] 
        ps = soup.find_all("p")
        text = "\n".join(p.get_text(" ", strip=True) for p in ps).strip()
        print(f"[XTRACT] bs4 ✓{tag}")
        return text

    def score(node):
        # συναρτηση βαθμολογιας οπου τιμωρουμε κομβους με πολλα script/aside/nav/footer/form
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

    # Αφαιρουμε ανεπιθυμητα στοιχεια
    for sel in exclude_sels or []:
        for tag in root.select(sel):
            tag.decompose()

    return str(root)


def postfilter_text_lines(text: str) -> str:
    lines = [ln.strip() for ln in text.splitlines()]
    keep = []
    for ln in lines:
        # πέτα embeds/links
        if re.search(r"(twitter\.com|pic\.twitter\.com|instagram\.com|youtu(\.be|be\.com))", ln, re.I):
            continue
        if re.search(r"https?://", ln, re.I):
            # γραμμή που είναι σχεδόν μόνο link
            if len(ln) < 80:
                continue
        # για bullets
        if len(ln) <= 3:
            continue
        keep.append(ln)
    return "\n".join(keep).strip()


GREEK_MONTHS = {
    "Ιανουαρίου": 1, "Φεβρουαρίου": 2, "Μαρτίου": 3, "Απριλίου": 4,
    "Μαΐου": 5, "Ιουνίου": 6, "Ιουλίου": 7, "Αυγούστου": 8,
    "Σεπτεμβρίου": 9, "Οκτωβρίου": 10, "Νοεμβρίου": 11, "Δεκεμβρίου": 12,
}

GREEK_DATE_RE = re.compile(
    r"\b(\d{1,2})\s+"
    r"(Ιανουαρίου|Φεβρουαρίου|Μαρτίου|Απριλίου|Μαΐου|Ιουνίου|Ιουλίου|Αυγούστου|Σεπτεμβρίου|Οκτωβρίου|Νοεμβρίου|Δεκεμβρίου)"
    r"\s+(\d{4})\s+(\d{2}):(\d{2})"
)

def extract_published_el(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")

    # Επιστρεφει αν υπαρχει <time datetime="..."> 
    t = soup.find("time")
    if t and t.get("datetime"):
        return t["datetime"]  

    # 2) Regex για ελληνικες ημερομηνιες
    txt = soup.get_text(" ", strip=True)
    m = GREEK_DATE_RE.search(txt)

    if m:
        day, mon_gr, year, hh, mm = m.groups()
        month = GREEK_MONTHS.get(mon_gr)
        try:
            dt = datetime(int(year), int(month), int(day), int(hh), int(mm))
            return dt.isoformat()  # πχ "2025-10-14T13:42:00"
        except Exception:
            pass

    return ""  


def is_paywalled(html: str, selectors: list[str] | None, phrases: list[str] | None) -> bool:
    soup = BeautifulSoup(html, "lxml")
    # δομη DOM 
    for sel in selectors or []:
        if soup.select_one(sel):
            print(f"[PAY] paywall detected by selector: {sel}")
            return True
    # ψαξιμο για φρασεις
    if phrases:
        txt = soup.get_text(" ", strip=True)
        for p in phrases:
            if p and p.lower() in txt.lower():
                print(f"[PAY] paywall detected by phrase: {p}")
                return True
    return False   


def fetch_dynamic_url(url: str, wait_selector: str = "article", timeout_ms: int = 12000) -> str | None:
    # Φορτωνω τη σελιδα με Playwright Chromium και αφου φορτωσει η js, παιρνω την html.
    if not HAVE_PLAYWRIGHT:
        return None
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(locale="en-US")
            page = context.new_page()

            # Πηγαινω στη σελιδα και περιμενω να εμφανιστει ο wait-selector μεχρι timeout, τοτε θα εχει φορωθει το κυριο σωμα του αρθρου
            page.goto(url, wait_until="domcontentloaded")
            page.wait_for_selector(wait_selector, timeout=timeout_ms)

            # Εκτελουμε JS για να ελεγξουμε αν υπαρχει κειμενο με αρκετο μηκος
            try:
                has_text = page.evaluate("""
                    (sel) => {
                        const nodes = Array.from(document.querySelectorAll(sel));
                        return nodes.some(n => (n.innerText || '').trim().length > 40);
                    }
                """, wait_selector)
                if not has_text:
                    # περιμενουμε λιγο ακομα μηπως ολοκληρωθει το φορτωμα
                    page.wait_for_timeout(1500)
            except Exception:
                pass

            html = page.content()
            context.close()
            browser.close()
            return html
    except Exception as e:
        print(f"[DynamicFetch] Failed for {url}: {e}")
        return None
    

def extract_bleacherreport_body(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    root = soup.select_one("[data-testid='article-body']") or soup

    # Διωχνω right-rail + pinned video + recommendations
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