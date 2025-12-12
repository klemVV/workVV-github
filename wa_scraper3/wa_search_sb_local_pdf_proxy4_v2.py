#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Combined WA scraper:

- Table results (HTML)  -> wa_results_<LETTER>.json
- Angular businessList  -> output_wa_combined/api/wa_api_<LETTER>_<KEYWORD>.json
- BusinessInformation   -> output_wa_combined/bi_html/wa_bi_<LETTER>_<KEYWORD>.json
- Tracking (per letter) ->

    wa_tracking_<LETTER>.json  (merged tracking per keyword: rows + API + BI)

Usage:
    python3 wa_search_sb_local21.py A
"""

import json
import re
import sys
import os
import requests
import pdfplumber
import time
import shutil
import argparse
import random

from dotenv import load_dotenv
from pathlib import Path

from bs4 import BeautifulSoup
from seleniumbase import SB
from selenium.common.exceptions import NoAlertPresentException
from concurrent.futures import ProcessPoolExecutor, as_completed, wait, FIRST_COMPLETED
from typing import Iterable

load_dotenv()

ADV_URL = "https://ccfs.sos.wa.gov/#/AdvancedSearch"

# Toggle to enable/disable scraping of details via HTML BusinessInformation page
FETCH_HTML_DETAILS = True
PROXY_URL = os.environ.get("WEBSHARE_PROXY") or None
EXC_PROXY_FILE = "exc_proxies.txt"
TEST_URL = ADV_URL  # or whatever target you prefer

# Directory for keyword files (relative to this script)
SCRIPT_DIR = Path(__file__).resolve().parent
LOG_DIR = Path("latest_logs")
KEYWORDS_DIR = SCRIPT_DIR / "search_keywords"

BASE_DIR = Path("/Users/klemanroy/Github/workVV-github/wa_scraper3")
LOG_DIR = BASE_DIR / "latest_logs"
DL_DIR = BASE_DIR / "downloaded_files"
ARCH_DIR = BASE_DIR / "archived_files"
TRACKING_SUFFIX = "wa_tracking_{letter}.json"

def ensure_log_dir():
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"[LOG] Warning: could not create log dir {LOG_DIR}: {e}")

def load_proxies_from_file(path: str) -> list[str]:
    """
    Load proxies from a text file, one per line.

    Each line can be either:
      - full URL:  http://127.0.0.1:9000
      - or host:port: 127.0.0.1:9000  (we'll prepend http://)

    Lines starting with '#' are ignored.
    """
    proxies: list[str] = []
    p = Path(path)
    if not p.exists():
        print(f"[PROXY] Proxy list file not found: {path}")
        return proxies

    for line in p.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if not re.match(r"^https?://", line, flags=re.IGNORECASE):
            line = "http://" + line
        proxies.append(line)

    print(f"[PROXY] Loaded {len(proxies)} proxies from {path}")
    return proxies

def load_excluded_proxies(path: Path) -> set[str]:
    """
    Load proxies that previously failed and should never be reused.
    """
    if not path.exists():
        return set()
    with path.open("r", encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip() and not line.startswith("#")}


def append_bad_proxy(path: Path, proxy: str):
    """
    Persist a bad proxy into exc_proxies.txt so we don't reuse it.
    """
    proxy = proxy.strip()
    if not proxy:
        return
    with path.open("a", encoding="utf-8") as f:
        f.write(proxy + "\n")


def is_proxy_working(proxy: str, test_url: str = TEST_URL, timeout: int = 10) -> bool:
    """
    Quick health check for a proxy using the 'requests' library.
    We only care that it can reach the site with a non-4xx/5xx status.
    """
    proxies = {
        "http": proxy,
        "https": proxy,
    }
    try:
        resp = requests.get(test_url, proxies=proxies, timeout=timeout)
        if 200 <= resp.status_code < 400:
            # print(f"[PROXY-TEST] OK  -> {proxy} (status {resp.status_code})")
            return True
        print(f"[PROXY-TEST] BAD -> {proxy} (status {resp.status_code})")
        return False
    except Exception as e:
        print(f"[PROXY-TEST] ERR -> {proxy}: {e}")
        return False


def assign_proxies_for_batch(
    all_proxies: list[str],
    excluded: set[str],
    keywords: list[str],
    batch_size: int,
    exc_file: Path,
    test_url: str = TEST_URL,
) -> dict[str, str]:
    """
    Given a list of proxies and a list of keywords, assign a UNIQUE,
    TESTED working proxy to each keyword in the first 'batch_size' keywords.

    - No proxy repeats within the batch.
    - Proxies that fail the test are appended to exc_file and added to 'excluded'.
    - If we cannot find enough working proxies, we raise RuntimeError.
    """
    # Ensure exc file exists
    exc_file.touch(exist_ok=True)

    # Candidates = all proxies not in excluded set
    candidate_proxies = [p for p in all_proxies if p not in excluded]

    if not candidate_proxies:
        raise RuntimeError("[PROXY] No available proxies (all excluded).")

    batch_keywords = keywords[:batch_size]
    if not batch_keywords:
        return {}

    assignments: dict[str, str] = {}
    used_in_batch: set[str] = set()

    for kw in batch_keywords:
        proxy_for_kw = None

        while True:
            remaining = [
                p for p in candidate_proxies
                if p not in used_in_batch and p not in excluded
            ]
            if not remaining:
                # We cannot satisfy this batch
                raise RuntimeError(
                    f"[PROXY] Not enough working proxies to assign all "
                    f"{len(batch_keywords)} keywords. "
                    f"Assigned {len(assignments)} so far."
                )

            candidate = random.choice(remaining)
            # print(f"[PROXY] Testing candidate {candidate} for keyword '{kw}'...")

            if is_proxy_working(candidate, test_url=test_url):
                proxy_for_kw = candidate
                used_in_batch.add(candidate)
                break
            else:
                # Mark this proxy as bad forever
                excluded.add(candidate)
                append_bad_proxy(exc_file, candidate)
                print(f"[PROXY] Marked as bad and added to {exc_file}: {candidate}")

        assignments[kw] = proxy_for_kw
        # print(f"[PROXY] Assigned '{kw}' -> {proxy_for_kw}")

    return assignments

# --- Load keywords from letter file ---
def load_keywords(letter: str):
    """
    Load search prefixes/keywords from ./search_keywords/<letter>.txt
    One keyword per line.
    """
    letter = str(letter).upper()
    txt_path = KEYWORDS_DIR / f"{letter}.txt"

    if not txt_path.exists():
        print(f"[ERROR] Keyword file not found: {txt_path}")
        return []

    keywords = []
    for line in txt_path.read_text(encoding="utf-8").splitlines():
        kw = line.strip()
        if kw:
            keywords.append(kw)

    # print(f"[INFO] Loaded {len(keywords)} keywords from {txt_path}")
    return keywords


# --- Parse pager text ---
def parse_pager(html: str):
    """
    Parse text like: 'Page 1 of 5, records 1 to 25 of 105'
    Returns dict or None.
    """
    m = re.search(
        r"Page\s+(\d+)\s+of\s+(\d+),\s*records\s+(\d+)\s+to\s+(\d+)\s+of\s+(\d+)",
        html,
        flags=re.IGNORECASE,
    )
    if not m:
        return None
    a, b, c, d, e = map(int, m.groups())

    # Ignore placeholder 'Page 0 of 0, records 0 to 0 of 0'
    if b == 0 or e == 0:
        return None

    return {
        "page": a,
        "total_pages": b,
        "start": c,
        "end": d,
        "total": e,
    }


# --- Extract rows from HTML ---
def parse_rows(html: str):
    soup = BeautifulSoup(html, "html.parser")

    # Main results table:
    table = soup.select_one("table.table.table-striped.table-responsive")
    if not table:
        # Fallback: any table with 'table-striped'
        table = soup.select_one("table.table-striped")
    if not table:
        # print("[DEBUG] parse_rows: no table with class 'table-striped' found.")
        return []

    trs = table.find_all("tr", attrs={"ng-repeat": True})
    # print(f"[DEBUG] parse_rows: found {len(trs)} <tr ng-repeat> rows in table.")

    rows = []
    for tr in trs:
        tds = [td.get_text(strip=True) for td in tr.find_all("td")]
        if not tds:
            continue

        link = tr.find("a", class_="btn-link")
        name = tds[0]
        business_id = None

        # Try to recover BusinessID from the ng-click attr if present
        if link:
            ng_click = link.get("ng-click", "") or ""
            m = re.search(r"showBusineInfo\((\d+),", ng_click)
            if m:
                business_id = m.group(1)

        rows.append(
            {
                "businessID": business_id,
                "name": name,
                "columns": tds,
            }
        )
    return rows


# --- Click "Next" (›) via JavaScript ---
def click_next_js(sb) -> bool:
    """
    Click the 'Next' page button using JavaScript only.
    Looks for: <a ng-click="search(pagePlus(1))">›</a>
    """
    js = r"""
var anchors = document.querySelectorAll('ul.pagination.pagination-sm a');
for (var i = 0; i < anchors.length; i++) {
    var el = anchors[i];
    var ng = el.getAttribute('ng-click') || '';
    if (ng.indexOf('pagePlus(1)') !== -1) {
        el.click();
        break;
    }
}
"""
    try:
        sb.execute_script(js)
        # print("[DEBUG] click_next_js: executed JS to click Next (if present)")
        return True   # best-effort; we verify via pager on next loop
    except Exception as e:
        # print(f"[DEBUG] click_next_js error: {e}")
        return False


# --- Click a specific page number (2, 3, 4, ...) via JavaScript ---
def click_page_number_js(sb, page_num: int) -> bool:
    """
    Click the numbered page link (e.g. '2', '3', '4') using JavaScript only.
    """
    js = r"""
var target = arguments[0].toString();
var anchors = document.querySelectorAll('ul.pagination.pagination-sm a');
for (var i = 0; i < anchors.length; i++) {
    var el = anchors[i];
    var txt = (el.textContent || '').trim();
    if (txt === target) {
        el.click();
        break;
    }
}
"""
    try:
        sb.execute_script(js, page_num)
        # print(f"[DEBUG] click_page_number_js: executed JS to click page {page_num} (if present)")
        return True
    except Exception as e:
        # print(f"[DEBUG] click_page_number_js error: {e}")
        return False


def sanitize_for_filename(s: str) -> str:
    """
    Make a keyword safe for filenames: keep alphanum, replace others with '_'.
    """
    return re.sub(r"[^A-Za-z0-9]+", "_", s).strip("_") or "kw"


def ensure_advanced_search(sb) -> bool:
    """
    Make sure we're on the Advanced Search page with #txtOrgname visible.
    If we're on the results page, click its Back-to-search button.
    If that fails, reload ADV_URL.
    """
    try:
        # Already on Advanced Search
        if sb.is_element_present("#txtOrgname"):
            return True

        # On results page? Use ReturnToSearch button there
        if sb.is_element_present("#btnReturnToSearch"):
            # print("[NAV] Clicking ReturnToSearch to go back to Advanced Search...")
            sb.click("#btnReturnToSearch")
            sb.wait_for_element("#txtOrgname", timeout=10)
            return True

        # Fallback: go directly to Advanced Search URL
        # print("[NAV] Neither #txtOrgname nor ReturnToSearch found; re-opening AdvancedSearch URL...")
        sb.open(ADV_URL)
        sb.sleep(2)
        sb.wait_for_element("#txtOrgname", timeout=10)
        return True

    except Exception as e:
        print(f"[ERROR] ensure_advanced_search failed: {e}")
        return False


def get_business_list_via_angular(sb):
    """
    From the current Business Search results page, reach into AngularJS scope
    and pull out `businessList`, which holds rich objects per business.

    Returns: list of plain dicts (or []).
    """
    js = r"""
var callback = arguments[arguments.length - 1];
try {
    if (typeof angular === "undefined") {
        callback(JSON.stringify({ ok: false, error: "angular global not found" }));
        return;
    }
    // tbody that is only visible when there are results
    var tbody = document.querySelector("tbody[ng-show*='businessList']");
    if (!tbody) {
        callback(JSON.stringify({ ok: false, error: "tbody with businessList not found" }));
        return;
    }
    // Walk up to something with an Angular scope that has businessList
    var el = tbody;
    var foundScope = null;
    for (var i = 0; i < 6 && el; i++) {
        var scope = angular.element(el).scope() || angular.element(el).isolateScope();
        if (scope && scope.businessList) {
            foundScope = scope;
            break;
        }
        el = el.parentElement;
    }
    if (!foundScope || !foundScope.businessList) {
        callback(JSON.stringify({ ok: false, error: "Angular scope with businessList not found" }));
        return;
    }

    var list = [];
    for (var j = 0; j < foundScope.businessList.length; j++) {
        list.push(foundScope.businessList[j]);
    }
    callback(JSON.stringify({ ok: true, data: list }));
} catch (e) {
    callback(JSON.stringify({ ok: false, error: String(e) }));
}
"""
    try:
        raw = sb.execute_async_script(js)
        result = json.loads(raw)
    except Exception as e:
        # print(f"[API] get_business_list_via_angular JS/parse error: {e}")
        return []

    if not result.get("ok"):
        # print(f"[API] get_business_list_via_angular failed: {result.get('error')}")
        return []

    data = result.get("data") or []
    if data:
        sample_keys = list(data[0].keys())
        # print(f"[API-DEBUG] Angular businessList[0] keys: {sample_keys}")
    return data


def dismiss_any_alert(sb):
    """
    Try to accept any JS alert (e.g., the stray 'null' popup after Cloudflare).
    Safe no-op if no alert is present.
    """
    try:
        alert = sb.driver.switch_to.alert
        text = alert.text
        # print(f"[ALERT] Found alert with text: {text!r}; accepting...")
        alert.accept()
    except NoAlertPresentException:
        # No alert to handle
        pass
    except Exception as e:
        # print(f"[ALERT] Error while trying to handle alert: {e}")
        pass

def handle_cloudflare_if_present(sb, context: str = "") -> bool:
    """
    Check if a Cloudflare / Turnstile challenge is likely present.
    If found, prompt the user to solve it in the browser and press ENTER.
    Returns True if a challenge was detected (whether or not it was solved).
    """
    try:
        html = sb.get_page_source()
    except Exception:
        html = ""

    lower = html.lower()
    if "turnstile" in lower or "cloudflare" in lower:
        tag = f" during {context}" if context else ""
        # print(f"[CF] Possible Cloudflare / Turnstile challenge{tag}.")
        # print("[CF] Please switch to the browser window, solve the challenge,")
        # print("[CF] then press ENTER here to continue.")
        try:
            # input()
            sb.sleep(8)
        except EOFError:
            # in case there's no stdin (e.g., some environments)
            pass
        sb.sleep(1)
        return True

    return False

# ---------- Parse BusinessInformation HTML ----------
def parse_business_information_html(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    info = {}

    main_div = soup.find("div", id="divBusinessInformation")
    if not main_div:
        return info

    def value_next_to_label(root, label_text: str):
        """
        Find a label with given text inside `root`, then return the text
        of the next 'col-md-*' sibling (strong/b text if possible).
        Works for both Business Info (col-md-3/3) and Agent block (col-md-5/7).
        """

        # Find any tag whose *text* matches the label (div or span)
        label_tag = root.find(
            lambda tag: tag.name in ("div", "span")
            and isinstance(tag.string, str)
            and tag.string.strip() == label_text
        )
        if not label_tag:
            return None

        # Climb up to the nearest ancestor with a 'col-md-*' class
        label_col = label_tag
        while label_col and not any(
            isinstance(label_col.get("class"), list)
            and any(cls.startswith("col-md-") for cls in label_col.get("class"))
            for _ in [0]
        ):
            label_col = label_col.parent

        if not label_col:
            return None

        # Now find the next sibling div that also has a 'col-md-*' class
        sib = label_col.find_next_sibling("div")
        while sib and not (
            isinstance(sib.get("class"), list)
            and any(cls.startswith("col-md-") for cls in sib.get("class"))
        ):
            sib = sib.find_next_sibling("div")

        if not sib:
            return None

        # Prefer <strong> or <b>, fall back to all text
        strong = sib.find("strong")
        if strong:
            return strong.get_text(strip=True)
        btag = sib.find("b")
        if btag:
            return btag.get_text(strip=True)

        text = sib.get_text(strip=True)
        return text or None

    # ---- Business Information fields (unchanged, but now use new helper) ----
    info["business_name"] = value_next_to_label(main_div, "Business Name:")
    info["ubi_number"] = value_next_to_label(main_div, "UBI Number:")
    info["business_type"] = value_next_to_label(main_div, "Business Type:")
    info["business_status"] = value_next_to_label(main_div, "Business Status:")
    info["principal_office_street"] = value_next_to_label(
        main_div, "Principal Office Street Address:"
    )
    info["principal_office_mailing"] = value_next_to_label(
        main_div, "Principal Office Mailing Address:"
    )
    info["expiration_date"] = value_next_to_label(main_div, "Expiration Date:")
    info["jurisdiction"] = value_next_to_label(main_div, "Jurisdiction:")
    info["formation_date"] = value_next_to_label(
        main_div, "Formation/ Registration Date:"
    )
    info["duration"] = value_next_to_label(main_div, "Period of Duration:")
    info["business_nature"] = value_next_to_label(main_div, "Nature of Business:")
    info["inactive_date"] = value_next_to_label(main_div, "Inactive Date:")

    # ---- Registered Agent block (use same helper) ----
    agent_header = soup.find(
        "div",
        class_="div_header",
        string=lambda s: s and "Registered Agent Information" in s
    )
    if agent_header:
        # The agent block is the enclosing ng-scope after that header
        agent_block = agent_header.find_parent("div", class_="ng-scope")
        if agent_block:
            info["agent_name"] = value_next_to_label(agent_block, "Registered Agent Name:")
            info["agent_street"] = value_next_to_label(agent_block, "Street Address:")
            info["agent_mailing"] = value_next_to_label(agent_block, "Mailing Address:")

    # ---- Governors (unchanged) ----
    governors = []
    gov_header = soup.find(
        "div",
        class_="div_header",
        string=lambda s: s and "Governors" in s
    )
    if gov_header:
        table = gov_header.find_next("table")
        if table:
            for row in table.select("tbody tr"):
                cells = [c.get_text(strip=True) for c in row.find_all("td")]
                if len(cells) >= 5:
                    title, entity_type, entity_name, first_name, last_name = cells[:5]
                    governors.append(
                        {
                            "title": title,
                            "entity_type": entity_type,
                            "entity_name": entity_name,
                            "first_name": first_name,
                            "last_name": last_name,
                        }
                    )
    info["governors"] = governors

    return info

def scrape_pdfs_for_business(
    sb,
    biz_obj,
    letter,
    keyword,
    letter_idx,
    page_idx,
    idx,              # row index within page
    out_dir: Path,
    first_detail_for_keyword: bool = False,
):
    biz_id = biz_obj.get("BusinessID") or biz_obj.get("ID")
    if not biz_id:
        return [], []

    # print(f"[PDF-PASS] Opening BusinessInformation for BusinessID={biz_id}...")

    opened = open_business_info_for_row(
        sb,
        biz_id=biz_id,
        row_index=idx - 1,  # your existing convention
        first_detail_for_keyword=first_detail_for_keyword,
        context="PDF-PASS",
    )
    if not opened:
        # print(f"[PDF-PASS] Unable to open BI for {biz_id}; skipping PDFs.")
        return [], []

    # Short pause so Filing History button appears
    sb.sleep(1)

    # Filing History + PDFs (existing logic, unchanged apart from Fix 4 below)
    filings, pdf_summaries = scrape_filing_history_and_pdfs(
        sb=sb,
        letter=letter,
        keyword=keyword,
        page_idx=page_idx,
        biz_index=idx,
        business_id=biz_id,
        out_dir=out_dir,
        max_pdfs_per_business=3,
    )

    # Return to search results
    try:
        click_back_with_cf(sb, description=f"PDF pass BusinessID={biz_id}")
        dismiss_any_alert(sb)
    except Exception as e:
        # print(f"[PDF-PASS] Warning returning to results after PDFs ({biz_id}): {e}")
        pass

    return filings, pdf_summaries

# ---------- open BusinessInformation via Angular & parse HTML ----------
def fetch_business_information_via_html(
    sb,
    biz_obj,
    letter,
    keyword,
    letter_idx,
    page_idx,
    idx,                     # row index within page
    out_dir: Path,
    details_dir: Path,
    first_detail_for_keyword: bool = False,
):
    """
    PASS 2: Opens BusinessInformation via Angular in a *clean* state (no Filing History),
    waits for the correct business name, saves & parses the BI HTML, and returns a record dict.
    DOES NOT touch Filing History or PDFs.
    """
    biz_id = biz_obj.get("BusinessID") or biz_obj.get("ID")
    if not biz_id:
        return None

    record = {
        "BusinessID": biz_id,
        "UBINumber": biz_obj.get("UBINumber"),
        "BusinessName": biz_obj.get("BusinessName") or biz_obj.get("EntityName"),
        "BusinessStatus": biz_obj.get("BusinessStatus") or biz_obj.get("Status"),
        "BusinessType": biz_obj.get("BusinessType") or biz_obj.get("Type"),
    }
    expected_name = (record["BusinessName"] or "").strip()

    # print(f"[DETAIL] (BI-PASS) Opening BusinessInformation for BusinessID={biz_id}...")

    opened = open_business_info_for_row(
        sb,
        biz_id=biz_id,
        row_index=idx - 1,  # again, 1-based -> 0-based
        first_detail_for_keyword=first_detail_for_keyword,
        context="BI-PASS",
    )
    if not opened:
        # print(f"[DETAIL] (BI-PASS) BI container not visible for {biz_id} after attempts.")
        return None


    # ---- Angular call to showBusineInfo(biz_id) ----
    open_js = r"""
var bid = arguments[0];
try {
    var tbl = document.querySelector("table.table-striped");
    if (!tbl) return "NO_TABLE";
    var ngEl = angular.element(tbl);
    var scope = ngEl.scope() || ngEl.isolateScope();
    if (!scope) return "NO_SCOPE";

    var fn = scope.showBusineInfo || scope.showBusinessInfo || scope.ShowBusineInfo;
    if (typeof fn !== "function") return "NO_FN";

    fn.call(scope, bid);
    return "OK";
} catch (e) {
    return "EX: " + e;
}
"""
    try:
        raw = sb.execute_script(open_js, biz_id)
        if raw != "OK":
            # print(f"[DETAIL] (BI-PASS) Warning: showBusineInfo returned {raw} for {biz_id}")
            pass
    except Exception as e:
        # print(f"[DETAIL] (BI-PASS) Error executing Angular showBusineInfo for {biz_id}: {e}")
        return None

    # Wait for BI container
    timeout = 10 if first_detail_for_keyword else 5
    try:
        sb.wait_for_element("#divBusinessInformation", timeout=timeout)
        dismiss_any_alert(sb)
    except Exception:
        # print(f"[DETAIL] (BI-PASS) BI container not visible for {biz_id}")
        return None

    # Wait for Business Name text to show and roughly match expected
    name_js = r"""
var el = document.querySelector(
  "#divBusinessInformation strong[data-ng-bind*='BusinessName'], \
   #divBusinessInformation span[data-ng-bind*='BusinessName'], \
   #divBusinessInformation h4[data-ng-bind*='BusinessName']"
);
return el ? (el.textContent||"").trim() : "";
"""
    actual_name = ""
    for _ in range(15):
        try:
            actual_name = (sb.execute_script(name_js) or "").strip()
        except Exception:
            actual_name = ""
        if actual_name:
            break
        sb.sleep(1.0)

    if not actual_name:
        # print(f"[DETAIL] (BI-PASS) Business Name still empty for {biz_id}")
        pass
    else:
        if expected_name and expected_name.lower() not in actual_name.lower():
            # print(
            #     f"[DETAIL] (BI-PASS) Name mismatch for {biz_id}: "
            #     f"expected ~'{expected_name}', got '{actual_name}'"
            # )
            pass

    # ---- Save & parse BI HTML in this clean state ----
    try:
        safe_kw = sanitize_for_filename(keyword)
        html_detail = sb.get_page_source()
        
        # html_path = (
        #     details_dir
        #     / f"bi_html_{letter}_{safe_kw}_p{page_idx+1}_r{idx+1}_bid_{biz_id}.html"
        # )
        # html_path.parent.mkdir(parents=True, exist_ok=True)
        # html_path.write_text(html_detail, encoding="utf-8")
        
        detail_parsed = parse_business_information_html(html_detail)
        # print(
        #     f"[DETAIL] (BI-PASS) Parsed {len(detail_parsed.keys())} BI fields for BusinessID={biz_id}"
        # )
    except Exception as e:
        # print(f"[DETAIL] (BI-PASS) Error saving/parsing BI HTML for {biz_id}: {e}")
        detail_parsed = {}

    # record["BusinessInformationHTMLPath"] = str(html_path)
    record["BusinessInformationHTML"] = detail_parsed

    # These will be merged from the PDF pass in scrape_keyword, not here
    record["FilingHistoryRecords"] = []
    record["PDFSummaries"] = []
    record["PDFDownloadedCount"] = 0

    # ---- Back to search results ----
    try:
        click_back_with_cf(sb, description=f"BI pass BusinessID={biz_id}")
        dismiss_any_alert(sb)
    except Exception as e:
        # print(f"[DETAIL] (BI-PASS) Warning returning to results ({biz_id}): {e}")
        pass

    return record

def extract_phone_email_from_pdf_text(text: str) -> dict:
    """
    Given full text of the PDF (all pages joined), extract:
      - phone (string or None)
      - email (string or None)
    Logic:
      * 'Phone:' may have value on same line or be blank.
      * 'Email:' may have value on same line or on one of the next few lines,
        where the actual email contains '@'.
    """
    phone = None
    email = None

    lines = [ln.strip() for ln in text.splitlines()]

    for i, line in enumerate(lines):
        # Phone
        if line.startswith("Phone:"):
            after = line.split("Phone:", 1)[1].strip()
            if after:
                phone = after
            else:
                # try next non-empty line if it's not 'Email:'
                for j in range(i + 1, min(i + 4, len(lines))):
                    candidate = lines[j].strip()
                    if not candidate or candidate.startswith("Email:"):
                        continue
                    # Very simple phone sanity check: digits in line
                    if any(ch.isdigit() for ch in candidate):
                        phone = candidate
                        break

        # Email
        if line.startswith("Email:"):
            # email might be same line or next 2–3 lines
            for j in range(i, min(i + 6, len(lines))):
                candidate = lines[j]
                if "@" in candidate:
                    match = re.search(r"[\w.\-+]+@[\w.\-]+", candidate)
                    if match:
                        email = match.group(0)
                        break

    # Fallback: if email still None, try first email-like pattern anywhere
    if email is None:
        m = re.search(r"[\w.\-+]+@[\w.\-]+", text)
        if m:
            email = m.group(0)

    return {"phone": phone, "email": email}

def extract_executors_from_pdf_text(text: str) -> list:
    """
    Parse 'EXECUTOR' table from the PDF text.
    It looks roughly like:

        436 157TH AVE SE, BELLEVUE, WA, 98008-4826, UNITED
        STATES
        EXECUTOR INDIVIDUAL ABHIJEET THACKER
        ...

    Strategy:
      * iterate lines
      * if a line starts with 'EXECUTOR ':
           tokens = ['EXECUTOR', entity_type, first, ...last...]
           address = previous non-empty line (or the 2 previous lines joined)
    """
    lines = [ln.strip() for ln in text.splitlines()]
    executors = []

    for i, line in enumerate(lines):
        if not line.startswith("EXECUTOR "):
            continue

        tokens = line.split()
        if len(tokens) < 3:
            continue

        role = tokens[0]  # 'EXECUTOR'
        entity_type = tokens[1]  # e.g. 'INDIVIDUAL'
        if len(tokens) >= 4:
            first_name = tokens[2]
            last_name = " ".join(tokens[3:])
            entity_name = f"{first_name} {last_name}"
        else:
            first_name = tokens[2]
            last_name = ""
            entity_name = first_name

        # Try to get address from one or two previous lines
        address_lines = []
        # previous 2–3 lines, skip empty and lines that start with 'EXECUTOR'
        for j in range(i - 1, max(i - 4, -1), -1):
            prev = lines[j].strip()
            if not prev:
                continue
            if prev.startswith("EXECUTOR "):
                break
            # We stop when we hit a header-like line to avoid pulling too much
            if prev.isupper() and len(prev.split()) <= 3:
                # e.g. 'UNITED STATES' may be its own line; keep it but then break
                address_lines.insert(0, prev)
                break
            address_lines.insert(0, prev)

        address = ", ".join(address_lines) if address_lines else None

        executors.append(
            {
                "role": role,
                "entity_type": entity_type,
                "entity_name": entity_name,
                "first_name": first_name,
                "last_name": last_name,
                "address": address,
            }
        )

    return executors

def parse_wa_filing_pdf(pdf_path: str) -> dict:
    """
    Open a WA filing PDF and return:
      {
        'phone': ...,
        'email': ...,
        'executors': [...]
      }
    """
    p = Path(pdf_path)
    if not is_valid_pdf(p):
        # print(f"[PDF] Skipping invalid PDF in parse_wa_filing_pdf: {pdf_path}")
        return {"phone": None, "email": None, "executors": []}
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            texts = [(p.extract_text() or "") for p in pdf.pages]
    except Exception as e:
        # print(f"[PDF] Failed to open {pdf_path}: {e}")
        return {"phone": None, "email": None, "executors": []}

    full_text = "\n".join(texts)

    phone_email = extract_phone_email_from_pdf_text(full_text)
    executors = extract_executors_from_pdf_text(full_text)

    return {
        "phone": phone_email.get("phone"),
        "email": phone_email.get("email"),
        "executors": executors,
    }

def open_filing_history_tab(sb) -> bool:
    """
    Click the 'Filing History' button on the BusinessInformation page.
    Supports <input id="btnFilingHistory"> used on WA site.
    """
    selectors = [
        "#btnFilingHistory",                         # BEST (id)
        "input#btnFilingHistory",                    # explicit input tag
        "css=input[value='Filing History']",         # value-based
        "xpath=//input[@value='Filing History']",    # XPath fallback
    ]

    for sel in selectors:
        try:
            sb.wait_for_element_visible(sel, timeout=5)
            # print(f"[FILING] Clicking Filing History via selector: {sel}")
            sb.click(sel)
            return True
        except Exception:
            continue

    # print("[FILING] Filing History tab not found/visible.")
    return False


def parse_filing_history_table(html: str) -> list:
    """
    Parse the Filing History table into a list of dicts:
      [
        {
          'filing_number': ...,
          'filing_date_time': ...,
          'effective_date': ...,
          'filing_type': ...,
        },
        ...
      ]
    """
    soup = BeautifulSoup(html, "html.parser")
    filings = []

    target_table = None
    for tbl in soup.select("table.table-striped"):
        header_text = " ".join(
            td.get_text(strip=True).upper()
            for td in tbl.select("thead td, thead th")
        )
        if "FILING NUMBER" in header_text and "FILING TYPE" in header_text:
            target_table = tbl
            break

    if target_table is None:
        # print("[FILING] Filing History table not found.")
        return filings

    for tr in target_table.select("tbody tr"):
        tds = tr.find_all("td")
        if len(tds) < 4:
            continue
        filings.append(
            {
                "filing_number": tds[0].get_text(strip=True),
                "filing_date_time": tds[1].get_text(strip=True),
                "effective_date": tds[2].get_text(strip=True),
                "filing_type": tds[3].get_text(strip=True),
            }
        )

    return filings

def download_pdf_for_filing(sb, filing_number: str, save_path: str) -> bool:
    """
    Use the WA 'DownloadFileByNumber?filingNo=' endpoint to fetch a PDF.
    We reuse Selenium's cookies so we stay within the same Cloudflare session.
    """
    base_url = "https://ccfs.sos.wa.gov/"
    pdf_url = base_url + f"Common/DownloadFileByNumber?filingNo={filing_number}"

    session = requests.Session()
    # Copy cookies from Selenium driver into requests session
    try:
        for c in sb.driver.get_cookies():
            session.cookies.set(c["name"], c["value"])
    except Exception:
        pass

    headers = {
        "User-Agent": (
            sb.get_user_agent() if hasattr(sb, "get_user_agent")
            else "Mozilla/5.0"
        ),
        "Referer": sb.get_current_url(),
    }

    try:
        resp = session.get(pdf_url, headers=headers, timeout=90)
    except Exception as e:
        # print(f"[PDF] Request failed for filing {filing_number}: {e}")
        return False

    if resp.status_code != 200:
        # print(
        #     f"[PDF] Non-200 status ({resp.status_code}) for filing {filing_number}"
        # )
        return False

    # Quick content-type sanity check
    ctype = (resp.headers.get("Content-Type") or "").lower()
    if "pdf" not in ctype:
        # print(
        #     f"[PDF] Filing {filing_number} content is not PDF (Content-Type={ctype})"
        # )
        pass

    try:
        with open(save_path, "wb") as f:
            f.write(resp.content)
        # print(f"[PDF] Saved filing {filing_number} to {save_path}")
        return True
    except Exception as e:
        # print(f"[PDF] Failed to write PDF {save_path}: {e}")
        return False

def close_view_documents_modal(sb, timeout=5):
    """
    Best-effort close for the 'View Documents' modal AND clear any leftover
    .modal-backdrop so it doesn't intercept future clicks.
    """
    import time

    end = time.time() + timeout

    while time.time() < end:
        try:
            # Try clicking common close buttons inside the modal
            sb.execute_script("""
                var modal = document.querySelector('.modal-dialog');
                if (modal) {
                    var btn = modal.querySelector('button.close, .btn-default, .btn[data-dismiss="modal"]');
                    if (btn) { btn.click(); }
                }
            """)
        except Exception:
            pass

        time.sleep(0.5)

        # Check if modal / backdrop are still there
        try:
            modal_visible = sb.is_element_visible(".modal-dialog")
        except Exception:
            modal_visible = False

        try:
            backdrop_visible = sb.is_element_visible(".modal-backdrop")
        except Exception:
            backdrop_visible = False

        if not modal_visible and not backdrop_visible:
            break

    # Hard kill any remaining backdrops and body modal state
    try:
        sb.execute_script("""
            var backs = document.querySelectorAll('.modal-backdrop');
            for (var i = 0; i < backs.length; i++) {
                backs[i].parentNode.removeChild(backs[i]);
            }
            if (document.body) {
                document.body.classList.remove('modal-open');
                document.body.style.removeProperty('padding-right');
            }
        """)
    except Exception:
        pass

    return True
  
def click_back_with_cf(sb, description: str = ""):
    """
    Robust navigation back to the BusinessSearch results grid.

    Designed for calls from:
      - Filing History (after downloading PDFs), or
      - Business Information, or
      - Already on the search results.

    Strategy:
      1. Close any open modal (e.g., View Documents).
      2. If already on Business Search results, stop.
      3. If on Business Information, click the 'Back' button (btn-back),
         which calls navBusinessSearch() and returns to the results list.
      4. Otherwise, use window.history.back() in small steps, checking
         for Business Search results after each step.
      5. If we reach AdvancedSearch, we stop and do NOT go further back.
    """

    # --- Helpers -----------------------------------------------------------
    def is_on_search_results() -> bool:
        """Detect the Business Search results page using URL + key text."""
        try:
            url = sb.get_current_url()
        except Exception:
            return False

        if "BusinessSearch" not in url:
            return False

        try:
            html = sb.get_page_source()
        except Exception:
            return False

        # Markers visible on the results page
        markers = [
            "Business Search Results",  # div_header title :contentReference[oaicite:2]{index=2}
            "Page 1 of",                # pager text :contentReference[oaicite:3]{index=3}
            "businessList.length  &gt; 0"  # ng-show on tbody :contentReference[oaicite:4]{index=4}
        ]
        if any(m in html for m in markers):
            # print("[NAV] Detected Business Search RESULTS page.")
            return True
        return False

    def is_on_business_info() -> bool:
        """Detect the Business Information page using URL + header text."""
        try:
            url = sb.get_current_url()
            html = sb.get_page_source()
        except Exception:
            return False

        if "BusinessInformation" in url:
            return True
        # Header: <h2>Business Information</h2> :contentReference[oaicite:5]{index=5}
        if "Business Information</h2>" in html:
            return True
        return False

    def reached_advanced_search() -> bool:
        """Detect if we overshot back to Advanced Search."""
        try:
            url = sb.get_current_url()
        except Exception:
            return False
        return "AdvancedSearch" in url

    def close_any_modal():
        """If a modal (like 'View Documents') is open, close it."""
        try:
            if sb.is_element_present("css=button.close[data-dismiss='modal']"):
                # print("[NAV] Closing modal via button.close[data-dismiss='modal'].")
                sb.click("css=button.close[data-dismiss='modal']")
                sb.sleep(2)
                return

            if sb.is_element_present("css=.modal-dialog .close"):
                # print("[NAV] Closing modal via .modal-dialog .close.")
                sb.click("css=.modal-dialog .close")
                sb.sleep(2)
                return

            if sb.is_element_present("css=.modal-backdrop"):
                # print("[NAV] Clicking modal backdrop as last-resort close.")
                sb.click("css=.modal-backdrop")
                sb.sleep(2)
        except Exception as e:
            # print(f"[NAV] Warning: error while trying to close modal: {e}")
            pass

    def handle_cloudflare(context: str):
        """Wait (up to ~5 minutes) if a Cloudflare challenge is present."""
        try:
            if sb.is_element_present("css=#cf-chl-widget") or sb.is_element_present("css=.cf-turnstile"):
                # print(f"[NAV] Cloudflare Turnstile detected ({context}).")
                # print("[NAV] Please solve it in the browser; I'll wait up to 5 minutes.")
                for _ in range(300):
                    if not (
                        sb.is_element_present("css=#cf-chl-widget")
                        or sb.is_element_present("css=.cf-turnstile")
                    ):
                        print("[NAV] Cloudflare challenge cleared.")
                        break
                    time.sleep(1)
        except Exception as e:
            # print(f"[NAV] Warning while checking Cloudflare: {e}")
            pass

    # --- Main logic --------------------------------------------------------
    if description:
        # print(f"[NAV] Returning to results ({description})...")
        pass

    # Step 0: close any modal (e.g., View Documents)
    if sb.is_element_present("css=.modal-dialog"):
        # print("[NAV] Modal detected while returning to results; closing it first.")
        close_any_modal()
        sb.sleep(1)

    # Step 1: if we are already on results, done.
    if is_on_search_results():
        return True

    # Step 2: if we are on Business Information, use the 'Back' button
    # (.btn-back → ng-click='navBusinessSearch()') to go to results. :contentReference[oaicite:6]{index=6}
    if is_on_business_info():
        try:
            if sb.is_element_present("css=button.btn-back"):
                # print("[NAV] On Business Information; clicking '.btn-back' to go to results.")
                sb.click("css=button.btn-back")
                sb.sleep(5)

                # Wait a bit for the results page to render
                for _ in range(10):
                    if is_on_search_results():
                        return True
                    if reached_advanced_search():
                        # print("[NAV] Landed on AdvancedSearch after '.btn-back'; stopping.")
                        return False
                    time.sleep(1)
        except Exception as e:
            # print(f"[NAV] Warning while clicking '.btn-back': {e}")
            pass

    # Step 3: Fallback – use history.back() a few times, checking after each
    for step in range(3):
        handle_cloudflare(f"before history.back step {step+1}")

        # Re-check before navigating
        if is_on_search_results():
            return True
        if reached_advanced_search():
            # print("[NAV] Already on AdvancedSearch; not navigating back further.")
            return False

        # Do one history step
        '''
        try:
            print(f"[NAV] Using window.history.back() (step {step+1}).")
            sb.driver.execute_script("window.history.back()")
        except Exception as e:
            print(f"[NAV] Warning calling window.history.back(): {e}")
            break

        sb.sleep(5)
        '''

        # print(f"[NAV] Using window.history.back() (step {step+1}).")
        sb.driver.execute_script("window.history.back()")

        # NEW: custom wait depending on step
        if step == 0:
            # Step 1: BI page needs a short stabilization wait
            sb.sleep(3)     # <-- adjust between 2–4 seconds if needed
        else:
            # Step 2: Returning to results takes longer
            sb.sleep(12)    # <-- your original long wait

        # After each back, wait up to ~10 seconds for results or AdvancedSearch
        for _ in range(10):
            if is_on_search_results():
                return True
            if reached_advanced_search():
                # print("[NAV] Reached AdvancedSearch page; stopping further back navigation.")
                return False
            time.sleep(1)

    # Final check
    if is_on_search_results():
        return True

    # print("[NAV] WARNING: Could not detect results grid after all navigation attempts.")
    return False

def is_valid_pdf(path: Path, min_size: int = 2048) -> bool:
    """
    Basic sanity check that 'path' is a real PDF and not an HTML error or a truncated file.
    - Exists
    - Size >= min_size bytes (default 2 KB)
    - Starts with '%PDF'
    """
    try:
        if not path.exists():
            return False
        if path.stat().st_size < min_size:
            return False
        with path.open("rb") as f:
            header = f.read(5)
        if not header.startswith(b"%PDF"):
            return False
        return True
    except Exception:
        return False


def wait_for_new_pdf(download_dir: Path, before_files: set, timeout: int = 60) -> Path | None:
    """Poll download_dir for a new PDF that wasn't in before_files.

    Returns the newest Path or None if no new file appeared within timeout.
    """
    end_time = time.time() + timeout
    while time.time() < end_time:
        try:
            current_pdfs = set(download_dir.glob("*.pdf"))
        except Exception:
            current_pdfs = set()
        new_files = current_pdfs - before_files
        if new_files:
            # Return the most recently modified new file
            return max(new_files, key=lambda p: p.stat().st_mtime)
        time.sleep(1)
    return None

def safe_return_to_results(sb, business_id: str | None = None, filing_no: str | None = None) -> bool:
    """
    Wrapper around click_back_with_cf() with nicer logging.

    Use this everywhere after you finish handling a BusinessInformation page
    (including after scrape_filing_history_and_pdfs).
    """
    parts = []
    if business_id:
        parts.append(f"BusinessID={business_id}")
    if filing_no:
        parts.append(f"filing={filing_no}")
    desc = ", ".join(parts) if parts else "generic"

    ok = click_back_with_cf(sb, description=desc)
    if not ok:
        # print(f"[DETAIL] Warning: could not return to results ({desc}).")
        pass
    return ok
  
def go_back_to_business_information(sb) -> bool:
    """
    From the Filing History view, try to click whatever brings us back to the
    Business Information view (tab/button/link). Returns True on success.
    """
    # A few likely selectors for the "Business Information" tab or back button.
    selectors = [
        "css=button[ng-click*='BusinessInformation']",
        "css=button[ng-click*='showBusineInfo']",
        "css=a[ng-click*='BusinessInformation']",
        "css=a[ng-click*='showBusineInfo']",
        "css=li[ng-click*='BusinessInformation'] a",
        "css=li[ng-click*='showBusineInfo'] a",
    ]
    for sel in selectors:
        try:
            if sb.is_element_present(sel):
                sb.click(sel)
                sb.sleep(4)
                # print(f"[FILING] Clicked Business Information control via selector: {sel}")
                return True
        except Exception:
            pass

    # Fallback: look for any clickable element whose visible text contains
    # "BUSINESS INFORMATION" (case-insensitive).
    try:
        elems = sb.driver.find_elements("css selector", "a, button, li, span")
        for el in elems:
            try:
                text = (el.text or "").strip().upper()
                if text and "BUSINESS INFORMATION" in text and el.is_displayed():
                    sb.driver.execute_script("arguments[0].click();", el)
                    sb.sleep(4)
                    # print("[FILING] Clicked element with text containing 'BUSINESS INFORMATION'")
                    return True
            except Exception:
                continue
    except Exception:
        pass

    return False

def close_modal(sb) -> bool:
    """Close the WA 'View Documents' modal reliably."""
    selectors = [
        "css=button.close[data-dismiss='modal']",
        "css=.modal-header .close",
        "css=.modal-content .close",
        "css=button.close",
    ]
    for sel in selectors:
        try:
            if sb.is_element_present(sel):
                sb.click(sel)
                sb.sleep(1.5)
                # print(f"[PDF] Closed modal via selector: {sel}")
                return True
        except Exception:
            pass
    # print("[PDF] Failed to close modal using all known selectors.")
    return False

def open_business_info_for_row(
    sb,
    biz_id: str,
    row_index: int,
    first_detail_for_keyword: bool = False,
    context: str = "",
) -> bool:
    """
    Try to open Business Information for a given business by:
    1) Calling Angular showBusineInfo(biz_id).
    2) If that fails to show #divBusinessInformation, fall back to
       clicking the corresponding row's business name link in the table.

    Returns True if #divBusinessInformation is visible, False otherwise.
    """

    # NEW: safety – clear any leftover modal / backdrop from previous filing
    try:
        close_view_documents_modal(sb, timeout=1)
    except Exception:
        pass

    label = f"{context} BusinessID={biz_id}" if context else f"BusinessID={biz_id}"

    # --- Step 1: Angular showBusineInfo(biz_id) ---
    open_js = r"""
var bid = arguments[0];
try {
    var tbl = document.querySelector("table.table-striped");
    if (!tbl) return "NO_TABLE";
    var ngEl = angular.element(tbl);
    var scope = ngEl.scope() || ngEl.isolateScope();
    if (!scope) return "NO_SCOPE";

    var fn = scope.showBusineInfo || scope.showBusinessInfo || scope.ShowBusineInfo;
    if (typeof fn !== "function") return "NO_FN";

    fn.call(scope, bid);
    return "OK";
} catch (e) {
    return "EX: " + e;
}
"""
    try:
        raw = sb.execute_script(open_js, biz_id)
        if raw != "OK":
            # print(f"[OPEN-BI] Warning ({label}): showBusineInfo returned {raw}")
            pass
    except Exception as e:
        # print(f"[OPEN-BI] Error executing showBusineInfo for {label}: {e}")
        pass

    timeout = 10 if first_detail_for_keyword else 5
    try:
        sb.wait_for_element("#divBusinessInformation", timeout=timeout)
        dismiss_any_alert(sb)
        return True
    except Exception:
        # Maybe Cloudflare or Angular didn't navigate. Try CF helper.
        if handle_cloudflare_if_present(sb, context=f"open BI for {label}"):
            try:
                sb.wait_for_element("#divBusinessInformation", timeout=20)
                dismiss_any_alert(sb)
                return True
            except Exception:
                # print(
                #     f"[OPEN-BI] #divBusinessInformation still not visible for {label} "
                #     f"after CF + Angular; trying row-click fallback."
                # )
                pass
        else:
            # print(
            #     f"[OPEN-BI] #divBusinessInformation not visible for {label} "
            #     f"after Angular call; trying row-click fallback."
            # )
            pass

    # --- Step 2: Fall back to clicking the row in the table ---
    try:
        tbl = sb.find_element("css selector", "table.table-striped")
        rows = tbl.find_elements("css selector", "tbody tr")
        if not rows:
            # print(f"[OPEN-BI] No rows found in table for {label}; cannot row-click.")
            return False

        # row_index is 0-based in our calls
        if row_index < 0 or row_index >= len(rows):
            # print(
            #     f"[OPEN-BI] row_index={row_index} out of range "
            #     f"(len={len(rows)}) for {label}; cannot row-click."
            # )
            return False

        row = rows[row_index]
        # Try to click anchor in the first non-empty cell
        clickable = None
        try:
            clickable = row.find_element("css selector", "a")
        except Exception:
            clickable = None
        if not clickable:
            try:
                clickable = row.find_element("css selector", "td:nth-child(1)")
            except Exception:
                clickable = None

        if not clickable:
            # print(f"[OPEN-BI] No clickable business link found in row for {label}.")
            return False

        clickable.click()

        # Wait again for BI container
        try:
            sb.wait_for_element("#divBusinessInformation", timeout=15)
            dismiss_any_alert(sb)
            # print(f"[OPEN-BI] Row-click succeeded for {label}.")
            return True
        except Exception:
            if handle_cloudflare_if_present(
                sb, context=f"row-click BI open for {label}"
            ):
                try:
                    sb.wait_for_element("#divBusinessInformation", timeout=20)
                    dismiss_any_alert(sb)
                    #  Row-click + CF succeeded for {label}.")
                    return True
                except Exception:
                    # print(
                    #     f"[OPEN-BI] #divBusinessInformation still not visible for {label} "
                    #     f"after row-click + CF."
                    # )
                    pass
            else:
                # print(
                #     f"[OPEN-BI] #divBusinessInformation not visible for {label} "
                #     f"after row-click."
                # )
                pass
    except Exception as e:
        # print(f"[OPEN-BI] Error during row-click fallback for {label}: {e}")
        pass

    return False

def scrape_keyword(sb: SB, keyword: str, letter: str, out_dir: Path, first_keyword: bool):
    print(f"\n[===] SCRAPING keyword '{keyword}' (letter {letter}) [===]")

    start_ts = time.time()

    # Make sure we're on the Advanced Search page
    if not ensure_advanced_search(sb):
        print(f"[ERROR] Could not reach Advanced Search for keyword '{keyword}'. Skipping.")
        return {
            "keyword": keyword,
            "pages_visited": 0,
            "records_scraped": 0,
            "pages": [],
            "rows": [],
            "details_file": None,
            "details_success": 0,
            "details_failed": 0,
            "api_file": None,
            "api_records": 0,
        }

    # === Fill search form with retry (handles transient '#entityStatus' issues) ===
    form_ok = False
    for attempt in range(1, 4):
        try:
            # Clear name field
            try:
                sb.clear("#txtOrgname")
            except Exception:
                # Fallback: Ctrl+A then blank it out
                sb.click("#txtOrgname")
                sb.send_keys("#txtOrgname", "CTRL+A")
                sb.type("#txtOrgname", "")

            # Selection dropdowns
            sb.select_option_by_value("#ddlSelection", "3")   # Starts With
            sb.type("#txtOrgname", keyword)
            # sb.select_option_by_value("#entityStatus", "1")   # ACTIVE (taken out since we did all last time 12/10/2025)

            # Click search
            sb.click("#btnSearch")

            # Auto-dismiss any stray 'null' alert right after search
            dismiss_any_alert(sb)

            form_ok = True
            break

        except Exception as e:
            # print(f"[WARN] Form fill failed on attempt {attempt}/3 for keyword '{keyword}': {e}")
            if attempt >= 3:
                print(f"[ERROR] Giving up on keyword '{keyword}' due to repeated form errors.")
                return {
                    "keyword": keyword,
                    "pages_visited": 0,
                    "records_scraped": 0,
                    "pages": [],
                    "rows": [],
                    "details_file": None,
                    "details_success": 0,
                    "details_failed": 0,
                    "api_file": None,
                    "api_records": 0,
                }
            wait_s = 5 * attempt
            print(f"[RETRY] Waiting {wait_s}s before form retry #{attempt+1} for keyword '{keyword}'...")
            sb.sleep(wait_s)
            ensure_advanced_search(sb)

    if not form_ok:
        return {
            "keyword": keyword,
            "pages_visited": 0,
            "records_scraped": 0,
            "pages": [],
            "rows": [],
            "details_file": None,
            "details_success": 0,
            "details_failed": 0,
            "api_file": None,
            "api_records": 0,
        }

    # --- AUTO-WAIT for first results table, NO input() ---
    if first_keyword:
        # print("[INFO] Waiting for the first results table to appear...")
        # print("[INFO] If a Cloudflare / Turnstile challenge appears, solve it in the browser; "
        #       "this script will keep waiting.")
        pass
    try:
        sb.wait_for_element("css=table.table-striped", timeout=30)
    except Exception:
        # print("[WARN] table.table-striped not found after waiting; continuing anyway.")
        pass

    # We’ll track stats per keyword
    keyword_rows = []
    pages_info = []
    page_index = 1

    # For HTML-based details per keyword
    html_details_records = []
    details_success = 0
    details_failed = 0

    # For Angular/API-style capture per keyword
    api_pages = []

    # Paths
    safe_kw = sanitize_for_filename(keyword)
    debug_dir = out_dir
    details_dir = out_dir / "bi_html"
    api_dir = out_dir / "api"

    details_dir = out_dir / "bi_html"
    api_dir = out_dir / "api"
    debug_dir = out_dir / "debug"

    details_dir.mkdir(parents=True, exist_ok=True)
    api_dir.mkdir(parents=True, exist_ok=True)
    debug_dir.mkdir(parents=True, exist_ok=True)

    visited_detail_ids = set()

    # NEW: store PDF data from PASS 1 here
    pdf_data_by_id: dict[str, dict] = {}

    while True:
        sb.sleep(2)
        html = sb.get_page_source()

        # --- Save full HTML for this page for debugging ---
        debug_path = debug_dir / f"debug_{letter}_{safe_kw}_page_{page_index}.html"
        debug_path.write_text(html, encoding="utf-8")
        # print(f"[DEBUG] Saved {debug_path}")

        # --- Parse rows from this page ---
        rows = parse_rows(html)

        """
        # If we see 0 rows on page 1, try a few incremental waits (5s, 10s, 15s)
        if page_index == 1 and len(rows) == 0:
            for retry in range(1, 4):
                wait_s = 5 * retry
                print(f"[WARN] 0 rows for keyword '{keyword}' on page 1; retry {retry}/3 after {wait_s}s...")
                sb.sleep(wait_s)
                html = sb.get_page_source()
                debug_retry_path = debug_dir / f"debug_{letter}_{safe_kw}_page_{page_index}_retry_{retry}.html"
                debug_retry_path.write_text(html, encoding="utf-8")
                # print(f"[DEBUG] Saved {debug_retry_path}")
                rows = parse_rows(html)
                if rows:
                    break
        """

        if page_index == 1 and len(rows) == 0:
            success = False
            for retry in range(1, 4):
                wait_s = 5 * retry
                print(f"[WARN] 0 rows for keyword '{keyword}' on page 1; retry {retry}/3 after {wait_s}s...")
                sb.sleep(wait_s)
                html = sb.get_page_source()
                debug_retry_path = debug_dir / f"debug_{letter}_{safe_kw}_page_{page_index}_retry_{retry}.html"
                debug_retry_path.write_text(html, encoding="utf-8")
                rows = parse_rows(html)
                if rows:
                    success = True
                    break

            if not success:
                rerun_dir = Path("rerun")
                rerun_dir.mkdir(parents=True, exist_ok=True)
                retry_path = rerun_dir / f"rerun_{letter}.txt"
                with retry_path.open("a", encoding="utf-8") as f:
                    f.write(keyword + "\n")

        # print(f"[DEBUG] parse_rows: found {len(rows)} rows")
        # print(f"[PAGE {page_index}] Extracted {len(rows)} rows for keyword '{keyword}'")
        keyword_rows.extend(rows)
        pages_info.append({"page": page_index, "rows_on_page": len(rows)})

        # --- Grab rich per-business objects via Angular's businessList ---
        business_list = get_business_list_via_angular(sb)
        if business_list:
            # print(
            #     f"[API] Angular businessList has {len(business_list)} entries on this page."
            # )
            api_pages.append(
                {
                    "page_index": page_index,
                    "business_list": business_list,
                }
            )

            # ========== PASS 1: PDFs only ==========
            for local_idx, biz_obj in enumerate(business_list):
                biz_id = biz_obj.get("BusinessID") or biz_obj.get("ID")
                if not biz_id:
                    continue

                if biz_id in pdf_data_by_id:
                    # Already did PDF pass for this biz_id (e.g., another page reload)
                    continue

                try:
                    filings, pdf_summaries = scrape_pdfs_for_business(
                        sb=sb,
                        biz_obj=biz_obj,
                        letter=letter,
                        keyword=keyword,
                        letter_idx=0,
                        page_idx=page_index - 1,
                        idx=local_idx + 1,
                        out_dir=out_dir,
                        first_detail_for_keyword=first_keyword
                        and page_index == 1,
                    )
                    pdf_data_by_id[biz_id] = {
                        "FilingHistoryRecords": filings,
                        "PDFSummaries": pdf_summaries,
                    }
                except Exception as e:
                    # print(
                    #     f"[PDF-PASS] Error while scraping PDFs for biz_id={biz_id}: {e}"
                    # )
                    # still record that we attempted PDFs
                    pdf_data_by_id.setdefault(
                        biz_id,
                        {"FilingHistoryRecords": [], "PDFSummaries": []},
                    )

            # ========== PASS 2: Business Information only ==========
            for local_idx, biz_obj in enumerate(business_list):
                biz_id = biz_obj.get("BusinessID") or biz_obj.get("ID")
                if not biz_id:
                    continue

                if biz_id in visited_detail_ids:
                    continue  # already have BI for this biz_id

                # --- Minimal per-business log ---
                ubi = biz_obj.get("UBINumber") or biz_id
                print(
                    f"[SCRAPE] {letter}-{keyword} | "
                    f"Page {page_index} | "
                    f"Business {local_idx+1}/{len(business_list)} | "
                    f"UBI={ubi}"
                )

                try:
                    rec = fetch_business_information_via_html(
                        sb,
                        biz_obj,
                        letter,
                        keyword,
                        letter_idx=0,
                        page_idx=page_index - 1,
                        idx=local_idx + 1,
                        out_dir=out_dir,
                        details_dir=details_dir,
                        first_detail_for_keyword=first_keyword
                        and page_index == 1,
                    )
                except Exception as e:
                    # print(
                    #     f"[DETAIL] Error in BI pass for biz_id={biz_id}: {e}"
                    # )
                    rec = None

                if rec:
                    # Merge PDF data from PASS 1
                    pdf_info = pdf_data_by_id.get(
                        biz_id, {"FilingHistoryRecords": [], "PDFSummaries": []}
                    )
                    rec["FilingHistoryRecords"] = pdf_info["FilingHistoryRecords"]
                    rec["PDFSummaries"] = pdf_info["PDFSummaries"]
                    rec["PDFDownloadedCount"] = len(pdf_info["PDFSummaries"])

                    html_details_records.append(rec)
                    details_success += 1
                    visited_detail_ids.add(biz_id)
                else:
                    details_failed += 1

        # --- Parse pager text (with retries if we catch the 0-of-0 placeholder) ---
        html = sb.get_page_source()
        pager = parse_pager(html)
        retry_count = 0
        while not pager and retry_count < 5:
            # print("[WARN] No valid pager found (or placeholder 0-of-0). Retrying...")
            sb.sleep(2)
            html = sb.get_page_source()
            pager = parse_pager(html)
            retry_count += 1

        if not pager:
            # print(f"[ERROR] Still no valid pager after retries; stopping keyword '{keyword}'.")
            break

        current_page = pager["page"]
        total_pages = pager["total_pages"]
        print(f"[PAGER] Keyword '{keyword}': Page {current_page} of {total_pages}")

        page_index = current_page

        # If this is the last page, stop
        if current_page >= total_pages:
            # print(f"[INFO] Keyword '{keyword}': reached last page; stopping.")
            break

        next_page = current_page + 1

        # --- Try JS Next (›) first ---
        moved = click_next_js(sb)
        if not moved:
            # Fallback: JS click by page number
            moved = click_page_number_js(sb, next_page)

        if not moved:
            # print(f"[INFO] Keyword '{keyword}': could not move to page {next_page}; stopping.")
            break

        # Give time for new page to load
        sb.sleep(3)
        page_index += 1

    

    # --- Save details records for this keyword (if any) ---
    details_path = None
    if FETCH_HTML_DETAILS and html_details_records:
        details_dir.mkdir(parents=True, exist_ok=True)
        details_path = details_dir / f"wa_bi_{letter}_{safe_kw}.json"
        with details_path.open("w", encoding="utf-8") as f:
            json.dump(html_details_records, f, indent=2)
        # print(
        #     f"[DETAIL] Saved {len(html_details_records)} BusinessInformation HTML records "
        #    f"for keyword '{keyword}' to {details_path}"
        # )

    # --- Save Angular/API-like businessList for this keyword (if any) ---
    api_path = None
    api_total_records = 0
    if api_pages:
        api_dir.mkdir(parents=True, exist_ok=True)
        api_path = api_dir / f"wa_api_{letter}_{safe_kw}.json"
        with api_path.open("w", encoding="utf-8") as f:
            json.dump(
                {
                    "letter": letter,
                    "keyword": keyword,
                    "pages": api_pages,
                },
                f,
                indent=2,
            )
        api_total_records = sum(len(p["business_list"]) for p in api_pages)
        print(
            f"[API] Saved Angular businessList for keyword '{keyword}' "
            f"({api_total_records} records) to {api_path}"
        )

    duration_sec = time.time() - start_ts

    # PDF success = businesses with at least 1 PDFSummary
    pdf_success = 0
    for rec in html_details_records:
        pdfs = rec.get("PDFSummaries") or []
        if pdfs:
            pdf_success += 1

    pdf_fail = max(0, details_success - pdf_success)

    # Build result structure for this keyword
    keyword_result = {
        "letter": letter,
        "keyword": keyword,
        "pages_visited": page_index,
        "records_scraped": len(keyword_rows),
        "pages": pages_info,
        "rows": keyword_rows,
        "details_file": str(details_path) if details_path else None,
        "details_success": details_success,
        "details_failed": details_failed,
        "api_file": str(api_path) if api_path else None,
        "api_records": api_total_records,
        "pdf_success": pdf_success,
        "pdf_fail": pdf_fail,
        "duration_sec": round(duration_sec, 2)
    }

    print(
        f"[SUMMARY] Keyword '{keyword}' (letter {letter}): pages_visited={page_index}, "
        f"records_scraped={len(keyword_rows)}, "
        f"duration={round(duration_sec, 2)} seconds, "
        f"details_success={details_success}, details_failed={details_failed}, "
        f"api_records={api_total_records}, pdf_ok={pdf_success}, pdf_fail={pdf_fail}"
    )

    return keyword_result

def scrape_filing_history_and_pdfs(
    sb,
    letter: str,
    keyword: str,
    page_idx: int,
    biz_index: int,
    business_id: str,
    out_dir: Path,
    max_pdfs_per_business: int = 3,
) -> tuple[list, list]:
    """
    From the BusinessInformation view:
      1. Click 'Filing History' tab.
      2. Wait 10 seconds for manual Cloudflare solve.
      3. Parse Filing History table.
      4. For filings whose Document Type contains 'FULFILLED',
         open 'View Documents', click the paper icon, download the PDF,
         parse it, and store it in a per-business folder.

    Returns:
      filings: full filing table list[dict]
      pdf_summaries: list[dict] with parsed PDF data for downloaded filings

    NOTE:
      This function leaves you on the Filing History tab (BI context),
      then the caller uses click_back_with_cf() to go BI -> Search.
    """
    filings: list = []
    pdf_summaries: list = []

    # 1) Open the Filing History tab from BI
    if not open_filing_history_tab(sb):
        # We are still on BI; caller's click_back_with_cf() will work as before
        return filings, pdf_summaries

    # Give you time to solve any Cloudflare / Turnstile on this tab
    # print("[FILING] Waiting 10 seconds for Cloudflare / Filing History to load...")
    sb.sleep(10)

    # 2) Capture Filing History HTML and parse table
    sb.wait_for_element_visible("table.table-striped", timeout=10)
    html_filing = sb.get_page_source()
    filings = parse_filing_history_table(html_filing)

    # Prepare folder:
    #   out_dir / 'pdf' / letter / keyword / 'page_{page_idx+1}' / 'bid_{business_id}'
    pdf_root = out_dir / "pdf" / letter / keyword / f"page_{page_idx+1}" / f"bid_{business_id}"
    pdf_root.mkdir(parents=True, exist_ok=True)

    download_dir = pdf_root

    # Set Chrome download path via CDP
    try:
        sb.driver.execute_cdp_cmd(
            "Page.setDownloadBehavior",
            {
                "behavior": "allow",
                "downloadPath": str(download_dir),
            },
        )
    except Exception as e:
        # print(f"[PDF] Warning: could not set Chrome download path: {e}")
        pass

    if not filings:
        # No filing rows; caller will still call click_back_with_cf()
        return filings, pdf_summaries

    # 3) Download and parse up to max_pdfs_per_business
    for idx, filing in enumerate(filings[:max_pdfs_per_business]):
        filing_no = (filing.get("filing_number") or "").strip()
        if not filing_no:
            continue

        pdf_dest = pdf_root / f"{filing_no}.pdf"

        # 3a) Open the "View Documents" modal for this filing row
        clicked = False
        try:
            js_open_modal = r"""
var tables = document.querySelectorAll("table.table-striped");
if (!tables.length) return false;

// Find the Filing History table by header text
var target = null;
for (var t = 0; t < tables.length; t++) {
    var hdr = tables[t].querySelector("thead");
    if (!hdr) continue;
    var hdrText = (hdr.textContent || "").toUpperCase();
    if (hdrText.includes("FILING NUMBER") && hdrText.includes("FILING TYPE")) {
        target = tables[t];
        break;
    }
}
if (!target) return false;

var rows = target.querySelectorAll("tbody tr");
if (!rows.length) return false;

var idx = arguments[0];  // 0-based index
if (idx >= rows.length) return false;
var row = rows[idx];

// Assume last cell is the Action column
var cells = row.querySelectorAll("td");
if (!cells.length) return false;
var action = cells[cells.length - 1];

// Click the first clickable element in Action cell
var clickable = action.querySelector("a, button, i, span[ng-click], i.fa, .fa-file-text-o");
if (!clickable) return false;
clickable.click();
return true;
"""
            ok = sb.execute_script(js_open_modal, idx)
            if ok:
                clicked = True
                # print(f"[PDF] Opened View Documents modal for filing {filing_no} via row index {idx}")
        except Exception as e:
            # print(f"[PDF] JS error opening modal for filing {filing_no}: {e}")
            pass

        if not clicked:
            # print(f"[PDF] Could not open 'View Documents' modal for filing {filing_no}.")
            continue

        # 3b) Wait for the "View Documents" modal to appear and be visible
        modal = None
        for _ in range(45):  # up to ~45 seconds
            try:
                modal = sb.driver.find_element("css selector", ".modal-dialog")
                if modal.is_displayed():
                    break
            except Exception:
                pass
            time.sleep(1)

        if not modal or not modal.is_displayed():
            # print(f"[PDF] View Documents modal did not become visible for filing {filing_no}")
            close_view_documents_modal(sb, timeout=2)
            continue


        # 3c) Inside the modal, find the FIRST row whose Document Type contains 'FULFILLED'
        #     and click its paper icon.
        try:
            js_click_fulfilled = r"""
var modal = document.querySelector(".modal-dialog") || document.querySelector(".searchresult");
if (!modal) return false;

var rows = modal.querySelectorAll("tbody tr");
for (var i = 0; i < rows.length; i++) {
    var text = (rows[i].textContent || "").toUpperCase();
    if (!(text.includes("FULFILLED") || text.includes("BUSINESS"))) continue;


    var icon = rows[i].querySelector("i.fa-file-text-o, .fa-file-text-o");
    if (!icon) continue;

    icon.click();
    return true;
}
return false;
"""

            # Snapshot existing PDFs BEFORE triggering the download
            before_files = set(download_dir.glob("*.pdf"))

            ok2 = sb.execute_script(js_click_fulfilled)
            if not ok2:
                # print(f"[PDF] No 'FULFILLED' document found in modal for filing {filing_no}")
                close_view_documents_modal(sb, timeout=2)
                continue

            # print(f"[PDF] Clicked paper icon for 'FULFILLED' document in modal for filing {filing_no}")
        except Exception as e:
            # print(f"[PDF] Failed to click 'FULFILLED' paper icon for filing {filing_no}: {e}")
            close_view_documents_modal(sb, timeout=2)
            continue

        # 3d) Wait for the new PDF to appear in download_dir
        new_pdf_path = wait_for_new_pdf(download_dir, before_files, timeout=60)
        if not new_pdf_path:
            # print(f"[PDF] No new PDF detected for filing {filing_no}")
            close_view_documents_modal(sb, timeout=2)
            continue

        # Give the OS a brief moment to finish writing the file
        time.sleep(1.0)

        # 3d-2) Validate that the new file is a real PDF (not HTML/error/truncated)
        if not is_valid_pdf(new_pdf_path):
            # print(f"[PDF] Invalid or corrupted PDF for filing {filing_no}: {new_pdf_path}")
            # Optional: small extra wait and one more check in case write is still in progress
            time.sleep(2.0)
            if not is_valid_pdf(new_pdf_path):
                # print(f"[PDF] Still invalid after re-check; skipping filing {filing_no}")
                close_view_documents_modal(sb, timeout=2)
                continue

        # print(f"[PDF] Downloaded valid PDF: {new_pdf_path}")

        # 3e) Close modal right after PDF download (best effort)
        if not close_view_documents_modal(sb, timeout=5):
            # print("[PDF] Could not close modal after download (continuing anyway).")
            pass

        # 3f) Move/copy to final target filename in pdf_root
        try:
            pdf_dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(new_pdf_path, pdf_dest)
        except Exception as e:
            # print(f"[PDF] Warning: could not copy PDF to {pdf_dest}: {e}")
            pass

        # Parse the PDF if it exists
        if pdf_dest.exists():
            pdf_info = parse_wa_filing_pdf(str(pdf_dest))
            pdf_summary = {
                "filing_number": filing_no,
                "filing_type": filing.get("filing_type"),
                "filing_date_time": filing.get("filing_date_time"),
                "effective_date": filing.get("effective_date"),
                "pdf_path": str(pdf_dest),
                "phone": pdf_info.get("phone"),
                "email": pdf_info.get("email"),
                "executors": pdf_info.get("executors", []),
            }
            pdf_summaries.append(pdf_summary)

    # Leave caller on Filing History; click_back_with_cf() will do the navigation.
    return filings, pdf_summaries

def run_single_keyword_worker(
    letter: str,
    keyword: str,
    proxy: str | None,
    out_dir_str: str,
    headless: bool,
) -> tuple[str, dict, dict]:
    """
    Run scraping for a single keyword in its own SB session (for parallel use).

    Returns:
      (keyword, result_dict, tracking_entry_dict)
    """

    # --- NEW: make sure per-worker dirs exist ---
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        DL_DIR.mkdir(parents=True, exist_ok=True)
        ARCH_DIR.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"[WORKER-SETUP] Failed to create base dirs for keyword '{keyword}': {e}")


    out_dir = Path(out_dir_str)
    out_dir.mkdir(parents=True, exist_ok=True)

    ensure_log_dir()

    print(f"[WORKER] Starting keyword='{keyword}' letter={letter} proxy={proxy}")

    # Decide proxy in this worker (keep env fallback if proxy is None)
    proxy_to_use = proxy
    if proxy_to_use is None:
        proxy_to_use = PROXY_URL
        if proxy_to_use:
            print(f"[WORKER] Using PROXY_URL from env: {proxy_to_use}")
        else:
            print("[WORKER] No proxy; running direct.")

    result: dict

    with SB(
        uc=True,
        locale_code="en",
        test=True,
        browser="chrome",
        headless=headless,
        proxy=proxy_to_use,
        proxy_bypass_list="127.0.0.1,localhost",
    ) as sb:
        sb.open(ADV_URL)
        sb.sleep(8)

        # In this architecture, every worker's first keyword is "first_keyword=True"
        result = scrape_keyword(
            sb=sb,
            keyword=keyword,
            letter=letter,
            out_dir=out_dir,
            first_keyword=True,
        )

    tracking_entry = {
        "keyword": keyword,
        "records_scraped": result.get("records_scraped", 0),
        "details_file": result.get("details_file"),
        "details_success": result.get("details_success", 0),
        "details_failed": result.get("details_failed", 0),
        "api_file": result.get("api_file"),
        "api_records": result.get("api_records", 0),
    }

    print(f"[WORKER] Finished keyword='{keyword}' letter={letter}")
    return keyword, result, tracking_entry

def run_single_keyword_workerF1(
    letter: str,
    keyword: str,
    proxy: str | None,
    out_dir_str: str,
    headless: bool,
) -> tuple[str, dict, dict]:
    """
    Run scraping for a single keyword in its own SB session (for parallel use).

    Returns:
      (keyword, result_dict, tracking_entry_dict)
    """
    import contextlib
    from seleniumbase import SB

    # --- NEW: make sure per-worker dirs exist ---
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        DL_DIR.mkdir(parents=True, exist_ok=True)
        ARCH_DIR.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"[WORKER-SETUP] Failed to create base dirs for keyword '{keyword}': {e}")

    out_dir = Path(out_dir_str)
    out_dir.mkdir(parents=True, exist_ok=True)

    ensure_log_dir()

    print(f"[WORKER] Starting keyword='{keyword}' letter={letter} proxy={proxy}")

    # Decide proxy in this worker (keep env fallback if proxy is None)
    proxy_to_use = proxy
    if proxy_to_use is None:
        proxy_to_use = PROXY_URL
        if proxy_to_use:
            print(f"[WORKER] Using PROXY_URL from env: {proxy_to_use}")
        else:
            print("[WORKER] No proxy; running direct.")

    result: dict = {}
    sb = None

    try:
        # ⚠️ use explicit enter/exit so we can control cleanup in finally
        sb = SB(
            uc=True,
            locale_code="en",
            test=True,
            browser="chrome",
            headless=headless,
            proxy=proxy_to_use,
            proxy_bypass_list="127.0.0.1,localhost",
        )
        sb.__enter__()

        sb.open(ADV_URL)
        sb.sleep(8)

        # In this architecture, every worker's first keyword is "first_keyword=True"
        result = scrape_keyword(
            sb=sb,
            keyword=keyword,
            letter=letter,
            out_dir=out_dir,
            first_keyword=True,
        )

        tracking_entry = {
            "keyword": keyword,
            "records_scraped": result.get("records_scraped", 0),
            "details_file": result.get("details_file"),
            "details_success": result.get("details_success", 0),
            "details_failed": result.get("details_failed", 0),
            "api_file": result.get("api_file"),
            "api_records": result.get("api_records", 0),
            "duration_sec": result.get("duration_sec"),
            "pdf_success": result.get("pdf_success"),
            "pdf_fail": result.get("pdf_fail"),
        }

        print(f"[WORKER] Finished keyword='{keyword}' letter={letter}")
        return keyword, result, tracking_entry

    except Exception as e:
        print(f"[WORKER] ERROR while scraping keyword='{keyword}' letter={letter}: {e}")
        # Let the caller (executor) handle adding an error tracking row
        raise

    finally:
        # 🔻 HARD CLOSE: make *very* sure Chrome is gone in this worker
        if sb is not None:
            with contextlib.suppress(Exception):
                sb.quit()
            with contextlib.suppress(Exception):
                driver = getattr(sb, "driver", None)
                if driver:
                    driver.quit()

def run_keywords_with_buffer(
    letter: str,
    keyword_iter: Iterable[str],   # this can be a generator over J
    proxies: list[str],            # or whatever you use
    out_dir: str,
    headless: bool,
    max_workers: int = 2,          # K
):
    """
    Run up to max_workers keywords in parallel at a time.
    keyword_iter can be huge; we only keep up to max_workers in flight.
    """

    # If J is huge, it's better to turn it into an iterator (not a list)
    kw_iter = iter(keyword_iter)

    def get_proxy_for_kw(kw: str, idx: int) -> str:
        # Example: round-robin proxy assignment. Replace with your logic.
        return proxies[idx % len(proxies)] if proxies else None

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_kw = {}
        next_index = 0  # counter for proxy assignment, logging, etc.

        # 1) Prime the pool with up to K tasks
        for _ in range(max_workers):
            kw = next(kw_iter, None)
            if kw is None:
                break
            proxy_for_kw = get_proxy_for_kw(kw, next_index)
            fut = executor.submit(
                run_single_keyword_worker,
                letter,
                kw,
                proxy_for_kw,
                out_dir,
                headless,
            )
            future_to_kw[fut] = (kw, proxy_for_kw)
            next_index += 1

        # 2) As tasks complete, refill slots from J
        while future_to_kw:
            done, _ = wait(list(future_to_kw.keys()), return_when=FIRST_COMPLETED)

            for fut in done:
                kw, proxy_for_kw = future_to_kw.pop(fut)

                try:
                    # Adapt this to your actual return structure:
                    # e.g. worker returns (kw, result, tracking_entry)
                    _, result, tracking_entry = fut.result()
                    # 🔹 Save your results / tracking here
                    # e.g., all_keywords_results.append(result)
                    #       tracking_combined.append(tracking_entry)
                except Exception as e:
                    print(f"[RUN] ERROR in worker for keyword '{kw}' (proxy {proxy_for_kw}): {e}")
                    # 🔹 append a failed tracking entry if you need to

                # Refill this just-freed slot with the next keyword from J
                next_kw = next(kw_iter, None)
                if next_kw is not None:
                    proxy_for_next_kw = get_proxy_for_kw(next_kw, next_index)
                    new_fut = executor.submit(
                        run_single_keyword_worker,
                        letter,
                        next_kw,
                        proxy_for_next_kw,
                        out_dir,
                        headless,
                    )
                    future_to_kw[new_fut] = (next_kw, proxy_for_next_kw)
                    next_index += 1

def run_letter(
    letter: str = "A",
    out_dir="./output_wa_combined",
    headless: bool = False,
    proxy: str | None = None,
    proxy_list: list[str] | None = None,
    batch_size: int = 5,
):
    """
    Main entry to scrape all keywords for a given letter.

    Modes:
      - If `proxy_list` is provided: use batch proxy assignment + parallel workers.
      - Else if `proxy` is provided: use a single SB session with that proxy.
      - Else: single SB session, direct (no proxy).

    Output:
      - wa_results_<LETTER>.json      (HTML table rows)
      - wa_tracking_<LETTER>.json     (merged tracking per keyword)
      - api/wa_api_<LETTER>_<KW>.json (per-keyword Angular data)
      - bi_html/wa_bi_<LETTER>_<KW>.json (per-keyword BusinessInformation details)
    """

    letter = str(letter).upper()
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    ensure_log_dir()

    keywords = load_keywords(letter)
    if not keywords:
        print("[ERROR] No keywords to scrape; exiting.")
        return

    # --- Resume helpers ---------------------------------------------------
    def load_existing_tracking(letter: str, out_dir: Path) -> tuple[list[dict], set[str]]:
        """Return (tracking_entries, done_keywords)."""
        path = out_dir / TRACKING_SUFFIX.format(letter=letter)
        if not path.exists():
            return [], set()
        try:
            data = json.loads(path.read_text())
            if isinstance(data, list):
                done = {str(entry.get("keyword")) for entry in data if entry.get("keyword")}
                return data, done
        except Exception as e:
            print(f"[RESUME] Failed to read tracking file {path}: {e}")
        return [], set()

    def load_existing_results(letter: str, out_dir: Path) -> dict[str, dict]:
        """Return mapping keyword -> result dict from existing wa_results_<letter>.json."""
        path = out_dir / f"wa_results_{letter}.json"
        if not path.exists():
            return {}
        try:
            data = json.loads(path.read_text())
            kw_list = data.get("keywords") if isinstance(data, dict) else None
            if isinstance(kw_list, list):
                return {str(item.get("keyword")): item for item in kw_list if isinstance(item, dict) and item.get("keyword")}
        except Exception as e:
            print(f"[RESUME] Failed to read results file {path}: {e}")
        return {}

    existing_tracking, done_keywords = load_existing_tracking(letter, out_dir)
    existing_results_map = load_existing_results(letter, out_dir)

    if done_keywords:
        print(f"[RESUME] Found {len(done_keywords)} completed keywords in tracking; skipping them.")

    pending_keywords = [kw for kw in keywords if kw not in done_keywords]
    if not pending_keywords:
        print("[RESUME] All keywords already completed according to tracking; nothing to do.")
    keywords = pending_keywords

    result_map: dict[str, dict] = dict(existing_results_map)
    tracking_combined: list[dict] = list(existing_tracking)

    def flush_tracking():
        combined_tracking_path = out_dir / TRACKING_SUFFIX.format(letter=letter)
        combined_tracking_path.parent.mkdir(parents=True, exist_ok=True)
        combined_tracking_path.write_text(json.dumps(tracking_combined, indent=2))

    def flush_results():
        results_path = out_dir / f"wa_results_tracking_{letter}.json"
        results_payload = {
            "letter": letter,
            "keywords": list(result_map.values()),
        }
        results_path.write_text(json.dumps(results_payload, indent=2))

    # ─────────────────────────────────────────────
    # CASE 1: Proxy list provided → sliding-window parallel with buffet
    # ─────────────────────────────────────────────
    if proxy_list:
        print(f"[INFO] Running letter {letter} with proxy list (buffer mode, max K={batch_size}).")

        all_proxies = list(proxy_list)
        exc_path = Path(EXC_PROXY_FILE)
        exc_path.touch(exist_ok=True)
        excluded = load_excluded_proxies(exc_path)

        # Load ALL keywords (J is huge)
        kw_iter = iter(keywords)

        # Sliding-window executor (K workers)
        from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED

        def get_next_assignment():
            """
            Pull next keyword from kw_iter and assign proxy via assign_proxies_for_batch.
            This ensures each keyword still gets a validated working proxy.
            """
            try:
                next_kw = next(kw_iter)
            except StopIteration:
                return None, None

            # Test & assign a single working proxy for this keyword
            try:
                assignments = assign_proxies_for_batch(
                    all_proxies=all_proxies,
                    excluded=excluded,
                    keywords=[next_kw],
                    batch_size=1,
                    exc_file=exc_path,
                    test_url=TEST_URL,
                )
            except RuntimeError as e:
                print(f"[PROXY] ERROR assigning proxy for KW '{next_kw}': {e}")
                return None, None

            proxy_for_kw = assignments[next_kw]
            return next_kw, proxy_for_kw

        # Start sliding window
        with ProcessPoolExecutor(max_workers=batch_size) as executor:
            future_to_kw = {}

            # 1) Prime up to K workers
            for _ in range(batch_size):
                kw, px = get_next_assignment()
                if kw is None:
                    break
                fut = executor.submit(
                    run_single_keyword_worker,
                    letter,
                    kw,
                    px,
                    str(out_dir),
                    headless,
                )
                future_to_kw[fut] = (kw, px)

            # 2) Sliding window: refill whenever a worker finishes
            while future_to_kw:
                done, _ = wait(list(future_to_kw.keys()), return_when=FIRST_COMPLETED)

                for fut in done:
                    kw, px = future_to_kw.pop(fut)

                    try:
                        _, result, tracking_entry = fut.result()
                        if result and result.get("keyword"):
                            result_map[result["keyword"]] = result
                        tracking_combined.append(tracking_entry)
                        flush_results()
                        flush_tracking()
                    except Exception as e:
                        print(f"[RUN] ERROR in KW '{kw}' (proxy={px}): {e}")
                        tracking_combined.append({
                            "keyword": kw,
                            "records_scraped": 0,
                            "details_file": None,
                            "details_success": 0,
                            "details_failed": 0,
                            "api_file": None,
                            "api_records": 0,
                            "duration_sec": None,
                            "pdf_success": 0,
                            "pdf_fail": 0,
                            "error": str(e),
                        })
                        flush_results()
                        flush_tracking()

                    # Refill slot with next keyword
                    next_kw, next_px = get_next_assignment()
                    if next_kw is not None:
                        new_fut = executor.submit(
                            run_single_keyword_worker,
                            letter,
                            next_kw,
                            next_px,
                            str(out_dir),
                            headless,
                        )
                        future_to_kw[new_fut] = (next_kw, next_px)

    # ─────────────────────────────────────────────
    # CASE 2: Single proxy provided → one SB, sequential
    # ─────────────────────────────────────────────
    elif proxy is not None:
        proxy_to_use = proxy
        print(f"[PROXY] Using explicit single proxy: {proxy_to_use}")

        with SB(
            uc=True,
            locale_code="en",
            test=True,
            browser="chrome",
            headless=headless,
            proxy=proxy_to_use,
            proxy_bypass_list="127.0.0.1,localhost",
        ) as sb:
            sb.open(ADV_URL)
            sb.sleep(8)

            for idx, keyword in enumerate(keywords, start=1):
                first_keyword = idx == 1
                try:
                    result = scrape_keyword(
                        sb=sb,
                        keyword=keyword,
                        letter=letter,
                        out_dir=out_dir,
                        first_keyword=first_keyword,
                    )
                    if result and result.get("keyword"):
                        result_map[result["keyword"]] = result
                    tracking_combined.append(
                        {
                            "keyword": keyword,
                            "records_scraped": result.get("records_scraped", 0),
                            "details_file": result.get("details_file"),
                            "details_success": result.get("details_success", 0),
                            "details_failed": result.get("details_failed", 0),
                            "api_file": result.get("api_file"),
                            "api_records": result.get("api_records", 0),
                            "duration_sec": result.get("duration_sec"),
                            "pdf_success": result.get("pdf_success"),
                            "pdf_fail": result.get("pdf_fail"),
                        }
                    )
                    flush_results()
                    flush_tracking()
                except Exception as e:
                    print(f"[ERROR] Exception while scraping keyword '{keyword}': {e}")
                    tracking_combined.append(
                        {
                            "keyword": keyword,
                            "records_scraped": 0,
                            "details_file": None,
                            "details_success": 0,
                            "details_failed": 0,
                            "api_file": None,
                            "api_records": 0,
                            "duration_sec": None,
                            "pdf_success": 0,
                            "pdf_fail": 0,
                            "error": str(e),
                        }
                    )
                    flush_results()
                    flush_tracking()

    # ─────────────────────────────────────────────
    # CASE 3: No proxy at all → one SB, sequential, direct
    # ─────────────────────────────────────────────
    else:
        proxy_to_use = PROXY_URL
        if proxy_to_use:
            print(f"[PROXY] Using PROXY_URL from env: {proxy_to_use}")
        else:
            print("[PROXY] No proxy configured; running direct.")

        with SB(
            uc=True,
            locale_code="en",
            test=True,
            browser="chrome",
            headless=headless,
            proxy=proxy_to_use if proxy_to_use else None,
            proxy_bypass_list="127.0.0.1,localhost",
        ) as sb:
            sb.open(ADV_URL)
            sb.sleep(8)

            for idx, keyword in enumerate(keywords, start=1):
                first_keyword = idx == 1
                try:
                    result = scrape_keyword(
                        sb=sb,
                        keyword=keyword,
                        letter=letter,
                        out_dir=out_dir,
                        first_keyword=first_keyword,
                    )
                    if result and result.get("keyword"):
                        result_map[result["keyword"]] = result
                    tracking_combined.append(
                        {
                            "keyword": keyword,
                            "records_scraped": result.get("records_scraped", 0),
                            "details_file": result.get("details_file"),
                            "details_success": result.get("details_success", 0),
                            "details_failed": result.get("details_failed", 0),
                            "api_file": result.get("api_file"),
                            "api_records": result.get("api_records", 0),
                        }
                    )
                    flush_results()
                    flush_tracking()
                except Exception as e:
                    print(f"[ERROR] Exception while scraping keyword '{keyword}': {e}")
                    tracking_combined.append(
                        {
                            "keyword": keyword,
                            "records_scraped": 0,
                            "details_file": None,
                            "details_success": 0,
                            "details_failed": 0,
                            "api_file": None,
                            "api_records": 0,
                            "error": str(e),
                        }
                    )
                    flush_results()
                    flush_tracking()

    # ─────────────────────────────────────────────
    # Save combined results for the letter
    # ─────────────────────────────────────────────
    letter_result = {
        "letter": letter,
        "keywords": list(result_map.values()),
    }

    results_path = out_dir / f"wa_results_{letter}.json"
    with results_path.open("w", encoding="utf-8") as f:
        json.dump(letter_result, f, indent=2)

    combined_tracking_path = out_dir / TRACKING_SUFFIX.format(letter=letter)
    with combined_tracking_path.open("w", encoding="utf-8") as f:
        json.dump(tracking_combined, f, indent=2)

    total_records = sum(k.get("records_scraped", 0) for k in result_map.values())
    print(
        f"\n[DONE] Letter {letter}: scraped {len(result_map)} keywords, "
        f"total {total_records} records."
    )
    print(f"[SAVED] {results_path}")
    print(f"[SAVED] {combined_tracking_path}")

    return letter_result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="WA CCFS scraper with optional proxy / proxy list support."
    )
    parser.add_argument(
        "letter",
        nargs="?",
        default="A2",
        help="Letter (or letter group) to scrape, e.g. A2",
    )
    parser.add_argument(
        "--out-dir",
        default="output_wa_pdf_proxy",
        help="Output directory (default: output_wa_pdf10)",
    )
    parser.add_argument(
        "--proxy",
        default=None,
        help="Single proxy URL, e.g. http://127.0.0.1:9000",
    )
    parser.add_argument(
        "--proxy-list-file",
        default=None,
        help="Path to text file containing proxy URLs, one per line.",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run Chrome in headless mode.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5,
        help="Number of keywords to run per proxy batch.",
    )


    args = parser.parse_args()

    # Load proxy list from file if provided
    proxy_list: list[str] | None = None
    if args.proxy_list_file:
        proxy_list = load_proxies_from_file(args.proxy_list_file)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    run_letter(
        letter=args.letter.upper(),
        out_dir=out_dir,
        headless=args.headless,
        proxy=args.proxy,
        proxy_list=proxy_list,
        batch_size=args.batch_size,
    )

'''
python3.11 wa_search_sb_local_pdf_proxy4_v2.py A \
    --proxy-list-file local_proxies/proxies_part1.txt \
    --batch-size 10 \
    --headless \
    --out-dir output_wa_pdf_proxy
'''
