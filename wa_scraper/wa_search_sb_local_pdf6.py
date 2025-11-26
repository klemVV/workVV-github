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

from dotenv import load_dotenv
from pathlib import Path

from bs4 import BeautifulSoup
from seleniumbase import SB
from selenium.common.exceptions import NoAlertPresentException

load_dotenv()

ADV_URL = "https://ccfs.sos.wa.gov/#/AdvancedSearch"

# Toggle to enable/disable scraping of details via HTML BusinessInformation page
FETCH_HTML_DETAILS = True
PROXY_URL = os.getenv("WEBSHARE_PROXY")

# Directory for keyword files (relative to this script)
SCRIPT_DIR = Path(__file__).resolve().parent
KEYWORDS_DIR = SCRIPT_DIR / "search_keywords"


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

    print(f"[INFO] Loaded {len(keywords)} keywords from {txt_path}")
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
        print("[DEBUG] parse_rows: no table with class 'table-striped' found.")
        return []

    trs = table.find_all("tr", attrs={"ng-repeat": True})
    print(f"[DEBUG] parse_rows: found {len(trs)} <tr ng-repeat> rows in table.")

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
        print("[DEBUG] click_next_js: executed JS to click Next (if present)")
        return True   # best-effort; we verify via pager on next loop
    except Exception as e:
        print(f"[DEBUG] click_next_js error: {e}")
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
        print(f"[DEBUG] click_page_number_js: executed JS to click page {page_num} (if present)")
        return True
    except Exception as e:
        print(f"[DEBUG] click_page_number_js error: {e}")
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
            print("[NAV] Clicking ReturnToSearch to go back to Advanced Search...")
            sb.click("#btnReturnToSearch")
            sb.wait_for_element("#txtOrgname", timeout=10)
            return True

        # Fallback: go directly to Advanced Search URL
        print("[NAV] Neither #txtOrgname nor ReturnToSearch found; re-opening AdvancedSearch URL...")
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
        print(f"[API] get_business_list_via_angular JS/parse error: {e}")
        return []

    if not result.get("ok"):
        print(f"[API] get_business_list_via_angular failed: {result.get('error')}")
        return []

    data = result.get("data") or []
    if data:
        sample_keys = list(data[0].keys())
        print(f"[API-DEBUG] Angular businessList[0] keys: {sample_keys}")
    return data


def dismiss_any_alert(sb):
    """
    Try to accept any JS alert (e.g., the stray 'null' popup after Cloudflare).
    Safe no-op if no alert is present.
    """
    try:
        alert = sb.driver.switch_to.alert
        text = alert.text
        print(f"[ALERT] Found alert with text: {text!r}; accepting...")
        alert.accept()
    except NoAlertPresentException:
        # No alert to handle
        pass
    except Exception as e:
        print(f"[ALERT] Error while trying to handle alert: {e}")


def click_detail_back_button(sb) -> bool:
    """
    From the BusinessInformation view, click the *Back* button that goes
    one level up to the results table (NOT the 'Return to Business Search').

    Strategy:
    - Look for any <button> or <a> whose visible text is exactly 'Back'
    - Click the first match.
    - If not found, fall back to #btnReturnToSearch, then history.back().
    """
    js_back = r"""
var btns = document.querySelectorAll("button, a");
for (var i = 0; i < btns.length; i++) {
    var el = btns[i];
    var text = (el.textContent || "").trim();
    if (text === "Back") {
        el.click();
        return true;
    }
}
return false;
"""
    try:
        clicked = sb.execute_script(js_back)
        if clicked:
            print("[NAV] Clicked detail 'Back' button (one level up to results).")
            return True
    except Exception as e:
        print(f"[NAV] JS error while trying to click detail 'Back' button: {e}")

    # Fallback 1: ReturnToSearch (may jump further than we like but better than stuck)
    try:
        if sb.is_element_present("#btnReturnToSearch"):
            print("[NAV] Fallback: clicking #btnReturnToSearch from details.")
            sb.click("#btnReturnToSearch")
            return True
    except Exception as e:
        print(f"[NAV] Error clicking #btnReturnToSearch fallback: {e}")

    # Fallback 2: history.back()
    try:
        print("[NAV] Fallback: using window.history.back() from details.")
        sb.execute_script("window.history.back();")
        return True
    except Exception as e:
        print(f"[NAV] Error calling window.history.back(): {e}")

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
    Opens BusinessInformation via Angular, scrapes Filing History + PDFs ASAP,
    then saves & parses the Business Information HTML, and returns a record dict.
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

    # --- (Angular agent fallback code stays the same here) ---

    print(f"[DETAIL] Opening BusinessInformation for BusinessID={biz_id}...")

    # --- Angular JS open (unchanged) ---
    open_js = r"""
var bid = arguments[0];
if (typeof angular === "undefined") return JSON.stringify({ok:false, error:"angular not found"});
var tbody = document.querySelector("tbody[ng-show*='businessList']");
if (!tbody) return JSON.stringify({ok:false, error:"tbody not found"});

var el = tbody, foundScope = null;
for (var i=0;i<6 && el;i++){
    var s = angular.element(el).scope() || angular.element(el).isolateScope();
    if (s && typeof s.showBusineInfo === "function") { foundScope = s; break; }
    el = el.parentElement;
}
if (!foundScope) return JSON.stringify({ok:false, error:"showBusineInfo not found"});

try { foundScope.showBusineInfo(bid); foundScope.$applyAsync(); return JSON.stringify({ok:true}); }
catch(e){ return JSON.stringify({ok:false, error:String(e)}); }
"""
    try:
        raw = sb.execute_script(open_js, biz_id)
        result = json.loads(raw) if isinstance(raw, str) else raw
        if not result.get("ok"):
            print(f"[DETAIL] Failed to open BI for {biz_id}: {result.get('error')}")
            return None
    except Exception as e:
        print(f"[DETAIL] JS error calling showBusineInfo: {e}")
        return None

    # --- Wait for BI container to appear ---
    timeout = 10 if first_detail_for_keyword else 5
    try:
        sb.wait_for_element("#divBusinessInformation", timeout=timeout)
    except:
        print(f"[DETAIL] BI not visible for {biz_id}")
        return None

    # Clear any alert
    dismiss_any_alert(sb)

    # ------------------------------------------------------------------
    # 🔴 NEW ORDER: Go to Filing History + PDFs *immediately*,
    # BEFORE waiting for Business Name to settle
    # ------------------------------------------------------------------
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

    # At this point, scrape_filing_history_and_pdfs() should have brought us
    # back to the Business Information view.
    dismiss_any_alert(sb)

    # ---------------------------------------------------------
    # RE-WAIT: ensure Business Information is fully reloaded
    # after coming back from Filing History
    # ---------------------------------------------------------
    try:
        # Make sure the BI panel is visible again
        sb.wait_for_element("#divBusinessInformation", timeout=10)
    except Exception:
        print(f"[DETAIL] BI panel not visible after Filing History for {biz_id}")
        # We can still attempt to scrape, but it may be incomplete

    # Now wait for Business Name or key text to appear
    name_js = r"""
var el = document.querySelector("#divBusinessInformation strong[data-ng-bind*='BusinessName']");
return el ? (el.textContent||"").trim() : "";
"""
    name = ""
    polls = 15 if first_detail_for_keyword else 8
    for _ in range(polls):
        try:
            name = (sb.execute_script(name_js) or "").strip()
        except Exception:
            name = ""
        if name:
            break
        sb.sleep(1)

    # Optional: small extra buffer for other BI fields (agent, addresses, etc.)
    sb.sleep(2)

    # --- Ensure BI is fully back and bound AFTER Filing History ---
    try:
        # 1) Wait for the BI container to be visible again
        sb.wait_for_element("#divBusinessInformation", timeout=10)
    except Exception:
        print(f"[DETAIL] Warning: BI container not visible after Filing History for {biz_id}")

    # 2) Give Angular a bit of time to re-bind fields
    sb.sleep(2)

    # 3) Re-poll the Business Name so we don't grab a half-loaded page
    expected_name = (record.get("BusinessName") or "").strip()
    refreshed_name = ""
    for _ in range(10):
        try:
            refreshed_name = (sb.execute_script(name_js) or "").strip()
        except Exception:
            refreshed_name = ""

        if refreshed_name:
            # Optionally sanity-check it against the Angular name
            if expected_name and expected_name.lower() not in refreshed_name.lower():
                print(f"[DETAIL] Note: BI name mismatch after FH for {biz_id}: "
                      f"expected ~'{expected_name}', got '{refreshed_name}'")
            break

        sb.sleep(1)

    # --- Save BI HTML AFTER Filing History is done ---
    html_detail = sb.get_page_source()
    safe_kw = sanitize_for_filename(keyword)
    html_path = details_dir / f"bi_html_{letter}_{safe_kw}_p{page_idx+1}_r{idx+1}_bid_{biz_id}.html"
    html_path.parent.mkdir(parents=True, exist_ok=True)
    html_path.write_text(html_detail, encoding="utf-8")

    # --- Parse BI HTML ---
    detail_parsed = parse_business_information_html(html_detail)
    if detail_parsed is None:
        print(f"[DETAIL] parse_business_information_html() returned None for BusinessID={biz_id}.")
    else:
        print(f"[DETAIL] Parsed {len(detail_parsed)} BI fields for BusinessID={biz_id}")

    # --- Fill record ---
    record["BusinessInformationHTMLPath"] = str(html_path)
    record["BusinessInformationHTML"] = detail_parsed
    record["FilingHistoryRecords"] = filings
    record["PDFSummaries"] = pdf_summaries
    record["PDFDownloadedCount"] = len(pdf_summaries)

    # --- Back to search results ---
    try:
        click_back_with_cf(sb, description=f"BusinessID={biz_id}")
        dismiss_any_alert(sb)
    except Exception as e:
        print(f"[DETAIL] Warning returning to results ({biz_id}): {e}")

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
    try:
        with pdfplumber.open(pdf_path) as pdf:
            texts = [(p.extract_text() or "") for p in pdf.pages]
    except Exception as e:
        print(f"[PDF] Failed to open {pdf_path}: {e}")
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
            print(f"[FILING] Clicking Filing History via selector: {sel}")
            sb.click(sel)
            return True
        except Exception:
            continue

    print("[FILING] Filing History tab not found/visible.")
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
        print("[FILING] Filing History table not found.")
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
        print(f"[PDF] Request failed for filing {filing_number}: {e}")
        return False

    if resp.status_code != 200:
        print(
            f"[PDF] Non-200 status ({resp.status_code}) for filing {filing_number}"
        )
        return False

    # Quick content-type sanity check
    ctype = (resp.headers.get("Content-Type") or "").lower()
    if "pdf" not in ctype:
        print(
            f"[PDF] Filing {filing_number} content is not PDF (Content-Type={ctype})"
        )

    try:
        with open(save_path, "wb") as f:
            f.write(resp.content)
        print(f"[PDF] Saved filing {filing_number} to {save_path}")
        return True
    except Exception as e:
        print(f"[PDF] Failed to write PDF {save_path}: {e}")
        return False

def close_view_documents_modal(sb, timeout: int = 5) -> bool:
    """
    Try to close the 'View Documents' modal if it is open.

    Uses the real HTML:
      <button class="close" data-dismiss="modal" ...>×</button>

    Returns True if the modal is gone, False otherwise.
    """
    try:
        for _ in range(timeout):
            # If no modal-dialog is present, we're done
            if not sb.is_element_present("css=.modal-dialog"):
                return True

            # Try the exact button you showed
            try:
                btns = sb.driver.find_elements(
                    "css selector", "button.close[data-dismiss='modal']"
                )
                for b in btns:
                    if b.is_displayed():
                        sb.driver.execute_script("arguments[0].click();", b)
                        sb.sleep(1)
                        if not sb.is_element_present("css=.modal-dialog"):
                            print("[PDF] Closed modal via button.close[data-dismiss='modal'].")
                            return True
            except Exception:
                pass

            # Fallbacks: any visible .close inside a modal
            try:
                close_candidates = sb.driver.find_elements(
                    "css selector", ".modal-content .close, .modal-dialog .close"
                )
                for c in close_candidates:
                    if c.is_displayed():
                        sb.driver.execute_script("arguments[0].click();", c)
                        sb.sleep(1)
                        if not sb.is_element_present("css=.modal-dialog"):
                            print("[PDF] Closed modal via .modal-content/.modal-dialog .close.")
                            return True
            except Exception:
                pass

            sb.sleep(1)

        print("[PDF] Failed to close modal using all known selectors.")
        return False

    except Exception as e:
        print(f"[PDF] Exception in close_view_documents_modal: {e}")
        return False
    
def click_back_with_cf1(sb, description: str = ""):
    """
    Return from:
       - View Documents modal  -> Filing History
       - Filing History        -> Business Information
       - Business Information  -> Business Search results table

    Handles Cloudflare/Turnstile if needed, but does NOT use window.history.back()
    anymore (to avoid overshooting past the results list).

    description: optional label for logging (e.g. 'BusinessID=123').
    """

    RESULTS_SELECTORS = [
        "css=tbody[ng-show*='businessList'] tr",     # tbody with ng-show on businessList
        "css=tbody tr[ng-repeat*='business']",       # rows in results table
    ]

    def close_view_documents_modal_if_open():
        """Close the 'View Documents' modal if it is still open."""
        try:
            # any open modal-dialog
            if sb.is_element_present("css=.modal-dialog"):
                print("[NAV] Modal detected; attempting to close it.")
                # preferred close button for this modal
                try:
                    sb.click("css=button.close[data-dismiss='modal']")
                    sb.sleep(1)
                    return
                except Exception:
                    pass
                # fallback close buttons inside modal
                try:
                    sb.click("css=#divSearchResult button.close")
                    sb.sleep(1)
                    return
                except Exception:
                    pass
                try:
                    sb.click("css=.modal-content .close")
                    sb.sleep(1)
                    return
                except Exception:
                    pass
                print("[NAV] WARNING: Could not close modal via known selectors.")
        except Exception as e:
            print(f"[NAV] Error while checking/closing modal: {e}")

    def wait_for_results_grid(label: str) -> bool:
        """Best-effort: check if the results table appears. No extra navigation."""
        for sel in RESULTS_SELECTORS:
            try:
                if sb.is_element_present(sel):
                    print(f"[NAV] Results grid detected after {label} via selector: {sel}")
                    return True
            except Exception:
                pass
        return False

    try:
        if description:
            print(f"[NAV] Returning to results ({description})...")

        # --- 0) If a 'View Documents' modal is open, close it first ---
        close_view_documents_modal_if_open()

        # --- 1) If there's a Cloudflare / Turnstile challenge, give user time ---
        # Note: this site uses a <cf-turnstile> widget; the visible wrapper has class 'cf-turnstile'.
        if sb.is_element_present("css=.cf-turnstile") or sb.is_element_present("css=#cf-challenge"):
            print("[NAV] Cloudflare / Turnstile challenge detected while returning.")
            print("[NAV] Please solve it in the browser; waiting up to 5 minutes...")
            for _ in range(300):  # up to ~5 minutes
                if not (sb.is_element_present("css=.cf-turnstile") or
                        sb.is_element_present("css=#cf-challenge")):
                    break
                time.sleep(1)

        # --- 2) If we're on Filing History, click 'Back to Business Information' ---
        # That button only exists on the Filing History view.
        try:
            if sb.is_element_present("css=button[ng-click*='showBusineInfo']"):
                print("[NAV] On Filing History: clicking 'Back to Business Information'.")
                sb.click("css=button[ng-click*='showBusineInfo']")
                sb.sleep(8)  # allow BI to load
            else:
                print("[NAV] No 'Back to Business Information' button found; likely not on Filing History.")
        except Exception as e:
            print(f"[NAV] Error clicking 'Back to Business Information': {e}")

        # --- 3) From Business Information, click 'Return to Business Search' ---
        # On BI, this is the button that takes you back to the results list.
        try:
            if sb.is_element_present("css=#btnReturnToSearch"):
                print("[NAV] On BI: clicking 'Return to Business Search'.")
                sb.click("css=#btnReturnToSearch")
                sb.sleep(8)
            else:
                print("[NAV] '#btnReturnToSearch' not found on this view.")
        except Exception as e:
            print(f"[NAV] Error clicking '#btnReturnToSearch': {e}")

        # --- 4) Cloudflare/Turnstile might appear again after navigation ---
        if sb.is_element_present("css=.cf-turnstile") or sb.is_element_present("css=#cf-challenge"):
            print("[NAV] Cloudflare / Turnstile detected after navigation.")
            print("[NAV] Please solve it in the browser; waiting up to 5 minutes...")
            for _ in range(300):
                if not (sb.is_element_present("css=.cf-turnstile") or
                        sb.is_element_present("css=#cf-challenge")):
                    break
                time.sleep(1)

        # --- 5) Best-effort check: are we back on the results list? ---
        if not wait_for_results_grid("click_back_with_cf()"):
            print("[NAV] WARNING: Results grid not detected, but NOT calling history.back(). "
                  "Continuing from current page anyway.")
        else:
            print("[NAV] Back on results list; navigation complete.")

        return True

    except Exception as e:
        print(f"[NAV] Unexpected error in click_back_with_cf(): {e}")
        return False

def click_back_with_cf2(sb, description: str = ""):
    """
    Robust navigation back to the BusinessSearch results grid.

    Handles, in order:
      - If a "View Documents" modal is open, close it.
      - Cloudflare challenge (waits for manual solve if present).
      - Uses up to 3 history steps and/or Return button to get back
        to the results list, checking the grid after each step.

    This function is designed to be safe whether you call it from:
      - Filing History (after downloading PDFs), or
      - Business Information, or
      - Already on the search results.
    """

    # --- Helpers -----------------------------------------------------------
    RESULTS_SELECTORS = [
        "css=tbody[ng-show*='businessList'] tr",   # original working selector
        "css=tbody tr[ng-repeat*='business']",     # rows in results table
        "css=table.table-striped tbody tr[ng-repeat]",  # generic Angular rows
    ]

    def results_visible(label: str = "") -> bool:
        """Check if the BusinessSearch results grid is present."""
        for sel in RESULTS_SELECTORS:
            try:
                sb.wait_for_element(sel, timeout=3)
                if label:
                    print(f"[NAV] Results grid detected ({label}) via selector: {sel}")
                else:
                    print(f"[NAV] Results grid detected via selector: {sel}")
                return True
            except Exception:
                continue
        return False

    def close_any_modal():
        """If a modal (like 'View Documents') is open, close it."""
        try:
            # Most accurate selector for the X button you showed:
            if sb.is_element_present("css=button.close[data-dismiss='modal']"):
                print("[NAV] Closing modal via button.close[data-dismiss='modal'].")
                sb.click("css=button.close[data-dismiss='modal']")
                sb.sleep(2)
                return

            # Fallback: any close button inside a visible modal
            if sb.is_element_present("css=.modal-dialog .close"):
                print("[NAV] Closing modal via .modal-dialog .close.")
                sb.click("css=.modal-dialog .close")
                sb.sleep(2)
                return

            # Extra fallback: click backdrop (if any)
            if sb.is_element_present("css=.modal-backdrop"):
                print("[NAV] Clicking modal backdrop as last-resort close.")
                sb.click("css=.modal-backdrop")
                sb.sleep(2)
        except Exception as e:
            print(f"[NAV] Warning: error while trying to close modal: {e}")

    def handle_cloudflare(context: str):
        """Wait (up to ~5 minutes) if a Cloudflare challenge is present."""
        try:
            if sb.is_element_present("css=#cf-challenge") or sb.is_element_present("css=.cf-challenge"):
                print(f"[NAV] Cloudflare challenge detected ({context}).")
                print("[NAV] Please solve it in the browser; I'll wait up to 5 minutes.")
                for _ in range(300):
                    if not (
                        sb.is_element_present("css=#cf-challenge")
                        or sb.is_element_present("css=.cf-challenge")
                    ):
                        print("[NAV] Cloudflare challenge cleared.")
                        break
                    time.sleep(1)
        except Exception as e:
            print(f"[NAV] Warning while checking Cloudflare: {e}")

    # --- Main logic --------------------------------------------------------
    if description:
        print(f"[NAV] Returning to results ({description})...")

    # Step 0: if a modal is open, close it first
    if sb.is_element_present("css=.modal-dialog"):
        print("[NAV] Modal detected while returning to results; closing it first.")
        close_any_modal()
        sb.sleep(1)

    # If we are already on the results page, just confirm and return
    if results_visible("initial check"):
        return True

    # We'll try up to 3 navigation steps to get back
    for step in range(2):
        handle_cloudflare(f"before nav step {step+1}")

        # If after Cloudflare we already see results, we're done
        if results_visible(f"after Cloudflare step {step+1}"):
            return True

        # Prefer the explicit "Return to Business Search" button if present
        try:
            if sb.is_element_present("css=#btnReturnToSearch"):
                print("[NAV] Clicking '#btnReturnToSearch' to go back to results.")
                sb.click("css=#btnReturnToSearch")
                sb.sleep(6)

                if results_visible(f"after #btnReturnToSearch (step {step+1})"):
                    return True
                # If not, continue to next loop iteration
                continue
        except Exception as e:
            print(f"[NAV] Warning clicking #btnReturnToSearch: {e}")

        # Otherwise, rely on browser history
        try:
            print(f"[NAV] Using window.history.back() (step {step+1}).")
            sb.driver.execute_script("window.history.back()")
            sb.sleep(15)
        except Exception as e:
            print(f"[NAV] Warning calling window.history.back(): {e}")

        # Check if we are back on results after this history step
        if results_visible(f"after history.back() (step {step+1})"):
            return True

    # Final check after all attempts
    if results_visible("final check"):
        return True

    print("[NAV] WARNING: Could not detect results grid after all navigation attempts.")
    return False

def click_back_with_cf3(sb, description: str = ""):
    """
    Robust navigation back to the BusinessSearch results grid.

    Handles, in order:
      - If a "View Documents" modal is open, close it.
      - Cloudflare challenge (waits for manual solve if present).
      - Uses Filing History "Back to Business Information",
        Business Information "Return to Business Search",
        and finally window.history.back() as fallback.

    It is safe to call this when:
      - On Filing History (after PDFs),
      - On Business Information,
      - Already on BusinessSearch results.

    It will NOT keep calling history.back() once the results
    grid is detected on the BusinessSearch route.
    """

    # --- Helper selectors / route checks -----------------------------------
    RESULTS_SELECTORS = [
        "css=tbody[ng-show*='businessList'] tr",     # original working selector
        "css=tbody tr[ng-repeat*='business']",       # rows in results table
        "css=table.table-striped tbody tr[ng-repeat]"  # generic Angular rows
    ]

    def get_url() -> str:
        try:
            return sb.driver.current_url or ""
        except Exception:
            return ""

    def is_on_results_page() -> bool:
        """We only treat it as 'results' if URL has BusinessSearch AND grid present."""
        url = get_url()
        if "BusinessSearch" not in url:
            return False
        for sel in RESULTS_SELECTORS:
            try:
                sb.wait_for_element(sel, timeout=3)
                print(f"[NAV] Results grid detected on BusinessSearch via selector: {sel}")
                return True
            except Exception:
                continue
        return False

    def is_on_bi_page() -> bool:
        """Heuristic: BI route + 'Return to Business Search' button."""
        url = get_url()
        if "BusinessInformation" not in url:
            return False
        return sb.is_element_present("css=#btnReturnToSearch")

    def is_on_filing_history_page() -> bool:
        """
        Heuristic: presence of Filing History 'Back to Business Information' button.
        The same button exists only on Filing History tab.
        """
        return sb.is_element_present("css=button[ng-click*='showBusineInfo']")

    def close_any_modal():
        """If a modal (like 'View Documents') is open, close it via the X button."""
        try:
            # Most accurate selector for the X button you showed:
            if sb.is_element_present("css=button.close[data-dismiss='modal']"):
                print("[NAV] Closing modal via button.close[data-dismiss='modal'].")
                sb.click("css=button.close[data-dismiss='modal']")
                sb.sleep(2)
                return

            # Fallback: any close button inside a visible modal
            if sb.is_element_present("css=.modal-dialog .close"):
                print("[NAV] Closing modal via .modal-dialog .close.")
                sb.click("css=.modal-dialog .close")
                sb.sleep(2)
                return

            # Extra fallback: click backdrop (if any)
            if sb.is_element_present("css=.modal-backdrop"):
                print("[NAV] Clicking modal backdrop as last-resort close.")
                sb.click("css=.modal-backdrop")
                sb.sleep(2)
        except Exception as e:
            print(f"[NAV] Warning: error while trying to close modal: {e}")

    def handle_cloudflare(context: str):
        """Wait (up to ~5 minutes) if a Cloudflare challenge is present."""
        try:
            if sb.is_element_present("css=#cf-challenge") or sb.is_element_present("css=.cf-challenge"):
                print(f"[NAV] Cloudflare challenge detected ({context}).")
                print("[NAV] Please solve it in the browser; I'll wait up to 5 minutes.")
                for _ in range(300):
                    if not (
                        sb.is_element_present("css=#cf-challenge")
                        or sb.is_element_present("css=.cf-challenge")
                    ):
                        print("[NAV] Cloudflare challenge cleared.")
                        break
                    time.sleep(1)
        except Exception as e:
            print(f"[NAV] Warning while checking Cloudflare: {e}")

    # --- Main logic --------------------------------------------------------
    if description:
        print(f"[NAV] Returning to results ({description})...")

    # Step 0: if a modal is open, close it first
    if sb.is_element_present("css=.modal-dialog"):
        print("[NAV] Modal detected while returning to results; closing it first.")
        close_any_modal()
        sb.sleep(1)

    # Early exit: if we are already on the results page, just confirm and return
    if is_on_results_page():
        return True

    # We will attempt up to 3 navigation actions total
    for step in range(3):
        handle_cloudflare(f"before nav step {step+1}")

        # If after Cloudflare we already see the results, we're done
        if is_on_results_page():
            return True

        # 1) If we are on Filing History, go BI via "Back to Business Information"
        if is_on_filing_history_page():
            print("[NAV] On Filing History: clicking 'Back to Business Information'.")
            try:
                sb.click("css=button[ng-click*='showBusineInfo']")
                sb.sleep(6)
            except Exception as e:
                print(f"[NAV] Warning clicking 'Back to Business Information': {e}")
            # After this click we should be on BI; continue loop to handle BI -> Search.
            continue

        # 2) If we are on Business Information, prefer the explicit "Return" button
        if is_on_bi_page():
            print("[NAV] On Business Information: clicking '#btnReturnToSearch'.")
            try:
                sb.click("css=#btnReturnToSearch")
                sb.sleep(6)
            except Exception as e:
                print(f"[NAV] Warning clicking #btnReturnToSearch: {e}")
            # After this we expect BusinessSearch; re-check in next iteration
            if is_on_results_page():
                return True
            continue

        # 3) If neither Filing History nor BI heuristics match, but we end up
        #    on BusinessSearch route without grid yet, give it some time.
        url = get_url()
        if "BusinessSearch" in url:
            print("[NAV] On BusinessSearch route but grid not yet visible; waiting briefly.")
            sb.sleep(4)
            if is_on_results_page():
                return True
            # If still no grid, fall through to history.back() as a last resort.

        # 4) Last resort: use browser history to go back exactly one step
        print(f"[NAV] Using window.history.back() (step {step+1}).")
        try:
            sb.driver.execute_script("window.history.back()")
            sb.sleep(8)
        except Exception as e:
            print(f"[NAV] Warning calling window.history.back(): {e}")

        # After history, if we are on results page, stop immediately.
        if is_on_results_page():
            return True

        # If we find ourselves on AdvancedSearch, do NOT go back further.
        url_after = get_url()
        if "AdvancedSearch" in url_after:
            print("[NAV] Reached AdvancedSearch page; stopping further back navigation.")
            break

    # Final check after all attempts
    if is_on_results_page():
        return True

    print("[NAV] WARNING: Could not detect results grid after all navigation attempts.")
    return False

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
            print("[NAV] Detected Business Search RESULTS page.")
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
                print("[NAV] Closing modal via button.close[data-dismiss='modal'].")
                sb.click("css=button.close[data-dismiss='modal']")
                sb.sleep(2)
                return

            if sb.is_element_present("css=.modal-dialog .close"):
                print("[NAV] Closing modal via .modal-dialog .close.")
                sb.click("css=.modal-dialog .close")
                sb.sleep(2)
                return

            if sb.is_element_present("css=.modal-backdrop"):
                print("[NAV] Clicking modal backdrop as last-resort close.")
                sb.click("css=.modal-backdrop")
                sb.sleep(2)
        except Exception as e:
            print(f"[NAV] Warning: error while trying to close modal: {e}")

    def handle_cloudflare(context: str):
        """Wait (up to ~5 minutes) if a Cloudflare challenge is present."""
        try:
            if sb.is_element_present("css=#cf-chl-widget") or sb.is_element_present("css=.cf-turnstile"):
                print(f"[NAV] Cloudflare Turnstile detected ({context}).")
                print("[NAV] Please solve it in the browser; I'll wait up to 5 minutes.")
                for _ in range(300):
                    if not (
                        sb.is_element_present("css=#cf-chl-widget")
                        or sb.is_element_present("css=.cf-turnstile")
                    ):
                        print("[NAV] Cloudflare challenge cleared.")
                        break
                    time.sleep(1)
        except Exception as e:
            print(f"[NAV] Warning while checking Cloudflare: {e}")

    # --- Main logic --------------------------------------------------------
    if description:
        print(f"[NAV] Returning to results ({description})...")

    # Step 0: close any modal (e.g., View Documents)
    if sb.is_element_present("css=.modal-dialog"):
        print("[NAV] Modal detected while returning to results; closing it first.")
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
                print("[NAV] On Business Information; clicking '.btn-back' to go to results.")
                sb.click("css=button.btn-back")
                sb.sleep(5)

                # Wait a bit for the results page to render
                for _ in range(10):
                    if is_on_search_results():
                        return True
                    if reached_advanced_search():
                        print("[NAV] Landed on AdvancedSearch after '.btn-back'; stopping.")
                        return False
                    time.sleep(1)
        except Exception as e:
            print(f"[NAV] Warning while clicking '.btn-back': {e}")

    # Step 3: Fallback – use history.back() a few times, checking after each
    for step in range(3):
        handle_cloudflare(f"before history.back step {step+1}")

        # Re-check before navigating
        if is_on_search_results():
            return True
        if reached_advanced_search():
            print("[NAV] Already on AdvancedSearch; not navigating back further.")
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

        print(f"[NAV] Using window.history.back() (step {step+1}).")
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
                print("[NAV] Reached AdvancedSearch page; stopping further back navigation.")
                return False
            time.sleep(1)

    # Final check
    if is_on_search_results():
        return True

    print("[NAV] WARNING: Could not detect results grid after all navigation attempts.")
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
        print(f"[DETAIL] Warning: could not return to results ({desc}).")
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
                print(f"[FILING] Clicked Business Information control via selector: {sel}")
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
                    print("[FILING] Clicked element with text containing 'BUSINESS INFORMATION'")
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
                print(f"[PDF] Closed modal via selector: {sel}")
                return True
        except Exception:
            pass
    print("[PDF] Failed to close modal using all known selectors.")
    return False



def scrape_keyword(sb: SB, keyword: str, letter: str, out_dir: Path, first_keyword: bool):
    print(f"\n[===] SCRAPING keyword '{keyword}' (letter {letter}) [===]")

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
            sb.select_option_by_value("#entityStatus", "1")   # ACTIVE

            # Click search
            sb.click("#btnSearch")

            # Auto-dismiss any stray 'null' alert right after search
            dismiss_any_alert(sb)

            form_ok = True
            break

        except Exception as e:
            print(f"[WARN] Form fill failed on attempt {attempt}/3 for keyword '{keyword}': {e}")
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
        print("[INFO] Waiting for the first results table to appear...")
        print("[INFO] If a Cloudflare / Turnstile challenge appears, solve it in the browser; "
              "this script will keep waiting.")
    try:
        sb.wait_for_element("css=table.table-striped", timeout=30)
    except Exception:
        print("[WARN] table.table-striped not found after waiting; continuing anyway.")

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

    visited_detail_ids = set()

    while True:
        sb.sleep(2)
        html = sb.get_page_source()

        # --- Save full HTML for this page for debugging ---
        debug_path = debug_dir / f"debug_{letter}_{safe_kw}_page_{page_index}.html"
        debug_path.write_text(html, encoding="utf-8")
        print(f"[DEBUG] Saved {debug_path}")

        # --- Parse rows from this page ---
        rows = parse_rows(html)

        # If we see 0 rows on page 1, try a few incremental waits (5s, 10s, 15s)
        if page_index == 1 and len(rows) == 0:
            for retry in range(1, 4):
                wait_s = 5 * retry
                print(f"[WARN] 0 rows for keyword '{keyword}' on page 1; retry {retry}/3 after {wait_s}s...")
                sb.sleep(wait_s)
                html = sb.get_page_source()
                debug_retry_path = debug_dir / f"debug_{letter}_{safe_kw}_page_{page_index}_retry_{retry}.html"
                debug_retry_path.write_text(html, encoding="utf-8")
                print(f"[DEBUG] Saved {debug_retry_path}")
                rows = parse_rows(html)
                if rows:
                    break

        print(f"[DEBUG] parse_rows: found {len(rows)} rows")
        print(f"[PAGE {page_index}] Extracted {len(rows)} rows for keyword '{keyword}'")
        keyword_rows.extend(rows)
        pages_info.append({"page": page_index, "rows_on_page": len(rows)})

        # --- Grab rich per-business objects via Angular's businessList ---
        business_list = get_business_list_via_angular(sb)
        if not business_list:
            print("[API] No Angular businessList found on this page.")
        else:
            print(f"[API] Angular businessList has {len(business_list)} entries on this page.")

            # Save Angular "API-like" data for this page
            api_pages.append(
                {
                    "page": page_index,
                    "business_list": business_list,
                }
            )

            # Optionally, open HTML BusinessInformation for each business
            # Optionally, open HTML BusinessInformation for each business
            if FETCH_HTML_DETAILS:
                for idx, biz in enumerate(business_list):
                    bid = biz.get("BusinessID") or biz.get("ID")
                    if not bid or bid in visited_detail_ids:
                        continue

                    first_detail_for_kw = (details_success == 0 and details_failed == 0)

                    detail_rec = fetch_business_information_via_html(
                        sb=sb,
                        biz_obj=biz,
                        letter=letter,
                        keyword=keyword,
                        letter_idx=0,              # placeholder; currently unused inside the function
                        page_idx=page_index - 1,   # internal 0-based index; filenames do +1
                        idx=idx,                   # row index within current page
                        out_dir=out_dir,           # same out_dir you passed into scrape_keyword
                        details_dir=details_dir,
                        first_detail_for_keyword=first_detail_for_kw,
                    )

                    if detail_rec is not None:
                        html_details_records.append(detail_rec)
                        details_success += 1
                        visited_detail_ids.add(bid)
                    else:
                        details_failed += 1


        # --- Parse pager text (with retries if we catch the 0-of-0 placeholder) ---
        html = sb.get_page_source()
        pager = parse_pager(html)
        retry_count = 0
        while not pager and retry_count < 5:
            print("[WARN] No valid pager found (or placeholder 0-of-0). Retrying...")
            sb.sleep(2)
            html = sb.get_page_source()
            pager = parse_pager(html)
            retry_count += 1

        if not pager:
            print(f"[ERROR] Still no valid pager after retries; stopping keyword '{keyword}'.")
            break

        current_page = pager["page"]
        total_pages = pager["total_pages"]
        print(f"[PAGER] Keyword '{keyword}': Page {current_page} of {total_pages}")

        page_index = current_page

        # If this is the last page, stop
        if current_page >= total_pages:
            print(f"[INFO] Keyword '{keyword}': reached last page; stopping.")
            break

        next_page = current_page + 1

        # --- Try JS Next (›) first ---
        moved = click_next_js(sb)
        if not moved:
            # Fallback: JS click by page number
            moved = click_page_number_js(sb, next_page)

        if not moved:
            print(f"[INFO] Keyword '{keyword}': could not move to page {next_page}; stopping.")
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
        print(
            f"[DETAIL] Saved {len(html_details_records)} BusinessInformation HTML records "
            f"for keyword '{keyword}' to {details_path}"
        )

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

    # Build result structure for this keyword
    keyword_result = {
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
    }

    print(
        f"[SUMMARY] Keyword '{keyword}': pages_visited={page_index}, "
        f"records_scraped={len(keyword_rows)}, "
        f"details_success={details_success}, details_failed={details_failed}, "
        f"api_records={api_total_records}"
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
    print("[FILING] Waiting 10 seconds for Cloudflare / Filing History to load...")
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
        print(f"[PDF] Warning: could not set Chrome download path: {e}")

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
                print(f"[PDF] Opened View Documents modal for filing {filing_no} via row index {idx}")
        except Exception as e:
            print(f"[PDF] JS error opening modal for filing {filing_no}: {e}")

        if not clicked:
            print(f"[PDF] Could not open 'View Documents' modal for filing {filing_no}.")
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
            print(f"[PDF] View Documents modal did not become visible for filing {filing_no}")
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
    var tds = rows[i].querySelectorAll("td");
    if (tds.length < 3) continue;
    var docType = (tds[0].textContent || "").toUpperCase();
    if (!docType.includes("FULFILLED")) continue;
    var icon = rows[i].querySelector("i.fa-file-text-o");
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
                print(f"[PDF] No 'FULFILLED' document found in modal for filing {filing_no}")
                close_view_documents_modal(sb, timeout=2)
                continue

            print(f"[PDF] Clicked paper icon for 'FULFILLED' document in modal for filing {filing_no}")
        except Exception as e:
            print(f"[PDF] Failed to click 'FULFILLED' paper icon for filing {filing_no}: {e}")
            close_view_documents_modal(sb, timeout=2)
            continue

        # 3d) Wait for the new PDF to appear in download_dir
        new_pdf_path = wait_for_new_pdf(download_dir, before_files, timeout=60)
        if not new_pdf_path:
            print(f"[PDF] No new PDF detected for filing {filing_no}")
            close_view_documents_modal(sb, timeout=2)
            continue

        print(f"[PDF] Downloaded PDF: {new_pdf_path}")

        # 3e) Close modal right after PDF download (best effort)
        if not close_view_documents_modal(sb, timeout=5):
            print("[PDF] Could not close modal after download (continuing anyway).")

        # 3f) Move/copy to final target filename in pdf_root
        try:
            pdf_dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(new_pdf_path, pdf_dest)
        except Exception as e:
            print(f"[PDF] Warning: could not copy PDF to {pdf_dest}: {e}")

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

def run_letter(letter="A", out_dir="./output_wa_combined", headless=False):
    """
    Main entry to scrape all keywords for a given letter.
    Loads keywords from search_keywords/<letter>.txt,
    scrapes each one in a single browser session, and saves:

    - wa_results_<LETTER>.json      (HTML table rows)
    - wa_tracking_<LETTER>.json     (merged tracking per keyword)
    - api/wa_api_<LETTER>_<KW>.json (per-keyword Angular data)
    - bi_html/wa_bi_<LETTER>_<KW>.json (per-keyword BusinessInformation details)
    """
    letter = str(letter).upper()
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    keywords = load_keywords(letter)
    if not keywords:
        print("[ERROR] No keywords to scrape; exiting.")
        return

    all_keywords_results = []
    tracking_combined = []

    with SB(
        uc=True,
        headless=headless,
        # If you want to use your Webshare proxy, uncomment the next line:
        # proxy=PROXY_URL if PROXY_URL else None,
    ) as sb:
        sb.open(ADV_URL)
        sb.sleep(2)

        # Run through all keywords for this letter
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
                all_keywords_results.append(result)

                # Combined tracking per keyword
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

    # Build final letter-level result
    letter_result = {
        "letter": letter,
        "keywords": all_keywords_results,
    }

    # Save result JSON per letter (HTML table scrape)
    results_path = out_dir / f"wa_results_{letter}.json"
    with results_path.open("w", encoding="utf-8") as f:
        json.dump(letter_result, f, indent=2)

    # Save combined tracking JSON per letter
    combined_tracking_path = out_dir / f"wa_tracking_{letter}.json"
    with combined_tracking_path.open("w", encoding="utf-8") as f:
        json.dump(tracking_combined, f, indent=2)

    total_records = sum(k["records_scraped"] for k in all_keywords_results)
    print(
        f"\n[DONE] Letter {letter}: scraped {len(all_keywords_results)} keywords, "
        f"total {total_records} records."
    )
    print(f"[SAVED] {results_path}")
    print(f"[SAVED] {combined_tracking_path}")

    return letter_result


if __name__ == "__main__":
    # Usage:
    #   python3 wa_search_sb_local21.py A
    # If no arg, defahtml_detail = sb.get_page_source()ult to 'A'
    if len(sys.argv) > 1:
        letter_arg = sys.argv[1]
    else:
        letter_arg = "A2"

    run_letter(letter_arg, out_dir="./output_wa_pdf38", headless=False)
