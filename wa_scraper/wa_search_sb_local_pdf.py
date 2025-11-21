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
    Opens BusinessInformation via Angular, scrapes HTML, scrapes Filing History,
    downloads PDFs (max 3), parses phone/email/executors, and returns a record dict.
    """

    biz_id = biz_obj.get("BusinessID") or biz_obj.get("ID")
    if not biz_id:
        return None

    # --- record skeleton ---
    record = {
        "BusinessID": biz_id,
        "UBINumber": biz_obj.get("UBINumber"),
        "BusinessName": biz_obj.get("BusinessName") or biz_obj.get("EntityName"),
        "BusinessStatus": biz_obj.get("BusinessStatus") or biz_obj.get("Status"),
        "BusinessType": biz_obj.get("BusinessType") or biz_obj.get("Type"),
    }

    # Save Angular agent fallback
    angular_agent_name = biz_obj.get("AgentName")
    angular_agent_street = None
    angular_agent_mailing = None
    agent_obj = biz_obj.get("Agent") or {}
    if isinstance(agent_obj, dict):
        street_addr = agent_obj.get("StreetAddress") or {}
        mailing_addr = agent_obj.get("MailingAddress") or {}
        if isinstance(street_addr, dict):
            angular_agent_street = street_addr.get("FullAddress")
        if isinstance(mailing_addr, dict):
            angular_agent_mailing = mailing_addr.get("FullAddress")

    print(f"[DETAIL] Opening BusinessInformation for BusinessID={biz_id}...")

    # --- Angular JS open ---
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

    # --- Wait for BI to load ---
    timeout = 30 if first_detail_for_keyword else 15
    try:
        sb.wait_for_element("#divBusinessInformation", timeout=timeout)
    except:
        print(f"[DETAIL] BI not visible for {biz_id}")
        return None

    dismiss_any_alert(sb)

    # --- Wait for Business Name to populate ---
    name_js = r"""
var el = document.querySelector("#divBusinessInformation strong[data-ng-bind*='BusinessName']");
return el ? (el.textContent||"").trim() : "";
"""
    name = ""
    polls = 30 if first_detail_for_keyword else 15
    for _ in range(polls):
        try:
            name = (sb.execute_script(name_js) or "").strip()
        except:
            name = ""
        if name:
            break
        sb.sleep(1)

    # --- Save BI HTML ---
    html_detail = sb.get_page_source()
    safe_kw = sanitize_for_filename(keyword)
    html_path = details_dir / f"bi_html_{letter}_{safe_kw}_p{page_idx+1}_r{idx+1}_bid_{biz_id}.html"
    html_path.parent.mkdir(parents=True, exist_ok=True)
    html_path.write_text(html_detail, encoding="utf-8")
    print(f"[DETAIL] Saved detail HTML → {html_path}")

    if not name:
        print(f"[DETAIL] BusinessName empty → abort detail for {biz_id}")
        try:
            click_detail_back_button(sb)
        except:
            pass
        return None

    # --- Parse BI HTML ---
    detail_parsed = parse_business_information_html(html_detail)

    # --- Filing History + PDFs ---
    try:
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
        if filings:
            record["FilingHistory"] = filings
        if pdf_summaries:
            record["FilingDocuments"] = pdf_summaries
    except Exception as e:
        print(f"[FILING] Error for {biz_id}: {e}")

    # --- Agent fallback fixes ---
    if not detail_parsed.get("agent_name") and angular_agent_name:
        detail_parsed["agent_name"] = angular_agent_name
    if not detail_parsed.get("agent_street") and angular_agent_street:
        detail_parsed["agent_street"] = angular_agent_street
    if not detail_parsed.get("agent_mailing") and angular_agent_mailing:
        detail_parsed["agent_mailing"] = angular_agent_mailing

    record["BusinessInformationHTMLPath"] = str(html_path)
    record["BusinessInformationHTML"] = detail_parsed

    # --- Back to search ---
    try:
        click_back_with_cf(sb)
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

def click_back_with_cf(sb):
    """
    From either Filing History or BusinessInformation, navigate all the way
    back to the search results list, with Cloudflare-aware waits.

    Path we handle:
      - Filing History -> BusinessInformation -> Search Results
      - Or directly: BusinessInformation -> Search Results
    """
    try:
        # 0) Already on results?
        if sb.is_element_present("css=tbody[ng-show*='businessList']"):
            print("[NAV] Already on results list.")
            return

        # 1) If 'Return to Business Search' is already visible, we're on BI.
        if sb.is_element_present("#btnReturnToSearch"):
            print("[NAV] On BI: clicking 'Return to Business Search'.")
            sb.click("#btnReturnToSearch")
            sb.sleep(9)  # Cloudflare / Angular
            sb.wait_for_element("css=tbody[ng-show*='businessList']", timeout=60)
            print("[NAV] Back on results after ReturnToSearch.")
            return

        # 2) Otherwise we’re probably on Filing History.
        #    Try its Back (showBusineInfo -> BI).
        if sb.is_element_present("css=button[ng-click*='showBusineInfo']"):
            print("[NAV] On Filing History: clicking Back to BI.")
            sb.click("css=button[ng-click*='showBusineInfo']")
            sb.sleep(9)  # Cloudflare / Angular

        # 3) Now we expect to be on BI. Prefer 'Return to Business Search' if present.
        if sb.is_element_present("#btnReturnToSearch"):
            print("[NAV] On BI now: clicking 'Return to Business Search'.")
            sb.click("#btnReturnToSearch")
            sb.sleep(9)
            sb.wait_for_element("css=tbody[ng-show*='businessList']", timeout=60)
            print("[NAV] Back on results after BI ReturnToSearch.")
            return

        # 4) Fallback: BI "Back" button (navBusinessSearch()).
        if sb.is_element_present("css=button[ng-click*='navBusinessSearch']"):
            print("[NAV] Using BI Back button (navBusinessSearch).")
            sb.click("css=button[ng-click*='navBusinessSearch']")
            sb.sleep(9)
            sb.wait_for_element("css=tbody[ng-show*='businessList']", timeout=60)
            print("[NAV] Back on results via BI Back.")
            return

        # 5) Last resort: browser history.
        print("[NAV] No explicit BI/Filing back buttons; using browser history().")
        sb.go_back()
        sb.sleep(9)
        if not sb.is_element_present("css=tbody[ng-show*='businessList']"):
            sb.go_back()
            sb.sleep(9)
        sb.wait_for_element("css=tbody[ng-show*='businessList']", timeout=60)
        print("[NAV] Back on results after history.back() fallback.")

    except Exception as e:
        print(f"[NAV] Error while returning to results: {e}")


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
            # sb.select_option_by_value("#entityStatus", "1")   # ACTIVE

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
      4. For the first <= max_pdfs_per_business filings, download PDFs,
         parse phone/email/executors, and store in a per-business folder.

    Returns:
      filings: full filing table list[dict]
      pdf_summaries: list[dict] with parsed PDF data for downloaded filings
    """
    filings: list = []
    pdf_summaries: list = []

    if not open_filing_history_tab(sb):
        return filings, pdf_summaries

    # Give you time to solve any Cloudflare / Turnstile on this tab
    print("[FILING] Waiting 10 seconds for Cloudflare / Filing History to load...")
    sb.sleep(10)

    # Capture Filing History HTML and parse table
    sb.wait_for_element_visible("table.table-striped", timeout=10)
    html_filing = sb.get_page_source()
    filings = parse_filing_history_table(html_filing)

    if not filings:
        return filings, pdf_summaries

    # Prepare folder:
    #   out_dir / 'pdf' / letter / keyword / 'page_{page_idx+1}' / 'bid_{business_id}'
    pdf_root = out_dir / "pdf" / letter / keyword / f"page_{page_idx+1}" / f"bid_{business_id}"
    pdf_root.mkdir(parents=True, exist_ok=True)

    # Download and parse up to max_pdfs_per_business
    for filing in filings[:max_pdfs_per_business]:
        filing_no = filing.get("filing_number")
        if not filing_no:
            continue

        pdf_path = pdf_root / f"{filing_no}.pdf"
        ok = download_pdf_for_filing(sb, filing_no, str(pdf_path))
        if not ok:
            continue

        parsed = parse_wa_filing_pdf(str(pdf_path))
        pdf_summaries.append(
            {
                "filing_number": filing_no,
                "filing_type": filing.get("filing_type"),
                "filing_date_time": filing.get("filing_date_time"),
                "effective_date": filing.get("effective_date"),
                "pdf_path": str(pdf_path),
                "phone": parsed.get("phone"),
                "email": parsed.get("email"),
                "executors": parsed.get("executors", []),
            }
        )

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
    # If no arg, default to 'A'
    if len(sys.argv) > 1:
        letter_arg = sys.argv[1]
    else:
        letter_arg = "A2"

    run_letter(letter_arg, out_dir="./output_wa_pdf4_1", headless=False)
