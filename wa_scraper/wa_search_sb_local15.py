#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import re
import sys
import os
from dotenv import load_dotenv
from pathlib import Path

from bs4 import BeautifulSoup
from seleniumbase import SB
from selenium.common.exceptions import NoAlertPresentException

load_dotenv()

ADV_URL = "https://ccfs.sos.wa.gov/#/AdvancedSearch"
API_DETAIL_URL = (
    "https://ccfs-api.prod.sos.wa.gov/api/BusinessSearch/BusinessInformation"
)

# Toggle to enable/disable detailed API fetching (Angular + BusinessInformation)
FETCH_API_DETAILS = True
FETCH_BUSINESS_INFORMATION = False # call BusinessInformation per businessID

# ------------ PROXY CONFIG (Webshare) ------------
USE_PROXY = False  # <-- set to True to actually use the Webshare proxy
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
    If we're on the results page, click Back.
    If that fails, reload ADV_URL.
    """
    try:
        # Already on Advanced Search
        if sb.is_element_present("#txtOrgname"):
            return True

        # On results page? Use Back button
        if sb.is_element_present("#btnReturnToSearch"):
            print("[NAV] Clicking Back to return to Advanced Search...")
            sb.click("#btnReturnToSearch")
            sb.wait_for_element("#txtOrgname", timeout=15)
            return True

        # Fallback: go directly to Advanced Search URL
        print("[NAV] Neither #txtOrgname nor Back found; re-opening AdvancedSearch URL...")
        sb.open(ADV_URL)
        sb.sleep(2)
        sb.wait_for_element("#txtOrgname", timeout=15)
        return True

    except Exception as e:
        print(f"[ERROR] ensure_advanced_search failed: {e}")
        return False


def get_business_list_via_angular(sb):
    """
    From the current Business Search results page, reach into AngularJS scope
    and pull out `businessList`, which already holds rich objects per business
    (BusinessID, UBINumber, BusinessName, AgentName, addresses, etc.).

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

def fetch_business_information_in_browser(sb, business_id: str):
    if not business_id:
        return {"ok": False, "status": None, "error": "no_business_id"}

    js = r"""
var businessID = arguments[0];
var callback = arguments[arguments.length - 1];

var url = "%s?businessID=" + encodeURIComponent(businessID);

fetch(url, {
    method: "GET",
    credentials: "include"
}).then(function(resp) {
    var status = resp.status;
    return resp.text().then(function(txt) {
        var baseResult = { ok: false, status: status, text: txt, error: null };
        // Non-200 -> keep raw text
        if (status !== 200) {
            callback(JSON.stringify(baseResult));
            return;
        }
        if (txt && txt.indexOf("System verification in progress") !== -1) {
            baseResult.text = "verification_page";
            callback(JSON.stringify(baseResult));
            return;
        }
        try {
            var data = JSON.parse(txt);
            callback(JSON.stringify({ ok: true, status: status, data: data }));
        } catch (e) {
            baseResult.error = String(e);
            baseResult.text = "json_parse_error";
            callback(JSON.stringify(baseResult));
        }
    });
}).catch(function(err) {
    callback(JSON.stringify({ ok: false, status: null, error: String(err) }));
});
""" % API_DETAIL_URL

    try:
        raw = sb.execute_async_script(js, str(business_id))
        if raw is None:
            # Avoid the "JSON object must be str, bytes or bytearray, not NoneType"
            return {"ok": False, "status": None, "error": "no_result_from_execute_async"}
        return json.loads(raw)
    except Exception as e:
        print(f"[INFO-API] JS fetch error for businessID={business_id}: {e}")
        return {"ok": False, "status": None, "error": str(e)}




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
            "api_details_file": None,
            "api_success": 0,
            "api_failed": 0,
            "details_success": 0,
            "details_failed": 0,
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
                    "api_details_file": None,
                    "api_success": 0,
                    "api_failed": 0,
                    "details_success": 0,
                    "details_failed": 0,
                }
            wait_s = 10 * attempt
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
            "api_details_file": None,
            "api_success": 0,
            "api_failed": 0,
            "details_success": 0,
            "details_failed": 0,
        }

    # --- ONE-TIME MANUAL STEP (only for first keyword) ---
    if first_keyword:
        print("\n[INFO] If Cloudflare challenge appears, solve it in the browser.")
        print("[INFO] Wait until you see the **results table** and 'Page 1 of X' at the bottom.")
        print("[INFO] Do NOT click any page numbers yourself.\n")
        input("When the first results page for this run is fully visible, press ENTER here... ")
    else:
        # For subsequent keywords, just wait for the table (best-effort)
        try:
            sb.wait_for_element('css=table.table-striped', timeout=30)
        except Exception:
            print("[WARN] table.table-striped not found after waiting; continuing anyway.")

    # We’ll track stats per keyword
    keyword_rows = []
    pages_info = []
    page_index = 1

    # For API-like details per keyword (from Angular businessList + BusinessInformation)
    api_records = []
    api_success = 0   # count of businesses for which Angular data is captured
    api_failed = 0    # Angular failures
    details_success = 0  # BusinessInformation success
    details_failed = 0   # BusinessInformation failures

    # Paths
    safe_kw = sanitize_for_filename(keyword)
    api_dir = out_dir / "api"
    api_dir.mkdir(parents=True, exist_ok=True)

    while True:
        sb.sleep(2)
        html = sb.get_page_source()

        # --- Save full HTML for this page for debugging ---
        debug_path = out_dir / f"debug_{letter}_{safe_kw}_page_{page_index}.html"
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
                debug_retry_path = out_dir / f"debug_{letter}_{safe_kw}_page_{page_index}_retry_{retry}.html"
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
        if FETCH_API_DETAILS:
            business_list = get_business_list_via_angular(sb)
            if not business_list:
                print("[API] No Angular businessList found on this page.")
            else:
                print(f"[API] Angular businessList has {len(business_list)} entries on this page.")
                for biz in business_list:
                    biz_id = biz.get("BusinessID") or biz.get("ID")
                    record = {
                        "businessID": biz_id,
                        "UBINumber": biz.get("UBINumber"),
                        "BusinessName": biz.get("BusinessName") or biz.get("EntityName"),
                        "Status": biz.get("Status") or biz.get("BusinessStatus"),
                        "Type": biz.get("Type") or biz.get("BusinessType"),
                        "AgentName": biz.get("AgentName"),
                        "CorrespondenceEmailAddress": biz.get("CorrespondenceEmailAddress"),
                        "angular": biz,  # raw Angular object
                    }

                    api_success += 1

                    # --- NEW: call BusinessInformation API via in-browser fetch ---
                    if FETCH_BUSINESS_INFORMATION and biz_id:
                        info_result = fetch_business_information_in_browser(sb, str(biz_id))

                        # Always keep the raw result, even on failures
                        record["BusinessInformation_result"] = info_result

                        status = info_result.get("status")
                        if info_result.get("ok") and info_result.get("data") is not None:
                            record["BusinessInformation"] = info_result["data"]
                            details_success += 1
                        else:
                            details_failed += 1
                            # Optional basic logging for debugging
                            print(f"[INFO-API] businessID={biz_id} status={status} "
                                f"ok={info_result.get('ok')} error={info_result.get('error')} "
                                f"text={info_result.get('text')}")

                    api_records.append(record)

        # --- Parse pager text (with retries if we catch the 0-of-0 placeholder) ---
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

    # --- Save API records for this keyword (if any) ---
    api_details_path = None
    if FETCH_API_DETAILS and api_records:
        # existing combined file
        api_details_path = api_dir / f"wa_api_{letter}_{safe_kw}.json"
        with api_details_path.open("w", encoding="utf-8") as f:
            json.dump(api_records, f, indent=2)

        # NEW: BusinessInformation-only file (for those that succeeded)
        bi_only = [
            {
                "businessID": rec["businessID"],
                "BusinessInformation": rec.get("BusinessInformation"),
            }
            for rec in api_records
            if "BusinessInformation" in rec
        ]
        bi_path = api_dir / f"wa_businessinfo_{letter}_{safe_kw}.json"
        with bi_path.open("w", encoding="utf-8") as f:
            json.dump(bi_only, f, indent=2)

        print(
            f"[API] Saved {len(api_records)} Angular+BusinessInformation records "
            f"for keyword '{keyword}' to {api_details_path}"
        )
        print(
            f"[API] Saved {len(bi_only)} pure BusinessInformation records "
            f"for keyword '{keyword}' to {bi_path}"
        )


    # Build result structure for this keyword
    keyword_result = {
        "keyword": keyword,
        "pages_visited": page_index,
        "records_scraped": len(keyword_rows),
        "pages": pages_info,
        "rows": keyword_rows,
        "api_details_file": str(api_details_path) if api_details_path else None,
        "api_success": api_success,
        "api_failed": api_failed,
        "details_success": details_success,
        "details_failed": details_failed,
    }

    print(
        f"[SUMMARY] Keyword '{keyword}': pages_visited={page_index}, "
        f"records_scraped={len(keyword_rows)}, "
        f"api_success={api_success}, api_failed={api_failed}, "
        f"details_success={details_success}, details_failed={details_failed}"
    )

    return keyword_result


def run_letter(letter="A", out_dir="./output_wa2", headless=False):
    """
    Main entry to scrape all keywords for a given letter.
    Loads keywords from search_keywords/<letter>.txt,
    scrapes each one in a single browser session, and saves
    a JSON per letter with stats + rows, plus API tracking.
    """
    letter = str(letter).upper()
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    keywords = load_keywords(letter)
    if not keywords:
        print("[ERROR] No keywords to scrape; exiting.")
        return

    all_keywords_results = []
    api_tracking = []

    with SB(
        uc=True,
        headless=headless,
        proxy=PROXY_URL if USE_PROXY else None,
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

                api_tracking.append(
                    {
                        "keyword": keyword,
                        "api_details_file": result.get("api_details_file"),
                        "api_success": result.get("api_success", 0),
                        "api_failed": result.get("api_failed", 0),
                        "details_success": result.get("details_success", 0),
                        "details_failed": result.get("details_failed", 0),
                        "records_scraped": result.get("records_scraped", 0),
                    }
                )

            except Exception as e:
                print(f"[ERROR] Exception while scraping keyword '{keyword}': {e}")
                api_tracking.append(
                    {
                        "keyword": keyword,
                        "api_details_file": None,
                        "api_success": 0,
                        "api_failed": 0,
                        "details_success": 0,
                        "details_failed": 0,
                        "records_scraped": 0,
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

    # Save API tracking JSON per letter
    tracking_path = out_dir / f"wa_api_tracking_{letter}.json"
    with tracking_path.open("w", encoding="utf-8") as f:
        json.dump(api_tracking, f, indent=2)

    total_records = sum(k["records_scraped"] for k in all_keywords_results)
    print(
        f"\n[DONE] Letter {letter}: scraped {len(all_keywords_results)} keywords, "
        f"total {total_records} records."
    )
    print(f"[SAVED] {results_path}")
    if FETCH_API_DETAILS:
        print(f"[SAVED] {tracking_path}")

    return letter_result


if __name__ == "__main__":
    # Usage:
    #   python3 wa_search_sb_local_BI_inbrowser.py A
    # If no arg, default to 'A'
    if len(sys.argv) > 1:
        letter_arg = sys.argv[1]
    else:
        letter_arg = "A"

    run_letter(letter_arg, out_dir="./output_wa3", headless=False)
