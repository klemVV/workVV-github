#!/usr/bin/env python3
"""
Rebuild wa_results_<LETTER>.json files from existing per-keyword outputs
under output_wa_pdf_proxy/api and output_wa_pdf_proxy/bi_html.

This is helpful if the main scraper finished but wa_results files are empty.
"""

import json
from collections import defaultdict
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
OUT_DIR = BASE_DIR / "output_wa_pdf_proxy"
API_DIR = OUT_DIR / "api"
BI_DIR = OUT_DIR / "bi_html"


def parse_name_parts(path: Path, prefix: str):
    """
    Expect filenames like wa_api_<LETTER>_<KW>.json or wa_bi_<LETTER>_<KW>.json.
    Returns (letter, safe_kw) or (None, None) on mismatch.
    """
    stem = path.stem  # e.g., wa_api_9_918
    parts = stem.split("_", 3)
    if len(parts) < 4 or parts[0] != "wa" or parts[1] != prefix:
        return None, None
    return parts[2], parts[3]


def collect_api_data():
    """
    Returns dict keyed by (letter, kw) with:
      {
        "api_file": <path>,
        "api_records": <int>,
        "keyword": <kw>,
        "letter": <letter>,
        "pages_visited": <int>,
      }
    """
    data = {}
    for path in API_DIR.glob("wa_api_*.json"):
        letter, safe_kw = parse_name_parts(path, "api")
        if not letter:
            continue
        try:
            payload = json.loads(path.read_text())
        except Exception:
            continue

        pages = payload.get("pages", [])
        api_records = 0
        for page in pages:
            api_records += len(page.get("business_list", []) or [])

        keyword = str(payload.get("keyword", safe_kw))
        pages_visited = len(pages) if isinstance(pages, list) else 0

        key = (letter, keyword)
        data[key] = {
            "api_file": str(path),
            "api_records": api_records,
            "keyword": keyword,
            "letter": letter,
            "pages_visited": pages_visited,
            "safe_kw": safe_kw,
        }
    return data


def collect_bi_data():
    """
    Returns dict keyed by (letter, kw) with:
      {
        "details_file": <path>,
        "details_success": <int>,
        "pdf_success": <int>,
      }
    """
    data = {}
    for path in BI_DIR.glob("wa_bi_*.json"):
        letter, safe_kw = parse_name_parts(path, "bi")
        if not letter:
            continue
        try:
            records = json.loads(path.read_text())
        except Exception:
            continue

        details_success = len(records) if isinstance(records, list) else 0
        pdf_success = 0
        if isinstance(records, list):
            for rec in records:
                pdfs = rec.get("PDFSummaries") or []
                if pdfs:
                    pdf_success += 1

        key = (letter, safe_kw)
        data[key] = {
            "details_file": str(path),
            "details_success": details_success,
            "pdf_success": pdf_success,
        }
    return data


def build_results():
    api_info = collect_api_data()
    bi_info = collect_bi_data()

    combined_keys = set(api_info.keys())
    for letter, safe_kw in bi_info.keys():
        combined_keys.add((letter, safe_kw))

    per_letter = defaultdict(list)

    for letter, kw in sorted(combined_keys):
        api = api_info.get((letter, kw))
        bi = bi_info.get((letter, kw))
        if not bi and api:
            bi = bi_info.get((letter, api.get("safe_kw")))

        api_records = api.get("api_records") if api else 0
        pages_visited = api.get("pages_visited") if api else 0
        details_success = bi.get("details_success") if bi else 0
        pdf_success = bi.get("pdf_success") if bi else 0
        pdf_fail = max(0, details_success - pdf_success)

        keyword = api.get("keyword") if api else kw

        result_entry = {
            "letter": letter,
            "keyword": keyword,
            "pages_visited": pages_visited,
            "records_scraped": api_records or details_success,
            "pages": [],
            "rows": [],
            "details_file": bi.get("details_file") if bi else None,
            "details_success": details_success,
            "details_failed": 0,
            "api_file": api.get("api_file") if api else None,
            "api_records": api_records,
            "pdf_success": pdf_success,
            "pdf_fail": pdf_fail,
            "duration_sec": None,
        }

        per_letter[letter].append(result_entry)

    return per_letter


def write_results(per_letter):
    for letter, entries in per_letter.items():
        out_path = OUT_DIR / f"wa_results_tracking_{letter}.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"letter": letter, "keywords": entries}
        out_path.write_text(json.dumps(payload, indent=2))
        print(f"[BACKFILL-RESULTS] Wrote {len(entries)} keyword entries to {out_path}")


if __name__ == "__main__":
    if not OUT_DIR.exists():
        raise SystemExit(f"Output dir not found: {OUT_DIR}")
    per_letter = build_results()
    if not per_letter:
        print("No API/BI files found; nothing to backfill.")
    else:
        write_results(per_letter)
