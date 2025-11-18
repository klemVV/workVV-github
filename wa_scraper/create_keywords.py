#!/usr/bin/env python3
"""
Generate 3-character alphanumeric search keywords for WA SOS scraper.
Each keyword: [0-9A-Z][0-9A-Z][0-9A-Z]
Output: grouped by first character (0–9, A–Z)
Example: 000.txt, A.txt, Z.txt, etc.
"""

import os
import string
from itertools import product
from pathlib import Path

# --- CONFIG ---
OUT_DIR = Path("/Users/klemanroy/Downloads/wa_scraper/search_keywords")
CHARS = string.digits + string.ascii_uppercase  # 0-9A-Z
OUT_DIR.mkdir(parents=True, exist_ok=True)

def main():
    total = 0
    for first_char in CHARS:
        out_path = OUT_DIR / f"{first_char}.txt"
        with out_path.open("w", encoding="utf-8") as f:
            for mid, last in product(CHARS, repeat=2):
                keyword = f"{first_char}{mid}{last}"
                f.write(keyword + "\n")
                total += 1
        print(f"Wrote {out_path.name}")

    print(f"✅ Done. Generated {total:,} total keywords into {len(CHARS)} files.")

if __name__ == "__main__":
    main()
