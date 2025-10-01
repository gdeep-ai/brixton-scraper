# FILE: brixtonjobs.py
# Single responsibility: fetch jobs (JSON first, HTML fallback) + optional detail scraping.

from __future__ import annotations
from typing import List, Dict, Any, Optional
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# -------------------------------
# Clean job descriptions
# -------------------------------
def clean_description(text: str) -> str:
    if not text:
        return ""

    # Step 1: Remove the repeated junk phrases if they appear
    junk_snippets = [
        "Take me back Apply to Job Share this Opportunity Apply to Job",
        "First Name Last Name Your Email Address Job Resume",
        "You agree to receive calls, AI-generated calls, text messages",
        "Message and data rates may apply",
        "You can access our privacy policy",
    ]
    for junk in junk_snippets:
        text = text.replace(junk, " ")

    # Step 2: If we find Responsibilities/Requirements/Qualifications, anchor there
    anchors = ["Responsibilities", "Requirements", "Qualifications"]
    for a in anchors:
        idx = text.lower().find(a.lower())
        if idx != -1:
            text = text[idx:]
            break

    # Step 3: Normalize whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text

# -------------------------------
# API & scraping setup
# -------------------------------
used_fallback: bool = False

BASE = "https://www.brixton.net/"
LISTING = urljoin(BASE, "job-listing/")
API = "https://brixton.net/data.brixton.net/public/api/jobs/fetch"

HEADERS = {
    "accept": "application/json, text/javascript, */*; q=0.01",
    "accept-language": "en-US,en;q=0.9",
    "origin": BASE.rstrip("/"),
    "referer": BASE,
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
}

BASE_PARAMS = {
    "draw": 1,
    "columns[0][data]": "action", "columns[0][searchable]": "false", "columns[0][orderable]": "false",
    "columns[1][data]": "title", "columns[1][searchable]": "true", "columns[1][orderable]": "true",
    "columns[2][data]": "position_type", "columns[2][searchable]": "true", "columns[2][orderable]": "true",
    "columns[3][data]": "work_model", "columns[3][searchable]": "true", "columns[3][orderable]": "true",
    "columns[4][data]": "city", "columns[4][searchable]": "true", "columns[4][orderable]": "true",
    "columns[5][data]": "state", "columns[5][searchable]": "true", "columns[5][orderable]": "true",
    "order[0][column]": 1, "order[0][dir]": "asc",
    "search[value]": "", "search[regex]": "false",
}


# -------------------------------
# Helpers
# -------------------------------
def _extract_href_from_action(action_html: str) -> Optional[str]:
    if not action_html:
        return None
    try:
        a = BeautifulSoup(action_html, "lxml").find("a")
        if a and a.get("href"):
            return urljoin(BASE, a["href"])
    except Exception:
        pass
    return None


def _fetch_json_page(start: int, length: int) -> Dict[str, Any]:
    params = dict(BASE_PARAMS)
    params.update({"start": start, "length": length})
    resp = requests.get(API, headers=HEADERS, params=params, timeout=30)
    return resp.json()


def _scrape_html_listing() -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    html = requests.get(LISTING, headers=HEADERS, timeout=30).text
    soup = BeautifulSoup(html, "lxml")
    for a in soup.select('a[href*="job-detail"]'):
        tr = a.find_parent("tr")
        if not tr:
            continue
        tds = [td.get_text(strip=True) for td in tr.find_all("td")]
        def _grab(i): return tds[i] if i < len(tds) else ""
        row = {
            "action": str(a),
            "title": _grab(0),
            "position_type": _grab(1),
            "work_model": _grab(2),
            "city": _grab(3),
            "state": _grab(4),
        }
        out.append(row)
    return out


def _fetch_detail_text(url: str) -> str:
    try:
        html = requests.get(url, headers=HEADERS, timeout=30).text
        soup = BeautifulSoup(html, "lxml")
        sel = [
            ".job-detail-description", "#job-detail-description",
            ".job-description", ".job-detail", "article .entry-content",
            ".elementor-widget-container", "main",
        ]
        node = None
        for s in sel:
            node = soup.select_one(s)
            if node and node.get_text(strip=True):
                break
        if not node:
            candidates = soup.find_all(["p", "div"], string=True)
            node = max(candidates, key=lambda n: len(n.get_text(strip=True)), default=None)

        text = node.get_text(" ", strip=True) if node else ""
        text = re.sub(r"\s+", " ", text).strip()
        return text
    except Exception:
        return ""


# -------------------------------
# Main fetcher
# -------------------------------
def fetch_jobs(
    mode: str = "auto",
    max_pages: Optional[int] = None,
    page_size: int = 50,
    fetch_details: bool = True,
) -> pd.DataFrame:
    """
    Returns a DataFrame with columns:
    ['title','position_type','work_model','city','state','detail_href','description','desc_snippet']
    """
    global used_fallback
    used_fallback = False
    rows: List[Dict[str, Any]] = []

    # Try JSON API
    api_allowed = mode in ("auto", "json")
    if api_allowed:
        try:
            start, pages = 0, 0
            while True:
                data = _fetch_json_page(start=start, length=page_size)
                aa = data.get("aaData") or data.get("data") or []
                if not aa:
                    break
                rows.extend(aa)
                start += len(aa); pages += 1
                if max_pages and pages >= max_pages: break
                if len(aa) < page_size: break
        except Exception:
            rows = []

    # Fallback to HTML scrape
    if not rows:
        used_fallback = True
        rows = _scrape_html_listing()
    if not rows:
        return pd.DataFrame()

    # Normalize
    norm = []
    for r in rows:
        href = _extract_href_from_action(r.get("action", ""))
        rec = {
            "title": r.get("title", ""),
            "position_type": r.get("position_type", ""),
            "work_model": r.get("work_model", ""),
            "city": r.get("city", ""),
            "state": r.get("state", ""),
            "detail_href": href or "",
        }
        norm.append(rec)

    # Fetch + clean descriptions
    if fetch_details:
        for rec in norm:
            if rec["detail_href"]:
                raw_desc = _fetch_detail_text(rec["detail_href"])
                cleaned = clean_description(raw_desc)
                rec["description"] = cleaned
                rec["desc_snippet"] = (cleaned[:500] + "…") if len(cleaned) > 500 else cleaned
            else:
                rec["description"] = ""
                rec["desc_snippet"] = ""

    return pd.DataFrame(norm)