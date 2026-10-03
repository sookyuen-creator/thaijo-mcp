"""
TCI tier checking for ThaiJO journals.

ศูนย์ TCI ไม่มี API สาธารณะ (เว็บ tci-thailand.org เป็น React app) จึงใช้ 2 ชั้น:

1) รายชื่ออ้างอิง (reference list) — ไฟล์ CSV ที่อาจารย์ดูแลเอง
   - ค่าเริ่มต้น: tci_tiers.csv ที่มากับโค้ด
   - หรือ Google Sheet ที่ "เผยแพร่เป็น CSV" แล้วตั้ง env TCI_TIERS_URL (แก้ข้อมูลได้โดยไม่ต้อง deploy ใหม่)
   ความเชื่อมั่นขึ้นกับคอลัมน์ source: official_tci_list = high, อื่น ๆ = medium

2) ตรวจจากหน้าเว็บวารสารบน ThaiJO (fallback) — วารสารส่วนใหญ่ประกาศกลุ่ม TCI บนหน้าแรก
   ความเชื่อมั่น medium (มีปีรอบที่ 5 กำกับ) หรือ low (ไม่มีปีกำกับ อาจเป็นข้อมูลเก่า)
"""

from __future__ import annotations

import csv
import html
import io
import os
import re
import time
from collections import Counter
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional
from urllib.parse import urlparse

TIERS_URL = os.environ.get("TCI_TIERS_URL", "").strip()
LOCAL_CSV = Path(__file__).with_name("tci_tiers.csv")
LIST_TTL_SEC = 24 * 3600
DETECT_TTL_SEC = 7 * 24 * 3600

# Years that indicate the current evaluation round (round 5: 2568-2572 / 2025-2029)
CURRENT_ROUND_MARKERS = ("2568", "2569", "2570", "2571", "2572",
                         "2025", "2026", "2027", "2028", "2029", "รอบที่ 5", "round 5", "5th")

_list_cache: dict[str, Any] = {"at": 0.0, "rows": [], "source": ""}
_detect_cache: dict[str, tuple[float, dict[str, Any]]] = {}

FetchFn = Callable[[str], Awaitable[str]]


# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------

def norm_name(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[\s\.\,\-\(\)\[\]:;\"'“”]+", "", s)
    return s


def norm_issn(s: str) -> str:
    return re.sub(r"[^0-9x]", "", (s or "").lower())


def journal_key(host: str, jpath: str) -> str:
    return f"{host.lower()}/{jpath.lower()}"


def key_from_url(url: str) -> Optional[str]:
    u = urlparse(url or "")
    m = re.match(r"^([a-z]{2}\d{2})\.tci-thaijo\.org$", u.netloc)
    parts = [p for p in u.path.split("/") if p]
    if m and len(parts) >= 2 and parts[0] == "index.php" and parts[1] != "index":
        return journal_key(m.group(1), parts[1])
    return None


# ---------------------------------------------------------------------------
# Layer 1: reference list
# ---------------------------------------------------------------------------

def _parse_csv(text: str) -> list[dict[str, str]]:
    rows = []
    for r in csv.DictReader(io.StringIO(text)):
        r = {(k or "").strip(): (v or "").strip() for k, v in r.items()}
        if not r.get("tier"):
            continue
        r["_key"] = key_from_url(r.get("thaijo_url", "")) or ""
        r["_issns"] = {norm_issn(r.get("issn_print", "")), norm_issn(r.get("issn_online", ""))} - {""}
        r["_names"] = {norm_name(r.get("journal_name", "")), norm_name(r.get("journal_name_en", ""))} - {""}
        rows.append(r)
    return rows


async def load_list(fetch_text: Optional[FetchFn] = None, force: bool = False) -> tuple[list[dict[str, str]], str]:
    if not force and time.time() - _list_cache["at"] < LIST_TTL_SEC and _list_cache["rows"]:
        return _list_cache["rows"], _list_cache["source"]
    rows: list[dict[str, str]] = []
    source = ""
    if TIERS_URL and fetch_text:
        try:
            rows = _parse_csv(await fetch_text(TIERS_URL))
            source = "Google Sheet / TCI_TIERS_URL"
        except Exception:  # noqa: BLE001 - fall back to local file
            rows = []
    if not rows and LOCAL_CSV.exists():
        rows = _parse_csv(LOCAL_CSV.read_text(encoding="utf-8-sig"))
        source = "tci_tiers.csv (ไฟล์ในเซิร์ฟเวอร์)"
    _list_cache.update(at=time.time(), rows=rows, source=source)
    return rows, source


def lookup(rows: list[dict[str, str]], *, key: Optional[str] = None, issn: Optional[str] = None,
           name: Optional[str] = None) -> Optional[dict[str, str]]:
    if key:
        for r in rows:
            if r["_key"] == key:
                return r
    if issn:
        n = norm_issn(issn)
        for r in rows:
            if n and n in r["_issns"]:
                return r
    if name:
        n = norm_name(name)
        for r in rows:
            if n and n in r["_names"]:
                return r
        if len(n) >= 6:
            for r in rows:
                if any(n in rn or rn in n for rn in r["_names"] if len(rn) >= 6):
                    return r
    return None


def row_to_result(r: dict[str, str]) -> dict[str, Any]:
    src = r.get("source", "")
    return {
        "tier": int(r["tier"]) if r["tier"].isdigit() else r["tier"],
        "journal_name": r.get("journal_name") or r.get("journal_name_en", ""),
        "valid_period": "-".join(x for x in (r.get("valid_from", ""), r.get("valid_to", "")) if x),
        "round": r.get("round", ""),
        "method": "reference_list",
        "confidence": "high" if src.startswith("official") else "medium",
        "evidence": f"รายชื่ออ้างอิง (source: {src or 'ไม่ระบุ'}) {r.get('note', '')}".strip(),
        "thaijo_url": r.get("thaijo_url", ""),
    }


# ---------------------------------------------------------------------------
# Layer 2: detect from the journal's ThaiJO website
# ---------------------------------------------------------------------------

_TIER_RE = re.compile(r"(?:tier\s*[-:]?\s*([123])\b)|(?:กลุ่ม\s*(?:ที่)?\s*([123])(?![0-9]))", re.I)
_ISSN_RE = re.compile(r"\b(\d{4}-\d{3}[\dXx])\b")
_YEAR_RANGE_RE = re.compile(r"^(19|20|25)\d\d-(19|20|25)\d\d$")


def html_to_text(raw: str) -> str:
    raw = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", raw)
    # keep alt/title text of badge images such as "TCI Tier 1"
    raw = re.sub(r'(?is)<img[^>]*?(?:alt|title)\s*=\s*"([^"]*)"[^>]*>', r" \1 ", raw)
    raw = re.sub(r"(?s)<[^>]+>", " ", raw)
    return re.sub(r"\s+", " ", html.unescape(raw))


def detect_tier_in_text(text: str) -> dict[str, Any]:
    current: list[int] = []
    any_round: list[int] = []
    snippets: list[str] = []
    for m in _TIER_RE.finditer(text):
        tier = int(m.group(1) or m.group(2))
        lo, hi = max(0, m.start() - 160), min(len(text), m.end() + 160)
        window = text[lo:hi]
        wl = window.lower()
        if "tci" not in wl and "ศูนย์ดัชนี" not in window and "citation index" not in wl:
            continue
        any_round.append(tier)
        if any(y in wl for y in CURRENT_ROUND_MARKERS):
            current.append(tier)
            snip = "…" + window.strip() + "…"
            if len(snippets) < 2 and not any(snip[20:80] in x for x in snippets):
                snippets.append(snip)
        elif not snippets:
            snippets.append("…" + window.strip() + "…")
    issns = sorted({i for i in _ISSN_RE.findall(text) if not _YEAR_RANGE_RE.match(i)})
    if current:
        tier = Counter(current).most_common(1)[0][0]
        conf = "medium"
    elif any_round:
        tier = Counter(any_round).most_common(1)[0][0]
        conf = "low"
    else:
        return {"tier": None, "confidence": "none", "evidence": "", "issns": issns}
    return {"tier": tier, "confidence": conf, "evidence": " | ".join(snippets)[:700], "issns": issns}


async def detect_from_website(host: str, jpath: str, fetch_text: FetchFn) -> dict[str, Any]:
    key = journal_key(host, jpath)
    hit = _detect_cache.get(key)
    if hit and time.time() - hit[0] < DETECT_TTL_SEC:
        return hit[1]
    base = f"https://{host}.tci-thaijo.org/index.php/{jpath}"
    result: dict[str, Any] = {"tier": None, "confidence": "none", "evidence": "", "issns": []}
    for path in ("/index", "/about"):
        try:
            text = html_to_text(await fetch_text(base + path))
        except Exception:  # noqa: BLE001
            continue
        r = detect_tier_in_text(text)
        result["issns"] = sorted(set(result["issns"]) | set(r["issns"]))
        if r["tier"] is not None and (result["tier"] is None or
                                      (r["confidence"] == "medium" and result["confidence"] == "low")):
            result.update(tier=r["tier"], confidence=r["confidence"], evidence=r["evidence"])
        if result["confidence"] == "medium":
            break
    result.update(method="journal_website", thaijo_url=base, valid_period="", round="",
                  journal_name="")
    _detect_cache[key] = (time.time(), result)
    return result


# ---------------------------------------------------------------------------
# Combined
# ---------------------------------------------------------------------------

async def check_tier(*, fetch_text: FetchFn, host: Optional[str] = None, jpath: Optional[str] = None,
                     issn: Optional[str] = None, name: Optional[str] = None,
                     allow_web: bool = True) -> dict[str, Any]:
    rows, _ = await load_list(fetch_text)
    key = journal_key(host, jpath) if host and jpath else None
    row = lookup(rows, key=key, issn=issn, name=name)
    if row:
        return row_to_result(row)
    if host and jpath and allow_web:
        det = await detect_from_website(host, jpath, fetch_text)
        # second chance: match the reference list by ISSN found on the website
        for i in det.get("issns", []):
            row = lookup(rows, issn=i)
            if row:
                return row_to_result(row)
        return det
    return {"tier": None, "confidence": "none", "method": "not_found",
            "evidence": "ไม่พบในรายชื่ออ้างอิง และไม่มี URL วารสารให้ตรวจจากหน้าเว็บ"}


def is_cached_or_listed(host: str, jpath: str) -> bool:
    key = journal_key(host, jpath)
    if key in _detect_cache and time.time() - _detect_cache[key][0] < DETECT_TTL_SEC:
        return True
    return any(r["_key"] == key for r in _list_cache.get("rows", []))
