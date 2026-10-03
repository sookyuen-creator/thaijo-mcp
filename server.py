"""
ThaiJO / TCI MCP Server
=======================
MCP server ที่เชื่อม Claude เข้ากับบทความวิชาการไทยใน ThaiJO (ฐานบทความของวารสาร TCI)
ผ่าน OAI-PMH endpoint อย่างเป็นทางการ (https://www.tci-thaijo.org/public/oai.html)

ข้อจำกัดของ ThaiJO: 10 requests/นาที/IP -> เซิร์ฟเวอร์นี้คุมจังหวะให้อัตโนมัติ (1 ครั้ง / 6.5 วินาที)
และ cache ผลลัพธ์ไว้เพื่อลดการเรียกซ้ำ

Tools:
  - thaijo_list_servers   : รายชื่อเซิร์ฟเวอร์ ThaiJO แยกตามกลุ่มสาขา
  - thaijo_list_journals  : รายชื่อวารสารในเซิร์ฟเวอร์ (กรองชื่อได้)
  - thaijo_list_articles  : ดึงบทความของวารสาร (แบ่งหน้าด้วย resumption token)
  - thaijo_search_articles: ค้นบทความตามคำสำคัญในชื่อเรื่อง/คำสำคัญ/บทคัดย่อ
  - thaijo_get_article    : ดึงข้อมูลบทความเดียวแบบเต็ม
  - thaijo_check_tci_tier : ตรวจกลุ่มคุณภาพ TCI (กลุ่ม 1/2) ของวารสาร
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import time
import xml.etree.ElementTree as ET
from enum import Enum
from typing import Any, Optional
from urllib.parse import urlparse

import httpx
from pydantic import BaseModel, ConfigDict, Field, field_validator

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

import tci_tier

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SERVER_GROUPS: dict[str, dict[str, Any]] = {
    "sc": {"name_th": "วิทยาศาสตร์", "name_en": "Science", "hosts": ["sc01"]},
    "li": {"name_th": "วิทยาศาสตร์ชีวภาพ", "name_en": "Life Sciences",
           "hosts": [f"li{i:02d}" for i in range(1, 6)]},
    "ph": {"name_th": "วิทยาศาสตร์กายภาพ", "name_en": "Physical Sciences",
           "hosts": [f"ph{i:02d}" for i in range(1, 6)]},
    "he": {"name_th": "วิทยาศาสตร์สุขภาพ", "name_en": "Health Sciences",
           "hosts": [f"he{i:02d}" for i in range(1, 6)]},
    "so": {"name_th": "สังคมศาสตร์และมนุษยศาสตร์", "name_en": "Social Sciences",
           "hosts": [f"so{i:02d}" for i in range(1, 21)]},
}
ALL_HOSTS = {h for g in SERVER_GROUPS.values() for h in g["hosts"]}

NS = {
    "oai": "http://www.openarchives.org/OAI/2.0/",
    "oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/",
    "dc": "http://purl.org/dc/elements/1.1/",
}
XML_LANG = "{http://www.w3.org/XML/1998/namespace}lang"

MIN_INTERVAL_SEC = float(os.environ.get("THAIJO_MIN_INTERVAL", "6.5"))  # <= 10 req/min
CACHE_TTL_SEC = int(os.environ.get("THAIJO_CACHE_TTL", str(6 * 3600)))
REQUEST_TIMEOUT = 45.0
ABSTRACT_PREVIEW_CHARS = 600
USER_AGENT = "ThaiJO-MCP/1.0 (academic research assistant; respects 10 req/min)"

# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------

mcp = FastMCP(
    "thaijo_mcp",
    instructions=(
        "เครื่องมือค้นบทความวิชาการไทยจาก ThaiJO (วารสารในฐาน TCI). "
        "ขั้นตอนแนะนำ: ถ้ารู้ URL วารสารให้ส่ง journal_url ได้เลย; ถ้าไม่รู้ ให้ใช้ "
        "thaijo_list_journals ในเซิร์ฟเวอร์ของสาขานั้นเพื่อหา journal path ก่อน "
        "แล้วใช้ thaijo_search_articles หรือ thaijo_list_articles. "
        "ThaiJO จำกัด 10 ครั้ง/นาที การค้นหลายหน้าจึงใช้เวลา ควรระบุวารสารเสมอ. "
        "ใช้ thaijo_check_tci_tier ตรวจกลุ่ม TCI ของวารสาร หรือส่ง tci_tiers=[1] ใน thaijo_search_articles "
        "เพื่อกรองเฉพาะวารสารกลุ่ม 1. แจ้งระดับความเชื่อมั่นของกลุ่ม TCI ให้ผู้ใช้ทราบเสมอ."
    ),
    host=os.environ.get("HOST", "0.0.0.0"),
    port=int(os.environ.get("PORT", "8000")),
    stateless_http=True,
    json_response=True,
    transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
)

# ---------------------------------------------------------------------------
# Shared infrastructure: rate limiter, cache, HTTP client, XML parsing
# ---------------------------------------------------------------------------

_rate_lock = asyncio.Lock()
_last_request_at = 0.0
_cache: dict[str, tuple[float, str]] = {}


class OAIError(Exception):
    """OAI-PMH protocol error returned by the repository."""

    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


async def _polite_get(url: str, params: Optional[dict[str, str]] = None) -> httpx.Response:
    """GET with a global rate limit shared by every call to ThaiJO (<= 10 req/min)."""
    global _last_request_at
    rate_limited = "tci-thaijo.org" in url
    if not rate_limited:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT, headers={"User-Agent": USER_AGENT},
                                     follow_redirects=True) as client:
            return await client.get(url, params=params)
    async with _rate_lock:
        wait = MIN_INTERVAL_SEC - (time.monotonic() - _last_request_at)
        if wait > 0:
            await asyncio.sleep(wait)
        try:
            async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT, headers={"User-Agent": USER_AGENT},
                                         follow_redirects=True) as client:
                return await client.get(url, params=params)
        finally:
            _last_request_at = time.monotonic()


async def _fetch_text(url: str) -> str:
    """Fetch a page/CSV as text (used by the TCI tier module)."""
    resp = await _polite_get(url)
    resp.raise_for_status()
    return resp.text


def _base_url(host: str) -> str:
    return f"https://{host}.tci-thaijo.org/index.php/index/oai"


async def _oai_request(host: str, params: dict[str, str]) -> ET.Element:
    """Call an OAI-PMH endpoint with global rate limiting + caching. Returns the root element."""
    key = host + "?" + "&".join(f"{k}={v}" for k, v in sorted(params.items()))

    cached = _cache.get(key)
    if cached and time.time() - cached[0] < CACHE_TTL_SEC:
        return _parse_root(cached[1])

    resp = await _polite_get(_base_url(host), params)
    if resp.status_code == 429:
        raise RuntimeError(
            "ThaiJO แจ้งว่าเรียกถี่เกินไป (HTTP 429) — รอ 1-2 นาทีแล้วลองใหม่ "
            "หรือลดจำนวนหน้าที่สแกน (max_pages)"
        )
    resp.raise_for_status()
    text = resp.text
    root = _parse_root(text)  # validates & raises OAIError if any

    # cache only successful responses
    if len(_cache) > 2000:
        _cache.clear()
    _cache[key] = (time.time(), text)
    return root


def _parse_root(text: str) -> ET.Element:
    try:
        root = ET.fromstring(text.encode("utf-8"))
    except ET.ParseError as e:
        raise RuntimeError(f"อ่าน XML จาก ThaiJO ไม่ได้: {e}") from e
    err = root.find("oai:error", NS)
    if err is not None:
        raise OAIError(err.get("code", "unknown"), (err.text or "").strip())
    return root


def _texts(md: ET.Element, tag: str) -> list[dict[str, str]]:
    out = []
    for el in md.findall(f"dc:{tag}", NS):
        t = (el.text or "").strip()
        if t:
            out.append({"text": t, "lang": el.get(XML_LANG, "")})
    return out


def _pick(values: list[dict[str, str]], prefer: str) -> str:
    """Pick text in preferred language prefix (th/en), else first."""
    for v in values:
        if v["lang"].lower().startswith(prefer):
            return v["text"]
    return values[0]["text"] if values else ""


def _parse_record(rec: ET.Element) -> Optional[dict[str, Any]]:
    header = rec.find("oai:header", NS)
    if header is None or header.get("status") == "deleted":
        return None
    identifier = (header.findtext("oai:identifier", default="", namespaces=NS) or "").strip()
    datestamp = (header.findtext("oai:datestamp", default="", namespaces=NS) or "").strip()
    set_specs = [s.text for s in header.findall("oai:setSpec", NS) if s.text]

    md = rec.find("oai:metadata/oai_dc:dc", NS)
    if md is None:
        return None

    titles = _texts(md, "title")
    abstracts = _texts(md, "description")
    subjects = [s["text"] for s in _texts(md, "subject")]
    keywords: list[str] = []
    for s in subjects:  # OJS often stores keywords as "a; b; c"
        keywords.extend(k.strip() for k in re.split(r"[;,]", s) if k.strip())

    identifiers = [i["text"] for i in _texts(md, "identifier")]
    url = next((i for i in identifiers if "/article/view/" in i), "")
    if not url:
        url = next((i for i in identifiers if i.startswith("http")), "")
    doi = next((i for i in identifiers if i.startswith("10.") or "doi.org/" in i), "")
    relations = [r["text"] for r in _texts(md, "relation")]
    pdf = next((r for r in relations if "/article/view/" in r or "/download/" in r), "")

    dates = [d["text"] for d in _texts(md, "date")]
    year = next((d[:4] for d in dates if re.match(r"^\d{4}", d)), "")

    return {
        "oai_identifier": identifier,
        "journal_set": set_specs[0].split(":")[0] if set_specs else "",
        "title_th": _pick(titles, "th") if any(t["lang"].startswith("th") for t in titles) else "",
        "title_en": _pick(titles, "en") if any(t["lang"].startswith("en") for t in titles) else "",
        "title": _pick(titles, "th"),
        "authors": [c["text"] for c in _texts(md, "creator")],
        "keywords": keywords,
        "abstract_th": _pick(abstracts, "th") if any(a["lang"].startswith("th") for a in abstracts) else "",
        "abstract_en": _pick(abstracts, "en") if any(a["lang"].startswith("en") for a in abstracts) else "",
        "abstract": _pick(abstracts, "th"),
        "publisher": _pick(_texts(md, "publisher"), "th"),
        "source": _pick(_texts(md, "source"), "th"),
        "year": year,
        "date": dates[0] if dates else "",
        "language": [l["text"] for l in _texts(md, "language")],
        "type": [t["text"] for t in _texts(md, "type")],
        "url": url,
        "pdf_link": pdf,
        "doi": doi,
        "datestamp": datestamp,
    }


def _records_and_token(root: ET.Element) -> tuple[list[dict[str, Any]], Optional[str], Optional[int]]:
    lr = root.find("oai:ListRecords", NS)
    if lr is None:
        return [], None, None
    records = [r for r in (_parse_record(x) for x in lr.findall("oai:record", NS)) if r]
    tok_el = lr.find("oai:resumptionToken", NS)
    token = (tok_el.text or "").strip() if tok_el is not None else ""
    total = None
    if tok_el is not None and tok_el.get("completeListSize", "").isdigit():
        total = int(tok_el.get("completeListSize"))
    return records, (token or None), total


def _resolve_host_and_journal(server: Optional[str], journal: Optional[str],
                              journal_url: Optional[str]) -> tuple[str, Optional[str]]:
    """Accept either server+journal path or a ThaiJO journal/article URL."""
    if journal_url:
        u = urlparse(journal_url)
        m = re.match(r"^([a-z]{2}\d{2})\.tci-thaijo\.org$", u.netloc)
        if not m:
            raise ValueError(
                "journal_url ต้องเป็น URL ของ ThaiJO เช่น "
                "https://so03.tci-thaijo.org/index.php/liberalarts"
            )
        parts = [p for p in u.path.split("/") if p]
        jpath = parts[1] if len(parts) >= 2 and parts[0] == "index.php" else None
        if jpath in (None, "index"):
            jpath = None
        return m.group(1), jpath
    if not server:
        raise ValueError("ต้องระบุ server (เช่น 'so03') หรือ journal_url อย่างใดอย่างหนึ่ง")
    return server, journal


def _fmt_article_md(a: dict[str, Any], full: bool = False) -> str:
    lines = [f"### {a['title_th'] or a['title']}"]
    if a["title_en"] and a["title_en"] != (a["title_th"] or a["title"]):
        lines.append(f"*{a['title_en']}*")
    meta = []
    if a["authors"]:
        meta.append("ผู้แต่ง: " + ", ".join(a["authors"]))
    if a["year"]:
        meta.append(f"ปี: {a['year']}")
    if a["source"]:
        meta.append(f"แหล่ง: {a['source']}")
    lines.append(" | ".join(meta))
    if a["keywords"]:
        lines.append("คำสำคัญ: " + "; ".join(a["keywords"][:12]))
    abstract = a["abstract_th"] or a["abstract"]
    if abstract:
        if not full and len(abstract) > ABSTRACT_PREVIEW_CHARS:
            abstract = abstract[:ABSTRACT_PREVIEW_CHARS].rstrip() + "…"
        lines.append(f"บทคัดย่อ: {abstract}")
    if full and a["abstract_en"]:
        lines.append(f"Abstract: {a['abstract_en']}")
    links = []
    if a["url"]:
        links.append(f"URL: {a['url']}")
    if a["doi"]:
        links.append(f"DOI: {a['doi']}")
    links.append(f"OAI ID: {a['oai_identifier']}")
    lines.append(" | ".join(links))
    return "\n".join(lines)


def _error_text(e: Exception) -> str:
    if isinstance(e, OAIError):
        hints = {
            "noRecordsMatch": "ไม่พบบทความตามเงื่อนไข ลองขยายช่วงวันที่หรือตรวจชื่อวารสาร",
            "badResumptionToken": "resumption token หมดอายุแล้ว ให้เริ่มดึงใหม่ตั้งแต่หน้าแรก",
            "badArgument": "พารามิเตอร์ไม่ถูกต้อง ตรวจรูปแบบวันที่ (YYYY-MM-DD) และ journal path",
            "idDoesNotExist": "ไม่พบ OAI identifier นี้ ตรวจว่าใช้ server ถูกตัวหรือไม่",
            "noSetHierarchy": "เซิร์ฟเวอร์นี้ไม่รองรับการแยกชุดวารสาร",
        }
        return f"ข้อผิดพลาดจาก ThaiJO ({e.code}): {e.message}. {hints.get(e.code, '')}".strip()
    if isinstance(e, httpx.TimeoutException):
        return "ThaiJO ตอบช้าเกินกำหนด ลองใหม่อีกครั้ง หรือลด max_pages"
    if isinstance(e, httpx.HTTPStatusError):
        return f"ThaiJO ตอบกลับ HTTP {e.response.status_code} ลองใหม่ภายหลัง"
    return f"เกิดข้อผิดพลาด: {e}"


# ---------------------------------------------------------------------------
# Input models
# ---------------------------------------------------------------------------

class ResponseFormat(str, Enum):
    MARKDOWN = "markdown"
    JSON = "json"


_DATE_RE = r"^\d{4}-\d{2}-\d{2}$"


class _Base(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")


def _validate_server(v: Optional[str]) -> Optional[str]:
    if v is None:
        return v
    v = v.lower().strip()
    if v not in ALL_HOSTS:
        raise ValueError(f"ไม่รู้จักเซิร์ฟเวอร์ '{v}' — ใช้ thaijo_list_servers เพื่อดูรหัสที่ถูกต้อง")
    return v


class _JournalTarget(_Base):
    server: Optional[str] = Field(
        default=None,
        description="รหัสเซิร์ฟเวอร์ ThaiJO เช่น 'so03', 'he02' (ดูได้จาก thaijo_list_servers หรือ subdomain ของ URL วารสาร)",
    )
    journal: Optional[str] = Field(
        default=None,
        description="journal path ของวารสาร เช่น 'liberalarts', 'tjph' (ส่วนหลัง /index.php/ ใน URL)",
        max_length=100,
    )
    journal_url: Optional[str] = Field(
        default=None,
        description="หรือวาง URL วารสาร/บทความใน ThaiJO แทน server+journal เช่น https://so03.tci-thaijo.org/index.php/liberalarts",
    )

    @field_validator("server")
    @classmethod
    def _check_server(cls, v: Optional[str]) -> Optional[str]:
        return _validate_server(v)


class ListJournalsInput(_Base):
    server: str = Field(..., description="รหัสเซิร์ฟเวอร์ เช่น 'so01'..'so20', 'he01'..'he05'")
    name_filter: Optional[str] = Field(
        default=None, description="กรองชื่อวารสาร (ไม่สนตัวพิมพ์) เช่น 'บริหาร', 'nursing'", max_length=100
    )
    response_format: ResponseFormat = Field(default=ResponseFormat.MARKDOWN)

    @field_validator("server")
    @classmethod
    def _check_server(cls, v: str) -> str:
        return _validate_server(v)  # type: ignore[return-value]


class ListArticlesInput(_JournalTarget):
    from_date: Optional[str] = Field(
        default=None, description="วันที่เริ่ม (YYYY-MM-DD) อิงวันที่บันทึก/แก้ไขเมทาดาทาในระบบ", pattern=_DATE_RE
    )
    until_date: Optional[str] = Field(default=None, description="วันที่สิ้นสุด (YYYY-MM-DD)", pattern=_DATE_RE)
    resumption_token: Optional[str] = Field(
        default=None, description="token สำหรับดึงหน้าถัดไป (ได้จากผลลัพธ์ครั้งก่อน) — ถ้าใช้ จะไม่ต้องส่งพารามิเตอร์อื่น"
    )
    response_format: ResponseFormat = Field(default=ResponseFormat.MARKDOWN)


class MatchMode(str, Enum):
    ANY = "any"
    ALL = "all"


class SearchArticlesInput(_JournalTarget):
    keywords: list[str] = Field(
        ..., description="คำค้น เช่น ['ภาวะผู้นำ', 'ความผูกพันต่อองค์กร'] หรือ ['TikTok','purchase intention']",
        min_length=1, max_length=10,
    )
    match: MatchMode = Field(default=MatchMode.ANY, description="'any' = มีคำใดคำหนึ่ง, 'all' = ต้องมีครบทุกคำ")
    year_from: Optional[int] = Field(default=None, description="ปีพิมพ์ ค.ศ. ตั้งแต่ (เช่น 2020)", ge=1950, le=2100)
    year_to: Optional[int] = Field(default=None, description="ปีพิมพ์ ค.ศ. ถึง", ge=1950, le=2100)
    max_pages: int = Field(
        default=3, description="จำนวนหน้าที่สแกน (หน้าละ ~100 บทความ, ~7 วินาที/หน้า) สูงสุด 8", ge=1, le=8
    )
    resumption_token: Optional[str] = Field(default=None, description="token เพื่อสแกนต่อจากครั้งก่อน")
    max_results: int = Field(default=20, description="จำนวนผลลัพธ์สูงสุดที่ส่งกลับ", ge=1, le=100)
    tci_tiers: Optional[list[int]] = Field(
        default=None,
        description="กรองเฉพาะวารสาร TCI กลุ่มที่ระบุ เช่น [1] = เฉพาะกลุ่ม 1, [1,2] = กลุ่ม 1 และ 2. "
                    "ถ้าไม่ระบุจะไม่กรอง แต่ยังแสดงกลุ่มให้เมื่อทราบ",
    )
    response_format: ResponseFormat = Field(default=ResponseFormat.MARKDOWN)

    @field_validator("tci_tiers")
    @classmethod
    def _check_tiers(cls, v: Optional[list[int]]) -> Optional[list[int]]:
        if v and any(t not in (1, 2, 3) for t in v):
            raise ValueError("tci_tiers รับเฉพาะค่า 1, 2 หรือ 3")
        return v


class CheckTierInput(_JournalTarget):
    journal_name: Optional[str] = Field(default=None, description="ชื่อวารสาร (ไทยหรืออังกฤษ) ใช้ค้นในรายชื่ออ้างอิง")
    issn: Optional[str] = Field(default=None, description="ISSN ของวารสาร เช่น '1513-9131'")
    refresh_list: bool = Field(default=False, description="โหลดรายชื่ออ้างอิงใหม่ทันที (หลังแก้ Google Sheet)")
    response_format: ResponseFormat = Field(default=ResponseFormat.MARKDOWN)


class GetArticleInput(_Base):
    oai_identifier: str = Field(
        ..., description="OAI identifier ของบทความ เช่น 'oai:ojs.so03.tci-thaijo.org:article/123456'", min_length=5
    )
    server: Optional[str] = Field(default=None, description="รหัสเซิร์ฟเวอร์ (ถ้าเว้นว่าง จะเดาจาก identifier)")
    response_format: ResponseFormat = Field(default=ResponseFormat.MARKDOWN)


_RO = {"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True}

# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

@mcp.tool(name="thaijo_list_servers", annotations={"title": "List ThaiJO servers", **_RO})
async def thaijo_list_servers() -> str:
    """แสดงรายชื่อเซิร์ฟเวอร์ ThaiJO ทั้งหมดแยกตามกลุ่มสาขา (ไม่เรียกเครือข่าย).

    ใช้เพื่อเลือก server ก่อนเรียก thaijo_list_journals. วารสารด้านบริหาร การศึกษา รัฐศาสตร์
    มนุษยศาสตร์ อยู่ในกลุ่ม 'so' (so01-so20); พยาบาล/สาธารณสุข/การแพทย์อยู่กลุ่ม 'he'.
    """
    lines = ["## เซิร์ฟเวอร์ ThaiJO", ""]
    for code, g in SERVER_GROUPS.items():
        lines.append(f"- **{code.upper()} – {g['name_th']} ({g['name_en']})**: {', '.join(g['hosts'])}")
    lines.append("")
    lines.append("เคล็ดลับ: subdomain ใน URL วารสาร (เช่น so03.tci-thaijo.org) คือรหัสเซิร์ฟเวอร์ของวารสารนั้น")
    return "\n".join(lines)


@mcp.tool(name="thaijo_list_journals", annotations={"title": "List journals on a ThaiJO server", **_RO})
async def thaijo_list_journals(params: ListJournalsInput) -> str:
    """แสดงวารสารทั้งหมดในเซิร์ฟเวอร์ ThaiJO หนึ่งตัว พร้อม journal path สำหรับใช้กับ tool อื่น.

    Returns: รายชื่อวารสาร (ชื่อ + journal path) กรองด้วย name_filter ได้
    """
    try:
        journals: dict[str, str] = {}
        token: Optional[str] = None
        for _ in range(10):
            p = {"verb": "ListSets"} if not token else {"verb": "ListSets", "resumptionToken": token}
            root = await _oai_request(params.server, p)
            ls = root.find("oai:ListSets", NS)
            if ls is None:
                break
            for s in ls.findall("oai:set", NS):
                spec = (s.findtext("oai:setSpec", default="", namespaces=NS) or "").strip()
                name = (s.findtext("oai:setName", default="", namespaces=NS) or "").strip()
                if spec and ":" not in spec:  # journal-level set (sections have 'journal:section')
                    journals[spec] = name
            tok_el = ls.find("oai:resumptionToken", NS)
            token = (tok_el.text or "").strip() if tok_el is not None else None
            if not token:
                break

        items = sorted(journals.items(), key=lambda kv: kv[1])
        if params.name_filter:
            f = params.name_filter.lower()
            items = [(k, v) for k, v in items if f in v.lower() or f in k.lower()]

        if params.response_format == ResponseFormat.JSON:
            return json.dumps(
                {"server": params.server, "count": len(items),
                 "journals": [{"journal": k, "name": v,
                               "url": f"https://{params.server}.tci-thaijo.org/index.php/{k}"} for k, v in items]},
                ensure_ascii=False, indent=2)

        if not items:
            return f"ไม่พบวารสารที่ตรงกับ '{params.name_filter}' ใน {params.server} ลองเซิร์ฟเวอร์อื่นในกลุ่มเดียวกัน"
        lines = [f"## วารสารใน {params.server} ({len(items)} รายการ)", ""]
        lines += [f"- {v} — journal: `{k}`" for k, v in items]
        return "\n".join(lines)
    except Exception as e:  # noqa: BLE001
        return _error_text(e)


@mcp.tool(name="thaijo_list_articles", annotations={"title": "List articles of a ThaiJO journal", **_RO})
async def thaijo_list_articles(params: ListArticlesInput) -> str:
    """ดึงรายการบทความ (เมทาดาทา Dublin Core) ของวารสาร ทีละหน้า (~100 บทความ/หน้า).

    เหมาะกับการรวบรวมบทความทั้งวารสารเพื่อทำ systematic review / bibliometric.
    ใช้ next_resumption_token ที่ได้ เพื่อดึงหน้าถัดไป.
    """
    try:
        if params.resumption_token:
            if not params.server and not params.journal_url:
                return "เมื่อใช้ resumption_token ต้องระบุ server หรือ journal_url เดิมด้วย"
            host, _ = _resolve_host_and_journal(params.server, None, params.journal_url)
            q = {"verb": "ListRecords", "resumptionToken": params.resumption_token}
        else:
            host, jpath = _resolve_host_and_journal(params.server, params.journal, params.journal_url)
            q = {"verb": "ListRecords", "metadataPrefix": "oai_dc"}
            if jpath:
                q["set"] = jpath
            if params.from_date:
                q["from"] = params.from_date
            if params.until_date:
                q["until"] = params.until_date

        root = await _oai_request(host, q)
        records, token, total = _records_and_token(root)

        if params.response_format == ResponseFormat.JSON:
            return json.dumps({"server": host, "count": len(records), "complete_list_size": total,
                               "has_more": bool(token), "next_resumption_token": token,
                               "articles": records}, ensure_ascii=False, indent=2)

        lines = [f"## บทความ ({len(records)} รายการในหน้านี้" + (f" จากทั้งหมด {total}" if total else "") + ")", ""]
        lines += [_fmt_article_md(a) + "\n" for a in records]
        if token:
            lines.append(f"**มีหน้าถัดไป** — next_resumption_token: `{token}` (server: {host})")
        return "\n".join(lines)
    except Exception as e:  # noqa: BLE001
        return _error_text(e)


@mcp.tool(name="thaijo_search_articles", annotations={"title": "Search ThaiJO articles by keyword", **_RO})
async def thaijo_search_articles(params: SearchArticlesInput) -> str:
    """ค้นบทความในวารสาร ThaiJO ตามคำสำคัญ โดยตรวจชื่อเรื่อง คำสำคัญ และบทคัดย่อ (ไทย/อังกฤษ).

    OAI-PMH ไม่มีระบบค้นในตัว tool นี้จึงสแกนทีละหน้าแล้วกรองให้ (~7 วินาที/หน้า ตามข้อจำกัด ThaiJO).
    ควรระบุวารสาร (journal หรือ journal_url) เพื่อความเร็ว. ถ้ายังสแกนไม่ครบ จะคืน resumption token
    ให้เรียกต่อเพื่อสแกนส่วนที่เหลือ.
    """
    try:
        host, jpath = _resolve_host_and_journal(params.server, params.journal, params.journal_url)
        kws = [k.lower() for k in params.keywords if k.strip()]
        matches: list[dict[str, Any]] = []
        scanned = 0
        token = params.resumption_token
        total = None
        pages = 0

        while pages < params.max_pages:
            if token:
                q = {"verb": "ListRecords", "resumptionToken": token}
            else:
                q = {"verb": "ListRecords", "metadataPrefix": "oai_dc"}
                if jpath:
                    q["set"] = jpath
            try:
                root = await _oai_request(host, q)
            except OAIError as e:
                if e.code == "noRecordsMatch":
                    break
                raise
            records, token, total = _records_and_token(root)
            pages += 1
            scanned += len(records)

            for a in records:
                if params.year_from and a["year"] and int(a["year"]) < params.year_from:
                    continue
                if params.year_to and a["year"] and int(a["year"]) > params.year_to:
                    continue
                hay = " ".join([a["title_th"], a["title_en"], a["title"], " ".join(a["keywords"]),
                                a["abstract_th"], a["abstract_en"], a["abstract"]]).lower()
                hits = [k for k in kws if k in hay]
                ok = (len(hits) == len(kws)) if params.match == MatchMode.ALL else bool(hits)
                if ok:
                    a = dict(a)
                    a["matched_keywords"] = hits
                    matches.append(a)
            if not token or len(matches) >= params.max_results:
                break

        # TCI tier tagging / filtering (one check per distinct journal, web detection capped)
        tier_unknown_skipped = 0
        tier_note = ""
        if matches:
            tier_map: dict[str, dict[str, Any]] = {}
            new_web_checks = 0
            for jset in {a["journal_set"] or (jpath or "") for a in matches}:
                if not jset:
                    continue
                allow_web = params.tci_tiers is not None and (
                    tci_tier.is_cached_or_listed(host, jset) or new_web_checks < 5)
                if allow_web and not tci_tier.is_cached_or_listed(host, jset):
                    new_web_checks += 1
                tier_map[jset] = await tci_tier.check_tier(fetch_text=_fetch_text, host=host, jpath=jset,
                                                           allow_web=allow_web)
            for a in matches:
                info = tier_map.get(a["journal_set"] or (jpath or ""), {})
                a["tci_tier"] = info.get("tier")
                a["tci_tier_confidence"] = info.get("confidence", "none")
            if params.tci_tiers is not None:
                before = len(matches)
                matches = [a for a in matches if a["tci_tier"] in params.tci_tiers]
                tier_unknown_skipped = before - len(matches)
                tier_note = (f"กรองเฉพาะ TCI กลุ่ม {params.tci_tiers}: ตัดออก {tier_unknown_skipped} รายการ "
                             "(อยู่กลุ่มอื่นหรือยังระบุกลุ่มไม่ได้)")

        # rank: more keyword hits first, then newer
        matches.sort(key=lambda a: (len(a["matched_keywords"]), a["year"] or "0"), reverse=True)
        shown = matches[: params.max_results]

        if params.response_format == ResponseFormat.JSON:
            return json.dumps({"server": host, "journal": jpath, "scanned": scanned, "complete_list_size": total,
                               "match_count": len(matches), "returned": len(shown),
                               "has_more_to_scan": bool(token), "next_resumption_token": token,
                               "tier_filter_note": tier_note, "articles": shown}, ensure_ascii=False, indent=2)

        scope = f"วารสาร `{jpath}`" if jpath else f"ทุกวารสารใน {host}"
        lines = [f"## ผลการค้น: {', '.join(params.keywords)}",
                 f"ขอบเขต: {scope} | สแกนแล้ว {scanned} บทความ" + (f" จาก {total}" if total else "")
                 + f" | พบ {len(matches)} รายการ"]
        if tier_note:
            lines.append(tier_note)
        lines.append("")
        if not shown:
            lines.append("ยังไม่พบบทความที่ตรงเงื่อนไขในส่วนที่สแกน")
        for a in shown:
            tier_txt = ""
            if a.get("tci_tier"):
                tier_txt = f" | TCI กลุ่ม {a['tci_tier']} (ความเชื่อมั่น: {a['tci_tier_confidence']})"
            lines.append(_fmt_article_md(a) + f"\n(ตรงกับ: {', '.join(a['matched_keywords'])}){tier_txt}\n")
        if token:
            lines.append(f"**ยังสแกนไม่ครบ** — เรียกซ้ำด้วย resumption_token: `{token}` เพื่อค้นส่วนที่เหลือ")
        return "\n".join(lines)
    except Exception as e:  # noqa: BLE001
        return _error_text(e)


@mcp.tool(name="thaijo_get_article", annotations={"title": "Get one ThaiJO article", **_RO})
async def thaijo_get_article(params: GetArticleInput) -> str:
    """ดึงเมทาดาทาเต็มของบทความเดียว (บทคัดย่อไทย+อังกฤษฉบับเต็ม คำสำคัญ ผู้แต่ง DOI ลิงก์).

    ใช้ oai_identifier ที่ได้จาก thaijo_search_articles หรือ thaijo_list_articles.
    """
    try:
        host = params.server
        if not host:
            m = re.search(r"([a-z]{2}\d{2})\.tci-thaijo", params.oai_identifier)
            if not m:
                return "เดาเซิร์ฟเวอร์จาก identifier ไม่ได้ กรุณาระบุ server ด้วย"
            host = m.group(1)
        if host not in ALL_HOSTS:
            return f"ไม่รู้จักเซิร์ฟเวอร์ '{host}'"
        root = await _oai_request(host, {"verb": "GetRecord", "identifier": params.oai_identifier,
                                         "metadataPrefix": "oai_dc"})
        rec = root.find("oai:GetRecord/oai:record", NS)
        a = _parse_record(rec) if rec is not None else None
        if not a:
            return "ไม่พบข้อมูลบทความ (อาจถูกถอนออกจากระบบ)"
        if params.response_format == ResponseFormat.JSON:
            return json.dumps(a, ensure_ascii=False, indent=2)
        return _fmt_article_md(a, full=True)
    except Exception as e:  # noqa: BLE001
        return _error_text(e)


@mcp.tool(name="thaijo_check_tci_tier", annotations={"title": "Check TCI tier of a journal", **_RO})
async def thaijo_check_tci_tier(params: CheckTierInput) -> str:
    """ตรวจว่าวารสารอยู่ใน TCI กลุ่มที่ 1, 2 หรือ 3 (รอบการประเมินปัจจุบัน).

    ลำดับการตรวจ: (1) รายชื่ออ้างอิงที่ผู้ใช้ดูแล (CSV/Google Sheet) -> (2) ข้อความประกาศบนหน้าเว็บวารสารใน ThaiJO.
    ระบุได้ด้วย journal_url, server+journal, journal_name หรือ issn.
    ผลมีระดับความเชื่อมั่น: high (รายชื่อทางการ), medium (ประกาศของวารสารที่ระบุปีรอบปัจจุบัน),
    low (พบคำว่ากลุ่ม/Tier แต่ไม่มีปีกำกับ อาจเป็นข้อมูลรอบเก่า).
    """
    try:
        host = jpath = None
        if params.journal_url or params.server:
            host, jpath = _resolve_host_and_journal(params.server, params.journal, params.journal_url)
        if params.refresh_list:
            await tci_tier.load_list(_fetch_text, force=True)
        if not (jpath or params.journal_name or params.issn):
            return "กรุณาระบุ journal_url (แนะนำ), server+journal, journal_name หรือ issn อย่างใดอย่างหนึ่ง"
        r = await tci_tier.check_tier(fetch_text=_fetch_text, host=host, jpath=jpath,
                                      issn=params.issn, name=params.journal_name)
        _, list_source = await tci_tier.load_list(_fetch_text)
        r["reference_list_source"] = list_source
        r["verify_at"] = "https://tci-thailand.org (ค้นชื่อวารสารเพื่อยืนยันก่อนใช้ยื่นขอตำแหน่งทางวิชาการ)"
        if params.response_format == ResponseFormat.JSON:
            return json.dumps(r, ensure_ascii=False, indent=2)

        if not r.get("tier"):
            return ("## ยังระบุกลุ่ม TCI ไม่ได้\n"
                    f"{r.get('evidence') or 'ไม่พบข้อความประกาศกลุ่ม TCI บนหน้าเว็บวารสาร'}\n\n"
                    "แนะนำ: ค้นชื่อวารสารที่ tci-thailand.org แล้วเพิ่มลงรายชื่ออ้างอิง (CSV/Google Sheet)")
        label = {"high": "สูง (รายชื่อทางการ)", "medium": "ปานกลาง", "low": "ต่ำ (อาจเป็นข้อมูลรอบเก่า)"}
        lines = [f"## TCI กลุ่มที่ {r['tier']}"]
        if r.get("journal_name"):
            lines.append(f"วารสาร: {r['journal_name']}")
        if r.get("valid_period"):
            lines.append(f"ช่วงรับรอง: {r['valid_period']}" + (f" (รอบที่ {r['round']})" if r.get("round") else ""))
        lines.append(f"วิธีตรวจ: {'รายชื่ออ้างอิง' if r['method'] == 'reference_list' else 'ประกาศบนหน้าเว็บวารสาร'}"
                     f" | ความเชื่อมั่น: {label.get(r['confidence'], r['confidence'])}")
        if r.get("evidence"):
            lines.append(f"หลักฐาน: {r['evidence']}")
        if r.get("issns"):
            lines.append("ISSN ที่พบ: " + ", ".join(r["issns"]))
        if r.get("thaijo_url"):
            lines.append(f"เว็บวารสาร: {r['thaijo_url']}")
        if r["confidence"] != "high":
            lines.append("\n⚠️ ควรยืนยันที่ tci-thailand.org ก่อนนำไปใช้ยื่นขอตำแหน่งทางวิชาการหรือสำเร็จการศึกษา")
        return "\n".join(lines)
    except Exception as e:  # noqa: BLE001
        return _error_text(e)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    transport = os.environ.get("MCP_TRANSPORT", "streamable-http")
    mcp.run(transport=transport)  # type: ignore[arg-type]
