#!/usr/bin/env python3
"""
internship_monitor.py  —  Unified Internship Alert System
──────────────────────────────────────────────────────────
Sources polled every 5 min:
  1.  zshah101 JSON API          — 3,500+ companies, auto-aggregated engine
  2.  GitHub/sndsh404             — manually maintained 2027 list
  3.  GitHub/vanshb03             — 7.8k-star community list
  4.  Greenhouse API              — direct ATS: Stripe, Coinbase, OpenAI, Anthropic,
                                    Airbnb, DoorDash, Lyft, Uber, Pinterest, Figma,
                                    Databricks, Plaid, Snowflake, Ramp(via GH), +more
  5.  Lever API                   — direct ATS: Palantir, Cloudflare, Reddit, Zoox, +more
  6.  Ashby API                   — direct ATS: Notion, 1Password, Ramp, Linear, +more
  7.  Workday API                 — direct ATS: Nvidia, Salesforce, Adobe, Shopify,
                                    Atlassian, Netflix, Boeing, Boeing, Capital One, +more
  8.  Google Careers API          — Google's own public job search API
  9.  Microsoft Careers API       — Microsoft's public job search API
  10. Amazon Jobs API             — Amazon's public job search API
  11. Meta Careers scraper        — metacareers.com HTML scrape
  12. Apple Jobs scraper          — jobs.apple.com JSON API
  13. YC Work at a Startup        — workatastartup.com API

Email strategy:
  • Tier-1 companies  →  immediate alert (within 1 poll cycle = ~5 min)
  • Everything else   →  hourly digest (batch, one email per hour max)

Deduplication:
  • Primary:   SHA1 hash of source+job_id stored in seen_jobs.json
  • Secondary: raw URL set catches same job across multiple sources
"""

import json
import time
import smtplib
import logging
import hashlib
import requests
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from html.parser import HTMLParser
from pathlib import Path


# ══════════════════════════════════════════════════════════════════════════════
# CONFIG — fill these in before running
# ══════════════════════════════════════════════════════════════════════════════

import os
GMAIL_USER   = os.environ.get("GMAIL_USER",   "your_gmail@gmail.com")
GMAIL_APP_PW = os.environ.get("GMAIL_APP_PW", "xxxx xxxx xxxx xxxx")
NOTIFY_EMAIL = os.environ.get("NOTIFY_EMAIL", "your_gmail@gmail.com")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")

POLL_INTERVAL   = 300  # seconds between full cycles — 5 min is the sweet spot
DIGEST_INTERVAL = 1800 # seconds between digest emails for non-tier-1 jobs (30 min)
SEEN_FILE       = "seen_jobs.json"

# ── Tier-1: immediate email the second one of these drops ─────────────────────
# Trimmed to: FAANG + AI leaders (default aspirational targets) + your actual
# active referral network. Cut: quant trading, aerospace/defense, and the
# generic enterprise-software long tail (Okta, Datadog, HubSpot, etc.) that
# wasn't tied to a real contact. Add anything back with one word.
TIER1 = {
    # FAANG / MANGO
    "google", "meta", "apple", "amazon", "netflix", "microsoft", "nvidia",
    # AI leaders
    "openai", "anthropic",
    # Your active referral network
    "coinbase", "snowflake", "shopify", "bloomberg", "boeing", "schwab",
    "capital one", "disney", "uber", "stripe", "pinterest", "sap", "marvell",
}

# ── Tier-2: everything not in TIER1 goes to 30-min digest ─────────────────────
TIER2 = set()

ALL_TARGETS = TIER1 | TIER2

# ── Role filters ──────────────────────────────────────────────────────────────
INTERN_KEYWORDS = {
    "intern", "internship", "co-op", "coop", "co op",
}

# Role must contain at least one of these to pass
SWE_INCLUDE_KEYWORDS = {
    "software engineer", "software developer", "swe",
    "backend", "back-end", "back end",
    "platform engineer", "platform developer",
    "infrastructure engineer", "infra engineer",
    "systems engineer", "systems developer",
    "software engineering",
    "site reliability", "sre",
    "devops engineer",
    "application engineer", "application developer",
}

# Role is excluded if it contains any of these — even if it also matches above
EXCLUDE_KEYWORDS = {
    # frontend / fullstack
    "frontend", "front-end", "front end",
    "fullstack", "full stack", "full-stack",
    "ui engineer", "ui developer",
    # data / ML / AI specializations
    "data science", "data scientist", "data engineer", "data analyst",
    "machine learning", "ml engineer", "ml research",
    "applied scientist", "research scientist", "research engineer",
    "nlp", "computer vision", "cv engineer", "deep learning",
    "artificial intelligence", "ai engineer", "ai researcher",
    # quant
    "quantitative", "quant developer", "quant research",
    # hardware / embedded (not SWE)
    "hardware engineer", "embedded", "firmware", "fpga",
    "electrical engineer", "mechanical engineer",
    # mobile (usually their own separate track)
    "ios", "android", "mobile engineer",
}


# ══════════════════════════════════════════════════════════════════════════════
# GITHUB REPOS
# ══════════════════════════════════════════════════════════════════════════════

GITHUB_REPOS = [
    {"owner": "sndsh404", "repo": "summer-2027-internships",  "branch": "main"},
    {"owner": "vanshb03",  "repo": "Summer2027-Internships",  "branch": "dev"},
]

ZSHAH101_API = (
    "https://zshah101.github.io/Automated-List-Of-Summer-2027-and-Fall-2026"
    "-Tech-Internships/api/jobs.json"
)


# ══════════════════════════════════════════════════════════════════════════════
# ATS SLUGS
# ══════════════════════════════════════════════════════════════════════════════

GREENHOUSE_SLUGS = [
    "stripe", "coinbase", "airbnb", "doordash", "lyft", "uber",
    "pinterest", "figma", "openai", "anthropic", "databricks",
    "plaid", "snowflake", "rocketlab", "andurilindustries", "podium81",
    "aquaticcapitalmanagement", "samsungresearchamericainternship",
    "figureai", "sharkninjaoperatingllc", "walleyecapital-external-students",
    "schonfeld", "robinhood", "asana", "brex", "rippling",
    "scale", "discord", "duolingo", "dropbox", "twitch",
]

LEVER_SLUGS = [
    "palantir", "cloudflare", "reddit", "zoox",
    "aisafety", "solopulseco", "hermeus",
]

ASHBY_SLUGS = [
    "ramp", "notion", "skydio", "1password", "saronic",
    "ellipsislabs", "homebase", "poshmark", "linear", "retool",
    "vercel", "mercury", "watershed", "dbt-labs", "anyscale",
]


# ══════════════════════════════════════════════════════════════════════════════
# WORKDAY CONFIGS
# Format: (company_display_name, subdomain, wd_version, site_name)
# API:    POST https://{subdomain}.wd{ver}.myworkdayjobs.com/wday/cxs/
#                  {subdomain}/{site_name}/jobs
# ══════════════════════════════════════════════════════════════════════════════

WORKDAY_CONFIGS = [
    # (display_name,         subdomain,          wd_ver, site_path)
    ("Nvidia",              "nvidia",            "5",    "NVIDIAExternalCareerSite"),
    ("Salesforce",          "salesforce",        "12",   "External_Career_Site"),
    ("Adobe",               "adobe",             "5",    "external_wday"),
    ("Shopify",             "shopify",           "5",    "Shopify"),
    ("Netflix",             "netflix",           "5",    "External"),
    ("Capital One",         "capitalone",        "1",    "Capital_One"),
    ("Boeing",              "boeing",            "1",    "EXTERNAL_CAREERS"),
    ("Target",              "target",            "5",    "TargetCareers"),
    ("Johnson & Johnson",   "jj",                "5",    "JnJExternalCareers"),
    ("GE Appliances",       "haier",             "3",    "GE_Appliances"),
    ("Blue Origin",         "blueorigin",        "5",    "BlueOrigin"),
    ("Motorola",            "motorolasolutions", "5",    "Careers"),
    ("TD Bank",             "td",                "3",    "TD_Bank_Careers"),
    # Added — confirmed live Workday tenants for two of your referral-network
    # companies that were in TIER1 but had zero ATS coverage:
    ("Disney",              "disney",            "5",    "disneycareer"),
    ("Marvell",             "marvell",           "1",    "MarvellCareers"),
]


# ══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# DEDUP STORE
# Primary key:   SHA1(source::uid)   — catches same job within one source
# Secondary key: raw apply URL       — catches same job across multiple sources
# ══════════════════════════════════════════════════════════════════════════════

def load_seen() -> tuple[set, set]:
    if Path(SEEN_FILE).exists():
        with open(SEEN_FILE) as f:
            data = json.load(f)
            # support both old (list) and new (dict) format
            if isinstance(data, list):
                return set(data), set()
            return set(data.get("ids", [])), set(data.get("urls", []))
    return set(), set()


def save_seen(ids: set, urls: set):
    with open(SEEN_FILE, "w") as f:
        json.dump({"ids": sorted(ids), "urls": sorted(urls)}, f)


def load_state() -> tuple[set, set, dict]:
    """Single read of SEEN_FILE returning ids, urls, and health tracking."""
    if Path(SEEN_FILE).exists():
        with open(SEEN_FILE) as f:
            data = json.load(f)
            if isinstance(data, list):
                return set(data), set(), {}
            return (set(data.get("ids", [])), set(data.get("urls", [])),
                    data.get("health", {}))
    return set(), set(), {}


def save_state(ids: set, urls: set, health: dict):
    with open(SEEN_FILE, "w") as f:
        json.dump({"ids": sorted(ids), "urls": sorted(urls), "health": health}, f)


def make_id(source: str, uid: str) -> str:
    return hashlib.sha1(f"{source}::{uid}".lower().encode()).hexdigest()


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE HEALTH TRACKING
# Persisted alongside seen_jobs.json so it survives across GitHub Actions runs
# (each run is a fresh container — nothing survives except what's committed).
#
# Per source we track:
#   consec_errors  — polls in a row that raised an exception (network/API break)
#   last_nonzero   — unix ts of the last time this source returned >0 jobs
#   last_alerted   — unix ts we last sent a health warning for this source,
#                    so we alert once per issue instead of every 30 min
#
# Two alert conditions:
#   ERRORING — 3+ consecutive exceptions → something is actively broken
#   SILENT   — 7+ days since last nonzero result → probably broken, just not
#              throwing (e.g. README table format changed, HTML scrape target
#              moved) — the exact failure mode flagged for GitHub/Meta
# ══════════════════════════════════════════════════════════════════════════════

ERROR_STREAK_THRESHOLD = 3
SILENT_DAYS_THRESHOLD  = 7
ALERT_COOLDOWN_SECONDS = 86400  # re-alert on the same issue at most once/day

ALL_SOURCES = [
    "zshah101", "github", "greenhouse", "lever", "ashby", "workday",
    "google", "microsoft", "amazon", "meta", "apple", "yc",
]


def run_source(name: str, fn, ids, urls, health: dict) -> list:
    """Wraps a poll_* call, updates health tracking, never lets one source's
    failure take down the whole cycle."""
    rec = health.setdefault(name, {
        "consec_errors": 0, "last_nonzero": time.time(), "last_alerted": 0,
    })
    try:
        results = fn(ids, urls)
        rec["consec_errors"] = 0
        if results:
            rec["last_nonzero"] = time.time()
        return results
    except Exception as e:
        rec["consec_errors"] += 1
        log.error(f"{name}: unhandled error ({e}) — consec_errors={rec['consec_errors']}")
        return []


def check_health_alerts(health: dict) -> list:
    """Returns a list of human-readable alert strings for sources that look
    broken, respecting the once-per-day cooldown per source."""
    now = time.time()
    alerts = []
    for name in ALL_SOURCES:
        rec = health.get(name)
        if not rec:
            continue
        if now - rec.get("last_alerted", 0) < ALERT_COOLDOWN_SECONDS:
            continue

        fired = False
        if rec.get("consec_errors", 0) >= ERROR_STREAK_THRESHOLD:
            alerts.append(
                f"⚠️ {name}: {rec['consec_errors']} consecutive failed polls — "
                f"likely broken (API/endpoint change)."
            )
            fired = True
        else:
            days_silent = (now - rec.get("last_nonzero", now)) / 86400
            if days_silent >= SILENT_DAYS_THRESHOLD:
                alerts.append(
                    f"⚠️ {name}: 0 results for {days_silent:.1f} days — "
                    f"probably broken silently (parser/format change), not just quiet."
                )
                fired = True

        if fired:
            rec["last_alerted"] = now

    return alerts


def is_new(ids: set, urls: set, source: str, uid: str, url: str) -> bool:
    """Returns True if job is genuinely new (not seen by either dedup layer)."""
    job_id = make_id(source, uid)
    norm_url = url.split("?")[0].rstrip("/").lower()   # strip tracking params
    if job_id in ids or (norm_url and norm_url in urls):
        return False
    ids.add(job_id)
    if norm_url:
        urls.add(norm_url)
    return True


# ══════════════════════════════════════════════════════════════════════════════
# FILTERING
# ══════════════════════════════════════════════════════════════════════════════

# ── Location filter ────────────────────────────────────────────────────────────
# Feeds report location as free text with zero consistency (city only, "Remote",
# "London, UK", "USA", state abbrev, etc). There's no reliable structured country
# field across all 13 sources, so this is a keyword heuristic, not a guarantee.
#
# Policy: block on any clear NON-US signal. If we can't tell (blank, ambiguous
# "Remote" with no country, unrecognized city), we let it through rather than
# silently drop it — false negatives (US job dropped) are worse than false
# positives (you get an email you delete in 2 seconds).

NON_US_KEYWORDS = {
    # explicit country / region names
    "canada", "united kingdom", "uk", "england", "scotland", "wales",
    "ireland", "germany", "france", "spain", "italy", "netherlands",
    "poland", "sweden", "switzerland", "austria", "belgium", "portugal",
    "india", "china", "japan", "singapore", "hong kong", "taiwan",
    "south korea", "korea", "australia", "new zealand",
    "mexico", "brazil", "argentina", "chile", "colombia",
    "israel", "uae", "united arab emirates", "dubai",
    "philippines", "vietnam", "indonesia", "malaysia", "thailand",
    "romania", "czech", "hungary", "greece", "denmark", "norway", "finland",
    "south africa", "nigeria", "egypt", "pakistan", "bangladesh",
    "eu remote", "emea", "apac", "latam",
    # common non-US cities that show up without a country label
    "london", "toronto", "vancouver", "montreal", "dublin", "berlin",
    "munich", "paris", "amsterdam", "warsaw", "madrid", "barcelona",
    "milan", "zurich", "tel aviv", "bangalore", "bengaluru", "hyderabad",
    "mumbai", "delhi", "pune", "shanghai", "beijing", "shenzhen",
    "tokyo", "seoul", "sydney", "melbourne", "sao paulo", "mexico city",
}

US_KEYWORDS = {
    "united states", "usa", "u.s.", "us remote", "remote - us", "remote, us",
    "remote (us)", "remote-usa", "remote usa",
    # states (name + common abbreviations, filtered for ones unlikely to
    # collide with the non-US city/country strings above)
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
    "connecticut", "delaware", "florida", "georgia", "hawaii", "idaho",
    "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana",
    "maine", "maryland", "massachusetts", "michigan", "minnesota",
    "mississippi", "missouri", "montana", "nebraska", "nevada",
    "new hampshire", "new jersey", "new mexico", "new york", "north carolina",
    "north dakota", "ohio", "oklahoma", "oregon", "pennsylvania",
    "rhode island", "south carolina", "south dakota", "tennessee", "texas",
    "utah", "vermont", "virginia", "washington", "west virginia",
    "wisconsin", "wyoming",
    # major US cities that commonly appear without "USA" appended
    "san francisco", "new york city", "nyc", "seattle", "austin", "chicago",
    "boston", "los angeles", "san jose", "sunnyvale", "mountain view",
    "menlo park", "palo alto", "redmond", "bellevue", "atlanta", "denver",
    "san diego", "portland", "miami", "dallas", "houston", "raleigh",
    "durham", "pittsburgh", "washington dc", "arlington", "cambridge",
}


def is_us_location(loc: str) -> bool:
    lo = loc.lower().strip()
    if not lo:
        return True  # unknown — let it through, don't silently drop

    if any(k in lo for k in US_KEYWORDS):
        return True

    if any(k in lo for k in NON_US_KEYWORDS):
        return False

    # "Remote" with no country/city qualifier at all — ambiguous, let through
    if "remote" in lo:
        return True

    # Unrecognized location string (couldn't match either list) — let through
    return True


def is_relevant(company: str, role: str, loc: str = "") -> bool:
    co = company.lower()
    ro = role.lower()

    # Must be a target company (or no filter set)
    if ALL_TARGETS and not any(t in co for t in ALL_TARGETS):
        return False

    # Must be an intern/co-op role
    if not any(k in ro for k in INTERN_KEYWORDS):
        return False

    # Must be a SWE-type role
    if not any(k in ro for k in SWE_INCLUDE_KEYWORDS):
        return False

    # Kill frontend-only, fullstack, data, ML, hardware, mobile
    if any(k in ro for k in EXCLUDE_KEYWORDS):
        return False

    # Must be a US location (or unknown/ambiguous — see is_us_location)
    if not is_us_location(loc):
        return False

    return True


def tier(company: str) -> int:
    co = company.lower()
    if any(t in co for t in TIER1):
        return 1
    if any(t in co for t in TIER2):
        return 2
    return 2  # default to digest if it made it through the filter


# ══════════════════════════════════════════════════════════════════════════════
# EMAIL
# ══════════════════════════════════════════════════════════════════════════════

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


def _build_email_html(jobs: list, label: str) -> str:
    rows = "".join(
        f"""<tr>
          <td style="padding:10px 12px;border-bottom:1px solid #eee;
              font-weight:600;white-space:nowrap">{j['company']}</td>
          <td style="padding:10px 12px;border-bottom:1px solid #eee">{j['role']}</td>
          <td style="padding:10px 12px;border-bottom:1px solid #eee;
              color:#666;font-size:13px">{j.get('location','—')}</td>
          <td style="padding:10px 12px;border-bottom:1px solid #eee;
              color:#999;font-size:11px">{j['source']}</td>
          <td style="padding:10px 12px;border-bottom:1px solid #eee">
            <a href="{j['url']}"
               style="background:#2563eb;color:#fff;padding:5px 12px;
                      border-radius:4px;text-decoration:none;
                      font-size:13px;font-weight:600">Apply →</a>
          </td>
        </tr>"""
        for j in jobs
    )
    return f"""
    <html><body style="font-family:-apple-system,BlinkMacSystemFont,sans-serif;
                       max-width:960px;margin:0 auto;padding:0">
      <div style="background:#0f172a;padding:20px 24px">
        <h2 style="color:#fff;margin:0;font-size:20px">{label}</h2>
        <p style="color:#94a3b8;margin:6px 0 0;font-size:13px">
          {datetime.now().strftime('%A %b %d, %Y · %H:%M')}
          · {len(jobs)} role{'s' if len(jobs)>1 else ''}
        </p>
      </div>
      <table style="border-collapse:collapse;width:100%;
                    border:1px solid #e2e8f0;border-top:none">
        <tr style="background:#f8fafc;font-size:12px;color:#64748b;
                   text-transform:uppercase;letter-spacing:.05em">
          <th style="padding:8px 12px;text-align:left">Company</th>
          <th style="padding:8px 12px;text-align:left">Role</th>
          <th style="padding:8px 12px;text-align:left">Location</th>
          <th style="padding:8px 12px;text-align:left">Source</th>
          <th style="padding:8px 12px;text-align:left">Link</th>
        </tr>
        {rows}
      </table>
      <p style="color:#94a3b8;font-size:11px;padding:12px">
        internship_monitor · apply fast, roles close fast
      </p>
    </body></html>
    """


def send_email(jobs: list, subject: str):
    if not jobs:
        return
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = GMAIL_USER
    msg["To"]      = NOTIFY_EMAIL
    msg.attach(MIMEText(_build_email_html(jobs, subject), "html"))
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
            s.login(GMAIL_USER, GMAIL_APP_PW)
            s.send_message(msg)
        log.info(f"✉  sent: {subject[:60]}")
    except Exception as e:
        log.error(f"email failed: {e}")


def send_health_alert(alerts: list):
    """One plain-text email listing any sources that look broken. Only fires
    when check_health_alerts() has something to say, and each source is
    capped at one alert per day (see ALERT_COOLDOWN_SECONDS)."""
    if not alerts:
        return
    body = (
        "Source health check flagged the following:\n\n"
        + "\n".join(alerts)
        + "\n\n— internship_monitor"
    )
    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"🔧 Monitor health warning — {len(alerts)} source(s) may be broken"
    msg["From"]    = GMAIL_USER
    msg["To"]      = NOTIFY_EMAIL
    msg.attach(MIMEText(body, "plain"))
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
            s.login(GMAIL_USER, GMAIL_APP_PW)
            s.send_message(msg)
        log.info(f"✉  sent health alert: {len(alerts)} issue(s)")
    except Exception as e:
        log.error(f"health alert email failed: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def gh_headers() -> dict:
    h = {"Accept": "application/vnd.github+json"}
    if GITHUB_TOKEN:
        h["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    return h


def extract_md_url(cell: str) -> str:
    if "](http" in cell:
        try:
            return cell.split("](")[1].split(")")[0].strip()
        except Exception:
            pass
    return ""


def job_dict(company, role, loc, url, source) -> dict:
    return {"company": company, "role": role,
            "location": loc, "url": url, "source": source}


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE 1 — zshah101 JSON API
# ══════════════════════════════════════════════════════════════════════════════

def poll_zshah101(ids, urls) -> list:
    new = []
    try:
        data = requests.get(ZSHAH101_API, timeout=15).json()
        jobs = data if isinstance(data, list) else data.get("jobs", [])
        for j in jobs:
            company = j.get("company", "")
            role    = j.get("role", j.get("title", ""))
            url     = j.get("url", j.get("apply", ""))
            loc     = j.get("location", "")
            if is_relevant(company, role, loc) and is_new(ids, urls, "zshah101", url, url):
                new.append(job_dict(company, role, loc, url, "zshah101"))
        log.info(f"zshah101:    {len(new):3d} new")
    except Exception as e:
        log.warning(f"zshah101: {e}")
    return new


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE 2 — GitHub repo commit monitor + README parser
# ══════════════════════════════════════════════════════════════════════════════

_last_sha: dict = {}


def poll_github(ids, urls) -> list:
    new = []
    for r in GITHUB_REPOS:
        key = f"{r['owner']}/{r['repo']}"
        try:
            resp = requests.get(
                f"https://api.github.com/repos/{r['owner']}/{r['repo']}/commits/{r['branch']}",
                headers=gh_headers(), timeout=10,
            )
            sha = resp.json().get("sha", "")
            if not sha or sha == _last_sha.get(key):
                continue
            _last_sha[key] = sha
            log.info(f"github:      new commit {key} ({sha[:8]})")
            new.extend(_parse_readme(r, ids, urls, key))
        except Exception as e:
            log.warning(f"github ({key}): {e}")
    if new:
        log.info(f"github:      {len(new):3d} new")
    return new


def _parse_readme(r, ids, urls, key) -> list:
    new = []
    try:
        raw = requests.get(
            f"https://raw.githubusercontent.com/{r['owner']}/{r['repo']}"
            f"/{r['branch']}/README.md",
            timeout=15,
        ).text
        for line in raw.splitlines():
            if not line.startswith("|") or "---" in line or "🔒" in line:
                continue
            cells = [c.strip() for c in line.split("|") if c.strip()]
            if len(cells) < 2:
                continue
            company = cells[0].lstrip("↳ ").strip()
            role    = cells[1]
            if company.lower() in {"company", "org", "role", "position"}:
                continue
            apply_url = next((extract_md_url(c) for c in cells if extract_md_url(c)), "")
            if not apply_url:
                continue
            loc = cells[2] if len(cells) > 2 else ""
            if is_relevant(company, role, loc) and is_new(ids, urls, key, apply_url, apply_url):
                new.append(job_dict(company, role, loc, apply_url, f"github/{r['owner']}"))
    except Exception as e:
        log.warning(f"readme parse ({key}): {e}")
    return new


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE 3 — Greenhouse
# ══════════════════════════════════════════════════════════════════════════════

def poll_greenhouse(ids, urls) -> list:
    new = []
    for slug in GREENHOUSE_SLUGS:
        try:
            data = requests.get(
                f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs",
                timeout=10,
            ).json()
            for j in data.get("jobs", []):
                role  = j.get("title", "")
                url   = j.get("absolute_url", "")
                loc   = j.get("location", {}).get("name", "")
                jid   = str(j.get("id", url))
                if is_relevant(slug, role, loc) and is_new(ids, urls, "greenhouse", jid, url):
                    new.append(job_dict(slug.replace("-"," ").title(), role, loc, url, "Greenhouse"))
            time.sleep(0.35)
        except Exception as e:
            log.warning(f"greenhouse ({slug}): {e}")
    log.info(f"greenhouse:  {len(new):3d} new")
    return new


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE 4 — Lever
# ══════════════════════════════════════════════════════════════════════════════

def poll_lever(ids, urls) -> list:
    new = []
    for slug in LEVER_SLUGS:
        try:
            jobs = requests.get(
                f"https://api.lever.co/v0/postings/{slug}?mode=json",
                timeout=10,
            ).json()
            if not isinstance(jobs, list):
                continue
            for j in jobs:
                role = j.get("text", "")
                url  = j.get("hostedUrl", "")
                loc  = j.get("categories", {}).get("location", "")
                jid  = j.get("id", url)
                if is_relevant(slug, role, loc) and is_new(ids, urls, "lever", jid, url):
                    new.append(job_dict(slug.title(), role, loc, url, "Lever"))
            time.sleep(0.35)
        except Exception as e:
            log.warning(f"lever ({slug}): {e}")
    log.info(f"lever:       {len(new):3d} new")
    return new


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE 5 — Ashby
# ══════════════════════════════════════════════════════════════════════════════

def poll_ashby(ids, urls) -> list:
    new = []
    for slug in ASHBY_SLUGS:
        try:
            data = requests.get(
                f"https://api.ashbyhq.com/posting-api/job-board/{slug}",
                timeout=10,
            ).json()
            for j in data.get("jobPostings", []):
                role = j.get("title", "")
                url  = j.get("jobPostingUrl", "")
                loc  = j.get("locationName", "")
                jid  = j.get("id", url)
                if is_relevant(slug, role, loc) and is_new(ids, urls, "ashby", jid, url):
                    new.append(job_dict(slug.title(), role, loc, url, "Ashby"))
            time.sleep(0.35)
        except Exception as e:
            log.warning(f"ashby ({slug}): {e}")
    log.info(f"ashby:       {len(new):3d} new")
    return new


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE 6 — Workday (POST API)
# Each company has its own Workday tenant but the API shape is identical.
# POST /wday/cxs/{subdomain}/{site}/jobs  →  {"jobPostings": [...]}
# ══════════════════════════════════════════════════════════════════════════════

def poll_workday(ids, urls) -> list:
    new = []
    body = {"appliedFacets": {}, "limit": 20, "offset": 0, "searchText": "intern"}
    for (display, subdomain, ver, site) in WORKDAY_CONFIGS:
        endpoint = (
            f"https://{subdomain}.wd{ver}.myworkdayjobs.com"
            f"/wday/cxs/{subdomain}/{site}/jobs"
        )
        base_url = (
            f"https://{subdomain}.wd{ver}.myworkdayjobs.com"
            f"/en-US/{site}"
        )
        try:
            resp = requests.post(
                endpoint, json=body,
                headers={**BROWSER_HEADERS, "Content-Type": "application/json"},
                timeout=12,
            )
            if resp.status_code != 200:
                continue
            for j in resp.json().get("jobPostings", []):
                role  = j.get("title", "")
                path  = j.get("externalPath", "")
                url   = f"{base_url}{path}" if path else base_url
                loc   = j.get("locationsText", "")
                jid   = path or role
                if is_relevant(display, role, loc) and is_new(ids, urls, f"workday/{subdomain}", jid, url):
                    new.append(job_dict(display, role, loc, url, "Workday"))
            time.sleep(0.5)
        except Exception as e:
            log.warning(f"workday ({display}): {e}")
    log.info(f"workday:     {len(new):3d} new")
    return new


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE 7 — Google Careers
# Public JSON API, no auth required
# ══════════════════════════════════════════════════════════════════════════════

def poll_google(ids, urls) -> list:
    new = []
    try:
        resp = requests.get(
            "https://careers.google.com/api/jobs/jobs-site/search/"
            "?q=software+engineer+intern&npage=1",
            headers=BROWSER_HEADERS, timeout=12,
        )
        if resp.status_code != 200:
            return new
        for j in resp.json().get("jobs", []):
            role = j.get("title", {})
            if isinstance(role, dict):
                role = role.get("rendered", "")
            loc  = j.get("locations", [{}])[0].get("display", "") if j.get("locations") else ""
            jid  = str(j.get("id", ""))
            url  = f"https://careers.google.com/jobs/results/{jid}"
            if is_relevant("google", role, loc) and is_new(ids, urls, "google", jid, url):
                new.append(job_dict("Google", role, loc, url, "Google Careers"))
        log.info(f"google:      {len(new):3d} new")
    except Exception as e:
        log.warning(f"google careers: {e}")
    return new


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE 8 — Microsoft Careers
# Public search API, no auth required
# ══════════════════════════════════════════════════════════════════════════════

def poll_microsoft(ids, urls) -> list:
    new = []
    try:
        resp = requests.get(
            "https://gcsservices.careers.microsoft.com/search/api/v1/search"
            "?q=software+intern&l=en_us&pg=1&pgSz=20&o=Relevance&flt=true",
            headers=BROWSER_HEADERS, timeout=12,
        )
        if resp.status_code != 200:
            return new
        jobs = (resp.json()
                .get("operationResult", {})
                .get("result", {})
                .get("jobs", []))
        for j in jobs:
            role = j.get("title", "")
            loc  = j.get("primaryWorkLocation", "")
            jid  = str(j.get("jobId", ""))
            url  = f"https://jobs.careers.microsoft.com/global/en/job/{jid}"
            if is_relevant("microsoft", role, loc) and is_new(ids, urls, "microsoft", jid, url):
                new.append(job_dict("Microsoft", role, loc, url, "Microsoft Careers"))
        log.info(f"microsoft:   {len(new):3d} new")
    except Exception as e:
        log.warning(f"microsoft: {e}")
    return new


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE 9 — Amazon Jobs
# Public search API. Returns JSON directly.
# ══════════════════════════════════════════════════════════════════════════════

def poll_amazon(ids, urls) -> list:
    new = []
    for query in ["software intern", "data science intern", "machine learning intern"]:
        try:
            q = query.replace(" ", "+")
            resp = requests.get(
                f"https://www.amazon.jobs/en/search.json"
                f"?base_query={q}&category=software-development"
                f"&normalized_country_code=US",
                headers=BROWSER_HEADERS, timeout=12,
            )
            if resp.status_code != 200:
                continue
            for j in resp.json().get("jobs", []):
                role = j.get("title", "")
                loc  = j.get("location", "")
                jid  = str(j.get("id", ""))
                path = j.get("job_path", "")
                url  = f"https://www.amazon.jobs{path}" if path else "https://www.amazon.jobs"
                if is_relevant("amazon", role, loc) and is_new(ids, urls, "amazon", jid, url):
                    new.append(job_dict("Amazon", role, loc, url, "Amazon Jobs"))
            time.sleep(1)
        except Exception as e:
            log.warning(f"amazon ({query}): {e}")
    log.info(f"amazon:      {len(new):3d} new")
    return new


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE 10 — Meta Careers (HTML scrape)
# metacareers.com exposes job data in a <script> tag as JSON
# ══════════════════════════════════════════════════════════════════════════════

def poll_meta(ids, urls) -> list:
    new = []
    try:
        resp = requests.get(
            "https://www.metacareers.com/jobs"
            "?roles[0]=intern&teams[0]=software-engineering",
            headers=BROWSER_HEADERS, timeout=15,
        )
        import re
        # Meta embeds job JSON in a script tag
        match = re.search(r'"job_listings":\s*(\[.+?\])', resp.text, re.DOTALL)
        if not match:
            return new
        jobs = json.loads(match.group(1))
        for j in jobs:
            role = j.get("title", "")
            loc  = j.get("locations", [""])[0] if j.get("locations") else ""
            jid  = str(j.get("id", ""))
            url  = f"https://www.metacareers.com/jobs/{jid}"
            if is_relevant("meta", role, loc) and is_new(ids, urls, "meta", jid, url):
                new.append(job_dict("Meta", role, loc, url, "Meta Careers"))
        log.info(f"meta:        {len(new):3d} new")
    except Exception as e:
        log.warning(f"meta: {e}")
    return new


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE 11 — Apple Jobs
# jobs.apple.com exposes a search JSON API
# ══════════════════════════════════════════════════════════════════════════════

def poll_apple(ids, urls) -> list:
    new = []
    try:
        resp = requests.post(
            "https://jobs.apple.com/api/role/search",
            json={
                "query": "intern",
                "locale": "en-us",
                "filters": {"postingpostLocation": ["postLocation-USA"]},
                "page": 1,
                "pageSize": 20,
                "sort": "newest",
            },
            headers={**BROWSER_HEADERS, "Content-Type": "application/json"},
            timeout=12,
        )
        if resp.status_code != 200:
            return new
        for j in resp.json().get("searchResults", []):
            role = j.get("postingTitle", "")
            loc  = j.get("locations", [{}])[0].get("name", "") if j.get("locations") else ""
            jid  = str(j.get("positionId", ""))
            url  = f"https://jobs.apple.com/en-us/details/{jid}"
            if is_relevant("apple", role, loc) and is_new(ids, urls, "apple", jid, url):
                new.append(job_dict("Apple", role, loc, url, "Apple Jobs"))
        log.info(f"apple:       {len(new):3d} new")
    except Exception as e:
        log.warning(f"apple: {e}")
    return new


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE 12 — YC Work at a Startup
# Covers YC-backed companies (Venu AI's cohort, etc.)
# ══════════════════════════════════════════════════════════════════════════════

def poll_yc(ids, urls) -> list:
    new = []
    try:
        resp = requests.get(
            "https://www.workatastartup.com/api/companies"
            "?companySize=any&remote=any&jobType=intern"
            "&jobRole=eng&jobTitle=software",
            headers=BROWSER_HEADERS, timeout=12,
        )
        if resp.status_code != 200:
            return new
        data = resp.json()
        companies = data if isinstance(data, list) else data.get("companies", [])
        for co in companies:
            co_name = co.get("name", "")
            for j in co.get("jobs", []):
                role = j.get("title", "")
                url  = f"https://www.workatastartup.com/jobs/{j.get('id','')}"
                loc  = j.get("remote_ok") and "Remote" or co.get("locations", [""])[0]
                jid  = str(j.get("id", ""))
                if is_relevant(co_name, role, loc) and is_new(ids, urls, "yc", jid, url):
                    new.append(job_dict(co_name, role, loc, url, "YC/WaaS"))
        log.info(f"yc/waas:     {len(new):3d} new")
    except Exception as e:
        log.warning(f"yc waas: {e}")
    return new


# ══════════════════════════════════════════════════════════════════════════════
# MAIN LOOP  —  tiered email dispatch
# ══════════════════════════════════════════════════════════════════════════════

def dispatch(all_new: list, digest_buffer: list) -> list:
    """
    Tier-1 jobs  →  immediate email
    Everything else → add to digest buffer (caller sends hourly)
    Returns updated buffer.
    """
    tier1_jobs = [j for j in all_new if tier(j["company"]) == 1]
    tier2_jobs = [j for j in all_new if tier(j["company"]) == 2]

    if tier1_jobs:
        n = len(tier1_jobs)
        send_email(
            tier1_jobs,
            f"🚨 TIER-1 ALERT: {n} new posting{'s' if n>1 else ''} "
            f"— {', '.join(set(j['company'] for j in tier1_jobs))}",
        )

    digest_buffer.extend(tier2_jobs)
    return digest_buffer


def main():
    log.info("internship_monitor starting")
    ids, urls, health = load_state()
    log.info(f"loaded {len(ids)} seen IDs, {len(urls)} seen URLs")
    log.info(f"targeting {len(ALL_TARGETS)} companies · polling every {POLL_INTERVAL}s")
    log.info(f"tier-1 ({len(TIER1)}): immediate alert")
    log.info(f"tier-2 ({len(TIER2)}): hourly digest")

    digest_buffer: list = []
    last_digest = time.time()

    while True:
        try:
            log.info("─── poll cycle ─────────────────────────────")
            all_new = []
            all_new += run_source("zshah101",   poll_zshah101,   ids, urls, health)
            all_new += run_source("github",      poll_github,     ids, urls, health)
            all_new += run_source("greenhouse",  poll_greenhouse, ids, urls, health)
            all_new += run_source("lever",       poll_lever,      ids, urls, health)
            all_new += run_source("ashby",       poll_ashby,      ids, urls, health)
            all_new += run_source("workday",     poll_workday,    ids, urls, health)
            all_new += run_source("google",      poll_google,     ids, urls, health)
            all_new += run_source("microsoft",   poll_microsoft,  ids, urls, health)
            all_new += run_source("amazon",      poll_amazon,     ids, urls, health)
            all_new += run_source("meta",        poll_meta,       ids, urls, health)
            all_new += run_source("apple",       poll_apple,      ids, urls, health)
            all_new += run_source("yc",          poll_yc,         ids, urls, health)
            log.info(f"─── {len(all_new)} new total ─────────────────────")

            digest_buffer = dispatch(all_new, digest_buffer)

            alerts = check_health_alerts(health)
            if alerts:
                send_health_alert(alerts)

            save_state(ids, urls, health)

            # Send hourly digest for tier-2
            if time.time() - last_digest >= DIGEST_INTERVAL and digest_buffer:
                n = len(digest_buffer)
                send_email(
                    digest_buffer,
                    f"📋 Hourly Digest: {n} new intern posting{'s' if n>1 else ''}",
                )
                digest_buffer.clear()
                last_digest = time.time()

        except KeyboardInterrupt:
            log.info("stopped — saving state")
            save_state(ids, urls, health)
            break
        except Exception as e:
            log.error(f"main loop error: {e}")

        time.sleep(POLL_INTERVAL)


# ══════════════════════════════════════════════════════════════════════════════
# SINGLE CYCLE MODE — used by GitHub Actions
# ══════════════════════════════════════════════════════════════════════════════

def run_once():
    log.info("internship_monitor — single cycle (GitHub Actions mode)")
    ids, urls, health = load_state()
    log.info(f"loaded {len(ids)} seen IDs, {len(urls)} seen URLs")

    all_new = []
    all_new += run_source("zshah101",   poll_zshah101,   ids, urls, health)
    all_new += run_source("github",      poll_github,     ids, urls, health)
    all_new += run_source("greenhouse",  poll_greenhouse, ids, urls, health)
    all_new += run_source("lever",       poll_lever,      ids, urls, health)
    all_new += run_source("ashby",       poll_ashby,      ids, urls, health)
    all_new += run_source("workday",     poll_workday,    ids, urls, health)
    all_new += run_source("google",      poll_google,     ids, urls, health)
    all_new += run_source("microsoft",   poll_microsoft,  ids, urls, health)
    all_new += run_source("amazon",      poll_amazon,     ids, urls, health)
    all_new += run_source("meta",        poll_meta,       ids, urls, health)
    all_new += run_source("apple",       poll_apple,      ids, urls, health)
    all_new += run_source("yc",          poll_yc,         ids, urls, health)
    log.info(f"─── {len(all_new)} new total ─────────────────────")

    if all_new:
        companies = list(set(j['company'] for j in all_new))[:3]
        extra = f" +{len(all_new)-3} more" if len(all_new) > 3 else ""
        send_email(
            all_new,
            f"🚨 {len(all_new)} new intern posting{'s' if len(all_new)>1 else ''} — {', '.join(companies)}{extra}",
        )

    alerts = check_health_alerts(health)
    if alerts:
        send_health_alert(alerts)

    save_state(ids, urls, health)
    log.info("done")


if __name__ == "__main__":
    import sys
    if "--once" in sys.argv:
        run_once()
    else:
        main()
