"""
CarAI v10.1 — Assistant CarEasy Benin
Moteur 100% autonome (sans Ollama)
Zero emoji dans toutes les reponses
Icons professionnels SVG via systeme de marqueurs [ICON:xxx]
Diagnostic automobile complet (12 pathologies)
Recherche robuste en cascade (8 niveaux)
Apprentissage non supervise persistant
"""

import os, json, re, math, httpx, time, hashlib, random
import unicodedata
import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List, Dict, Any, Tuple
from dotenv import load_dotenv
from datetime import datetime
from collections import defaultdict

load_dotenv()

LARAVEL_BASE    = os.getenv("LARAVEL_API_URL",  "https://careasy26.alwaysdata.net/api")
REDIS_URL       = os.getenv("REDIS_URL",        "redis://default:AZExAAIncDE1M2I2MDYzOGQyYzg0ZTNiOTNhYzg4OWU5MjUzZTlhYnAxMzcxNjk@on-turtle-37169.upstash.io:6379")
USE_NOMINATIM   = os.getenv("USE_NOMINATIM",    "true").lower() == "true"
GOOGLE_MAPS_KEY = os.getenv("GOOGLE_MAPS_KEY",  "")
SITE_URL        = os.getenv("FRONTEND_URL",     "https://careasy.vercel.app")
LEARN_FILE      = os.getenv("LEARN_FILE",       "/tmp/carai_learn_v101.json")
APP_VERSION     = "10.1.0"

app = FastAPI(title="CarAI v10.1", version=APP_VERSION, docs_url="/docs")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

redis_client: aioredis.Redis | None = None
_RAM: Dict[str, Dict] = defaultdict(lambda: {"history": [], "ctx": {}})
_GEO: Dict[str, Tuple[float, float]] = {}

_LEARN: Dict[str, Any] = {
    "pattern_scores":  {},
    "bad_patterns":    [],
    "good_patterns":   [],
    "intent_clusters": {},
    "faq_corrections": {},
    "query_stats":     {},
    "diag_stats":      {},
    "stats": {
        "total": 0, "fallback": 0,
        "db_hits": 0, "db_misses": 0,
        "feedback_pos": 0, "feedback_neg": 0,
        "diag_queries": 0,
    },
}


# ═══════════════════════════════════════════════════════════════════════════════
#  SYSTEME D'ICONS PROFESSIONNELS — SVG inline encodés en base64
#  Chaque icone est un SVG 16x16 propre, utilisable dans Flutter/HTML
#  Le champ icon_set est retourne dans chaque reponse pour rendu cote app
# ═══════════════════════════════════════════════════════════════════════════════

# Marqueurs textuels utilises dans les reponses (l'app les remplace par les SVG)
# Format: [ICON:nom] — l'application Flutter parse et remplace par l'icone SVG

ICON_MARKERS = {
    # Navigation et actions
    "search":      "[ICON:search]",
    "location":    "[ICON:location]",
    "phone":       "[ICON:phone]",
    "whatsapp":    "[ICON:whatsapp]",
    "map":         "[ICON:map]",
    "clock":       "[ICON:clock]",
    "price":       "[ICON:price]",
    "star":        "[ICON:star]",
    "info":        "[ICON:info]",
    "check":       "[ICON:check]",
    "warning":     "[ICON:warning]",
    "alert":       "[ICON:alert]",
    "wrench":      "[ICON:wrench]",
    "gear":        "[ICON:gear]",
    "car":         "[ICON:car]",
    "garage":      "[ICON:garage]",
    "battery":     "[ICON:battery]",
    "fuel":        "[ICON:fuel]",
    "tire":        "[ICON:tire]",
    "oil":         "[ICON:oil]",
    "brake":       "[ICON:brake]",
    "engine":      "[ICON:engine]",
    "ac":          "[ICON:ac]",
    "electric":    "[ICON:electric]",
    "calendar":    "[ICON:calendar]",
    "support":     "[ICON:support]",
    "account":     "[ICON:account]",
    "payment":     "[ICON:payment]",
    "distance":    "[ICON:distance]",
    "urgent":      "[ICON:urgent]",
    "ok":          "[ICON:ok]",
    "step":        "[ICON:step]",
}

# Catalogue SVG complet retourne via /icons pour utilisation dans Flutter
SVG_ICONS: Dict[str, str] = {
    "search": '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>',
    "location": '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0118 0z"/><circle cx="12" cy="10" r="3"/></svg>',
    "phone": '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 16.92v3a2 2 0 01-2.18 2 19.79 19.79 0 01-8.63-3.07A19.5 19.5 0 013.07 9.81a19.79 19.79 0 01-3.07-8.67A2 2 0 012 1h3a2 2 0 012 1.72c.127.96.361 1.903.7 2.81a2 2 0 01-.45 2.11L6.91 8.56a16 16 0 006.53 6.53l1.32-1.32a2 2 0 012.11-.45c.907.339 1.85.573 2.81.7A2 2 0 0122 16.92z"/></svg>',
    "whatsapp": '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 11.5a8.38 8.38 0 01-.9 3.8 8.5 8.5 0 01-7.6 4.7 8.38 8.38 0 01-3.8-.9L3 21l1.9-5.7a8.38 8.38 0 01-.9-3.8 8.5 8.5 0 014.7-7.6 8.38 8.38 0 013.8-.9h.5a8.48 8.48 0 018 8v.5z"/></svg>',
    "map": '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="3 6 9 3 15 6 21 3 21 18 15 21 9 18 3 21"/><line x1="9" y1="3" x2="9" y2="18"/><line x1="15" y1="6" x2="15" y2="21"/></svg>',
    "clock": '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>',
    "price": '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="1" x2="12" y2="23"/><path d="M17 5H9.5a3.5 3.5 0 000 7h5a3.5 3.5 0 010 7H6"/></svg>',
    "star": '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/></svg>',
    "info": '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>',
    "check": '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg>',
    "warning": '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>',
    "alert": '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#dc2626" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>',
    "wrench": '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14.7 6.3a1 1 0 000 1.4l1.6 1.6a1 1 0 001.4 0l3.77-3.77a6 6 0 01-7.94 7.94l-6.91 6.91a2.12 2.12 0 01-3-3l6.91-6.91a6 6 0 017.94-7.94l-3.76 3.76z"/></svg>',
    "gear": '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 00.33 1.82l.06.06a2 2 0 010 2.83 2 2 0 01-2.83 0l-.06-.06a1.65 1.65 0 00-1.82-.33 1.65 1.65 0 00-1 1.51V21a2 2 0 01-4 0v-.09A1.65 1.65 0 009 19.4a1.65 1.65 0 00-1.82.33l-.06.06a2 2 0 01-2.83-2.83l.06-.06A1.65 1.65 0 004.68 15a1.65 1.65 0 00-1.51-1H3a2 2 0 010-4h.09A1.65 1.65 0 004.6 9a1.65 1.65 0 00-.33-1.82l-.06-.06a2 2 0 012.83-2.83l.06.06A1.65 1.65 0 009 4.68a1.65 1.65 0 001-1.51V3a2 2 0 014 0v.09a1.65 1.65 0 001 1.51 1.65 1.65 0 001.82-.33l.06-.06a2 2 0 012.83 2.83l-.06.06A1.65 1.65 0 0019.4 9a1.65 1.65 0 001.51 1H21a2 2 0 010 4h-.09a1.65 1.65 0 00-1.51 1z"/></svg>',
    "car": '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 17H3a2 2 0 01-2-2V5a2 2 0 012-2h11l5 5v9a2 2 0 01-2 2h-3"/><circle cx="7.5" cy="17.5" r="2.5"/><circle cx="17.5" cy="17.5" r="2.5"/></svg>',
    "garage": '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 9l9-7 9 7v11a2 2 0 01-2 2H5a2 2 0 01-2-2z"/><polyline points="9 22 9 12 15 12 15 22"/></svg>',
    "battery": '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="1" y="6" width="18" height="12" rx="2" ry="2"/><line x1="23" y1="13" x2="23" y2="11"/></svg>',
    "fuel": '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="3" y1="22" x2="15" y2="22"/><line x1="4" y1="9" x2="14" y2="9"/><path d="M14 22V4a2 2 0 00-2-2H6a2 2 0 00-2 2v18"/><path d="M14 13h2a2 2 0 012 2v2a2 2 0 002 2 2 2 0 002-2V9.83a2 2 0 00-.59-1.42L18 5"/></svg>',
    "tire": '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><circle cx="12" cy="12" r="3"/></svg>',
    "oil": '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 14.76V3.5a2.5 2.5 0 00-5 0v11.26a4.5 4.5 0 105 0z"/></svg>',
    "brake": '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><circle cx="12" cy="12" r="6"/><circle cx="12" cy="12" r="2"/></svg>',
    "engine": '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="9" width="18" height="10" rx="2"/><path d="M8 9V5h8v4"/><line x1="8" y1="14" x2="8" y2="14"/><line x1="16" y1="14" x2="16" y2="14"/></svg>',
    "ac": '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M8 6h13M8 12h13M8 18h13M3 6h.01M3 12h.01M3 18h.01"/></svg>',
    "electric": '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg>',
    "calendar": '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="4" width="18" height="18" rx="2" ry="2"/><line x1="16" y1="2" x2="16" y2="6"/><line x1="8" y1="2" x2="8" y2="6"/><line x1="3" y1="10" x2="21" y2="10"/></svg>',
    "support": '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 16.92v3a2 2 0 01-2.18 2 19.79 19.79 0 01-8.63-3.07A19.5 19.5 0 013.07 9.81 19.79 19.79 0 01.98 1.18a2 2 0 011.45-.9A2 2 0 013.42.11l3 .78a2 2 0 011.4 1.64l.73 3.64a2 2 0 01-.49 1.73l-1.32 1.32a16 16 0 006.53 6.53l1.32-1.32a2 2 0 011.73-.49l3.64.73A2 2 0 0122 16.92z"/><path d="M14.05 2a9 9 0 018 7.94"/><path d="M14.05 6A5 5 0 0118 10"/></svg>',
    "account": '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M20 21v-2a4 4 0 00-4-4H8a4 4 0 00-4 4v2"/><circle cx="12" cy="7" r="4"/></svg>',
    "payment": '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="1" y="4" width="22" height="16" rx="2" ry="2"/><line x1="1" y1="10" x2="23" y2="10"/></svg>',
    "distance": '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 12h18M12 3l9 9-9 9"/></svg>',
    "urgent": '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#dc2626" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>',
    "ok": '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#16a34a" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 11-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg>',
    "step": '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="9 18 15 12 9 6"/></svg>',
}


def strip_emoji(text: str) -> str:
    """Supprime tout emoji Unicode du texte."""
    emoji_pattern = re.compile(
        "["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map
        u"\U0001F1E0-\U0001F1FF"  # flags
        u"\U00002702-\U000027B0"
        u"\U000024C2-\U0001F251"
        u"\U0001f926-\U0001f937"
        u"\U00010000-\U0010ffff"
        u"\u2640-\u2642"
        u"\u2600-\u2B55"
        u"\u200d"
        u"\u23cf"
        u"\u23e9"
        u"\u231a"
        u"\ufe0f"
        u"\u3030"
        "]+",
        flags=re.UNICODE,
    )
    return emoji_pattern.sub("", text).strip()


# ═══════════════════════════════════════════════════════════════════════════════
#  MODELES PYDANTIC
# ═══════════════════════════════════════════════════════════════════════════════

class ChatRequest(BaseModel):
    message:         str
    conversation_id: str
    user_id:         Optional[int]   = None
    latitude:        Optional[float] = None
    longitude:       Optional[float] = None
    language:        Optional[str]   = None

class ChatResponse(BaseModel):
    reply:       str
    services:    List[Dict[str, Any]] = []
    map_url:     Optional[str]        = None
    itinerary:   Optional[Dict]       = None
    intent:      Optional[str]        = None
    language:    Optional[str]        = None
    suggestions: List[str]            = []
    confidence:  float                = 1.0
    diagnostic:  Optional[Dict]       = None
    icons:       Dict[str, str]       = {}   # catalogue SVG retourne avec chaque reponse

class FeedbackRequest(BaseModel):
    conversation_id: str
    message_text:    str
    reply_text:      str
    rating:          int
    comment:         Optional[str] = None

class DiagRequest(BaseModel):
    symptoms:  List[str]
    vehicle:   Optional[str] = None
    mileage:   Optional[int] = None
    year:      Optional[int] = None


# ═══════════════════════════════════════════════════════════════════════════════
#  DEMARRAGE / ARRET
# ═══════════════════════════════════════════════════════════════════════════════

@app.on_event("startup")
async def startup():
    global redis_client
    try:
        redis_client = aioredis.from_url(
            REDIS_URL, encoding="utf-8", decode_responses=True
        )
        await redis_client.ping()
        print("[CarAI] Redis connecte")
    except Exception as e:
        print(f"[CarAI] Redis KO ({e}) — RAM actif")
        redis_client = None

    _load_learn()
    print(f"[CarAI] v{APP_VERSION} | {LARAVEL_BASE} | Mode autonome — zero emoji — icons SVG")

    try:
        async with httpx.AsyncClient(timeout=8) as c:
            r = await c.get(f"{LARAVEL_BASE}/ai/domaines")
            if r.status_code == 200:
                nb = len(r.json().get("data", []))
                print(f"[CarAI] Laravel OK — {nb} domaines")
    except Exception as e:
        print(f"[CarAI] ATTENTION: Laravel inaccessible: {e}")

    try:
        async with httpx.AsyncClient(timeout=8) as c:
            r = await c.get(
                f"{LARAVEL_BASE}/ai/services/nearby",
                params={"lat": 6.3654, "lng": 2.4183, "radius": 50, "limit": 3}
            )
            if r.status_code == 200:
                nb = len(r.json().get("data", []))
                print(f"[CarAI] BDD OK — {nb} services pres de Cotonou")
    except Exception as e:
        print(f"[CarAI] ATTENTION: services/nearby inaccessible: {e}")


@app.on_event("shutdown")
async def shutdown():
    _save_learn()


# ═══════════════════════════════════════════════════════════════════════════════
#  APPRENTISSAGE NON SUPERVISE
# ═══════════════════════════════════════════════════════════════════════════════

def _load_learn():
    global _LEARN
    try:
        if os.path.exists(LEARN_FILE):
            with open(LEARN_FILE, "r", encoding="utf-8") as f:
                saved = json.load(f)
                _LEARN.update(saved)
            print(f"[Learn] {len(_LEARN['pattern_scores'])} patterns, "
                  f"{len(_LEARN['faq_corrections'])} corrections")
    except Exception as e:
        print(f"[Learn] Chargement ignore: {e}")


def _save_learn():
    try:
        with open(LEARN_FILE, "w", encoding="utf-8") as f:
            json.dump(_LEARN, f, ensure_ascii=False, indent=2, default=str)
    except Exception as e:
        print(f"[Learn] Sauvegarde echouee: {e}")


def _h(text: str) -> str:
    return hashlib.md5(_normalize(text).encode()).hexdigest()[:12]


def _score_update(message: str, reply: str, score: int):
    key = _h(message)
    p   = _LEARN["pattern_scores"].setdefault(key, {"score": 0.0, "count": 0, "examples": []})
    p["score"] = (p["score"] * p["count"] + score) / (p["count"] + 1)
    p["count"] += 1
    if len(p["examples"]) < 5:
        p["examples"].append(message[:100])
    if score <= 2 and message[:80] not in _LEARN["bad_patterns"]:
        _LEARN["bad_patterns"].append(message[:80])
        if len(_LEARN["bad_patterns"]) > 300:
            _LEARN["bad_patterns"] = _LEARN["bad_patterns"][-300:]
    elif score >= 4 and message[:80] not in _LEARN["good_patterns"]:
        _LEARN["good_patterns"].append(message[:80])
    _save_learn()


def _cluster_intent(intent: str, message: str):
    cluster = _LEARN["intent_clusters"].setdefault(intent, [])
    if message[:60] not in cluster:
        cluster.append(message[:60])
    if len(cluster) > 100:
        _LEARN["intent_clusters"][intent] = cluster[-100:]


def _track_query(domaine: Optional[str], location: Optional[str], found: int):
    key = f"{domaine or 'any'}|{location or 'any'}"
    q   = _LEARN["query_stats"].setdefault(key, {"count": 0, "hits": 0})
    q["count"] += 1
    if found > 0:
        q["hits"] += 1
    if _LEARN["stats"]["total"] % 50 == 0:
        _save_learn()


def _correction(message: str) -> Optional[str]:
    return _LEARN["faq_corrections"].get(_h(message))


def _confidence(message: str, intent: str) -> float:
    key = _h(message)
    if key in _LEARN["pattern_scores"]:
        return max(0.1, min(1.0, _LEARN["pattern_scores"][key]["score"] / 5.0))
    n = len(_LEARN["intent_clusters"].get(intent, []))
    return 0.9 if n > 20 else (0.8 if n > 10 else 0.75)


# ═══════════════════════════════════════════════════════════════════════════════
#  UTILITAIRES TEXTE
# ═══════════════════════════════════════════════════════════════════════════════

def _normalize(text: str) -> str:
    """Normalise le texte : minuscules, sans accents, sans caracteres speciaux."""
    text = unicodedata.normalize("NFC", text)
    # Normaliser les apostrophes typographiques
    text = text.replace("\u2019", "'").replace("\u2018", "'") \
               .replace("\u02BC", "'").replace("\u0060", "'") \
               .replace("\u00B4", "'")
    text = text.replace("\u2013", "-").replace("\u2014", "-")
    nfkd = unicodedata.normalize("NFKD", text)
    text_no_accent = "".join(c for c in nfkd if not unicodedata.combining(c))
    return text_no_accent.lower()

# Alias pour compatibilite
normalize_text = _normalize


def _clean_reply(text: str) -> str:
    """
    Nettoie le texte de reponse :
    - Supprime tous les emojis
    - Remplace localhost par le vrai URL
    - Normalise les espaces
    """
    text = strip_emoji(text)
    text = re.sub(r"http://localhost[^\s]*", SITE_URL, text)
    text = re.sub(r"localhost:\d+", SITE_URL, text)
    # Supprimer les lignes vides multiples
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


# ═══════════════════════════════════════════════════════════════════════════════
#  BASE DE DIAGNOSTIC AUTOMOBILE
# ═══════════════════════════════════════════════════════════════════════════════

DIAG_SYMPTOMS: Dict[str, Dict] = {
    "moteur_ne_demarre_pas": {
        "keywords": [
            "ne demarre pas", "ne demarr", "ne veut pas demarrer", "voiture ne start",
            "moteur mort", "clic clic", "rien quand je tourne la cle",
            "le demarreur tourne mais", "demarre pas", "start pas",
            "voiture demarre pas", "moteur ne tourne pas",
        ],
        "titre":  "Moteur ne demarre pas",
        "icon":   "engine",
        "urgence": "haute",
        "causes_probables": [
            {"cause": "Batterie decharge ou defectueuse", "probabilite": 60, "icon": "battery"},
            {"cause": "Demarreur defaillant",             "probabilite": 20, "icon": "gear"},
            {"cause": "Injection ou carburant vide",      "probabilite": 12, "icon": "fuel"},
            {"cause": "Calage de la distribution",        "probabilite": 5,  "icon": "wrench"},
            {"cause": "Immobiliseur / cle non reconnue",  "probabilite": 3,  "icon": "gear"},
        ],
        "diagnostic_rapide": [
            "Les phares s'allument-ils normalement ?",
            "Entendez-vous un clic-clic ou silence total au demarrage ?",
            "La derniere recharge de batterie date de quand ?",
            "Y a-t-il du carburant dans le reservoir ?",
        ],
        "actions_immediates": [
            "Verifier la tension de batterie (doit etre superieure a 12,4V)",
            "Essayer un demarrage en poussette ou cables de demarrage",
            "Verifier le niveau de carburant",
            "Verifier les bornes de batterie (oxydation blanche)",
        ],
        "domaine_recommande": "Garage mecanique",
        "cout_estimatif":     "5 000 - 150 000 FCFA selon la cause",
        "delai_recommande":   "Immediat",
    },

    "surchauffe_moteur": {
        "keywords": [
            "temperature monte", "surchauffe", "moteur chauffe", "jauge temperature rouge",
            "vapeur moteur", "fumee blanche moteur", "radiateur chauffe",
            "voyant temperature", "moteur en surchauffe", "thermometre monte",
            "le moteur chauffe", "voiture chauffe",
        ],
        "titre":  "Surchauffe moteur",
        "icon":   "engine",
        "urgence": "critique",
        "causes_probables": [
            {"cause": "Niveau de liquide de refroidissement bas", "probabilite": 40, "icon": "oil"},
            {"cause": "Thermostat bloque ferme",                  "probabilite": 20, "icon": "gear"},
            {"cause": "Fuite du circuit de refroidissement",      "probabilite": 18, "icon": "warning"},
            {"cause": "Pompe a eau defaillante",                  "probabilite": 12, "icon": "wrench"},
            {"cause": "Joint de culasse perce",                   "probabilite": 10, "icon": "alert"},
        ],
        "diagnostic_rapide": [
            "Y a-t-il de la fumee blanche sortant du capot ou du pot d'echappement ?",
            "Le niveau de liquide de refroidissement est-il bas ?",
            "Y a-t-il des traces de fuite sous la voiture ?",
            "Le ventilateur de refroidissement fonctionne-t-il ?",
        ],
        "actions_immediates": [
            "ARRET IMMEDIAT du vehicule — continuer endommage le moteur definitivement",
            "NE PAS ouvrir le bouchon du radiateur a chaud — risque de brulure grave",
            "Attendre refroidissement complet : 20 a 30 minutes minimum",
            "Appeler un depanneur si fuite visible sous la voiture",
        ],
        "domaine_recommande": "Garage mecanique",
        "cout_estimatif":     "15 000 - 500 000 FCFA (joint de culasse = cout eleve)",
        "delai_recommande":   "Immediat — ne pas rouler",
    },

    "consommation_huile_excessive": {
        "keywords": [
            "huile diminue", "perd de l huile", "fumee bleue", "fumee noire",
            "consomme huile", "voyant huile allume", "niveau huile bas",
            "huile moteur baisse", "manque huile", "brule huile",
            "fuite huile", "tache huile sol",
        ],
        "titre":  "Consommation d'huile excessive / voyant huile",
        "icon":   "oil",
        "urgence": "haute",
        "causes_probables": [
            {"cause": "Segments pistons uses (fumee bleue)",       "probabilite": 35, "icon": "wrench"},
            {"cause": "Joints de soupapes defaillants",           "probabilite": 28, "icon": "wrench"},
            {"cause": "Fuite externe (joints, couvre-culasse)",   "probabilite": 22, "icon": "warning"},
            {"cause": "Vidange depassee / huile degradee",        "probabilite": 10, "icon": "oil"},
            {"cause": "Niveau bas sans fuite (vidange necessaire)", "probabilite": 5, "icon": "info"},
        ],
        "diagnostic_rapide": [
            "Y a-t-il de la fumee bleue au demarrage a froid ?",
            "Des traces d'huile sous le vehicule ?",
            "Quand a ete faite la derniere vidange ?",
            "Le voyant huile rouge est-il allume en roulant ?",
        ],
        "actions_immediates": [
            "Verifier le niveau d'huile immediatement avec la jauge",
            "Si niveau tres bas : ARRET et appoint avant de rouler",
            "Verifier les traces de fuite sous le moteur",
            "Programmer une vidange si compteur depasse 10 000 km",
        ],
        "domaine_recommande": "Changement d'huile",
        "cout_estimatif":     "8 000 - 200 000 FCFA",
        "delai_recommande":   "Cette semaine",
    },

    "frein_probleme": {
        "keywords": [
            "frein dur", "frein mou", "frein grince", "frein crie", "pedal frein",
            "pedale s enfonce", "voiture ne freine pas", "frein chauffe",
            "bruit frein", "freinage mauvais", "freins uses",
            "plaquettes", "disque frein", "freins", "pedale frein molle",
            "abs", "esp freinage",
        ],
        "titre":  "Probleme de freinage",
        "icon":   "brake",
        "urgence": "critique",
        "causes_probables": [
            {"cause": "Plaquettes de frein usees",              "probabilite": 40, "icon": "wrench"},
            {"cause": "Disques de frein uses ou voiles",        "probabilite": 22, "icon": "gear"},
            {"cause": "Fuite de liquide de frein",              "probabilite": 18, "icon": "warning"},
            {"cause": "Liquide de frein degrade (ebullition)",  "probabilite": 10, "icon": "oil"},
            {"cause": "Etrier de frein bloque",                 "probabilite": 10, "icon": "wrench"},
        ],
        "diagnostic_rapide": [
            "La pedale s'enfonce-t-elle progressivement (pedale molle) ?",
            "Y a-t-il un bruit de grincement ou couinement au freinage ?",
            "La voiture tire-t-elle d'un cote lors du freinage ?",
            "Le voyant ABS ou frein est-il allume ?",
        ],
        "actions_immediates": [
            "SECURITE CRITIQUE — reduire la vitesse et eviter les autoroutes",
            "Verifier le niveau de liquide de frein dans le reservoir transparent",
            "Si pedale molle : ne pas rouler — appeler un depanneur",
            "Eviter les descentes prolongees sans utiliser le frein moteur",
        ],
        "domaine_recommande": "Garage mecanique",
        "cout_estimatif":     "20 000 - 150 000 FCFA",
        "delai_recommande":   "Immediat — securite prioritaire",
    },

    "pneu_probleme": {
        "keywords": [
            "pneu crev", "creve", "roue crevee", "pneu plat", "pneu gonfle",
            "pression pneu", "pneu user", "bande pneu", "vibration pneu",
            "voiture penche", "pneu eclate", "valve pneu",
            "crevaison", "pneu a plat",
        ],
        "titre":  "Probleme de pneumatiques",
        "icon":   "tire",
        "urgence": "variable",
        "causes_probables": [
            {"cause": "Crevaison simple (clou, vis)",           "probabilite": 50, "icon": "warning"},
            {"cause": "Valve defaillante (perte lente)",        "probabilite": 20, "icon": "gear"},
            {"cause": "Pneu use / bande de roulement lisse",   "probabilite": 18, "icon": "wrench"},
            {"cause": "Jante abimee (bosse, voile)",           "probabilite": 7,  "icon": "wrench"},
            {"cause": "Crevaison interne (flanc endommage)",   "probabilite": 5,  "icon": "alert"},
        ],
        "diagnostic_rapide": [
            "Le pneu est-il completement a plat ou se degonfle-t-il progressivement ?",
            "Y a-t-il un clou ou un objet visible dans le pneu ?",
            "La crevaison est-elle sur le flanc ou la bande centrale ?",
            "La roue de secours est-elle disponible et gonflee ?",
        ],
        "actions_immediates": [
            "S'arreter en securite hors de la chaussee — allumer les feux de detresse",
            "Monter la roue de secours si disponible et gonflee",
            "Appeler un vulcanisateur si pas de roue de secours",
            "Ne jamais rouler sur un pneu completement a plat — abime la jante",
        ],
        "domaine_recommande": "Pneumatique / vulcanisation",
        "cout_estimatif":     "1 000 - 50 000 FCFA (reparation ou remplacement)",
        "delai_recommande":   "Immediat",
    },

    "batterie_probleme": {
        "keywords": [
            "batterie", "batterie faible", "batterie morte", "batterie decharge",
            "phares faibles", "alarme sonne", "radio s eteint", "alternateur",
            "voyant batterie", "batterie vide", "charge batterie",
            "voiture demarrage difficile", "moteur tourne lentement",
        ],
        "titre":  "Probleme electrique / batterie",
        "icon":   "battery",
        "urgence": "moyenne",
        "causes_probables": [
            {"cause": "Batterie decharge (anciennete sup a 3 ans)", "probabilite": 45, "icon": "battery"},
            {"cause": "Alternateur defaillant (ne recharge pas)",   "probabilite": 28, "icon": "gear"},
            {"cause": "Consommateur parasite (lumiere restee allumee)", "probabilite": 15, "icon": "info"},
            {"cause": "Mauvaise connexion des bornes",             "probabilite": 7,  "icon": "wrench"},
            {"cause": "Regulateur de tension defaillant",          "probabilite": 5,  "icon": "gear"},
        ],
        "diagnostic_rapide": [
            "La batterie a-t-elle plus de 3 ans ?",
            "Le voyant batterie rouge est-il allume en roulant ?",
            "Les phares brillent-ils normalement moteur en marche ?",
            "Le demarrage est-il difficile (moteur tourne lentement) ?",
        ],
        "actions_immediates": [
            "Tester la tension batterie : bon etat = 12,6V a vide, 13,5 a 14,5V moteur tourne",
            "Verifier l'etat des bornes — pas d'oxydation blanche",
            "Si alternateur suspect : ne pas couper le moteur avant garage",
            "Prevoir remplacement si batterie a plus de 4 ans",
        ],
        "domaine_recommande": "Electricien auto",
        "cout_estimatif":     "15 000 - 80 000 FCFA (batterie seule)",
        "delai_recommande":   "Cette semaine",
    },

    "bruit_suspect": {
        "keywords": [
            "bruit bizarre", "bruit moteur", "bruit cliquetis", "cognement",
            "toc toc moteur", "sifflement", "grondement", "vibration voiture",
            "bruit au virage", "craquement", "bruit suspension", "bruit en roulant",
            "bruit direction", "bruit bizarre voiture", "bruit etrange",
            "son bizarre", "claquement voiture",
        ],
        "titre":  "Bruit suspect / anomalie sonore",
        "icon":   "wrench",
        "urgence": "moyenne",
        "causes_probables": [
            {"cause": "Roulement de roue use (grondement en virage)", "probabilite": 30, "icon": "gear"},
            {"cause": "Cardans defaillants (cliquetis en virage)",   "probabilite": 22, "icon": "gear"},
            {"cause": "Silentblocs / amortisseurs uses",             "probabilite": 20, "icon": "wrench"},
            {"cause": "Courroie de distribution usee (cliquetis moteur)", "probabilite": 15, "icon": "alert"},
            {"cause": "Echappement perce (sifflement ou grondement)", "probabilite": 13, "icon": "wrench"},
        ],
        "diagnostic_rapide": [
            "Le bruit apparait-il surtout en virage, en freinage ou en permanence ?",
            "Est-ce un cliquetis metallique, un grondement sourd ou un sifflement ?",
            "Le bruit augmente-t-il avec la vitesse ?",
            "Est-ce recent ou progressif depuis plusieurs semaines ?",
        ],
        "actions_immediates": [
            "Un cliquetis metallique rapide = URGENT — distribution ou moteur en danger",
            "Un grondement en virage = roulement a surveiller — risque de blocage de roue",
            "Eviter les longs trajets avant diagnostic professionnel",
            "Faire lire les codes OBD pour identifier l'origine electronique",
        ],
        "domaine_recommande": "Diagnostic automobile",
        "cout_estimatif":     "5 000 - 300 000 FCFA selon l'origine",
        "delai_recommande":   "Dans la semaine",
    },

    "voyant_allume": {
        "keywords": [
            "voyant allume", "voyant rouge", "voyant orange", "check engine",
            "voyant moteur", "tableau de bord allume", "lampe allumee tableau",
            "warning allume", "code erreur", "obd", "scanner",
            "voyant jaune", "voyant tableau bord", "lampe rouge allumee",
        ],
        "titre":  "Voyant(s) allume(s) tableau de bord",
        "icon":   "warning",
        "urgence": "variable",
        "causes_probables": [
            {"cause": "Capteur defaillant (O2, MAF, temperature)", "probabilite": 35, "icon": "gear"},
            {"cause": "Probleme systeme depollution (FAP, catalyseur)", "probabilite": 25, "icon": "engine"},
            {"cause": "Pression huile ou temperature anormale",   "probabilite": 18, "icon": "warning"},
            {"cause": "Probleme systeme de freinage (ABS / ESP)", "probabilite": 12, "icon": "brake"},
            {"cause": "Defaut electrique mineur (contacteur, capteur)", "probabilite": 10, "icon": "electric"},
        ],
        "diagnostic_rapide": [
            "Le voyant est-il ROUGE (urgence) ou ORANGE / JAUNE (avertissement) ?",
            "Plusieurs voyants sont-ils allumes simultanement ?",
            "Y a-t-il une perte de puissance moteur associee ?",
            "Le voyant clignote-t-il ou est-il fixe ?",
        ],
        "actions_immediates": [
            "ROUGE fixe ou clignotant = arret immediat recommande — ne pas forcer",
            "ORANGE / JAUNE fixe = diagnostic professionnel dans les 48 heures",
            "Ne pas effacer les codes erreur avant le diagnostic — perte d'information",
            "Faire lire les codes OBD par un professionnel avec valise de diagnostic",
        ],
        "domaine_recommande": "Diagnostic automobile",
        "cout_estimatif":     "3 000 - 200 000 FCFA selon le defaut identifie",
        "delai_recommande":   "Selon couleur du voyant",
    },

    "climatisation_probleme": {
        "keywords": [
            "clim ne refroidit pas", "clim ne fonctionne pas", "clim chaude",
            "air conditionne ne marche pas", "recharger clim", "gaz clim",
            "clim souffle chaud", "mauvaise climatisation", "clim inefficace",
            "odeur clim", "clim fait du bruit", "climatisation en panne",
        ],
        "titre":  "Climatisation defaillante",
        "icon":   "ac",
        "urgence": "basse",
        "causes_probables": [
            {"cause": "Gaz refrigerant insuffisant (fuite ou recharge necessaire)", "probabilite": 50, "icon": "ac"},
            {"cause": "Condenseur encrase ou endommage",    "probabilite": 20, "icon": "wrench"},
            {"cause": "Compresseur defaillant",             "probabilite": 15, "icon": "gear"},
            {"cause": "Filtre habitacle encrase",           "probabilite": 10, "icon": "wrench"},
            {"cause": "Resistance de ventilateur defaillante", "probabilite": 5, "icon": "electric"},
        ],
        "diagnostic_rapide": [
            "La clim souffle-t-elle de l'air mais pas froid ?",
            "Y a-t-il un sifflement ou bruit inhabituel quand la clim est allumee ?",
            "Des mauvaises odeurs a la mise en route de la clim ?",
            "Quand la clim a-t-elle ete rechargee pour la derniere fois ?",
        ],
        "actions_immediates": [
            "Verifier que le compresseur s'enclenche — ecouter le clic de l'embrayage magnetique",
            "Inspecter le condenseur devant le radiateur — nettoyage si encrase",
            "Changer le filtre d'habitacle si plus de 15 000 km ou plus d'un an",
            "Recharge de gaz recommandee tous les 2 ans en utilisation normale",
        ],
        "domaine_recommande": "Climatisation auto",
        "cout_estimatif":     "10 000 - 120 000 FCFA",
        "delai_recommande":   "Cette semaine",
    },

    "panne_electrique": {
        "keywords": [
            "phare ne marche pas", "clignotant", "essuie glace", "vitres electriques",
            "verrouillage porte", "centrale clignotant", "fusible grille",
            "court circuit", "tableau de bord eteint", "calculateur",
            "probleme electrique voiture", "prise obd", "panne electrique",
        ],
        "titre":  "Panne electrique",
        "icon":   "electric",
        "urgence": "variable",
        "causes_probables": [
            {"cause": "Fusible grille",                        "probabilite": 40, "icon": "electric"},
            {"cause": "Relais defaillant",                     "probabilite": 25, "icon": "gear"},
            {"cause": "Cablage endommage (rongeurs, humidite)", "probabilite": 20, "icon": "wrench"},
            {"cause": "Calculateur defaillant",                "probabilite": 10, "icon": "gear"},
            {"cause": "Masse carrosserie desserree",           "probabilite": 5,  "icon": "wrench"},
        ],
        "diagnostic_rapide": [
            "L'equipement defaillant est-il seul ou plusieurs en meme temps ?",
            "Le probleme est-il intermittent ou permanent ?",
            "Y a-t-il eu recemment de l'eau dans l'habitacle ?",
            "Y a-t-il une odeur de brule dans la voiture ?",
        ],
        "actions_immediates": [
            "Verifier en premier le coffret a fusibles — capot moteur et habitacle",
            "Identifier le fusible correspondant dans le guide utilisateur du vehicule",
            "Ne jamais remplacer un fusible par un amperage superieur au calibre d'origine",
            "Si odeur de brule : deconnecter la batterie et appeler un electricien auto",
        ],
        "domaine_recommande": "Electricien auto",
        "cout_estimatif":     "2 000 - 150 000 FCFA",
        "delai_recommande":   "Selon gravite",
    },

    "transmission_probleme": {
        "keywords": [
            "boite vitesse", "vitesse ne passe pas", "rapport dur", "boite automatique",
            "transmission", "embrayage patine", "embrayage dur", "glisse embrayage",
            "pedale embrayage", "marche arriere", "boite de vitesse",
            "passage vitesse difficile", "rapport bloque",
        ],
        "titre":  "Probleme boite de vitesses / transmission",
        "icon":   "gear",
        "urgence": "haute",
        "causes_probables": [
            {"cause": "Embrayage use (patinage, prise haute)",    "probabilite": 40, "icon": "gear"},
            {"cause": "Cable ou tringlerie de boite mal reglee",  "probabilite": 20, "icon": "wrench"},
            {"cause": "Huile de boite insuffisante ou degradee",  "probabilite": 18, "icon": "oil"},
            {"cause": "Synchroniseurs uses (passages durs)",      "probabilite": 14, "icon": "gear"},
            {"cause": "Boite automatique : capteur ou solenoide", "probabilite": 8,  "icon": "electric"},
        ],
        "diagnostic_rapide": [
            "Le probleme est-il au passage de rapport ou a la prise de mouvement ?",
            "Y a-t-il une odeur de brule lors de la montee en cote ?",
            "Le regime moteur monte-t-il sans que la vitesse augmente ?",
            "La pedale d'embrayage est-elle molle ou tres haute ?",
        ],
        "actions_immediates": [
            "Eviter les cotes abruptes si l'embrayage patine — aggrave l'usure",
            "Verifier le niveau d'huile de boite sous le capot ou sous le vehicule",
            "Ne pas forcer les passages de rapport — endommage les synchroniseurs",
            "Diagnostic professionnel recommande avant aggravation",
        ],
        "domaine_recommande": "Garage mecanique",
        "cout_estimatif":     "50 000 - 400 000 FCFA",
        "delai_recommande":   "Dans la semaine",
    },

    "direction_suspension": {
        "keywords": [
            "direction dure", "volant tremble", "volant tire", "alignement",
            "geometrie", "amortisseur", "suspension", "voiture oscille",
            "tient mal la route", "craquement volant", "pneu mange",
            "usure inegale pneu", "direction assistee",
            "voiture tire a droite", "voiture tire a gauche",
        ],
        "titre":  "Direction et suspension",
        "icon":   "gear",
        "urgence": "moyenne",
        "causes_probables": [
            {"cause": "Amortisseurs uses (oscillation, bruit)",   "probabilite": 35, "icon": "wrench"},
            {"cause": "Parallelisme / geometrie deregle",         "probabilite": 28, "icon": "car"},
            {"cause": "Rotule de direction usee",                 "probabilite": 18, "icon": "gear"},
            {"cause": "Direction assistee defaillante (huile, pompe)", "probabilite": 12, "icon": "oil"},
            {"cause": "Triangle de suspension endommage",         "probabilite": 7,  "icon": "wrench"},
        ],
        "diagnostic_rapide": [
            "La voiture tire-t-elle d'un cote sur route droite ?",
            "Y a-t-il des vibrations dans le volant a partir de 80 km/h ?",
            "Les pneus s'usent-ils de maniere inegale d'un cote seulement ?",
            "Y a-t-il des craquements en tournant a basse vitesse ?",
        ],
        "actions_immediates": [
            "Verifier le niveau de liquide de direction assistee",
            "Inspecter les pneus pour detecter une usure asymetrique",
            "Verifier que les roues ne vibrent pas en les secouant a la main (voiture levee)",
            "Realiser un parallelisme apres tout changement de pneus ou choc sur le train avant",
        ],
        "domaine_recommande": "Garage mecanique",
        "cout_estimatif":     "15 000 - 200 000 FCFA",
        "delai_recommande":   "Dans la semaine",
    },
}

# Index de recherche rapide par mots-cles
DIAG_KEYWORD_INDEX: Dict[str, str] = {}
for _code, _data in DIAG_SYMPTOMS.items():
    for _kw in _data["keywords"]:
        DIAG_KEYWORD_INDEX[_normalize(_kw)] = _code


def detect_diagnostic_intent(text: str) -> Optional[str]:
    """Retourne le code de diagnostic si la question porte sur une panne auto."""
    t = _normalize(text)
    best_code, best_score = None, 0

    for kw in sorted(DIAG_KEYWORD_INDEX.keys(), key=len, reverse=True):
        if kw in t:
            code  = DIAG_KEYWORD_INDEX[kw]
            score = len(kw)
            if score > best_score:
                best_score = score
                best_code  = code

    DIAG_TRIGGERS = [
        "probleme", "panne", "ne fonctionne", "ne marche", "grince", "vibre",
        "clignote", "allume", "fuit", "fume", "chauffe", "bruit", "voyant",
        "pourquoi ma", "pourquoi mon", "diagnostiquer", "qu est ce qui",
        "que se passe", "diagnosis", "symptome", "defaut", "anomalie",
        "ma voiture", "mon vehicule", "ma moto",
    ]
    is_diag_context = any(trigger in t for trigger in DIAG_TRIGGERS)

    if best_code and (best_score >= 4 or is_diag_context):
        return best_code
    return None


def build_diagnostic_response(
    diag_code: str,
    services:  List[Dict],
    location:  Optional[str],
    ulat:      Optional[float],
) -> Tuple[str, Dict]:
    """Construit une reponse de diagnostic complete sans aucun emoji."""
    diag = DIAG_SYMPTOMS[diag_code]
    _LEARN["stats"]["diag_queries"] += 1
    _LEARN["diag_stats"][diag_code] = _LEARN["diag_stats"].get(diag_code, 0) + 1

    urgence_labels = {
        "critique": "CRITIQUE — ne pas rouler",
        "haute":    "HAUTE — intervention rapide",
        "moyenne":  "MOYENNE — a surveiller",
        "basse":    "NON URGENTE — entretien preventif",
        "variable": "VARIABLE — depend de la couleur du voyant",
    }
    urgence_label = urgence_labels.get(diag["urgence"], "A evaluer")

    lines = []
    lines.append(f"Diagnostic : {diag['titre']}")
    lines.append(f"Urgence : {urgence_label}")
    lines.append("")

    lines.append("Causes probables :")
    for i, cause in enumerate(diag["causes_probables"][:3], 1):
        pct  = cause["probabilite"]
        bar  = "|" * (pct // 10) + "." * (10 - pct // 10)
        lines.append(f"  {i}. {cause['cause']} ({pct}%)")
        lines.append(f"     [{bar}]")
    lines.append("")

    lines.append("Questions pour affiner le diagnostic :")
    for i, q in enumerate(diag["diagnostic_rapide"][:3], 1):
        lines.append(f"  Q{i}. {q}")
    lines.append("")

    lines.append("Actions immediates :")
    for action in diag["actions_immediates"][:3]:
        lines.append(f"  - {action}")
    lines.append("")

    lines.append(f"Cout estimatif : {diag['cout_estimatif']}")
    lines.append(f"Delai recommande : {diag['delai_recommande']}")

    domaine_rec = diag["domaine_recommande"]
    if services:
        lieu = f"a {location}" if location else ("pres de vous" if ulat else "au Benin")
        lines.append(f"\nPrestataires recommandes en {domaine_rec} {lieu} :")
        for i, s in enumerate(services[:3], 1):
            e    = s.get("entreprise", {}) or {}
            dist = s.get("distance_km")
            dst  = f" ({dist:.1f} km)" if dist is not None else ""
            ph   = e.get("call_phone") or "—"
            wa   = e.get("whatsapp_phone") or "—"
            lines.append(f"  {i}. {e.get('name', 'Inconnu')}{dst}")
            lines.append(f"     Tel : {ph}  |  WhatsApp : {wa}")
    else:
        lines.append(f"\nRecherchez un specialiste en : {domaine_rec}")
        lines.append("Utilisez CarEasy pour trouver le plus proche de vous.")

    diag_data = {
        "code":               diag_code,
        "titre":              diag["titre"],
        "urgence":            diag["urgence"],
        "urgence_label":      urgence_label,
        "icon":               diag["icon"],
        "causes_probables":   diag["causes_probables"][:3],
        "actions_immediates": diag["actions_immediates"][:3],
        "questions":          diag["diagnostic_rapide"][:3],
        "domaine_recommande": domaine_rec,
        "cout_estimatif":     diag["cout_estimatif"],
        "delai_recommande":   diag["delai_recommande"],
    }

    return _clean_reply("\n".join(lines)), diag_data


# ═══════════════════════════════════════════════════════════════════════════════
#  DOMAINES — mapping etendu avec variantes sans accent
# ═══════════════════════════════════════════════════════════════════════════════

DOMAINES: Dict[str, List[str]] = {
    "Pneumatique / vulcanisation": [
        "pneu", "pneus", "vulcanis", "crevaison", "creve", "roue crevee",
        "chambre a air", "pneumatique", "gomme", "tubeless",
        "pneu creve", "pneu plat",
    ],
    "Garage mecanique": [
        "garage", "mecanicien", "mecani", "reparer", "reparation", "panne",
        "moteur", "boite vitesse", "frein", "embrayage", "courroie",
        "revision", "entretien", "controle", "mechanic", "repair",
        "voiture en panne", "ma voiture", "mon vehicule", "mecanique",
        "garage auto", "atelier",
    ],
    "Lavage automobile": [
        "lavage", "carwash", "car wash", "nettoyage voiture",
        "polissage", "lustrage", "laver voiture", "laver ma voiture",
        "laver auto", "lavage auto", "lavage voiture",
    ],
    "Electricien auto": [
        "electricien auto", "batterie voiture", "alternateur", "demarreur auto",
        "cablage", "phare voiture", "electrician auto", "probleme electrique",
        "electricien", "batterie", "electrique auto",
    ],
    "Climatisation auto": [
        "climatisation", "clim voiture", "air conditionne voiture", "recharge clim",
        "clim", "ac voiture", "recharge climatisation", "air conditionne",
    ],
    "Peinture auto": [
        "peinture voiture", "rayure carrosserie", "retouche peinture", "vernis voiture",
        "peinture auto", "peindre voiture",
    ],
    "Tolerie": [
        "tolerie", "carrosserie", "bosselage", "debosselage", "dent voiture",
        "tole", "carosserie",
    ],
    "Depannage / remorquage": [
        "depannage", "depanneur", "remorquage", "voiture en panne",
        "sos auto", "assistance routiere", "towing", "urgence voiture",
        "en panne", "tombe en panne", "depanner",
    ],
    "Changement d'huile": [
        "vidange", "huile moteur", "filtre huile", "oil change",
        "changer huile", "faire vidange", "huile",
    ],
    "Diagnostic automobile": [
        "diagnostic", "scanner voiture", "valise diagnostic", "code erreur voiture",
        "voyant allume", "check engine", "obd", "voyant rouge", "diagnostique",
    ],
    "Station d'essence": [
        "essence", "carburant", "gasoil", "diesel", "station service",
        "faire le plein", "pompe essence", "station d'essence",
        "station essence", "plein", "sans plomb", "fuel", "petrole",
        "station", "benzine",
    ],
    "Location de voitures": [
        "location voiture", "louer voiture", "voiture de location",
        "louer une voiture", "location auto",
    ],
    "Assurance automobile": [
        "assurance auto", "sinistre auto", "police assurance",
        "assurance voiture", "assurance",
    ],
    "Ecole de conduite": [
        "permis de conduire", "auto-ecole", "autoecole", "driving school",
        "permis conduire", "apprendre conduire", "auto ecole", "ecole conduite",
    ],
    "Vente de pieces detachees": [
        "pieces detachees", "spare part", "plaquette frein", "disque frein",
        "bougie", "filtre voiture", "piece voiture", "pieces auto",
        "pieces detachee", "accessoires auto",
    ],
    "Reparation moto": [
        "moto", "zemidjan", "zem", "scooter", "moto taxi",
        "reparer moto", "moto en panne", "moto mecanique", "motos",
    ],
    "Vente de voitures": [
        "acheter voiture", "achat voiture", "vente voiture", "concessionnaire",
        "voiture d'occasion", "voiture neuve", "occasion", "voiture a vendre",
    ],
    "Maintenance poids lourds": [
        "poids lourd", "camion", "semi-remorque", "gros porteur",
        "poids lourds", "camions",
    ],
    "Vente de motos": [
        "acheter moto", "vente moto", "moto neuve", "moto occasion",
        "vendre moto",
    ],
    "Vente de velos / entretien": [
        "velo", "bicyclette", "vtt", "reparation velo", "velos",
    ],
}

KW2DOM: Dict[str, str] = {}
for _dom, _kws in DOMAINES.items():
    for _kw in _kws:
        KW2DOM[_normalize(_kw)] = _dom
for _dom in DOMAINES.keys():
    _dn = _normalize(_dom)
    if _dn not in KW2DOM:
        KW2DOM[_dn] = _dom

VILLES = [
    "Cotonou", "Porto-Novo", "Parakou", "Abomey", "Bohicon", "Calavi",
    "Ouidah", "Natitingou", "Lokossa", "Djougou", "Kandi", "Malanville",
    "Nikki", "Savalou", "Save", "Tchaourou", "Bassila", "Dogbo", "Aplahoue",
    "Dassa-Zoume", "Abomey-Calavi", "Allada", "Kpomasse", "Ze",
    "Adjarra", "Adjohoun", "Seme-Kpodji", "Godomey", "Fidjrosse",
    "Akpakpa", "Cadjehoun", "Gbegamey", "Haie Vive", "Vedoko",
    "Zogbo", "Agla", "Jericho", "Menontin", "Akogbato", "Cocotiers",
    "Dantokpa", "Houeyiho", "Pobe", "Ketou", "Sakete", "Ifangni",
    "Avrankou", "Dangbo", "Grand-Popo", "Athieme", "Come",
]

STOP_LOC = {
    "moi", "vous", "nous", "lui", "elle", "eux", "cela", "ca", "toi",
    "plus", "moins", "tout", "rien", "ici", "la", "bien", "mal",
    "savoir", "faire", "chercher", "trouver", "aide", "careasy", "carai",
}


# ═══════════════════════════════════════════════════════════════════════════════
#  FAQ COMPLETE
# ═══════════════════════════════════════════════════════════════════════════════

FAQ: List[Dict] = [
    {
        "tags": ["inscription", "creer compte", "devenir prestataire", "inscrire entreprise",
                 "rejoindre", "soumettre dossier", "enregistrer entreprise", "creer une entreprise",
                 "comment creer entreprise"],
        "content": (
            "Pour inscrire votre entreprise sur CarEasy :\n"
            "1. Ouvrez l'application CarEasy et creez un compte.\n"
            "2. Dans la barre de navigation, appuyez sur Entreprise puis Creer.\n"
            "3. Remplissez le formulaire en 4 etapes : informations generales, documents legaux (IFU, RCCM, certificat), dirigeant et contacts, localisation.\n"
            "4. Soumettez votre dossier — validation sous 24 a 48 heures ouvrables.\n"
            "5. Apres validation : essai gratuit de 30 jours avec 3 services maximum."
        )
    },
    {
        "tags": ["documents requis", "ifu", "rccm", "certificat", "pieces dossier"],
        "content": (
            "Documents requis pour l'inscription sur CarEasy :\n"
            "- IFU (Identifiant Fiscal Unique)\n"
            "- RCCM (Registre du Commerce)\n"
            "- Certificat d'immatriculation\n"
            "Formats acceptes : PDF, JPG, PNG — max 5 Mo chacun."
        )
    },
    {
        "tags": ["validation", "delai validation", "dossier en attente"],
        "content": (
            "Apres soumission, la validation prend 24 a 48 heures ouvrables. "
            "Vous recevrez une notification dans l'app et par email. "
            "Suivez le statut dans l'onglet Mes entreprises."
        )
    },
    {
        "tags": ["dossier rejete", "refus", "pourquoi rejete"],
        "content": (
            "Si votre dossier est rejete, la raison est visible dans l'onglet Mes entreprises. "
            "Causes frequentes : documents illisibles ou informations incompletes. "
            "Appuyez sur Resoumettre une demande, corrigez et renvoyez."
        )
    },
    {
        "tags": ["mot de passe oublie", "reinitialiser", "forgot password", "reset"],
        "content": (
            "Pour reinitialiser votre mot de passe :\n"
            "1. Ecran de connexion -> Mot de passe oublie.\n"
            "2. Entrez votre email ou numero de telephone.\n"
            "3. Saisissez le code OTP a 6 chiffres recu (valable 5 minutes).\n"
            "4. Definissez votre nouveau mot de passe."
        )
    },
    {
        "tags": ["rendez-vous", "prendre rdv", "reserver", "booking"],
        "content": (
            "Pour prendre un rendez-vous dans CarEasy :\n"
            "1. Depuis l'accueil, selectionnez un service.\n"
            "2. Appuyez sur Prendre rendez-vous sur la fiche du service.\n"
            "3. Choisissez la date disponible puis le creneau horaire.\n"
            "4. Ajoutez des notes si besoin et confirmez.\n"
            "Le prestataire confirme ensuite — vous etes notifie a chaque etape."
        )
    },
    {
        "tags": ["annuler rdv", "annuler rendez-vous", "cancel rdv"],
        "content": (
            "Pour annuler un rendez-vous :\n"
            "1. Onglet Rendez-vous dans la navigation.\n"
            "2. Selectionnez le RDV concerne.\n"
            "3. Appuyez sur Annuler et indiquez le motif.\n"
            "Possible uniquement si le statut est En attente ou Confirme."
        )
    },
    {
        "tags": ["message", "contacter prestataire", "messagerie", "contacter"],
        "content": (
            "Pour contacter un prestataire via CarEasy :\n"
            "1. Ouvrez la fiche du service.\n"
            "2. Appuyez sur Message pour la messagerie interne ou WhatsApp pour WhatsApp direct.\n"
            "3. Vous pouvez aussi appeler directement via le bouton Appeler.\n"
            "La messagerie supporte texte, images, videos, messages vocaux et localisation GPS."
        )
    },
    {
        "tags": ["abonnement", "plans", "tarifs", "prix careasy", "offres prestataire"],
        "content": (
            "Plans CarEasy prestataire (Parametres -> Plans et Abonnements) :\n"
            "- Essentiel : 25 000 FCFA / mois (5 services)\n"
            "- Professionnel : 50 000 FCFA / mois (15 services, statistiques, support prioritaire)\n"
            "- Premium : 100 000 FCFA / mois (illimite, SMS clients, API)\n"
            "- Annuel : 1 000 000 FCFA / an (Premium + 2 mois offerts)\n"
            "Essai gratuit 30 jours inclus automatiquement a la validation."
        )
    },
    {
        "tags": ["essai gratuit", "trial", "30 jours", "periode essai"],
        "content": (
            "L'essai gratuit de 30 jours demarre automatiquement apres validation. "
            "Il inclut 3 services maximum, la visibilite clients et la gestion des rendez-vous. "
            "Suivez le decompte dans l'onglet Mes entreprises — badge Essai gratuit. "
            "Un plan payant est requis apres les 30 jours."
        )
    },
    {
        "tags": ["payer", "paiement", "fedapay", "mobile money", "orange money", "mtn"],
        "content": (
            "Pour souscrire a un plan CarEasy :\n"
            "1. Parametres -> Plans et Abonnements.\n"
            "2. Choisissez votre plan et appuyez sur Souscrire.\n"
            "3. Payez via FedaPay : Orange Money, MTN Money, Moov Money ou carte bancaire.\n"
            "Une facture est envoyee par email apres paiement."
        )
    },
    {
        "tags": ["support", "aide", "bug", "contacter careasy"],
        "content": (
            "Support CarEasy :\n"
            "- Dans l'application : Parametres -> Aide et support\n"
            "- Email : support@careasy.bj\n"
            "- WhatsApp : disponible depuis la page A propos de l'app\n"
            "Disponible du lundi au vendredi de 8h a 18h."
        )
    },
    {
        "tags": ["creer service", "ajouter service", "publier service"],
        "content": (
            "Pour creer un service dans CarEasy :\n"
            "1. Onglet Entreprise -> Mes entreprises -> selectionnez votre entreprise.\n"
            "2. Appuyez sur Gerer puis Nouveau service.\n"
            "3. Renseignez : nom, domaine, prix (ou Sur devis), horaires, photos.\n"
            "4. Confirmez — le service est immediatement visible.\n"
            "Note : pendant l'essai gratuit, 3 services maximum."
        )
    },
    {
        "tags": ["position gps", "geolocalisation", "localisation", "activer gps"],
        "content": (
            "Pour activer la geolocalisation dans CarEasy : "
            "L'application demande l'autorisation au premier lancement. "
            "Si refuse : Parametres telephone -> Applications -> CarEasy -> Autorisations -> Localisation. "
            "La geolocalisation affiche automatiquement les prestataires les plus proches."
        )
    },
    {
        "tags": ["laisser avis", "noter", "evaluer", "review", "donner note"],
        "content": (
            "Pour laisser un avis dans CarEasy :\n"
            "1. Onglet Rendez-vous -> onglet Termines.\n"
            "2. Selectionnez le RDV termine.\n"
            "3. Appuyez sur Noter ce service.\n"
            "4. Donnez une note de 1 a 5 etoiles et un commentaire optionnel."
        )
    },
    {
        "tags": ["modifier profil", "changer photo", "modifier compte"],
        "content": (
            "Pour modifier votre profil dans CarEasy :\n"
            "1. Onglet Profil dans la navigation.\n"
            "2. Appuyez sur Modifier le profil pour changer nom, email ou telephone.\n"
            "3. Pour la photo : icone appareil photo sur l'avatar -> Galerie ou Camera.\n"
            "4. Pour le mot de passe : Parametres -> Confidentialite et securite -> Changer le mot de passe."
        )
    },
    {
        "tags": ["notifications", "alertes", "notification"],
        "content": (
            "Pour gerer les notifications dans CarEasy : "
            "Parametres -> Notifications. "
            "Activez ou desactivez les notifications push, email et SMS, "
            "et choisissez le son de notification."
        )
    },
    {
        "tags": ["theme", "mode sombre", "apparence", "dark mode"],
        "content": (
            "Pour changer le theme de l'application : "
            "Parametres -> Apparence -> choisissez Clair, Sombre ou Systeme."
        )
    },
    {
        "tags": ["connexion qr", "qr code connexion", "scanner qr", "autre telephone"],
        "content": (
            "Pour se connecter sur un autre telephone via QR code :\n"
            "1. Sur l'appareil connecte : Parametres -> Confidentialite et securite -> Appareils connectes -> Ajouter via QR.\n"
            "2. Sur le nouvel appareil : ecran de bienvenue -> Connexion rapide via QR code.\n"
            "3. Scannez le QR code — connexion automatique (valable 2 minutes)."
        )
    },
    {
        "tags": ["messages", "conversations", "tchat", "chat"],
        "content": (
            "Pour acceder a vos messages : "
            "Appuyez sur l'onglet Messages dans la barre de navigation en bas. "
            "Toutes vos conversations avec les prestataires y sont listees. "
            "Un badge rouge indique le nombre de messages non lus."
        )
    },
]


def faq_lookup(text: str) -> Optional[str]:
    t          = _normalize(text)
    correction = _correction(text)
    if correction:
        return correction
    best_score, best = 0, None
    for entry in FAQ:
        score = sum(
            (2 if len(tag) > 15 else 1)
            for tag in entry["tags"] if _normalize(tag) in t
        )
        if score > best_score:
            best_score, best = score, entry["content"]
    return best if best_score >= 1 else None


# ═══════════════════════════════════════════════════════════════════════════════
#  NLP — Extraction et classification
# ═══════════════════════════════════════════════════════════════════════════════

def detect_lang(text: str) -> str:
    t  = _normalize(text)
    fr = sum(1 for w in [
        "je", "cherche", "besoin", "comment", "combien", "prix",
        "pour", "dans", "sur", "bonjour", "merci", "veux",
        "voudrais", "quel", "les", "des", "un", "une",
    ] if f" {w} " in f" {t} ")
    en = sum(1 for w in [
        "i", "need", "find", "where", "how", "much", "price",
        "looking", "near", "can", "you", "help", "hello", "want",
    ] if f" {w} " in f" {t} ")
    return "en" if en > fr and en >= 2 else "fr"


def extract_domaine(text: str) -> Optional[str]:
    t = _normalize(text)
    for kw in sorted(KW2DOM.keys(), key=len, reverse=True):
        if kw in t:
            return KW2DOM[kw]
    return None


def extract_location(text: str) -> Optional[str]:
    t = _normalize(text)
    for v in sorted(VILLES, key=len, reverse=True):
        if _normalize(v) in t:
            return v
    geo = ["a ", "au ", "en ", "vers ", "pres de ", "autour de ", "quartier ", "zone "]
    if not any(g in t for g in geo):
        return None
    for pat in [
        r"(?:a|au|en|vers|pres de|autour de)\s+([A-Za-z][a-z\-]{2,}(?:\s+[A-Za-z\-]+)?)",
        r"(?:quartier|commune de|zone de?)\s+([A-Za-z][a-z\-]{2,}(?:\s+[A-Za-z\-]+)?)",
    ]:
        m = re.search(pat, t, re.IGNORECASE)
        if m:
            c = m.group(1).strip()
            if len(c) > 2 and c not in STOP_LOC:
                return c.title()
    return None


def extract_radius(text: str) -> float:
    m = re.search(r"(\d+)\s*km", _normalize(text))
    if m:
        return min(float(m.group(1)), 100)
    return 10 if any(w in _normalize(text) for w in ["proche", "pres", "coin"]) else 20


def _needs_db(intent: str, domaine: Optional[str], location: Optional[str],
              ctx: Dict, wc: int) -> bool:
    HARD_SKIP = {"salutation", "remerciement", "aurevoir", "bot_info", "perso"}
    if intent in HARD_SKIP:
        return False
    if domaine or location:
        return True
    if ctx.get("last_domaine") or ctx.get("last_lat"):
        return True
    if intent in {"urgence", "recherche", "diagnostic"}:
        return True
    if intent == "general" and wc >= 4:
        return True
    return False


def intent_classify(text: str, ctx: Dict) -> str:
    t  = _normalize(text)
    wc = len(t.split())

    SAL = ["bonjour", "bonsoir", "salut", "hello", "hi ", "salam", "alafia", "bonne journee"]
    if any(s in t for s in SAL) and wc <= 5:
        return "salutation"

    if any(s in t for s in ["merci", "thank you", "thanks"]) and wc <= 6:
        return "remerciement"

    if any(s in t for s in ["au revoir", "bye", "a bientot", "tchao"]) and wc <= 5:
        return "aurevoir"

    if any(s in t for s in [
        "comment tu t appelle", "qui es-tu", "c est quoi careasy",
        "c est quoi carai", "que peux-tu faire", "tu es qui", "presente-toi",
        "qu est-ce que careasy",
    ]):
        return "bot_info"

    if any(s in t for s in ["comment tu vas", "tu vas bien", "ca va"]) and wc <= 5:
        return "perso"

    # Detecter un diagnostic AVANT le reste
    if detect_diagnostic_intent(text):
        return "diagnostic"

    FAQ_KW = [
        "comment creer", "comment modifier", "comment supprimer", "comment payer",
        "comment annuler", "comment prendre", "comment envoyer", "comment activer",
        "comment ajouter", "comment inscrire", "comment fonctionne", "comment ca",
        "qu est-ce que", "ca fonctionne", "devenir prestataire", "inscrire mon entreprise",
        "rejoindre careasy", "mot de passe", "abonnement", "paiement", "fedapay",
        "essai gratuit", "rendez-vous", "prendre rdv", "creer service", "modifier service",
        "plan essenti", "plan profes", "plan premium", "tarif", "document requis",
        "ifu", "rccm", "certificat", "support careasy", "contacter careasy",
        "creer une entreprise", "inscrire entreprise", "notification", "theme",
        "mode sombre", "modifier profil", "changer photo", "connexion qr",
        "navigation", "onglet", "parametres", "application", "comment utiliser",
        "ou trouver", "ou est", "comment acceder",
    ]
    if sum(1 for kw in FAQ_KW if kw in t) >= 1:
        if extract_domaine(text) or extract_location(text):
            return "recherche"
        return "faq"

    if ctx.get("last_services"):
        RANKS = [
            "premier", "deuxieme", "troisieme", "1er", "2eme", "3eme",
            "numero 1", "numero 2", "le 1", "le 2", "le 3",
        ]
        FKWS = [
            "numero", "contact", "appeler", "whatsapp", "telephone",
            "adresse", "situe", "localisation", "prix", "combien",
            "horaire", "ouvre", "itineraire", "aller", "route",
        ]
        VAGUE = ["celui-la", "cet endroit", "ce prestataire", "cette entreprise", "la-bas", "ce garage"]

        is_rank  = any(r in t for r in RANKS)
        is_vague = any(v in t for v in VAGUE)
        is_short = wc <= 7 and any(f in t for f in FKWS)

        if is_rank or is_vague or is_short:
            if any(f in t for f in ["numero", "contact", "appeler", "whatsapp", "telephone"]):
                return "followup_contact"
            if any(f in t for f in ["adresse", "situe", "localisation", "ou sont", "ou est"]):
                return "followup_adresse"
            if any(f in t for f in ["prix", "combien", "tarif"]):
                return "followup_prix"
            if any(f in t for f in ["horaire", "ouvre", "ferme"]):
                return "followup_horaires"
            if any(f in t for f in ["itineraire", "aller", "route", "chemin"]):
                return "followup_itineraire"
            return "followup_info"

    if any(u in t for u in ["urgent", "urgence", "vite", "sos", "en panne", "emergency"]):
        return "urgence"

    if extract_domaine(text) or extract_location(text):
        return "recherche"

    if ctx.get("last_domaine") and wc <= 5:
        return "recherche"

    return "general"


def resolve_ref(text: str, ctx: Dict) -> Optional[Dict]:
    t    = _normalize(text)
    svcs = ctx.get("last_services", [])
    if not svcs:
        return None

    RANKS = {
        1: ["premier", "1er", "numero 1", "le 1", "premiere", "#1"],
        2: ["deuxieme", "2eme", "numero 2", "le 2", "#2", "second"],
        3: ["troisieme", "3eme", "numero 3", "le 3"],
        4: ["quatrieme", "4eme", "le 4"],
        5: ["cinquieme", "5eme", "le 5"],
    }
    for rank, patterns in RANKS.items():
        if any(p in t for p in patterns):
            return svcs[rank - 1] if rank - 1 < len(svcs) else None

    VAGUE = ["celui-la", "cet endroit", "ce prestataire", "cette entreprise", "la-bas", "ce garage"]
    if any(v in t for v in VAGUE):
        return svcs[0]

    for s in svcs:
        for fname in [
            _normalize(s.get("name") or ""),
            _normalize((s.get("entreprise") or {}).get("name") or ""),
        ]:
            for word in fname.split():
                if len(word) >= 4 and word in t:
                    return s

    FKWS = ["numero", "contact", "appeler", "whatsapp", "telephone", "adresse", "prix", "horaire"]
    if len(text.split()) <= 6 and any(f in t for f in FKWS):
        return svcs[0]

    return None


def resolve_all(text: str, ctx: Dict) -> List[Dict]:
    t     = _normalize(text)
    MULTI = [
        "tous", "toutes", "chacun", "leurs numeros", "leurs contacts",
        "leurs adresses", "les prestataires", "tous les",
    ]
    if any(m in t for m in MULTI):
        return ctx.get("last_services", [])
    return []


# ═══════════════════════════════════════════════════════════════════════════════
#  MEMOIRE (Redis + RAM)
# ═══════════════════════════════════════════════════════════════════════════════

async def mem_get(cid: str) -> Dict:
    if redis_client:
        try:
            raw = await redis_client.get(f"carai101:{cid}")
            if raw:
                return json.loads(raw)
        except Exception:
            pass
    d = _RAM[cid]
    return {"history": list(d["history"])[-20:], "ctx": dict(d["ctx"])}


async def mem_save(cid: str, data: Dict):
    data["history"] = data.get("history", [])[-20:]
    _RAM[cid] = {"history": data["history"], "ctx": data.get("ctx", {})}
    if redis_client:
        try:
            await redis_client.setex(
                f"carai101:{cid}", 14400,
                json.dumps(data, ensure_ascii=False, default=str)
            )
        except Exception:
            pass
    for turn in data["history"]:
        if turn.get("role") == "user":
            _cluster_intent(turn.get("intent", "general"), turn.get("content", ""))
    _LEARN["stats"]["total"] += 1


# ═══════════════════════════════════════════════════════════════════════════════
#  GEOCODAGE
# ═══════════════════════════════════════════════════════════════════════════════

async def geocode(location: str) -> Optional[Tuple[float, float]]:
    key = _normalize(location)
    if key in _GEO:
        return _GEO[key]

    try:
        async with httpx.AsyncClient(timeout=8) as c:
            r = await c.get(f"{LARAVEL_BASE}/ai/locations", params={"q": location, "limit": 1})
            if r.status_code == 200:
                data = r.json().get("data", [])
                if data:
                    coords: Tuple[float, float] = (float(data[0]["latitude"]), float(data[0]["longitude"]))
                    _GEO[key] = coords
                    return coords
    except Exception as e:
        print(f"[GEO] Laravel: {e}")

    if USE_NOMINATIM:
        try:
            async with httpx.AsyncClient(timeout=8) as c:
                r = await c.get(
                    "https://nominatim.openstreetmap.org/search",
                    params={"q": f"{location}, Benin", "format": "json", "limit": 1, "countrycodes": "bj"},
                    headers={"User-Agent": "CarEasy-CarAI/10.1"},
                )
                if r.status_code == 200 and r.json():
                    d = r.json()[0]
                    coords = (float(d["lat"]), float(d["lon"]))
                    _GEO[key] = coords
                    return coords
        except Exception as e:
            print(f"[GEO] Nominatim: {e}")

    if GOOGLE_MAPS_KEY:
        try:
            async with httpx.AsyncClient(timeout=8) as c:
                r = await c.get(
                    "https://maps.googleapis.com/maps/api/geocode/json",
                    params={"address": f"{location}, Benin", "key": GOOGLE_MAPS_KEY},
                )
                if r.status_code == 200 and r.json().get("results"):
                    loc = r.json()["results"][0]["geometry"]["location"]
                    coords = (float(loc["lat"]), float(loc["lng"]))
                    _GEO[key] = coords
                    return coords
        except Exception as e:
            print(f"[GEO] Google: {e}")

    return None


def haversine(lat1, lon1, lat2, lon2) -> float:
    R  = 6371
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a  = (
        math.sin(math.radians(lat2 - lat1) / 2) ** 2
        + math.cos(p1) * math.cos(p2)
        * math.sin(math.radians(lon2 - lon1) / 2) ** 2
    )
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def map_link(ulat, ulng, dlat, dlng) -> str:
    if GOOGLE_MAPS_KEY:
        return f"https://www.google.com/maps/dir/{ulat},{ulng}/{dlat},{dlng}"
    return f"https://www.openstreetmap.org/directions?engine=fossgis_osrm_car&route={ulat},{ulng};{dlat},{dlng}"


def dur(km: float) -> str:
    speed = 20 if km < 5 else (35 if km < 20 else 60)
    m = int((km / speed) * 60)
    return f"{m} min" if m < 60 else f"{m // 60}h{m % 60:02d}"


# ═══════════════════════════════════════════════════════════════════════════════
#  API LARAVEL
# ═══════════════════════════════════════════════════════════════════════════════

async def api_nearby(
    lat: float, lng: float,
    domaine: Optional[str] = None,
    radius: float = 20,
    limit: int = 10,
) -> List[Dict]:
    try:
        p: Dict[str, Any] = {"lat": lat, "lng": lng, "radius": radius, "limit": limit}
        if domaine:
            p["domaine"] = domaine
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(f"{LARAVEL_BASE}/ai/services/nearby", params=p)
            if r.status_code == 200:
                data = r.json().get("data", [])
                _LEARN["stats"]["db_hits"] += 1
                normalized = [_normalize_service(s) for s in data]
                return sorted(normalized, key=lambda x: x.get("distance_km") or 999)
    except Exception as e:
        print(f"[DB] api_nearby ERREUR: {e}")
    _LEARN["stats"]["db_misses"] += 1
    return []


def _normalize_service(s: Dict) -> Dict:
    e   = s.get("entreprise", {}) or {}
    dom = s.get("domaine")
    if isinstance(dom, dict):
        dom = dom.get("name")
    s_copy = dict(s)
    s_copy["domaine"] = dom
    if isinstance(e, dict) and e.get("latitude") is not None:
        e_copy = dict(e)
        if not e_copy.get("address"):
            e_copy["address"] = e_copy.get("google_formatted_address")
        s_copy["entreprise"] = e_copy
        return s_copy
    s_copy["entreprise"] = {
        "id": e.get("id"), "name": e.get("name"),
        "latitude": e.get("latitude"), "longitude": e.get("longitude"),
        "google_formatted_address": e.get("google_formatted_address") or e.get("address"),
        "address": e.get("google_formatted_address") or e.get("address"),
        "call_phone": e.get("call_phone"), "whatsapp_phone": e.get("whatsapp_phone"),
        "status_online": e.get("status_online", True), "logo": e.get("logo"),
    }
    return s_copy


async def api_by_domaine(domaine: str, limit: int = 15) -> List[Dict]:
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(f"{LARAVEL_BASE}/ai/services", params={"domaine": domaine, "limit": limit})
            if r.status_code == 200:
                data = r.json().get("data", [])
                _LEARN["stats"]["db_hits"] += 1
                return [_normalize_service(s) for s in data]
    except Exception as e:
        print(f"[DB] api_by_domaine ERREUR: {e}")
    _LEARN["stats"]["db_misses"] += 1
    return []


async def api_services_all(domaine: Optional[str] = None, limit: int = 20) -> List[Dict]:
    try:
        params: Dict[str, Any] = {"limit": limit}
        if domaine:
            params["domaine"] = domaine
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(f"{LARAVEL_BASE}/ai/services", params=params)
            if r.status_code == 200:
                data = r.json().get("data", [])
                return [_normalize_service(s) for s in data]
    except Exception as e:
        print(f"[DB] api_services_all ERREUR: {e}")
    return []


async def api_domaines() -> List[str]:
    try:
        async with httpx.AsyncClient(timeout=5) as c:
            r = await c.get(f"{LARAVEL_BASE}/ai/domaines")
            if r.status_code == 200:
                return [d["name"] for d in r.json().get("data", [])]
    except Exception:
        pass
    return list(DOMAINES.keys())


def clean_svc(s: Dict) -> Dict:
    e    = s.get("entreprise") or {}
    dist = s.get("distance_km")
    try:
        dist = round(float(dist), 1) if dist is not None else None
    except (TypeError, ValueError):
        dist = None
    domaine_val = s.get("domaine")
    if isinstance(domaine_val, dict):
        domaine_val = domaine_val.get("name")
    address = e.get("google_formatted_address") or e.get("address") or None
    return {
        "id":                  s.get("id"),
        "name":                s.get("name"),
        "domaine":             domaine_val,
        "price":               s.get("price"),
        "price_promo":         s.get("price_promo"),
        "is_price_on_request": s.get("is_price_on_request"),
        "has_promo":           s.get("has_promo"),
        "is_open_24h":         s.get("is_always_open") or s.get("is_open_24h"),
        "start_time":          s.get("start_time"),
        "end_time":            s.get("end_time"),
        "distance_km":         dist,
        "average_rating":      s.get("average_rating"),
        "total_reviews":       s.get("total_reviews"),
        "entreprise": {
            "id":            e.get("id"),
            "name":          e.get("name"),
            "address":       address,
            "latitude":      e.get("latitude"),
            "longitude":     e.get("longitude"),
            "call_phone":    e.get("call_phone"),
            "whatsapp_phone": e.get("whatsapp_phone"),
            "logo":          e.get("logo"),
            "status_online": e.get("status_online"),
        },
    }


def fmt_price(s: Dict) -> str:
    if s.get("is_price_on_request"):
        return "sur devis"
    p, pp = s.get("price"), s.get("price_promo")
    try:
        if pp and s.get("has_promo") and p:
            return f"{int(float(pp)):,} FCFA (promo, au lieu de {int(float(p)):,} FCFA)".replace(",", " ")
        if p:
            return f"{int(float(p)):,} FCFA".replace(",", " ")
    except (TypeError, ValueError):
        pass
    return "prix non renseigne"


def fmt_hours(s: Dict) -> str:
    if s.get("is_always_open") or s.get("is_open_24h"):
        return "ouvert 24h/24"
    st, et = s.get("start_time"), s.get("end_time")
    return f"{st} - {et}" if st and et else "horaires non renseignes"


def fmt_rating(s: Dict) -> str:
    r = s.get("average_rating")
    n = s.get("total_reviews", 0)
    if r and n:
        return f"note {r}/5 ({n} avis)"
    return ""


# ═══════════════════════════════════════════════════════════════════════════════
#  RECHERCHE ROBUSTE — 8 niveaux en cascade
# ═══════════════════════════════════════════════════════════════════════════════

async def search_services_robust(
    domaine:  Optional[str],
    location: Optional[str],
    ulat:     Optional[float],
    ulng:     Optional[float],
    ctx:      Dict,
    radius:   float = 20,
) -> Tuple[List[Dict], bool]:
    services: List[Dict] = []
    has_gps     = (ulat is not None and ulng is not None)
    ctx_lat     = ctx.get("last_lat")
    ctx_lng     = ctx.get("last_lng")
    has_ctx_gps = (ctx_lat is not None and ctx_lng is not None)

    # Niveau 1 : GPS temps reel + domaine
    if has_gps:
        services = await api_nearby(ulat, ulng, domaine, radius, limit=10)
        if not services and radius < 50:
            services = await api_nearby(ulat, ulng, domaine, 50, limit=10)
        if not services and radius < 100:
            services = await api_nearby(ulat, ulng, domaine, 100, limit=10)
        if services:
            return services, True

    # Niveau 2 : GPS contexte + domaine
    if not services and has_ctx_gps and not location:
        services = await api_nearby(float(ctx_lat), float(ctx_lng), domaine, radius, limit=10)
        if not services:
            services = await api_nearby(float(ctx_lat), float(ctx_lng), domaine, 100, limit=10)
        if services:
            return services, True

    # Niveau 3 : Ville mentionnee + domaine
    if not services and location:
        coords = await geocode(location)
        if coords:
            services = await api_nearby(coords[0], coords[1], domaine, radius, limit=10)
            if not services:
                services = await api_nearby(coords[0], coords[1], domaine, 100, limit=10)
        if services:
            return services, True

    # Niveau 4 : Domaine sans GPS
    if not services and domaine:
        services = await api_by_domaine(domaine, limit=15)
        if services:
            return services, True

    # Niveau 5 : GPS temps reel sans filtre domaine
    if not services and has_gps:
        services = await api_nearby(ulat, ulng, None, 50, limit=10)
        if services:
            return services, False

    # Niveau 6 : GPS contexte sans filtre domaine
    if not services and has_ctx_gps:
        services = await api_nearby(float(ctx_lat), float(ctx_lng), None, 50, limit=10)
        if services:
            return services, False

    # Niveau 7 : Ville sans filtre domaine
    if not services and location:
        coords = await geocode(location)
        if coords:
            services = await api_nearby(coords[0], coords[1], None, 100, limit=10)
        if services:
            return services, False

    # Niveau 8 : Tous les services
    if not services:
        services = await api_services_all(None, limit=15)
        if services:
            return services, False

    return services, False


# ═══════════════════════════════════════════════════════════════════════════════
#  MOTEUR DE REPONSE AUTONOME — Zero emoji, zero Ollama
# ═══════════════════════════════════════════════════════════════════════════════

class ReplyEngine:
    """
    Genere des reponses naturelles, professionnelles et sans emoji.
    Utilise des templates composes dynamiquement selon le contexte.
    """

    @staticmethod
    def salutation(lang: str) -> str:
        fr = [
            "Bonjour. Je suis CarAI, votre assistant CarEasy Benin. Que puis-je faire pour vous ?",
            "Bonjour. Comment puis-je vous aider aujourd'hui ?",
            "Bonsoir. Dites-moi ce que vous recherchez : garage, depannage, vulcanisateur...",
            "Bienvenue sur CarAI. Quelle est votre recherche ?",
        ]
        en = [
            "Hello. I'm CarAI, your CarEasy Benin assistant. How can I help you?",
            "Hi. Looking for a mechanic, fuel station or roadside assistance?",
        ]
        return random.choice(en if lang == "en" else fr)

    @staticmethod
    def remerciement(lang: str) -> str:
        fr = ["Avec plaisir.", "De rien, bonne route.", "Je suis la pour ca.", "Toujours disponible."]
        en = ["You're welcome.", "Happy to help.", "Glad I could assist."]
        return random.choice(en if lang == "en" else fr)

    @staticmethod
    def aurevoir(lang: str) -> str:
        fr = ["A bientot. Bonne route.", "Au revoir. Conduisez prudemment.", "A la prochaine."]
        en = ["Goodbye. Drive safe.", "See you soon.", "Take care."]
        return random.choice(en if lang == "en" else fr)

    @staticmethod
    def bot_info(lang: str) -> str:
        if lang == "en":
            return (
                f"I'm CarAI v{APP_VERSION}, the autonomous assistant of the CarEasy Benin mobile app.\n\n"
                f"I can help you with:\n"
                f"- Finding auto service providers (garage, tire, station, etc.)\n"
                f"- Diagnosing a vehicle breakdown or anomaly\n"
                f"- Using the CarEasy app (account, appointments, payments...)\n\n"
                f"I operate 24/7 with real-time access to the CarEasy database.\n"
                f"Website: {SITE_URL}"
            )
        return (
            f"Je suis CarAI v{APP_VERSION}, l'assistant autonome de l'application mobile CarEasy Benin.\n\n"
            f"Je peux vous aider avec :\n"
            f"  - Trouver des prestataires automobiles (garage, vulcanisateur, station, etc.)\n"
            f"  - Diagnostiquer une panne ou anomalie sur votre vehicule\n"
            f"  - Utiliser l'application CarEasy (compte, rendez-vous, paiement...)\n\n"
            f"Je fonctionne 24h/24 avec acces en temps reel a la base de donnees CarEasy.\n"
            f"Site web : {SITE_URL}"
        )

    @staticmethod
    def perso() -> str:
        return random.choice([
            "Je fonctionne parfaitement. Dites-moi comment je peux vous aider.",
            "Toujours operationnel. En quoi puis-je vous etre utile ?",
            "Tres bien. Que puis-je faire pour vous aujourd'hui ?",
        ])

    @staticmethod
    def faq_response(msg: str) -> str:
        ans = faq_lookup(msg)
        if ans:
            return _clean_reply(ans)
        return _clean_reply(
            f"Je n'ai pas trouve de reponse precise pour votre question sur l'application CarEasy.\n\n"
            f"Contactez le support :\n"
            f"  - Email : support@careasy.bj\n"
            f"  - Dans l'app : Parametres -> Aide et support\n"
            f"  - Site web : {SITE_URL}\n\n"
            f"L'equipe repond generalement dans la journee (lundi-vendredi 8h-18h)."
        )

    @staticmethod
    def urgence(
        services: List[Dict],
        domaine:  Optional[str],
        ulat:     Optional[float],
        ulng:     Optional[float],
    ) -> str:
        lines = ["Situation d'urgence. Recherche d'aide immediate.\n"]
        if services:
            e0   = (services[0].get("entreprise") or {})
            nom  = e0.get("name", "Prestataire")
            dist = services[0].get("distance_km")
            dst  = f" — a {dist:.1f} km de vous" if dist else ""
            lines.append(f"Prestataire le plus proche{dst} :")
            lines.append(f"  {nom}")
            if e0.get("call_phone"):
                lines.append(f"  Tel : {e0['call_phone']}")
            if e0.get("whatsapp_phone"):
                lines.append(f"  WhatsApp : {e0['whatsapp_phone']}")
            if ulat and ulng and e0.get("latitude") and e0.get("longitude"):
                try:
                    d_km = haversine(ulat, ulng, float(e0["latitude"]), float(e0["longitude"]))
                    url  = map_link(ulat, ulng, float(e0["latitude"]), float(e0["longitude"]))
                    lines.append(f"  Itineraire : {url} ({d_km:.1f} km — environ {dur(d_km)})")
                except Exception:
                    pass
            if len(services) > 1:
                lines.append("")
                lines.append("Autres options disponibles :")
                for s in services[1:3]:
                    e = s.get("entreprise") or {}
                    lines.append(f"  - {e.get('name', 'Inconnu')} | Tel : {e.get('call_phone') or '—'}")
        else:
            lines.append("Appelez le 166 (Gendarmerie) ou le 197 (Police) pour assistance immedite.")
            lines.append(f"Cherchez un prestataire sur {SITE_URL}")
        return _clean_reply("\n".join(lines))

    @staticmethod
    def services_found(
        services:    List[Dict],
        domaine:     Optional[str],
        location:    Optional[str],
        ulat:        Optional[float],
        ulng:        Optional[float],
        found_exact: bool,
    ) -> str:
        if not services:
            return _clean_reply(
                f"Aucun prestataire n'est encore inscrit sur CarEasy pour ce service au Benin.\n"
                f"De nouveaux prestataires rejoignent CarEasy chaque semaine.\n\n"
                f"Vous etes prestataire ? Inscrivez-vous sur l'application CarEasy — "
                f"essai gratuit de 30 jours inclus."
            )

        lieu = f"a {location}" if location else ("pres de vous" if ulat else "au Benin")
        lines = []

        if not found_exact and domaine:
            lines.append(
                f"Aucun prestataire en {domaine} {lieu} sur CarEasy pour le moment. "
                f"Voici les services disponibles" + (f" {lieu} :" if lieu != "au Benin" else " au Benin :")
            )
            for i, s in enumerate(services[:5], 1):
                e    = s.get("entreprise", {}) or {}
                dist = s.get("distance_km")
                dst  = f" ({dist:.1f} km)" if dist is not None else ""
                dom_s = s.get("domaine") or ""
                note  = fmt_rating(s)
                lines.append(
                    f"\n{i}. {e.get('name', 'Inconnu')}{dst} — {dom_s}"
                    + (f" — {note}" if note else "") + "\n"
                    f"   {s.get('name', '')} | {fmt_hours(s)} | {fmt_price(s)}\n"
                    f"   Tel : {e.get('call_phone') or '—'}   WhatsApp : {e.get('whatsapp_phone') or '—'}"
                )
            if len(services) > 5:
                lines.append(f"\n... et {len(services) - 5} autre(s) disponible(s) dans l'application.")
            lines.append(f"\nVous etes prestataire en {domaine} ? Rejoignez CarEasy — essai gratuit 30 jours.")
        else:
            lines.append(f"{len(services)} prestataire(s) en {domaine or 'automobile'} {lieu} :")
            for i, s in enumerate(services[:5], 1):
                e    = s.get("entreprise", {}) or {}
                dist = s.get("distance_km")
                dst  = f" ({dist:.1f} km)" if dist is not None else ""
                note = fmt_rating(s)
                lines.append(
                    f"\n{i}. {e.get('name', 'Inconnu')}{dst}"
                    + (f" — {note}" if note else "") + "\n"
                    f"   {s.get('name', '')} | {fmt_hours(s)} | {fmt_price(s)}\n"
                    f"   Tel : {e.get('call_phone') or '—'}   WhatsApp : {e.get('whatsapp_phone') or '—'}"
                )
            if len(services) > 5:
                lines.append(f"\n... et {len(services) - 5} autre(s) dans l'application CarEasy.")

        lines.append("\nVoulez-vous l'itineraire ou les contacts d'un prestataire en particulier ?")
        return _clean_reply("\n".join(lines))

    @staticmethod
    def followup(
        intent:  str,
        ref_svc: Optional[Dict],
        all_svcs: List[Dict],
        ulat:    Optional[float],
        ulng:    Optional[float],
    ) -> str:
        if all_svcs:
            lines = ["Contacts de tous les prestataires :"]
            for i, s in enumerate(all_svcs, 1):
                e  = s.get("entreprise", {}) or {}
                ph = e.get("call_phone") or "—"
                wa = e.get("whatsapp_phone") or "—"
                lines.append(f"  {i}. {e.get('name', 'Inconnu')}")
                lines.append(f"     Tel : {ph}  |  WhatsApp : {wa}")
            return _clean_reply("\n".join(lines))

        if not ref_svc:
            return "Quel prestataire vous interesse ? Indiquez le numero (1, 2, 3...) ou le nom."

        e    = ref_svc.get("entreprise", {}) or {}
        ent  = e.get("name", "Ce prestataire")
        svc  = ref_svc.get("name", "ce service")
        addr = e.get("google_formatted_address") or e.get("address") or "adresse non renseignee"

        if "contact" in intent:
            ph = e.get("call_phone") or ""
            wa = e.get("whatsapp_phone") or ""
            if not ph and not wa:
                return f"Aucun contact n'est renseigne pour {ent} pour le moment."
            parts = []
            if ph: parts.append(f"Tel : {ph}")
            if wa: parts.append(f"WhatsApp : {wa}")
            return _clean_reply(f"{ent} — {' | '.join(parts)}")

        if "adresse" in intent or "itineraire" in intent:
            if ulat and ulng and e.get("latitude") and e.get("longitude"):
                try:
                    d   = haversine(ulat, ulng, float(e["latitude"]), float(e["longitude"]))
                    url = map_link(ulat, ulng, float(e["latitude"]), float(e["longitude"]))
                    return _clean_reply(
                        f"{ent} — {addr}\n"
                        f"Distance : {d:.1f} km (environ {dur(d)})\n"
                        f"Itineraire : {url}"
                    )
                except Exception:
                    pass
            return _clean_reply(f"{ent} : {addr}")

        if "prix" in intent:
            return _clean_reply(f"Le service {svc} chez {ent} est a {fmt_price(ref_svc)}.")

        if "horaire" in intent:
            return _clean_reply(f"{ent} est {fmt_hours(ref_svc)}.")

        ph   = e.get("call_phone") or "—"
        wa   = e.get("whatsapp_phone") or "—"
        note = fmt_rating(ref_svc)
        return _clean_reply(
            f"{ent} ({svc})\n"
            f"Tel : {ph}  |  WhatsApp : {wa}\n"
            f"Adresse : {addr}\n"
            f"Horaires : {fmt_hours(ref_svc)}\n"
            f"Prix : {fmt_price(ref_svc)}"
            + (f"\n{note}" if note else "")
        )

    @staticmethod
    def general(msg: str, ctx: Dict, lang: str) -> str:
        t = _normalize(msg)

        if any(w in t for w in ["combien", "prix", "tarif", "cout"]):
            dom = ctx.get("last_domaine")
            if dom:
                return _clean_reply(
                    f"Le prix pour {dom} varie selon le prestataire et le type d'intervention.\n"
                    f"Utilisez CarEasy pour comparer les tarifs en temps reel et reserver directement.\n"
                    f"Les prestataires indiquent leurs prix et promotions dans l'application."
                )

        if any(w in t for w in ["meilleur", "recommande", "conseil", "top"]):
            return _clean_reply(
                f"Pour trouver le meilleur prestataire automobile pres de vous :\n"
                f"  1. Ouvrez CarEasy et activez la geolocalisation\n"
                f"  2. Filtrez par domaine (garage, vulcanisateur, lavage...)\n"
                f"  3. Consultez les avis et notes des clients\n"
                f"  4. Contactez directement ou prenez rendez-vous\n\n"
                f"Dites-moi votre ville ou activez votre GPS pour voir les options autour de vous."
            )

        return _clean_reply(
            f"Je suis CarAI, votre assistant CarEasy Benin.\n\n"
            f"Dites-moi ce que vous cherchez :\n"
            f"  - Un service auto (garage, vulcanisateur, lavage, station...)\n"
            f"  - Un diagnostic de panne vehicule\n"
            f"  - De l'aide pour utiliser l'application CarEasy\n\n"
            f"Exemples : 'garage mecanique a Cotonou' ou 'le moteur de ma voiture chauffe'"
        )


def generate_reply(
    intent:      str,
    msg:         str,
    services:    List[Dict],
    ref_svc:     Optional[Dict],
    all_svcs:    List[Dict],
    domaine:     Optional[str],
    location:    Optional[str],
    ctx:         Dict,
    lang:        str,
    ulat:        Optional[float] = None,
    ulng:        Optional[float] = None,
    found_exact: bool = True,
    diag_code:   Optional[str]  = None,
) -> Tuple[str, Optional[Dict]]:
    """
    Genere une reponse complete et propre (zero emoji).
    Retourne (reply_text, diagnostic_data_ou_None).
    """
    _LEARN["stats"]["fallback"] += 1

    if intent == "diagnostic" and diag_code:
        reply, diag_data = build_diagnostic_response(diag_code, services, location, ulat)
        return reply, diag_data

    if intent == "salutation":
        return ReplyEngine.salutation(lang), None
    if intent == "remerciement":
        return ReplyEngine.remerciement(lang), None
    if intent == "aurevoir":
        return ReplyEngine.aurevoir(lang), None
    if intent == "bot_info":
        return ReplyEngine.bot_info(lang), None
    if intent == "perso":
        return ReplyEngine.perso(), None
    if intent == "faq":
        return ReplyEngine.faq_response(msg), None
    if "followup" in intent:
        return ReplyEngine.followup(intent, ref_svc, all_svcs, ulat, ulng), None
    if intent == "urgence":
        return ReplyEngine.urgence(services, domaine, ulat, ulng), None
    if intent in {"recherche", "general"}:
        return ReplyEngine.services_found(services, domaine, location, ulat, ulng, found_exact), None

    return ReplyEngine.general(msg, ctx, lang), None


SUGG_BASE = [
    "Trouver un garage mecanique",
    "Vulcanisateur disponible",
    "Lavage auto",
    "Electricien auto",
    "Depannage routier",
]

SUGG_DIAG = [
    "Le moteur chauffe",
    "Voyant rouge allume",
    "Voiture ne demarre pas",
    "Bruit au freinage",
    "Clim ne refroidit plus",
]


def suggestions(domaine: Optional[str], location: Optional[str], ctx: Dict, intent: str) -> List[str]:
    result = []
    if ctx.get("last_services"):
        result += ["Contacts de tous", "Itineraire vers le plus proche"]
    if domaine and location:
        result.append(f"{domaine} a {location}")
    elif domaine:
        result.append(f"{domaine} a Cotonou")
        result.append(f"{domaine} a Abomey-Calavi")
    if location:
        result.append(f"Tous les services a {location}")
    if intent == "diagnostic":
        result += SUGG_DIAG[:3]
    else:
        result += SUGG_BASE
    seen, final = set(), []
    for s in result:
        if s not in seen:
            seen.add(s)
            final.append(s)
    return final[:5]


# ═══════════════════════════════════════════════════════════════════════════════
#  ENDPOINT PRINCIPAL — v10.1
# ═══════════════════════════════════════════════════════════════════════════════

@app.post("/chat", response_model=ChatResponse)
async def chat(req: ChatRequest, bg: BackgroundTasks):
    t0 = time.time()

    # Nettoyer le message entrant (supprimer emojis)
    msg_clean = strip_emoji(req.message).strip()
    if not msg_clean:
        msg_clean = req.message

    mem     = await mem_get(req.conversation_id)
    ctx     = mem["ctx"]
    history = mem["history"]

    lang     = req.language or detect_lang(msg_clean)
    intent   = intent_classify(msg_clean, ctx)
    domaine  = extract_domaine(msg_clean)
    location = extract_location(msg_clean)
    radius   = extract_radius(msg_clean)
    wc       = len(msg_clean.split())

    # Identifier le code de diagnostic
    diag_code: Optional[str] = None
    if intent == "diagnostic":
        diag_code = detect_diagnostic_intent(msg_clean)
        if diag_code and DIAG_SYMPTOMS.get(diag_code):
            domaine = DIAG_SYMPTOMS[diag_code]["domaine_recommande"]

    FOLLOW_INTENTS = {
        "followup_contact", "followup_adresse", "followup_prix",
        "followup_horaires", "followup_itineraire", "followup_info", "urgence",
    }

    # Heritage du contexte
    if not domaine and ctx.get("last_domaine") and (
        intent in FOLLOW_INTENTS or ctx.get("last_services") or wc <= 5
    ):
        domaine = ctx.get("last_domaine")

    if not location and ctx.get("last_location") and intent in {
        "followup_adresse", "followup_itineraire", "recherche",
    }:
        location = ctx.get("last_location")

    ref_svc    = resolve_ref(msg_clean, ctx)
    all_svcs   = resolve_all(msg_clean, ctx)
    is_followup = bool(ref_svc or all_svcs) and intent in FOLLOW_INTENTS

    services:    List[Dict] = []
    found_exact: bool = True
    mapurl:      Optional[str] = None
    itinerary:   Optional[Dict] = None

    should_query_db = _needs_db(intent, domaine, location, ctx, wc)

    if should_query_db and not is_followup:
        services, found_exact = await search_services_robust(
            domaine=domaine, location=location,
            ulat=req.latitude, ulng=req.longitude,
            ctx=ctx, radius=radius,
        )
        if req.latitude:
            ctx["last_lat"] = req.latitude
        if req.longitude:
            ctx["last_lng"] = req.longitude

        if services and (req.latitude or ctx.get("last_lat")):
            use_lat = req.latitude or float(ctx.get("last_lat", 0))
            use_lng = req.longitude or float(ctx.get("last_lng", 0))
            e0 = (services[0].get("entreprise") or {})
            if e0.get("latitude") and e0.get("longitude"):
                try:
                    d_km  = haversine(use_lat, use_lng, float(e0["latitude"]), float(e0["longitude"]))
                    mapurl = map_link(use_lat, use_lng, float(e0["latitude"]), float(e0["longitude"]))
                    itinerary = {
                        "maps_url":    mapurl,
                        "distance":    f"{d_km:.1f} km",
                        "duration":    dur(d_km),
                        "destination": e0.get("name", ""),
                    }
                except Exception:
                    pass

    _track_query(domaine, location, len(services))

    active = services or (ctx.get("last_services", []) if is_followup else [])

    reply, diag_data = generate_reply(
        intent=intent, msg=msg_clean,
        services=active, ref_svc=ref_svc, all_svcs=all_svcs,
        domaine=domaine, location=location,
        ctx=ctx, lang=lang,
        ulat=req.latitude, ulng=req.longitude,
        found_exact=found_exact, diag_code=diag_code,
    )

    # Nettoyage final garantissant zero emoji et zero localhost
    reply = _clean_reply(reply)

    cleaned = [clean_svc(s) for s in services[:8]]

    history.append({
        "role": "user", "content": msg_clean,
        "intent": intent, "domaine": domaine, "location": location,
        "diag_code": diag_code, "ts": datetime.now().isoformat(),
    })
    history.append({
        "role": "assistant", "content": reply[:400],
        "intent": intent, "services": cleaned,
        "ts": datetime.now().isoformat(),
    })

    if domaine:        ctx["last_domaine"]  = domaine
    if location:       ctx["last_location"] = location
    if req.latitude:   ctx["last_lat"]      = req.latitude
    if req.longitude:  ctx["last_lng"]      = req.longitude
    if cleaned:        ctx["last_services"] = cleaned
    if diag_code:      ctx["last_diag"]     = diag_code

    mem["history"] = history
    mem["ctx"]     = ctx
    bg.add_task(mem_save, req.conversation_id, mem)

    elapsed = time.time() - t0
    print(
        f"[CHAT] {req.conversation_id[:12]} | intent={intent} | "
        f"domaine={domaine or '-'} | loc={location or '-'} | "
        f"diag={diag_code or '-'} | services={len(active)} | "
        f"exact={found_exact} | {elapsed:.2f}s"
    )

    # Retourner uniquement les icones pertinentes pour cette reponse
    used_icons = {}
    if intent == "diagnostic" and diag_data:
        for ic in ["warning", "wrench", "gear", "alert", "check", "info", diag_data.get("icon", "wrench")]:
            if ic in SVG_ICONS:
                used_icons[ic] = SVG_ICONS[ic]
    elif intent in {"recherche", "general"}:
        for ic in ["location", "phone", "whatsapp", "clock", "price", "star", "distance"]:
            used_icons[ic] = SVG_ICONS[ic]
    elif intent == "urgence":
        for ic in ["urgent", "phone", "whatsapp", "map"]:
            used_icons[ic] = SVG_ICONS[ic]
    elif "followup" in intent:
        for ic in ["phone", "whatsapp", "location", "map"]:
            used_icons[ic] = SVG_ICONS[ic]
    elif intent == "faq":
        for ic in ["info", "check", "step"]:
            used_icons[ic] = SVG_ICONS[ic]
    else:
        for ic in ["info", "car", "search"]:
            used_icons[ic] = SVG_ICONS[ic]

    return ChatResponse(
        reply       = reply,
        services    = cleaned if cleaned else (ctx.get("last_services", [])[:3] if is_followup else []),
        map_url     = mapurl,
        itinerary   = itinerary,
        intent      = domaine or intent,
        language    = lang,
        suggestions = suggestions(domaine, location, ctx, intent),
        confidence  = _confidence(msg_clean, intent),
        diagnostic  = diag_data,
        icons       = used_icons,
    )


# ═══════════════════════════════════════════════════════════════════════════════
#  ENDPOINT DIAGNOSTIC DEDIE
# ═══════════════════════════════════════════════════════════════════════════════

@app.post("/diagnostic")
async def diagnostic_endpoint(req: DiagRequest):
    """
    Endpoint dedie pour diagnostic approfondi depuis l'app Flutter.
    Accepte une liste de symptomes et retourne l'analyse complete.
    """
    if not req.symptoms:
        raise HTTPException(400, "Au moins un symptome requis")

    symptoms_clean = [strip_emoji(s).strip() for s in req.symptoms if s.strip()]
    combined  = " ".join(symptoms_clean)
    diag_code = detect_diagnostic_intent(combined)

    if not diag_code:
        for s in symptoms_clean:
            code = detect_diagnostic_intent(s)
            if code:
                diag_code = code
                break

    if not diag_code:
        return {
            "found":      False,
            "message":    "Symptomes non reconnus. Consultez un mecanicien pour un diagnostic precis.",
            "suggestion": "Utilisez CarEasy pour trouver un specialiste en diagnostic automobile.",
        }

    diag = DIAG_SYMPTOMS[diag_code]
    _LEARN["stats"]["diag_queries"] += 1
    _LEARN["diag_stats"][diag_code] = _LEARN["diag_stats"].get(diag_code, 0) + 1

    urgence_labels = {
        "critique": "CRITIQUE — ne pas rouler",
        "haute":    "HAUTE — intervention rapide",
        "moyenne":  "MOYENNE — a surveiller",
        "basse":    "NON URGENTE — entretien preventif",
        "variable": "VARIABLE — depend du voyant",
    }

    note_mileage = None
    if req.mileage and req.mileage > 100000:
        note_mileage = (
            f"A {req.mileage:,} km, verifiez la courroie de distribution, "
            f"les plaquettes de frein et les filtres.".replace(",", " ")
        )

    return {
        "found":               True,
        "code":                diag_code,
        "titre":               diag["titre"],
        "icon":                diag["icon"],
        "urgence":             diag["urgence"],
        "urgence_label":       urgence_labels.get(diag["urgence"], "A evaluer"),
        "symptomes_detectes":  symptoms_clean,
        "causes_probables":    diag["causes_probables"],
        "diagnostic_rapide":   diag["diagnostic_rapide"],
        "actions_immediates":  diag["actions_immediates"],
        "domaine_recommande":  diag["domaine_recommande"],
        "cout_estimatif":      diag["cout_estimatif"],
        "delai_recommande":    diag["delai_recommande"],
        "vehicle":             req.vehicle,
        "mileage":             req.mileage,
        "note_mileage":        note_mileage,
        "icons":               {
            "diagnostic": SVG_ICONS.get(diag["icon"], SVG_ICONS["wrench"]),
            "warning":    SVG_ICONS["warning"],
            "check":      SVG_ICONS["check"],
            "wrench":     SVG_ICONS["wrench"],
            "urgent":     SVG_ICONS["urgent"],
        },
    }


@app.get("/diagnostic/symptoms")
async def list_symptoms():
    """Retourne tous les diagnostics disponibles avec leurs mots-cles."""
    return {
        "count": len(DIAG_SYMPTOMS),
        "diagnostics": [
            {
                "code":               code,
                "titre":              data["titre"],
                "icon":               data["icon"],
                "urgence":            data["urgence"],
                "domaine_recommande": data["domaine_recommande"],
                "keywords_sample":    data["keywords"][:5],
                "svg_icon":           SVG_ICONS.get(data["icon"], SVG_ICONS["wrench"]),
            }
            for code, data in DIAG_SYMPTOMS.items()
        ],
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  ENDPOINT ICONS — catalogue complet SVG
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/icons")
async def icons_endpoint(names: Optional[str] = Query(None)):
    """
    Retourne le catalogue d'icones SVG professionnels.
    Parametre optionnel : names=search,phone,car (liste separee par virgules)
    """
    if names:
        requested = [n.strip() for n in names.split(",") if n.strip()]
        return {
            "icons": {
                name: SVG_ICONS[name]
                for name in requested
                if name in SVG_ICONS
            },
            "available": list(SVG_ICONS.keys()),
        }
    return {
        "count": len(SVG_ICONS),
        "icons": SVG_ICONS,
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  AUTRES ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════════

@app.post("/feedback")
async def feedback_endpoint(req: FeedbackRequest, bg: BackgroundTasks):
    bg.add_task(_score_update, req.message_text, req.reply_text, req.rating)
    if req.rating <= 2:
        _LEARN["stats"]["feedback_neg"] += 1
        if req.comment:
            comment_clean = strip_emoji(req.comment)
            _LEARN["faq_corrections"][_h(req.message_text)] = comment_clean
            _save_learn()
    else:
        _LEARN["stats"]["feedback_pos"] += 1
    return {
        "saved":       True,
        "rating":      req.rating,
        "corrections": len(_LEARN["faq_corrections"]),
    }


@app.get("/health")
async def health():
    redis_ok = db_ok = False

    if redis_client:
        try:
            await redis_client.ping()
            redis_ok = True
        except Exception:
            pass

    try:
        async with httpx.AsyncClient(timeout=4) as c:
            r    = await c.get(f"{LARAVEL_BASE}/ai/domaines")
            db_ok = r.status_code == 200
    except Exception:
        pass

    return {
        "status":         "ok",
        "version":        APP_VERSION,
        "mode":           "autonomous (rules+nlp+db) — no Ollama — zero emoji",
        "redis":          redis_ok,
        "database":       db_ok,
        "sessions":       len(_RAM),
        "geo_cached":     len(_GEO),
        "diag_supported": len(DIAG_SYMPTOMS),
        "icons_count":    len(SVG_ICONS),
        "learn":          _LEARN["stats"],
        "laravel":        LARAVEL_BASE,
        "site":           SITE_URL,
        "ts":             datetime.now().isoformat(),
    }


@app.delete("/conversation/{cid}")
async def clear_conv(cid: str):
    _RAM.pop(cid, None)
    if redis_client:
        try:
            await redis_client.delete(f"carai101:{cid}")
        except Exception:
            pass
    return {"cleared": True}


@app.get("/geocode")
async def geocode_ep(q: str = Query(...)):
    c = await geocode(q)
    if c:
        return {"location": q, "lat": c[0], "lng": c[1]}
    raise HTTPException(404, f"'{q}' introuvable")


@app.get("/nearby")
async def nearby_ep(
    lat:     float         = Query(...),
    lng:     float         = Query(...),
    domaine: Optional[str] = Query(None),
    radius:  float         = Query(20),
    limit:   int           = Query(8),
):
    svcs = await api_nearby(lat, lng, domaine, radius, limit)
    return {"data": [clean_svc(s) for s in svcs], "count": len(svcs)}


@app.get("/domaines")
async def domaines_ep():
    return {"data": await api_domaines()}


@app.get("/faq")
async def faq_ep(q: str = Query(...)):
    ans = faq_lookup(q)
    return {"found": bool(ans), "answer": ans}


@app.get("/learn/stats")
async def learn_ep():
    return {
        "stats":       _LEARN["stats"],
        "intents":     {k: len(v) for k, v in _LEARN["intent_clusters"].items()},
        "bad":         len(_LEARN["bad_patterns"]),
        "good":        len(_LEARN["good_patterns"]),
        "corrections": len(_LEARN["faq_corrections"]),
        "patterns":    len(_LEARN["pattern_scores"]),
        "diag_stats":  _LEARN["diag_stats"],
        "top_queries": sorted(
            _LEARN["query_stats"].items(),
            key=lambda x: x[1]["count"], reverse=True
        )[:10],
    }


@app.post("/learn/reset_bad")
async def reset_bad():
    n = len(_LEARN["bad_patterns"])
    _LEARN["bad_patterns"] = []
    _save_learn()
    return {"cleared": n}


@app.get("/test")
async def test_ep():
    results: Dict[str, str] = {}

    try:
        async with httpx.AsyncClient(timeout=6) as c:
            r = await c.get(f"{LARAVEL_BASE}/ai/domaines")
            n = len(r.json().get("data", [])) if r.status_code == 200 else 0
            results["laravel_domaines"] = f"OK — {n} domaines" if r.status_code == 200 else f"ERREUR HTTP {r.status_code}"
    except Exception as e:
        results["laravel_domaines"] = f"ERREUR: {e}"

    try:
        svcs = await api_nearby(6.3654, 2.4183, None, 50, 5)
        results["laravel_nearby_cotonou"] = f"OK — {len(svcs)} services"
    except Exception as e:
        results["laravel_nearby_cotonou"] = f"ERREUR: {e}"

    # Tests extraction domaine
    test_domain = [
        ("je cherche de l'essence",              "Station d'essence"),
        ("je cherche de l\u2019essence",          "Station d'essence"),
        ("garage mecanique pres de moi",         "Garage mecanique"),
        ("je veux faire une vidange",            "Changement d'huile"),
        ("mon pneu est creve",                   "Pneumatique / vulcanisation"),
        ("je cherche un electricien auto",       "Electricien auto"),
        ("besoin d'un depanneur urgent",         "Depannage / remorquage"),
        ("lavage voiture a Cotonou",             "Lavage automobile"),
        ("recharge de clim",                     "Climatisation auto"),
    ]
    for text, expected in test_domain:
        dom    = extract_domaine(text)
        status = "OK" if dom == expected else f"FAIL (attendu: {expected})"
        results[f"dom_{_normalize(text)[:28]}"] = f"{status} — '{dom}'"

    # Tests diagnostic
    test_diag = [
        ("ma voiture ne demarre pas",              "moteur_ne_demarre_pas"),
        ("le moteur chauffe trop",                 "surchauffe_moteur"),
        ("mon pneu est a plat",                    "pneu_probleme"),
        ("la pedale de frein est molle",           "frein_probleme"),
        ("voyant rouge allume tableau de bord",    "voyant_allume"),
        ("la clim ne refroidit plus",              "climatisation_probleme"),
        ("bruit etrange au virage",                "bruit_suspect"),
        ("ma batterie est decharge",               "batterie_probleme"),
        ("la boite de vitesse ne passe plus",      "transmission_probleme"),
    ]
    for text, expected in test_diag:
        code   = detect_diagnostic_intent(text)
        status = "OK" if code == expected else f"FAIL (attendu: {expected})"
        results[f"diag_{_normalize(text)[:28]}"] = f"{status} — '{code}'"

    # Test emoji stripping (utilise des codes Unicode pour eviter les emojis dans le source)
    wrench_emoji = "\U0001F527"
    car_emoji    = "\U0001F697"
    test_emoji   = f"Bonjour je cherche un garage {wrench_emoji}{car_emoji}"
    cleaned      = strip_emoji(test_emoji)
    has_emoji    = wrench_emoji in cleaned or car_emoji in cleaned
    results["emoji_strip"] = f"OK — '{cleaned}'" if not has_emoji else "FAIL — emoji present"

    # Tests intent
    test_intent = [
        ("bonjour",                           "salutation"),
        ("merci beaucoup",                    "remerciement"),
        ("mon moteur chauffe",                "diagnostic"),
        ("garage a cotonou",                  "recherche"),
        ("comment creer mon entreprise",      "faq"),
    ]
    for text, expected in test_intent:
        got    = intent_classify(text, {})
        status = "OK" if got == expected else f"FAIL (attendu: {expected})"
        results[f"intent_{_normalize(text)[:28]}"] = f"{status} — '{got}'"

    try:
        coords = await geocode("Abomey-Calavi")
        results["geocode_abomey_calavi"] = f"OK — {coords}" if coords else "Aucun resultat"
    except Exception as e:
        results["geocode_abomey_calavi"] = f"ERREUR: {e}"

    results["redis"]        = "Connecte" if redis_client else "RAM actif (non bloquant)"
    results["site_url"]     = SITE_URL
    results["version"]      = APP_VERSION
    results["kw2dom_count"] = str(len(KW2DOM))
    results["diag_count"]   = str(len(DIAG_SYMPTOMS))
    results["icons_count"]  = str(len(SVG_ICONS))
    results["mode"]         = "autonomous — no Ollama — zero emoji"

    return {"version": APP_VERSION, "tests": results}