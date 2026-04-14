"""
CarAI v10.0 — Assistant CarEasy Bénin
• Autonome (sans Ollama) — moteur NLP + règles + templates enrichis
• Diagnostic automobile intégré (arbre de décision + base de symptômes)
• Icônes professionnelles SVG (aucun emoji)
• Recherche robuste en cascade (8 niveaux)
• Apprentissage non supervisé persistant
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
LEARN_FILE      = os.getenv("LEARN_FILE",       "/tmp/carai_learn_v10.json")
APP_VERSION     = "10.0.0"

app = FastAPI(title="CarAI v10.0", version=APP_VERSION, docs_url="/docs")
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
#  UTILITAIRES
# ═══════════════════════════════════════════════════════════════════════════════

def normalize_text(text: str) -> str:
    text = unicodedata.normalize("NFC", text)
    text = text.replace("\u2019", "'").replace("\u2018", "'") \
               .replace("\u02BC", "'").replace("\u0060", "'") \
               .replace("\u00B4", "'")
    text = text.replace("\u2013", "-").replace("\u2014", "-")
    nfkd = unicodedata.normalize("NFKD", text)
    text_no_accent = "".join(c for c in nfkd if not unicodedata.combining(c))
    return text_no_accent.lower()


# ═══════════════════════════════════════════════════════════════════════════════
#  MODÈLES PYDANTIC
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
#  DÉMARRAGE / ARRÊT
# ═══════════════════════════════════════════════════════════════════════════════

@app.on_event("startup")
async def startup():
    global redis_client
    try:
        redis_client = aioredis.from_url(
            REDIS_URL, encoding="utf-8", decode_responses=True
        )
        await redis_client.ping()
        print("[CarAI] Redis connecté")
    except Exception as e:
        print(f"[CarAI] Redis KO ({e}) — RAM actif")
        redis_client = None

    _load_learn()
    print(f"[CarAI] v{APP_VERSION} | {LARAVEL_BASE} | Mode autonome (sans Ollama)")

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
                print(f"[CarAI] BDD OK — {nb} services près de Cotonou")


@app.on_event("shutdown")
async def shutdown():
    _save_learn()


# ═══════════════════════════════════════════════════════════════════════════════
#  APPRENTISSAGE NON SUPERVISÉ
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
        print(f"[Learn] Chargement ignoré: {e}")


def _save_learn():
    try:
        with open(LEARN_FILE, "w", encoding="utf-8") as f:
            json.dump(_LEARN, f, ensure_ascii=False, indent=2, default=str)
    except Exception as e:
        print(f"[Learn] Sauvegarde échouée: {e}")


def _h(text: str) -> str:
    return hashlib.md5(normalize_text(text).encode()).hexdigest()[:12]


def _score_update(message: str, reply: str, score: int):
    key = _h(message)
    p   = _LEARN["pattern_scores"].setdefault(
        key, {"score": 0.0, "count": 0, "examples": []}
    )
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
#  BASE DE DIAGNOSTIC AUTOMOBILE — Moteur de règles robuste
#  Couvre les pannes les plus fréquentes au Bénin
# ═══════════════════════════════════════════════════════════════════════════════

DIAG_SYMPTOMS: Dict[str, Dict] = {
    # ── MOTEUR ──────────────────────────────────────────────────────────────
    "moteur_ne_demarre_pas": {
        "keywords": [
            "ne demarre pas", "ne demarr", "ne veut pas demarrer", "voiture ne start",
            "moteur mort", "clic clic", "rien quand je tourne la cle",
            "le demarreur tourne mais", "demarre pas", "start pas"
        ],
        "titre": "Moteur ne démarre pas",
        "icon": "engine",
        "urgence": "haute",
        "causes_probables": [
            {"cause": "Batterie déchargée ou défectueuse", "probabilite": 60, "icon": "battery"},
            {"cause": "Démarreur défaillant", "probabilite": 20, "icon": "gear"},
            {"cause": "Problème d'injection / carburant vide", "probabilite": 12, "icon": "fuel"},
            {"cause": "Calage de la distribution", "probabilite": 5, "icon": "wrench"},
            {"cause": "Immobiliseur / clé non reconnue", "probabilite": 3, "icon": "key"},
        ],
        "diagnostic_rapide": [
            "Les phares s'allument-ils normalement ?",
            "Entendez-vous un 'clic clic' ou silence total au démarrage ?",
            "La dernière recharge de batterie date de quand ?",
            "Y a-t-il du carburant dans le réservoir ?",
        ],
        "actions_immediates": [
            "Vérifier la tension de batterie (doit être > 12,4V)",
            "Essayer un démarrage en poussette ou câbles de démarrage",
            "Vérifier le niveau de carburant",
            "Vérifier les bornes de batterie (oxydation)",
        ],
        "domaine_recommande": "Garage mecanique",
        "cout_estimatif": "5 000 – 150 000 FCFA selon la cause",
        "delai_recommande": "Immédiat",
    },

    "surchauffe_moteur": {
        "keywords": [
            "temperature monte", "surchauffe", "moteur chauffe", "jauge temperature rouge",
            "vapeur moteur", "fumee blanche moteur", "radiateur chauffe",
            "voyant temperature", "moteur en surchauffe", "thermometre monte"
        ],
        "titre": "Surchauffe moteur",
        "icon": "temperature",
        "urgence": "critique",
        "causes_probables": [
            {"cause": "Niveau de liquide de refroidissement bas", "probabilite": 40, "icon": "droplet"},
            {"cause": "Thermostat bloqué fermé", "probabilite": 20, "icon": "gear"},
            {"cause": "Fuite du circuit de refroidissement", "probabilite": 18, "icon": "alert"},
            {"cause": "Pompe à eau défaillante", "probabilite": 12, "icon": "wrench"},
            {"cause": "Joint de culasse percé", "probabilite": 10, "icon": "alert"},
        ],
        "diagnostic_rapide": [
            "Y a-t-il de la fumée blanche sortant du capot ou du pot d'échappement ?",
            "Le niveau de liquide de refroidissement est-il bas ?",
            "Y a-t-il des traces de fuite sous la voiture ?",
            "Le ventilateur de refroidissement fonctionne-t-il ?",
        ],
        "actions_immediates": [
            "ARRÊT IMMÉDIAT du véhicule — continuer endommage le moteur",
            "NE PAS ouvrir le bouchon du radiateur à chaud (risque de brûlure)",
            "Attendre refroidissement complet (20-30 min minimum)",
            "Vérifier le niveau de liquide de refroidissement à froid",
            "Appeler un dépanneur si fuite visible",
        ],
        "domaine_recommande": "Garage mecanique",
        "cout_estimatif": "15 000 – 500 000 FCFA (joint de culasse = coût élevé)",
        "delai_recommande": "Immédiat — ne pas rouler",
    },

    "consommation_huile_excessive": {
        "keywords": [
            "huile diminue", "perd de l huile", "fumee bleue", "fumee noire",
            "consomme huile", "voyant huile allume", "niveau huile bas",
            "huile moteur baisse", "manque huile", "brule huile"
        ],
        "titre": "Consommation d'huile excessive / voyant huile",
        "icon": "oil",
        "urgence": "haute",
        "causes_probables": [
            {"cause": "Segments pistons usés (fumée bleue)", "probabilite": 35, "icon": "wrench"},
            {"cause": "Joints de soupapes défaillants", "probabilite": 28, "icon": "wrench"},
            {"cause": "Fuite externe (joints, couvre-culasse)", "probabilite": 22, "icon": "alert"},
            {"cause": "Vidange dépassée / huile dégradée", "probabilite": 10, "icon": "oil"},
            {"cause": "Niveau bas sans fuite (vidange nécessaire)", "probabilite": 5, "icon": "info"},
        ],
        "diagnostic_rapide": [
            "Y a-t-il de la fumée bleue au démarrage à froid ?",
            "Des traces d'huile sous le véhicule ?",
            "Quand a été faite la dernière vidange ?",
            "Le voyant huile (rouge) est-il allumé en roulant ?",
        ],
        "actions_immediates": [
            "Vérifier le niveau d'huile immédiatement (jauge)",
            "Si niveau très bas : ARRÊT et appoint avant de rouler",
            "Vérifier les traces de fuite sous le moteur",
            "Programmer une vidange si > 10 000 km",
        ],
        "domaine_recommande": "Changement d'huile",
        "cout_estimatif": "8 000 – 200 000 FCFA",
        "delai_recommande": "Cette semaine",
    },

    "frein_probleme": {
        "keywords": [
            "frein dur", "frein mou", "frein grince", "frein crie", "pedal frein",
            "pedale s enfonce", "voiture ne freine pas", "frein chauffe",
            "bruit frein", "freinage mauvais", "ABS", "freins usés",
            "plaquettes", "disque frein", "freins"
        ],
        "titre": "Problème de freinage",
        "icon": "brake",
        "urgence": "critique",
        "causes_probables": [
            {"cause": "Plaquettes de frein usées", "probabilite": 40, "icon": "wrench"},
            {"cause": "Disques de frein usés ou voilés", "probabilite": 22, "icon": "gear"},
            {"cause": "Fuite de liquide de frein", "probabilite": 18, "icon": "alert"},
            {"cause": "Liquide de frein dégradé (bouillonnement)", "probabilite": 10, "icon": "droplet"},
            {"cause": "Étrier de frein bloqué", "probabilite": 10, "icon": "wrench"},
        ],
        "diagnostic_rapide": [
            "La pédale s'enfonce-t-elle progressivement (pédale molle) ?",
            "Y a-t-il un bruit de grincement ou couinement ?",
            "La voiture tire-t-elle d'un côté au freinage ?",
            "Le voyant ABS ou frein est-il allumé ?",
        ],
        "actions_immediates": [
            "SÉCURITÉ CRITIQUE — réduire vitesse et éviter les autoroutes",
            "Vérifier le niveau de liquide de frein (réservoir transparent)",
            "Si pédale molle : ne pas rouler, appeler dépanneur",
            "Éviter les descentes prolongées sans frein moteur",
        ],
        "domaine_recommande": "Garage mecanique",
        "cout_estimatif": "20 000 – 150 000 FCFA",
        "delai_recommande": "Immédiat — sécurité",
    },

    "pneu_probleme": {
        "keywords": [
            "pneu crev", "creve", "roue crevee", "pneu plat", "pneu gonfle",
            "pression pneu", "pneu user", "bande pneu", "vibration pneu",
            "voiture penche", "pneu eclate", "valve pneu"
        ],
        "titre": "Problème de pneumatiques",
        "icon": "tire",
        "urgence": "variable",
        "causes_probables": [
            {"cause": "Crevaison simple (clou, vis)", "probabilite": 50, "icon": "alert"},
            {"cause": "Valve défaillante (perte lente)", "probabilite": 20, "icon": "gear"},
            {"cause": "Pneu usé / bande de roulement lisse", "probabilite": 18, "icon": "wrench"},
            {"cause": "Jante abîmée (bosse, voile)", "probabilite": 7, "icon": "wrench"},
            {"cause": "Crevaison interne (flanc endommagé)", "probabilite": 5, "icon": "alert"},
        ],
        "diagnostic_rapide": [
            "Le pneu est-il complètement à plat ou se dégonfle-t-il progressivement ?",
            "Y a-t-il un clou ou un objet visible dans le pneu ?",
            "La crevaison est-elle sur le flanc ou la bande centrale ?",
            "La roue de secours est-elle disponible et gonflée ?",
        ],
        "actions_immediates": [
            "S'arrêter en sécurité hors de la chaussée",
            "Allumer les feux de détresse",
            "Monter la roue de secours si disponible",
            "Appeler un vulcanisateur si pas de roue de secours",
        ],
        "domaine_recommande": "Pneumatique / vulcanisation",
        "cout_estimatif": "1 000 – 50 000 FCFA (réparation/remplacement)",
        "delai_recommande": "Immédiat",
    },

    "batterie_probleme": {
        "keywords": [
            "batterie", "batterie faible", "batterie morte", "batterie decharge",
            "phares faibles", "alarme sonne", "radio s eteint", "alternateur",
            "voyant batterie", "batterie vide", "charge batterie"
        ],
        "titre": "Problème électrique / batterie",
        "icon": "battery",
        "urgence": "moyenne",
        "causes_probables": [
            {"cause": "Batterie déchargée (ancienneté > 3 ans)", "probabilite": 45, "icon": "battery"},
            {"cause": "Alternateur défaillant (ne recharge pas)", "probabilite": 28, "icon": "gear"},
            {"cause": "Consommateur parasite (lumière restée allumée)", "probabilite": 15, "icon": "info"},
            {"cause": "Mauvaise connexion des bornes", "probabilite": 7, "icon": "wrench"},
            {"cause": "Régulateur de tension défaillant", "probabilite": 5, "icon": "gear"},
        ],
        "diagnostic_rapide": [
            "La batterie a-t-elle plus de 3 ans ?",
            "Le voyant batterie (rouge) est-il allumé en roulant ?",
            "Les phares brillent-ils normalement moteur en marche ?",
            "Le démarrage est-il difficile (moteur tourne lentement) ?",
        ],
        "actions_immediates": [
            "Tester la tension batterie (bon état : 12,6V à vide, 13,5-14,5V moteur tournant)",
            "Vérifier l'état des bornes (pas d'oxydation blanche)",
            "Si alternateur suspect : ne pas couper le moteur",
            "Prévoir remplacement si batterie > 4 ans",
        ],
        "domaine_recommande": "Electricien auto",
        "cout_estimatif": "15 000 – 80 000 FCFA (batterie)",
        "delai_recommande": "Cette semaine",
    },

    "bruit_suspect": {
        "keywords": [
            "bruit bizarre", "bruit moteur", "bruit cliquetis", "cognement",
            "toc toc moteur", "sifflement", "grondement", "vibration voiture",
            "bruit au virage", "craquement", "bruit suspension", "bruit en roulant",
            "bruit direction", "bruit frein", "bruit bizarre voiture"
        ],
        "titre": "Bruit suspect / anomalie sonore",
        "icon": "sound",
        "urgence": "moyenne",
        "causes_probables": [
            {"cause": "Roulement de roue usé (grondement en virage)", "probabilite": 30, "icon": "gear"},
            {"cause": "Cardans défaillants (cliquetis en virage)", "probabilite": 22, "icon": "gear"},
            {"cause": "Silentblocs / amortisseurs usés (bruit suspension)", "probabilite": 20, "icon": "wrench"},
            {"cause": "Courroie de distribution usée (cliquetis moteur)", "probabilite": 15, "icon": "alert"},
            {"cause": "Échappement percé (sifflement/grondement)", "probabilite": 13, "icon": "wrench"},
        ],
        "diagnostic_rapide": [
            "Le bruit apparaît-il surtout en virage, en freinage ou en permanence ?",
            "Est-ce un cliquetis métallique, un grondement sourd ou un sifflement ?",
            "Le bruit augmente-t-il avec la vitesse ?",
            "Est-ce récent ou progressif depuis plusieurs semaines ?",
        ],
        "actions_immediates": [
            "Un cliquetis métallique rapide = URGENT (distribution ou moteur)",
            "Un grondement en virage = roulement à surveiller (risque de blocage)",
            "Éviter les longs trajets avant diagnostic",
            "Faire un diagnostic électronique pour lire les codes erreur",
        ],
        "domaine_recommande": "Diagnostic automobile",
        "cout_estimatif": "5 000 – 300 000 FCFA selon l'origine",
        "delai_recommande": "Dans la semaine",
    },

    "voyant_allume": {
        "keywords": [
            "voyant allume", "voyant rouge", "voyant orange", "check engine",
            "voyant moteur", "tableau de bord allume", "lampe allumee tableau",
            "warning allume", "voyant", "code erreur", "obd", "scanner"
        ],
        "titre": "Voyant(s) allumé(s) tableau de bord",
        "icon": "warning",
        "urgence": "variable",
        "causes_probables": [
            {"cause": "Capteur défaillant (O2, MAF, température)", "probabilite": 35, "icon": "sensor"},
            {"cause": "Problème système dépollution (FAP, catalyseur)", "probabilite": 25, "icon": "exhaust"},
            {"cause": "Pression huile ou température anormale", "probabilite": 18, "icon": "alert"},
            {"cause": "Problème système de freinage (ABS/ESP)", "probabilite": 12, "icon": "brake"},
            {"cause": "Défaut électrique mineur (contacteur, capteur)", "probabilite": 10, "icon": "electric"},
        ],
        "diagnostic_rapide": [
            "Le voyant est-il ROUGE (urgence) ou ORANGE/JAUNE (avertissement) ?",
            "Plusieurs voyants sont-ils allumés simultanément ?",
            "Y a-t-il une perte de puissance moteur associée ?",
            "Le voyant clignote-t-il ou est-il fixe ?",
        ],
        "actions_immediates": [
            "ROUGE fixe ou clignotant = arrêt immédiat recommandé",
            "ORANGE/JAUNE fixe = diagnostic dans les 48h",
            "Ne pas effacer les codes erreur avant diagnostic (perte d'information)",
            "Faire lire les codes OBD par un professionnel",
        ],
        "domaine_recommande": "Diagnostic automobile",
        "cout_estimatif": "3 000 – 200 000 FCFA selon le défaut",
        "delai_recommande": "Selon couleur du voyant",
    },

    "climatisation_probleme": {
        "keywords": [
            "clim ne refroidit pas", "clim ne fonctionne pas", "clim chaude",
            "air conditionne ne marche pas", "recharger clim", "gaz clim",
            "clim souffle chaud", "mauvaise climatisation", "clim inefficace",
            "odeur clim", "clim fait du bruit"
        ],
        "titre": "Climatisation défaillante",
        "icon": "ac",
        "urgence": "basse",
        "causes_probables": [
            {"cause": "Gaz réfrigérant insuffisant (fuite ou recharge nécessaire)", "probabilite": 50, "icon": "droplet"},
            {"cause": "Condenseur encrassé ou endommagé", "probabilite": 20, "icon": "wrench"},
            {"cause": "Compresseur défaillant", "probabilite": 15, "icon": "gear"},
            {"cause": "Filtre habitacle encrassé", "probabilite": 10, "icon": "filter"},
            {"cause": "Résistance de ventilateur défaillante", "probabilite": 5, "icon": "electric"},
        ],
        "diagnostic_rapide": [
            "La clim souffle-t-elle de l'air mais pas froid ?",
            "Y a-t-il un sifflement ou bruit inhabituel quand la clim est allumée ?",
            "Des mauvaises odeurs à la mise en route ?",
            "Quand la clim a-t-elle été rechargée pour la dernière fois ?",
        ],
        "actions_immediates": [
            "Vérifier que le compresseur s'enclenche (embrayage magnétique)",
            "Inspecter le condenseur (devant du radiateur) — nettoyage si encrassé",
            "Changer le filtre d'habitacle si > 15 000 km ou 1 an",
            "Recharge de gaz recommandée tous les 2 ans",
        ],
        "domaine_recommande": "Climatisation auto",
        "cout_estimatif": "10 000 – 120 000 FCFA",
        "delai_recommande": "Cette semaine",
    },

    "panne_electrique": {
        "keywords": [
            "phare ne marche pas", "clignotant", "essuie glace", "vitres electriques",
            "verrouillage porte", "centrale clignotant", "fusible grille",
            "court circuit", "tableau de bord eteint", "ordinateur bord",
            "probleme electrique", "prise obd", "calculateur"
        ],
        "titre": "Panne électrique",
        "icon": "electric",
        "urgence": "variable",
        "causes_probables": [
            {"cause": "Fusible grillé", "probabilite": 40, "icon": "fuse"},
            {"cause": "Relais défaillant", "probabilite": 25, "icon": "gear"},
            {"cause": "Câblage endommagé (rongeurs, humidité)", "probabilite": 20, "icon": "cable"},
            {"cause": "Calculateur défaillant", "probabilite": 10, "icon": "chip"},
            {"cause": "Masse carrosserie desserrée", "probabilite": 5, "icon": "wrench"},
        ],
        "diagnostic_rapide": [
            "L'équipement défaillant est-il seul ou plusieurs en même temps ?",
            "Le problème est-il intermittent ou permanent ?",
            "Y a-t-il eu récemment de l'eau dans l'habitacle ?",
            "Y a-t-il une odeur de brûlé ?",
        ],
        "actions_immediates": [
            "Vérifier en premier le coffret à fusibles (capot et habitacle)",
            "Identifier le fusible correspondant dans le guide utilisateur",
            "Ne pas remplacer un fusible par un ampérage supérieur",
            "Si odeur de brûlé : déconnecter la batterie et appeler un électricien auto",
        ],
        "domaine_recommande": "Electricien auto",
        "cout_estimatif": "2 000 – 150 000 FCFA",
        "delai_recommande": "Selon gravité",
    },

    "transmission_probleme": {
        "keywords": [
            "boite vitesse", "vitesse ne passe pas", "rapport dur", "boite automatique",
            "transmission", "embrayage patine", "embrayage dur", "glisse embrayage",
            "pedale embrayage", "marche arriere", "boite de vitesse"
        ],
        "titre": "Problème boîte de vitesses / transmission",
        "icon": "transmission",
        "urgence": "haute",
        "causes_probables": [
            {"cause": "Embrayage usé (patinage, prise haute)", "probabilite": 40, "icon": "gear"},
            {"cause": "Câble ou tringlerie de boîte mal réglée", "probabilite": 20, "icon": "wrench"},
            {"cause": "Huile de boîte insuffisante ou dégradée", "probabilite": 18, "icon": "oil"},
            {"cause": "Synchroniseurs usés (passages durs)", "probabilite": 14, "icon": "gear"},
            {"cause": "Boîte automatique : capteur ou solénoïde", "probabilite": 8, "icon": "chip"},
        ],
        "diagnostic_rapide": [
            "Le problème est-il au passage de rapport ou à la prise de mouvement ?",
            "Y a-t-il une odeur de brûlé lors de la montée en côte ?",
            "Le régime moteur monte-t-il sans que la vitesse augmente ?",
            "La pédale d'embrayage est-elle molle ou très haute ?",
        ],
        "actions_immediates": [
            "Éviter les côtes abruptes si l'embrayage patine",
            "Vérifier le niveau d'huile de boîte (sous le capot ou dessous)",
            "Ne pas forcer les passages de rapport (endommage les synchroniseurs)",
            "Diagnostic professionnel recommandé avant aggravation",
        ],
        "domaine_recommande": "Garage mecanique",
        "cout_estimatif": "50 000 – 400 000 FCFA",
        "delai_recommande": "Dans la semaine",
    },

    "direction_suspension": {
        "keywords": [
            "direction dure", "volant tremble", "volant tire", "alignement",
            "geometrie", "amortisseur", "suspension", "voiture oscille",
            "tient mal la route", "craquement volant", "pneu mange",
            "usure inégale pneu", "direction assistee"
        ],
        "titre": "Direction et suspension",
        "icon": "steering",
        "urgence": "moyenne",
        "causes_probables": [
            {"cause": "Amortisseurs usés (oscillation, bruit)", "probabilite": 35, "icon": "wrench"},
            {"cause": "Parallélisme / géométrie déréglée", "probabilite": 28, "icon": "alignment"},
            {"cause": "Rotule de direction usée", "probabilite": 18, "icon": "gear"},
            {"cause": "Direction assistée défaillante (huile, pompe)", "probabilite": 12, "icon": "oil"},
            {"cause": "Triangle de suspension endommagé", "probabilite": 7, "icon": "wrench"},
        ],
        "diagnostic_rapide": [
            "La voiture tire-t-elle d'un côté sur route droite ?",
            "Y a-t-il des vibrations dans le volant à partir de 80 km/h ?",
            "Les pneus s'usent-ils de manière inégale (d'un côté seulement) ?",
            "Y a-t-il des craquements en tournant à basse vitesse ?",
        ],
        "actions_immediates": [
            "Vérifier le niveau de liquide de direction assistée",
            "Inspecter les pneus pour l'usure asymétrique",
            "Vérifier que les roues ne vibrent pas en les secouant à la main",
            "Réaliser un parallélisme après tout changement de pneus",
        ],
        "domaine_recommande": "Garage mecanique",
        "cout_estimatif": "15 000 – 200 000 FCFA",
        "delai_recommande": "Dans la semaine",
    },
}

# Index de recherche rapide
DIAG_KEYWORD_INDEX: Dict[str, str] = {}
for code, data in DIAG_SYMPTOMS.items():
    for kw in data["keywords"]:
        DIAG_KEYWORD_INDEX[normalize_text(kw)] = code


def detect_diagnostic_intent(text: str) -> Optional[str]:
    """Retourne le code de diagnostic si la question est un diagnostic, sinon None."""
    t = normalize_text(text)
    best_code, best_score = None, 0

    # Recherche par mots-clés (triés par longueur décroissante)
    for kw in sorted(DIAG_KEYWORD_INDEX.keys(), key=len, reverse=True):
        if kw in t:
            code = DIAG_KEYWORD_INDEX[kw]
            score = len(kw)
            if score > best_score:
                best_score = score
                best_code = code

    # Si la question est clairement un problème auto (mots indicateurs)
    DIAG_TRIGGERS = [
        "probleme", "panne", "ne fonctionne", "ne marche", "grince", "vibre",
        "clignote", "allume", "fuit", "fume", "chauffe", "bruit", "voyant",
        "pourquoi ma", "pourquoi mon", "diagnostiquer", "qu est ce qui",
        "que se passe", "diagnosis", "symptome", "defaut", "anomalie"
    ]
    is_diag_context = any(trigger in t for trigger in DIAG_TRIGGERS)

    if best_code and (best_score >= 4 or is_diag_context):
        return best_code

    return None


def build_diagnostic_response(diag_code: str, services: List[Dict], location: Optional[str], ulat: Optional[float]) -> Tuple[str, Dict]:
    """Construit une réponse de diagnostic complète avec recommandations de prestataires."""
    diag = DIAG_SYMPTOMS[diag_code]
    _LEARN["stats"]["diag_queries"] += 1
    _LEARN["diag_stats"][diag_code] = _LEARN["diag_stats"].get(diag_code, 0) + 1

    lines = []

    # En-tête
    urgence_label = {
        "critique": "CRITIQUE — ne pas rouler",
        "haute":    "HAUTE — intervention rapide",
        "moyenne":  "MOYENNE — à surveiller",
        "basse":    "NON URGENTE — entretien préventif",
        "variable": "VARIABLE — dépend du voyant",
    }.get(diag["urgence"], "À évaluer")

    lines.append(f"Diagnostic : {diag['titre']}")
    lines.append(f"Niveau d'urgence : {urgence_label}")
    lines.append("")

    # Causes probables (top 3)
    lines.append("Causes probables :")
    for i, cause in enumerate(diag["causes_probables"][:3], 1):
        bar = "█" * (cause["probabilite"] // 10) + "░" * (10 - cause["probabilite"] // 10)
        lines.append(f"{i}. {cause['cause']} — {cause['probabilite']}%")
        lines.append(f"   [{bar}]")

    lines.append("")

    # Questions de diagnostic
    lines.append("Pour affiner le diagnostic, répondez à ces questions :")
    for i, q in enumerate(diag["diagnostic_rapide"][:3], 1):
        lines.append(f"Q{i}. {q}")

    lines.append("")

    # Actions immédiates (top 3)
    lines.append("Actions immédiates :")
    for action in diag["actions_immediates"][:3]:
        lines.append(f"- {action}")

    lines.append("")
    lines.append(f"Coût estimatif : {diag['cout_estimatif']}")
    lines.append(f"Délai recommandé : {diag['delai_recommande']}")

    # Recommandation prestataire
    domaine_rec = diag["domaine_recommande"]
    if services:
        lieu = f"à {location}" if location else ("près de vous" if ulat else "au Bénin")
        lines.append(f"\nPrestataires en {domaine_rec} {lieu} :")
        for i, s in enumerate(services[:3], 1):
            e    = s.get("entreprise", {}) or {}
            dist = s.get("distance_km")
            dst  = f" ({dist:.1f} km)" if dist is not None else ""
            lines.append(
                f"{i}. {e.get('name', 'Inconnu')}{dst}\n"
                f"   Tél : {e.get('call_phone') or '—'} | WA : {e.get('whatsapp_phone') or '—'}"
            )
    else:
        lines.append(f"\nRecherchez un spécialiste en : {domaine_rec}")
        lines.append("Utilisez CarEasy pour trouver le plus proche de vous.")

    diag_data = {
        "code":              diag_code,
        "titre":             diag["titre"],
        "urgence":           diag["urgence"],
        "causes_probables":  diag["causes_probables"][:3],
        "actions_immediates": diag["actions_immediates"][:3],
        "domaine_recommande": domaine_rec,
        "cout_estimatif":    diag["cout_estimatif"],
        "delai_recommande":  diag["delai_recommande"],
        "questions":         diag["diagnostic_rapide"][:3],
    }

    return "\n".join(lines), diag_data


# ═══════════════════════════════════════════════════════════════════════════════
#  DOMAINES — mapping étendu avec variantes sans accent
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
for dom, kws in DOMAINES.items():
    for kw in kws:
        KW2DOM[normalize_text(kw)] = dom
for dom in DOMAINES.keys():
    dom_n = normalize_text(dom)
    if dom_n not in KW2DOM:
        KW2DOM[dom_n] = dom

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
#  FAQ COMPLÈTE
# ═══════════════════════════════════════════════════════════════════════════════

FAQ: List[Dict] = [
    {
        "tags": ["inscription", "creer compte", "devenir prestataire", "inscrire entreprise",
                 "rejoindre", "soumettre dossier", "enregistrer entreprise", "creer une entreprise",
                 "comment creer entreprise"],
        "content": (
            "Pour inscrire votre entreprise sur CarEasy :\n"
            "1) Ouvrez l'application CarEasy et créez un compte.\n"
            "2) Dans la barre de navigation, appuyez sur Entreprise puis Créer.\n"
            "3) Remplissez le formulaire en 4 étapes : informations générales, documents légaux (IFU, RCCM, certificat), dirigeant et contacts, localisation.\n"
            "4) Soumettez votre dossier — validation sous 24 à 48 heures ouvrables.\n"
            "5) Après validation : essai gratuit de 30 jours avec 3 services maximum."
        )
    },
    {
        "tags": ["documents requis", "ifu", "rccm", "certificat", "pieces dossier"],
        "content": (
            "Documents requis pour l'inscription de votre entreprise dans l'app CarEasy :\n"
            "- IFU (Identifiant Fiscal Unique)\n"
            "- RCCM (Registre du Commerce)\n"
            "- Certificat d'immatriculation\n"
            "Formats acceptés : PDF, JPG, PNG — max 5 Mo chacun."
        )
    },
    {
        "tags": ["validation", "delai validation", "dossier en attente"],
        "content": (
            "Après soumission dans l'application CarEasy, la validation prend 24 à 48 heures ouvrables. "
            "Vous recevrez une notification dans l'app et par email dès que votre dossier est traité. "
            "Vous pouvez suivre le statut dans l'onglet Mes entreprises."
        )
    },
    {
        "tags": ["dossier rejete", "refus", "pourquoi rejete"],
        "content": (
            "Si votre dossier est rejeté, la raison est visible dans l'onglet Mes entreprises de l'application. "
            "Causes fréquentes : documents illisibles ou informations incomplètes. "
            "Appuyez sur Resoumettre une demande, corrigez et renvoyez."
        )
    },
    {
        "tags": ["mot de passe oublie", "reinitialiser", "forgot password", "reset"],
        "content": (
            "Pour réinitialiser votre mot de passe depuis l'application CarEasy :\n"
            "1) Écran de connexion → Mot de passe oublié.\n"
            "2) Entrez votre email ou numéro de téléphone.\n"
            "3) Saisissez le code OTP à 6 chiffres reçu par SMS ou email (valable 5 minutes).\n"
            "4) Définissez votre nouveau mot de passe."
        )
    },
    {
        "tags": ["rendez-vous", "prendre rdv", "reserver", "booking"],
        "content": (
            "Pour prendre un rendez-vous dans l'application CarEasy :\n"
            "1) Depuis l'accueil, sélectionnez un service.\n"
            "2) Sur la fiche du service, appuyez sur Prendre rendez-vous.\n"
            "3) Choisissez la date parmi les jours disponibles, puis le créneau horaire.\n"
            "4) Ajoutez des notes si besoin et confirmez.\n"
            "Le prestataire confirme ensuite — vous êtes notifié à chaque étape."
        )
    },
    {
        "tags": ["annuler rdv", "annuler rendez-vous", "cancel rdv"],
        "content": (
            "Pour annuler un rendez-vous dans l'app CarEasy :\n"
            "1) Onglet Rendez-vous dans la navigation.\n"
            "2) Sélectionnez le RDV concerné.\n"
            "3) Appuyez sur Annuler et indiquez le motif.\n"
            "Possible uniquement si le statut est En attente ou Confirmé."
        )
    },
    {
        "tags": ["message", "contacter prestataire", "messagerie", "contacter"],
        "content": (
            "Pour contacter un prestataire via l'application CarEasy :\n"
            "1) Ouvrez la fiche du service.\n"
            "2) Appuyez sur Message pour la messagerie interne ou WhatsApp pour WhatsApp direct.\n"
            "3) Vous pouvez aussi appeler directement via le bouton Appeler.\n"
            "La messagerie supporte texte, images, vidéos, messages vocaux et localisation GPS."
        )
    },
    {
        "tags": ["abonnement", "plans", "tarifs", "prix careasy", "offres prestataire"],
        "content": (
            "Plans CarEasy prestataire (section Plans et Abonnements dans Paramètres) :\n"
            "- Essentiel : 25 000 FCFA/mois (5 services)\n"
            "- Professionnel : 50 000 FCFA/mois (15 services, statistiques, support prioritaire)\n"
            "- Premium : 100 000 FCFA/mois (illimité, SMS clients, API)\n"
            "- Annuel : 1 000 000 FCFA/an (Premium + 2 mois offerts)\n"
            "Essai gratuit 30 jours inclus automatiquement à la validation."
        )
    },
    {
        "tags": ["essai gratuit", "trial", "30 jours", "periode essai"],
        "content": (
            "L'essai gratuit de 30 jours démarre automatiquement après validation de votre entreprise. "
            "Il inclut 3 services maximum, la visibilité clients et la gestion des rendez-vous. "
            "Suivez le décompte dans l'onglet Mes entreprises — badge bleu Essai gratuit. "
            "Un plan payant est requis après les 30 jours pour continuer."
        )
    },
    {
        "tags": ["payer", "paiement", "fedapay", "mobile money", "orange money", "mtn"],
        "content": (
            "Pour souscrire à un plan dans l'application CarEasy :\n"
            "1) Allez dans Paramètres → Plans et Abonnements.\n"
            "2) Choisissez votre plan et appuyez sur Souscrire.\n"
            "3) Payez via FedaPay : Orange Money, MTN Money, Moov Money ou carte bancaire.\n"
            "Une facture est envoyée par email après paiement."
        )
    },
    {
        "tags": ["support", "aide", "probleme", "bug", "contacter careasy"],
        "content": (
            "Support CarEasy :\n"
            "- Dans l'application : Paramètres → Aide et support\n"
            "- Email : support@careasy.bj\n"
            "- WhatsApp : disponible depuis la page A propos de l'app\n"
            "Disponible du lundi au vendredi de 8h à 18h."
        )
    },
    {
        "tags": ["creer service", "ajouter service", "publier service"],
        "content": (
            "Pour créer un service dans l'application CarEasy :\n"
            "1) Onglet Entreprise → Mes entreprises → sélectionnez votre entreprise.\n"
            "2) Appuyez sur Gérer puis Nouveau service.\n"
            "3) Renseignez : nom, domaine, prix (ou Sur devis), horaires d'ouverture par jour, photos.\n"
            "4) Confirmez — le service est immédiatement visible par les clients.\n"
            "Note : pendant l'essai gratuit, 3 services maximum."
        )
    },
    {
        "tags": ["position gps", "geolocalisation", "localisation", "activer gps"],
        "content": (
            "Pour activer la géolocalisation dans CarEasy : "
            "L'application vous demande l'autorisation au premier lancement. "
            "Si vous l'avez refusée, allez dans les Paramètres de votre téléphone → Applications → CarEasy → Autorisations → Localisation. "
            "La géolocalisation affiche automatiquement les prestataires les plus proches de vous sur la carte."
        )
    },
    {
        "tags": ["laisser avis", "noter", "evaluer", "review", "donner note"],
        "content": (
            "Pour laisser un avis dans l'application CarEasy :\n"
            "1) Onglet Rendez-vous → onglet Terminés.\n"
            "2) Sélectionnez le RDV terminé.\n"
            "3) Appuyez sur Noter ce service.\n"
            "4) Donnez une note de 1 à 5 étoiles et un commentaire optionnel."
        )
    },
    {
        "tags": ["modifier profil", "changer photo", "modifier compte"],
        "content": (
            "Pour modifier votre profil dans CarEasy :\n"
            "1) Onglet Profil (icône personne) dans la barre de navigation.\n"
            "2) Appuyez sur Modifier le profil pour changer nom, email ou téléphone.\n"
            "3) Pour la photo : icône appareil photo sur votre avatar → choisir Galerie ou Appareil photo.\n"
            "4) Pour le mot de passe : Paramètres → Confidentialité et sécurité → Changer le mot de passe."
        )
    },
    {
        "tags": ["notifications", "alertes", "notification"],
        "content": (
            "Pour gérer les notifications dans CarEasy : "
            "Paramètres → Notifications. "
            "Vous pouvez activer/désactiver les notifications push, email et SMS, "
            "et choisir le son de notification."
        )
    },
    {
        "tags": ["theme", "mode sombre", "apparence", "dark mode"],
        "content": (
            "Pour changer le thème de l'application CarEasy : "
            "Paramètres → Apparence → choisissez Clair, Sombre ou Système (suit votre téléphone)."
        )
    },
    {
        "tags": ["connexion qr", "qr code connexion", "scanner qr", "autre telephone"],
        "content": (
            "Pour vous connecter sur un autre téléphone via QR code :\n"
            "1) Sur l'appareil déjà connecté : Paramètres → Confidentialité et sécurité → Appareils connectés → Ajouter via QR.\n"
            "2) Sur le nouvel appareil : écran de bienvenue → Connexion rapide via QR code.\n"
            "3) Scannez le QR code — connexion automatique et sécurisée (valable 2 minutes)."
        )
    },
    {
        "tags": ["messages", "conversations", "tchat", "chat"],
        "content": (
            "Pour accéder à vos messages dans CarEasy : "
            "Appuyez sur l'onglet Messages (icône bulle) dans la barre de navigation en bas. "
            "Vous y trouvez toutes vos conversations avec les prestataires. "
            "L'onglet affiche un badge rouge avec le nombre de messages non lus."
        )
    },
]


def faq_lookup(text: str) -> Optional[str]:
    t          = normalize_text(text)
    correction = _correction(text)
    if correction:
        return correction
    best_score, best = 0, None
    for entry in FAQ:
        score = sum(
            (2 if len(tag) > 15 else 1)
            for tag in entry["tags"] if normalize_text(tag) in t
        )
        if score > best_score:
            best_score, best = score, entry["content"]
    return best if best_score >= 1 else None


# ═══════════════════════════════════════════════════════════════════════════════
#  NLP — Extraction et classification
# ═══════════════════════════════════════════════════════════════════════════════

def detect_lang(text: str) -> str:
    t  = normalize_text(text)
    fr = sum(1 for w in [
        "je", "cherche", "besoin", "comment", "combien", "prix",
        "pour", "dans", "sur", "bonjour", "merci", "veux",
        "voudrais", "quel", "les", "des", "un", "une"
    ] if f" {w} " in f" {t} ")
    en = sum(1 for w in [
        "i", "need", "find", "where", "how", "much", "price",
        "looking", "near", "can", "you", "help", "hello", "want"
    ] if f" {w} " in f" {t} ")
    return "en" if en > fr and en >= 2 else "fr"


def extract_domaine(text: str) -> Optional[str]:
    t = normalize_text(text)
    for kw in sorted(KW2DOM.keys(), key=len, reverse=True):
        if kw in t:
            return KW2DOM[kw]
    return None


def extract_location(text: str) -> Optional[str]:
    t = normalize_text(text)
    for v in sorted(VILLES, key=len, reverse=True):
        if normalize_text(v) in t:
            return v
    geo = ["a ", "au ", "en ", "vers ", "pres de ", "autour de ", "quartier ", "zone "]
    if not any(g in t for g in geo):
        return None
    for pat in [
        r"(?:à|au|en|vers|près de|autour de|a |pres de)\s+([A-ZÀ-Ÿa-zà-ÿ][a-zà-ÿ\-]{2,}(?:\s+[A-Za-zà-ÿ\-]+)?)",
        r"(?:quartier|commune de|zone de?)\s+([A-ZÀ-Ÿa-zà-ÿ][a-zà-ÿ\-]{2,}(?:\s+[A-Za-zà-ÿ\-]+)?)",
    ]:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            c = m.group(1).strip()
            if len(c) > 2 and normalize_text(c) not in STOP_LOC:
                return c
    return None


def extract_radius(text: str) -> float:
    m = re.search(r"(\d+)\s*km", normalize_text(text))
    if m:
        return min(float(m.group(1)), 100)
    return 10 if any(w in normalize_text(text) for w in ["proche", "pres", "coin"]) else 20


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
    t  = normalize_text(text)
    wc = len(t.split())

    SAL = ["bonjour", "bonsoir", "salut", "hello", "hi ", "salam", "alafia", "bonne journee"]
    if any(s in t for s in SAL) and wc <= 5:
        return "salutation"

    if any(s in t for s in ["merci", "thank you", "thanks"]) and wc <= 6:
        return "remerciement"

    if any(s in t for s in ["au revoir", "bye", "a bientot", "tchao"]) and wc <= 5:
        return "aurevoir"

    if any(s in t for s in [
        "comment tu t'appelle", "qui es-tu", "c'est quoi careasy",
        "c'est quoi carai", "que peux-tu faire", "tu es qui", "presente-toi",
        "qu'est-ce que careasy",
    ]):
        return "bot_info"

    if any(s in t for s in ["comment tu vas", "tu vas bien", "ca va"]) and wc <= 5:
        return "perso"

    # Détecter un diagnostic AVANT le reste
    if detect_diagnostic_intent(text):
        return "diagnostic"

    FAQ_KW = [
        "comment creer", "comment modifier", "comment supprimer", "comment payer",
        "comment annuler", "comment prendre", "comment envoyer", "comment activer",
        "comment ajouter", "comment inscrire", "comment fonctionne", "comment ca",
        "qu'est-ce que", "ca fonctionne", "devenir prestataire", "inscrire mon entreprise",
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
            "numero 1", "numero 2", "le 1", "le 2", "le 3"
        ]
        VAGUE = [
            "celui-la", "cet endroit", "ce prestataire",
            "cette entreprise", "la-bas", "ce garage"
        ]
        FKWS = [
            "numero", "contact", "appeler", "whatsapp", "telephone",
            "adresse", "situe", "localisation", "prix", "combien",
            "horaire", "ouvre", "itineraire", "aller", "route"
        ]
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
    t    = normalize_text(text)
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
            normalize_text(s.get("name") or ""),
            normalize_text((s.get("entreprise") or {}).get("name") or "")
        ]:
            for word in fname.split():
                if len(word) >= 4 and word in t:
                    return s

    FKWS = ["numero", "contact", "appeler", "whatsapp", "telephone", "adresse", "prix", "horaire"]
    if len(text.split()) <= 6 and any(f in t for f in FKWS):
        return svcs[0]

    return None


def resolve_all(text: str, ctx: Dict) -> List[Dict]:
    t     = normalize_text(text)
    MULTI = [
        "tous", "toutes", "chacun", "leurs numeros", "leurs contacts",
        "leurs adresses", "les prestataires", "tous les"
    ]
    if any(m in t for m in MULTI):
        return ctx.get("last_services", [])
    return []


# ═══════════════════════════════════════════════════════════════════════════════
#  MÉMOIRE (Redis + RAM)
# ═══════════════════════════════════════════════════════════════════════════════

async def mem_get(cid: str) -> Dict:
    if redis_client:
        try:
            raw = await redis_client.get(f"carai10:{cid}")
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
                f"carai10:{cid}", 14400,
                json.dumps(data, ensure_ascii=False, default=str)
            )
        except Exception:
            pass
    for turn in data["history"]:
        if turn.get("role") == "user":
            _cluster_intent(turn.get("intent", "general"), turn.get("content", ""))
    _LEARN["stats"]["total"] += 1


# ═══════════════════════════════════════════════════════════════════════════════
#  GÉOCODAGE
# ═══════════════════════════════════════════════════════════════════════════════

async def geocode(location: str) -> Optional[Tuple[float, float]]:
    key = normalize_text(location)
    if key in _GEO:
        return _GEO[key]

    try:
        async with httpx.AsyncClient(timeout=8) as c:
            r = await c.get(
                f"{LARAVEL_BASE}/ai/locations",
                params={"q": location, "limit": 1}
            )
            if r.status_code == 200:
                data = r.json().get("data", [])
                if data:
                    coords: Tuple[float, float] = (
                        float(data[0]["latitude"]),
                        float(data[0]["longitude"])
                    )
                    _GEO[key] = coords
                    return coords
    except Exception as e:
        print(f"[GEO] Laravel: {e}")

    if USE_NOMINATIM:
        try:
            async with httpx.AsyncClient(timeout=8) as c:
                r = await c.get(
                    "https://nominatim.openstreetmap.org/search",
                    params={
                        "q": f"{location}, Bénin",
                        "format": "json", "limit": 1, "countrycodes": "bj"
                    },
                    headers={"User-Agent": "CarEasy-CarAI/10.0"},
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
                    params={"address": f"{location}, Bénin", "key": GOOGLE_MAPS_KEY},
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
    return (
        f"https://www.openstreetmap.org/directions"
        f"?engine=fossgis_osrm_car&route={ulat},{ulng};{dlat},{dlng}"
    )


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
    limit: int = 10
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
    e = s.get("entreprise", {}) or {}
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
            r = await c.get(
                f"{LARAVEL_BASE}/ai/services",
                params={"domaine": domaine, "limit": limit}
            )
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
    e = s.get("entreprise") or {}
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
            "id": e.get("id"), "name": e.get("name"),
            "address": address,
            "latitude": e.get("latitude"), "longitude": e.get("longitude"),
            "call_phone": e.get("call_phone"), "whatsapp_phone": e.get("whatsapp_phone"),
            "logo": e.get("logo"), "status_online": e.get("status_online"),
        },
    }


def fmt_price(s: Dict) -> str:
    if s.get("is_price_on_request"):
        return "sur devis"
    p, pp = s.get("price"), s.get("price_promo")
    try:
        if pp and s.get("has_promo") and p:
            return f"{int(float(pp)):,} FCFA (promo)".replace(",", " ")
        if p:
            return f"{int(float(p)):,} FCFA".replace(",", " ")
    except (TypeError, ValueError):
        pass
    return "prix non renseigné"


def fmt_hours(s: Dict) -> str:
    if s.get("is_always_open") or s.get("is_open_24h"):
        return "ouvert 24h/24"
    st, et = s.get("start_time"), s.get("end_time")
    return f"{st} - {et}" if st and et else "horaires non renseignés"


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
    domaine: Optional[str],
    location: Optional[str],
    ulat: Optional[float],
    ulng: Optional[float],
    ctx: Dict,
    radius: float = 20,
) -> Tuple[List[Dict], bool]:
    services: List[Dict] = []
    has_gps    = (ulat is not None and ulng is not None)
    ctx_lat    = ctx.get("last_lat")
    ctx_lng    = ctx.get("last_lng")
    has_ctx_gps = (ctx_lat is not None and ctx_lng is not None)

    if has_gps:
        services = await api_nearby(ulat, ulng, domaine, radius, limit=10)
        if not services and radius < 50:
            services = await api_nearby(ulat, ulng, domaine, 50, limit=10)
        if not services and radius < 100:
            services = await api_nearby(ulat, ulng, domaine, 100, limit=10)
        if services:
            return services, True

    if not services and has_ctx_gps and not location:
        services = await api_nearby(float(ctx_lat), float(ctx_lng), domaine, radius, limit=10)
        if not services:
            services = await api_nearby(float(ctx_lat), float(ctx_lng), domaine, 100, limit=10)
        if services:
            return services, True

    if not services and location:
        coords = await geocode(location)
        if coords:
            services = await api_nearby(coords[0], coords[1], domaine, radius, limit=10)
            if not services:
                services = await api_nearby(coords[0], coords[1], domaine, 100, limit=10)
        if services:
            return services, True

    if not services and domaine:
        services = await api_by_domaine(domaine, limit=15)
        if services:
            return services, True

    if not services and has_gps:
        services = await api_nearby(ulat, ulng, None, 50, limit=10)
        if services:
            return services, False

    if not services and has_ctx_gps:
        services = await api_nearby(float(ctx_lat), float(ctx_lng), None, 50, limit=10)
        if services:
            return services, False

    if not services and location:
        coords = await geocode(location)
        if coords:
            services = await api_nearby(coords[0], coords[1], None, 100, limit=10)
        if services:
            return services, False

    if not services:
        services = await api_services_all(None, limit=15)
        if services:
            return services, False

    return services, False


# ═══════════════════════════════════════════════════════════════════════════════
#  MOTEUR DE RÉPONSE AUTONOME — Templates riches, sans Ollama
# ═══════════════════════════════════════════════════════════════════════════════

class ReplyEngine:
    """
    Génère des réponses naturelles et riches sans LLM externe.
    Utilise des templates, des arbres de décision et la composition dynamique.
    """

    @staticmethod
    def salutation(lang: str) -> str:
        fr = [
            "Bonjour ! Je suis CarAI, votre assistant CarEasy Bénin. Que puis-je faire pour vous ?",
            "Bonjour ! Comment puis-je vous aider aujourd'hui ?",
            "Bonsoir ! Dites-moi ce que vous recherchez — garage, dépannage, vulcanisateur...",
            "Salut ! CarAI à votre service. Quelle est votre recherche ?",
        ]
        en = [
            "Hello! I'm CarAI, your CarEasy Benin assistant. How can I help you?",
            "Hi there! Looking for a mechanic, fuel station or roadside help?",
        ]
        return random.choice(en if lang == "en" else fr)

    @staticmethod
    def remerciement(lang: str) -> str:
        fr = ["Avec plaisir !", "De rien, bonne route !", "Je suis là pour ça !", "Toujours disponible pour vous !"]
        en = ["You're welcome!", "Happy to help!", "Glad I could assist!"]
        return random.choice(en if lang == "en" else fr)

    @staticmethod
    def aurevoir(lang: str) -> str:
        fr = ["À bientôt ! Bonne route.", "Au revoir ! Conduisez prudemment.", "À la prochaine !"]
        en = ["Goodbye! Drive safe.", "See you soon!", "Take care!"]
        return random.choice(en if lang == "en" else fr)

    @staticmethod
    def bot_info(lang: str) -> str:
        return (
            f"Je suis CarAI v{APP_VERSION}, l'assistant intelligent de l'application mobile CarEasy Bénin.\n\n"
            f"Je peux vous aider avec :\n"
            f"- Trouver des prestataires automobiles (garage, vulcanisateur, station, etc.)\n"
            f"- Diagnostiquer une panne ou anomalie sur votre vehicule\n"
            f"- Utiliser l'application CarEasy (compte, rendez-vous, paiement...)\n\n"
            f"Je fonctionne 24h/24 et j'ai accès en temps réel à la base de données CarEasy.\n"
            f"Site web : {SITE_URL}"
        ) if lang != "en" else (
            f"I'm CarAI v{APP_VERSION}, the intelligent assistant of the CarEasy Benin mobile app.\n"
            f"I can help you find auto service providers, diagnose car issues, and use the CarEasy app.\n"
            f"Website: {SITE_URL}"
        )

    @staticmethod
    def perso() -> str:
        return random.choice([
            "Je fonctionne parfaitement, merci ! Dites-moi comment je peux vous aider.",
            "Toujours opérationnel ! En quoi puis-je vous être utile ?",
            "Très bien ! Que puis-je faire pour vous aujourd'hui ?",
        ])

    @staticmethod
    def faq_response(msg: str) -> str:
        ans = faq_lookup(msg)
        if ans:
            return ans
        return (
            f"Je n'ai pas trouvé de réponse précise pour votre question sur l'application CarEasy.\n\n"
            f"Contactez le support :\n"
            f"- Email : support@careasy.bj\n"
            f"- Dans l'app : Paramètres → Aide et support\n"
            f"- Site web : {SITE_URL}\n\n"
            f"L'équipe répond généralement dans la journée (lundi-vendredi 8h-18h)."
        )

    @staticmethod
    def urgence(services: List[Dict], domaine: Optional[str], ulat: Optional[float], ulng: Optional[float]) -> str:
        lines = ["Situation d'urgence détectée — je cherche de l'aide immédiate.\n"]
        if services:
            e0  = (services[0].get("entreprise") or {})
            ph  = e0.get("call_phone") or e0.get("whatsapp_phone")
            nom = e0.get("name", "Prestataire")
            dist = services[0].get("distance_km")
            dst  = f" — à {dist:.1f} km de vous" if dist else ""
            lines.append(f"Prestataire le plus proche{dst} :")
            lines.append(f"{nom}")
            if e0.get("call_phone"):
                lines.append(f"Tel : {e0['call_phone']}")
            if e0.get("whatsapp_phone"):
                lines.append(f"WhatsApp : {e0['whatsapp_phone']}")
            if ulat and ulng and e0.get("latitude") and e0.get("longitude"):
                try:
                    d_km = haversine(ulat, ulng, float(e0["latitude"]), float(e0["longitude"]))
                    url  = map_link(ulat, ulng, float(e0["latitude"]), float(e0["longitude"]))
                    lines.append(f"Itineraire : {url} ({d_km:.1f} km ~ {dur(d_km)})")
                except Exception:
                    pass
            lines.append("")
            if len(services) > 1:
                lines.append("Autres options disponibles :")
                for s in services[1:3]:
                    e = s.get("entreprise") or {}
                    lines.append(f"- {e.get('name', 'Inconnu')} | Tel : {e.get('call_phone') or '—'}")
        else:
            lines.append("Appelez le 166 (Gendarmerie) ou le 197 (Police) pour assistance.")
            lines.append(f"Cherchez un prestataire sur {SITE_URL}")
        return "\n".join(lines)

    @staticmethod
    def services_found(
        services: List[Dict],
        domaine: Optional[str],
        location: Optional[str],
        ulat: Optional[float],
        ulng: Optional[float],
        found_exact: bool,
    ) -> str:
        if not services:
            return (
                f"Aucun prestataire n'est encore inscrit sur CarEasy pour ce service au Bénin.\n"
                f"De nouveaux prestataires rejoignent CarEasy chaque semaine.\n\n"
                f"Vous êtes prestataire ? Inscrivez-vous sur l'application CarEasy — "
                f"essai gratuit de 30 jours !"
            )

        lieu = f"à {location}" if location else ("près de vous" if ulat else "au Bénin")

        lines = []

        if not found_exact and domaine:
            domaines_dispo = list(dict.fromkeys(s.get("domaine") or "Autre" for s in services))
            lines.append(
                f"Pas encore de prestataire en {domaine} {lieu} sur CarEasy. "
                f"Voici ce qui est disponible"
                + (f" {lieu} :" if lieu != "au Bénin" else " au Bénin :")
            )
            for i, s in enumerate(services[:5], 1):
                e    = s.get("entreprise", {}) or {}
                dist = s.get("distance_km")
                dst  = f" ({dist:.1f} km)" if dist is not None else ""
                dom_s = s.get("domaine") or ""
                note  = fmt_rating(s)
                lines.append(
                    f"\n{i}. {e.get('name', 'Inconnu')}{dst} — {dom_s}"
                    + (f" | {note}" if note else "") + "\n"
                    f"   {s.get('name', '')} | {fmt_hours(s)} | {fmt_price(s)}\n"
                    f"   Tel : {e.get('call_phone') or '—'}   WA : {e.get('whatsapp_phone') or '—'}"
                )
            if len(services) > 5:
                lines.append(f"\n... et {len(services) - 5} autre(s) disponible(s) dans l'app.")
            lines.append(f"\nVous êtes prestataire en {domaine} ? Rejoignez CarEasy — essai gratuit 30 jours !")
        else:
            lines.append(f"J'ai trouvé {len(services)} prestataire(s) en {domaine or 'automobile'} {lieu} :")
            for i, s in enumerate(services[:5], 1):
                e    = s.get("entreprise", {}) or {}
                dist = s.get("distance_km")
                dst  = f" ({dist:.1f} km)" if dist is not None else ""
                note  = fmt_rating(s)
                lines.append(
                    f"\n{i}. {e.get('name', 'Inconnu')}{dst}"
                    + (f" — {note}" if note else "") + "\n"
                    f"   {s.get('name', '')} | {fmt_hours(s)} | {fmt_price(s)}\n"
                    f"   Tel : {e.get('call_phone') or '—'}   WA : {e.get('whatsapp_phone') or '—'}"
                )
            if len(services) > 5:
                lines.append(f"\n... et {len(services) - 5} autre(s) dans l'application CarEasy.")

        lines.append("\nVoulez-vous l'itinéraire ou les contacts d'un prestataire en particulier ?")
        return "\n".join(lines)

    @staticmethod
    def followup(
        intent: str, ref_svc: Optional[Dict], all_svcs: List[Dict],
        ulat: Optional[float], ulng: Optional[float]
    ) -> str:
        if all_svcs:
            lines = ["Contacts de tous les prestataires :"]
            for i, s in enumerate(all_svcs, 1):
                e  = s.get("entreprise", {}) or {}
                ph = e.get("call_phone") or "—"
                wa = e.get("whatsapp_phone") or "—"
                lines.append(f"{i}. {e.get('name', 'Inconnu')} — Tel : {ph}  |  WA : {wa}")
            return "\n".join(lines)

        if not ref_svc:
            return "Quel prestataire vous intéresse ? Indiquez le numéro (1, 2, 3...) ou le nom."

        e   = ref_svc.get("entreprise", {}) or {}
        ent = e.get("name", "Ce prestataire")
        svc = ref_svc.get("name", "ce service")
        addr = e.get("google_formatted_address") or e.get("address") or "adresse non renseignée"

        if "contact" in intent:
            ph = e.get("call_phone") or ""
            wa = e.get("whatsapp_phone") or ""
            if not ph and not wa:
                return f"Aucun contact n'est renseigné pour {ent} pour le moment."
            parts = []
            if ph: parts.append(f"Tel : {ph}")
            if wa: parts.append(f"WhatsApp : {wa}")
            return f"{ent} — {' | '.join(parts)}"

        if "adresse" in intent or "itineraire" in intent:
            if ulat and ulng and e.get("latitude") and e.get("longitude"):
                try:
                    d   = haversine(ulat, ulng, float(e["latitude"]), float(e["longitude"]))
                    url = map_link(ulat, ulng, float(e["latitude"]), float(e["longitude"]))
                    return (
                        f"{ent} — {addr}\n"
                        f"Distance : {d:.1f} km (environ {dur(d)})\n"
                        f"Itineraire : {url}"
                    )
                except Exception:
                    pass
            return f"{ent} : {addr}"

        if "prix" in intent:
            return f"Le service {svc} chez {ent} est à {fmt_price(ref_svc)}."

        if "horaire" in intent:
            return f"{ent} est {fmt_hours(ref_svc)}."

        ph   = e.get("call_phone") or "—"
        wa   = e.get("whatsapp_phone") or "—"
        note = fmt_rating(ref_svc)
        return (
            f"{ent} ({svc})\n"
            f"Tel : {ph}  |  WhatsApp : {wa}\n"
            f"Adresse : {addr}\n"
            f"Horaires : {fmt_hours(ref_svc)}\n"
            f"Prix : {fmt_price(ref_svc)}"
            + (f"\n{note}" if note else "")
        )

    @staticmethod
    def general(msg: str, ctx: Dict, lang: str) -> str:
        t = normalize_text(msg)

        if any(w in t for w in ["combien", "prix", "tarif", "cout"]):
            dom = ctx.get("last_domaine")
            if dom:
                return (
                    f"Le prix pour {dom} varie selon le prestataire et le type d'intervention.\n"
                    f"Utilisez CarEasy pour comparer les tarifs en temps réel et réserver directement.\n"
                    f"Les prestataires indiquent leurs prix et promotions dans l'application."
                )

        if any(w in t for w in ["meilleur", "recommande", "conseil", "top"]):
            return (
                f"Pour trouver le meilleur prestataire automobile près de vous :\n"
                f"1) Ouvrez CarEasy et activez la géolocalisation\n"
                f"2) Filtrez par domaine (garage, vulcanisateur, lavage...)\n"
                f"3) Consultez les avis et notes des clients\n"
                f"4) Contactez directement ou prenez rendez-vous\n\n"
                f"Dites-moi votre ville ou activez votre GPS pour voir les options autour de vous."
            )

        return (
            f"Je suis CarAI, assistant CarEasy Bénin.\n\n"
            f"Dites-moi ce que vous cherchez :\n"
            f"- Un type de service auto (garage, vulcanisateur, lavage...)\n"
            f"- Un diagnostic de panne\n"
            f"- De l'aide pour utiliser l'application CarEasy\n\n"
            f"Exemple : 'garage mecanique à Cotonou' ou 'mon moteur chauffe'"
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
    diag_code:   Optional[str] = None,
) -> Tuple[str, Optional[Dict]]:
    """
    Génère une réponse complète. Retourne (reply_text, diagnostic_data_or_None).
    """
    _LEARN["stats"]["fallback"] += 1

    # ── DIAGNOSTIC ──────────────────────────────────────────────────────────
    if intent == "diagnostic" and diag_code:
        diag_services = services  # services déjà filtrés sur le domaine recommandé
        reply, diag_data = build_diagnostic_response(diag_code, diag_services, location, ulat)
        return reply, diag_data

    # ── SALUTATIONS ET META ─────────────────────────────────────────────────
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

    # ── FAQ ──────────────────────────────────────────────────────────────────
    if intent == "faq":
        return ReplyEngine.faq_response(msg), None

    # ── SUIVI ────────────────────────────────────────────────────────────────
    if "followup" in intent:
        return ReplyEngine.followup(intent, ref_svc, all_svcs, ulat, ulng), None

    # ── URGENCE ──────────────────────────────────────────────────────────────
    if intent == "urgence":
        return ReplyEngine.urgence(services, domaine, ulat, ulng), None

    # ── RECHERCHE ────────────────────────────────────────────────────────────
    if intent in {"recherche", "general"}:
        return ReplyEngine.services_found(services, domaine, location, ulat, ulng, found_exact), None

    # ── GÉNÉRAL ──────────────────────────────────────────────────────────────
    return ReplyEngine.general(msg, ctx, lang), None


# ═══════════════════════════════════════════════════════════════════════════════
#  SUGGESTIONS CONTEXTUELLES
# ═══════════════════════════════════════════════════════════════════════════════

SUGG_BASE = [
    "Trouver un garage mecanique",
    "Vulcanisateur disponible",
    "Lavage auto",
    "Electricien auto",
    "Depannage routier",
]

SUGG_DIAG = [
    "Mon moteur chauffe",
    "Voyant rouge allume",
    "Ma voiture ne demarre pas",
    "Bruit de freinage",
    "Clim ne refroidit pas",
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
#  ENDPOINT PRINCIPAL — v10.0
# ═══════════════════════════════════════════════════════════════════════════════

@app.post("/chat", response_model=ChatResponse)
async def chat(req: ChatRequest, bg: BackgroundTasks):
    t0 = time.time()

    mem     = await mem_get(req.conversation_id)
    ctx     = mem["ctx"]
    history = mem["history"]

    lang     = req.language or detect_lang(req.message)
    intent   = intent_classify(req.message, ctx)
    domaine  = extract_domaine(req.message)
    location = extract_location(req.message)
    radius   = extract_radius(req.message)
    wc       = len(req.message.split())

    # Identifier le code de diagnostic si intent == diagnostic
    diag_code: Optional[str] = None
    if intent == "diagnostic":
        diag_code = detect_diagnostic_intent(req.message)
        # Si on a un diagnostic, on cherche des prestataires dans le bon domaine
        if diag_code and DIAG_SYMPTOMS.get(diag_code):
            domaine = DIAG_SYMPTOMS[diag_code]["domaine_recommande"]

    FOLLOW_INTENTS = {
        "followup_contact", "followup_adresse", "followup_prix",
        "followup_horaires", "followup_itineraire", "followup_info", "urgence",
    }

    # Héritage du contexte
    if not domaine and ctx.get("last_domaine") and (
        intent in FOLLOW_INTENTS or ctx.get("last_services") or wc <= 5
    ):
        domaine = ctx.get("last_domaine")

    if not location and ctx.get("last_location") and intent in {
        "followup_adresse", "followup_itineraire", "recherche"
    }:
        location = ctx.get("last_location")

    ref_svc    = resolve_ref(req.message, ctx)
    all_svcs   = resolve_all(req.message, ctx)
    is_followup = bool(ref_svc or all_svcs) and intent in FOLLOW_INTENTS

    services:    List[Dict] = []
    found_exact: bool = True
    mapurl:      Optional[str] = None
    itinerary:   Optional[Dict] = None

    should_query_db = _needs_db(intent, domaine, location, ctx, wc)

    if should_query_db and not is_followup:
        services, found_exact = await search_services_robust(
            domaine=domaine,
            location=location,
            ulat=req.latitude,
            ulng=req.longitude,
            ctx=ctx,
            radius=radius,
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
        intent=intent,
        msg=req.message,
        services=active,
        ref_svc=ref_svc,
        all_svcs=all_svcs,
        domaine=domaine,
        location=location,
        ctx=ctx,
        lang=lang,
        ulat=req.latitude,
        ulng=req.longitude,
        found_exact=found_exact,
        diag_code=diag_code,
    )

    # Nettoyage final
    reply = re.sub(r"http://localhost[^\s]*", SITE_URL, reply)
    reply = re.sub(r"localhost:\d+", SITE_URL, reply)

    cleaned = [clean_svc(s) for s in services[:8]]

    history.append({
        "role": "user", "content": req.message,
        "intent": intent, "domaine": domaine, "location": location,
        "diag_code": diag_code,
        "ts": datetime.now().isoformat(),
    })
    history.append({
        "role": "assistant", "content": reply[:400],
        "intent": intent, "services": cleaned,
        "ts": datetime.now().isoformat(),
    })

    if domaine:        ctx["last_domaine"]   = domaine
    if location:       ctx["last_location"]  = location
    if req.latitude:   ctx["last_lat"]       = req.latitude
    if req.longitude:  ctx["last_lng"]       = req.longitude
    if cleaned:        ctx["last_services"]  = cleaned
    if diag_code:      ctx["last_diag"]      = diag_code

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

    return ChatResponse(
        reply=reply,
        services=cleaned if cleaned else (ctx.get("last_services", [])[:3] if is_followup else []),
        map_url=mapurl,
        itinerary=itinerary,
        intent=domaine or intent,
        language=lang,
        suggestions=suggestions(domaine, location, ctx, intent),
        confidence=_confidence(req.message, intent),
        diagnostic=diag_data,
    )


# ═══════════════════════════════════════════════════════════════════════════════
#  ENDPOINT DIAGNOSTIC DÉDIÉ
# ═══════════════════════════════════════════════════════════════════════════════

@app.post("/diagnostic")
async def diagnostic_endpoint(req: DiagRequest):
    """
    Endpoint dédié pour un diagnostic approfondi à partir d'une liste de symptômes.
    Idéal pour une intégration dans un formulaire de l'app Flutter.
    """
    if not req.symptoms:
        raise HTTPException(400, "Au moins un symptôme requis")

    # Rechercher le meilleur code de diagnostic
    combined = " ".join(req.symptoms)
    diag_code = detect_diagnostic_intent(combined)

    if not diag_code:
        # Recherche élargie en cherchant chaque symptôme individuellement
        for s in req.symptoms:
            code = detect_diagnostic_intent(s)
            if code:
                diag_code = code
                break

    if not diag_code:
        return {
            "found": False,
            "message": "Symptômes non reconnus. Consultez un mécanicien pour un diagnostic précis.",
            "suggestion": "Utilisez CarEasy pour trouver un spécialiste en diagnostic automobile.",
        }

    diag = DIAG_SYMPTOMS[diag_code]
    _LEARN["stats"]["diag_queries"] += 1
    _LEARN["diag_stats"][diag_code] = _LEARN["diag_stats"].get(diag_code, 0) + 1

    # Scoring des causes selon les symptômes multiples
    cause_scores = {}
    for i, cause in enumerate(diag["causes_probables"]):
        base_score = cause["probabilite"]
        # Bonus si plusieurs symptômes concordants
        bonus = min(len(req.symptoms) - 1, 3) * 2
        cause_scores[i] = base_score + bonus

    return {
        "found":              True,
        "code":               diag_code,
        "titre":              diag["titre"],
        "urgence":            diag["urgence"],
        "symptomes_detectes": req.symptoms,
        "causes_probables":   diag["causes_probables"],
        "diagnostic_rapide":  diag["diagnostic_rapide"],
        "actions_immediates": diag["actions_immediates"],
        "domaine_recommande": diag["domaine_recommande"],
        "cout_estimatif":     diag["cout_estimatif"],
        "delai_recommande":   diag["delai_recommande"],
        "vehicle":            req.vehicle,
        "mileage":            req.mileage,
        "note_mileage": (
            f"À {req.mileage:,} km, vérifiez la courroie de distribution, les plaquettes et les filtres."
            if req.mileage and req.mileage > 100000 else None
        ),
    }


@app.get("/diagnostic/symptoms")
async def list_symptoms():
    """Retourne tous les codes de diagnostic disponibles avec leurs mots-clés."""
    return {
        "count": len(DIAG_SYMPTOMS),
        "diagnostics": [
            {
                "code":               code,
                "titre":              data["titre"],
                "urgence":            data["urgence"],
                "domaine_recommande": data["domaine_recommande"],
                "keywords_sample":    data["keywords"][:5],
            }
            for code, data in DIAG_SYMPTOMS.items()
        ]
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
            _LEARN["faq_corrections"][_h(req.message_text)] = req.comment
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
        "status":          "ok",
        "version":         APP_VERSION,
        "mode":            "autonomous (rules+nlp+db)",
        "redis":           redis_ok,
        "database":        db_ok,
        "sessions":        len(_RAM),
        "geo_cached":      len(_GEO),
        "diag_supported":  len(DIAG_SYMPTOMS),
        "learn":           _LEARN["stats"],
        "laravel":         LARAVEL_BASE,
        "site":            SITE_URL,
        "ts":              datetime.now().isoformat(),
    }


@app.delete("/conversation/{cid}")
async def clear_conv(cid: str):
    _RAM.pop(cid, None)
    if redis_client:
        try:
            await redis_client.delete(f"carai10:{cid}")
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
        "stats":         _LEARN["stats"],
        "intents":       {k: len(v) for k, v in _LEARN["intent_clusters"].items()},
        "bad":           len(_LEARN["bad_patterns"]),
        "good":          len(_LEARN["good_patterns"]),
        "corrections":   len(_LEARN["faq_corrections"]),
        "patterns":      len(_LEARN["pattern_scores"]),
        "diag_stats":    _LEARN["diag_stats"],
        "top_queries":   sorted(
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
    test_cases = [
        ("je cherche de l'essence", "Station d'essence"),
        ("je cherche de l\u2019essence", "Station d'essence"),
        ("garage mecanique pres de moi", "Garage mecanique"),
        ("je veux faire une vidange", "Changement d'huile"),
        ("mon pneu est creve", "Pneumatique / vulcanisation"),
        ("je cherche un electricien auto", "Electricien auto"),
        ("besoin d'un depanneur urgent", "Depannage / remorquage"),
    ]
    for text, expected in test_cases:
        dom = extract_domaine(text)
        status = "OK" if dom == expected else f"FAIL (attendu: {expected})"
        results[f"extract_domaine_{normalize_text(text)[:30]}"] = f"{status} — '{dom}'"

    # Tests diagnostic
    diag_cases = [
        ("ma voiture ne demarre pas", "moteur_ne_demarre_pas"),
        ("le moteur chauffe trop", "surchauffe_moteur"),
        ("mon pneu est a plat", "pneu_probleme"),
        ("la pedale de frein est molle", "frein_probleme"),
        ("voyant rouge allume tableau de bord", "voyant_allume"),
        ("la clim ne refroidit plus", "climatisation_probleme"),
        ("bruit etrange au virage", "bruit_suspect"),
    ]
    for text, expected in diag_cases:
        code = detect_diagnostic_intent(text)
        status = "OK" if code == expected else f"FAIL (attendu: {expected})"
        results[f"diag_{normalize_text(text)[:30]}"] = f"{status} — '{code}'"

    try:
        coords = await geocode("Abomey-Calavi")
        results["geocode_abomey_calavi"] = f"OK — {coords}" if coords else "Aucun résultat"
    except Exception as e:
        results["geocode_abomey_calavi"] = f"ERREUR: {e}"

    results["redis"]         = "Connecté" if redis_client else "RAM actif (non bloquant)"
    results["site_url"]      = SITE_URL
    results["version"]       = APP_VERSION
    results["kw2dom_count"]  = str(len(KW2DOM))
    results["diag_count"]    = str(len(DIAG_SYMPTOMS))
    results["mode"]          = "autonomous (no Ollama needed)"

    return {"version": APP_VERSION, "tests": results}