"""
CarAI v9.0 — Assistant IA CarEasy Bénin
========================================
CORRECTIONS v9 (par rapport à v6/v7) :

1. BDD TOUJOURS INTERROGÉE
   - SKIP_DB réduit à seulement : salutation, remerciement, aurevoir, bot_info, perso
   - "general" et "faq" ne skipent plus la BDD si un domaine ou une ville est détectable
   - Nouveau : _needs_db() — logique centralisée, lisible, testable

2. CONTEXTE CONVERSATIONNEL FORT
   - Si ctx["last_domaine"] existe, il est injecté automatiquement dans toute requête
   - "Et il y a des entreprises ?" → ré-interroge la BDD avec le domaine du contexte
   - Messages courts (<= 4 mots) avec contexte domaine → toujours recherche BDD

3. PERSONALITÉ HUMAINE
   - Prompt SYS v9 : direct, chaleureux, béninois, jamais robotique
   - Fallback : variantes aléatoires, formulations naturelles
   - Suppression définitive de "localhost" dans toutes les réponses

4. APPRENTISSAGE NON SUPERVISÉ
   - _track_query() enrichi avec taux de succès
   - Feedback négatif → correction mémorisée et réutilisée
   - Stats db_hits/db_misses pour monitorer
"""

import os, json, re, math, httpx, time, hashlib, random
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
REDIS_URL       = os.getenv("REDIS_URL",        "redis://localhost:6379")
USE_NOMINATIM   = os.getenv("USE_NOMINATIM",    "true").lower() == "true"
GOOGLE_MAPS_KEY = os.getenv("GOOGLE_MAPS_KEY",  "")
OLLAMA_URL      = os.getenv("OLLAMA_URL",       "http://localhost:11434")
OLLAMA_MODEL    = os.getenv("OLLAMA_MODEL",     "llama3")
USE_OLLAMA      = os.getenv("USE_OLLAMA",       "true").lower() == "true"
SITE_URL        = os.getenv("FRONTEND_URL",     "https://careasy.bj")
LEARN_FILE      = os.getenv("LEARN_FILE",       "/tmp/carai_learn_v9.json")

app = FastAPI(title="CarAI v9.0", version="9.0.0", docs_url="/docs")
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
    "stats": {
        "total": 0, "ollama_ok": 0, "fallback": 0,
        "db_hits": 0, "db_misses": 0,
        "feedback_pos": 0, "feedback_neg": 0,
    },
}


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

class FeedbackRequest(BaseModel):
    conversation_id: str
    message_text:    str
    reply_text:      str
    rating:          int
    comment:         Optional[str] = None


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

    if USE_OLLAMA:
        try:
            async with httpx.AsyncClient(timeout=5) as c:
                r = await c.get(f"{OLLAMA_URL}/api/tags")
                if r.status_code == 200:
                    models = [m["name"] for m in r.json().get("models", [])]
                    ok = any(OLLAMA_MODEL in m for m in models)
                    print(f"[CarAI] Ollama '{OLLAMA_MODEL}' {'OK' if ok else 'ABSENT'}")
        except Exception as e:
            print(f"[CarAI] Ollama KO: {e}")

    print(f"[CarAI] v9.0 | {LARAVEL_BASE} | Ollama={'ON' if USE_OLLAMA else 'OFF'}")


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
        print(f"[Learn] Chargement: {e}")


def _save_learn():
    try:
        with open(LEARN_FILE, "w", encoding="utf-8") as f:
            json.dump(_LEARN, f, ensure_ascii=False, indent=2, default=str)
    except Exception as e:
        print(f"[Learn] Sauvegarde: {e}")


def _h(text: str) -> str:
    return hashlib.md5(text.lower().strip().encode()).hexdigest()[:12]


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
    """Mémorise les combinaisons domaine/ville fréquentes pour l'apprentissage."""
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
    return 0.9 if n > 20 else (0.8 if n > 10 else 0.70)


# ═══════════════════════════════════════════════════════════════════════════════
#  DOMAINES & VILLES
# ═══════════════════════════════════════════════════════════════════════════════

DOMAINES: Dict[str, List[str]] = {
    "Pneumatique / vulcanisation": [
        "pneu", "pneus", "vulcanis", "crevaison", "crevé", "roue crevée",
        "chambre à air", "pneumatique", "gomme", "tubeless",
    ],
    "Garage mécanique": [
        "garage", "mécanicien", "mécani", "réparer", "réparation", "panne",
        "moteur", "boîte vitesse", "frein", "embrayage", "courroie",
        "révision", "entretien", "contrôle", "mechanic", "repair",
    ],
    "Lavage automobile": [
        "lavage", "laver voiture", "carwash", "car wash", "nettoyage voiture",
        "polissage", "lustrage",
    ],
    "Électricien auto": [
        "électricien auto", "batterie voiture", "alternateur", "démarreur auto",
        "câblage", "phare voiture", "electrician auto",
    ],
    "Climatisation auto": [
        "climatisation", "clim voiture", "air conditionné voiture", "recharge clim",
    ],
    "Peinture auto": [
        "peinture voiture", "rayure carrosserie", "retouche peinture", "vernis voiture",
    ],
    "Tôlerie": ["tôlerie", "carrosserie", "bosselage", "débosselage", "dent voiture"],
    "Dépannage / remorquage": [
        "dépannage", "dépanneur", "remorquage", "voiture en panne",
        "sos auto", "assistance routière", "towing",
    ],
    "Changement d'huile": ["vidange", "huile moteur", "filtre huile", "oil change"],
    "Diagnostic automobile": [
        "diagnostic", "scanner voiture", "valise diagnostic", "code erreur voiture",
        "voyant allumé", "check engine", "obd",
    ],
    "Station d'essence": [
        "essence", "carburant", "gasoil", "diesel", "station service",
        "faire le plein", "pompe essence",
    ],
    "Location de voitures": ["location voiture", "louer voiture", "voiture de location"],
    "Assurance automobile": ["assurance auto", "sinistre auto", "police assurance"],
    "École de conduite": ["permis de conduire", "auto-école", "autoecole", "driving school"],
    "Vente de pièces détachées": [
        "pièces détachées", "spare part", "plaquette frein", "disque frein",
        "bougie", "filtre voiture", "pièce voiture",
    ],
    "Réparation moto": ["moto", "zémidjan", "zem", "zemidjan", "scooter", "moto taxi"],
    "Vente de voitures": [
        "acheter voiture", "achat voiture", "vente voiture", "concessionnaire",
        "voiture d'occasion", "voiture neuve",
    ],
    "Maintenance poids lourds": ["poids lourd", "camion", "semi-remorque", "gros porteur"],
    "Vente de motos": ["acheter moto", "vente moto", "moto neuve", "moto occasion"],
    "Vente de vélos / entretien": ["vélo", "bicyclette", "vtt", "réparation vélo"],
}

KW2DOM: Dict[str, str] = {
    kw.lower(): dom for dom, kws in DOMAINES.items() for kw in kws
}

VILLES = [
    "Cotonou", "Porto-Novo", "Parakou", "Abomey", "Bohicon", "Calavi",
    "Ouidah", "Natitingou", "Lokossa", "Djougou", "Kandi", "Malanville",
    "Nikki", "Savalou", "Savè", "Tchaourou", "Bassila", "Dogbo", "Aplahoue",
    "Dassa-Zoumé", "Abomey-Calavi", "Allada", "Kpomassè", "Zè",
    "Adjarra", "Adjohoun", "Sèmè-Kpodji", "Godomey", "Fidjrossè",
    "Akpakpa", "Cadjèhoun", "Gbégamey", "Haie Vive", "Vèdoko",
    "Zogbo", "Agla", "Jéricho", "Mènontin", "Akogbato", "Cocotiers",
    "Dantokpa", "Houéyiho", "Pobe", "Ketou", "Sakete", "Ifangni",
    "Avrankou", "Dangbo", "Grand-Popo", "Athiémé", "Come",
]

STOP_LOC = {
    "moi", "vous", "nous", "lui", "elle", "eux", "cela", "ça", "toi",
    "plus", "moins", "tout", "rien", "ici", "là", "bien", "mal",
    "savoir", "faire", "chercher", "trouver", "aide", "careasy", "carai",
}


# ═══════════════════════════════════════════════════════════════════════════════
#  FAQ
# ═══════════════════════════════════════════════════════════════════════════════

FAQ: List[Dict] = [
    {
        "tags": ["inscription", "créer compte", "devenir prestataire", "inscrire entreprise",
                 "rejoindre", "soumettre dossier", "enregistrer entreprise", "créer une entreprise",
                 "comment créer entreprise"],
        "content": (
            "Pour inscrire votre entreprise sur CarEasy : "
            "1) Créez un compte avec votre email ou téléphone. "
            "2) Cliquez sur Devenir prestataire et remplissez le formulaire. "
            "3) Téléchargez vos documents : IFU, RCCM et certificat d'immatriculation. "
            "4) Soumettez votre dossier — validation sous 24 à 48 heures ouvrables. "
            "5) Après validation : essai gratuit de 30 jours avec 3 services maximum."
        )
    },
    {
        "tags": ["documents requis", "ifu", "rccm", "certificat", "pièces dossier"],
        "content": "Documents requis : IFU, RCCM, certificat d'immatriculation. Formats : PDF, JPG, PNG — max 5 Mo chacun."
    },
    {
        "tags": ["validation", "délai validation", "dossier en attente"],
        "content": "La validation prend 24 à 48 heures ouvrables. Vous êtes notifié par email et SMS."
    },
    {
        "tags": ["dossier rejeté", "refus", "pourquoi rejeté"],
        "content": "En cas de refus, la raison est précisée dans la notification. Causes fréquentes : documents illisibles ou informations incomplètes. Vous pouvez corriger et soumettre à nouveau."
    },
    {
        "tags": ["mot de passe oublié", "réinitialiser", "forgot password", "reset"],
        "content": "Pour réinitialiser votre mot de passe : cliquez Mot de passe oublié, entrez votre email ou téléphone, saisissez le code OTP à 6 chiffres valable 5 minutes, puis définissez votre nouveau mot de passe."
    },
    {
        "tags": ["rendez-vous", "prendre rdv", "réserver", "booking"],
        "content": "Pour prendre un rendez-vous : ouvrez la fiche d'un service, cliquez Prendre RDV, choisissez la date et le créneau, confirmez. Le prestataire confirme ensuite avec notification à chaque étape."
    },
    {
        "tags": ["annuler rdv", "annuler rendez-vous", "cancel rdv"],
        "content": "Pour annuler un RDV : Mes rendez-vous, sélectionnez le RDV, Annuler, indiquez le motif. Possible uniquement si le statut est En attente ou Confirmé."
    },
    {
        "tags": ["message", "contacter prestataire", "messagerie", "contacter"],
        "content": "Pour contacter un prestataire : fiche du service, bouton Message ou WhatsApp. La messagerie interne supporte texte, images, vidéos, vocaux et localisation."
    },
    {
        "tags": ["abonnement", "plans", "tarifs", "prix careasy", "offres prestataire"],
        "content": (
            "Plans CarEasy prestataire : "
            "Essentiel 25 000 FCFA par mois (5 services). "
            "Professionnel 50 000 FCFA par mois (15 services, statistiques, support prioritaire). "
            "Premium 100 000 FCFA par mois (illimité, SMS clients, API). "
            "Annuel 1 000 000 FCFA par an (Premium + 2 mois offerts). "
            "Essai gratuit 30 jours inclus à la validation."
        )
    },
    {
        "tags": ["essai gratuit", "trial", "30 jours", "période essai"],
        "content": "L'essai gratuit de 30 jours se déclenche automatiquement à la validation. Il inclut 3 services, visibilité clients et gestion des RDV. Un plan payant est requis après les 30 jours."
    },
    {
        "tags": ["payer", "paiement", "fedapay", "mobile money", "orange money", "mtn"],
        "content": "Paiement via FedaPay : Orange Money, MTN Money, Moov Money ou carte bancaire. Allez dans Abonnements, choisissez votre plan et payez. Une facture est envoyée par email."
    },
    {
        "tags": ["support", "aide", "problème", "bug", "contacter careasy"],
        "content": "Support CarEasy : support@careasy.bj ou via WhatsApp sur le site. Disponible du lundi au vendredi de 8h à 18h."
    },
    {
        "tags": ["créer service", "ajouter service", "publier service"],
        "content": "Pour créer un service : Espace prestataire, Mes services, Ajouter. Renseignez nom, domaine, prix ou sur devis, horaires et photos. Pendant l'essai : 3 services maximum."
    },
    {
        "tags": ["position gps", "géolocalisation", "localisation"],
        "content": "Activez la géolocalisation pour voir les prestataires proches de vous. Ou mentionnez votre quartier ou votre ville directement dans le message."
    },
    {
        "tags": ["laisser avis", "noter", "évaluer", "review", "donner note"],
        "content": "Après une prestation : Mes rendez-vous, onglet Terminés, Laisser un avis. Note de 1 à 5 étoiles avec commentaire optionnel."
    },
]


def faq_lookup(text: str) -> Optional[str]:
    t          = text.lower()
    correction = _correction(text)
    if correction:
        return correction
    best_score, best = 0, None
    for entry in FAQ:
        score = sum(
            (2 if len(tag) > 15 else 1)
            for tag in entry["tags"] if tag in t
        )
        if score > best_score:
            best_score, best = score, entry["content"]
    return best if best_score >= 1 else None


# ═══════════════════════════════════════════════════════════════════════════════
#  NLP — Extraction et classification
# ═══════════════════════════════════════════════════════════════════════════════

def detect_lang(text: str) -> str:
    t  = text.lower()
    for m in ["mɛ̌", "ɖò", "nɔ ", "bló", "wɛ ", "alɔ", "aca"]:
        if m in t:
            return "fon"
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
    t = text.lower()
    for kw in sorted(KW2DOM.keys(), key=len, reverse=True):
        if kw in t:
            return KW2DOM[kw]
    return None


def extract_location(text: str) -> Optional[str]:
    t = text.lower()
    for v in sorted(VILLES, key=len, reverse=True):
        if v.lower() in t:
            return v
    geo = ["à ", "au ", "en ", "vers ", "près de ", "autour de ", "quartier ", "zone "]
    if not any(g in t for g in geo):
        return None
    for pat in [
        r"(?:à|au|en|vers|près de|autour de)\s+([A-ZÀ-Ÿa-zà-ÿ][a-zà-ÿ\-]{2,}(?:\s+[A-Za-zà-ÿ\-]+)?)",
        r"(?:quartier|commune de|zone de?)\s+([A-ZÀ-Ÿa-zà-ÿ][a-zà-ÿ\-]{2,}(?:\s+[A-Za-zà-ÿ\-]+)?)",
    ]:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            c = m.group(1).strip()
            if len(c) > 2 and c.lower() not in STOP_LOC:
                return c
    return None


def extract_radius(text: str) -> float:
    m = re.search(r"(\d+)\s*km", text.lower())
    if m:
        return min(float(m.group(1)), 100)
    return 10 if any(w in text.lower() for w in ["proche", "près", "coin"]) else 20


def _needs_db(intent: str, domaine: Optional[str], location: Optional[str],
              ctx: Dict, wc: int) -> bool:
    """
    Décide si on doit interroger la BDD.
    RÈGLE : interroger SAUF si c'est une salutation/politesse pure sans contexte.
    """
    # Jamais pour les intents purement conversationnels
    HARD_SKIP = {"salutation", "remerciement", "aurevoir", "bot_info", "perso"}
    if intent in HARD_SKIP:
        return False

    # Toujours si un domaine ou une ville est détectée dans le message
    if domaine or location:
        return True

    # Toujours si on a un contexte domaine (continuation de conversation)
    if ctx.get("last_domaine"):
        return True

    # Toujours pour "urgence" et "recherche"
    if intent in {"urgence", "recherche"}:
        return True

    # Pour faq/general : interroger si on a un contexte GPS
    if ctx.get("last_lat") and ctx.get("last_lng"):
        return True

    return False


def intent_classify(text: str, ctx: Dict) -> str:
    t  = text.lower().strip()
    wc = len(t.split())

    # Salutations strictes
    SAL = ["bonjour", "bonsoir", "salut", "hello", "hi ", "salam", "alafia", "bonne journée"]
    if any(s in t for s in SAL) and wc <= 5:
        return "salutation"

    if any(s in t for s in ["merci", "thank you", "thanks"]) and wc <= 6:
        return "remerciement"

    if any(s in t for s in ["au revoir", "bye", "à bientôt", "tchao"]) and wc <= 5:
        return "aurevoir"

    if any(s in t for s in [
        "comment tu t'appelle", "qui es-tu", "c'est quoi careasy",
        "c'est quoi carai", "que peux-tu faire", "tu es qui", "présente-toi"
    ]):
        return "bot_info"

    if any(s in t for s in ["comment tu vas", "tu vas bien", "ça va", "ca va"]) and wc <= 5:
        return "perso"

    # FAQ keywords
    FAQ_KW = [
        "comment créer", "comment modifier", "comment supprimer", "comment payer",
        "comment annuler", "comment prendre", "comment envoyer", "comment activer",
        "comment ajouter", "comment inscrire", "comment fonctionne", "comment ça",
        "qu'est-ce que", "ça fonctionne", "devenir prestataire", "inscrire mon entreprise",
        "rejoindre careasy", "mot de passe", "abonnement", "paiement", "fedapay",
        "essai gratuit", "rendez-vous", "prendre rdv", "créer service", "modifier service",
        "plan essenti", "plan profes", "plan premium", "tarif", "document requis",
        "ifu", "rccm", "certificat", "support careasy", "contacter careasy",
        "créer une entreprise", "inscrire entreprise",
    ]
    if sum(1 for kw in FAQ_KW if kw in t) >= 1:
        # Si on a aussi un domaine/ville → priorité recherche
        if extract_domaine(text) or extract_location(text):
            return "recherche"
        return "faq"

    # Suivi conversationnel — messages courts faisant référence à des services déjà présentés
    if ctx.get("last_services"):
        RANKS = [
            "premier", "deuxième", "troisième", "1er", "2ème", "3ème",
            "numéro 1", "numéro 2", "le 1", "le 2", "le 3"
        ]
        VAGUE = [
            "celui-là", "cet endroit", "ce prestataire",
            "cette entreprise", "là-bas", "ce garage"
        ]
        FKWS = [
            "numéro", "contact", "appeler", "whatsapp", "téléphone",
            "adresse", "situé", "localisation", "prix", "combien",
            "horaire", "ouvre", "itinéraire", "aller", "route"
        ]
        IMPLICIT_WORDS = [
            "et ", "eux", "elles", "ils", "leur", "leurs",
            "y a", "actuellement", "en ce moment", "pour le moment",
            "entreprise", "prestataire", "service"
        ]

        is_rank     = any(r in t for r in RANKS)
        is_vague    = any(v in t for v in VAGUE)
        is_short    = wc <= 7 and any(f in t for f in FKWS)
        is_implicit = wc <= 8 and any(w in t for w in IMPLICIT_WORDS)

        if is_rank or is_vague or is_short or is_implicit:
            if any(f in t for f in ["numéro", "contact", "appeler", "whatsapp", "téléphone"]):
                return "followup_contact"
            if any(f in t for f in ["adresse", "situé", "localisation", "où sont", "où est"]):
                return "followup_adresse"
            if any(f in t for f in ["prix", "combien", "tarif"]):
                return "followup_prix"
            if any(f in t for f in ["horaire", "ouvre", "fermé"]):
                return "followup_horaires"
            if any(f in t for f in ["itinéraire", "aller", "route", "chemin"]):
                return "followup_itineraire"
            return "followup_info"

    if any(u in t for u in ["urgent", "urgence", "vite", "sos", "en panne", "emergency"]):
        return "urgence"

    if extract_domaine(text) or extract_location(text):
        return "recherche"

    # Message court avec contexte domaine = continuation de recherche
    if ctx.get("last_domaine") and wc <= 5:
        return "recherche"

    return "general"


def resolve_ref(text: str, ctx: Dict) -> Optional[Dict]:
    t    = text.lower()
    svcs = ctx.get("last_services", [])
    if not svcs:
        return None

    RANKS = {
        1: ["premier", "1er", "numéro 1", "le 1", "première", "#1"],
        2: ["deuxième", "2ème", "numéro 2", "le 2", "#2", "second"],
        3: ["troisième", "3ème", "numéro 3", "le 3"],
        4: ["quatrième", "4ème", "le 4"],
        5: ["cinquième", "5ème", "le 5"],
    }
    for rank, patterns in RANKS.items():
        if any(p in t for p in patterns):
            return svcs[rank - 1] if rank - 1 < len(svcs) else None

    VAGUE = [
        "celui-là", "cet endroit", "ce prestataire",
        "cette entreprise", "là-bas", "ce garage"
    ]
    if any(v in t for v in VAGUE):
        return svcs[0]

    for s in svcs:
        for fname in [
            (s.get("name") or "").lower(),
            (s.get("entreprise", {}).get("name") or "").lower()
        ]:
            for word in fname.split():
                if len(word) >= 4 and word in t:
                    return s

    FKWS = ["numéro", "contact", "appeler", "adresse", "prix", "horaire", "itinéraire"]
    if len(text.split()) <= 6 and any(f in t for f in FKWS):
        return svcs[0]

    return None


def resolve_all(text: str, ctx: Dict) -> List[Dict]:
    t     = text.lower()
    MULTI = [
        "tous", "toutes", "chacun", "leurs numéros", "leurs contacts",
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
            raw = await redis_client.get(f"carai9:{cid}")
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
                f"carai9:{cid}", 14400,
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
    key = location.lower().strip()
    if key in _GEO:
        return _GEO[key]

    # 1. Laravel BDD locale (priorité absolue)
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

    # 2. Nominatim (OpenStreetMap)
    if USE_NOMINATIM:
        try:
            async with httpx.AsyncClient(timeout=8) as c:
                r = await c.get(
                    "https://nominatim.openstreetmap.org/search",
                    params={
                        "q": f"{location}, Bénin",
                        "format": "json", "limit": 1, "countrycodes": "bj"
                    },
                    headers={"User-Agent": "CarEasy-CarAI/9.0"},
                )
                if r.status_code == 200 and r.json():
                    d = r.json()[0]
                    coords = (float(d["lat"]), float(d["lon"]))
                    _GEO[key] = coords
                    return coords
        except Exception as e:
            print(f"[GEO] Nominatim: {e}")

    # 3. Google Maps
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
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
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
#  API LARAVEL — REQUÊTES BASE DE DONNÉES EN TEMPS RÉEL
# ═══════════════════════════════════════════════════════════════════════════════

async def api_nearby(
    lat: float, lng: float,
    domaine: Optional[str] = None,
    radius: float = 20,
    limit: int = 10
) -> List[Dict]:
    """Interroge la BDD Laravel par GPS. Toujours appelé si coordonnées disponibles."""
    try:
        p: Dict[str, Any] = {"lat": lat, "lng": lng, "radius": radius, "limit": limit}
        if domaine:
            p["domaine"] = domaine
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(f"{LARAVEL_BASE}/ai/services/nearby", params=p)
            if r.status_code == 200:
                data = r.json().get("data", [])
                _LEARN["stats"]["db_hits"] += 1
                print(f"[DB] api_nearby({lat:.2f},{lng:.2f},{domaine}) -> {len(data)} résultats")
                return sorted(data, key=lambda x: x.get("distance_km", 999))
    except Exception as e:
        print(f"[DB] api_nearby ERREUR: {e}")
    _LEARN["stats"]["db_misses"] += 1
    return []


async def api_by_domaine(domaine: str, limit: int = 15) -> List[Dict]:
    """Interroge la BDD Laravel par domaine (sans GPS)."""
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(
                f"{LARAVEL_BASE}/ai/services",
                params={"domaine": domaine, "limit": limit}
            )
            if r.status_code == 200:
                data = r.json().get("data", [])
                _LEARN["stats"]["db_hits"] += 1
                print(f"[DB] api_by_domaine({domaine}) -> {len(data)} résultats")
                return data
    except Exception as e:
        print(f"[DB] api_by_domaine ERREUR: {e}")
    _LEARN["stats"]["db_misses"] += 1
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
    e = s.get("entreprise", {})
    return {
        "id":                  s.get("id"),
        "name":                s.get("name"),
        "domaine":             s.get("domaine"),
        "price":               s.get("price"),
        "price_promo":         s.get("price_promo"),
        "is_price_on_request": s.get("is_price_on_request"),
        "has_promo":           s.get("has_promo"),
        "is_open_24h":         s.get("is_always_open") or s.get("is_open_24h"),
        "start_time":          s.get("start_time"),
        "end_time":            s.get("end_time"),
        "distance_km":         s.get("distance_km"),
        "average_rating":      s.get("average_rating"),
        "total_reviews":       s.get("total_reviews"),
        "entreprise": {
            "id":             e.get("id"),
            "name":           e.get("name"),
            "address":        e.get("google_formatted_address"),
            "latitude":       e.get("latitude"),
            "longitude":      e.get("longitude"),
            "call_phone":     e.get("call_phone"),
            "whatsapp_phone": e.get("whatsapp_phone"),
            "logo":           e.get("logo"),
            "status_online":  e.get("status_online"),
        },
    }


def fmt_price(s: Dict) -> str:
    if s.get("is_price_on_request"):
        return "sur devis"
    p, pp = s.get("price"), s.get("price_promo")
    if pp and s.get("has_promo") and p:
        return f"{int(pp):,} FCFA (promotion — au lieu de {int(p):,} FCFA)".replace(",", " ")
    if p:
        return f"{int(p):,} FCFA".replace(",", " ")
    return "prix non renseigné"


def fmt_hours(s: Dict) -> str:
    if s.get("is_always_open") or s.get("is_open_24h"):
        return "ouvert 24h/24, 7j/7"
    st, et = s.get("start_time"), s.get("end_time")
    return f"{st} – {et}" if st and et else "horaires non renseignés"


def fmt_rating(s: Dict) -> str:
    r = s.get("average_rating")
    n = s.get("total_reviews", 0)
    if r and n:
        return f"note {r}/5 ({n} avis)"
    return ""


# ═══════════════════════════════════════════════════════════════════════════════
#  PROMPT SYSTÈME OLLAMA v9
# ═══════════════════════════════════════════════════════════════════════════════

SYS = """Tu es CarAI, l'assistant intelligent de CarEasy Bénin.

IDENTITE :
CarEasy connecte conducteurs et prestataires automobiles au Bénin.
Tu parles comme un conseiller béninois de confiance : direct, chaleureux, humain.
Pas de formules robotiques. Pas de discours corporate. Pas de "Je suis spécialisé dans...".

LANGUE : Réponds en français. En anglais si le client parle anglais.

STYLE :
- Phrases courtes et naturelles. Varie tes formulations.
- Donne les vrais contacts, prix et horaires directement.
- Si tu ne sais pas quelque chose, dis-le simplement.
- JAMAIS de "localhost" dans tes réponses — utilise {site_url}.
- JAMAIS d'emojis — l'application mobile les gère elle-même.

REGLES ABSOLUES :
1. Si la base de données contient des prestataires -> liste-les avec leurs contacts réels.
2. Si aucun prestataire -> explique clairement et propose des alternatives.
3. Ne réponds qu'aux sujets automobiles et CarEasy.
4. Pour les questions de suivi, utilise les prestataires déjà présentés.

CONTEXTE CONVERSATION :
{ctx}

DONNEES BASE DE DONNEES CAREASY (temps réel) :
{db}

INFORMATIONS PLATEFORME :
{faq}"""


def build_db_block(
    services: List[Dict],
    ref_svc:  Optional[Dict],
    all_svcs: List[Dict]
) -> str:
    if ref_svc:
        e    = ref_svc.get("entreprise", {})
        note = fmt_rating(ref_svc)
        return (
            f"Prestataire : {e.get('name', ref_svc.get('name', 'Inconnu'))}\n"
            f"Service : {ref_svc.get('name', 'N/A')}\n"
            f"Prix : {fmt_price(ref_svc)} | Horaires : {fmt_hours(ref_svc)}\n"
            f"Telephone : {e.get('call_phone') or 'non renseigne'}\n"
            f"WhatsApp : {e.get('whatsapp_phone') or 'non renseigne'}\n"
            f"Adresse : {e.get('address') or 'non renseignee'}"
            + (f"\n{note}" if note else "")
        )

    if all_svcs:
        lines = [f"{len(all_svcs)} prestataire(s) en memoire :"]
        for i, s in enumerate(all_svcs, 1):
            e = s.get("entreprise", {})
            lines.append(
                f"{i}. {e.get('name', 'Inconnu')} "
                f"| Tel: {e.get('call_phone') or '—'} "
                f"| WA: {e.get('whatsapp_phone') or '—'}"
            )
        return "\n".join(lines)

    if not services:
        return "AUCUN prestataire trouve dans la base de donnees CarEasy pour cette recherche."

    lines = [f"{len(services)} prestataire(s) trouve(s) dans la base de donnees CarEasy :"]
    for i, s in enumerate(services, 1):
        e    = s.get("entreprise", {})
        dist = s.get("distance_km")
        dst  = f" | {dist:.1f} km" if dist is not None else ""
        note = fmt_rating(s)
        lines.append(
            f"{i}. {e.get('name', 'Inconnu')} — {s.get('name', 'N/A')}{dst}"
            + (f" | {note}" if note else "") + "\n"
            f"   Prix: {fmt_price(s)} | Horaires: {fmt_hours(s)}\n"
            f"   Tel: {e.get('call_phone') or '—'} | WA: {e.get('whatsapp_phone') or '—'}\n"
            f"   Adresse: {e.get('address') or 'non renseignee'}"
        )
    return "\n".join(lines)


def build_ctx_block(ctx: Dict, history: List[Dict]) -> str:
    parts = []
    if ctx.get("last_domaine"):
        parts.append(f"Service recherche : {ctx['last_domaine']}")
    if ctx.get("last_location"):
        parts.append(f"Localisation : {ctx['last_location']}")
    if ctx.get("last_services"):
        noms = [
            s.get("entreprise", {}).get("name", s.get("name", "Inconnu"))
            for s in ctx["last_services"][:4]
        ]
        parts.append(f"Prestataires deja presentes : {', '.join(noms)}")
    for turn in history[-4:]:
        role = "Client" if turn.get("role") == "user" else "CarAI"
        parts.append(f"{role}: {turn.get('content', '')[:150]}")
    return "\n".join(parts) if parts else "Debut de conversation"


# ═══════════════════════════════════════════════════════════════════════════════
#  OLLAMA
# ═══════════════════════════════════════════════════════════════════════════════

async def ask_ollama(
    user_msg: str,
    ctx:      Dict,
    history:  List[Dict],
    services: List[Dict],
    ref_svc:  Optional[Dict],
    all_svcs: List[Dict],
    faq_hint: Optional[str] = None,
) -> Optional[str]:
    if not USE_OLLAMA:
        return None

    system = SYS.format(
        site_url=SITE_URL,
        ctx=build_ctx_block(ctx, history),
        db=build_db_block(services, ref_svc, all_svcs),
        faq=faq_hint or "Pas d'information specifique sur la plateforme.",
    )

    msgs = [{"role": "system", "content": system}]
    for t in history[-4:]:
        role    = t.get("role", "user")
        content = t.get("content", "")
        if content and role in ("user", "assistant"):
            msgs.append({"role": role, "content": content[:250]})
    msgs.append({"role": "user", "content": user_msg})

    try:
        async with httpx.AsyncClient(timeout=55) as c:
            r = await c.post(
                f"{OLLAMA_URL}/api/chat",
                json={
                    "model": OLLAMA_MODEL,
                    "messages": msgs,
                    "stream": False,
                    "options": {
                        "temperature": 0.75,
                        "num_predict": 500,
                        "num_ctx":     4096,
                        "repeat_penalty": 1.1,
                        "top_k": 40,
                        "top_p": 0.92,
                    },
                },
            )
            if r.status_code == 200:
                reply = r.json().get("message", {}).get("content", "").strip()
                reply = re.sub(r"^(assistant\s*:\s*|carai\s*:\s*)", "", reply, flags=re.IGNORECASE)
                reply = re.sub(r"http://localhost[^\s]*", SITE_URL, reply)
                reply = re.sub(r"localhost:\d+", SITE_URL, reply)
                if reply and len(reply) > 10:
                    _LEARN["stats"]["ollama_ok"] += 1
                    return reply
    except Exception as e:
        print(f"[Ollama] {type(e).__name__}: {e}")

    return None


# ═══════════════════════════════════════════════════════════════════════════════
#  FALLBACK RÈGLES — Réponses humaines si Ollama indisponible
# ═══════════════════════════════════════════════════════════════════════════════

def fallback(
    intent:   str,
    msg:      str,
    services: List[Dict],
    ref_svc:  Optional[Dict],
    all_svcs: List[Dict],
    domaine:  Optional[str],
    location: Optional[str],
    ctx:      Dict,
    lang:     str,
    ulat:     Optional[float] = None,
    ulng:     Optional[float] = None,
) -> str:
    _LEARN["stats"]["fallback"] += 1

    if intent == "salutation":
        return random.choice([
            "Bonjour ! Que puis-je faire pour vous aujourd'hui ?",
            "Bonjour ! Je suis CarAI, votre assistant CarEasy. Comment puis-je vous aider ?",
            "Bonsoir ! Dites-moi ce que vous recherchez.",
        ])

    if intent == "remerciement":
        return random.choice([
            "Avec plaisir !",
            "De rien, bonne route !",
            "Je suis là pour ça !",
        ])

    if intent == "aurevoir":
        return random.choice([
            "A bientot ! Bonne route.",
            "Au revoir !",
        ])

    if intent == "bot_info":
        return (
            f"Je suis CarAI, l'assistant de CarEasy Benin. "
            f"Je vous aide a trouver des prestataires automobiles partout au Benin "
            f"et a utiliser la plateforme CarEasy. Site : {SITE_URL}"
        )

    if intent == "perso":
        return "Je vais bien, merci ! Dites-moi comment je peux vous aider."

    if intent == "faq":
        ans = faq_lookup(msg)
        if ans:
            return ans.replace("{site}", SITE_URL)
        return (
            f"Pour cette question, consultez {SITE_URL} "
            "ou ecrivez a support@careasy.bj. L'equipe repond en general dans la journee."
        )

    # Suivi — tous les prestataires
    if all_svcs and "followup" in intent:
        lines = ["Voici les contacts des prestataires listes :"]
        for i, s in enumerate(all_svcs, 1):
            e  = s.get("entreprise", {})
            ph = e.get("call_phone") or "—"
            wa = e.get("whatsapp_phone") or "—"
            lines.append(f"{i}. {e.get('name', 'Inconnu')} — Tel : {ph}  |  WA : {wa}")
        return "\n".join(lines)

    # Suivi — un prestataire précis
    if ref_svc and "followup" in intent:
        e   = ref_svc.get("entreprise", {})
        ent = e.get("name", "Ce prestataire")
        svc = ref_svc.get("name", "ce service")

        if "contact" in intent:
            ph = e.get("call_phone") or ""
            wa = e.get("whatsapp_phone") or ""
            if not ph and not wa:
                return f"Aucun contact renseigne pour {ent} pour le moment."
            parts = []
            if ph: parts.append(f"Tel : {ph}")
            if wa: parts.append(f"WhatsApp : {wa}")
            return f"{ent} — {' | '.join(parts)}"

        if "adresse" in intent or "itineraire" in intent:
            addr = e.get("address") or ""
            if ulat and ulng and e.get("latitude") and e.get("longitude"):
                d   = haversine(ulat, ulng, float(e["latitude"]), float(e["longitude"]))
                url = map_link(ulat, ulng, float(e["latitude"]), float(e["longitude"]))
                return (
                    f"{ent} — {addr or 'adresse non renseignee'}. "
                    f"Distance : {d:.1f} km (environ {dur(d)}). "
                    f"Itineraire : {url}"
                )
            return f"{ent} : {addr or 'adresse non renseignee'}"

        if "prix" in intent:
            return f"Le service {svc} chez {ent} est a {fmt_price(ref_svc)}."

        if "horaire" in intent:
            return f"{ent} est {fmt_hours(ref_svc)}."

        ph   = e.get("call_phone") or "—"
        wa   = e.get("whatsapp_phone") or "—"
        addr = e.get("address") or "adresse non renseignee"
        return f"{ent} ({svc})\nTel : {ph}  |  WhatsApp : {wa}\nAdresse : {addr}"

    # Résultats de recherche
    lieu = f"a {location}" if location else ("pres de vous" if ulat else "au Benin")

    if not services:
        conseils = ""
        if location:
            conseils = " Vous pouvez aussi essayer une ville voisine ou elargir le rayon."
        return (
            f"Je n'ai trouve aucun prestataire en "
            f"{domaine or 'ce domaine'} {lieu} pour le moment.{conseils} "
            f"De nouveaux prestataires rejoignent CarEasy chaque semaine. "
            f"Vous etes prestataire ? Inscrivez-vous sur {SITE_URL}"
        )

    lines = [f"J'ai trouve {len(services)} prestataire(s) en {domaine or 'automobile'} {lieu} :"]
    for i, s in enumerate(services[:5], 1):
        e    = s.get("entreprise", {})
        dist = s.get("distance_km")
        dst  = f" ({dist:.1f} km)" if dist is not None else ""
        note = fmt_rating(s)
        lines.append(
            f"\n{i}. {e.get('name', 'Inconnu')}{dst}"
            + (f" — {note}" if note else "") + "\n"
            f"   {s.get('name', '')} | {fmt_hours(s)} | {fmt_price(s)}\n"
            f"   Tel : {e.get('call_phone') or '—'}   WA : {e.get('whatsapp_phone') or '—'}"
        )
    if len(services) > 5:
        lines.append(f"\n...et {len(services) - 5} autre(s) disponible(s).")
    lines.append("\nVoulez-vous l'itineraire ou les contacts d'un prestataire en particulier ?")
    return "\n".join(lines)


SUGG_BASE = [
    "Trouver un garage mecanique",
    "Vulcanisateur disponible",
    "Lavage auto",
    "Electricien auto",
    "Depannage routier",
]


def suggestions(
    domaine:  Optional[str],
    location: Optional[str],
    ctx:      Dict
) -> List[str]:
    result = []
    if ctx.get("last_services"):
        result += ["Contacts de tous", "Itineraire vers le plus proche"]
    if domaine and location:
        result.append(f"{domaine} a {location}")
    elif domaine:
        result.append(f"{domaine} a Cotonou")
        result.append(f"{domaine} a Abomey")
    if location:
        result.append(f"Tous les services a {location}")
    result += SUGG_BASE
    seen, final = set(), []
    for s in result:
        if s not in seen:
            seen.add(s)
            final.append(s)
    return final[:5]


# ═══════════════════════════════════════════════════════════════════════════════
#  ENDPOINT PRINCIPAL /chat
# ═══════════════════════════════════════════════════════════════════════════════

@app.post("/chat", response_model=ChatResponse)
async def chat(req: ChatRequest, bg: BackgroundTasks):
    t0 = time.time()

    # ── 1. Récupération mémoire ────────────────────────────────────────────
    mem     = await mem_get(req.conversation_id)
    ctx     = mem["ctx"]
    history = mem["history"]

    lang     = req.language or detect_lang(req.message)
    intent   = intent_classify(req.message, ctx)
    domaine  = extract_domaine(req.message)
    location = extract_location(req.message)
    radius   = extract_radius(req.message)
    wc       = len(req.message.split())

    # ── 2. Enrichissement contextuel ──────────────────────────────────────
    FOLLOW_INTENTS = {
        "followup_contact", "followup_adresse", "followup_prix",
        "followup_horaires", "followup_itineraire", "followup_info", "urgence",
    }

    # Hériter du domaine du contexte si pas détecté dans le message
    if not domaine and ctx.get("last_domaine") and (
        intent in FOLLOW_INTENTS
        or ctx.get("last_services")
        or wc <= 5
    ):
        domaine = ctx.get("last_domaine")

    # Hériter de la localisation du contexte
    if not location and ctx.get("last_location") and intent in {
        "followup_adresse", "followup_itineraire", "recherche"
    }:
        location = ctx.get("last_location")

    # ── 3. Résolution de références conversationnelles ─────────────────────
    ref_svc    = resolve_ref(req.message, ctx)
    all_svcs   = resolve_all(req.message, ctx)
    is_followup = bool(ref_svc or all_svcs) and intent in FOLLOW_INTENTS

    # ── 4. REQUÊTE BASE DE DONNÉES (logique centralisée) ──────────────────
    services:  List[Dict] = []
    mapurl:    Optional[str] = None
    itinerary: Optional[Dict] = None

    should_query_db = _needs_db(intent, domaine, location, ctx, wc)

    if should_query_db and not is_followup:
        # Cas 1 : GPS disponible dans la requête courante
        if req.latitude and req.longitude:
            services = await api_nearby(
                req.latitude, req.longitude, domaine, radius, limit=10
            )
            if services:
                e0 = services[0].get("entreprise", {})
                if e0.get("latitude") and e0.get("longitude"):
                    d_km = haversine(
                        req.latitude, req.longitude,
                        float(e0["latitude"]), float(e0["longitude"])
                    )
                    mapurl    = map_link(
                        req.latitude, req.longitude,
                        float(e0["latitude"]), float(e0["longitude"])
                    )
                    itinerary = {
                        "maps_url":    mapurl,
                        "distance":    f"{d_km:.1f} km",
                        "duration":    dur(d_km),
                        "destination": e0.get("name", ""),
                    }

        # Cas 2 : GPS mémorisé dans le contexte (si pas de GPS dans la requête)
        elif ctx.get("last_lat") and ctx.get("last_lng") and not location:
            services = await api_nearby(
                float(ctx["last_lat"]), float(ctx["last_lng"]),
                domaine, radius, limit=10
            )

        # Cas 3 : Ville mentionnée → géocodage puis recherche
        elif location:
            coords = await geocode(location)
            if coords:
                services = await api_nearby(
                    coords[0], coords[1], domaine, radius, limit=10
                )
                # Élargir automatiquement si aucun résultat
                if not services:
                    services = await api_nearby(
                        coords[0], coords[1], domaine, radius * 2, limit=10
                    )
            # Fallback par domaine si toujours rien
            if not services and domaine:
                services = await api_by_domaine(domaine, limit=15)

        # Cas 4 : Seulement un domaine détecté → recherche nationale
        elif domaine:
            services = await api_by_domaine(domaine, limit=15)

    _track_query(domaine, location, len(services))

    # Services actifs pour Ollama (résultats frais ou résultats mémorisés pour followup)
    active = services or (ctx.get("last_services", []) if is_followup else [])

    # ── 5. Hint FAQ pour Ollama ────────────────────────────────────────────
    faq_hint = None
    if intent in {"faq", "general", "bot_info"}:
        faq_hint = faq_lookup(req.message)

    # ── 6. Génération de réponse via Ollama ───────────────────────────────
    reply = await ask_ollama(
        user_msg=req.message,
        ctx=ctx,
        history=history,
        services=active,
        ref_svc=ref_svc,
        all_svcs=all_svcs,
        faq_hint=faq_hint,
    )

    # ── 7. Fallback si Ollama indisponible ────────────────────────────────
    if not reply:
        reply = fallback(
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
        )

    # ── 8. Nettoyage final ────────────────────────────────────────────────
    reply = re.sub(r"http://localhost[^\s]*", SITE_URL, reply)
    reply = re.sub(r"localhost:\d+", SITE_URL, reply)

    # ── 9. Sauvegarde mémoire ─────────────────────────────────────────────
    cleaned = [clean_svc(s) for s in services[:8]]

    history.append({
        "role": "user", "content": req.message,
        "intent": intent, "domaine": domaine, "location": location,
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

    mem["history"] = history
    mem["ctx"]     = ctx
    bg.add_task(mem_save, req.conversation_id, mem)

    elapsed = time.time() - t0
    print(
        f"[CHAT] {req.conversation_id[:12]} | intent={intent} | "
        f"domaine={domaine or '-'} | loc={location or '-'} | "
        f"services={len(active)} | db={'requete' if should_query_db else 'skip'} | "
        f"{elapsed:.2f}s | hits={_LEARN['stats']['db_hits']}"
    )

    return ChatResponse(
        reply=reply,
        services=cleaned if cleaned else (
            ctx.get("last_services", [])[:3] if is_followup else []
        ),
        map_url=mapurl,
        itinerary=itinerary,
        intent=domaine or intent,
        language=lang,
        suggestions=suggestions(domaine, location, ctx),
        confidence=_confidence(req.message, intent),
    )


# ═══════════════════════════════════════════════════════════════════════════════
#  FEEDBACK
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


# ═══════════════════════════════════════════════════════════════════════════════
#  ENDPOINTS UTILITAIRES
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/health")
async def health():
    redis_ok  = False
    ollama_ok = False
    model_ok  = False
    db_ok     = False

    if redis_client:
        try:
            await redis_client.ping()
            redis_ok = True
        except Exception:
            pass

    if USE_OLLAMA:
        try:
            async with httpx.AsyncClient(timeout=4) as c:
                r = await c.get(f"{OLLAMA_URL}/api/tags")
                if r.status_code == 200:
                    ollama_ok = True
                    model_ok  = any(
                        OLLAMA_MODEL in m["name"]
                        for m in r.json().get("models", [])
                    )
        except Exception:
            pass

    try:
        async with httpx.AsyncClient(timeout=4) as c:
            r    = await c.get(f"{LARAVEL_BASE}/ai/domaines")
            db_ok = r.status_code == 200
    except Exception:
        pass

    return {
        "status":      "ok",
        "version":     "9.0.0",
        "mode":        "ollama+rules+db" if ollama_ok else "rules+db",
        "redis":       redis_ok,
        "ollama":      ollama_ok,
        "model":       model_ok,
        "database":    db_ok,
        "sessions":    len(_RAM),
        "geo_cached":  len(_GEO),
        "learn":       _LEARN["stats"],
        "laravel":     LARAVEL_BASE,
        "site":        SITE_URL,
        "ts":          datetime.now().isoformat(),
    }


@app.delete("/conversation/{cid}")
async def clear_conv(cid: str):
    _RAM.pop(cid, None)
    if redis_client:
        try:
            await redis_client.delete(f"carai9:{cid}")
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

    # Test 1 : Laravel domaines
    try:
        async with httpx.AsyncClient(timeout=6) as c:
            r = await c.get(f"{LARAVEL_BASE}/ai/domaines")
            n = len(r.json().get("data", [])) if r.status_code == 200 else 0
            results["laravel_domaines"] = (
                f"OK — {n} domaines" if r.status_code == 200
                else f"ERREUR HTTP {r.status_code}"
            )
    except Exception as e:
        results["laravel_domaines"] = f"ERREUR: {e}"

    # Test 2 : Services proches de Cotonou
    try:
        svcs = await api_nearby(6.3654, 2.4183, None, 20, 3)
        results["laravel_nearby_cotonou"] = f"OK — {len(svcs)} services"
    except Exception as e:
        results["laravel_nearby_cotonou"] = f"ERREUR: {e}"

    # Test 3 : Services par domaine
    try:
        svcs = await api_by_domaine("Garage mecanique", limit=3)
        results["laravel_by_domaine"] = f"OK — {len(svcs)} services"
    except Exception as e:
        results["laravel_by_domaine"] = f"ERREUR: {e}"

    # Test 4 : Géocodage
    try:
        coords = await geocode("Abomey")
        results["geocode_abomey"] = f"OK — {coords}" if coords else "Aucun résultat"
    except Exception as e:
        results["geocode_abomey"] = f"ERREUR: {e}"

    # Test 5 : Ollama
    if USE_OLLAMA:
        try:
            async with httpx.AsyncClient(timeout=6) as c:
                r = await c.get(f"{OLLAMA_URL}/api/tags")
                models = [m["name"] for m in r.json().get("models", [])] if r.status_code == 200 else []
                results["ollama"] = f"OK — modeles: {models}"
        except Exception as e:
            results["ollama"] = f"ERREUR: {e}"
    else:
        results["ollama"] = "Desactive"

    results["redis"]    = "Connecte" if redis_client else "RAM actif"
    results["site_url"] = SITE_URL
    results["version"]  = "9.0.0"

    return {"version": "9.0.0", "tests": results}