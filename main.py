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
OLLAMA_URL      = os.getenv("OLLAMA_URL",       "http://localhost:11434")
OLLAMA_MODEL    = os.getenv("OLLAMA_MODEL",     "llama3")
USE_OLLAMA      = os.getenv("USE_OLLAMA",       "true").lower() == "true"
SITE_URL        = os.getenv("FRONTEND_URL",     "https://careasy.vercel.app")
LEARN_FILE      = os.getenv("LEARN_FILE",       "/tmp/carai_learn_v9.json")

app = FastAPI(title="CarAI v9.2", version="9.2.0", docs_url="/docs")
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
#  UTILITAIRE — Normalisation texte
# ═══════════════════════════════════════════════════════════════════════════════

def normalize_text(text: str) -> str:
    text = unicodedata.normalize("NFC", text)
    text = text.replace("\u2019", "'").replace("\u2018", "'") \
               .replace("\u02BC", "'").replace("\u0060", "'") \
               .replace("\u00B4", "'")
    text = text.replace("\u2013", "-").replace("\u2014", "-")
    return text.lower()


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
                    print(f"[CarAI] Ollama '{OLLAMA_MODEL}' {'OK' if ok else 'ABSENT — mode fallback actif'}")
                else:
                    print(f"[CarAI] Ollama KO (HTTP {r.status_code}) — mode fallback actif")
        except Exception as e:
            print(f"[CarAI] Ollama KO ({e}) — mode fallback actif")

    print(f"[CarAI] v9.2 | {LARAVEL_BASE} | Ollama={'ON' if USE_OLLAMA else 'OFF'}")

    try:
        async with httpx.AsyncClient(timeout=8) as c:
            r = await c.get(f"{LARAVEL_BASE}/ai/domaines")
            if r.status_code == 200:
                nb = len(r.json().get("data", []))
                print(f"[CarAI] Laravel OK — {nb} domaines disponibles")
            else:
                print(f"[CarAI] ATTENTION: Laravel répond {r.status_code} sur /ai/domaines")
    except Exception as e:
        print(f"[CarAI] ATTENTION: Laravel inaccessible au démarrage: {e}")

    try:
        async with httpx.AsyncClient(timeout=8) as c:
            r = await c.get(
                f"{LARAVEL_BASE}/ai/services/nearby",
                params={"lat": 6.3654, "lng": 2.4183, "radius": 50, "limit": 3}
            )
            if r.status_code == 200:
                nb = len(r.json().get("data", []))
                print(f"[CarAI] BDD OK — {nb} services trouvés près de Cotonou")
                if nb == 0:
                    print("[CarAI] ATTENTION: 0 services — vérifiez status=validated ET coords GPS")
            else:
                print(f"[CarAI] ATTENTION: /ai/services/nearby répond {r.status_code}")
    except Exception as e:
        print(f"[CarAI] ATTENTION: Test services/nearby échoué: {e}")

    try:
        async with httpx.AsyncClient(timeout=8) as c:
            r = await c.get(f"{LARAVEL_BASE}/ai/services", params={"limit": 3})
            if r.status_code == 200:
                nb = len(r.json().get("data", []))
                print(f"[CarAI] Fallback /ai/services OK — {nb} services disponibles")
    except Exception as e:
        print(f"[CarAI] ATTENTION: Test /ai/services échoué: {e}")


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
    return 0.9 if n > 20 else (0.8 if n > 10 else 0.70)


# ═══════════════════════════════════════════════════════════════════════════════
#  DOMAINES
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
        "voiture en panne", "ma voiture", "mon vehicule",
    ],
    "Lavage automobile": [
        "lavage", "carwash", "car wash", "nettoyage voiture",
        "polissage", "lustrage", "laver voiture", "laver ma voiture",
        "laver auto", "lavage auto", "lavage voiture",
    ],
    "Electricien auto": [
        "electricien auto", "batterie voiture", "alternateur", "demarreur auto",
        "cablage", "phare voiture", "electrician auto", "probleme electrique",
        "electricien", "batterie",
    ],
    "Climatisation auto": [
        "climatisation", "clim voiture", "air conditionne voiture", "recharge clim",
        "clim", "ac voiture", "recharge climatisation",
    ],
    "Peinture auto": [
        "peinture voiture", "rayure carrosserie", "retouche peinture", "vernis voiture",
    ],
    "Tolerie": [
        "tolerie", "tolerie", "carrosserie", "bosselage", "debosselage", "dent voiture",
    ],
    "Depannage / remorquage": [
        "depannage", "depanneur", "remorquage", "voiture en panne",
        "sos auto", "assistance routiere", "towing", "urgence voiture",
        "en panne", "tombe en panne",
    ],
    "Changement d'huile": [
        "vidange", "huile moteur", "filtre huile", "oil change",
        "changer huile", "faire vidange",
    ],
    "Diagnostic automobile": [
        "diagnostic", "scanner voiture", "valise diagnostic", "code erreur voiture",
        "voyant allume", "check engine", "obd", "voyant rouge",
    ],
    "Station d'essence": [
        "essence", "carburant", "gasoil", "diesel", "station service",
        "faire le plein", "pompe essence", "station d'essence",
        "station essence", "plein", "sans plomb",
    ],
    "Location de voitures": [
        "location voiture", "louer voiture", "voiture de location",
        "louer une voiture",
    ],
    "Assurance automobile": [
        "assurance auto", "sinistre auto", "police assurance",
        "assurance voiture",
    ],
    "Ecole de conduite": [
        "permis de conduire", "auto-ecole", "autoecole", "driving school",
        "permis conduire", "apprendre conduire",
    ],
    "Vente de pieces detachees": [
        "pieces detachees", "spare part", "plaquette frein", "disque frein",
        "bougie", "filtre voiture", "piece voiture", "pieces auto",
    ],
    "Reparation moto": [
        "moto", "zemidjan", "zem", "scooter", "moto taxi",
        "reparer moto", "moto en panne",
    ],
    "Vente de voitures": [
        "acheter voiture", "achat voiture", "vente voiture", "concessionnaire",
        "voiture d'occasion", "voiture neuve", "occasion",
    ],
    "Maintenance poids lourds": [
        "poids lourd", "camion", "semi-remorque", "gros porteur",
    ],
    "Vente de motos": [
        "acheter moto", "vente moto", "moto neuve", "moto occasion",
    ],
    "Vente de velos / entretien": [
        "velo", "bicyclette", "vtt", "reparation velo",
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
#  FAQ — avec guidance navigation application mobile CarEasy
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
            "Plans CarEasy prestataire (section Plans & Abonnements dans Paramètres) :\n"
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
            "Suivez le décompte dans l'onglet Mes entreprises → badge bleu Essai gratuit. "
            "Un plan payant est requis après les 30 jours pour continuer."
        )
    },
    {
        "tags": ["payer", "paiement", "fedapay", "mobile money", "orange money", "mtn"],
        "content": (
            "Pour souscrire à un plan dans l'application CarEasy :\n"
            "1) Allez dans Paramètres → Plans & Abonnements.\n"
            "2) Choisissez votre plan et appuyez sur Souscrire.\n"
            "3) Payez via FedaPay : Orange Money, MTN Money, Moov Money ou carte bancaire.\n"
            "Une facture est envoyée par email après paiement."
        )
    },
    {
        "tags": ["support", "aide", "probleme", "bug", "contacter careasy"],
        "content": (
            "Support CarEasy :\n"
            "- Dans l'application : Paramètres → Aide & support\n"
            "- Email : support@careasy.bj\n"
            "- WhatsApp : disponible depuis la page À propos de l'app\n"
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
            "Pour activer la géolocalisation dans CarEasy :\n"
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
            "4) Pour le mot de passe : Paramètres → Confidentialité & sécurité → Changer le mot de passe."
        )
    },
    {
        "tags": ["notifications", "alertes", "notification"],
        "content": (
            "Pour gérer les notifications dans CarEasy :\n"
            "Paramètres → Notifications. "
            "Vous pouvez activer/désactiver les notifications push, email et SMS, "
            "et choisir le son de notification."
        )
    },
    {
        "tags": ["theme", "mode sombre", "apparence", "dark mode"],
        "content": (
            "Pour changer le thème de l'application CarEasy :\n"
            "Paramètres → Apparence → choisissez Clair, Sombre ou Système (suit votre téléphone)."
        )
    },
    {
        "tags": ["connexion qr", "qr code connexion", "scanner qr", "autre telephone"],
        "content": (
            "Pour vous connecter sur un autre téléphone via QR code :\n"
            "1) Sur l'appareil déjà connecté : Paramètres → Confidentialité & sécurité → Appareils connectés → Ajouter via QR.\n"
            "2) Sur le nouvel appareil : écran de bienvenue → Connexion rapide via QR code.\n"
            "3) Scannez le QR code — connexion automatique et sécurisée (valable 2 minutes)."
        )
    },
    {
        "tags": ["messages", "conversations", "tchat", "chat"],
        "content": (
            "Pour accéder à vos messages dans CarEasy :\n"
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
    for m in ["me\u030c", "\u0256o\u0300", "n\u0254 ", "blo\u0301", "we\u0300 ", "al\u0254", "aca"]:
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
    if intent in {"urgence", "recherche"}:
        return True
    if intent == "general" and wc >= 4:
        return True
    if intent == "faq" and ctx.get("last_services"):
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
        "qu'est-ce que careasy", "kesako careasy",
    ]):
        return "bot_info"

    if any(s in t for s in ["comment tu vas", "tu vas bien", "ca va"]) and wc <= 5:
        return "perso"

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

    VAGUE = [
        "celui-la", "cet endroit", "ce prestataire",
        "cette entreprise", "la-bas", "ce garage"
    ]
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

    FKWS = ["numero", "contact", "appeler", "whatsapp", "telephone", "adresse", "prix", "horaire", "itineraire"]
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
                    headers={"User-Agent": "CarEasy-CarAI/9.2"},
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
                print(f"[DB] api_nearby({lat:.2f},{lng:.2f},{domaine}) -> {len(data)} résultats")
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
        "id":                       e.get("id"),
        "name":                     e.get("name"),
        "latitude":                 e.get("latitude"),
        "longitude":                e.get("longitude"),
        "google_formatted_address": e.get("google_formatted_address") or e.get("address"),
        "address":                  e.get("google_formatted_address") or e.get("address"),
        "call_phone":               e.get("call_phone"),
        "whatsapp_phone":           e.get("whatsapp_phone"),
        "status_online":            e.get("status_online", True),
        "logo":                     e.get("logo"),
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
                print(f"[DB] api_by_domaine({domaine!r}) -> {len(data)} résultats")
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
                print(f"[DB] api_services_all({domaine!r}) -> {len(data)} résultats")
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

    address = (
        e.get("google_formatted_address")
        or e.get("address")
        or None
    )

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
            "id":             e.get("id"),
            "name":           e.get("name"),
            "address":        address,
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
    try:
        if pp and s.get("has_promo") and p:
            return f"{int(float(pp)):,} FCFA (promo — au lieu de {int(float(p)):,} FCFA)".replace(",", " ")
        if p:
            return f"{int(float(p)):,} FCFA".replace(",", " ")
    except (TypeError, ValueError):
        pass
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
#  PROMPT SYSTÈME OLLAMA v9.2
#  CORRECTION BUG #1 : Réponses hors-sujet → prompt ancré sur l'app mobile
#  CORRECTION BUG #2 : Double réponse → Ollama REMPLACE le fallback, pas l'inverse
# ═══════════════════════════════════════════════════════════════════════════════

SYS = """Tu es CarAI, l'assistant de l'APPLICATION MOBILE CarEasy Bénin.

CONTEXTE ABSOLU :
CarEasy est une application mobile Flutter (Android & iOS) qui connecte conducteurs et prestataires automobiles au Bénin.
Tu aides les utilisateurs à utiliser l'APPLICATION MOBILE CarEasy et à trouver des prestataires.
Tu ne parles QUE de l'application CarEasy, de l'automobile et des services associés au Bénin.

NAVIGATION DE L'APPLICATION :
- Barre de navigation en bas : Accueil | Messages | Rendez-vous | Entreprise | Profil
- Accueil : liste les services et entreprises, bouton Contacter et Détails
- Messages : toutes les conversations avec les prestataires
- Rendez-vous : gérer les RDV (En attente / Confirmé / Terminé / Annulé)
- Entreprise : Mes entreprises, créer/gérer entreprise et services
- Profil (Paramètres) : modifier profil, notifications, apparence, sécurité, plans, aide, à propos
- CarAI : bouton flottant rouge en bas à droite de l'accueil

STYLE DE RÉPONSE :
- Phrases courtes et naturelles, ton chaleureux et direct comme un conseiller béninois.
- Donne les vrais contacts, prix et horaires directement sans détour.
- Pour les questions sur l'app, donne le chemin de navigation exact (ex: "Onglet Profil → Paramètres → Notifications")
- JAMAIS de "localhost" → utilise {site_url}
- JAMAIS d'emojis (l'app les gère)
- Si tu ne sais pas, dis-le simplement.

RÈGLES ABSOLUES :
1. Si des prestataires sont dans les DONNÉES CI-DESSOUS → liste-les TOUS avec contacts réels.
2. Si aucun prestataire trouvé → explique et propose alternatives.
3. Ne réponds qu'aux sujets : application CarEasy, automobile au Bénin, services auto.
4. Pour les questions de suivi, utilise les prestataires déjà présentés dans CONTEXTE.
5. Ne dis JAMAIS "je n'ai pas accès" si des données sont fournies ci-dessous.
6. Tes réponses sont DÉFINITIVES — ne génère PAS d'introduction avant de lister, liste directement.

CONTEXTE CONVERSATION :
{ctx}

DONNÉES BASE DE DONNÉES CAREASY (temps réel) :
{db}

INFORMATIONS PLATEFORME :
{faq}"""


def build_db_block(
    services: List[Dict],
    ref_svc:  Optional[Dict],
    all_svcs: List[Dict]
) -> str:
    if ref_svc:
        e    = ref_svc.get("entreprise", {}) or {}
        note = fmt_rating(ref_svc)
        addr = e.get("google_formatted_address") or e.get("address") or "non renseignée"
        return (
            f"Prestataire : {e.get('name', ref_svc.get('name', 'Inconnu'))}\n"
            f"Service : {ref_svc.get('name', 'N/A')}\n"
            f"Prix : {fmt_price(ref_svc)} | Horaires : {fmt_hours(ref_svc)}\n"
            f"Telephone : {e.get('call_phone') or 'non renseigne'}\n"
            f"WhatsApp : {e.get('whatsapp_phone') or 'non renseigne'}\n"
            f"Adresse : {addr}"
            + (f"\n{note}" if note else "")
        )

    if all_svcs:
        lines = [f"{len(all_svcs)} prestataire(s) en mémoire :"]
        for i, s in enumerate(all_svcs, 1):
            e = s.get("entreprise", {}) or {}
            lines.append(
                f"{i}. {e.get('name', 'Inconnu')} "
                f"| Tel: {e.get('call_phone') or '—'} "
                f"| WA: {e.get('whatsapp_phone') or '—'}"
            )
        return "\n".join(lines)

    if not services:
        return "AUCUN prestataire trouvé dans la base de données CarEasy pour cette recherche."

    lines = [f"{len(services)} prestataire(s) trouvé(s) dans la base de données CarEasy :"]
    for i, s in enumerate(services, 1):
        e    = s.get("entreprise", {}) or {}
        dist = s.get("distance_km")
        dst  = f" | {dist:.1f} km" if dist is not None else ""
        note = fmt_rating(s)
        addr = e.get("google_formatted_address") or e.get("address") or "adresse non renseignée"
        lines.append(
            f"{i}. {e.get('name', 'Inconnu')} — {s.get('name', 'N/A')}{dst}"
            + (f" | {note}" if note else "") + "\n"
            f"   Prix: {fmt_price(s)} | Horaires: {fmt_hours(s)}\n"
            f"   Tel: {e.get('call_phone') or '—'} | WA: {e.get('whatsapp_phone') or '—'}\n"
            f"   Adresse: {addr}"
        )
    return "\n".join(lines)


def build_ctx_block(ctx: Dict, history: List[Dict]) -> str:
    parts = []
    if ctx.get("last_domaine"):
        parts.append(f"Service recherché : {ctx['last_domaine']}")
    if ctx.get("last_location"):
        parts.append(f"Localisation : {ctx['last_location']}")
    if ctx.get("last_services"):
        noms = [
            (s.get("entreprise") or {}).get("name") or s.get("name") or "Inconnu"
            for s in ctx["last_services"][:4]
        ]
        parts.append(f"Prestataires déjà présentés : {', '.join(noms)}")
    for turn in history[-4:]:
        role = "Client" if turn.get("role") == "user" else "CarAI"
        parts.append(f"{role}: {turn.get('content', '')[:150]}")
    return "\n".join(parts) if parts else "Début de conversation"


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
        faq=faq_hint or "Pas d'information spécifique sur la plateforme.",
    )

    msgs = [{"role": "system", "content": system}]
    for turn in history[-4:]:
        role    = turn.get("role", "user")
        content = turn.get("content", "")
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
#  FALLBACK RÈGLES — utilisé UNIQUEMENT si Ollama échoue ou est désactivé
#  CORRECTION BUG #2 : Le fallback ne s'exécute JAMAIS en même temps qu'Ollama
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
        return random.choice(["Avec plaisir !", "De rien, bonne route !", "Je suis là pour ça !"])

    if intent == "aurevoir":
        return random.choice(["À bientôt ! Bonne route.", "Au revoir !"])

    if intent == "bot_info":
        return (
            f"Je suis CarAI, l'assistant de l'application mobile CarEasy Bénin. "
            f"Je vous aide à trouver des prestataires automobiles partout au Bénin "
            f"et à utiliser l'application CarEasy. Site : {SITE_URL}"
        )

    if intent == "perso":
        return "Je vais bien, merci ! Dites-moi comment je peux vous aider."

    if intent == "faq":
        ans = faq_lookup(msg)
        if ans:
            return ans.replace("{site}", SITE_URL)
        return (
            f"Pour cette question sur l'application CarEasy, consultez {SITE_URL} "
            "ou écrivez à support@careasy.bj. L'équipe répond en général dans la journée."
        )

    # Suivi — tous les prestataires
    if all_svcs and "followup" in intent:
        lines = ["Voici les contacts des prestataires listés :"]
        for i, s in enumerate(all_svcs, 1):
            e  = s.get("entreprise", {}) or {}
            ph = e.get("call_phone") or "—"
            wa = e.get("whatsapp_phone") or "—"
            lines.append(f"{i}. {e.get('name', 'Inconnu')} — Tél : {ph}  |  WA : {wa}")
        return "\n".join(lines)

    # Suivi — un prestataire précis
    if ref_svc and "followup" in intent:
        e   = ref_svc.get("entreprise", {}) or {}
        ent = e.get("name", "Ce prestataire")
        svc = ref_svc.get("name", "ce service")
        addr = e.get("google_formatted_address") or e.get("address") or "adresse non renseignée"

        if "contact" in intent:
            ph = e.get("call_phone") or ""
            wa = e.get("whatsapp_phone") or ""
            if not ph and not wa:
                return f"Aucun contact renseigné pour {ent} pour le moment."
            parts = []
            if ph: parts.append(f"Tél : {ph}")
            if wa: parts.append(f"WhatsApp : {wa}")
            return f"{ent} — {' | '.join(parts)}"

        if "adresse" in intent or "itineraire" in intent:
            if ulat and ulng and e.get("latitude") and e.get("longitude"):
                try:
                    d   = haversine(ulat, ulng, float(e["latitude"]), float(e["longitude"]))
                    url = map_link(ulat, ulng, float(e["latitude"]), float(e["longitude"]))
                    return (
                        f"{ent} — {addr}. "
                        f"Distance : {d:.1f} km (environ {dur(d)}). "
                        f"Itinéraire : {url}"
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
        return f"{ent} ({svc})\nTél : {ph}  |  WhatsApp : {wa}\nAdresse : {addr}"

    # Résultats de recherche
    lieu = f"à {location}" if location else ("près de vous" if ulat else "au Bénin")

    if not services:
        conseils = " Vous pouvez aussi essayer une ville voisine ou élargir le rayon." if location else ""
        return (
            f"Je n'ai trouvé aucun prestataire en "
            f"{domaine or 'ce domaine'} {lieu} pour le moment.{conseils} "
            f"De nouveaux prestataires rejoignent CarEasy chaque semaine. "
            f"Vous êtes prestataire ? Inscrivez-vous sur {SITE_URL}"
        )

    lines = [f"J'ai trouvé {len(services)} prestataire(s) en {domaine or 'automobile'} {lieu} :"]
    for i, s in enumerate(services[:5], 1):
        e    = s.get("entreprise", {}) or {}
        dist = s.get("distance_km")
        dst  = f" ({dist:.1f} km)" if dist is not None else ""
        note = fmt_rating(s)
        lines.append(
            f"\n{i}. {e.get('name', 'Inconnu')}{dst}"
            + (f" — {note}" if note else "") + "\n"
            f"   {s.get('name', '')} | {fmt_hours(s)} | {fmt_price(s)}\n"
            f"   Tél : {e.get('call_phone') or '—'}   WA : {e.get('whatsapp_phone') or '—'}"
        )
    if len(services) > 5:
        lines.append(f"\n...et {len(services) - 5} autre(s) disponible(s).")
    lines.append("\nVoulez-vous l'itinéraire ou les contacts d'un prestataire en particulier ?")
    return "\n".join(lines)


SUGG_BASE = [
    "Trouver un garage mécanique",
    "Vulcanisateur disponible",
    "Lavage auto",
    "Électricien auto",
    "Dépannage routier",
]


def suggestions(domaine: Optional[str], location: Optional[str], ctx: Dict) -> List[str]:
    result = []
    if ctx.get("last_services"):
        result += ["Contacts de tous", "Itinéraire vers le plus proche"]
    if domaine and location:
        result.append(f"{domaine} à {location}")
    elif domaine:
        result.append(f"{domaine} à Cotonou")
        result.append(f"{domaine} à Abomey-Calavi")
    if location:
        result.append(f"Tous les services à {location}")
    result += SUGG_BASE
    seen, final = set(), []
    for s in result:
        if s not in seen:
            seen.add(s)
            final.append(s)
    return final[:5]


# ═══════════════════════════════════════════════════════════════════════════════
#  ENDPOINT PRINCIPAL — CORRECTION BUG #2 DOUBLE RÉPONSE
#  Logique : Ollama OU fallback, jamais les deux
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

    FOLLOW_INTENTS = {
        "followup_contact", "followup_adresse", "followup_prix",
        "followup_horaires", "followup_itineraire", "followup_info", "urgence",
    }

    # Hériter du domaine du contexte si pas détecté
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

    ref_svc    = resolve_ref(req.message, ctx)
    all_svcs   = resolve_all(req.message, ctx)
    is_followup = bool(ref_svc or all_svcs) and intent in FOLLOW_INTENTS

    services:  List[Dict] = []
    mapurl:    Optional[str] = None
    itinerary: Optional[Dict] = None

    should_query_db = _needs_db(intent, domaine, location, ctx, wc)

    if should_query_db and not is_followup:

        # CAS 1 : GPS en temps réel
        if req.latitude and req.longitude:
            services = await api_nearby(req.latitude, req.longitude, domaine, radius, limit=10)
            if not services:
                services = await api_nearby(req.latitude, req.longitude, domaine, 50, limit=10)
            if not services and domaine:
                services = await api_by_domaine(domaine, limit=15)
            if not services:
                services = await api_services_all(domaine, limit=15)
            if services:
                e0 = (services[0].get("entreprise") or {})
                if e0.get("latitude") and e0.get("longitude"):
                    try:
                        d_km  = haversine(req.latitude, req.longitude,
                                          float(e0["latitude"]), float(e0["longitude"]))
                        mapurl = map_link(req.latitude, req.longitude,
                                          float(e0["latitude"]), float(e0["longitude"]))
                        itinerary = {
                            "maps_url":    mapurl,
                            "distance":    f"{d_km:.1f} km",
                            "duration":    dur(d_km),
                            "destination": e0.get("name", ""),
                        }
                    except Exception:
                        pass

        # CAS 2 : GPS mémorisé en contexte
        elif ctx.get("last_lat") and ctx.get("last_lng") and not location:
            services = await api_nearby(float(ctx["last_lat"]), float(ctx["last_lng"]),
                                        domaine, radius, limit=10)
            if not services and domaine:
                services = await api_by_domaine(domaine, limit=15)
            if not services:
                services = await api_services_all(domaine, limit=15)

        # CAS 3 : Ville mentionnée → géocodage
        elif location:
            coords = await geocode(location)
            if coords:
                services = await api_nearby(coords[0], coords[1], domaine, radius, limit=10)
                if not services:
                    services = await api_nearby(coords[0], coords[1], domaine, radius * 3, limit=10)
            if not services and domaine:
                services = await api_by_domaine(domaine, limit=15)
            if not services:
                services = await api_services_all(domaine, limit=15)

        # CAS 4 : Domaine détecté, pas de GPS ni ville
        elif domaine:
            services = await api_by_domaine(domaine, limit=15)
            if not services:
                services = await api_services_all(domaine, limit=15)

        # CAS 5 : Contexte domaine mémorisé
        elif ctx.get("last_domaine") and intent not in {"faq", "general"}:
            services = await api_by_domaine(ctx["last_domaine"], limit=10)

        # CAS 6 : Message général suffisamment long
        elif intent == "general" and wc >= 4:
            services = await api_services_all(limit=10)

    _track_query(domaine, location, len(services))

    active = services or (ctx.get("last_services", []) if is_followup else [])

    faq_hint = None
    if intent in {"faq", "general", "bot_info"}:
        faq_hint = faq_lookup(req.message)

    # ─────────────────────────────────────────────────────────────────────
    # CORRECTION BUG #2 : Ollama SEUL si disponible, fallback UNIQUEMENT sinon
    # Avant : fallback() était appelé PUIS ask_ollama() → double réponse possible
    # ─────────────────────────────────────────────────────────────────────
    reply = await ask_ollama(
        user_msg=req.message,
        ctx=ctx,
        history=history,
        services=active,
        ref_svc=ref_svc,
        all_svcs=all_svcs,
        faq_hint=faq_hint,
    )

    # Fallback activé UNIQUEMENT si Ollama retourne None
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

    # Nettoyage final — aucun lien localhost dans la réponse
    reply = re.sub(r"http://localhost[^\s]*", SITE_URL, reply)
    reply = re.sub(r"localhost:\d+", SITE_URL, reply)

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
        f"services={len(active)} | db={'oui' if should_query_db else 'non'} | "
        f"{'ollama' if _LEARN['stats']['ollama_ok'] > 0 else 'fallback'} | "
        f"{elapsed:.2f}s"
    )

    return ChatResponse(
        reply=reply,
        services=cleaned if cleaned else (ctx.get("last_services", [])[:3] if is_followup else []),
        map_url=mapurl,
        itinerary=itinerary,
        intent=domaine or intent,
        language=lang,
        suggestions=suggestions(domaine, location, ctx),
        confidence=_confidence(req.message, intent),
    )


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
    redis_ok = ollama_ok = model_ok = db_ok = False

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
                    model_ok  = any(OLLAMA_MODEL in m["name"] for m in r.json().get("models", []))
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
        "version":     "9.2.0",
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

    try:
        svcs = await api_by_domaine("Station d'essence", limit=3)
        results["laravel_by_domaine_essence"] = f"OK — {len(svcs)} services"
    except Exception as e:
        results["laravel_by_domaine_essence"] = f"ERREUR: {e}"

    try:
        svcs = await api_services_all(limit=5)
        results["laravel_services_all"] = f"OK — {len(svcs)} services"
    except Exception as e:
        results["laravel_services_all"] = f"ERREUR: {e}"

    try:
        dom = extract_domaine("je cherche de l'essence")
        results["extract_domaine_essence"] = f"OK — '{dom}'" if dom else "ECHEC — None retourné"
    except Exception as e:
        results["extract_domaine_essence"] = f"ERREUR: {e}"

    try:
        dom = extract_domaine("je cherche de l\u2019essence")
        results["extract_domaine_essence_unicode"] = f"OK — '{dom}'" if dom else "ECHEC — None retourné"
    except Exception as e:
        results["extract_domaine_essence_unicode"] = f"ERREUR: {e}"

    try:
        dom = extract_domaine("garage mecanique pres de moi")
        results["extract_domaine_garage"] = f"OK — '{dom}'" if dom else "ECHEC — None retourné"
    except Exception as e:
        results["extract_domaine_garage"] = f"ERREUR: {e}"

    try:
        coords = await geocode("Abomey-Calavi")
        results["geocode_abomey_calavi"] = f"OK — {coords}" if coords else "Aucun résultat"
    except Exception as e:
        results["geocode_abomey_calavi"] = f"ERREUR: {e}"

    if USE_OLLAMA:
        try:
            async with httpx.AsyncClient(timeout=6) as c:
                r = await c.get(f"{OLLAMA_URL}/api/tags")
                models = [m["name"] for m in r.json().get("models", [])] if r.status_code == 200 else []
                results["ollama"] = f"OK — modèles: {models}"
        except Exception as e:
            results["ollama"] = f"ERREUR (non bloquant): {e}"
    else:
        results["ollama"] = "Désactivé — mode fallback actif"

    results["redis"]        = "Connecté" if redis_client else "RAM actif (non bloquant)"
    results["site_url"]     = SITE_URL
    results["version"]      = "9.2.0"
    results["kw2dom_count"] = str(len(KW2DOM))

    return {"version": "9.2.0", "tests": results}