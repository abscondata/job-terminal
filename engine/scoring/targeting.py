from __future__ import annotations

import re
from typing import Iterable

from .profile import UserProfile


_PARIS_CORE_RE = re.compile(
    r"\b(?:paris|750\d{2}|75\d{3}|1er|2e|2eme|3e|4e|5e|6e|7e|8e|9e|10e|11e|12e|13e|14e|15e|16e|17e|18e|19e|20e)\b",
    re.I,
)
_PARIS_REGION_RE = re.compile(
    r"\b(?:la\s+defense|la\s+d[ée]fense|courbevoie|puteaux|neuilly|boulogne(?:-|\s+)billancourt|"
    r"levallois(?:-|\s+)perret|issy(?:-|\s+)les(?:-|\s+)moulineaux|saint(?:-|\s+)denis|"
    r"montreuil|aubervilliers|clichy|montrouge|ile[-\s]?de[-\s]?france|"
    r"ile[-\s]?de[-\s]?france|idf)\b",
    re.I,
)
_NYC_RE = re.compile(
    r"\b(?:new\s+york|nyc|manhattan|brooklyn|queens|bronx|staten\s+island|long\s+island\s+city)\b",
    re.I,
)
_MIAMI_RE = re.compile(
    r"\b(?:miami|miami\s+beach|brickell|wynwood|coral\s+gables|aventura|dade)\b",
    re.I,
)
_FRANCE_OTHER_RE = re.compile(
    r"\b(?:france|lyon|marseille|nice|bordeaux|lille|nantes|toulouse|strasbourg)\b",
    re.I,
)
_US_OTHER_RE = re.compile(
    r"\b(?:united\s+states|usa|us\b|los\s+angeles|san\s+francisco|chicago|boston|atlanta|seattle|washington)\b",
    re.I,
)
_REMOTE_RE = re.compile(r"\b(?:remote|teletravail|t[ée]l[ée]travail|home\s+office|work\s+from\s+home)\b", re.I)

_YEARS_RE = re.compile(r"\b(\d+)\+?\s*(?:years?|yrs?|ans?)\b", re.I)
_SENIOR_TITLE_RE = re.compile(
    r"\b(?:director|head|manager|senior|lead|responsable|vp|vice\s+president|chief|principal)\b",
    re.I,
)
_STRETCH_TITLE_RE = re.compile(
    r"\b(?:specialist|expert|consultant|responsable|chef\s+de\s+projet|project\s+manager)\b",
    re.I,
)
_JUNIORISH_TITLE_RE = re.compile(
    r"\b(?:assistant|assistante|assistant\.?e?|coordinator|coordinateur|coordinatrice|"
    r"project\s+assistant|assistant\s+chef\s+de\s+projet|support|associate|junior)\b",
    re.I,
)

_TOP_WORLD_PATTERNS = [
    # Top-tier financial institutions (banks, BDs, asset managers)
    re.compile(r"\bjp\s*morgan\b", re.I),
    re.compile(r"\bgoldman\s+sachs\b", re.I),
    re.compile(r"\bmorgan\s+stanley\b", re.I),
    re.compile(r"\bciti(?:group|bank)?\b", re.I),
    re.compile(r"\bbank\s+of\s+america\b", re.I),
    re.compile(r"\bbarclays\b", re.I),
    re.compile(r"\bubs\b", re.I),
    re.compile(r"\bdeutsche\s+bank\b", re.I),
    re.compile(r"\bhsbc\b", re.I),
    re.compile(r"\bbnp\s+paribas\b", re.I),
    re.compile(r"\bsociete\s+generale\b", re.I),
    re.compile(r"\bmufg\b", re.I),
    re.compile(r"\bsmbc\b", re.I),
    re.compile(r"\bjefferies\b", re.I),
    re.compile(r"\bcredit\s+(?:agricole|suisse)\b", re.I),
    re.compile(r"\bblackstone\b", re.I),
    re.compile(r"\bkkr\b", re.I),
    re.compile(r"\bapollo\b", re.I),
    re.compile(r"\bcarlyle\b", re.I),
    re.compile(r"\bcitadel\b", re.I),
    re.compile(r"\btwo\s+sigma\b", re.I),
    re.compile(r"\bjane\s+street\b", re.I),
    re.compile(r"\bman\s+group\b", re.I),
    re.compile(r"\bnasdaq\b", re.I),
    re.compile(r"\brothschild\b", re.I),
    re.compile(r"\blazard\b", re.I),
    re.compile(r"\bcoinbase\b", re.I),
    re.compile(r"\brobinhood\b", re.I),
    re.compile(r"\bvanguard\b", re.I),
    re.compile(r"\bfidelity\b", re.I),
    re.compile(r"\bpimco\b", re.I),
    re.compile(r"\btd\s+(?:bank|securities)\b", re.I),
    re.compile(r"\bwells\s+fargo\b", re.I),
    re.compile(r"\bramp\b", re.I),
    re.compile(r"\bstripe\b", re.I),
    re.compile(r"\bklarna\b", re.I),
    re.compile(r"\bmeta\b", re.I),
    re.compile(r"\bwebull\b", re.I),
    re.compile(r"\bmoomoo\b", re.I),
    re.compile(r"\bmoody'?s\b", re.I),
    re.compile(r"\banchorage\s+digital\b", re.I),
    re.compile(r"\bclear\s+street\b", re.I),
]

_WORLD_PATTERNS = [
    (re.compile(r"\b(?:financial\s+services|financial\s+institution|financi[ae]l)\b", re.I), "financial_services"),
    (re.compile(r"\b(?:bank(?:ing)?|investment\s+bank(?:ing)?|commercial\s+bank(?:ing)?)\b", re.I), "banking"),
    (re.compile(r"\b(?:broker[-\s]?dealer|bd|securities\s+firm|brokerage)\b", re.I), "broker_dealer"),
    (re.compile(r"\b(?:asset\s+management|investment\s+management|wealth\s+management|fund\s+management)\b", re.I), "asset_management"),
    (re.compile(r"\b(?:private\s+equity|pe\s+firm|venture\s+capital)\b", re.I), "private_equity"),
    (re.compile(r"\b(?:hedge\s+fund|alternative\s+investments?|multi[-\s]?strategy)\b", re.I), "hedge_fund"),
    (re.compile(r"\b(?:fintech|financial\s+technology|payments?|crypto|digital\s+assets?|blockchain)\b", re.I), "fintech"),
    (re.compile(r"\b(?:capital\s+markets|equities|fixed\s+income|trading|exchanges?)\b", re.I), "capital_markets"),
    (re.compile(r"\b(?:compliance|regulatory|finra|sec|aml|kyc|bsa)\b", re.I), "compliance"),
    (re.compile(r"\b(?:luxury|luxe|maison|haute\s+couture)\b", re.I), "luxury"),
    (re.compile(r"\b(?:fashion|mode|couture|rtw|ready[-\s]?to[-\s]?wear)\b", re.I), "fashion"),
    (re.compile(r"\b(?:jewelry|jewellery|joaillerie|watch(?:es)?|horlogerie)\b", re.I), "jewelry"),
    (re.compile(r"\b(?:beauty|beaute|beauté|cosmetics?|fragrance|parfum|skincare|makeup)\b", re.I), "beauty"),
    (re.compile(r"\b(?:art|gallery|galerie|auction|ench[eè]res|exhibition|museum|foundation|fondation|culture|cultural)\b", re.I), "culture"),
    (re.compile(r"\b(?:premium|high[-\s]?end|client\s+experience|retail\s+excellence|brand\s+experience)\b", re.I), "premium_brand"),
]

_LOW_SIGNAL_WORLD_PATTERNS = [
    re.compile(r"\b(?:mall|outlet|discount|fast\s+fashion|mass[-\s]?market|wholesale|marketplace|drop[-\s]?ship)\b", re.I),
    re.compile(r"\b(?:american\s+eagle|macy'?s|forever\s*21|shein|boohoo|old\s+navy|primark)\b", re.I),
]

_ROLE_BUCKETS = [
    ("assistant chef de projet communication", 1, re.compile(r"\bassistant\s+chef\s+de\s+projet\s+communication\b", re.I)),
    ("assistant chef de projet evenementiel", 1, re.compile(r"\b(?:assistant\s+chef\s+de\s+projet|event)\s+(?:event|evenementiel|événementiel|events?)\b", re.I)),
    ("communication coordinator", 1, re.compile(r"\b(?:communication|communications|internal communication)\s+(?:coordinator|assistant|support)\b|\bcoordinateur(?:rice)?\s+communication\b", re.I)),
    ("content production coordinator", 1, re.compile(r"\b(?:content|contenu|production|campaign)\s+(?:coordinator|coordination|assistant|support)\b", re.I)),
    ("visual merchandising coordination", 1, re.compile(r"\b(?:visual\s+merchandising|vm|merchandising)\s+(?:coordinator|coordination|assistant)\b", re.I)),
    ("project support", 1, re.compile(r"\b(?:project|projet)\s+(?:assistant|support|coordinator|coordination)\b|\bassistant\s+chef\s+de\s+projet\b", re.I)),
    ("retail excellence", 2, re.compile(r"\bretail\s+excellence\b", re.I)),
    ("client experience project support", 2, re.compile(r"\b(?:client|customer)\s+experience\b", re.I)),
    ("omnichannel project assistant", 2, re.compile(r"\bomnichannel\b", re.I)),
    ("digital project support", 2, re.compile(r"\bdigital\s+(?:project|marketing|content)\b", re.I)),
    ("international coordination", 2, re.compile(r"\binternational\s+(?:coordination|coordinator|project)\b", re.I)),
    ("brand operations / launch support", 2, re.compile(r"\b(?:brand\s+operations|launch\s+support|internal communication)\b", re.I)),
    ("gallery coordinator", 3, re.compile(r"\bgallery\s+(?:coordinator|operations|assistant)\b|\bgalerie\b", re.I)),
    ("auction operations", 3, re.compile(r"\b(?:auction|sale|pre-sale)\s+(?:operations|coordinator|assistant)\b|\bpr[eé]-sale\b", re.I)),
    ("art operations", 3, re.compile(r"\bart\s+(?:operations|coordinator|assistant|logistics)\b", re.I)),
    ("foundation / cultural coordination", 3, re.compile(r"\b(?:foundation|fondation|museum|cultural|culture)\s+(?:coordinator|assistant|operations|project)\b", re.I)),
]

_FUNCTION_PATTERNS = [
    (re.compile(r"\b(?:project|projet)\b", re.I), "project"),
    (re.compile(r"\b(?:coordination|coordinate|coordinator|coordinateur|coordinatrice)\b", re.I), "coordination"),
    (re.compile(r"\b(?:communication|communications|internal communication)\b", re.I), "communication"),
    (re.compile(r"\b(?:event|events|evenementiel|événementiel)\b", re.I), "events"),
    (re.compile(r"\b(?:content|contenu|production|campaign|asset workflow)\b", re.I), "content"),
    (re.compile(r"\b(?:visual\s+merchandising|merchandising|vm|display)\b", re.I), "visual_merchandising"),
    (re.compile(r"\b(?:retail\s+excellence|client\s+experience|customer\s+experience|service excellence)\b", re.I), "client_experience"),
    (re.compile(r"\b(?:gallery|auction|art logistics|exhibition|foundation|museum)\b", re.I), "art_ops"),
    (re.compile(r"\b(?:excel|reporting|dashboard|tracking|follow[-\s]?up|documentation|stakeholder|operational execution)\b", re.I), "execution"),
]

_FUNCTION_FAMILIES = [
    ("Compliance / Regulatory Operations", re.compile(r"\b(?:compliance\s+(?:analyst|associate|officer|specialist|coordinator|operations)|regulatory\s+(?:operations|compliance|affairs|analyst|associate)|finra|sec\s+compliance)\b", re.I), 92, "core"),
    ("KYC / Client Onboarding", re.compile(r"\b(?:kyc|know\s+your\s+customer|cdd|due\s+diligence|onboarding\s+(?:analyst|associate|specialist)|client\s+onboarding|account\s+opening|customer\s+identification)\b", re.I), 90, "core"),
    ("AML / Financial Crime", re.compile(r"\b(?:aml|anti[-\s]?money\s+laundering|financial\s+crime|fincen|bsa|bank\s+secrecy|sanctions|ofac|transaction\s+monitoring)\b", re.I), 84, "core"),
    ("Securities Operations", re.compile(r"\b(?:securities\s+(?:operations|ops|analyst)|broker[-\s]?dealer\s+(?:operations|compliance|ops)|bd\s+operations|trade\s+(?:operations|support|processing)|middle\s+office|back\s+office\s+operations)\b", re.I), 86, "core"),
    ("Licensing / Registration", re.compile(r"\b(?:licens(?:ing|e)|registration(?:s)?|finra\s+registration|series\s+7|series\s+63|series\s+66|u4|u5|crd|iard)\b", re.I), 88, "core"),
    ("Trade Surveillance", re.compile(r"\b(?:trade\s+surveillance|market\s+surveillance|surveillance\s+(?:analyst|associate))\b", re.I), 72, "adjacent"),
    ("Risk / Controls", re.compile(r"\b(?:operational\s+risk|risk\s+(?:analyst|associate|management)|internal\s+controls|sox\s+compliance)\b", re.I), 64, "adjacent"),
    ("General Operations", re.compile(r"\b(?:operations(?:\s+(?:analyst|associate|coordinator|support))?|logistics|planning|back\s+office|business\s+support)\b", re.I), 58, "adjacent"),
    ("Gallery / Cultural Coordination", re.compile(r"\b(?:gallery|galerie|auction|pre-sale|art\s+operations|art\s+logistics|foundation|fondation|museum|exhibition|cultural)\b", re.I), 30, "off_lane"),
    ("Content / Production Support", re.compile(r"\b(?:content|contenu|production|campaign|asset workflow|shoot production)\b", re.I), 28, "off_lane"),
    ("Visual Merchandising / Presentation", re.compile(r"\b(?:visual\s+merchandising|visuel\s+merchandising|merchandising|vm|display|vitrine|window\s+display)\b", re.I), 26, "off_lane"),
    ("Events / Experiential Support", re.compile(r"\b(?:event|events|evenementiel|événementiel|activation|experience\s+client|experiential|exhibition\s+support)\b", re.I), 26, "off_lane"),
    ("Communications Support", re.compile(r"\b(?:communication|communications|internal communication|press coordination|pr support)\b", re.I), 26, "off_lane"),
    ("Client Experience / Retail Excellence", re.compile(r"\b(?:client\s+experience|customer\s+experience|retail\s+excellence|omnichannel|crm support)\b", re.I), 24, "off_lane"),
    ("Project Coordination", re.compile(r"\b(?:project\s+(?:assistant|support|coordinator)|assistant\s+chef\s+de\s+projet|chef\s+de\s+projet|coordination|project management support)\b", re.I), 30, "off_lane"),
    (
        "Sales / Client Service",
        re.compile(
            r"\b(?:sales|sales\s+associate|store\s+associate|retail\s+associate|beauty\s+advisor|"
            r"business development|account executive|account manager|customer support|call center|service center|"
            r"conseiller\s+de\s+vente|conseiller\s+beaut[eé]|vendeur|vendeuse|"
            r"h[oô]te\s+de\s+caisse|caissier|caissi[eè]re)\b",
            re.I,
        ),
        18,
        "off_lane",
    ),
]

_CORPORATE_SLOP_PATTERNS = [
    (re.compile(r"\b(?:call center|support center|service center|customer support)\b", re.I), "support_center"),
    (re.compile(r"\b(?:saas|b2b|shared services)\b", re.I), "corporate"),
    (re.compile(r"\b(?:insurance\s+(?:company|agent|adjuster|underwriter)|pharmaceutical|biotech|healthcare\s+compliance)\b", re.I), "wrong_industry"),
]

_SALES_PATTERNS = [
    re.compile(
        r"\b(?:sales|sales\s+associate|store\s+associate|retail\s+associate|beauty\s+advisor|"
        r"account executive|business development|hunter|revenue|quota|commission|"
        r"conseiller\s+de\s+vente|conseiller\s+beaut[eé]|vendeur|vendeuse|"
        r"h[oô]te\s+de\s+caisse|caissier|caissi[eè]re)\b",
        re.I,
    ),
]
_COPY_PATTERNS = [
    re.compile(
        r"\b(?:copywriter|editor|editorial|journalist|redacteur|r[eé]daction|"
        r"content\s+creator|social\s+media\s+creator|ugc|tiktok)\b",
        re.I,
    ),
]
_ART_SPECIALIST_PATTERNS = [
    re.compile(r"\b(?:curator|curatorial|specialist|provenance|catalogu(?:ing|er)|valuer|art advisor)\b", re.I),
]
_LUXURY_EXPERIENCE_PATTERNS = [
    re.compile(r"\b(?:luxury|fashion|beauty|retail|art)\s+experience\s+(?:required|mandatory)\b", re.I),
    re.compile(r"\b\d\+?\s+years?\s+(?:in|within)\s+(?:luxury|fashion|beauty|jewelry|art)\b", re.I),
]

_FRENCH_STRONG_PATTERNS = [
    re.compile(r"\b(?:native|mother tongue)\s+french\b", re.I),
    re.compile(r"\b(?:fluent|excellent|perfect|impeccable)\s+(?:written\s+)?french\b", re.I),
    re.compile(r"\bfran[cç]ais\s+(?:courant|langue\s+maternelle|bilingue|parfait)\b", re.I),
    re.compile(r"\b(?:ma[iî]trise|excellent\s+niveau)\s+(?:du\s+)?fran[cç]ais\b", re.I),
    re.compile(r"\b(?:r[eé]daction|orthographe)\b.*\bfran[cç]ais\b", re.I),
]
_FRENCH_MID_PATTERNS = [
    re.compile(r"\b(?:french\s+required|bilingual\s+french|english\s+and\s+french)\b", re.I),
    re.compile(r"\bfran[cç]ais\s+(?:souhait[eé]|appr[eé]ci[eé]|requis)\b", re.I),
]
_ENGLISH_FRIENDLY_PATTERNS = [
    re.compile(r"\b(?:english\s+required|native english|international environment|anglais courant|english-speaking)\b", re.I),
]

_WORK_TYPE_PATTERNS = [
    ("internship", "Internship", re.compile(r"\b(?:internship|intern|stage|stagiaire)\b", re.I)),
    ("traineeship", "Traineeship / Apprenticeship", re.compile(r"\b(?:traineeship|apprentice(?:ship)?|apprentissage|alternance|graduate program)\b", re.I)),
    ("part_time", "Part-time", re.compile(r"\b(?:part[-\s]?time|temps partiel)\b", re.I)),
    ("contract", "Contract / Freelance", re.compile(r"\b(?:contract|freelance|interim|temporary|temporaire|consulting mission)\b", re.I)),
]

_APPLY_BLOCKER_TITLE_RE = re.compile(
    r"\b(?:teacher|engineer|developer|lawyer|attorney|nurse|warehouse|mechanic)\b",
    re.I,
)


def normalize_blob(*parts: str) -> str:
    return " ".join(part for part in parts if part)


def _extract_years_required(text: str) -> int | None:
    best = 0
    for match in _YEARS_RE.finditer(text or ""):
        try:
            best = max(best, int(match.group(1)))
        except (TypeError, ValueError):
            continue
    return best or None


def resolve_work_location(
    location_text: str,
    description_text: str = "",
    arrangement: str = "",
) -> dict:
    raw = normalize_blob(location_text, description_text)
    arrangement_key = (arrangement or "").lower()
    if _PARIS_CORE_RE.search(location_text or ""):
        return {"label": "paris_core", "resolved_location": "Paris", "reason": "paris_core", "source": "location_field"}
    if _PARIS_REGION_RE.search(location_text or ""):
        return {"label": "paris_region", "resolved_location": (location_text or "").strip(), "reason": "paris_region", "source": "location_field"}
    if _NYC_RE.search(location_text or ""):
        return {"label": "nyc", "resolved_location": "New York City", "reason": "nyc", "source": "location_field"}
    if _MIAMI_RE.search(location_text or ""):
        return {"label": "miami", "resolved_location": "Miami", "reason": "miami", "source": "location_field"}
    if _PARIS_CORE_RE.search(description_text or ""):
        return {"label": "paris_core", "resolved_location": "Paris", "reason": "paris_core_desc", "source": "description"}
    if _PARIS_REGION_RE.search(description_text or ""):
        return {"label": "paris_region", "resolved_location": (location_text or description_text or "").strip(), "reason": "paris_region_desc", "source": "description"}
    if _NYC_RE.search(description_text or ""):
        return {"label": "nyc", "resolved_location": "New York City", "reason": "nyc_desc", "source": "description"}
    if _MIAMI_RE.search(description_text or ""):
        return {"label": "miami", "resolved_location": "Miami", "reason": "miami_desc", "source": "description"}
    if arrangement_key in {"remote", "remote_anywhere", "remote_us_only"} or _REMOTE_RE.search(raw):
        return {"label": "remote", "resolved_location": "Remote", "reason": "remote", "source": "arrangement"}
    if _FRANCE_OTHER_RE.search(raw):
        return {"label": "france_other", "resolved_location": (location_text or "").strip(), "reason": "france_other", "source": "location_field"}
    if _US_OTHER_RE.search(raw):
        return {"label": "us_other", "resolved_location": (location_text or "").strip(), "reason": "us_other", "source": "location_field"}
    if location_text:
        return {"label": "other", "resolved_location": location_text.strip(), "reason": "other_market", "source": "location_field"}
    return {"label": "unknown", "resolved_location": "", "reason": "unknown", "source": "location_field"}


def assess_market_preference(location_text: str, profile: UserProfile | None = None) -> dict:
    resolved = resolve_work_location(location_text)
    profile = profile or UserProfile()
    score = profile.location_priority_scores.get(resolved["label"], 24)
    label_map = {
        "paris_core": "paris",
        "paris_region": "paris",
        "nyc": "nyc",
        "miami": "miami",
    }
    return {
        "label": label_map.get(resolved["label"], resolved["label"]),
        "location_label": resolved["label"],
        "score": int(score),
        "reason": resolved["reason"],
        "resolved_location": resolved["resolved_location"],
    }


def assess_location_priority(
    location_text: str,
    arrangement: str,
    profile: UserProfile | None = None,
) -> dict:
    profile = profile or UserProfile()
    resolved = resolve_work_location(location_text, arrangement=arrangement)
    score = profile.location_priority_scores.get(resolved["label"], 24)
    market_map = {
        "paris_core": "paris",
        "paris_region": "paris",
        "nyc": "nyc",
        "miami": "miami",
    }
    return {
        "label": resolved["label"],
        "score": int(score),
        "market": market_map.get(resolved["label"], resolved["label"]),
        "arrangement": (arrangement or "unknown").lower() or "unknown",
        "reason": resolved["reason"],
        "resolved_location": resolved["resolved_location"],
    }


def city_lane(label: str) -> dict:
    if label == "paris_core":
        return {"label": "Paris", "priority": "primary", "story": "dream lane", "score": 100}
    if label == "paris_region":
        return {"label": "Paris Region", "priority": "primary", "story": "dream lane with commute realism", "score": 92}
    if label == "nyc":
        return {"label": "NYC", "priority": "secondary", "story": "english-heavy direction lane", "score": 78}
    if label == "miami":
        return {"label": "Miami", "priority": "tertiary", "story": "backup city lane", "score": 56}
    if label == "france_other":
        return {"label": "France Outside Paris", "priority": "adjacent", "story": "france move without the main city payoff", "score": 42}
    if label == "us_other":
        return {"label": "US Other", "priority": "adjacent", "story": "platform move without the main cities", "score": 34}
    if label == "remote":
        return {"label": "Remote", "priority": "off_lane", "story": "does not solve the city move", "score": 14}
    if label == "other":
        return {"label": "Off-Lane", "priority": "off_lane", "story": "outside the core city plan", "score": 24}
    return {"label": "Unknown", "priority": "unclear", "story": "city value not confirmed", "score": 28}


def world_hits(text: str, company: str = "") -> list[str]:
    hits: list[str] = []
    blob = normalize_blob(text, company)
    for pattern, label in _WORLD_PATTERNS:
        if pattern.search(blob) and label not in hits:
            hits.append(label)
    for pattern in _TOP_WORLD_PATTERNS:
        if pattern.search(blob) and "target_brand" not in hits:
            hits.append("target_brand")
    return hits


def world_tier(text: str, company: str = "") -> dict:
    blob = normalize_blob(text, company)
    hits = world_hits(text, company)
    top_brand = "target_brand" in hits
    low_signal = any(pattern.search(blob) for pattern in _LOW_SIGNAL_WORLD_PATTERNS)
    if top_brand:
        return {
            "label": "Top Luxury / Culture World",
            "score": 96,
            "reason": "top_brand_or_institution",
            "top_brand": True,
            "low_signal": False,
        }
    if low_signal:
        return {
            "label": "Low-Signal Commercial World",
            "score": 26,
            "reason": "mass_market_or_low_signal_brand",
            "top_brand": False,
            "low_signal": True,
        }
    if {"financial_services", "banking", "broker_dealer", "asset_management", "private_equity", "hedge_fund", "capital_markets", "compliance"} & set(hits):
        return {
            "label": "Financial Services World",
            "score": 82,
            "reason": "financial_services_alignment",
            "top_brand": False,
            "low_signal": False,
        }
    if "fintech" in hits:
        return {
            "label": "Fintech World",
            "score": 72,
            "reason": "fintech_alignment",
            "top_brand": False,
            "low_signal": False,
        }
    if {"luxury", "fashion", "jewelry", "beauty", "culture"} & set(hits):
        return {
            "label": "Real Adjacent World",
            "score": 36,
            "reason": "credible_world_alignment",
            "top_brand": False,
            "low_signal": False,
        }
    if "premium_brand" in hits:
        return {
            "label": "Premium But Generic World",
            "score": 30,
            "reason": "premium_adjacent_without_strong_world_signal",
            "top_brand": False,
            "low_signal": False,
        }
    if corporate_slop_hits(blob):
        return {
            "label": "Corporate World",
            "score": 18,
            "reason": "corporate_signals_dominate",
            "top_brand": False,
            "low_signal": False,
        }
    return {
        "label": "Unknown World",
        "score": 36,
        "reason": "world_not_clearly_signaled",
        "top_brand": False,
        "low_signal": False,
    }


def function_hits(text: str) -> list[str]:
    hits: list[str] = []
    for pattern, label in _FUNCTION_PATTERNS:
        if pattern.search(text or "") and label not in hits:
            hits.append(label)
    return hits


def primary_function_family(title: str, description: str = "") -> dict:
    title_blob = title or ""
    blob = normalize_blob(title, description)
    for label, pattern, score, strength in _FUNCTION_FAMILIES:
        if pattern.search(title_blob):
            return {"label": label, "score": score, "strength": strength}
    for label, pattern, score, strength in _FUNCTION_FAMILIES:
        if pattern.search(blob):
            return {"label": label, "score": score, "strength": strength}
    return {"label": "Unclear", "score": 42, "strength": "unclear"}


def corporate_slop_hits(text: str) -> list[str]:
    hits: list[str] = []
    for pattern, label in _CORPORATE_SLOP_PATTERNS:
        if pattern.search(text or "") and label not in hits:
            hits.append(label)
    return hits


def role_bucket(title: str, description: str = "") -> dict:
    blob = normalize_blob(title, description)
    for label, tier, pattern in _ROLE_BUCKETS:
        if pattern.search(blob):
            return {"label": label, "tier": tier}
    return {"label": "unclassified", "tier": 0}


def work_type(title: str, description: str = "") -> dict:
    blob = normalize_blob(title, description)
    for key, label, pattern in _WORK_TYPE_PATTERNS:
        if pattern.search(blob):
            return {"key": key, "label": label}
    return {"key": "full_time", "label": "Full-time"}


def french_access(text: str, title: str = "") -> dict:
    blob = normalize_blob(title, text)
    if any(pattern.search(blob) for pattern in _FRENCH_STRONG_PATTERNS):
        return {"score": 8, "risk": "high", "reason": "strong_written_or_native_french"}
    if any(pattern.search(blob) for pattern in _FRENCH_MID_PATTERNS):
        return {"score": 42, "risk": "medium", "reason": "french_required_or_strongly_preferred"}
    if any(pattern.search(blob) for pattern in _ENGLISH_FRIENDLY_PATTERNS):
        return {"score": 90, "risk": "low", "reason": "english_friendly"}
    if re.search(r"\b(?:communication|content|editorial|copy|redaction|r[eé]daction)\b", title or "", re.I):
        return {"score": 58, "risk": "medium", "reason": "written_comms_role_language_risk"}
    if re.search(r"\b(?:fran[cç]ais|paris|maison|fondation|galerie)\b", blob, re.I):
        return {"score": 70, "risk": "medium", "reason": "french_environment"}
    return {"score": 86, "risk": "low", "reason": "no_strong_french_signal"}


def seniority_risk(title: str, description: str = "") -> dict:
    years = _extract_years_required(description)
    if _SENIOR_TITLE_RE.search(title or ""):
        return {"score": 10, "risk": "high", "reason": "senior_title", "years_required": years}
    if years is not None and years >= 5:
        return {"score": 14, "risk": "high", "reason": "5_plus_years", "years_required": years}
    if years is not None and years >= 4:
        return {"score": 30, "risk": "medium", "reason": "4_years", "years_required": years}
    if _STRETCH_TITLE_RE.search(title or "") and not _JUNIORISH_TITLE_RE.search(title or ""):
        return {"score": 52, "risk": "medium", "reason": "stretch_title", "years_required": years}
    return {"score": 88, "risk": "low", "reason": "within_range", "years_required": years}


def realism_risks(title: str, description: str = "") -> list[str]:
    blob = normalize_blob(title, description)
    risks: list[str] = []
    if any(pattern.search(blob) for pattern in _LUXURY_EXPERIENCE_PATTERNS):
        risks.append("direct_luxury_experience_required")
    if any(pattern.search(blob) for pattern in _ART_SPECIALIST_PATTERNS):
        risks.append("specialist_art_market_background")
    if any(pattern.search(blob) for pattern in _SALES_PATTERNS):
        risks.append("sales_heavy")
    if any(pattern.search(blob) for pattern in _COPY_PATTERNS):
        risks.append("copywriting_or_editorial")
    if re.search(r"\b(?:degree required|bachelor'?s required|master'?s required)\b", blob, re.I):
        risks.append("degree_gate")
    return risks


def classify_exclusion(title: str, description: str = "") -> str | None:
    blob = normalize_blob(title, description)
    if _APPLY_BLOCKER_TITLE_RE.search(title or ""):
        return "hard_reject:off_mission_role"
    if re.search(r"\b20\s+hours?\s+weekly\b", blob, re.I):
        return "hard_reject:part_time"
    return None


def role_bucket_bonus(bucket: dict) -> int:
    tier = int(bucket.get("tier", 0))
    if tier == 1:
        return 92
    if tier == 2:
        return 76
    if tier == 3:
        return 68
    return 28


def count_matches(values: Iterable[str], wanted: Iterable[str]) -> int:
    wanted_set = {value.lower() for value in wanted}
    return sum(1 for value in values if value.lower() in wanted_set)
