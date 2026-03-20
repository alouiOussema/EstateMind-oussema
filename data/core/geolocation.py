from typing import Dict, Any, Tuple, List, Optional
import math
import time
import json
import re
import unicodedata
from functools import lru_cache
from pathlib import Path

import requests

from core.models import Location, POI
from config.logging_config import log


def build_location_from_subtitle(subtitle: str) -> Location:
    parts = [p.strip() for p in subtitle.split(",") if p.strip()]
    city = None
    district = None

    if len(parts) >= 2:
        district = parts[-2]
        city = parts[-1]
    elif len(parts) == 1:
        city = parts[0]

    return Location(city=city or "", district=district)


def infer_governorate(city: str) -> str:
    city_norm = (city or "").strip().lower()
    mapping = {
        "tunis": "Tunis",
        "ariane": "Ariana",
        "ariana": "Ariana",
        "la marsa": "Tunis",
        "carthage": "Tunis",
        "la soukra": "Ariana",
        "nabeul": "Nabeul",
        "bizerte": "Bizerte",
        "sousse": "Sousse",
        "monastir": "Monastir",
        "mahdia": "Mahdia",
        "sfax": "Sfax",
        "gabes": "Gabes",
        "medenine": "Medenine",
        "djerba": "Medenine",
        "tataouine": "Tataouine",
        "kairouan": "Kairouan",
        "kasserine": "Kasserine",
        "gafsa": "Gafsa",
        "tozeur": "Tozeur",
        "kebili": "Kebili",
        "siliana": "Siliana",
        "jendouba": "Jendouba",
        "beja": "Beja",
        "le kef": "Kef",
        "kef": "Kef",
        "zaghouan": "Zaghouan",
        "manouba": "Manouba",
        "ben arous": "Ben Arous",
    }
    return mapping.get(city_norm, "")


def infer_region_and_zone(location: Dict[str, Any]) -> Tuple[str, str]:
    gov_raw = location.get("governorate") or ""
    city_raw = location.get("city") or ""
    governorate = str(gov_raw).strip().lower()
    city = str(city_raw).strip().lower()

    if not governorate and city:
        governorate = infer_governorate(city).lower()

    region_name = governorate.title() if governorate else None

    north_govs = {
        "tunis",
        "ariana",
        "ben arous",
        "manouba",
        "nabeul",
        "bizerte",
        "beja",
        "jendouba",
        "kef",
        "zaghouan",
        "siliana",
    }
    east_govs = {
        "nabeul",
        "sousse",
        "monastir",
        "mahdia",
        "sfax",
    }
    west_govs = {
        "beja",
        "jendouba",
        "kef",
        "siliana",
        "kairouan",
        "kasserine",
        "sidi bouzid",
    }
    south_govs = {
        "sfax",
        "gabes",
        "medenine",
        "gafsa",
        "tozeur",
        "kebili",
        "tataouine",
    }

    zone = None
    if governorate in north_govs:
        zone = "nord"
    elif governorate in east_govs:
        zone = "east"
    elif governorate in west_govs:
        zone = "west"
    elif governorate in south_govs:
        zone = "south"

    return region_name, zone


def normalize_poi_dict(raw_poi: Dict[str, Any]) -> List[POI]:
    pois: List[POI] = []
    for cat, entries in raw_poi.items():
        for name in entries:
            if not name:
                continue
            pois.append(POI(name=name, category=cat, distance_m=None))
    return pois


def _normalize_text(value: Optional[str]) -> str:
    if not value:
        return ""
    text = unicodedata.normalize("NFKD", str(value))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return text.strip()


@lru_cache(maxsize=1)
def _load_tunisia_municipalities() -> List[Dict[str, Any]]:
    try:
        base_dir = Path(__file__).resolve().parent.parent
        ts_path = base_dir / "data" / "data.ts"
        raw = ts_path.read_text(encoding="utf-8")
    except Exception as e:
        log.error(f"Failed to read Tunisia municipalities file: {e}")
        return []
    m = re.search(r"export\s+const\s+data\s*=\s*(\[.*\]);?\s*$", raw, re.S)
    if not m:
        log.error("Could not locate data array in data.ts")
        return []
    payload = m.group(1)
    for key in ("Name", "NameAr", "Value", "Delegations", "PostalCode", "Latitude", "Longitude"):
        payload = re.sub(rf"{key}\s*:", f'"{key}":', payload)
    payload = re.sub(r",(\s*[}\]])", r"\1", payload)
    try:
        data = json.loads(payload)
    except Exception as e:
        log.error(f"Failed to parse Tunisia municipalities JSON: {e}")
        return []
    return data


@lru_cache(maxsize=1)
def _flatten_tunisia_delegations() -> List[Dict[str, Any]]:
    data = _load_tunisia_municipalities()
    flat: List[Dict[str, Any]] = []
    for gov in data:
        gov_name = gov.get("Name") or ""
        gov_value = gov.get("Value") or ""
        gov_norm = _normalize_text(gov_value or gov_name)
        for d in gov.get("Delegations") or []:
            muni_name = d.get("Name") or ""
            muni_value = d.get("Value") or ""
            flat.append(
                {
                    "governorate_name": gov_name,
                    "governorate_value": gov_value,
                    "governorate_norm": gov_norm,
                    "muni_name": muni_name,
                    "muni_value": muni_value,
                    "muni_norm": _normalize_text(muni_value or muni_name),
                    "postal_code": str(d.get("PostalCode") or ""),
                    "lat": d.get("Latitude"),
                    "lon": d.get("Longitude"),
                }
            )
    return flat


def _match_local_delegation(city: Optional[str], governorate: Optional[str], address: Optional[str]) -> Optional[Dict[str, Any]]:
    all_text = " ".join(
        part for part in [city or "", governorate or "", address or ""] if part
    )
    norm_all = _normalize_text(all_text)
    norm_city = _normalize_text(city)
    norm_gov = _normalize_text(governorate)
    if not norm_all and not norm_city:
        return None
    candidates = _flatten_tunisia_delegations()
    best = None
    best_score = 0
    for rec in candidates:
        score = 0
        muni_norm = rec["muni_norm"]
        gov_norm = rec["governorate_norm"]
        if muni_norm and muni_norm in norm_all:
            score += 4
        if muni_norm and muni_norm == norm_city:
            score += 3
        if gov_norm and gov_norm in norm_all:
            score += 2
        if gov_norm and gov_norm == norm_gov:
            score += 1
        if score > best_score:
            best_score = score
            best = rec
            if best_score >= 5:
                break
    return best


def _build_address_query(city: Optional[str], governorate: Optional[str], address: Optional[str]) -> Optional[str]:
    parts: List[str] = []
    if address:
        parts.append(str(address))
    if city:
        parts.append(str(city))
    if governorate:
        parts.append(str(governorate))
    parts.append("Tunisia")
    joined = ", ".join([p for p in parts if p])
    if not joined:
        return None
    return joined


def geocode_location(city: Optional[str], governorate: Optional[str], address: Optional[str]) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    local_match = _match_local_delegation(city, governorate, address)
    if local_match and local_match.get("lat") is not None and local_match.get("lon") is not None:
        lat = local_match["lat"]
        lon = local_match["lon"]
        muni_raw = local_match["muni_value"] or local_match["muni_name"]
        municipality = str(muni_raw).title() if muni_raw else None
        return lat, lon, municipality
    query = _build_address_query(city, governorate, address)
    if not query:
        return None, None, None
    try:
        resp = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={
                "q": query,
                "format": "json",
                "addressdetails": 1,
                "limit": 1,
            },
            headers={"User-Agent": "EstateMind/1.0"},
            timeout=10,
        )
    except Exception as e:
        log.error(f"Geocoding error for query={query!r}: {e}")
        return None, None, None
    if resp.status_code != 200:
        log.error(f"Geocoding HTTP {resp.status_code} for query={query!r}")
        return None, None, None
    try:
        data = resp.json()
    except Exception as e:
        log.error(f"Geocoding parse error for query={query!r}: {e}")
        return None, None, None
    if not data:
        return None, None, None
    first = data[0]
    try:
        lat = float(first.get("lat"))
        lon = float(first.get("lon"))
    except Exception:
        return None, None, None
    address_data = first.get("address") or {}
    muni_candidates = [
        "municipality",
        "city_district",
        "suburb",
        "town",
        "village",
        "neighbourhood",
    ]
    municipality = None
    for key in muni_candidates:
        value = address_data.get(key)
        if value:
            municipality = value
            break
    time.sleep(1.0)
    return lat, lon, municipality


def _haversine_distance_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


_POI_CACHE: Dict[Tuple[float, float, int], List[POI]] = {}


def fetch_pois(lat: float, lon: float, radius_m: int = 750) -> List[POI]:
    key = (round(lat, 4), round(lon, 4), int(radius_m))
    cached = _POI_CACHE.get(key)
    if cached is not None:
        return cached
    query = f"""
    [out:json][timeout:25];
    (
      node["amenity"="school"](around:{radius_m},{lat},{lon});
      node["amenity"="university"](around:{radius_m},{lat},{lon});
      node["amenity"="hospital"](around:{radius_m},{lat},{lon});
      node["amenity"="clinic"](around:{radius_m},{lat},{lon});
      node["amenity"="pharmacy"](around:{radius_m},{lat},{lon});
      node["shop"](around:{radius_m},{lat},{lon});
      node["amenity"="restaurant"](around:{radius_m},{lat},{lon});
      node["amenity"="cafe"](around:{radius_m},{lat},{lon});
      node["public_transport"](around:{radius_m},{lat},{lon});
      node["bus_station"](around:{radius_m},{lat},{lon});
      node["railway"="station"](around:{radius_m},{lat},{lon});
    );
    out body;
    """
    try:
        resp = requests.post(
            "https://overpass-api.de/api/interpreter",
            data=query.encode("utf-8"),
            headers={"User-Agent": "EstateMind/1.0"},
            timeout=30,
        )
    except Exception as e:
        log.error(f"POI fetch error at ({lat},{lon}): {e}")
        return []
    if resp.status_code != 200:
        log.error(f"POI HTTP {resp.status_code} at ({lat},{lon})")
        return []
    try:
        data = resp.json()
    except Exception as e:
        log.error(f"POI parse error at ({lat},{lon}): {e}")
        return []
    elements = data.get("elements") or []
    pois: List[POI] = []
    for el in elements:
        tags = el.get("tags") or {}
        name = tags.get("name")
        if not name:
            continue
        amenity = tags.get("amenity") or ""
        shop = tags.get("shop") or ""
        pt = tags.get("public_transport") or ""
        cat = "other"
        if amenity in {"school", "university"}:
            cat = "school"
        elif amenity in {"hospital", "clinic", "pharmacy"}:
            cat = "hospital"
        elif shop:
            cat = "shopping"
        elif amenity in {"restaurant", "cafe"}:
            cat = "restaurant"
        elif pt or amenity in {"bus_station"} or tags.get("railway") == "station":
            cat = "transport"
        try:
            plat = float(el.get("lat"))
            plon = float(el.get("lon"))
            dist = _haversine_distance_m(lat, lon, plat, plon)
        except Exception:
            dist = None
        pois.append(POI(name=name, category=cat, distance_m=dist))
    _POI_CACHE[key] = pois
    time.sleep(1.0)
    return pois
