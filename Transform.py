"""
transform.py — Fase TRANSFORM dell'ETL
========================================
Unisce staging e transform in un solo passo.

Input:  products.csv, reviews.csv
Output: stg_products.csv, stg_reviews.csv

Products:
  - filtra gli accessori (non-lavastoviglie)
  - pulisce il nome (virgolette, model code finale)
  - rilevamento has_* flags e snippet evidence

Reviews:
  - filtra le review fuori catalogo
  - aggiunge language e country dal locale
  - codifica numerica di age, household, usage_frequency
  - traduce title e text in inglese (con cache)

Requisiti:
    pip install deep-translator

Uso:
    python transform.py
    python transform.py --products products.csv --reviews reviews.csv
"""

import csv
import json
import re
import time
import argparse
import logging
from pathlib import Path
from deep_translator import GoogleTranslator
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

RATE_LIMIT_SLEEP = 0.15
BATCH_LOG_EVERY  = 100
SKIP_LOCALES     = {"en_GB", "en_IE", "en_US"}

# ---------------------------------------------------------------------------
# Pulizia nome prodotto
# ---------------------------------------------------------------------------

_MODEL_CODE_SUFFIX = re.compile(r'[A-Z]{2,4}\d[A-Z0-9]{5,13}$')


def clean_name(name: str) -> str:
    name = name.replace('"', '').replace("'", '')
    name = _MODEL_CODE_SUFFIX.sub('', name).strip(', ')
    return name


# ---------------------------------------------------------------------------
# Filtro prodotti
# ---------------------------------------------------------------------------

def is_dishwasher(row: dict) -> bool:
    url  = row.get("url", "").lower()
    name = row.get("name", "").lower()
    if "/accessori/" in url:
        return False
    return "/lavastoviglie" in url or "lavastoviglie" in name or "dishwasher" in name


# ---------------------------------------------------------------------------
# Feature detection (da highlights_raw)
# ---------------------------------------------------------------------------

FEATURES = {
    # CONNECTIVITY
    "has_home_connect":       re.compile(r'Home\s*Connect', re.I),
    "has_smart_start":        re.compile(r'Smart\s+Start', re.I),
    "has_programme_download": re.compile(r'Programme\s+Download', re.I),
    "has_intelligent_prog":   re.compile(r'Intelligent\s+Programme', re.I),
    "has_wash_assistant":     re.compile(r'Assistente\s+al\s+lavaggio', re.I),

    # DRYING
    "has_efficient_dry": re.compile(r'Efficient\s*Dry', re.I),
    "has_perfect_dry":   re.compile(r'PerfectDry|Perfect\s+Dry', re.I),

    # CLEANING
    "has_extra_clean_zone": re.compile(r'Extra\s+Clean\s+Zone', re.I),

    # HYGIENE
    "has_hygiene_certified":  re.compile(r'igiene\s+certificato|Livello\s+di\s+igiene', re.I),
    "has_remote_diagnostics": re.compile(r'Diagnostica\s+da\s+remoto', re.I),

    # SUSTAINABILITY
    "has_green_collection": re.compile(r'Green\s+Collection', re.I),

    # NOISE / MOTOR
    "has_silent_power_drive": re.compile(r'SilentPowerDrive|Silent\s+Power\s+Drive', re.I),

    # SAFETY
    "has_aquastop": re.compile(r'Aqua\s*Stop', re.I),

    # UX / DESIGN / LOADING
    "has_time_light":    re.compile(r'Time\s+Light', re.I),
    "has_emotion_light": re.compile(r'Emotion\s+Light', re.I),
    "has_status_light":  re.compile(r'Status\s+Light', re.I),
    "has_open_assist":   re.compile(r'Open\s+Assist', re.I),
    "has_rackmatic":     re.compile(r'RackMatic', re.I),
    "has_max_flex_pro":  re.compile(r'Max\s+Flex\s+Pro', re.I),
    "has_dosage_assist": re.compile(r'Dosage\s+Assist', re.I),
    "has_vario_hinge":   re.compile(r'Vario\s+Hinge', re.I),
    "has_favorite_program": re.compile(r'Programma\s+Preferito', re.I),
}

_NOISE = [
    re.compile(r'^Nota[\s\*]'),
    re.compile(r'^https?://'),
    re.compile(r'Visualizza tutte', re.I),
    re.compile(r'Assistenza gratuita', re.I),
    re.compile(r'Registra il tuo', re.I),
    re.compile(r'Scopri di più', re.I),
]


def extract_claims(highlights_raw: str) -> list[str]:
    if not highlights_raw:
        return []
    return [
        p.strip()
        for p in highlights_raw.split(" | ")
        if p.strip() and len(p.strip()) > 10 and not any(n.search(p) for n in _NOISE)
    ]


def detect(pattern, claims: list[str]) -> tuple[bool, str]:
    for claim in claims:
        m = pattern.search(claim)
        if m:
            start   = max(0, m.start() - 40)
            end     = min(len(claim), m.end() + 60)
            snippet = re.sub(r'\s+', ' ', claim[start:end].strip())
            return True, f"...{snippet}..."
    return False, ""


# ---------------------------------------------------------------------------
# Arricchimento recensioni
# ---------------------------------------------------------------------------

# Mappa locale → (language, country)
# Split su '_' copre tutti i casi; questa mappa serve per i nomi estesi
# se in futuro si volesse usarli. Per ora usiamo direttamente lo split.

AGE_MIDPOINT = {
    "17 o meno":   15,
    "Da 18 a 24":  21,
    "Da 25 a 34":  29,
    "Da 35 a 44":  39,
    "Da 45 a 54":  49,
    "Da 55 a 64":  59,
    "65 o più":    68,
}

HOUSEHOLD_SIZE = {
    "uomo":                        1,
    "donna":                       1,
    "coppia":                      2,
    "famiglia con figli piccoli":  4,
    "famiglia con figli più grandi": 4,
}

USAGE_WEEKLY = {
    "(quasi) tutti i giorni":       7,
    "un paio di volte a settimana": 2.5,
    "un paio di volte al mese":     0.5,
    "raramente":                    0.1,
}


def locale_parts(locale: str) -> tuple[str, str]:
    """'it_IT' → ('it', 'IT')"""
    parts = locale.split("_", 1)
    return (parts[0], parts[1]) if len(parts) == 2 else (locale, "")


# ---------------------------------------------------------------------------
# Traduzione con cache
# ---------------------------------------------------------------------------

def load_cache(path: Path) -> dict:
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def save_cache(cache: dict, path: Path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False)


def translate(text: str) -> str:
    if not text or not text.strip():
        return ""
    result = GoogleTranslator(source="auto", target="en").translate(text)
    time.sleep(RATE_LIMIT_SLEEP)
    return result or ""


# ---------------------------------------------------------------------------
# Transform products
# ---------------------------------------------------------------------------

def transform_products(products_path: Path, out_path: Path) -> set:
    with open(products_path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    dishwashers = [r for r in rows if is_dishwasher(r)]
    log.info("Products: %d totali → %d lavastoviglie (esclusi %d accessori/altro)",
             len(rows), len(dishwashers), len(rows) - len(dishwashers))

    output_rows = []
    for row in dishwashers:
        out = dict(row)
        out["name"] = clean_name(row.get("name", ""))
        claims = extract_claims(row.get("highlights_raw", ""))
        out["highlights"] = " | ".join(claims)
        for feat, pattern in FEATURES.items():
            found, snippet        = detect(pattern, claims)
            out[feat]             = found
            out[f"{feat}_evidence"] = snippet
        output_rows.append(out)

    base_keys = [k for k in dishwashers[0].keys() if k != "highlights_raw"]
    feat_keys = [key for feat in FEATURES for key in (feat, f"{feat}_evidence")]
    all_keys  = base_keys + ["highlights"] + feat_keys

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(output_rows)

    log.info("Salvato %s (%d righe)", out_path, len(output_rows))

    active = [r for r in output_rows if r.get("is_discontinued") not in (True, "True")]
    log.info("\nFeature distribution (su %d prodotti attivi):", len(active))
    for feat in FEATURES:
        n = sum(1 for r in active if r.get(feat) in (True, "True"))
        log.info("  %-30s %3d/%d (%d%%)", feat, n, len(active),
                 100 * n // len(active) if active else 0)

    return {r["model_code"] for r in output_rows}


# ---------------------------------------------------------------------------
# Transform reviews
# ---------------------------------------------------------------------------

def transform_reviews(reviews_path: Path, valid_codes: set,
                      out_path: Path, cache_path: Path):
    with open(reviews_path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    filtered = [r for r in rows if r.get("queried_product_id") in valid_codes]
    log.info("Reviews: %d totali → %d in catalogo (escluse %d)",
             len(rows), len(filtered), len(rows) - len(filtered))

    cache          = load_cache(cache_path)
    translated_now = 0
    log.info("Cache traduzioni: %d entry", len(cache))

    # Campi aggiuntivi
    extra = ["language", "country", "age_midpoint", "household_size",
             "usage_weekly", "title_en", "text_en", "year", "month", "day", "time"]
    out_fields = list(filtered[0].keys()) + extra

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=out_fields)
        writer.writeheader()

        for i, row in enumerate(filtered):
            if i > 0 and i % BATCH_LOG_EVERY == 0:
                log.info("Progresso: %d/%d  |  tradotte ora: %d",
                         i, len(filtered), translated_now)

            # Locale → language + country
            lang, country = locale_parts(row.get("locale", ""))
            row["language"] = lang
            row["country"]  = country

            # date
            row_date = row.get("date")

            if row_date:
                dt = datetime.fromisoformat(row_date.replace("Z", "+00:00"))

                row["year"]  = dt.year
                row["month"] = dt.month
                row["day"]   = dt.day
                row["time"]  = str(dt.time())
            else:
                row["year"] = row["month"] = row["day"] = row["time"] = None

            # Codifiche numeriche
            row["age_midpoint"]    = AGE_MIDPOINT.get(row.get("age", ""), None)
            row["household_size"]  = HOUSEHOLD_SIZE.get(row.get("household", ""), None)
            row["usage_weekly"]    = USAGE_WEEKLY.get(row.get("usage_frequency", ""), None)

            # Traduzione
            rid    = row.get("review_id", "")
            locale = row.get("locale", "")

            if rid in cache:
                row["title_en"] = cache[rid]["title_en"]
                row["text_en"]  = cache[rid]["text_en"]
            elif locale in SKIP_LOCALES:
                row["title_en"] = row.get("title", "")
                row["text_en"]  = row.get("text",  "")
                cache[rid] = {"title_en": row["title_en"], "text_en": row["text_en"]}
            else:
                row["title_en"] = translate(row.get("title", ""))
                row["text_en"]  = translate(row.get("text",  ""))
                translated_now += 1
                cache[rid] = {"title_en": row["title_en"], "text_en": row["text_en"]}
                if translated_now % 50 == 0:
                    save_cache(cache, cache_path)

            writer.writerow(row)

    save_cache(cache, cache_path)
    log.info("Tradotte: %d  |  da cache: %d  |  Salvato %s",
             translated_now, len(filtered) - translated_now, out_path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--products", default="products.csv")
    parser.add_argument("--reviews",  default="reviews.csv")
    args = parser.parse_args()

    log.info("=== TRANSFORM: products ===")
    valid_codes = transform_products(
        Path(args.products),
        Path("stg_products.csv"),
    )

    log.info("=== TRANSFORM: reviews ===")
    transform_reviews(
        Path(args.reviews),
        valid_codes,
        Path("stg_reviews.csv"),
        Path(".translation_cache.json"),
    )

    log.info("=== TRANSFORM COMPLETO ===")


if __name__ == "__main__":
    main()
