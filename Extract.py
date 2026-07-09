"""
Bosch Dishwasher Scraper
========================
ETL — fase EXTRACT: salva highlights_raw grezzo e recensioni.
La trasformazione (has_* flags) è delegata a transform.py.
"""

import requests
import json
import uuid
import time
import re
import csv
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Configurazione
# ---------------------------------------------------------------------------

session = requests.Session()
session.headers.update({
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "accept": "*/*",
    "content-type": "application/json",
    "apollographql-client-name": "product-app-browser",
    "apollographql-client-version": "product-app-2026-06-23t06.56z__73364b2",
    "x-operation": "reviews",
    "referer": "https://www.bosch-home.com/it/it/mkt-category/lavastoviglie",
})

GRAPHQL_URL   = "https://www.bosch-home.com/graphql"
GRAPHQL_HASH  = "fd99ca6531bee549f397ad6a2c5652c779fb515bee7816f5c6b3d5233f90941d"
BASE_URL      = "https://www.bosch-home.com"
DELAY         = 1.5
REVIEWS_LIMIT = 50

REVIEW_LOCALES = [
    "it_IT", "it_CH",
    "es_ES",
    "fr_FR", "fr_BE",
    "de_DE", "de_AT", "de_CH",
    "da_DK",
    "el_GR",
    "en_GB", "en_IE",
    "pl_PL",
    "hr_HR",
    "sr_RS",
    "pt_PT", "pt_BR",
    "cs_CZ",
    "sk_SK",
    "nl_NL", "nl_BE",
    "ro_RO",
]

SITEMAP_URL = "https://www.bosch-home.com/it/sitemap.xml"

PRODUCT_PATTERN = re.compile(
    r'https://www\.bosch-home\.com(/it/it/mkt-product/lavastoviglie/[^<\s"]+/([A-Z0-9]{8,14}))',
    re.IGNORECASE
)


# ---------------------------------------------------------------------------
# FASE 1 – raccolta URL prodotti
# ---------------------------------------------------------------------------

def get_all_product_urls():
    print("=== FASE 1: raccolta product URL ===")
    resp = session.get(SITEMAP_URL, timeout=20)
    resp.raise_for_status()

    product_urls = {}
    for m in PRODUCT_PATTERN.finditer(resp.text):
        product_urls[m.group(2)] = BASE_URL + m.group(1)

    print(f"  Trovati {len(product_urls)} prodotti")
    return list(product_urls.items())


# ---------------------------------------------------------------------------
# FASE 2a – scraping pagina prodotto
# ---------------------------------------------------------------------------

def scrape_product_page(model_code, product_url):
    """
    Estrae i dati grezzi della pagina prodotto.
    highlights_raw contiene i claim dal payload JSON di Next.js.
    """
    resp = session.get(product_url, timeout=15)
    soup = BeautifulSoup(resp.text, "html.parser")

    # Discontinued
    page_text = soup.get_text(separator=" ", strip=True)
    is_discontinued = bool(re.search(
        r'non\s+[èe]\s+pi[ùu]\s+disponibil|prodotto.*non.*disponibil',
        page_text, re.I
    ))

    # Nome
    h1   = soup.find("h1")
    name = h1.get_text(strip=True) if h1 else ""
    name = re.sub(r'\d+[\.,]\d+\s*\(\d+\)', '', name).strip()

    # Serie
    serie_m = re.search(r'Serie\s+(\d+)', name, re.I)
    serie   = serie_m.group(0) if serie_m else ""

    # Tipo installazione
    if re.search(r'libera\s+installazione|freestanding', name, re.I):
        install_type = "freestanding"
    elif re.search(r'incasso|built.in|integrat|scomparsa', name, re.I):
        install_type = "built-in"
    else:
        install_type = ""

    # Classe energetica
    energy_class = ""
    em = re.search(r'ENERGY_CLASS_ICON_\d+_([A-G])\.', resp.text)
    if em:
        energy_class = em.group(1)
    else:
        for img in soup.find_all("img", alt=True):
            m = re.search(r'[Ee]tichetta\s+energetica\s+([A-G])\b', img.get("alt", ""))
            if m:
                energy_class = m.group(1)
                break

    # Rating aggregato
    rating_m     = re.search(r'(\d+[\.,]\d+)\s*\((\d+)\)', resp.text)
    avg_rating   = float(rating_m.group(1).replace(',', '.')) if rating_m else None
    review_count = int(rating_m.group(2)) if rating_m else None

    # Claim dal payload JSON di Next.js (self.__next_f)
    claims = []
    raw_chunks = re.findall(
        r'self\.__next_f\.push\(\[1,"((?:[^"\\]|\\.)*)"\]\)',
        resp.text
    )
    for chunk in raw_chunks:
        if model_code not in chunk or 'highlights' not in chunk:
            continue
        decoded = json.loads('"' + chunk + '"')
        if '"highlights":[' not in decoded:
            continue
        hl_start = decoded.find('"highlights":[') + len('"highlights":[')
        depth, pos = 1, hl_start
        while pos < len(decoded) and depth > 0:
            c = decoded[pos]
            if c == '[':   depth += 1
            elif c == ']': depth -= 1
            pos += 1
        hl_section = decoded[hl_start:pos - 1]
        hl_texts = re.findall(
            r'"headline":\s*\{"footnoteDataArray":\s*\[.*?\]\s*,\s*"text":\s*"([^"]+)"',
            hl_section, re.DOTALL
        )
        seen = set()
        for t in hl_texts:
            t = t.strip()
            if t and len(t) > 20 and t not in seen:
                claims.append(t)
                seen.add(t)
        if claims:
            break

    return {
        "model_code":      model_code,
        "name":            name,
        "serie":           serie,
        "install_type":    install_type,
        "energy_class":    energy_class,
        "avg_rating":      avg_rating,
        "review_count":    review_count,
        "is_discontinued": is_discontinued,
        "highlights_raw":  " | ".join(claims),
        "url":             product_url,
    }


# ---------------------------------------------------------------------------
# FASE 2b – recensioni GraphQL
# ---------------------------------------------------------------------------

def get_reviews_page(product_id, page=0, limit=REVIEWS_LIMIT):
    variables = {
        "productId":              product_id,
        "locales":                ",".join(REVIEW_LOCALES),
        "reviewsLimit":           limit,
        "pageNumber":             page,
        "sortOrder":              "BEST",
        "filters":                [],
        "ratings":                [],
        "includeRetailerReviews": True,
    }
    extensions = {
        "persistedQuery": {"version": 1, "sha256Hash": GRAPHQL_HASH},
        "shop": {"brand": "BOSCH", "country": "IT", "language": "it",
                 "channel": "B2C", "cas": True},
    }
    resp = session.get(
        GRAPHQL_URL,
        params={
            "operationName": "reviews",
            "variables":     json.dumps(variables),
            "extensions":    json.dumps(extensions),
        },
        headers={"x-flow-id": str(uuid.uuid4())},
        timeout=15,
    )
    data = resp.json()
    return data.get("data", {}).get("reviews", {}).get("reviews", [])


def get_all_reviews(product_id):
    all_reviews = []
    page = 0
    while True:
        reviews = get_reviews_page(product_id, page=page)
        if not reviews:
            break
        all_reviews.extend(reviews)
        print(f"    pag {page}: {len(reviews)} rec (tot {len(all_reviews)})")
        if len(reviews) < REVIEWS_LIMIT:
            break
        page += 1
        time.sleep(DELAY)
    return all_reviews


def flatten_review(review, queried_product_id):
    secondary = {r["category"]: r["value"] for r in review.get("secondaryRating", [])}
    badge_ids = [b["id"] for b in review.get("badges", [])]
    context   = {c["category"]: c["value"] for c in review.get("context", [])}
    return {
        "queried_product_id": queried_product_id,
        "review_product_id":  review.get("productId"),
        "review_id":          review.get("id"),
        "locale":             review.get("locale"),
        "date":               review.get("date"),
        "rating":             review.get("rating"),
        "title":              review.get("title", "").replace("\n", " ").replace("\r", " "),
        "text":               review.get("text",  "").replace("\n", " ").replace("\r", " "),
        "user":               review.get("user"),
        "recommendation":     review.get("recommendation"),
        "is_incentivized":    "incentivizedReview" in badge_ids,
        "is_verified":        "verifiedPurchaser"  in badge_ids,
        "upvotes":            review.get("upVotes", 0),
        "downvotes":          review.get("downVotes", 0),
        "rating_programs":    secondary.get("Varietà di programmi"),
        "rating_loading":     secondary.get("Comfort di carico"),
        "rating_washing":     secondary.get("Risultati di lavaggio ed asciugatura"),
        "rating_noise":       secondary.get("Rumorosità"),
        "age":                context.get("Età"),
        "household":          context.get("Descrivi te stesso/a:"),
        "usage_frequency":    context.get("Quanto spesso usi questo prodotto?"),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    products_info = get_all_product_urls()

    print(f"\n=== FASE 2: scraping {len(products_info)} prodotti ===")

    products_rows = []
    reviews_rows  = []

    for i, (model_code, product_url) in enumerate(products_info):
        print(f"\n[{i+1}/{len(products_info)}] {model_code}")

        print("  scraping pagina prodotto...")
        row = scrape_product_page(model_code, product_url)
        products_rows.append(row)
        time.sleep(DELAY)

        if (row.get("review_count") or 0) > 0:
            print("  downloading recensioni...")
            reviews = get_all_reviews(model_code)
            for r in reviews:
                reviews_rows.append(flatten_review(r, model_code))
            print(f"  scaricate: {len(reviews)}")
        else:
            print("  nessuna recensione.")
        time.sleep(DELAY)

    print("\n=== Salvataggio CSV ===")

    all_keys = list(dict.fromkeys(k for row in products_rows for k in row))
    with open("products.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(products_rows)
    print(f"  products.csv: {len(products_rows)} righe")

    if reviews_rows:
        with open("reviews.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(reviews_rows[0].keys()))
            writer.writeheader()
            writer.writerows(reviews_rows)
        print(f"  reviews.csv: {len(reviews_rows)} righe")


if __name__ == "__main__":
    main()
