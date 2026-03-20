"""
EstateMind Dashboard — Django views.

All data comes from Pinecone (vector DB) instead of PostgreSQL.
The API response shape is identical to the old version so React needs no changes.
"""
import json
from collections import defaultdict
from datetime import datetime, timezone

from django.contrib.auth import authenticate, login, logout
from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.contrib.auth.decorators import login_required


def _get_pinecone_index():
    """Return a connected Pinecone index object."""
    import os
    from pinecone import Pinecone
    api_key = os.getenv("PINECONE_API_KEY")
    if not api_key:
        raise RuntimeError("PINECONE_API_KEY not set")
    pc = Pinecone(api_key=api_key)
    index_name = os.getenv("PINECONE_INDEX_NAME", "property-listings")
    return pc.Index(index_name)


def _fetch_all_metadata(index, top_k: int = 10000):
    """
    Fetch metadata for all vectors using a broad dummy query.
    Pinecone free tier supports up to 10k results per query.
    We use a zero vector to fetch as many records as possible.
    """
    try:
        stats = index.describe_index_stats()
        dimension = stats.dimension
        total = stats.total_vector_count

        if total == 0:
            return [], 0

        # Query with zero vector to get metadata — no real semantic meaning,
        # just used to retrieve all metadata records
        zero_vec = [0.0] * dimension
        result = index.query(
            vector=zero_vec,
            top_k=min(top_k, total),
            include_metadata=True,
            include_values=False,
        )
        records = [m.metadata for m in result.matches if m.metadata]
        return records, total
    except Exception as e:
        return [], 0


@login_required
def dashboard(request):
    """Legacy HTML dashboard — kept for backwards compatibility."""
    return render(request, "dashboard.html", {
        "total_listings": 0,
        "total_by_source": [],
        "latest_runs": [],
    })


@login_required
def metrics_api(request):
    """
    /api/metrics/ — powers MetricsGrid and DashboardTabs in React.
    Returns total_listings, latest_run, per_source, recent_runs.
    """
    try:
        index = _get_pinecone_index()
        records, total = _fetch_all_metadata(index)
    except Exception as e:
        return JsonResponse({
            "total_listings": 0,
            "latest_run": _empty_run(),
            "per_source": [],
            "recent_runs": [],
            "error": str(e),
        })

    # Per-source counts
    source_counts = defaultdict(int)
    latest_scraped = {}

    for rec in records:
        src = rec.get("source_name", "unknown")
        source_counts[src] += 1
        scraped_at = rec.get("scraped_at", "")
        if scraped_at:
            if src not in latest_scraped or scraped_at > latest_scraped[src]:
                latest_scraped[src] = scraped_at

    per_source = sorted(
        [{"source_name": k, "count": v} for k, v in source_counts.items()],
        key=lambda x: -x["count"],
    )

    # Build recent_runs from per-source latest scrape times
    recent_runs = [
        {
            "source_name": src,
            "fetched": count,
            "inserted": count,
            "updated": 0,
            "unchanged": 0,
            "errors": 0,
            "started_at": latest_scraped.get(src),
        }
        for src, count in sorted(source_counts.items(),
                                  key=lambda x: latest_scraped.get(x[0], ""),
                                  reverse=True)
    ][:10]

    # Latest run = most recently scraped source
    latest_src = recent_runs[0] if recent_runs else None

    return JsonResponse({
        "total_listings": total,
        "latest_run": {
            "source_name":  latest_src["source_name"] if latest_src else None,
            "strategy":     "BALANCED",
            "fetched":      latest_src["fetched"] if latest_src else None,
            "inserted":     latest_src["inserted"] if latest_src else None,
            "updated":      0,
            "unchanged":    0,
            "errors":       0,
            "started_at":   latest_src["started_at"] if latest_src else None,
            "finished_at":  latest_src["started_at"] if latest_src else None,
        },
        "per_source":   per_source,
        "recent_runs":  recent_runs,
    })


@login_required
def eda_metrics(request):
    """
    /api/eda/ — powers EDADashboard charts in React.
    Computes all stats from Pinecone metadata in Python.
    """
    try:
        index = _get_pinecone_index()
        records, total = _fetch_all_metadata(index)
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)

    if not records:
        return JsonResponse(_empty_eda())

    # 1. Region stats
    region_counts = defaultdict(int)
    # 2. Price stats per region
    region_prices = defaultdict(list)
    # 3. Transaction type
    transaction_counts = defaultdict(int)
    # 4. Property type
    type_counts = defaultdict(int)
    # 5. City counts
    city_counts = defaultdict(int)
    # 6. Trend (by date)
    date_counts = defaultdict(int)
    # 7. Price/m2
    region_price_m2 = defaultdict(list)

    for rec in records:
        region = rec.get("region") or "Unknown"
        price  = rec.get("price")
        surface = rec.get("surface")
        tx_type = rec.get("transaction_type") or "Unknown"
        prop_type = rec.get("type") or "Unknown"
        city = rec.get("city") or rec.get("municipalite") or "Unknown"
        scraped_at = rec.get("scraped_at", "")

        region_counts[region] += 1

        if price and price > 0:
            region_prices[region].append(price)

        transaction_counts[tx_type] += 1
        type_counts[prop_type] += 1

        if city and city != "Unknown":
            city_counts[city] += 1

        if scraped_at:
            try:
                date_str = scraped_at[:10]  # "YYYY-MM-DD"
                date_counts[date_str] += 1
            except Exception:
                pass

        if price and price > 0 and surface and surface > 0:
            region_price_m2[region].append(price / surface)

    # Build response structures
    region_stats = sorted(
        [{"region": k, "count": v} for k, v in region_counts.items()],
        key=lambda x: -x["count"],
    )

    price_stats = []
    for region, prices in region_prices.items():
        if prices:
            price_stats.append({
                "region": region,
                "min_price": min(prices),
                "max_price": max(prices),
                "avg_price": sum(prices) / len(prices),
            })
    price_stats.sort(key=lambda x: x["region"])

    transaction_stats = sorted(
        [{"transaction_type": k, "count": v} for k, v in transaction_counts.items()],
        key=lambda x: -x["count"],
    )

    property_type_stats = sorted(
        [{"type": k, "count": v} for k, v in type_counts.items()],
        key=lambda x: -x["count"],
    )

    top_areas = sorted(
        [{"city": k, "count": v} for k, v in city_counts.items()],
        key=lambda x: -x["count"],
    )[:10]

    trend_stats = sorted(
        [{"date": k, "count": v} for k, v in date_counts.items()],
        key=lambda x: x["date"],
    )

    price_m2_stats = sorted(
        [
            {"region": region, "avg_m2": sum(vals) / len(vals)}
            for region, vals in region_price_m2.items()
            if vals
        ],
        key=lambda x: -x["avg_m2"],
    )

    return JsonResponse({
        "region_stats":        region_stats,
        "price_stats":         price_stats,
        "transaction_stats":   transaction_stats,
        "property_type_stats": property_type_stats,
        "top_areas":           top_areas,
        "trend_stats":         trend_stats,
        "price_m2_stats":      price_m2_stats,
    })


# ── Auth endpoints ────────────────────────────────────────────────────────────

@csrf_exempt
def api_login(request):
    if request.method != "POST":
        return JsonResponse({"detail": "Method not allowed"}, status=405)
    try:
        payload = json.loads(request.body.decode("utf-8"))
    except Exception:
        return JsonResponse({"detail": "Invalid JSON"}, status=400)
    username = payload.get("username")
    password = payload.get("password")
    if not username or not password:
        return JsonResponse({"detail": "Username and password required"}, status=400)
    user = authenticate(request, username=username, password=password)
    if user is None:
        return JsonResponse({"detail": "Invalid credentials"}, status=400)
    login(request, user)
    return JsonResponse({"detail": "ok"})


@csrf_exempt
def api_logout(request):
    if request.method != "POST":
        return JsonResponse({"detail": "Method not allowed"}, status=405)
    logout(request)
    return JsonResponse({"detail": "ok"})


@login_required
def api_session(request):
    user = request.user
    return JsonResponse({
        "is_authenticated": user.is_authenticated,
        "username":         user.username,
        "is_superuser":     user.is_superuser,
        "last_login":       user.last_login.isoformat() if user.last_login else None,
    })


# ── Helpers ───────────────────────────────────────────────────────────────────

def _empty_run():
    return {
        "source_name": None, "strategy": None, "fetched": None,
        "inserted": None, "updated": None, "unchanged": None,
        "errors": None, "started_at": None, "finished_at": None,
    }

def _empty_eda():
    return {
        "region_stats": [], "price_stats": [], "transaction_stats": [],
        "property_type_stats": [], "top_areas": [], "trend_stats": [],
        "price_m2_stats": [],
    }