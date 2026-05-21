#!/usr/bin/env python3
"""Multimodal Top-k retrieval from Elasticsearch.

Supports:
- Text description → embed via Qwen3-VL-Embedding-2B, vector field: vector-{model_name}-{instruction_key}
- Image path       → embed via ImAge4VPR,              vector field: vector-ImAge4VPR
- Geo-distance filter (center lat/lon + radius in meters)
- RRF fusion when both description and image are provided (requires commercial ES license)
- min-description-score / min-image-similarity thresholds

ES Configuration:
- Index: street
- Vector fields:
  - vector-ImAge4VPR (4096 dims, cosine similarity, index=true, BBQ HNSW)
  - vector-Qwen3-VL-Embedding-2B_urban_governance (2048 dims, cosine similarity, index=true, BBQ HNSW)
- Metadata fields:
  - metadata.latitude (double)
  - metadata.longitude (double)
  - metadata.utm_easting (double)
  - metadata.utm_northing (double)

Services:
- Elasticsearch: http://localhost:3128
- Model Service: http://localhost:3130

Note: RRF (Reciprocal Rank Fusion) requires a commercial Elasticsearch license.
      If RRF is not available, use separate text and image queries instead.
"""

from __future__ import annotations

import argparse
import json
import os
from typing import Any

from elasticsearch import Elasticsearch

import street_server

DEFAULT_TOP_K = 3
DEFAULT_INDEX = "street"
DEFAULT_TEXT_MODEL = "Qwen3-VL-Embedding-2B"
DEFAULT_IMAGE_MODEL = "ImAge4VPR"


# ---------------------------------------------------------------------------
# ES client
# ---------------------------------------------------------------------------


def build_es_client(args: argparse.Namespace) -> Elasticsearch:
    hosts = args.es_url or os.getenv("ES_URL", "http://localhost:3128")
    api_key = args.es_api_key or os.getenv("ES_API_KEY")
    username = args.es_username or os.getenv("ES_USERNAME")
    password = args.es_password or os.getenv("ES_PASSWORD")

    if api_key:
        return Elasticsearch(hosts=hosts, api_key=api_key)
    if username and password:
        return Elasticsearch(hosts=hosts, basic_auth=(username, password))
    return Elasticsearch(hosts=hosts)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def deep_get(data: dict[str, Any], path: str, default: Any = None) -> Any:
    current: Any = data
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return default
        current = current[part]
    return current


def parse_target_fields(raw: str) -> list[str]:
    fields = [f.strip() for f in raw.split(",") if f.strip()]
    if not fields:
        raise ValueError("--target-field cannot be empty")
    return fields


def haversine_meters(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance between two points using Haversine formula."""
    import math
    
    R = 6371000  # Earth's radius in meters
    
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    
    a = math.sin(delta_phi / 2) ** 2 + \
        math.cos(phi1) * math.cos(phi2) * \
        math.sin(delta_lambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    return R * c


# ---------------------------------------------------------------------------
# Geo filter
# ---------------------------------------------------------------------------


def make_geo_filter(args: argparse.Namespace) -> list[dict[str, Any]]:
    has_lat = args.center_latitude is not None
    has_lon = args.center_longitude is not None
    has_dist = args.max_distance is not None

    if any([has_lat, has_lon, has_dist]) and not all([has_lat, has_lon, has_dist]):
        raise ValueError(
            "Geo filter requires --center-latitude, --center-longitude and --max-distance together."
        )

    if not all([has_lat, has_lon, has_dist]):
        return []

    return [
        {
            "script": {
                "script": {
                    "lang": "painless",
                    "source": (
                        "if (doc[params.lat_field].size() == 0 || doc[params.lon_field].size() == 0) return false; "
                        "double lat = doc[params.lat_field].value; "
                        "double lon = doc[params.lon_field].value; "
                        "double dLat = Math.toRadians(lat - params.center_lat); "
                        "double dLon = Math.toRadians(lon - params.center_lon); "
                        "double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) + "
                        "Math.cos(Math.toRadians(params.center_lat)) * Math.cos(Math.toRadians(lat)) * "
                        "Math.sin(dLon / 2) * Math.sin(dLon / 2); "
                        "double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)); "
                        "double dist = 6371000.0 * c; "
                        "return dist <= params.max_distance;"
                    ),
                    "params": {
                        "lat_field": "metadata.latitude",
                        "lon_field": "metadata.longitude",
                        "center_lat": args.center_latitude,
                        "center_lon": args.center_longitude,
                        "max_distance": args.max_distance,
                    },
                }
            }
        }
    ]


# ---------------------------------------------------------------------------
# Source fields
# ---------------------------------------------------------------------------


def build_source_fields(target_fields: list[str]) -> list[str]:
    return list(target_fields)


# ---------------------------------------------------------------------------
# Query body builder
# ---------------------------------------------------------------------------


def build_query_body(
    args: argparse.Namespace,
    geo_filters: list[dict[str, Any]],
    target_fields: list[str],
    text_vector: list[float] | None,
    image_vector: list[float] | None,
) -> dict[str, Any]:
    has_text = text_vector is not None
    has_image = image_vector is not None

    # Vector field names follow the naming convention in the index:
    #   text  → vector-{model_name}-{instruction_key}
    #   image → vector-{model_name}
    text_model = args.text_model or DEFAULT_TEXT_MODEL
    image_model = args.image_model or DEFAULT_IMAGE_MODEL

    if has_text:
        if args.instruction_key:
            text_vector_field = f"vector-{text_model}_{args.instruction_key}"
        else:
            text_vector_field = f"vector-{text_model}"
    if has_image:
        image_vector_field = f"vector-{image_model}"

    source_fields = build_source_fields(target_fields)
    k = args.k

    # ---- RRF (text + image) ----
    if has_text and has_image:
        text_retriever: dict[str, Any] = {
            "standard": {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "knn": {
                                    "field": text_vector_field,
                                    "query_vector": text_vector,
                                    "k": max(k * 10, k),
                                    "num_candidates": max(k * 20, 100),
                                    "filter": geo_filters,
                                }
                            }
                        ],
                        "filter": geo_filters,
                    }
                }
            }
        }
        if args.min_description_score is not None:
            text_retriever["standard"]["min_score"] = args.min_description_score

        image_retriever: dict[str, Any] = {
            "knn": {
                "field": image_vector_field,
                "query_vector": image_vector,
                "k": max(k * 10, k),
                "num_candidates": max(k * 20, 100),
                "filter": geo_filters,
            }
        }
        if args.min_image_similarity is not None:
            image_retriever["knn"]["similarity"] = args.min_image_similarity

        return {
            "size": k,
            "_source": source_fields,
            "retriever": {
                "rrf": {
                    "rank_window_size": max(k * 10, 50),
                    "rank_constant": 60,
                    "retrievers": [text_retriever, image_retriever],
                }
            },
        }

    # ---- Text only ----
    if has_text:
        body: dict[str, Any] = {
            "size": k,
            "_source": source_fields,
            "knn": {
                "field": text_vector_field,
                "query_vector": text_vector,
                "k": k,
                "num_candidates": max(k * 20, 100),
                "filter": geo_filters,
            },
        }
        if args.min_description_score is not None:
            body["knn"]["similarity"] = args.min_description_score
        return body

    # ---- Image only ----
    if has_image:
        body = {
            "size": k,
            "_source": source_fields,
            "knn": {
                "field": image_vector_field,
                "query_vector": image_vector,
                "k": k,
                "num_candidates": max(k * 20, 100),
                "filter": geo_filters,
            },
        }
        if args.min_image_similarity is not None:
            body["knn"]["similarity"] = args.min_image_similarity
        return body

    # ---- No query vectors: match-all with geo filter ----
    return {
        "size": k,
        "_source": source_fields,
        "query": {
            "bool": {
                "must": [{"match_all": {}}],
                "filter": geo_filters,
            }
        },
    }


# ---------------------------------------------------------------------------
# Result formatting
# ---------------------------------------------------------------------------


def format_results(
    hits: list[dict[str, Any]],
    args: argparse.Namespace,
    target_fields: list[str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for rank, hit in enumerate(hits, start=1):
        source = hit.get("_source", {})
        targets = {field: deep_get(source, field) for field in target_fields}
        row: dict[str, Any] = {"rank": rank, "es_score": hit.get("_score")}
        if len(target_fields) == 1:
            row["target"] = targets[target_fields[0]]
        else:
            row["targets"] = targets
        rows.append(row)
    return rows


def summarize(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "没有检索到匹配结果。"
    best = rows[0]
    best_target = best.get("target") or str(best.get("targets", ""))
    if len(rows) > 1:
        backup_target = rows[1].get("target") or str(rows[1].get("targets", ""))
        return f"Top-1: {best_target}; Top-2: {backup_target}"
    return f"Top-1: {best_target}"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Multimodal Top-k retrieval from Elasticsearch (text + image + geo)"
    )
    parser.add_argument("--index", default=DEFAULT_INDEX, help="Elasticsearch index name")
    parser.add_argument(
        "--target-field",
        required=True,
        help="Field(s) to return as target; comma-separated for multiple",
    )
    parser.add_argument("--k", type=int, default=DEFAULT_TOP_K, help="Top-k results")

    parser.add_argument("--description", default=None, help="Text description for semantic retrieval")
    parser.add_argument("--image-path", default=None, help="Server-local image path for image retrieval")
    parser.add_argument(
        "--instruction-key",
        default="urban_governance",
        help="Instruction key for text embedding (e.g. urban_governance). Also used to determine the vector field name.",
    )
    parser.add_argument(
        "--text-model",
        default=DEFAULT_TEXT_MODEL,
        help=f"Model name for text embedding (default: {DEFAULT_TEXT_MODEL})",
    )
    parser.add_argument(
        "--image-model",
        default=DEFAULT_IMAGE_MODEL,
        help=f"Model name for image embedding (default: {DEFAULT_IMAGE_MODEL})",
    )

    parser.add_argument(
        "--min-description-score",
        type=float,
        default=0.6,
        help="Minimum similarity threshold for text/description kNN retrieval",
    )
    parser.add_argument(
        "--min-image-similarity",
        type=float,
        default=0.6,
        help="Minimum similarity threshold for image kNN retrieval",
    )

    parser.add_argument("--center-latitude", type=float, default=None)
    parser.add_argument("--center-longitude", type=float, default=None)
    parser.add_argument(
        "--max-distance",
        type=float,
        default=None,
        help="Max geo distance in meters",
    )

    parser.add_argument("--es-url", default=None, help="Elasticsearch URL")
    parser.add_argument("--es-username", default=None, help="Basic auth username")
    parser.add_argument("--es-password", default=None, help="Basic auth password")
    parser.add_argument("--es-api-key", default=None, help="API key")

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    es = build_es_client(args)
    target_fields = parse_target_fields(args.target_field)
    geo_filters = make_geo_filter(args)

    # Embed description text if provided
    text_vector: list[float] | None = None
    if args.description and args.description.strip():
        embeddings = street_server.embed_text(
            texts=[args.description.strip()],
            model_name=args.text_model or DEFAULT_TEXT_MODEL,
            instruction_key=args.instruction_key,
        )
        text_vector = embeddings[0]

    # Embed image if provided
    image_vector: list[float] | None = None
    if args.image_path:
        embeddings = street_server.embed_image(
            image_path=args.image_path,
            model_name=args.image_model or DEFAULT_IMAGE_MODEL,
        )
        image_vector = embeddings[0]
        # Truncate vectors if they exceed 4096 dimensions, as some ES setups may have limits on vector field dimensions
        if len(image_vector) > 4096:
            image_vector = image_vector[:4096]

    body = build_query_body(args, geo_filters, target_fields, text_vector, image_vector)
    response = es.search(index=args.index, body=body)
    hits = response.get("hits", {}).get("hits", [])
    rows = format_results(hits, args, target_fields)

    result = {
        "index": args.index,
        "query_info": {
            "target_fields": target_fields,
            "k": args.k,
            "description_provided": bool(args.description),
            "image_provided": bool(args.image_path),
            "instruction_key": args.instruction_key,
            "min_description_score": args.min_description_score,
            "min_image_similarity": args.min_image_similarity,
            "geo_filter": {
                "center_latitude": args.center_latitude,
                "center_longitude": args.center_longitude,
                "max_distance_m": args.max_distance,
            } if geo_filters else None,
        },
        "top_k": rows,
        "conclusion": summarize(rows),
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
