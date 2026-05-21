import sys
import httpx

from deerflow.routing.es_store import SkillRouterElasticStore
from deerflow.routing.embedding_client import SkillRouterEmbeddingClient
from deerflow.routing.reranker_client import SkillRouterRerankerClient


def check_http_service(name: str, base_url: str) -> None:
    try:
        r = httpx.get(f"{base_url}/models", timeout=5)
        if r.status_code < 400:
            print(f"{name}: OK ({base_url}/models)")
            return
    except Exception:
        pass

    health_url = base_url.rstrip("/").removesuffix("/v1") + "/health"
    r = httpx.get(health_url, timeout=5)
    if r.status_code >= 400:
        raise RuntimeError(f"{name} failed: {health_url} status={r.status_code}, body={r.text[:200]}")

    print(f"{name}: OK ({health_url})")


def main() -> None:
    es = SkillRouterElasticStore()
    auth = (es.username, es.password) if es.username else None

    r = httpx.head(f"{es.es_url}/{es.index}", auth=auth, timeout=5)
    if r.status_code >= 400:
        raise RuntimeError(f"ES index not found or inaccessible: {es.index}, status={r.status_code}")

    r2 = httpx.get(f"{es.es_url}/{es.index}/_count", auth=auth, timeout=5)
    r2.raise_for_status()
    count = r2.json().get("count", 0)

    print(f"ES connected: {es.index} has {count} documents")

    if count == 0:
        raise RuntimeError("No skills indexed. Run: make build-skill-router-index")

    emb = SkillRouterEmbeddingClient()
    check_http_service("Embedding service", emb.base_url)

    rer = SkillRouterRerankerClient()
    check_http_service("Reranker service", rer.base_url)

    print("All SkillRouter services healthy.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"SkillRouter health check FAILED: {exc}")
        sys.exit(1)
