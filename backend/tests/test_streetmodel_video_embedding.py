from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import sys
import tempfile
import unittest
import urllib.error
from io import BytesIO
from pathlib import Path
from typing import Any
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
VIDEO_EMBED_SCRIPT = ROOT / "skills/custom/video-embedding-index/scripts/run.py"
VIDEO_SEARCH_SCRIPT = ROOT / "skills/custom/video-search/scripts/run.py"
VECTOR_FIELD = "video_vector-Qwen3-VL-Embedding-2B_urban_governance"


def load_script(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class FakeResponse:
    def __init__(self, status: int = 200, payload: Any | None = None):
        self.status = status
        self.payload = payload if payload is not None else {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self) -> bytes:
        return json.dumps(self.payload, ensure_ascii=False).encode("utf-8")


def http_error(code: int, payload: Any | None = None) -> urllib.error.HTTPError:
    data = json.dumps(payload if payload is not None else {}, ensure_ascii=False).encode("utf-8")
    return urllib.error.HTTPError("http://example.test", code, "error", {}, BytesIO(data))


class StreetModelVideoEmbeddingTests(unittest.TestCase):
    def test_streetmodel_path_mapping_rules(self):
        module = load_script(VIDEO_EMBED_SCRIPT, "video_embedding_index_path_test")

        self.assertEqual(
            module.to_streetmodel_video_uri(
                "/data/deerflow/videos/camera01/0001.mp4",
                "/data/deerflow/videos",
                "/nfsdat2/home/xhuangslm/shared_videos",
            ),
            "/nfsdat2/home/xhuangslm/shared_videos/camera01/0001.mp4",
        )
        self.assertEqual(
            module.to_streetmodel_video_uri(
                "/nfsdat2/home/xhuangslm/shared_videos/camera01/0001.mp4",
                "/data/deerflow/videos",
                "/nfsdat2/home/xhuangslm/shared_videos",
            ),
            "/nfsdat2/home/xhuangslm/shared_videos/camera01/0001.mp4",
        )
        with self.assertRaises(module.EmbeddingError) as ctx:
            module.to_streetmodel_video_uri("/tmp/local.mp4", "/data/deerflow/videos", "/nfsdat2/home/xhuangslm/shared_videos")
        self.assertEqual(ctx.exception.code, "VIDEO_URI_NOT_MAPPABLE")

    def test_streetmodel_video_embedding_writes_personal_vector_index(self):
        module = load_script(VIDEO_EMBED_SCRIPT, "video_embedding_index_main_test")
        vector = [0.25] * 2048
        streetmodel_payloads: list[dict[str, Any]] = []
        upserts: list[tuple[str, dict[str, Any]]] = []

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            config_path = tmp_path / "config.json"
            config_path.write_text(
                json.dumps(
                    {
                        "streetmodel_embedding": {
                            "base_url": "http://streetmodel.test",
                            "model_name": "Qwen3-VL-Embedding-2B",
                            "vector_field": VECTOR_FIELD,
                            "dimensions": 2048,
                            "batch_size": 1,
                            "timeout_seconds": 600,
                            "video": {
                                "deerflow_path_prefix": "/data/deerflow/videos",
                                "streetmodel_path_prefix": "/nfsdat2/home/xhuangslm/shared_videos",
                            },
                        },
                        "elasticsearch": {"hosts": ["http://es.test"]},
                    }
                ),
                encoding="utf-8",
            )
            output_path = tmp_path / "result.json"

            def fake_urlopen(req, timeout=0):
                url = req.full_url
                method = req.get_method()
                body = json.loads(req.data.decode("utf-8")) if req.data else None
                if url == "http://streetmodel.test/health":
                    return FakeResponse(payload={"status": "ok"})
                if url == "http://streetmodel.test/models":
                    return FakeResponse(payload={"models": [{"name": "Qwen3-VL-Embedding-2B", "kind": "embedding", "exists": True, "loaded": False}]})
                if url == "http://streetmodel.test/embed":
                    streetmodel_payloads.append(body)
                    return FakeResponse(payload={"model_name": "Qwen3-VL-Embedding-2B", "embeddings": [vector], "shape": [1, 2048]})
                if url == "http://es.test/huangxiao-video-library-vector-v1" and method == "HEAD":
                    raise http_error(404)
                if url == "http://es.test/huangxiao-video-library-vector-v1" and method == "PUT":
                    self.assertEqual(body["mappings"]["properties"][VECTOR_FIELD]["dims"], 2048)
                    return FakeResponse(payload={"acknowledged": True})
                if url == "http://es.test/citybrain-video-library/_search" and method == "POST":
                    self.assertIn(VECTOR_FIELD, body["_source"]["excludes"])
                    return FakeResponse(
                        payload={
                            "hits": {
                                "hits": [
                                    {
                                        "_source": {
                                            "video_id": "camera01_0001",
                                            "camera_id": "camera01",
                                            "file_path": "/data/deerflow/videos/camera01/0001.mp4",
                                            "content_text": "night traffic",
                                        }
                                    }
                                ]
                            }
                        }
                    )
                if url == "http://es.test/huangxiao-video-library-vector-v1/_doc/camera01_0001" and method == "PUT":
                    upserts.append((url, body))
                    return FakeResponse(payload={"result": "created"})
                if url == "http://es.test/huangxiao-video-library-vector-v1/_refresh" and method == "POST":
                    return FakeResponse(payload={"refreshed": True})
                raise AssertionError(f"Unexpected request: {method} {url}")

            argv = ["run.py", "--embedding-provider", "streetmodel", "--config", str(config_path), "--output", str(output_path)]
            with (
                mock.patch.object(module.urllib.request, "urlopen", side_effect=fake_urlopen),
                mock.patch.object(sys, "argv", argv),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                self.assertEqual(module.main(), 0)

            result = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(result["data"]["embedded_count"], 1)
            self.assertEqual(result["data"]["vector_field"], VECTOR_FIELD)
            self.assertEqual(streetmodel_payloads[0]["items"], [{"type": "video", "uri": "/nfsdat2/home/xhuangslm/shared_videos/camera01/0001.mp4"}])
            indexed_doc = upserts[0][1]
            self.assertEqual(indexed_doc[VECTOR_FIELD], vector)
            self.assertEqual(indexed_doc["embedding_provider"], "streetmodel")
            self.assertEqual(indexed_doc["embedding_model"], "Qwen3-VL-Embedding-2B")
            self.assertEqual(indexed_doc["video_embedding_uri"], "/nfsdat2/home/xhuangslm/shared_videos/camera01/0001.mp4")
            self.assertEqual(indexed_doc["video_embedding_dimensions"], 2048)

    def test_streetmodel_query_embedding_outputs_query_vector(self):
        module = load_script(VIDEO_EMBED_SCRIPT, "video_embedding_index_query_test")
        vector = [0.5] * 2048
        embed_payloads: list[dict[str, Any]] = []

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            query_vector_path = tmp_path / "query-vector.json"
            output_path = tmp_path / "result.json"

            def fake_urlopen(req, timeout=0):
                url = req.full_url
                body = json.loads(req.data.decode("utf-8")) if req.data else None
                if url.endswith("/health"):
                    return FakeResponse(payload={"status": "ok"})
                if url.endswith("/models"):
                    return FakeResponse(payload={"models": [{"name": "Qwen3-VL-Embedding-2B", "exists": True}]})
                if url.endswith("/embed"):
                    embed_payloads.append(body)
                    return FakeResponse(payload={"embeddings": [vector], "shape": [1, 2048]})
                raise AssertionError(f"Unexpected request: {url}")

            argv = [
                "run.py",
                "--embedding-provider",
                "streetmodel",
                "--base-url",
                "http://streetmodel.test",
                "--query",
                "夜晚路口有很多车辆经过",
                "--query-vector-output",
                str(query_vector_path),
                "--output",
                str(output_path),
            ]
            with (
                mock.patch.object(module.urllib.request, "urlopen", side_effect=fake_urlopen),
                mock.patch.object(sys, "argv", argv),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                self.assertEqual(module.main(), 0)

            payload = json.loads(query_vector_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["dimensions"], 2048)
            self.assertEqual(payload["vector"], vector)
            self.assertEqual(payload["vector_field"], VECTOR_FIELD)
            self.assertEqual(embed_payloads[0]["items"], [{"type": "text", "content": "夜晚路口有很多车辆经过"}])

    def test_explicit_video_uri_writes_vector_without_source_index_search(self):
        module = load_script(VIDEO_EMBED_SCRIPT, "video_embedding_index_explicit_video_test")
        vector = [0.75] * 2048
        upserts: list[dict[str, Any]] = []

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            config_path = tmp_path / "config.json"
            config_path.write_text(json.dumps({"elasticsearch": {"hosts": ["http://es.test"]}}), encoding="utf-8")
            vector_output = tmp_path / "video-vector.json"
            output_path = tmp_path / "result.json"

            def fake_urlopen(req, timeout=0):
                url = req.full_url
                method = req.get_method()
                body = json.loads(req.data.decode("utf-8")) if req.data else None
                self.assertNotIn("citybrain-video-library/_search", url)
                if url == "http://streetmodel.test/health":
                    return FakeResponse(payload={"status": "ok"})
                if url == "http://streetmodel.test/models":
                    return FakeResponse(payload={"models": [{"name": "Qwen3-VL-Embedding-2B", "exists": True}]})
                if url == "http://streetmodel.test/embed":
                    self.assertEqual(body["items"], [{"type": "video", "uri": "/nfsdat2/home/xhuangslm/shared_videos/camera01/0001.mp4"}])
                    return FakeResponse(payload={"embeddings": [vector], "shape": [1, 2048]})
                if url == "http://es.test/huangxiao-video-library-vector-v1" and method == "HEAD":
                    raise http_error(404)
                if url == "http://es.test/huangxiao-video-library-vector-v1" and method == "PUT":
                    return FakeResponse(payload={"acknowledged": True})
                if url == "http://es.test/huangxiao-video-library-vector-v1/_doc/direct-video-001" and method == "PUT":
                    upserts.append(body)
                    return FakeResponse(payload={"result": "created"})
                if url == "http://es.test/huangxiao-video-library-vector-v1/_refresh" and method == "POST":
                    return FakeResponse(payload={"refreshed": True})
                raise AssertionError(f"Unexpected request: {method} {url}")

            argv = [
                "run.py",
                "--embedding-provider",
                "streetmodel",
                "--base-url",
                "http://streetmodel.test",
                "--target-index",
                "huangxiao-video-library-vector-v1",
                "--video-id",
                "direct-video-001",
                "--video-uri",
                "/nfsdat2/home/xhuangslm/shared_videos/camera01/0001.mp4",
                "--video-vector-output",
                str(vector_output),
                "--config",
                str(config_path),
                "--output",
                str(output_path),
            ]
            with (
                mock.patch.object(module.urllib.request, "urlopen", side_effect=fake_urlopen),
                mock.patch.object(sys, "argv", argv),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                self.assertEqual(module.main(), 0)

            result = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(result["data"]["embedded_count"], 1)
            self.assertEqual(result["data"]["documents"][0]["video_id"], "direct-video-001")
            vector_payload = json.loads(vector_output.read_text(encoding="utf-8"))
            self.assertEqual(vector_payload["dimensions"], 2048)
            self.assertEqual(vector_payload["vector"], vector)
            self.assertEqual(upserts[0][VECTOR_FIELD], vector)
            self.assertEqual(upserts[0]["video_embedding_dimensions"], 2048)

    def test_explicit_local_video_can_be_copied_to_shared_prefix(self):
        module = load_script(VIDEO_EMBED_SCRIPT, "video_embedding_index_copy_video_test")
        vector = [0.8] * 2048
        embed_payloads: list[dict[str, Any]] = []
        upserts: list[dict[str, Any]] = []

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            local_video = tmp_path / "upload.mp4"
            local_video.write_bytes(b"fake-video")
            deerflow_prefix = tmp_path / "deerflow" / "videos"
            config_path = tmp_path / "config.json"
            config_path.write_text(json.dumps({"elasticsearch": {"hosts": ["http://es.test"]}}), encoding="utf-8")
            output_path = tmp_path / "result.json"

            def fake_urlopen(req, timeout=0):
                url = req.full_url
                method = req.get_method()
                body = json.loads(req.data.decode("utf-8")) if req.data else None
                if url == "http://streetmodel.test/health":
                    return FakeResponse(payload={"status": "ok"})
                if url == "http://streetmodel.test/models":
                    return FakeResponse(payload={"models": [{"name": "Qwen3-VL-Embedding-2B", "exists": True}]})
                if url == "http://streetmodel.test/embed":
                    embed_payloads.append(body)
                    self.assertTrue(body["items"][0]["uri"].startswith("/street/shared/direct_uploads/local-video-001-"))
                    return FakeResponse(payload={"embeddings": [vector], "shape": [1, 2048]})
                if url == "http://es.test/huangxiao-video-library-vector-v1" and method == "HEAD":
                    raise http_error(404)
                if url == "http://es.test/huangxiao-video-library-vector-v1" and method == "PUT":
                    return FakeResponse(payload={"acknowledged": True})
                if url == "http://es.test/huangxiao-video-library-vector-v1/_doc/local-video-001" and method == "PUT":
                    upserts.append(body)
                    return FakeResponse(payload={"result": "created"})
                if url == "http://es.test/huangxiao-video-library-vector-v1/_refresh" and method == "POST":
                    return FakeResponse(payload={"refreshed": True})
                raise AssertionError(f"Unexpected request: {method} {url}")

            argv = [
                "run.py",
                "--embedding-provider",
                "streetmodel",
                "--base-url",
                "http://streetmodel.test",
                "--target-index",
                "huangxiao-video-library-vector-v1",
                "--video-id",
                "local-video-001",
                "--video-uri",
                str(local_video),
                "--deerflow-path-prefix",
                str(deerflow_prefix),
                "--streetmodel-path-prefix",
                "/street/shared",
                "--copy-video-to-shared",
                "--config",
                str(config_path),
                "--output",
                str(output_path),
            ]
            with (
                mock.patch.object(module.urllib.request, "urlopen", side_effect=fake_urlopen),
                mock.patch.object(sys, "argv", argv),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                self.assertEqual(module.main(), 0)

            self.assertEqual(len(embed_payloads), 1)
            copied_path = Path(upserts[0]["deerflow_video_path"])
            self.assertTrue(copied_path.is_file())
            self.assertEqual(copied_path.read_bytes(), b"fake-video")
            self.assertTrue(upserts[0]["video_embedding_uri"].startswith("/street/shared/direct_uploads/local-video-001-"))

    def test_video_search_uses_configured_hyphenated_vector_field(self):
        module = load_script(VIDEO_SEARCH_SCRIPT, "video_search_vector_field_test")
        searches: list[dict[str, Any]] = []

        class FakeEsClient:
            def __init__(self, config):
                self.config = config

            def mapping_has_vector(self, index, vector_field="vector"):
                return vector_field == VECTOR_FIELD

            def search(self, index, body):
                searches.append(body)
                return {"hits": {"total": {"value": 0}, "hits": []}}

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            vector_path = tmp_path / "vector.json"
            vector_path.write_text(json.dumps({"vector": [0.1, 0.2, 0.3]}), encoding="utf-8")
            config_path = tmp_path / "config.json"
            config_path.write_text(json.dumps({"streetmodel_embedding": {"vector_field": VECTOR_FIELD}}), encoding="utf-8")
            output_path = tmp_path / "result.json"
            argv = [
                "run.py",
                "--index",
                "huangxiao-video-library-vector-v1",
                "--query",
                "night traffic",
                "--query-vector-json",
                str(vector_path),
                "--config",
                str(config_path),
                "--output",
                str(output_path),
            ]

            with (
                mock.patch.object(module, "EsClient", FakeEsClient),
                mock.patch.object(sys, "argv", argv),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                self.assertEqual(module.main(), 0)

            script = searches[0]["query"]["script_score"]["script"]["source"]
            self.assertIn(f"doc['{VECTOR_FIELD}'].size()", script)
            self.assertIn(f"cosineSimilarity(params.query_vector, '{VECTOR_FIELD}')", script)
            result = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(result["data"]["query_mode"], "vector_keyword_filter")
            self.assertEqual(result["data"]["vector_field"], VECTOR_FIELD)

    def test_video_search_can_embed_query_with_streetmodel(self):
        module = load_script(VIDEO_SEARCH_SCRIPT, "video_search_streetmodel_query_test")
        vector = [0.1, 0.2, 0.3]
        searches: list[dict[str, Any]] = []
        embed_payloads: list[dict[str, Any]] = []

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            config_path = tmp_path / "config.json"
            config_path.write_text(
                json.dumps(
                    {
                        "streetmodel_embedding": {"vector_field": VECTOR_FIELD},
                        "elasticsearch": {"hosts": ["http://es.test"]},
                    }
                ),
                encoding="utf-8",
            )
            output_path = tmp_path / "result.json"

            def fake_urlopen(req, timeout=0):
                url = req.full_url
                method = req.get_method()
                body = json.loads(req.data.decode("utf-8")) if req.data else None
                if url == "http://streetmodel.test/health":
                    return FakeResponse(payload={"status": "ok"})
                if url == "http://streetmodel.test/models":
                    return FakeResponse(payload={"models": [{"name": "Qwen3-VL-Embedding-2B", "exists": True}]})
                if url == "http://streetmodel.test/embed":
                    embed_payloads.append(body)
                    return FakeResponse(payload={"embeddings": [vector], "shape": [1, 3]})
                if url == "http://es.test/huangxiao-video-library-vector-v1/_mapping" and method == "GET":
                    return FakeResponse(
                        payload={
                            "huangxiao-video-library-vector-v1": {
                                "mappings": {"properties": {VECTOR_FIELD: {"type": "dense_vector", "dims": 3}}}
                            }
                        }
                    )
                if url == "http://es.test/huangxiao-video-library-vector-v1/_search" and method == "POST":
                    searches.append(body)
                    return FakeResponse(payload={"hits": {"total": {"value": 0}, "hits": []}})
                raise AssertionError(f"Unexpected request: {method} {url}")

            argv = [
                "run.py",
                "--index",
                "huangxiao-video-library-vector-v1",
                "--query",
                "夜晚路口有很多车辆经过",
                "--embedding-provider",
                "streetmodel",
                "--base-url",
                "http://streetmodel.test",
                "--dimensions",
                "3",
                "--config",
                str(config_path),
                "--output",
                str(output_path),
            ]

            with (
                mock.patch.object(module.urllib.request, "urlopen", side_effect=fake_urlopen),
                mock.patch.object(sys, "argv", argv),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                self.assertEqual(module.main(), 0)

            self.assertEqual(embed_payloads[0]["items"], [{"type": "text", "content": "夜晚路口有很多车辆经过"}])
            script = searches[0]["query"]["script_score"]["script"]["source"]
            self.assertIn(f"cosineSimilarity(params.query_vector, '{VECTOR_FIELD}')", script)
            self.assertEqual(searches[0]["query"]["script_score"]["query"], {"bool": {"must": [{"match_all": {}}], "filter": []}})
            result = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(result["data"]["query_mode"], "vector_filter")
            self.assertEqual(result["data"]["query_embedding"]["dimensions"], 3)

    def test_existing_vector_field_dimension_conflict_blocks_mapping_update(self):
        module = load_script(VIDEO_EMBED_SCRIPT, "video_embedding_index_mapping_test")
        client = module.EsClient({"elasticsearch": {"hosts": ["http://es.test"]}})
        methods: list[str] = []

        def fake_urlopen(req, timeout=0):
            methods.append(f"{req.get_method()} {req.full_url}")
            if req.full_url == "http://es.test/target-index" and req.get_method() == "HEAD":
                return FakeResponse(status=200)
            if req.full_url == "http://es.test/target-index/_mapping" and req.get_method() == "GET":
                return FakeResponse(
                    payload={
                        "target-index": {
                            "mappings": {
                                "properties": {
                                    VECTOR_FIELD: {"type": "dense_vector", "dims": 1024, "index": True}
                                }
                            }
                        }
                    }
                )
            raise AssertionError(f"Unexpected request: {req.get_method()} {req.full_url}")

        with mock.patch.object(module.urllib.request, "urlopen", side_effect=fake_urlopen):
            with self.assertRaises(module.EsError) as ctx:
                client.ensure_index("target-index", 2048, VECTOR_FIELD)
        self.assertEqual(ctx.exception.code, "VECTOR_FIELD_DIMENSION_MISMATCH")
        self.assertFalse(any(method.startswith("PUT ") for method in methods))


if __name__ == "__main__":
    unittest.main()
