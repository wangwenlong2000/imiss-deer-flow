from __future__ import annotations

import asyncio
import importlib.util
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
sys.path.insert(0, str(ROOT))

from mcp_servers.streetmodel_video_embedding import core

VECTOR_FIELD = "video_vector-Qwen3-VL-Embedding-2B_urban_governance"


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


class FakeCompletedProcess:
    def __init__(self, returncode: int = 0, stdout: str = "", stderr: str = ""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def write_config(
    tmp_path: Path,
    *,
    dimensions: int = 2048,
    deerflow_prefix: str = "/data/deerflow/videos",
    streetmodel_prefix: str = "/nfsdat2/home/xhuangslm/shared_videos",
) -> Path:
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "streetmodel_embedding": {
                    "base_url": "http://streetmodel.test",
                    "model_name": "Qwen3-VL-Embedding-2B",
                    "vector_field": VECTOR_FIELD,
                    "dimensions": dimensions,
                    "batch_size": 1,
                    "timeout_seconds": 600,
                    "video": {
                        "deerflow_path_prefix": deerflow_prefix,
                        "streetmodel_path_prefix": streetmodel_prefix,
                    },
                },
                "elasticsearch": {"hosts": ["http://es.test"]},
            }
        ),
        encoding="utf-8",
    )
    return config_path


class StreetModelVideoEmbeddingMcpCoreTests(unittest.TestCase):
    def test_core_path_mapping_rules(self):
        self.assertEqual(
            core.map_video_uri(
                "/data/deerflow/videos/camera01/0001.mp4",
                "/data/deerflow/videos",
                "/nfsdat2/home/xhuangslm/shared_videos",
            ),
            "/nfsdat2/home/xhuangslm/shared_videos/camera01/0001.mp4",
        )
        self.assertEqual(
            core.map_video_uri(
                "/nfsdat2/home/xhuangslm/shared_videos/camera01/0001.mp4",
                "/data/deerflow/videos",
                "/nfsdat2/home/xhuangslm/shared_videos",
            ),
            "/nfsdat2/home/xhuangslm/shared_videos/camera01/0001.mp4",
        )
        with self.assertRaises(Exception) as ctx:
            core.map_video_uri("/tmp/local.mp4", "/data/deerflow/videos", "/nfsdat2/home/xhuangslm/shared_videos")
        self.assertEqual(getattr(ctx.exception, "code", None), "VIDEO_URI_NOT_MAPPABLE")

    def test_core_rejects_unsafe_target_index_before_network_calls(self):
        with mock.patch.object(core, "_runtime") as runtime:
            result = core.streetmodel_embed_video(
                "/nfsdat2/home/xhuangslm/shared_videos/camera01/0001.mp4",
                video_id="camera01_0001",
                target_index="citybrain-video-library",
            )

        runtime.assert_not_called()
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["error_code"], "UNSAFE_TARGET_INDEX")
        self.assertEqual(result["embedded_count"], 0)
        self.assertEqual(result["failed_count"], 1)

    def test_core_embed_video_writes_es_and_hides_vector_by_default(self):
        skill_module = core._load_skill_module()
        vector = [0.25] * 2048
        upserts: list[dict[str, Any]] = []

        with tempfile.TemporaryDirectory() as tmp:
            config_path = write_config(Path(tmp))

            def fake_urlopen(req, timeout=0):
                url = req.full_url
                method = req.get_method()
                body = json.loads(req.data.decode("utf-8")) if req.data else None
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
                if url == "http://es.test/huangxiao-video-library-vector-v1/_doc/camera01_0001" and method == "PUT":
                    upserts.append(body)
                    return FakeResponse(payload={"result": "created"})
                if url == "http://es.test/huangxiao-video-library-vector-v1/_refresh" and method == "POST":
                    return FakeResponse(payload={"refreshed": True})
                raise AssertionError(f"Unexpected request: {method} {url}")

            with (
                mock.patch.object(skill_module.urllib.request, "urlopen", side_effect=fake_urlopen),
                mock.patch.dict(core.os.environ, {"DEER_FLOW_CONFIG_PATH": str(config_path)}, clear=False),
            ):
                result = core.streetmodel_embed_video(
                    "/nfsdat2/home/xhuangslm/shared_videos/camera01/0001.mp4",
                    video_id="camera01_0001",
                    video_preprocess="none",
                )

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["embedded_count"], 1)
        self.assertEqual(result["video_id"], "camera01_0001")
        self.assertEqual(result["vector_field"], VECTOR_FIELD)
        self.assertNotIn("vector", result)
        self.assertNotIn("vector", result["documents"][0])
        self.assertEqual(upserts[0][VECTOR_FIELD], vector)
        self.assertEqual(upserts[0]["video_embedding_dimensions"], 2048)

    def test_core_embed_video_creates_frame_proxy_and_writes_single_doc(self):
        skill_module = core._load_skill_module()
        vector = [0.35] * 2048
        upserts: list[dict[str, Any]] = []
        embed_payloads: list[dict[str, Any]] = []

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            deerflow_prefix = tmp_path / "deerflow" / "videos"
            local_video = deerflow_prefix / "camera01" / "0001.mp4"
            local_video.parent.mkdir(parents=True)
            local_video.write_bytes(b"fake-video")
            config_path = write_config(tmp_path, deerflow_prefix=str(deerflow_prefix), streetmodel_prefix="/street/shared")

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
                    self.assertTrue(body["items"][0]["uri"].startswith("/street/shared/embedding_proxies/mcp-proxy-video-"))
                    return FakeResponse(payload={"embeddings": [vector], "shape": [1, 2048]})
                if url == "http://es.test/huangxiao-video-library-vector-v1" and method == "HEAD":
                    raise http_error(404)
                if url == "http://es.test/huangxiao-video-library-vector-v1" and method == "PUT":
                    return FakeResponse(payload={"acknowledged": True})
                if url == "http://es.test/huangxiao-video-library-vector-v1/_doc/mcp-proxy-video" and method == "PUT":
                    upserts.append(body)
                    return FakeResponse(payload={"result": "created"})
                if url == "http://es.test/huangxiao-video-library-vector-v1/_refresh" and method == "POST":
                    return FakeResponse(payload={"refreshed": True})
                raise AssertionError(f"Unexpected request: {method} {url}")

            def fake_run(cmd, capture_output=True, text=True, check=False):
                if cmd[0] == "ffprobe":
                    return FakeCompletedProcess(
                        stdout=json.dumps(
                            {
                                "streams": [{"width": 1280, "height": 720}],
                                "format": {"duration": "80.0"},
                            }
                        )
                    )
                if cmd[0] == "ffmpeg":
                    Path(cmd[-1]).write_bytes(b"proxy-video")
                    return FakeCompletedProcess()
                raise AssertionError(f"Unexpected command: {cmd}")

            with (
                mock.patch.object(skill_module.urllib.request, "urlopen", side_effect=fake_urlopen),
                mock.patch.object(skill_module.shutil, "which", side_effect=lambda name: f"/usr/bin/{name}" if name in {"ffmpeg", "ffprobe"} else None),
                mock.patch.object(skill_module.subprocess, "run", side_effect=fake_run),
                mock.patch.dict(core.os.environ, {"DEER_FLOW_CONFIG_PATH": str(config_path)}, clear=False),
            ):
                result = core.streetmodel_embed_video(str(local_video), video_id="mcp-proxy-video")

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["embedded_count"], 1)
        self.assertEqual(result["video_embedding_preprocess_mode"], "frame_proxy")
        self.assertEqual(len(embed_payloads), 1)
        self.assertEqual(len(upserts), 1)
        self.assertEqual(upserts[0][VECTOR_FIELD], vector)
        self.assertEqual(upserts[0]["video_embedding_source_uri"], "/street/shared/camera01/0001.mp4")
        self.assertEqual(upserts[0]["video_embedding_preprocess_mode"], "frame_proxy")
        self.assertEqual(upserts[0]["video_embedding_sampled_frames"], 80)

    def test_core_embed_query_invalid_shape_is_structured_failure(self):
        skill_module = core._load_skill_module()

        with tempfile.TemporaryDirectory() as tmp:
            config_path = write_config(Path(tmp), dimensions=2048)

            def fake_urlopen(req, timeout=0):
                url = req.full_url
                if url == "http://streetmodel.test/health":
                    return FakeResponse(payload={"status": "ok"})
                if url == "http://streetmodel.test/models":
                    return FakeResponse(payload={"models": [{"name": "Qwen3-VL-Embedding-2B", "exists": True}]})
                if url == "http://streetmodel.test/embed":
                    return FakeResponse(payload={"embeddings": [[0.1, 0.2, 0.3]], "shape": [1, 3]})
                raise AssertionError(f"Unexpected request: {req.get_method()} {url}")

            with (
                mock.patch.object(skill_module.urllib.request, "urlopen", side_effect=fake_urlopen),
                mock.patch.dict(core.os.environ, {"DEER_FLOW_CONFIG_PATH": str(config_path)}, clear=False),
            ):
                result = core.streetmodel_embed_query("night traffic")

        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["error_code"], "STREETMODEL_INVALID_RESPONSE")
        self.assertEqual(result["embedded_count"], 0)
        self.assertEqual(result["failed_count"], 1)


@unittest.skipIf(
    importlib.util.find_spec("mcp") is None or importlib.util.find_spec("langchain_mcp_adapters") is None,
    "Python MCP SDK or langchain MCP adapters are not installed",
)
class StreetModelVideoEmbeddingMcpServerSmokeTests(unittest.TestCase):
    def test_mcp_server_smoke_imports_expected_tools(self):
        from mcp_servers.streetmodel_video_embedding import server

        self.assertTrue(callable(server.streetmodel_health))
        self.assertTrue(callable(server.streetmodel_embed_video))
        self.assertTrue(callable(server.streetmodel_embed_query))

    def test_mcp_client_discovers_expected_tools_over_stdio(self):
        from langchain_mcp_adapters.client import MultiServerMCPClient

        async def discover_tool_names() -> set[str]:
            client = MultiServerMCPClient(
                {
                    "streetmodel-video-embedding": {
                        "transport": "stdio",
                        "command": sys.executable,
                        "args": ["-m", "mcp_servers.streetmodel_video_embedding.server"],
                        "env": {"PYTHONPATH": str(ROOT)},
                    }
                }
            )
            tools = await client.get_tools()
            return {tool.name for tool in tools}

        names = asyncio.run(discover_tool_names())
        self.assertTrue(
            {"streetmodel_health", "streetmodel_embed_video", "streetmodel_embed_query"}.issubset(names)
        )


if __name__ == "__main__":
    unittest.main()
