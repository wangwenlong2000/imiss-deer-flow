#!/usr/bin/env python3
"""Unit tests for verifying script execution under conda environment.

Tests that scripts in the scripts/ directory can be executed via:
  conda run -n deerflow-street python <script_path>

Test image: /mnt/nas/streetview_meta/queries/247query/247query/00001.jpg
Test description: 日本东京涩谷十字路口的街景描述
Elasticsearch index: street
ES URL: http://localhost:3128
Model Service URL: http://localhost:3130
"""

import json
import os
import subprocess
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

# Paths
SCRIPTS_DIR = Path(__file__).parent.parent
TEST_IMAGE = "/mnt/nas/streetview_meta/queries/247query/247query/00001.jpg"
TEST_DESCRIPTION = (
    "日本东京涩谷十字路口的街景，画面中可以看到标志性的玻璃幕墙建筑——Q-FRONT大楼，"
    "其底层设有星巴克和TSUTAYA书店。画面左侧是密集的广告牌和商店招牌，包括"
    "\"サロンパス\"、\"三千里薬品\"等日文标识，展现了涩谷作为潮流文化中心的视觉冲击力。"
    "街道上行人正在等待红绿灯，呈现出典型的都市节奏。这个十字路口被誉为\"全世界最繁忙的十字路口\"，"
    "在高峰时段，每次绿灯亮起时，可有上千人同时从四面八方穿越马路，形成壮观的\"人潮交响曲\"。"
    "它不仅是东京的地标，也是全球流行文化的重要取景地，曾出现在《迷失东京》《速度与激情3》等多部影视作品中"
)
ES_INDEX = "street"
ES_URL = "http://localhost:3128"
MODEL_SERVICE_URL = "http://localhost:3130"
CONDA_ENV = "deerflow-street"


def run_script_with_conda(script_name: str, args: list[str] | None = None, input_data: str | None = None) -> subprocess.CompletedProcess:
    """Run a script via conda run command.
    
    Args:
        script_name: Name of the script file in scripts/ directory.
        args: List of command-line arguments to pass to the script.
        input_data: Optional stdin data to pipe to the script.
    
    Returns:
        CompletedProcess instance with returncode, stdout, stderr.
    """
    script_path = SCRIPTS_DIR / script_name
    
    cmd = [
        "conda", "run", "-n", CONDA_ENV,
        "python", str(script_path)
    ]
    
    if args:
        cmd.extend(args)
    
    result = subprocess.run(
        cmd,
        input=input_data,
        capture_output=True,
        text=True,
        timeout=120
    )
    
    return result


class TestScriptExecution(unittest.TestCase):
    """Test that scripts can be executed via conda run."""
    
    def test_conda_environment_exists(self):
        """Verify that the conda environment exists."""
        result = subprocess.run(
            ["conda", "env", "list"],
            capture_output=True,
            text=True
        )
        self.assertIn(CONDA_ENV, result.stdout)
    
    def test_es_get_mapping_help(self):
        """Test es_get_mapping.py --help execution."""
        result = run_script_with_conda("es_get_mapping.py", ["--help"])
        self.assertEqual(result.returncode, 0)
        self.assertIn("index", result.stdout.lower())
    
    def test_es_list_indices_help(self):
        """Test es_list_indices.py --help execution."""
        result = run_script_with_conda("es_list_indices.py", ["--help"])
        self.assertEqual(result.returncode, 0)
        self.assertIn("elasticsearch", result.stdout.lower())
    
    def test_es_query_dsl_help(self):
        """Test es_query_dsl.py --help execution."""
        result = run_script_with_conda("es_query_dsl.py", ["--help"])
        self.assertEqual(result.returncode, 0)
        self.assertIn("dsl", result.stdout.lower())
    
    def test_es_retrieve_topk_help(self):
        """Test es_retrieve_topk.py --help execution."""
        result = run_script_with_conda("es_retrieve_topk.py", ["--help"])
        self.assertEqual(result.returncode, 0)
        self.assertIn("retrieval", result.stdout.lower())
    
    def test_geocoding_help(self):
        """Test geocoding.py --help execution."""
        result = run_script_with_conda("geocoding.py", ["--help"])
        self.assertEqual(result.returncode, 0)
        self.assertIn("address", result.stdout.lower())


class TestESGetMapping(unittest.TestCase):
    """Test es_get_mapping.py script functionality."""
    
    def test_es_get_mapping_street_index(self):
        """Test getting mapping for 'street' index from ES."""
        result = run_script_with_conda(
            "es_get_mapping.py",
            ["--index", ES_INDEX, "--es-url", ES_URL]
        )
        self.assertEqual(result.returncode, 0)
        
        # Should output valid JSON
        mapping = json.loads(result.stdout)
        self.assertIn(ES_INDEX, mapping)
        
        # Verify expected fields exist in mapping
        index_mapping = mapping[ES_INDEX]
        self.assertIn("mappings", index_mapping)
        properties = index_mapping["mappings"].get("properties", {})
        
        # Check for expected fields based on mapping strategy
        self.assertIn("id", properties)
        self.assertIn("source_path", properties)
        self.assertIn("metadata", properties)
        self.assertIn("vector-ImAge4VPR", properties)
        # Text embedding field with instruction key
        self.assertIn("vector-Qwen3-VL-Embedding-2B_urban_governance", properties)
        
        # Verify field types
        self.assertEqual(properties["id"]["type"], "keyword")
        self.assertEqual(properties["source_path"]["type"], "keyword")
        self.assertEqual(properties["vector-ImAge4VPR"]["type"], "dense_vector")
        # Vector fields now have index=true for knn search
        self.assertTrue(properties["vector-ImAge4VPR"].get("index", False))
        self.assertTrue(properties["vector-Qwen3-VL-Embedding-2B_urban_governance"].get("index", False))
        
        # Verify vector dimensions and similarity
        self.assertEqual(properties["vector-ImAge4VPR"]["dims"], 4096)
        self.assertEqual(properties["vector-ImAge4VPR"]["similarity"], "cosine")
        self.assertEqual(properties["vector-Qwen3-VL-Embedding-2B_urban_governance"]["dims"], 2048)
        self.assertEqual(properties["vector-Qwen3-VL-Embedding-2B_urban_governance"]["similarity"], "cosine")


class TestESListIndices(unittest.TestCase):
    """Test es_list_indices.py script functionality."""
    
    def test_es_list_indices(self):
        """Test listing all ES indices."""
        result = run_script_with_conda(
            "es_list_indices.py",
            ["--es-url", ES_URL]
        )
        self.assertEqual(result.returncode, 0)
        
        # Should output valid JSON array
        indices = json.loads(result.stdout)
        self.assertIsInstance(indices, list)
        
        # Should contain 'street' index
        index_names = [idx.get("index") for idx in indices]
        self.assertIn(ES_INDEX, index_names)
    
    def test_es_list_indices_contains_expected_fields(self):
        """Test that street index exists and has basic info."""
        result = run_script_with_conda(
            "es_list_indices.py",
            ["--es-url", ES_URL]
        )
        self.assertEqual(result.returncode, 0)
        
        indices = json.loads(result.stdout)
        street_index = None
        for idx in indices:
            if idx.get("index") == ES_INDEX:
                street_index = idx
                break
        
        self.assertIsNotNone(street_index, f"Index '{ES_INDEX}' not found")
        # Should have basic index information
        self.assertIn("docs.count", street_index)
        self.assertIn("store.size", street_index)


class TestESQueryDSL(unittest.TestCase):
    """Test es_query_dsl.py script functionality."""
    
    def test_es_query_dsl_simple(self):
        """Test executing a simple DSL query."""
        # Simple match_all query
        dsl_query = json.dumps({
            "query": {"match_all": {}},
            "size": 1
        })
        
        result = run_script_with_conda(
            "es_query_dsl.py",
            ["--index", ES_INDEX, "--dsl", dsl_query, "--es-url", ES_URL]
        )
        self.assertEqual(result.returncode, 0)
        
        # Should output valid JSON
        response = json.loads(result.stdout)
        self.assertIn("hits", response)
        self.assertIn("total", response["hits"])
    
    @unittest.skip("stdin doesn't work well with conda run")
    def test_es_query_dsl_stdin(self):
        """Test executing DSL query via stdin."""
        dsl_query = json.dumps({
            "query": {"match_all": {}},
            "size": 1
        })
        
        result = run_script_with_conda(
            "es_query_dsl.py",
            ["--index", ES_INDEX, "--es-url", ES_URL],
            input_data=dsl_query
        )
        self.assertEqual(result.returncode, 0)
        
        response = json.loads(result.stdout)
        self.assertIn("hits", response)
    
    def test_es_query_dsl_with_source(self):
        """Test DSL query with _source filtering."""
        dsl_query = json.dumps({
            "query": {"match_all": {}},
            "size": 1,
            "_source": ["id", "source_path"]
        })
        
        result = run_script_with_conda(
            "es_query_dsl.py",
            ["--index", ES_INDEX, "--dsl", dsl_query, "--es-url", ES_URL]
        )
        self.assertEqual(result.returncode, 0)
        
        response = json.loads(result.stdout)
        self.assertIn("hits", response)
        
        # If there are hits, verify they have the expected fields
        hits = response["hits"].get("hits", [])
        if hits:
            source = hits[0].get("_source", {})
            self.assertIn("id", source)
            self.assertIn("source_path", source)


class TestESRetrieveTopK(unittest.TestCase):
    """Test es_retrieve_topk.py script functionality."""
    
    def test_retrieve_by_description(self):
        """Test top-k retrieval using text description."""
        result = run_script_with_conda(
            "es_retrieve_topk.py",
            [
                "--index", ES_INDEX,
                "--target-field", "source_path",
                "--description", TEST_DESCRIPTION,
                "--instruction-key", "urban_governance",
                "--k", "5",
                "--es-url", ES_URL
            ]
        )
        self.assertEqual(result.returncode, 0)
        
        response = json.loads(result.stdout)
        self.assertIn("top_k", response)
        self.assertIn("conclusion", response)
        self.assertIn("query_info", response)
        
        # Verify query info
        query_info = response["query_info"]
        self.assertTrue(query_info["description_provided"])
        self.assertFalse(query_info["image_provided"])
        self.assertEqual(query_info["instruction_key"], "urban_governance")
    
    def test_retrieve_by_image(self):
        """Test top-k retrieval using image.
        
        ES vector fields now have index=true, enabling knn search.
        """
        if not Path(TEST_IMAGE).exists():
            self.skipTest(f"Test image not found: {TEST_IMAGE}")
        
        result = run_script_with_conda(
            "es_retrieve_topk.py",
            [
                "--index", ES_INDEX,
                "--target-field", "source_path",
                "--image-path", TEST_IMAGE,
                "--k", "5",
                "--es-url", ES_URL
            ]
        )
        self.assertEqual(result.returncode, 0)
        
        response = json.loads(result.stdout)
        self.assertIn("top_k", response)
        self.assertIn("conclusion", response)
        
        # Verify query info
        query_info = response["query_info"]
        self.assertFalse(query_info["description_provided"])
        self.assertTrue(query_info["image_provided"])
    
    @unittest.skip("RRF requires commercial Elasticsearch license (security_exception)")
    def test_retrieve_multimodal(self):
        """Test top-k retrieval using both text and image (RRF fusion).
        
        Note: This test is skipped because RRF (Reciprocal Rank Fusion) requires
        a commercial Elasticsearch license. Current license is non-compliant for RRF.
        Error: AuthorizationException(403, 'security_exception', 'current license is non-compliant for [Reciprocal Rank Fusion (RRF)]')
        
        To enable this test, upgrade to an Elasticsearch license that supports RRF
        (Gold, Platinum, or Enterprise).
        """
        if not Path(TEST_IMAGE).exists():
            self.skipTest(f"Test image not found: {TEST_IMAGE}")
        
        result = run_script_with_conda(
            "es_retrieve_topk.py",
            [
                "--index", ES_INDEX,
                "--target-field", "source_path",
                "--description", TEST_DESCRIPTION,
                "--image-path", TEST_IMAGE,
                "--instruction-key", "urban_governance",
                "--k", "5",
                "--es-url", ES_URL
            ]
        )
        self.assertEqual(result.returncode, 0)
        
        response = json.loads(result.stdout)
        self.assertIn("top_k", response)
        
        # For RRF,both text and image should be provided
        query_info = response["query_info"]
        self.assertTrue(query_info["description_provided"])
        self.assertTrue(query_info["image_provided"])
    
    def test_retrieve_with_geo_filter(self):
        """Test top-k retrieval with geo-distance filter."""
        result = run_script_with_conda(
            "es_retrieve_topk.py",
            [
                "--index", ES_INDEX,
                "--target-field", "source_path",
                "--description", TEST_DESCRIPTION,
                "--center-latitude", "35.6595",
                "--center-longitude", "139.7004",
                "--max-distance", "1000",
                "--k", "5",
                "--es-url", ES_URL
            ]
        )
        self.assertEqual(result.returncode, 0)
        
        response = json.loads(result.stdout)
        self.assertIn("top_k", response)
        
        # Verify geo filter is applied
        query_info = response["query_info"]
        self.assertIsNotNone(query_info["geo_filter"])
        self.assertAlmostEqual(query_info["geo_filter"]["center_latitude"], 35.6595, places=2)
        self.assertAlmostEqual(query_info["geo_filter"]["center_longitude"], 139.7004, places=2)
        self.assertEqual(query_info["geo_filter"]["max_distance_m"], 1000)
    
    def test_retrieve_with_thresholds(self):
        """Test top-k retrieval with score thresholds."""
        result = run_script_with_conda(
            "es_retrieve_topk.py",
            [
                "--index", ES_INDEX,
                "--target-field", "source_path",
                "--description", TEST_DESCRIPTION,
                "--instruction-key", "urban_governance",
                "--k", "10",
                "--min-description-score", "0.5",
                "--es-url", ES_URL
            ]
        )
        self.assertEqual(result.returncode, 0)
        
        response = json.loads(result.stdout)
        self.assertIn("top_k", response)
        
        # Verify threshold is set in query info
        query_info = response["query_info"]
        self.assertEqual(query_info["min_description_score"], 0.5)


class TestGeocoding(unittest.TestCase):
    """Test geocoding.py script functionality."""

    def test_geocoding_fixed_location(self):
        """Test geocoding returns fixed Tokyo coordinates."""
        result = run_script_with_conda(
            "geocoding.py",
            ["--address", "日本东京涩谷"]
        )
        self.assertEqual(result.returncode, 0)

        response = json.loads(result.stdout)
        self.assertIn("latitude", response)
        self.assertIn("longitude", response)

        # Should return fixed coordinates (Shibuya, Tokyo)
        self.assertAlmostEqual(response["latitude"], 35.654008, places=4)
        self.assertAlmostEqual(response["longitude"], 139.705398, places=4)

    def test_geocoding_various_addresses(self):
        """Test geocoding with various address inputs."""
        test_addresses = [
            "北京市朝阳区",
            "上海市浦东新区",
            "广州市天河区",
            "Tokyo Shibuya",
            "",  # Empty address
        ]
        
        for address in test_addresses:
            with self.subTest(address=address):
                result = run_script_with_conda(
                    "geocoding.py",
                    ["--address", address]
                )
                self.assertEqual(result.returncode, 0)
                
                response = json.loads(result.stdout)
                self.assertIn("latitude", response)
                self.assertIn("longitude", response)
                # Should always return the same fixed coordinates
                self.assertAlmostEqual(response["latitude"], 35.654008, places=4)
                self.assertAlmostEqual(response["longitude"], 139.705398, places=4)


class TestStreetServer(unittest.TestCase):
    """Test street_server.py module functionality."""
    
    def test_health_check(self):
        """Test model service health check."""
        import sys
        sys.path.insert(0, str(SCRIPTS_DIR))
        
        # Temporarily override the base URL
        import street_server
        original_url = street_server._BASE_URL
        street_server._BASE_URL = MODEL_SERVICE_URL
        
        try:
            health_status = street_server.health()
            self.assertEqual(health_status["status"], "ok")
            self.assertIn("loaded_embed_models", health_status)
            # Should have at least one model loaded
            self.assertIsInstance(health_status["loaded_embed_models"], list)
        finally:
            # Restore original URL
            street_server._BASE_URL = original_url
    
    def test_list_models(self):
        """Test listing available models."""
        import sys
        sys.path.insert(0, str(SCRIPTS_DIR))
        import street_server
        
        original_url = street_server._BASE_URL
        street_server._BASE_URL = MODEL_SERVICE_URL
        
        try:
            models = street_server.list_models()
            self.assertIsInstance(models, list)
            self.assertGreater(len(models), 0)
            
            # Check for expected models
            model_names = [m["name"] for m in models]
            self.assertIn("Qwen3-VL-Embedding-2B", model_names)
            self.assertIn("ImAge4VPR", model_names)
        finally:
            street_server._BASE_URL = original_url
    
    def test_embed_text(self):
        """Test text embedding."""
        import sys
        sys.path.insert(0, str(SCRIPTS_DIR))
        import street_server
        
        original_url = street_server._BASE_URL
        street_server._BASE_URL = MODEL_SERVICE_URL
        
        try:
            embeddings = street_server.embed_text(
                texts=["测试文本", "Tokyo street"],
                model_name="Qwen3-VL-Embedding-2B",
                instruction_key="urban_governance"
            )
            self.assertIsInstance(embeddings, list)
            self.assertEqual(len(embeddings), 2)
            self.assertIsInstance(embeddings[0], list)
            # Should be 2048 dimensions
            self.assertEqual(len(embeddings[0]), 2048)
        finally:
            street_server._BASE_URL = original_url
    
    def test_embed_image(self):
        """Test image embedding."""
        if not Path(TEST_IMAGE).exists():
            self.skipTest(f"Test image not found: {TEST_IMAGE}")
        
        import sys
        sys.path.insert(0, str(SCRIPTS_DIR))
        import street_server
        
        original_url = street_server._BASE_URL
        street_server._BASE_URL = MODEL_SERVICE_URL
        
        try:
            embeddings = street_server.embed_image(
                image_path=TEST_IMAGE,
                model_name="ImAge4VPR"
            )
            self.assertIsInstance(embeddings, list)
            self.assertEqual(len(embeddings), 1)
            # Note: Current ES mapping has 4096 dims, not 6144
            # The vector is truncated to 4096 in es_retrieve_topk.py
            self.assertEqual(len(embeddings[0]), 6144)
        finally:
            street_server._BASE_URL = original_url
    
    def test_embed_text_with_instruction(self):
        """Test text embedding with different instruction keys."""
        import sys
        sys.path.insert(0, str(SCRIPTS_DIR))
        import street_server
        
        original_url = street_server._BASE_URL
        street_server._BASE_URL = MODEL_SERVICE_URL
        
        instruction_keys = ["urban_governance", "traffic_order", "safety_hazard"]
        
        try:
            for key in instruction_keys:
                with self.subTest(instruction_key=key):
                    embeddings = street_server.embed_text(
                        texts=[TEST_DESCRIPTION],
                        model_name="Qwen3-VL-Embedding-2B",
                        instruction_key=key
                    )
                    self.assertEqual(len(embeddings), 1)
                    self.assertEqual(len(embeddings[0]), 2048)
        finally:
            street_server._BASE_URL = original_url


class TestScriptImports(unittest.TestCase):
    """Test that scripts can be imported without errors."""
    
    def test_import_es_get_mapping_via_conda(self):
        """Test importing es_get_mapping functions via conda."""
        # Just verify the script can be loaded with --help (which imports everything)
        result = run_script_with_conda("es_get_mapping.py", ["--help"])
        self.assertEqual(result.returncode, 0)

    def test_import_es_list_indices_via_conda(self):
        """Test importing es_list_indices functions via conda."""
        result = run_script_with_conda("es_list_indices.py", ["--help"])
        self.assertEqual(result.returncode, 0)

    def test_import_es_query_dsl_via_conda(self):
        """Test importing es_query_dsl functions via conda."""
        result = run_script_with_conda("es_query_dsl.py", ["--help"])
        self.assertEqual(result.returncode, 0)

    def test_import_es_retrieve_topk_via_conda(self):
        """Test importing es_retrieve_topk functions via conda."""
        result = run_script_with_conda("es_retrieve_topk.py", ["--help"])
        self.assertEqual(result.returncode, 0)

    def test_import_geocoding(self):
        """Test importing geocoding constants."""
        import sys
        sys.path.insert(0, str(SCRIPTS_DIR))

        from geocoding import FIXED_LATITUDE, FIXED_LONGITUDE

        self.assertAlmostEqual(FIXED_LATITUDE, 35.654008, places=4)
        self.assertAlmostEqual(FIXED_LONGITUDE, 139.705398, places=4)

    def test_import_street_server(self):
        """Test importing street_server functions."""
        import sys
        sys.path.insert(0, str(SCRIPTS_DIR))

        from street_server import (
            health,
            list_models,
            embed,
            embed_image,
            embed_text,
            rerank,
            rerank_text_image,
            batch,
        )

        self.assertTrue(callable(health))
        self.assertTrue(callable(list_models))
        self.assertTrue(callable(embed))
        self.assertTrue(callable(embed_image))
        self.assertTrue(callable(embed_text))
        self.assertTrue(callable(rerank))
        self.assertTrue(callable(rerank_text_image))
        self.assertTrue(callable(batch))


if __name__ == "__main__":
    unittest.main()
