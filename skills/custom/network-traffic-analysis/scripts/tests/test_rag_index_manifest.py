#!/usr/bin/env python3
"""Lightweight unit tests for RAG index manifest helpers.

No Elasticsearch required — pure function coverage only.
"""
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

SCRIPT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SCRIPT_DIR))

# Add utils path for path resolution helpers
REPO_ROOT = SCRIPT_DIR.parents[3]
sys.path.insert(0, str(REPO_ROOT))

from index_rag_docs import source_delete_filters, build_manifest


SAMPLE_DOCS = [
    {
        "doc_id": "doc-001",
        "dataset_name": "Neris",
        "source_file": "datasets/network-traffic/processed/Neris/Neris.flow.csv",
        "schema_version": "rag_doc_v2",
        "doc_type": "flow_summary",
        "title": "test",
        "content": "test content",
        "summary": "test summary",
        "keywords": [],
        "metadata": {},
        "embedding": [0.1] * 1024,
        "embedding_model": "bge-m3",
        "embedding_dimensions": 1024,
    },
    {
        "doc_id": "doc-002",
        "dataset_name": "Neris",
        "source_file": "datasets/network-traffic/processed/Neris/Neris.flow.csv",
        "schema_version": "rag_doc_v2",
        "doc_type": "anomaly_summary",
        "title": "test",
        "content": "test content",
        "summary": "test summary",
        "keywords": [],
        "metadata": {},
        "embedding": [0.2] * 1024,
        "embedding_model": "bge-m3",
        "embedding_dimensions": 1024,
    },
]


class TestSourceDeleteFilters(unittest.TestCase):
    def test_source_delete_filters_are_deterministic(self):
        """Filters should be sorted and unique regardless of input order."""
        shuffled = list(reversed(SAMPLE_DOCS))
        result = source_delete_filters(shuffled)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["dataset_name"], "Neris")
        self.assertEqual(result[0]["source_file"], "datasets/network-traffic/processed/Neris/Neris.flow.csv")
        self.assertEqual(result[0]["schema_version"], "rag_doc_v2")

    def test_source_delete_filters_multiple_datasets(self):
        """Multiple dataset/source combos should each get a filter."""
        docs = [
            {**SAMPLE_DOCS[0], "dataset_name": "Neris", "source_file": "a.csv"},
            {**SAMPLE_DOCS[0], "dataset_name": "Zeus", "source_file": "b.csv"},
            {**SAMPLE_DOCS[0], "dataset_name": "Neris", "source_file": "a.csv"},  # duplicate
        ]
        result = source_delete_filters(docs)
        self.assertEqual(len(result), 2)
        datasets = {f["dataset_name"] for f in result}
        self.assertEqual(datasets, {"Neris", "Zeus"})

    def test_source_delete_filters_require_dataset_and_source(self):
        """Documents without dataset_name or source_file should be skipped."""
        docs = [
            {"doc_id": "x"},
            {"doc_id": "y", "dataset_name": "Neris"},
            {"doc_id": "z", "source_file": "a.csv"},
            {**SAMPLE_DOCS[0], "dataset_name": "Neris", "source_file": "a.csv"},
        ]
        result = source_delete_filters(docs)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["dataset_name"], "Neris")

    def test_source_delete_filters_empty_schema_version(self):
        """Missing schema_version should produce an empty string filter."""
        docs = [{**SAMPLE_DOCS[0], "schema_version": ""}]
        result = source_delete_filters(docs)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["schema_version"], "")


class TestBuildManifest(unittest.TestCase):
    @patch("index_rag_docs.sha256_file", return_value="fake-sha256")
    @patch("index_rag_docs.to_repo_relative_display", side_effect=lambda v: str(v))
    def test_build_manifest_includes_replace_result(self, mock_display, mock_sha):
        """Manifest should include replace_result fields even when not using replace-source."""
        mock_config = {
            "hosts": ["http://localhost:9200"],
            "config_path": "/tmp/config.yaml",
        }
        manifest = build_manifest(
            input_files=["/tmp/rag_embeddings.jsonl"],
            output_file=Path("/tmp/index_manifest.json"),
            index_name="test-index",
            indexed_count=10,
            documents=SAMPLE_DOCS,
            index_status="existing",
            config=mock_config,
            index_duration_seconds=1.0,
            es_count_by_dataset={"Neris": 10},
            replace_result={
                "replace_mode": "source",
                "deleted_before_index": 5,
                "delete_filters": [
                    {
                        "dataset_name": "Neris",
                        "source_file": "a.csv",
                        "schema_version": "v1",
                    }
                ],
            },
        )
        self.assertEqual(manifest["replace_mode"], "source")
        self.assertEqual(manifest["deleted_before_index"], 5)
        self.assertEqual(len(manifest["delete_filters"]), 1)

    @patch("index_rag_docs.sha256_file", return_value="fake-sha256")
    @patch("index_rag_docs.to_repo_relative_display", side_effect=lambda v: str(v))
    def test_build_manifest_no_replace(self, mock_display, mock_sha):
        """Manifest should default replace fields to none/0/[] when not provided."""
        mock_config = {
            "hosts": ["http://localhost:9200"],
            "config_path": "/tmp/config.yaml",
        }
        manifest = build_manifest(
            input_files=["/tmp/rag_embeddings.jsonl"],
            output_file=Path("/tmp/index_manifest.json"),
            index_name="test-index",
            indexed_count=10,
            documents=SAMPLE_DOCS,
            index_status="existing",
            config=mock_config,
            index_duration_seconds=1.0,
        )
        self.assertEqual(manifest["replace_mode"], "none")
        self.assertEqual(manifest["deleted_before_index"], 0)
        self.assertEqual(manifest["delete_filters"], [])

    @patch("index_rag_docs.sha256_file", return_value="fake-sha256")
    @patch("index_rag_docs.to_repo_relative_display", side_effect=lambda v: str(v))
    def test_build_manifest_dataset_counts(self, mock_display, mock_sha):
        """Manifest should include per-dataset input and type counts."""
        mock_config = {
            "hosts": ["http://localhost:9200"],
            "config_path": "/tmp/config.yaml",
        }
        docs = [
            {**SAMPLE_DOCS[0], "dataset_name": "Neris", "doc_type": "flow_summary"},
            {**SAMPLE_DOCS[0], "dataset_name": "Neris", "doc_type": "flow_summary"},
            {**SAMPLE_DOCS[0], "dataset_name": "Zeus", "doc_type": "anomaly_summary"},
        ]
        manifest = build_manifest(
            input_files=["/tmp/rag_embeddings.jsonl"],
            output_file=Path("/tmp/index_manifest.json"),
            index_name="test-index",
            indexed_count=3,
            documents=docs,
            index_status="existing",
            config=mock_config,
            index_duration_seconds=1.0,
        )
        self.assertEqual(manifest["input_document_count_by_dataset"]["Neris"], 2)
        self.assertEqual(manifest["input_document_count_by_dataset"]["Zeus"], 1)
        self.assertEqual(manifest["document_types_by_dataset"]["Neris"]["flow_summary"], 2)
        self.assertEqual(manifest["document_types_by_dataset"]["Zeus"]["anomaly_summary"], 1)


if __name__ == "__main__":
    unittest.main()
