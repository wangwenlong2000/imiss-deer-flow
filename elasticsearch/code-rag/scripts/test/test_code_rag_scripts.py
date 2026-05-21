from pathlib import Path
from unittest.mock import patch
import unittest

from code_chunker import build_chunks_for_file
from code_embedding import build_embedding_text
from code_indexer import parse_args as parse_index_args, vector_field_name
from code_retrieve_topk import parse_args as parse_retrieve_args, rrf_fuse


class CodeRagScriptTests(unittest.TestCase):
    def test_build_chunks_for_file_records_metadata(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            source = tmp_path / "backend" / "tools.py"
            source.parent.mkdir()
            source.write_text(
                "\n".join(
                    [
                        "import ast",
                        "from langchain.tools import tool",
                        "",
                        "def code_search_tool(query):",
                        "    return query",
                    ]
                ),
                encoding="utf-8",
            )

            chunks = build_chunks_for_file(source, root_path=tmp_path, repo="deerflow")
            function = next(chunk for chunk in chunks if chunk.kind == "function")

            self.assertEqual(function.path, "backend/tools.py")
            self.assertEqual(function.symbol, "code_search_tool")
            self.assertEqual(function.imports, ["ast", "langchain"])
            self.assertTrue({"function", "python", "tool", "retrieval", "code-analysis"}.issubset(function.tags))
            self.assertEqual(len(function.id), 24)
            self.assertEqual(len(function.content_hash), 64)
            self.assertEqual(function.metadata["path"], function.path)

    def test_build_embedding_text_includes_metadata(self):
        document = {
            "path": "backend/tools.py",
            "language": "python",
            "kind": "function",
            "symbol": "code_search_tool",
            "tags": ["tool", "retrieval"],
            "imports": ["langchain"],
            "code": "def code_search_tool(query): pass",
            "metadata": {},
        }

        text = build_embedding_text(document)

        self.assertIn("path: backend/tools.py", text)
        self.assertIn("symbol: code_search_tool", text)
        self.assertIn("imports: langchain", text)
        self.assertIn("def code_search_tool", text)

    def test_vector_field_name_is_es_safe(self):
        self.assertEqual(vector_field_name("BAAI/bge-m3"), "vector-BAAI__bge_m3")

    def test_rrf_fuse_combines_dense_and_keyword_rankings(self):
        dense = [
            {"_id": "a", "_score": 0.9, "_source": {"path": "a.py"}},
            {"_id": "b", "_score": 0.8, "_source": {"path": "b.py"}},
        ]
        keyword = [
            {"_id": "b", "_score": 10.0, "_source": {"path": "b.py"}},
            {"_id": "c", "_score": 9.0, "_source": {"path": "c.py"}},
        ]

        fused = rrf_fuse([dense, keyword], rank_constant=60)

        self.assertEqual(fused[0]["_id"], "b")
        self.assertEqual(fused[0]["dense_score"], 0.8)
        self.assertEqual(fused[0]["keyword_score"], 10.0)
        self.assertEqual({item["_id"] for item in fused}, {"a", "b", "c"})

    def test_indexer_reads_config_defaults(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / "config.yaml"
            root_path = tmp.replace("\\", "/")
            config.write_text(
                "\n".join(
                    [
                        "elasticsearch:",
                        '  url: "http://example.test:9200"',
                        "source:",
                        f'  root_path: "{root_path}"',
                        '  repo: "sample"',
                        "index:",
                        '  name: "sample_code"',
                        "embedding:",
                        '  model: "sentence-transformers/all-MiniLM-L6-v2"',
                    ]
                ),
                encoding="utf-8",
            )
            with patch("sys.argv", ["code_indexer.py", "--config", str(config)]):
                args = parse_index_args()

        self.assertEqual(args.root_path.replace("\\", "/"), tmp.replace("\\", "/"))
        self.assertEqual(args.repo, "sample")
        self.assertEqual(args.index, "sample_code")
        self.assertEqual(args.es_url, "http://example.test:9200")

    def test_retriever_reads_config_defaults(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / "config.yaml"
            config.write_text(
                "\n".join(
                    [
                        "source:",
                        '  repo: "sample"',
                        "index:",
                        '  name: "sample_code"',
                        "retrieval:",
                        "  k: 3",
                        '  language: "python"',
                        "  tags:",
                        '    - "tool"',
                    ]
                ),
                encoding="utf-8",
            )
            with patch("sys.argv", ["code_retrieve_topk.py", "--config", str(config), "--query", "search tool"]):
                args = parse_retrieve_args()

        self.assertEqual(args.query, "search tool")
        self.assertEqual(args.repo, "sample")
        self.assertEqual(args.index, "sample_code")
        self.assertEqual(args.k, 3)
        self.assertEqual(args.language, "python")
        self.assertEqual(args.tag, ["tool"])


if __name__ == "__main__":
    unittest.main()
