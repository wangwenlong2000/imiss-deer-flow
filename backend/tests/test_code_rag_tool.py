import json
from types import SimpleNamespace


def _mock_config(root_path, **extra):
    model_extra = {"root_path": str(root_path), **extra}
    tool_config = SimpleNamespace(model_extra=model_extra)
    return SimpleNamespace(get_tool_config=lambda name: tool_config if name == "code_search" else None)


def test_code_search_returns_snippet_with_line_numbers(tmp_path, monkeypatch):
    from deerflow.community.code_rag import tools

    tools.clear_code_search_cache()
    source = tmp_path / "service.py"
    source.write_text(
        "\n".join(
            [
                "def ordinary():",
                "    return 'nothing'",
                "",
                "def fetch_user_profile(client, user_id):",
                "    response = client.get(f'/users/{user_id}')",
                "    return response.json()",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(tools, "get_app_config", lambda: _mock_config(tmp_path, snippet_context_lines=1))

    raw = tools.code_search_tool.invoke({"query": "fetch_user_profile", "top_k": 3})
    result = json.loads(raw)

    assert result["total_matches"] == 1
    assert result["candidate_files"] == 1
    match = result["results"][0]
    assert match["path"] == "service.py"
    assert match["start_line"] == 3
    assert match["end_line"] == 5
    assert "4 | def fetch_user_profile" in match["snippet"]


def test_code_search_filters_language_and_path_glob(tmp_path, monkeypatch):
    from deerflow.community.code_rag import tools

    tools.clear_code_search_cache()
    backend = tmp_path / "backend"
    frontend = tmp_path / "frontend"
    backend.mkdir()
    frontend.mkdir()
    (backend / "agent.py").write_text("def make_lead_agent():\n    return 'agent'\n", encoding="utf-8")
    (frontend / "agent.ts").write_text("export const makeLeadAgent = 'agent';\n", encoding="utf-8")
    monkeypatch.setattr(tools, "get_app_config", lambda: _mock_config(tmp_path))

    raw = tools.code_search_tool.invoke(
        {
            "query": "agent",
            "top_k": 10,
            "language": "python",
            "path_glob": "backend/**/*.py",
        }
    )
    result = json.loads(raw)

    assert [match["path"] for match in result["results"]] == ["backend/agent.py"]


def test_python_ast_chunking_extracts_header_class_method_and_function():
    from deerflow.community.code_rag import tools

    chunks = tools._chunk_source(
        "\n".join(
            [
                '"""module docs"""',
                "import os",
                "VALUE = 1",
                "",
                "class UserRepo:",
                "    def get_user(self, user_id):",
                "        return user_id",
                "",
                "def helper():",
                "    return VALUE",
            ]
        ),
        tools.Path("repo.py"),
    )

    chunk_keys = {(chunk.kind, chunk.symbol, chunk.start_line, chunk.end_line) for chunk in chunks}
    assert ("file_header", "module", 1, 3) in chunk_keys
    assert ("class", "UserRepo", 5, 7) in chunk_keys
    assert ("method", "UserRepo.get_user", 6, 7) in chunk_keys
    assert ("function", "helper", 9, 10) in chunk_keys


def test_code_search_returns_chunk_metadata_for_method_match(tmp_path, monkeypatch):
    from deerflow.community.code_rag import tools

    tools.clear_code_search_cache()
    source = tmp_path / "repo.py"
    source.write_text(
        "\n".join(
            [
                "class UserRepo:",
                "    def get_user(self, user_id):",
                "        return user_id",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(tools, "get_app_config", lambda: _mock_config(tmp_path, snippet_context_lines=0))

    raw = tools.code_search_tool.invoke({"query": "get_user", "top_k": 1})
    result = json.loads(raw)

    assert result["candidate_chunks"] == 2
    match = result["results"][0]
    assert match["chunk_kind"] == "method"
    assert match["symbol"] == "UserRepo.get_user"
    assert match["chunk_start_line"] == 2
    assert match["chunk_end_line"] == 3
    assert match["start_line"] == 2
    assert match["metadata"]["path"] == "repo.py"
    assert match["metadata"]["kind"] == "method"
    assert match["metadata"]["symbol"] == "UserRepo.get_user"
    assert match["metadata"]["language"] == "python"
    assert len(match["metadata"]["id"]) == 24
    assert len(match["metadata"]["content_hash"]) == 64
    assert len(match["metadata"]["file_hash"]) == 64
    assert "def get_user" in match["snippet"]


def test_code_chunk_metadata_records_imports_and_tags(tmp_path, monkeypatch):
    from deerflow.community.code_rag import tools

    tools.clear_code_search_cache()
    source_dir = tmp_path / "backend" / "agents"
    source_dir.mkdir(parents=True)
    source = source_dir / "agent_tool.py"
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
    monkeypatch.setattr(tools, "get_app_config", lambda: _mock_config(tmp_path, snippet_context_lines=0))

    raw = tools.code_search_tool.invoke({"query": "code_search_tool", "top_k": 1})
    result = json.loads(raw)
    metadata = result["results"][0]["metadata"]

    assert metadata["path"] == "backend/agents/agent_tool.py"
    assert metadata["imports"] == ["ast", "langchain"]
    assert {"agent", "code-analysis", "python", "retrieval", "tool"}.issubset(metadata["tags"])
    assert metadata["start_line"] == 4
    assert metadata["end_line"] == 5


def test_code_search_excludes_secret_and_generated_files(tmp_path, monkeypatch):
    from deerflow.community.code_rag import tools

    tools.clear_code_search_cache()
    generated = tmp_path / "generated"
    generated.mkdir()
    (tmp_path / ".env").write_text("SECRET_TOKEN=needle\n", encoding="utf-8")
    (tmp_path / "private.pem").write_text("needle\n", encoding="utf-8")
    (generated / "client.py").write_text("def generated_needle():\n    return 'needle'\n", encoding="utf-8")
    (tmp_path / "service.py").write_text("def real_needle():\n    return 'needle'\n", encoding="utf-8")
    monkeypatch.setattr(tools, "get_app_config", lambda: _mock_config(tmp_path))

    raw = tools.code_search_tool.invoke({"query": "needle", "top_k": 10})
    result = json.loads(raw)

    assert [match["path"] for match in result["results"]] == ["service.py"]
    assert result["scan_stats"]["skipped_by_directory"] == 1
    assert result["scan_stats"]["skipped_by_glob"] >= 2


def test_code_search_skips_large_files(tmp_path, monkeypatch):
    from deerflow.community.code_rag import tools

    tools.clear_code_search_cache()
    (tmp_path / "large.py").write_text("needle = '" + ("x" * 100) + "'\n", encoding="utf-8")
    (tmp_path / "small.py").write_text("needle = 'small'\n", encoding="utf-8")
    monkeypatch.setattr(tools, "get_app_config", lambda: _mock_config(tmp_path, max_file_size_bytes=30))

    raw = tools.code_search_tool.invoke({"query": "needle", "top_k": 10})
    result = json.loads(raw)

    assert [match["path"] for match in result["results"]] == ["small.py"]
    assert result["scan_stats"]["skipped_by_size"] == 1


def test_code_search_enforces_allowed_root_path(tmp_path, monkeypatch):
    from deerflow.community.code_rag import tools

    root = tmp_path / "repo"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    monkeypatch.setattr(
        tools,
        "get_app_config",
        lambda: _mock_config(outside, allowed_root_path=root),
    )

    raw = tools.code_search_tool.invoke({"query": "needle"})
    result = json.loads(raw)

    assert result["error"] == "root_path is outside allowed_root_path"
    assert result["allowed_root_path"] == str(root.resolve())


def test_code_search_respects_scan_limit(tmp_path, monkeypatch):
    from deerflow.community.code_rag import tools

    tools.clear_code_search_cache()
    for index in range(3):
        (tmp_path / f"file_{index}.py").write_text("needle = True\n", encoding="utf-8")
    monkeypatch.setattr(tools, "get_app_config", lambda: _mock_config(tmp_path, max_files_scanned=1))

    raw = tools.code_search_tool.invoke({"query": "needle", "top_k": 10})
    result = json.loads(raw)

    assert result["scanned_files"] == 1
    assert result["candidate_files"] == 1
    assert result["scan_stats"]["stopped_by_scan_limit"] == 1


def test_code_search_uses_cache_and_invalidates_on_file_change(tmp_path, monkeypatch):
    from deerflow.community.code_rag import tools

    tools.clear_code_search_cache()
    source = tmp_path / "service.py"
    source.write_text("needle = 'one'\n", encoding="utf-8")
    monkeypatch.setattr(tools, "get_app_config", lambda: _mock_config(tmp_path))

    first = json.loads(tools.code_search_tool.invoke({"query": "needle"}))
    second = json.loads(tools.code_search_tool.invoke({"query": "needle"}))
    source.write_text("needle = 'two updated'\n", encoding="utf-8")
    third = json.loads(tools.code_search_tool.invoke({"query": "needle"}))

    assert first["cache"] == {"hits": 0, "misses": 1, "entries": 1}
    assert second["cache"]["hits"] == 1
    assert second["cache"]["misses"] == 0
    assert third["cache"]["hits"] == 0
    assert third["cache"]["misses"] == 1
    assert third["cache"]["entries"] == 1
    assert "two updated" in third["results"][0]["snippet"]
