#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


CURRENT_FILE = Path(__file__).resolve()
PHONE_NETWORK_ROOT = CURRENT_FILE.parents[1]
GRAPH_OPERATOR_SCRIPT = (
    PHONE_NETWORK_ROOT / "graph-operator" / "scripts" / "graph_operator_wrapper.py"
)


def _unique_paths(items: List[Any]) -> List[Path]:
    result = []
    seen = set()
    for item in items:
        if not item:
            continue
        p = Path(item).expanduser()
        s = str(p)
        if s in seen:
            continue
        seen.add(s)
        result.append(p)
    return result


def resolve_default_call_graph_path() -> str:
    """
    自动兼容两类环境：
    1. DeerFlow sandbox: /mnt/datasets/...
    2. 宿主机项目目录: ~/imiss-deer-flow-main/datasets/...
    """
    candidates = _unique_paths([
        os.environ.get("PHONE_NETWORK_CALL_GRAPH"),
        "/mnt/datasets/phone-network/processed/unified/call_edges.csv",
        CURRENT_FILE.parents[4] / "datasets/phone-network/processed/unified/call_edges.csv",
        "/workspace/imiss-deer-flow-main/datasets/phone-network/processed/unified/call_edges.csv",
    ])

    for p in candidates:
        if p.exists():
            return str(p)

    # 都不存在时，优先返回 sandbox 版本，便于前端使用
    return "/mnt/datasets/phone-network/processed/unified/call_edges.csv"


def _python_can_import_required_packages(python_bin: str) -> bool:
    try:
        proc = subprocess.run(
            [python_bin, "-c", "import networkx, pandas; print('OK')"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        return proc.returncode == 0 and "OK" in (proc.stdout or "")
    except Exception:
        return False


def _find_python_with_required_packages() -> Tuple[Optional[str], List[str]]:
    """
    找一个能 import networkx + pandas 的 Python。
    """
    candidates = []

    env_python = os.environ.get("GRAPH_OPERATOR_PYTHON")
    if env_python:
        candidates.append(env_python)

    # 宿主机项目 venv
    try:
        candidates.append(str(CURRENT_FILE.parents[4] / "backend/.venv/bin/python"))
    except Exception:
        pass

    # 容器里常见 venv
    candidates.append("/app/backend/.venv/bin/python")

    which_python = shutil.which("python")
    if which_python:
        candidates.append(which_python)

    which_python3 = shutil.which("python3")
    if which_python3:
        candidates.append(which_python3)

    if sys.executable:
        candidates.append(sys.executable)

    checked = []
    for py in candidates:
        if not py:
            continue
        if not Path(py).exists():
            checked.append(f"{py} [not found]")
            continue
        ok = _python_can_import_required_packages(py)
        checked.append(f"{py} [{'OK' if ok else 'missing packages'}]")
        if ok:
            return py, checked

    return None, checked


def _run_command(cmd: List[str]) -> Dict[str, Any]:
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
    )

    stdout = (proc.stdout or "").strip()
    stderr = (proc.stderr or "").strip()

    if proc.returncode != 0:
        return {
            "ok": False,
            "error": "subprocess_failed",
            "returncode": proc.returncode,
            "stdout": stdout,
            "stderr": stderr,
            "cmd": cmd,
        }

    try:
        return json.loads(stdout)
    except Exception:
        return {
            "ok": False,
            "error": "invalid_json_output",
            "stdout": stdout,
            "stderr": stderr,
            "cmd": cmd,
        }


def run_graph_path_query(
    phone_a: str,
    phone_b: str,
    graph_path: Optional[str] = None,
    graph_format: str = "csv",
    source_col: str = "src_user_id",
    target_col: str = "dst_counterparty_id",
    directed: bool = False,
) -> Dict[str, Any]:
    if not GRAPH_OPERATOR_SCRIPT.exists():
        return {
            "ok": False,
            "error": "graph_operator_script_not_found",
            "script_path": str(GRAPH_OPERATOR_SCRIPT),
        }

    resolved_graph_path = graph_path or resolve_default_call_graph_path()

    python_bin, checked = _find_python_with_required_packages()
    if not python_bin:
        return {
            "ok": False,
            "error": "no_python_with_required_packages",
            "required_packages": ["networkx", "pandas"],
            "checked_candidates": checked,
        }

    cmd = [
        python_bin,
        str(GRAPH_OPERATOR_SCRIPT),
        "--operator", "path_trace",
        "--graph-path", resolved_graph_path,
        "--graph-format", graph_format,
        "--source-col", source_col,
        "--target-col", target_col,
        "--phone-a", phone_a,
        "--phone-b", phone_b,
    ]

    if directed:
        cmd.append("--directed")

    result = _run_command(cmd)
    result["adapter_meta"] = {
        "python_bin": python_bin,
        "resolved_graph_path": resolved_graph_path,
        "checked_candidates": checked,
    }
    return result


def run_graph_subgraph_query(
    center_node: str,
    hops: int = 1,
    max_nodes: int = 100,
    graph_path: Optional[str] = None,
    graph_format: str = "csv",
    source_col: str = "src_user_id",
    target_col: str = "dst_counterparty_id",
    directed: bool = False,
) -> Dict[str, Any]:
    if not GRAPH_OPERATOR_SCRIPT.exists():
        return {
            "ok": False,
            "error": "graph_operator_script_not_found",
            "script_path": str(GRAPH_OPERATOR_SCRIPT),
        }

    resolved_graph_path = graph_path or resolve_default_call_graph_path()

    python_bin, checked = _find_python_with_required_packages()
    if not python_bin:
        return {
            "ok": False,
            "error": "no_python_with_required_packages",
            "required_packages": ["networkx", "pandas"],
            "checked_candidates": checked,
        }

    cmd = [
        python_bin,
        str(GRAPH_OPERATOR_SCRIPT),
        "--operator", "subgraph_extract",
        "--graph-path", resolved_graph_path,
        "--graph-format", graph_format,
        "--source-col", source_col,
        "--target-col", target_col,
        "--center-node", center_node,
        "--hops", str(hops),
        "--max-nodes", str(max_nodes),
    ]

    if directed:
        cmd.append("--directed")

    result = _run_command(cmd)
    result["adapter_meta"] = {
        "python_bin": python_bin,
        "resolved_graph_path": resolved_graph_path,
        "checked_candidates": checked,
    }
    return result


def run_graph_basic_metrics(
    graph_path: Optional[str] = None,
    graph_format: str = "csv",
    source_col: str = "src_user_id",
    target_col: str = "dst_counterparty_id",
    directed: bool = False,
) -> Dict[str, Any]:
    if not GRAPH_OPERATOR_SCRIPT.exists():
        return {
            "ok": False,
            "error": "graph_operator_script_not_found",
            "script_path": str(GRAPH_OPERATOR_SCRIPT),
        }

    resolved_graph_path = graph_path or resolve_default_call_graph_path()

    python_bin, checked = _find_python_with_required_packages()
    if not python_bin:
        return {
            "ok": False,
            "error": "no_python_with_required_packages",
            "required_packages": ["networkx", "pandas"],
            "checked_candidates": checked,
        }

    cmd = [
        python_bin,
        str(GRAPH_OPERATOR_SCRIPT),
        "--operator", "basic_graph_metrics",
        "--graph-path", resolved_graph_path,
        "--graph-format", graph_format,
        "--source-col", source_col,
        "--target-col", target_col,
    ]

    if directed:
        cmd.append("--directed")

    result = _run_command(cmd)
    result["adapter_meta"] = {
        "python_bin": python_bin,
        "resolved_graph_path": resolved_graph_path,
        "checked_candidates": checked,
    }
    return result


def run_graph_neighbor_query(*args, **kwargs):
    raise NotImplementedError("当前这一步先不启用 neighbor_query，先把 association-path-analysis 跑稳。")


def run_graph_common_neighbor_query(*args, **kwargs):
    raise NotImplementedError("当前这一步先不启用 common_neighbor，后续再接 common_counterparty。")


if __name__ == "__main__":
    demo = run_graph_path_query(
        phone_a="d1beac94365462bd76de70f17c025864646a237800c4ac8a47cc724d63f04353b2ef3e1ff90a9677422bdc30da2a788a7fc9fd571d9146c0a1a4c2de49bfb12b",
        phone_b="bdece8ac3e7d68e8dd24c70cd87bdeddcc0be03123f001080219b52cd77ce33695964de4686d4b189d72b5c29d325b640f27a83131e9bfb599349f080d225dc8",
    )
    print(json.dumps(demo, ensure_ascii=False, indent=2))