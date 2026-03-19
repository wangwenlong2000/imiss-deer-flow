from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from langchain.tools import tool


def _find_repo_root() -> Path:
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "skills" / "custom" / "network-traffic-analysis" / "scripts" / "prepare_pcap.py").exists():
            return parent
    raise FileNotFoundError("Could not locate repository root for network traffic prepare tool")


REPO_ROOT = _find_repo_root()
SKILL_SCRIPTS_DIR = REPO_ROOT / "skills" / "custom" / "network-traffic-analysis" / "scripts"
PREPARE_SCRIPT = SKILL_SCRIPTS_DIR / "prepare_pcap.py"


def _run_prepare(command: list[str]) -> str:
    completed = subprocess.run(
        command,
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    stdout = (completed.stdout or "").strip()
    stderr = (completed.stderr or "").strip()
    if completed.returncode == 0:
        try:
            payload = json.loads(stdout) if stdout else {}
        except json.JSONDecodeError:
            if stderr:
                return f"{stderr}\n\n{stdout}".strip()
            return stdout or "PCAP preprocessing completed successfully."
        lines = [
            f"Prepared dataset: {payload.get('dataset_name', 'unknown')}",
            f"Source PCAP files: {len(payload.get('source_files', []))}",
            f"Packet rows: {payload.get('packet_rows', 0)}",
            f"Flow rows: {payload.get('flow_rows', 0)}",
            f"packet.csv: {payload.get('packet_csv', '')}",
            f"flow.csv: {payload.get('flow_csv', '')}",
            f"metadata: {REPO_ROOT / 'datasets' / 'network-traffic' / 'processed' / payload.get('dataset_name', '') / 'metadata.json' if payload.get('dataset_name') else payload.get('metadata', '')}",
            f"Use this flow file for the next analysis step: {payload.get('flow_csv', '')}",
        ]
        if stderr:
            lines.insert(0, stderr)
        return "\n".join(lines).strip()
    parts = ["Network traffic PCAP preprocessing failed."]
    parts.append(f"Command: {' '.join(command)}")
    if stderr:
        parts.append(f"stderr:\n{stderr}")
    if stdout:
        parts.append(f"stdout:\n{stdout}")
    return "\n\n".join(parts)


@tool("network_traffic_prepare", parse_docstring=True)
def network_traffic_prepare_tool(
    references: list[str],
    dataset_name: str | None = None,
    output_dir: str | None = None,
) -> str:
    """Preprocess one or more PCAP files on the host into standardized packet.csv and flow.csv datasets.

    Use this tool when the user provides `.pcap` or `.pcapng` files and wants analysis. The tool runs
    a host-side preprocessing script that uses `scapy` to extract packet-level records and then aggregates
    them into a normalized `flow.csv` that works with `network_traffic_analyze`.

    The input references can be:
    - Exact filenames like `capture-a.pcap` or `Geodo.pcap`
    - Dataset-relative suffixes like `corp/day-1/capture-a.pcap`
    - Full repo-relative paths such as `datasets/network-traffic/raw/corp/day-1/capture-a.pcap`
    - Explicit absolute host paths

    Multiple files are supported in one call. They will be merged into one prepared dataset.

    Args:
        references: One or more PCAP references to preprocess together. Bare filenames are valid and should be tried before asking the user for a path.
        dataset_name: Optional output dataset directory name under `datasets/network-traffic/processed`.
        output_dir: Optional explicit output directory. If omitted, output goes to `datasets/network-traffic/processed/<dataset_name>`.
    """
    if not references:
        return "Network traffic PCAP preprocessing failed.\n\nNo PCAP references were provided."

    command = [sys.executable, str(PREPARE_SCRIPT), "--files", *references, "--format", "json"]
    if dataset_name not in (None, ""):
        command.extend(["--dataset-name", dataset_name])
    if output_dir not in (None, ""):
        command.extend(["--output-dir", output_dir])
    return _run_prepare(command)
