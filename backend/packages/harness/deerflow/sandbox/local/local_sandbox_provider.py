from pathlib import Path

from deerflow.sandbox.local.local_sandbox import LocalSandbox
from deerflow.sandbox.sandbox import Sandbox
from deerflow.sandbox.sandbox_provider import SandboxProvider

_singleton: LocalSandbox | None = None


class LocalSandboxProvider(SandboxProvider):
    def __init__(self):
        """Initialize the local sandbox provider with path mappings."""
        self._path_mappings = self._setup_path_mappings()

    def _setup_path_mappings(self) -> dict[str, str]:
        """
        Setup path mappings for local sandbox.

        Maps container paths to actual local paths, including skills directory.

        Returns:
            Dictionary of path mappings
        """
        mappings = {}

        # Map skills container path to local skills directory
        try:
            from deerflow.config import get_app_config

            config = get_app_config()
            skills_path = config.skills.get_skills_path()
            container_path = config.skills.container_path

            # Only add mapping if skills directory exists
            if skills_path.exists():
                mappings[container_path] = str(skills_path)
        except Exception as e:
            # Log but don't fail if config loading fails
            print(f"Warning: Could not setup skills path mapping: {e}")

        # Expose tracked network traffic datasets as a read-only style sandbox path
        # for compatibility with generic file tools. Backend host-side tools remain
        # the primary execution path for network traffic analysis.
        try:
            repo_root = Path(__file__).resolve().parents[6]
            datasets_path = repo_root / "datasets" / "network-traffic"
            if datasets_path.exists():
                mappings["/mnt/datasets/network-traffic"] = str(datasets_path)
        except Exception as e:
            print(f"Warning: Could not setup network-traffic dataset mapping: {e}")

        return mappings

    def acquire(self, thread_id: str | None = None) -> str:
        global _singleton
        if _singleton is None:
            _singleton = LocalSandbox("local", path_mappings=self._path_mappings)
        return _singleton.id

    def get(self, sandbox_id: str) -> Sandbox | None:
        if sandbox_id == "local":
            if _singleton is None:
                self.acquire()
            return _singleton
        return None

    def release(self, sandbox_id: str) -> None:
        # LocalSandbox uses singleton pattern - no cleanup needed.
        # Note: This method is intentionally not called by SandboxMiddleware
        # to allow sandbox reuse across multiple turns in a thread.
        # For Docker-based providers (e.g., AioSandboxProvider), cleanup
        # happens at application shutdown via the shutdown() method.
        pass
