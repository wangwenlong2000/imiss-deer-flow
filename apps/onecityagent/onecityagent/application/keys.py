"""Public login key derived from the site's configured private key."""

from __future__ import annotations

from hashlib import sha256
from pathlib import Path

from Cryptodome.PublicKey import RSA

from onecityagent.application.settings import runtime_value


def login_public_key() -> dict[str, str]:
    """Expose only the public half; never retain or serialize private material."""
    path = runtime_value("ONECITY_RSA_PRIVATE_KEY_PATH", "PRIVATE_KEY_PATH")
    if not path:
        raise RuntimeError("Login key is not configured")
    key = RSA.import_key(
        Path(path).read_bytes(),
        passphrase=runtime_value(
            "ONECITY_RSA_PRIVATE_KEY_PASSPHRASE", "PRIVATE_KEY_PASSPHRASE", ""
        ),
    )
    if not key.has_private() or key.size_in_bits() < 2048:
        raise RuntimeError("Login key must be an RSA private key of at least 2048 bits")
    public = key.publickey()
    return {
        "public_key": public.export_key(format="PEM").decode("ascii"),
        "key_id": sha256(public.export_key(format="DER")).hexdigest(),
        "algorithm": "RSA-OAEP-256",
    }
