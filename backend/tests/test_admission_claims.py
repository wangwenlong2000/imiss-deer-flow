from __future__ import annotations

from app.admission_claims import build_admission_metadata, verify_admission_metadata


def _claims(monkeypatch, *, user_id="user-a", org_id="org-a", identity_class="ordinary_user", task_type="analysis"):
    monkeypatch.setenv("DEER_FLOW_ADMISSION_SIGNING_KEY", "test-signing-key-with-at-least-32-bytes")
    return build_admission_metadata(
        user_id=user_id,
        org_id=org_id,
        identity_class=identity_class,
        task_type=task_type,
        thread_id="thread-1",
        now=1_800_000_000,
    )


def test_signed_claim_uses_compliance_identity_taxonomy_and_binds_thread(monkeypatch):
    metadata = _claims(monkeypatch)

    assert verify_admission_metadata(metadata, expected_thread_id="thread-1", now=1_800_000_001) == {
        "user_key": metadata["deerflow_admission"]["user_key"],
        "identity_class": "ordinary_user",
        "task_type": "analysis",
    }
    assert verify_admission_metadata(metadata, expected_thread_id="another-thread", now=1_800_000_001) is None
    assert verify_admission_metadata(metadata, expected_thread_id=None, now=1_800_000_001) is None


def test_claim_signature_prevents_identity_and_task_type_tampering(monkeypatch):
    metadata = _claims(monkeypatch)
    metadata["deerflow_admission"]["identity_class"] = "publisher"
    assert verify_admission_metadata(metadata, expected_thread_id="thread-1", now=1_800_000_001) is None


def test_user_key_is_tenant_scoped_and_does_not_expose_user_id(monkeypatch):
    first = _claims(monkeypatch, user_id="same-id", org_id="org-a")
    second = _claims(monkeypatch, user_id="same-id", org_id="org-b")

    assert first["deerflow_admission"]["user_key"] != second["deerflow_admission"]["user_key"]
    assert "same-id" not in str(first)


def test_admission_preserves_verified_identity_bytes_like_resource_ownership(monkeypatch):
    first = _claims(monkeypatch, user_id="user-a", org_id="org-a")
    user_with_spaces = _claims(monkeypatch, user_id=" user-a ", org_id="org-a")
    org_with_spaces = _claims(monkeypatch, user_id="user-a", org_id=" org-a ")
    keys = {claim["deerflow_admission"]["user_key"] for claim in (first, user_with_spaces, org_with_spaces)}
    assert len(keys) == 3


def test_expired_claim_is_rejected_for_new_runs_but_countable_while_active(monkeypatch):
    metadata = _claims(monkeypatch)

    assert verify_admission_metadata(metadata, expected_thread_id="thread-1", now=1_800_000_400) is None
    assert verify_admission_metadata(
        metadata,
        expected_thread_id="thread-1",
        now=1_800_000_400,
        allow_expired=True,
    ) is not None
