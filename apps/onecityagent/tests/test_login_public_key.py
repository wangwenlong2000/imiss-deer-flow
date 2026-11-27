"""New sites can log in using their own key without rebuilding the web app."""

from __future__ import annotations

import base64
from hashlib import sha256

from Cryptodome.Cipher import PKCS1_OAEP
from Cryptodome.Hash import SHA256
from Cryptodome.PublicKey import RSA
from peewee import SqliteDatabase
import pytest
from werkzeug.security import check_password_hash

from onecityagent.application.api import create_api_app
from onecityagent.application.bootstrap import seed_initial_data
from onecityagent.models import create_tables
from onecityagent.models.db_models import User


@pytest.fixture
def key_site(tmp_path, monkeypatch):
    key = RSA.generate(2048)
    private_path = tmp_path / "private.pem"
    private_path.write_bytes(key.export_key())
    private_path.chmod(0o600)
    database = SqliteDatabase(str(tmp_path / "catalogue.sqlite"), autoconnect=False)
    app = create_api_app(
        {
            "SECRET_KEY": "synthetic-test-secret",
            "ONECITY_RSA_PRIVATE_KEY_PATH": str(private_path),
            "ONECITY_RSA_PRIVATE_KEY_PASSPHRASE": "",
        },
        database=database,
    )
    with app.app_context():
        create_tables()
        with database.connection_context():
            seed_initial_data(
                admin_email="admin@example.test", admin_password="synthetic-password"
            )
    from onecityagent.service.plan_data import PlanDataService

    monkeypatch.setattr(
        PlanDataService, "create_default_plans", classmethod(lambda cls, *_args: None)
    )
    yield app, app.test_client(), database, private_path
    if not database.is_closed():
        database.close()


def encrypted_password(document, text):
    public = RSA.import_key(document["public_key"])
    return base64.b64encode(
        PKCS1_OAEP.new(public, hashAlgo=SHA256).encrypt(text.encode("utf-8"))
    ).decode("ascii")


def test_public_key_is_site_specific_public_only_and_not_cached(key_site):
    _app, client, _database, path = key_site
    response = client.get("/v1/user/public-key")
    assert response.status_code == 200
    assert response.headers["Cache-Control"] == "no-store"
    data = response.json["data"]
    public = RSA.import_key(data["public_key"])
    assert public.has_private() is False
    assert public == RSA.import_key(path.read_bytes()).publickey()
    assert data["key_id"] == sha256(public.export_key(format="DER")).hexdigest()
    assert data["algorithm"] == "RSA-OAEP-256"
    assert set(data) == {"public_key", "key_id", "algorithm"}


def test_login_and_registration_use_real_oaep_sha256_with_current_site_key(key_site):
    _app, client, database, _path = key_site
    key = client.get("/v1/user/public-key").json["data"]
    login = client.post(
        "/v1/user/login",
        json={"email": "admin@example.test", "password": encrypted_password(key, "synthetic-password")},
    )
    assert login.status_code == 200 and login.json["code"] == 0
    assert login.headers["Authorization"]
    registration = client.post(
        "/v1/user/register",
        json={
            "email": "new@example.test",
            "nickname": "Test",
            "password": encrypted_password(key, "测试-Synthetic-Pass1!"),
        },
    )
    assert registration.status_code == 200 and registration.json["code"] == 0
    with database.connection_context():
        user = User.get(User.email == "new@example.test")
        assert check_password_hash(user.password, "测试-Synthetic-Pass1!")


def test_key_rotation_is_visible_without_process_restart(key_site):
    _app, client, _database, path = key_site
    old = client.get("/v1/user/public-key").json["data"]
    path.write_bytes(RSA.generate(2048).export_key())
    new = client.get("/v1/user/public-key").json["data"]
    assert old["key_id"] != new["key_id"]
    assert client.post(
        "/v1/user/login",
        json={"email": "admin@example.test", "password": encrypted_password(new, "synthetic-password")},
    ).status_code == 200
    assert client.post(
        "/v1/user/login",
        json={"email": "admin@example.test", "password": encrypted_password(old, "synthetic-password")},
    ).status_code != 200


def test_password_change_encrypts_both_credentials_with_current_key(key_site):
    _app, client, database, _path = key_site
    key = client.get("/v1/user/public-key").json["data"]
    client.post(
        "/v1/user/login",
        json={"email": "admin@example.test", "password": encrypted_password(key, "synthetic-password")},
    )
    change = client.post(
        "/v1/user/setting",
        json={
            "password": encrypted_password(key, "synthetic-password"),
            "new_password": encrypted_password(key, "New-Synthetic-Password!"),
        },
    )
    assert change.status_code == 200 and change.json["code"] == 0
    with database.connection_context():
        assert check_password_hash(
            User.get(User.email == "admin@example.test").password,
            "New-Synthetic-Password!",
        )
    rejected = client.post(
        "/v1/user/setting",
        json={"password": encrypted_password(key, "New-Synthetic-Password!"), "new_password": "bad!"},
    )
    assert rejected.status_code == 400
    with database.connection_context():
        assert check_password_hash(
            User.get(User.email == "admin@example.test").password,
            "New-Synthetic-Password!",
        )


def test_encrypted_private_key_passphrase_is_honored(key_site):
    app, client, _database, path = key_site
    key = RSA.import_key(path.read_bytes())
    path.write_bytes(key.export_key(
        format="PEM", pkcs=8, passphrase="synthetic-key-passphrase",
        protection="PBKDF2WithHMAC-SHA512AndAES256-CBC",
    ))
    app.extensions["onecity.settings"]["ONECITY_RSA_PRIVATE_KEY_PASSPHRASE"] = "synthetic-key-passphrase"
    response = client.get("/v1/user/public-key")
    assert response.status_code == 200
    assert RSA.import_key(response.json["data"]["public_key"]) == key.publickey()


def test_undersized_site_key_is_unavailable(key_site):
    _app, client, _database, path = key_site
    path.write_bytes(RSA.generate(1024).export_key())
    assert client.get("/v1/user/public-key").status_code == 503


@pytest.mark.parametrize("ciphertext", ["not-base64!", "", None])
def test_invalid_registration_ciphertext_never_creates_empty_password_user(key_site, ciphertext):
    _app, client, database, _path = key_site
    response = client.post(
        "/v1/user/register",
        json={"email": "invalid@example.test", "nickname": "Test", "password": ciphertext},
    )
    assert response.status_code != 200
    with database.connection_context():
        assert User.get_or_none(User.email == "invalid@example.test") is None


def test_public_key_configuration_error_is_sanitized(key_site):
    _app, client, _database, path = key_site
    path.unlink()
    response = client.get("/v1/user/public-key")
    assert response.status_code == 503
    assert str(path) not in response.get_data(as_text=True)
    assert response.json["data"] is None
