"""Authoritative account checks shared by the platform and its IAM bridge."""

from onecityagent.application.settings import setting

from onecityagent.constants import StatusEnum
from onecityagent.models.db_models import DB, Role, User


def token_from_header(value):
    if not isinstance(value, str):
        return None
    value = value.strip()
    if value.lower() == "bearer":
        return None
    if value.lower().startswith("bearer "):
        value = value[7:].strip()
    if not value or any(character.isspace() for character in value):
        return None
    return value


def load_active_user(token):
    if not token:
        return None
    with DB.connection_context():
        return User.get_or_none(
            (User.access_token == token)
            & (User.status == str(StatusEnum.VALID.value))
            & (User.is_active == "1")
            & (User.is_authenticated == "1")
        )


def revoke_login_token(user_id, token):
    """Do not revoke a newer login that raced with this session's logout."""
    with DB.connection_context():
        User.update(access_token=None).where(
            (User.id == user_id) & (User.access_token == token)
        ).execute()


def self_registration_role_id():
    """Ignore the browser's requested role; resolve an ordinary role server-side."""
    code = setting("ONECITY_SELF_REGISTER_ROLE_CODE", "common").strip()
    if not code or code.lower() in {"admin", "super_admin", "superadmin"}:
        raise ValueError("Self-registration requires an ordinary role code")
    with DB.connection_context():
        role = Role.get_or_none(
            (Role.code == code) & (Role.status == StatusEnum.VALID.value)
        )
    if role is None:
        raise ValueError("The self-registration role is not configured")
    return str(role.id)
