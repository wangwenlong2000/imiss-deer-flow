"""Explicit, idempotent initial catalogue bootstrap helpers."""

from __future__ import annotations

from uuid import uuid4

from werkzeug.security import generate_password_hash

from onecityagent.application.database import DB
from onecityagent.constants import StatusEnum, UserTenantRole
from onecityagent.models.db_models import Role, Tenant, User, UserTenant
from onecityagent.utils.common_utils import datetime_format


def seed_initial_data(
    database=None,
    *,
    admin_email: str,
    admin_password: str,
    admin_nickname: str = "Administrator",
) -> dict[str, str | bool]:
    """Ensure common/admin roles and create a first administrator if absent.

    This is an explicit offline/bootstrap operation; it is never called by an
    application factory or a request. It does not reset or rotate existing
    administrator credentials.
    """
    if database is not None:
        DB.configure(database)
    database = DB.get_instance()
    if not admin_email.strip() or not admin_password:
        raise ValueError("admin_email and admin_password are required")

    with database.atomic():
        common = Role.get_or_none(Role.code == "common")
        if common is None:
            common = Role.create(
                id=uuid4().hex,
                name="普通角色",
                code="common",
                sort=2,
                data_scope=1,
                data_scope_dept_ids="",
                status=int(StatusEnum.VALID),
                type=1,
                remark="OneCity ordinary account",
            )

        admin = Role.get_or_none(Role.code == "admin")
        if admin is None:
            admin = Role.create(
                id=uuid4().hex,
                name="管理员",
                code="admin",
                sort=1,
                data_scope=1,
                data_scope_dept_ids="",
                status=int(StatusEnum.VALID),
                type=1,
                remark="OneCity bootstrap administrator",
            )

        existing = (
            User.select()
            .join(Role, on=(User.role == Role.id))
            .where(
                (Role.code == "admin")
                & (User.status == str(int(StatusEnum.VALID)))
                & (User.is_active == "1")
            )
            .first()
        )
        if existing is not None:
            return {
                "created": False,
                "admin_user_id": str(existing.id),
                "common_role_id": str(common.id),
                "admin_role_id": str(admin.id),
            }

        now = datetime_format(__import__("datetime").datetime.now())
        user_id = uuid4().hex
        user = User.create(
            id=user_id,
            access_token=None,
            nickname=admin_nickname.strip() or "Administrator",
            password=generate_password_hash(admin_password),
            email=admin_email.strip(),
            avatar="",
            language="Chinese",
            color_schema="Bright",
            timezone="UTC+8\tAsia/Shanghai",
            last_login_time=None,
            is_authenticated="1",
            is_active="1",
            is_anonymous="0",
            login_channel="password",
            status=str(int(StatusEnum.VALID)),
            role=admin.id,
            is_superuser=True,
        )
        tenant_id = uuid4().hex
        Tenant.create(
            id=tenant_id,
            name=f"{user.nickname}'s Workspace",
            public_key=None,
            llm_id="",
            embd_id="",
            asr_id="",
            img2txt_id="",
            rerank_id="",
            tts_id="",
            parser_ids="",
            credit=0,
            status=str(int(StatusEnum.VALID)),
            create_date=now,
            update_date=now,
        )
        UserTenant.create(
            id=uuid4().hex,
            user_id=user_id,
            tenant_id=tenant_id,
            role=int(UserTenantRole.OWNER),
            invited_by=user_id,
            status=str(int(StatusEnum.VALID)),
        )
        return {
            "created": True,
            "admin_user_id": user_id,
            "workspace_id": tenant_id,
            "common_role_id": str(common.id),
            "admin_role_id": str(admin.id),
        }
