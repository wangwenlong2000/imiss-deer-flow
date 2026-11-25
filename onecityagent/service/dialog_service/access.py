"""Authoritative access checks shared by the platform's dialog APIs."""

from onecityagent.constants import StatusEnum
from onecityagent.models.db_models import DB, Dialog, Tenant, UserTenant


def owned_dialog_ids(user_id):
    """Return active dialogs owned by a user in their current memberships."""
    with DB.connection_context():
        memberships = (
            UserTenant.select(UserTenant.tenant_id)
            .join(Tenant, on=(Tenant.id == UserTenant.tenant_id))
            .where(
                (UserTenant.user_id == str(user_id))
                & (UserTenant.status == StatusEnum.VALID.value)
                & (Tenant.status == StatusEnum.VALID.value)
            )
        )
        return [
            str(row.id)
            for row in Dialog.select(Dialog.id).where(
                (Dialog.creator_id == str(user_id))
                & (Dialog.status == StatusEnum.VALID.value)
                & Dialog.tenant_id.in_(memberships)
            )
        ]


def owns_dialog(user_id, dialog_id):
    """Check exact ownership, tenant membership and active resource status."""
    if not dialog_id:
        return False
    with DB.connection_context():
        dialog = Dialog.get_or_none(
            (Dialog.id == str(dialog_id))
            & (Dialog.creator_id == str(user_id))
            & (Dialog.status == StatusEnum.VALID.value)
        )
        if dialog is None:
            return False
        return (
            UserTenant.select(UserTenant.id)
            .join(Tenant, on=(Tenant.id == UserTenant.tenant_id))
            .where(
                (UserTenant.user_id == str(user_id))
                & (UserTenant.tenant_id == str(dialog.tenant_id))
                & (UserTenant.status == StatusEnum.VALID.value)
                & (Tenant.status == StatusEnum.VALID.value)
            )
            .exists()
        )
