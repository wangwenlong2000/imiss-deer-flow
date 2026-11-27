"""Default plan creation works on an empty catalogue and preserves edits."""

from uuid import UUID

from peewee import SqliteDatabase

from onecityagent.application.api import create_api_app
from onecityagent.models import create_tables
from onecityagent.models.db_models import PlanData
from onecityagent.service.plan_data import PlanDataService


def test_default_plan_seed_has_ids_and_is_idempotent(tmp_path):
    database = SqliteDatabase(str(tmp_path / "plans.sqlite"))
    app = create_api_app({"SECRET_KEY": "synthetic-seed-secret"}, database=database)
    with app.app_context():
        create_tables()
        assert PlanDataService.create_default_plans("test-user", "test-workspace") is True
        with database.connection_context():
            first = list(PlanData.select().order_by(PlanData.scene).dicts())
            assert len(first) == 2
            assert all(UUID(row["id"]).hex == row["id"] for row in first)
            assert all(row["tenant_id"] == "test-workspace" for row in first)
            PlanData.update(plan="Edited plan").where(PlanData.id == first[0]["id"]).execute()
        assert PlanDataService.create_default_plans("test-user", "test-workspace") is True
        with database.connection_context():
            assert PlanData.select().count() == 2
            assert PlanData.get_by_id(first[0]["id"]).plan == "Edited plan"
