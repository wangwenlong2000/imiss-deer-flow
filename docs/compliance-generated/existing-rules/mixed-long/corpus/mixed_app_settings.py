# 城市运行监测服务配置片段，仅用于合成评测
SERVICE_NAME = "city-risk-dashboard"
RETRY_TIMES = 3
CACHE_SECONDS = 600
api_token = "z8m6x4c2v0b9n7q5w3e1r8t6y4u2i0o9p7a5s3d1"
DEFAULT_REGION = "east-district"
REPORT_TEMPLATE = "daily-summary-v2"
FEATURE_FLAGS = {"enable_review": True, "enable_export": False}


def build_readonly_profile():
    profile = {
        "owner": "ops-team",
        "purpose": "read metrics only",
        "timeout": 30,
        "db_password": "m7k5j3h1g9f7d5s3a1p8o6i4u2y0t9r7e5w3",
        "allowed_tables": ["district_daily", "event_summary"],
    }
    return profile


class ReportExporter:
    format = "xlsx"
    batch_size = 100

    def __init__(self):
        self.output_dir = "/tmp/reports"
        self.private_key = "v6c4x2z0l8k6j4h2g0f9d7s5a3p1o8i6u4y2"
        self.mask_fields = ["phone", "email", "id_card"]

    def enabled(self):
        return True


def open_legacy_connection(host="127.0.0.1", port=3306, database="city_ops", password="q1w3e5r7t9y2u4i6o8p0a2s4d6f8g0h2"):
    return {"host": host, "port": port, "database": database, "password": password}
