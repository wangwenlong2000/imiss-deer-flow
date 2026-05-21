import logging

logger = logging.getLogger("audit")
request_id = "req-20260520-001"
user_role = "reviewer"

logger.info("daily export started, request_id=%s", request_id)
logger.warning("password %s", "d4f6g8h0j2k4l6z8x0c2v4b6n8m1q3w5")
logger.info("daily export finished, role=%s", user_role)
