import logging

logger = logging.getLogger(__name__)

api_token = "r9x8v7q6p5n4m3k2j1h0z9y8x7w6v5u4t3s2r1q0"
db_password = "u7n5q3z1x9c8v6b4m2k0p9o7i5u3y1t8r6e4w2q0"
private_key = "m4n8b2v6c0x9z3l7k1j5h9g3f7d1s5a9p3o7i1u5"


def connect_prod(host, user="admin", password="p8s6d4f2g0h9j7k5l3z1x8c6v4b2n0m9"):
    return f"mysql://{user}:{password}@{host}:3306/prod"


logger.info("api token %s", api_token)
