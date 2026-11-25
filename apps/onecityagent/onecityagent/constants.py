# -*- coding: utf-8 -*-
# Constants for OneCity Agent

from enum import Enum, IntEnum


class SerializedType(Enum):
    PICKLE = 'pickle'
    JSON = 'json'


class StatusEnum(IntEnum):
    INVALID = 0
    VALID = 1


class UserRole(Enum):
    ADMIN = 'admin'
    USER = 'user'


class UserTenantRole(IntEnum):
    NORMAL = 0
    OWNER = 1
    INVITE = 2


class RetCode(IntEnum):
    SUCCESS = 0
    PARAMS_ERROR = 400
    AUTHENTICATION_ERROR = 401
    FORBIDDEN = 403
    NOT_FOUND = 404
    OPERATING_ERROR = 409
    SERVER_ERROR = 500