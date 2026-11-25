# -*- coding: utf-8 -*-
# Role Controller for OneCity Agent

import logging
from flask import request, Blueprint
from flask_login import login_required, current_user
from flasgger import swag_from

from onecityagent.service.role_service import RoleService
from onecityagent.utils.api_utils import (
    get_json_result,
    server_error_response,
    construct_response
)
from onecityagent.constants import RetCode

# 创建蓝图
role_bp = Blueprint('role', __name__, url_prefix='/role')


@role_bp.route("/list", methods=["GET"])
@login_required
@swag_from({
    "tags": ["Role"],
    "summary": "获取角色列表",
    "description": "获取系统中所有可用的角色列表",
    "responses": {
        "200": {
            "description": "获取成功",
            "schema": {
                "type": "object",
                "properties": {
                    "code": {
                        "type": "integer",
                        "example": 0
                    },
                    "data": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "role_id": {
                                    "type": "string",
                                    "description": "角色ID"
                                },
                                "name": {
                                    "type": "string",
                                    "description": "角色名称"
                                },
                                "code": {
                                    "type": "string",
                                    "description": "角色代码"
                                },
                                "sort": {
                                    "type": "integer",
                                    "description": "排序"
                                },
                                "status": {
                                    "type": "integer",
                                    "description": "状态"
                                },
                                "type": {
                                    "type": "integer",
                                    "description": "角色类型"
                                },
                                "data_scope": {
                                    "type": "integer",
                                    "description": "数据范围"
                                },
                                "data_scope_dept_ids": {
                                    "type": "string",
                                    "description": "数据范围部门IDs"
                                },
                                "remark": {
                                    "type": "string",
                                    "description": "备注"
                                },
                                "create_time": {
                                    "type": "integer",
                                    "description": "创建时间"
                                },
                                "create_date": {
                                    "type": "string",
                                    "description": "创建日期"
                                },
                                "update_time": {
                                    "type": "integer",
                                    "description": "更新时间"
                                },
                                "update_date": {
                                    "type": "string",
                                    "description": "更新日期"
                                }
                            }
                        }
                    },
                    "message": {
                        "type": "string",
                        "example": "success"
                    }
                }
            }
        },
        "401": {
            "description": "未授权",
            "schema": {
                "type": "object",
                "properties": {
                    "code": {
                        "type": "integer",
                        "example": 401
                    },
                    "message": {
                        "type": "string",
                        "example": "Unauthorized"
                    }
                }
            }
        },
        "500": {
            "description": "服务器错误",
            "schema": {
                "type": "object",
                "properties": {
                    "code": {
                        "type": "integer",
                        "example": 500
                    },
                    "message": {
                        "type": "string",
                        "example": "Internal Server Error"
                    }
                }
            }
        }
    }
})
def role_list():
    """
    获取角色列表
    """
    try:
        role_list = RoleService.get_role_list()
        return get_json_result(data=role_list)
    except Exception as e:
        return server_error_response(e)


@role_bp.route("/user_role_email", methods=["GET"])
@swag_from({
    "tags": ["Role"],
    "summary": "根据邮箱获取用户角色",
    "description": "根据用户邮箱获取用户的角色信息",
    "parameters": [
        {
            "name": "email",
            "in": "query",
            "type": "string",
            "required": True,
            "description": "用户邮箱"
        }
    ],
    "responses": {
        "200": {
            "description": "获取成功",
            "schema": {
                "type": "object",
                "properties": {
                    "code": {
                        "type": "integer",
                        "example": 0
                    },
                    "data": {
                        "type": "object",
                        "properties": {
                            "role_id": {
                                "type": "string",
                                "description": "角色ID"
                            },
                            "name": {
                                "type": "string",
                                "description": "角色名称"
                            },
                            "code": {
                                "type": "string",
                                "description": "角色代码"
                            },
                            "sort": {
                                "type": "integer",
                                "description": "排序"
                            },
                            "status": {
                                "type": "integer",
                                "description": "状态"
                            },
                            "type": {
                                "type": "integer",
                                "description": "角色类型"
                            },
                            "data_scope": {
                                "type": "integer",
                                "description": "数据范围"
                            },
                            "data_scope_dept_ids": {
                                "type": "string",
                                "description": "数据范围部门IDs"
                            },
                            "remark": {
                                "type": "string",
                                "description": "备注"
                            }
                        }
                    },
                    "message": {
                        "type": "string",
                        "example": "success"
                    }
                }
            }
        },
        "400": {
            "description": "参数错误",
            "schema": {
                "type": "object",
                "properties": {
                    "code": {
                        "type": "integer",
                        "example": 400
                    },
                    "message": {
                        "type": "string",
                        "example": "Email is required"
                    }
                }
            }
        },
        "404": {
            "description": "用户角色不存在",
            "schema": {
                "type": "object",
                "properties": {
                    "code": {
                        "type": "integer",
                        "example": 404
                    },
                    "message": {
                        "type": "string",
                        "example": "User role not found"
                    }
                }
            }
        },
        "500": {
            "description": "服务器错误",
            "schema": {
                "type": "object",
                "properties": {
                    "code": {
                        "type": "integer",
                        "example": 500
                    },
                    "message": {
                        "type": "string",
                        "example": "Internal Server Error"
                    }
                }
            }
        }
    }
})
def get_user_role():
    """
    根据邮箱获取用户角色
    """
    try:
        email = request.args.get("email")
        if not email:
            return get_json_result(
                data=None,
                code=RetCode.PARAMS_ERROR,
                message="Email is required"
            )
        
        user_role = RoleService.get_user_role_by_email(email)
        if not user_role:
            return get_json_result(
                data=None,
                code=RetCode.NOT_FOUND,
                message="User role not found"
            )
        
        return get_json_result(data=user_role)
    except Exception as e:
        return server_error_response(e)


@role_bp.route("/user_role", methods=["GET"])
@login_required
@swag_from({
    "tags": ["Role"],
    "summary": "获取当前用户角色",
    "description": "根据当前用户的访问令牌获取用户的角色信息",
    "responses": {
        "200": {
            "description": "获取成功",
            "schema": {
                "type": "object",
                "properties": {
                    "code": {
                        "type": "integer",
                        "example": 0
                    },
                    "data": {
                        "type": "object",
                        "properties": {
                            "role_id": {
                                "type": "string",
                                "description": "角色ID"
                            },
                            "name": {
                                "type": "string",
                                "description": "角色名称"
                            },
                            "code": {
                                "type": "string",
                                "description": "角色代码"
                            },
                            "sort": {
                                "type": "integer",
                                "description": "排序"
                            },
                            "status": {
                                "type": "integer",
                                "description": "状态"
                            },
                            "type": {
                                "type": "integer",
                                "description": "角色类型"
                            },
                            "data_scope": {
                                "type": "integer",
                                "description": "数据范围"
                            },
                            "data_scope_dept_ids": {
                                "type": "string",
                                "description": "数据范围部门IDs"
                            },
                            "remark": {
                                "type": "string",
                                "description": "备注"
                            }
                        }
                    },
                    "message": {
                        "type": "string",
                        "example": "success"
                    }
                }
            }
        },
        "401": {
            "description": "未授权",
            "schema": {
                "type": "object",
                "properties": {
                    "code": {
                        "type": "integer",
                        "example": 401
                    },
                    "message": {
                        "type": "string",
                        "example": "Unauthorized"
                    }
                }
            }
        },
        "404": {
            "description": "用户角色不存在",
            "schema": {
                "type": "object",
                "properties": {
                    "code": {
                        "type": "integer",
                        "example": 404
                    },
                    "message": {
                        "type": "string",
                        "example": "User role not found"
                    }
                }
            }
        },
        "500": {
            "description": "服务器错误",
            "schema": {
                "type": "object",
                "properties": {
                    "code": {
                        "type": "integer",
                        "example": 500
                    },
                    "message": {
                        "type": "string",
                        "example": "Internal Server Error"
                    }
                }
            }
        }
    }
})
def get_user_role_by_token():
    """
    获取当前用户角色
    """
    try:
        access_token = current_user.access_token
        user_role = RoleService.get_user_role_by_token(access_token)
        if not user_role:
            return get_json_result(
                data=None,
                code=RetCode.NOT_FOUND,
                message="User role not found"
            )
        
        return get_json_result(data=user_role)
    except Exception as e:
        return server_error_response(e)