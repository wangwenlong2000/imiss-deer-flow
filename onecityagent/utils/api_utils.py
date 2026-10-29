# -*- coding: utf-8 -*-
# API utilities for OneCity Agent

import logging
import functools
from typing import Any, Dict, List, Optional, Union, Callable

from flask import jsonify, request

from onecityagent.constants import RetCode


def get_json_result(data: Any = None, code: int = RetCode.SUCCESS, message: str = "success") -> Dict:
    """
    构造统一的JSON响应格式，并根据业务code设置HTTP状态码。
    规则：
    - 当code为RetCode.SUCCESS(0)时，HTTP状态码为200。
    - 其他情况使用code对应的HTTP语义码（400/401/403/404/409/500）。
    """
    response = jsonify({
        "code": int(code),
        "data": data,
        "message": message
    })
    # 将业务code映射为HTTP状态码
    response.status_code = 200 if int(code) == int(RetCode.SUCCESS) else int(code)
    return response


def construct_response(data: Any = None, auth: str = None, message: str = "success", code: int = RetCode.SUCCESS) -> Dict:
    """
    构造带认证信息的响应
    """
    response = get_json_result(data, code, message)
    if auth:
        response.headers["Authorization"] = auth
    return response


def server_error_response(error: Exception) -> Dict:
    """
    构造服务器错误响应
    """
    logging.exception(error)
    return get_json_result(
        data=False,
        code=RetCode.SERVER_ERROR,
        message=str(error)
    )


def get_data_error_result(message: str = "Invalid data") -> Dict:
    """
    构造数据错误响应
    """
    return get_json_result(
        data=False,
        code=RetCode.PARAMS_ERROR,
        message=message
    )


def validate_request(*required_fields: str) -> Callable:
    """
    请求参数验证装饰器
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if not request.json:
                return get_json_result(
                    data=False,
                    code=RetCode.PARAMS_ERROR,
                    message="Missing JSON data"
                )
            
            for field in required_fields:
                if field not in request.json or not request.json[field]:
                    return get_json_result(
                        data=False,
                        code=RetCode.PARAMS_ERROR,
                        message=f"Missing required field: {field}"
                    )
            
            return func(*args, **kwargs)
        return wrapper
    return decorator