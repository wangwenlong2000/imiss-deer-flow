# -*- coding: utf-8 -*-
# User Controller for OneCity Agent

import logging
import json
from datetime import datetime

from onecityagent.logger import logger

from flask import request, session, redirect, Blueprint
from werkzeug.security import generate_password_hash, check_password_hash
from flask_login import login_required, current_user, login_user, logout_user


from onecityagent.service.user_service import UserService, TenantService, UserTenantService
from onecityagent.service.plan_data import PlanDataService
from onecityagent.models.db_models import DB, UserTenant
from onecityagent.service.authentication import revoke_login_token, self_registration_role_id
from onecityagent.utils.api_utils import (
    get_json_result,
    validate_request,
    server_error_response,
    get_data_error_result,
    construct_response
)
from onecityagent.utils.common_utils import (
    get_uuid,
    get_format_time,
    decrypt,
    current_timestamp,
    datetime_format,
)
from onecityagent.constants import RetCode, StatusEnum, UserTenantRole

# 使用服务类
# 注意：这些服务的方法都是类方法，不需要创建实例
# user_service = UserService()
# tenant_service = TenantService()
# user_tenant_service = UserTenantService()

# 创建蓝图
user_bp = Blueprint('user', __name__, url_prefix='/user')
tenant_bp = Blueprint('tenant', __name__, url_prefix='/tenant')


@user_bp.get("/public-key")
def public_key():
    """Return this site's public login key without requiring a login."""
    from onecityagent.application.keys import login_public_key

    try:
        response = get_json_result(data=login_public_key())
    except (OSError, ValueError, TypeError, RuntimeError):
        logger.error("OneCity login public key is unavailable")
        response = get_json_result(
            data=None, code=RetCode.SERVER_ERROR, message="登录加密配置不可用"
        )
        response.status_code = 503
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    return response


@user_bp.route("/login", methods=["GET", "POST"])
def login():
    """
    用户登录接口
    ---
    tags:
      - User
    parameters:
      - in: body
        name: body
        description: 登录凭证
        required: true
        schema:
          type: object
          properties:
            email:
              type: string
              description: 用户邮箱
            password:
              type: string
              description: 用户密码
    responses:
      200:
        description: 登录成功
        schema:
          type: object
      401:
        description: 认证失败
        schema:
          type: object
    """
    # 处理GET请求（重定向到登录页面时）
    if request.method == "GET":
        return get_json_result(
            data=False,
            code=RetCode.AUTHENTICATION_ERROR,
            message="请提供邮箱和密码进行登录"
        )

    if not request.json:
        return get_json_result(
            data=False, code=RetCode.AUTHENTICATION_ERROR, message="未授权访问！"
        )

    email = request.json.get("email", "")
    users = UserService.query(email=email)
    if not users:
        return get_json_result(
            data=False,
            code=RetCode.AUTHENTICATION_ERROR,
            message=f"邮箱或密码不正确",
        )

    password = request.json.get("password")
    try:
        password = decrypt(password)
    except Exception:
        return get_json_result(
            data=False, code=RetCode.SERVER_ERROR, message="密码解密失败"
        )

    user = UserService.query_user(email, password)
    if user and str(user.is_active) == "1" and str(user.is_authenticated) == "1":
        user.access_token = get_uuid()
        user.update_date = datetime_format(datetime.now())
        with DB.connection_context():
            user.save()
        login_user(user)
        response_data = dict(user.to_json())
        response_data.pop("password", None)
        msg = "登录成功！"

        # 获取用户角色信息
        role = UserService.get_user_role(user.role)
        if role:
            response_data["role"] = {
                "role_id": role.id,
                "role_name": role.name,
                "role_code": role.code
            }

        return construct_response(data=response_data, auth=user.get_id(), message=msg)
    else:
        return get_json_result(
            data=False,
            code=RetCode.AUTHENTICATION_ERROR,
            message="邮箱或密码不正确",
        )


@user_bp.route("/logout", methods=["GET"])
@login_required
def logout():
    """
    用户登出接口
    ---
    tags:
      - User
    responses:
      200:
        description: 登出成功
        schema:
          type: object
    """
    try:
        revoke_login_token(current_user.id, current_user.access_token)
    except Exception as error:
        logger.error("Unable to revoke login token (%s)", type(error).__name__)
        return get_json_result(data=False, code=RetCode.SERVER_ERROR, message="登出失败，请稍后重试"), 503
    logout_user()
    return get_json_result(data=True, message="登出成功")


@user_bp.route("/register", methods=["POST"])
def register():
    """
    用户注册接口
    ---
    tags:
      - User
    parameters:
      - in: body
        name: body
        description: 注册信息
        required: true
        schema:
          type: object
          properties:
            email:
              type: string
              description: 用户邮箱
            password:
              type: string
              description: 用户密码
            nickname:
              type: string
              description: 用户昵称
    responses:
      200:
        description: 注册成功
        schema:
          type: object
      400:
        description: 请求错误
        schema:
          type: object
    """
    if not request.json:
        return get_json_result(
            data=False, code=RetCode.PARAMS_ERROR, message="无效的请求"
        )

    email = request.json.get("email", "")
    password = request.json.get("password", "")
    nickname = request.json.get("nickname", "")
    try:
        role = self_registration_role_id()
    except ValueError:
        return get_json_result(data=False, code=RetCode.SERVER_ERROR, message="注册角色尚未配置"), 503

    # 验证邮箱格式
    if not email or '@' not in email:
        return get_json_result(
            data=False, code=RetCode.PARAMS_ERROR, message="无效的邮箱格式"
        )

    # 检查邮箱是否已注册
    users = UserService.query(email=email)
    if users:
        return get_json_result(
            data=False, code=RetCode.PARAMS_ERROR, message="邮箱已被注册"
        )

    try:
        password = decrypt(password)
    except Exception:
        return get_json_result(
            data=False, code=RetCode.SERVER_ERROR, message="无法解密密码"
        )

    # 创建新用户
    user_id = get_uuid()
    hashed_password = generate_password_hash(password)

    # 创建用户数据，添加默认值
    user_data = {
        "id": user_id,
        "email": email,
        "password": hashed_password,
        "nickname": nickname,
        "create_date": datetime_format(datetime.now()),
        "update_date": datetime_format(datetime.now()),
        "is_authenticated": "1",
        "is_active": "1",
        "is_anonymous": "0",
        "status": StatusEnum.VALID.value,
        "role": role,
        "language": "Chinese",
        "color_schema": "Bright",
        "timezone": "UTC+8\tAsia/Shanghai",
        "login_channel": "password"
    }

    # 使用 UserService 类方法
    success = UserService.save(**user_data)
    if not success:
        return get_json_result(
            data=False, code=RetCode.SERVER_ERROR, message="创建用户失败"
        )

    # 创建默认租户
    tenant_id = get_uuid()
    tenant_data = {
        "id": tenant_id,
        "name": f"{nickname}'s Workspace",
        # A new workspace has no platform model selection yet. Supply all
        # required catalogue fields rather than relying on database defaults.
        "llm_id": "", "embd_id": "", "asr_id": "", "img2txt_id": "",
        "rerank_id": "", "tts_id": "", "parser_ids": "", "credit": 0,
        "status": StatusEnum.VALID.value,
        "create_date": datetime_format(datetime.now()),
        "update_date": datetime_format(datetime.now())
    }

    # 使用模块级的 tenant_service 实例
    success = TenantService.save(**tenant_data)
    if not success:
        # 回滚用户创建
        UserService.delete(user_id)
        return get_json_result(
            data=False, code=RetCode.SERVER_ERROR, message="创建租户失败"
        )

    # 创建用户-租户关系
    user_tenant_data = {
        "id": get_uuid(),
        "user_id": user_id,
        "tenant_id": tenant_id,
        "role": UserTenantRole.OWNER.value,
        "invited_by": user_id,
        "status": StatusEnum.VALID.value,
        "create_date": datetime_format(datetime.now()),
        "update_date": datetime_format(datetime.now())
    }

    # 使用模块级的 user_tenant_service 实例
    success = UserTenantService.save(**user_tenant_data)
    if not success:
        # 回滚用户和租户创建
        UserService.delete(user_id)
        TenantService.delete(tenant_id)
        return get_json_result(
            data=False, code=RetCode.SERVER_ERROR, message="创建用户-租户关系失败"
        )

    # 创建默认计划数据
    try:
        PlanDataService.create_default_plans(user_id, tenant_id)
    except Exception as e:
        # 记录警告但不回滚，因为计划数据不是核心功能
        logger.warning(f"为用户 {user_id} 创建默认计划失败: {e}")

    return get_json_result(data=True, message="用户注册成功")


@tenant_bp.route("/list", methods=["GET"])
@login_required
def tenant_list():
    """
    获取用户的租户列表
    ---
    tags:
      - Tenant
    responses:
      200:
        description: 成功返回租户列表
        schema:
          type: object
    """
    try:
        # 获取用户所有相关的租户信息（包括owner、normal、invite状态）
        tenants = UserTenantService.get_tenants_by_user_id(current_user.id)
        return get_json_result(data=tenants)
    except Exception as e:
        return server_error_response(e)


@tenant_bp.route("/createTenant", methods=["POST"])
@login_required
def create_tenant():
    """
    创建新租户
    ---
    tags:
      - Tenant
    parameters:
      - in: body
        name: body
        schema:
          type: object
          properties:
            name:
              type: string
              description: 租户名称
            project_name:
              type: string
              description: 项目名称
    responses:
      200:
        description: 创建成功
        schema:
          type: object
      500:
        description: 服务器错误
    """
    try:
        req = request.json or {}
        project_name = req.get("project_name", "New")

        # 创建租户数据
        tenant_id = get_uuid()
        tenant = {
            "id": tenant_id,
            "name": req.get("name", f"{project_name} Project"),
            "status": StatusEnum.VALID.value,
            "create_date": datetime_format(datetime.now()),
            "update_date": datetime_format(datetime.now())
        }

        # 保存租户信息
        # 使用模块级的 tenant_service 实例
        if not TenantService.save(**tenant):
            return get_data_error_result(message="创建租户失败！")

        # 创建用户-租户关系
        user_tenant_data = {
            "id": get_uuid(),
            "user_id": current_user.id,
            "tenant_id": tenant_id,
            "role": UserTenantRole.OWNER.value,
            "status": StatusEnum.VALID.value,
            "create_date": datetime_format(datetime.now()),
            "update_date": datetime_format(datetime.now())
        }

        # 使用模块级的 user_tenant_service 实例
        if not UserTenantService.save(**user_tenant_data):
            # 回滚租户创建
            TenantService.delete(tenant_id)
            return get_data_error_result(message="创建用户-租户关系失败！")

        # 获取创建的租户信息
        success, tenant_obj = TenantService.get_by_id(tenant_id)
        if not success:
            return get_data_error_result(message="获取租户信息失败！")

        return get_json_result(data=tenant_obj.to_dict())

    except Exception as e:
        logging.error(f"Create tenant error: {str(e)}")
        return server_error_response(e)


@user_bp.route("/info", methods=["GET"])
@login_required
def user_info():
    """
    获取用户信息
    ---
    tags:
      - User
    security:
      - ApiKeyAuth: []
    responses:
      200:
        description: 用户信息获取成功
        schema:
          type: object
    """
    user_data = dict(current_user.to_dict())
    user_data.pop("password", None)
    user_data.pop("access_token", None)
    return get_json_result(data=user_data)


@user_bp.route("/setting", methods=["POST"])
@login_required
def setting():
    """
    更新用户设置
    ---
    tags:
      - User
    security:
      - ApiKeyAuth: []
    parameters:
      - in: body
        name: body
        description: 用户设置更新
        required: true
        schema:
          type: object
          properties:
            nickname:
              type: string
              description: 新昵称
            password:
              type: string
              description: 当前密码
            new_password:
              type: string
              description: 新密码
    responses:
      200:
        description: 设置更新成功
        schema:
          type: object
    """
    update_dict = {}
    request_data = request.json or {}

    if request_data.get("password"):
        new_password = request_data.get("new_password")
        try:
            current_password = decrypt(request_data["password"])
            replacement_password = decrypt(new_password) if new_password else None
        except ValueError:
            return get_json_result(
                data=False, code=RetCode.PARAMS_ERROR, message="密码加密数据无效"
            )
        if not check_password_hash(
                current_user.password, current_password
        ):
            return get_json_result(
                data=False,
                code=RetCode.AUTHENTICATION_ERROR,
                message="Password error!",
            )

        if new_password:
            update_dict["password"] = generate_password_hash(replacement_password)

    # Only personal preferences are writable here. Role, account state and
    # credentials must never be accepted from the profile editor.
    editable_fields = {"nickname", "avatar", "language", "color_schema", "timezone"}
    for key in editable_fields:
        if key in request_data:
            update_dict[key] = request_data[key]

    try:
        update_dict["update_date"] = datetime_format(datetime.now())
        UserService.update_by_id(current_user.id, update_dict)
        return get_json_result(data=True)
    except Exception as e:
        logging.exception(e)
        return get_json_result(
            data=False, message="Update failure!", code=RetCode.SERVER_ERROR
        )


@user_bp.route("/tenant_info", methods=["GET"])
@login_required
def tenant_info():
    """
    获取用户当前租户信息
    ---
    tags:
      - User
    security:
      - ApiKeyAuth: []
    responses:
      200:
        description: 租户信息获取成功
        schema:
          type: object
    """
    try:
        # 获取当前用户作为owner的租户信息
        tenant_info = TenantService.get_info_by(current_user.id)
        if tenant_info:
            return get_json_result(data=tenant_info[0])
        else:
            return get_json_result(data=None, message="No tenant found")
    except Exception as e:
        return server_error_response(e)


@user_bp.route("/set_tenant_info", methods=["POST"])
@login_required
def set_tenant_info():
    """
    设置租户信息
    ---
    tags:
      - User
    security:
      - ApiKeyAuth: []
    parameters:
      - in: body
        name: body
        description: 租户信息
        required: true
        schema:
          type: object
          properties:
            tenant_id:
              type: string
              description: 租户ID
            name:
              type: string
              description: 租户名称
    responses:
      200:
        description: 租户信息设置成功
        schema:
          type: object
    """
    try:
        req = request.json or {}
        tenant_id = req.get("tenant_id")

        if not tenant_id:
            return get_json_result(
                data=False, code=RetCode.PARAMS_ERROR, message="Tenant ID is required"
            )

        # 检查用户是否有权限修改该租户
        user_tenant = UserTenantService.query(user_id=current_user.id, tenant_id=tenant_id)
        if not user_tenant:
            return get_json_result(
                data=False, code=RetCode.AUTHENTICATION_ERROR, message="No permission"
            )

        update_dict = {}
        for k in req.keys():
            if k in ["id", "create_time", "create_date"]:
                continue
            update_dict[k] = req[k]

        update_dict["update_date"] = datetime_format(datetime.now())

        TenantService.update_by_id(tenant_id, update_dict)
        return get_json_result(data=True)
    except Exception as e:
        return server_error_response(e)


@user_bp.route("/role/list", methods=["GET"])
@login_required
def role_list():
    """
    获取角色列表
    ---
    tags:
      - User
    security:
      - ApiKeyAuth: []
    responses:
      200:
        description: 角色列表获取成功
        schema:
          type: object
    """
    try:
        roles = UserService.get_all_roles()
        role_data = []
        for role in roles:
            role_data.append({
                "id": role["id"],
                "name": role["name"],
                "code": role["code"],
                "description": role.get('description', '')
            })
        return get_json_result(data=role_data)
    except Exception as e:
        return server_error_response(e)


@user_bp.route("/user_role", methods=["GET"])
@login_required
def user_role():
    """
    获取用户角色信息
    ---
    tags:
      - User
    security:
      - ApiKeyAuth: []
    parameters:
      - in: query
        name: email
        type: string
        description: 用户邮箱
    responses:
      200:
        description: 用户角色信息获取成功
        schema:
          type: object
    """
    try:
        email = request.args.get("email")
        if email:
            users = UserService.query(email=email)
            if users:
                user = users[0]
                role = UserService.get_user_role(user.role)
                if role:
                    return get_json_result(data={
                        "role_id": role.id,
                        "role_name": role.name,
                        "role_code": role.code
                    })

        # 如果没有指定邮箱或用户不存在，返回当前用户角色
        role = UserService.get_user_role(current_user.role)
        if role:
            return get_json_result(data={
                "role_id": role.id,
                "role_name": role.name,
                "role_code": role.code
            })
        else:
            return get_json_result(data=None, message="未找到角色")
    except Exception as e:
        return server_error_response(e)


# 租户用户管理相关API
@tenant_bp.route("/<tenant_id>/user/list", methods=["GET"])
@login_required
def list_tenant_user(tenant_id):
    """
    获取租户用户列表
    ---
    tags:
      - Tenant
    security:
      - ApiKeyAuth: []
    parameters:
      - in: path
        name: tenant_id
        type: string
        required: true
        description: 租户ID
    responses:
      200:
        description: 租户用户列表获取成功
        schema:
          type: object
    """
    try:
        # 检查用户是否有权限访问该租户（必须是该租户的成员，且角色为owner、normal或invite）
        user_tenant = UserTenantService.query(user_id=current_user.id, tenant_id=tenant_id)
        if not user_tenant or user_tenant[0].role not in [UserTenantRole.OWNER.value, UserTenantRole.NORMAL.value, UserTenantRole.INVITE.value]:
            return get_json_result(
                data=False,
                message='没有授权。',
                code=RetCode.AUTHENTICATION_ERROR
            )

        users = UserTenantService.get_by_tenant_id(tenant_id)
        return get_json_result(data=users)
    except Exception as e:
        return server_error_response(e)


@tenant_bp.route("/<tenant_id>/user", methods=["POST"])
@login_required
def add_tenant_user(tenant_id):
    """
    添加用户到租户
    ---
    tags:
      - Tenant
    security:
      - ApiKeyAuth: []
    parameters:
      - in: path
        name: tenant_id
        type: string
        required: true
        description: 租户ID
      - in: body
        name: body
        description: 用户信息
        required: true
        schema:
          type: object
          properties:
            email:
              type: string
              description: 用户邮箱
    responses:
      200:
        description: 用户添加成功
        schema:
          type: object
    """
    try:
        # 检查用户是否有权限管理该租户（只有owner可以添加用户）
        user_tenant = UserTenantService.query(user_id=current_user.id, tenant_id=tenant_id)
        if not user_tenant or user_tenant[0].role != UserTenantRole.OWNER.value:
            return get_json_result(
                data=False,
                message='没有授权。只有租户所有者可以删除其他用户。',
                code=RetCode.AUTHENTICATION_ERROR
            )

        req = request.json or {}
        email = req.get("email")
        if not email:
            return get_data_error_result(message="邮箱是必需的。")

        users = UserService.query(email=email)
        if not users:
            return get_data_error_result(message="未找到用户。")

        user_id = users[0].id
        existing_user_tenants = UserTenantService.query(user_id=user_id, tenant_id=tenant_id)
        if existing_user_tenants:
            return get_data_error_result(message="该用户已在租户中。")

        # 添加用户到租户（初始状态为invite，需要用户同意后才变为normal）
        user_tenant_data = {
            "id": get_uuid(),
            "user_id": user_id,
            "tenant_id": tenant_id,
            "role": UserTenantRole.INVITE.value,
            "invited_by": current_user.id,
            "status": StatusEnum.VALID.value,
            "create_date": datetime_format(datetime.now()),
            "update_date": datetime_format(datetime.now())
        }

        # 使用模块级的 user_tenant_service 实例
        UserTenantService.save(**user_tenant_data)

        usr = users[0].to_dict()
        usr = {k: v for k, v in usr.items() if k in ["id", "avatar", "email", "nickname"]}

        return get_json_result(data=usr)
    except Exception as e:
        return server_error_response(e)


@tenant_bp.route("/<tenant_id>/user/<user_id>", methods=["DELETE"])
@login_required
def delete_tenant_user(tenant_id, user_id):
    """
    从租户中删除用户
    ---
    tags:
      - Tenant
    security:
      - ApiKeyAuth: []
    parameters:
      - in: path
        name: tenant_id
        type: string
        required: true
        description: 租户ID
      - in: path
        name: user_id
        type: string
        required: true
        description: 用户ID
    responses:
      200:
        description: 用户删除成功
        schema:
          type: object
    """
    try:
        # 检查权限：只有租户owner或用户本人可以删除
        user_tenant = UserTenantService.query(user_id=current_user.id, tenant_id=tenant_id)
        if not user_tenant:
            return get_json_result(
                data=False,
                message='没有授权。',
                code=RetCode.AUTHENTICATION_ERROR
            )

        # 租户owner可以删除任何用户，普通用户只能删除自己
        if current_user.id != user_id and user_tenant[0].role != UserTenantRole.OWNER.value:
            return get_json_result(
                data=False,
                message='没有授权。只有租户所有者可以添加用户。',
                code=RetCode.AUTHENTICATION_ERROR
            )

        UserTenantService.filter_delete([
            UserTenant.user_id == user_id,
            UserTenant.tenant_id == tenant_id
        ])
        return get_json_result(data=True)
    except Exception as e:
        return server_error_response(e)


@tenant_bp.route("/agree/<tenant_id>", methods=["PUT"])
@login_required
def agree_tenant(tenant_id):
    """
    同意加入租户
    ---
    tags:
      - Tenant
    security:
      - ApiKeyAuth: []
    parameters:
      - in: path
        name: tenant_id
        type: string
        required: true
        description: 租户ID
    responses:
      200:
        description: 同意成功
        schema:
          type: object
    """
    try:
        UserTenantService.filter_update(
            [UserTenant.user_id == current_user.id, UserTenant.tenant_id == tenant_id],
            {"role": UserTenantRole.NORMAL.value, "update_date": datetime_format(datetime.now())}
        )
        return get_json_result(data=True)
    except Exception as e:
        return server_error_response(e)
