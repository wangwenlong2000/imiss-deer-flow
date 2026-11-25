from flask import Blueprint, request, jsonify
from flask_login import login_required, current_user
from flasgger import swag_from

from onecityagent.service.dialog_service.dialog_service import DialogService
from onecityagent.service.dialog_service.access import owns_dialog
from onecityagent.service.user_service import TenantService
from ..utils.api_utils import get_json_result, get_data_error_result, server_error_response


dialog_bp = Blueprint('dialog', __name__, url_prefix='/api/dialog')
dialog_service = DialogService()


@dialog_bp.before_request
def authorize_dialog_resource():
    """A login alone never authorizes another user's dialog identifier."""
    dialog_id = (request.view_args or {}).get("dialog_id")
    if dialog_id and current_user.is_authenticated and not owns_dialog(current_user.id, dialog_id):
        return jsonify({"code": 404, "message": "对话会话不存在", "data": None}), 404
    return None


@dialog_bp.route('/create', methods=['POST'])
@login_required
def create_dialog():
    """
    创建新的对话会话
    ---
    tags:
      - 会话管理
    summary: 创建对话会话
    description: 创建一个新的对话会话
    parameters:
      - name: body
        in: body
        required: true
        schema:
          type: object
          properties:
            name:
              type: string
              description: 对话会话名称
            description:
              type: string
              description: 对话会话描述
          required:
            - name
    responses:
      200:
        description: 创建成功
        schema:
          type: object
          properties:
            code:
              type: integer
              example: 0
            message:
              type: string
              example: "对话会话创建成功"
            data:
              type: object
              description: 创建的对话会话信息
      400:
        description: 请求参数错误
      500:
        description: 服务器内部错误
    """
    try:
        data = request.get_json()
        if not data:
            return get_data_error_result("请求数据不能为空")

        name = data.get('name')
        description = data.get('description')

        if not name:
            return get_data_error_result("name为必填字段")

        # 使用当前登录用户信息
        creator_id = current_user.id

        # 获取当前用户作为owner的租户信息
        tenant_info = TenantService.get_info_by(current_user.id)
        if not tenant_info:
            return get_data_error_result(message="用户没有关联的租户空间")

        tenant_id = tenant_info[0]['tenant_id']

        result = dialog_service.create_dialog(name, creator_id, tenant_id, description)
        return get_json_result(data=result, message="对话会话创建成功")

    except Exception as e:
        return server_error_response(e)





@dialog_bp.route('/get/<dialog_id>', methods=['GET'])
@login_required
def get_dialog(dialog_id: str):
    """
    获取对话会话信息
    ---
    tags:
      - 会话管理
    summary: 获取对话会话
    description: 根据ID获取对话会话的详细信息
    parameters:
      - name: dialog_id
        in: path
        type: string
        required: true
        description: 对话会话ID
    responses:
      200:
        description: 获取成功
        schema:
          type: object
          properties:
            code:
              type: integer
              example: 0
            message:
              type: string
              example: "获取对话会话成功"
            data:
              type: object
              description: 对话会话详细信息
      404:
        description: 对话会话不存在
      500:
        description: 服务器内部错误
    """
    try:
        result = dialog_service.get_dialog(dialog_id)
        if result:
            return get_json_result(data=result, message="获取对话会话成功")
        else:
            return get_json_result(data=None, code=404, message="对话会话不存在")

    except Exception as e:
        return server_error_response(e)


@dialog_bp.route('/list', methods=['GET'])
@login_required
def get_dialogs():
    """
    获取当前用户的全部对话会话列表（不分页）
    ---
    tags:
      - 会话管理
    security:
      - cookieAuth: []
    summary: 获取对话会话列表
    description: 获取当前用户在所属租户空间下的全部对话会话列表
    responses:
      200:
        description: 获取成功
        schema:
          type: object
          properties:
            code:
              type: integer
              example: 0
            message:
              type: string
              example: "获取对话会话列表成功"
            data:
              type: array
              description: 全部对话会话列表
              items:
                type: object
                properties:
                  dialog_id:
                    type: string
                  name:
                    type: string
                  creator_id:
                    type: string
                  tenant_id:
                    type: string
                  description:
                    type: string
                  create_date:
                    type: string
                  update_date:
                    type: string
                  status:
                    type: integer
                  finished:
                    type: integer
      401:
        description: 未授权
      500:
        description: 服务器内部错误
    """
    try:
        # 自动使用当前用户信息
        creator_id = current_user.id

        # 获取当前用户作为owner的租户信息
        tenant_info = TenantService.get_info_by(current_user.id)
        if not tenant_info:
            return get_data_error_result(message="用户没有关联的租户空间")

        tenant_id = tenant_info[0]['tenant_id']
        result = dialog_service.get_all_dialogs(creator_id, tenant_id)
        return get_json_result(data=result, message="获取对话会话列表成功")

    except Exception as e:
        return server_error_response(e)

@dialog_bp.route('/update/<dialog_id>', methods=['PUT'])
@login_required
def update_dialog(dialog_id: str):
    """
    更新对话会话信息
    ---
    tags:
      - 会话管理
    summary: 更新对话会话
    description: 更新指定对话会话的信息
    parameters:
      - name: dialog_id
        in: path
        type: string
        required: true
        description: 对话会话ID
      - name: body
        in: body
        required: true
        schema:
          type: object
          properties:
            name:
              type: string
              description: 对话会话名称
            description:
              type: string
              description: 对话会话描述
    responses:
      200:
        description: 更新成功
        schema:
          type: object
          properties:
            code:
              type: integer
              example: 0
            message:
              type: string
              example: "更新对话会话成功"
            data:
              type: object
              description: 更新后的对话会话信息
      400:
        description: 请求参数错误
      404:
        description: 对话会话不存在
      500:
        description: 服务器内部错误
    """
    try:
        data = request.get_json()
        if not data:
            return get_data_error_result("请求数据不能为空")

        name = data.get('name')
        description = data.get('description')

        result = dialog_service.update_dialog(dialog_id, name, description)
        if result:
            return get_json_result(data=result, message="更新对话会话成功")
        else:
            return get_json_result(data=None, code=404, message="对话会话不存在")

    except Exception as e:
        return server_error_response(e)


@dialog_bp.route('/finish/<dialog_id>', methods=['PUT'])
@login_required
def finish_dialog(dialog_id: str):
    """
    将对话会话标记为已完成
    ---
    tags:
      - 会话管理
    summary: 完成对话会话
    description: 将指定对话会话标记为已完成状态
    parameters:
      - name: dialog_id
        in: path
        type: string
        required: true
        description: 对话会话ID
    responses:
      200:
        description: 更新成功
        schema:
          type: object
          properties:
            code:
              type: integer
              example: 0
            message:
              type: string
              example: "对话会话已标记为完成"
            data:
              type: object
              description: 更新后的对话会话信息
      404:
        description: 对话会话不存在
      500:
        description: 服务器内部错误
    """
    try:
        result = dialog_service.update_dialog_finished(dialog_id, finished=1)
        if result:
            return get_json_result(data=result, message="对话会话已标记为完成")
        else:
            return get_json_result(data=None, code=404, message="对话会话不存在")

    except Exception as e:
        return server_error_response(e)


@dialog_bp.route('/delete/<dialog_id>', methods=['DELETE'])
@login_required
def delete_dialog(dialog_id: str):
    """
    删除对话会话
    ---
    tags:
      - 会话管理
    summary: 删除对话会话
    description: 删除指定的对话会话
    parameters:
      - name: dialog_id
        in: path
        type: string
        required: true
        description: 对话会话ID
    responses:
      200:
        description: 删除成功
        schema:
          type: object
          properties:
            code:
              type: integer
              example: 0
            message:
              type: string
              example: "删除对话会话成功"
            data:
              type: object
              nullable: true
              description: 删除结果（通常为null）
      404:
        description: 对话会话不存在
      500:
        description: 服务器内部错误
    """
    try:
        result = dialog_service.delete_dialog(dialog_id)
        if result:
            return get_json_result(data=None, message="删除对话会话成功")
        else:
            return get_json_result(data=None, code=404, message="对话会话不存在")

    except Exception as e:
        return server_error_response(e)
