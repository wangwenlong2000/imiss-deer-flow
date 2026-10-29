# -*- coding: utf-8 -*-
# Conversation Controller for OneCity Agent

import json
import ast
import logging
import time
from datetime import datetime
from uuid import uuid4
from flask import Blueprint, request
from flask_login import login_required, current_user
from flasgger import swag_from

from onecityagent.service.conversation_service import ConversationService
from onecityagent.utils.api_utils import get_json_result, get_data_error_result, server_error_response, validate_request

# 创建蓝图
conversation_bp = Blueprint('conversation', __name__)

# 创建 ConversationService 实例
conversation_service = ConversationService()


@conversation_bp.route('/set', methods=['POST'])
@login_required
def set_conversation():
    """
    创建或更新会话
    ---
    tags:
      - 对话管理
    summary: 创建或更新会话
    description: 创建或更新会话
    parameters:
      - name: body
        in: body
        required: true
        schema:
          type: object
          properties:
            conversation_id:
              type: string
              description: 会话ID
            dialog_id:
              type: string
              description: 对话配置ID
            sort_order:
              type: integer
              description: 会话排序
            message:
              type: array
              description: 初始消息列表
              items:
                type: object
            reference:
              type: array
              description: 参考资料列表
              items:
                type: object
            scene:
              type: string
              description: 场景名称
            plan:
              type: string
              description: 计划内容
          required: []
    responses:
      200:
        description: 操作成功
        schema:
          type: object
          properties:
            code:
              type: integer
              example: 0
            message:
              type: string
              example: "success"
            data:
              type: object
              description: 会话信息
      400:
        description: 请求参数错误
      500:
        description: 服务器内部错误
    """
    req = request.json
    conv_id = req.get("conversation_id")
    dialog_id = req.get("dialog_id")
    sort_order = req.get("sort_order")
    user_id = current_user.id
    
    # 优先级1: 如果提供了conversation_id，直接更新该会话
    if conv_id:
        try:
            # 移除不需要的字段
            update_data = {k: v for k, v in req.items() if k not in ["conversation_id"]}
            #update_data['update_time'] = int(time.time() * 1000)
            update_data['update_date'] = datetime.now()
            
            # 如果前端传递了message和reference参数，添加到更新数据中
            if "message" in req:
                update_data["message"] = req["message"]
            if "reference" in req:
                update_data["reference"] = req["reference"]
            
            if not conversation_service.update_conversation(conv_id, update_data):
                return get_data_error_result(message="会话未找到！")
                
            exists, conv = conversation_service.get_conversation_by_id(conv_id)
            if not exists or not conv:
                return get_data_error_result(message="更新会话失败！")
                
            return get_json_result(data=conv.to_dict())
            
        except Exception as e:
            logging.error(f"Error updating conversation: {str(e)}")
            return server_error_response(e)
    
    # 优先级2: 如果提供了dialog_id和sort_order且没有conv_id，检查是否已存在相同排序的对话
    if not conv_id and dialog_id and sort_order is not None:
        try:
            # 查询该dialog下是否已存在相同sort_order的对话
            success, existing_conv = conversation_service.get_conversation_by_dialog_and_sort(
                dialog_id=dialog_id,
                sort_order=sort_order
            )
            
            if success and existing_conv:
                # 如果存在相同排序的对话，则更新该对话
                existing_conv_id = existing_conv.id
                update_data = {k: v for k, v in req.items() if k not in ["conversation_id"]}
                #update_data['update_time'] = int(time.time() * 1000)
                update_data['update_date'] = datetime.now()
                
                # 如果前端传递了message和reference参数，添加到更新数据中
                if "message" in req:
                    update_data["message"] = req["message"]
                if "reference" in req:
                    update_data["reference"] = req["reference"]
                
                if not conversation_service.update_conversation(existing_conv_id, update_data):
                    return get_data_error_result(message="更新现有会话失败！")
                    
                exists, conv = conversation_service.get_conversation_by_id(existing_conv_id)
                if not exists or not conv:
                    return get_data_error_result(message="获取更新后的会话失败！")
                    
                return get_json_result(data=conv.to_dict())
            else:
                # 如果不存在相同排序的对话，则创建新会话
                try:
                    # 生成会话ID
                    if not conv_id:
                        conv_id = str(uuid4())
                        
                    # 创建会话数据
                    conv_data = {
                        "conversation_id": conv_id,
                        "dialog_id": dialog_id,
                        "sort_order": sort_order
                    }
                    
                    # 如果前端传递了message、reference、scene和plan参数，添加到会话数据中
                    if "message" in req:
                        conv_data["message"] = req["message"]
                    if "reference" in req:
                        conv_data["reference"] = req["reference"]
                    if "scene" in req:
                        conv_data["scene"] = req["scene"]
                    if "plan" in req:
                        conv_data["plan"] = req["plan"]
                    
                    # 创建会话
                    success = conversation_service.create_conversation(**conv_data)

                    success, conv = conversation_service.get_conversation_by_id(conv_id)
                    if not success:
                        return get_data_error_result(message="创建会话失败！")
                        
                    return get_json_result(data=conv.to_dict())
                    
                except Exception as e:
                    logging.error(f"Error creating conversation: {str(e)}")
                    return server_error_response(e)
                
        except Exception as e:
            logging.error(f"Error checking existing conversation: {str(e)}")
            return server_error_response(e)
    
    # 如果没有提供dialog_id和sort_order，返回错误
    return get_data_error_result(message="需要提供conversation_id或者同时提供dialog_id和sort_order！")


@conversation_bp.route('/get', methods=['GET'])
@login_required
def get_conversation():
    """
    获取会话详情
    ---
    tags:
      - 对话管理
    summary: 获取会话详情
    description: 根据会话ID获取会话的详细信息
    parameters:
      - name: conversation_id
        in: query
        type: string
        required: true
        description: 会话ID
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
              example: "success"
            data:
              type: object
              description: 会话详情
      400:
        description: 请求参数错误
      404:
        description: 会话不存在
      500:
        description: 服务器内部错误
    """
    conv_id = request.args.get("conversation_id")
    if not conv_id:
        return get_data_error_result(message="会话ID是必需的！")
        
    try:
        exists, conv = conversation_service.get_conversation_by_id(conv_id)
        if not exists or not conv:
            return get_data_error_result(message="会话未找到！")
        
        # 统一格式化 plan 字段：仅支持字符串列表格式
        try:
            conv_dict = conv.to_dict()
            plan_value = conv_dict.get("plan")
            if plan_value:
                if isinstance(plan_value, str):
                    s = plan_value.strip()
                    if s.startswith("[") and s.endswith("]"):
                        try:
                            conv_dict["plan"] = json.loads(s)
                        except Exception:
                            try:
                                conv_dict["plan"] = [str(item).strip() for item in ast.literal_eval(s)]
                            except Exception:
                                conv_dict["plan"] = [s]
                    else:
                        conv_dict["plan"] = [s]
                else:
                    conv_dict["plan"] = []
            else:
                conv_dict["plan"] = []
        except Exception as e:
            logging.error(f"Format plan field error in /get: {str(e)}")
            conv_dict = conv.to_dict()
            if not conv_dict.get("plan"):
                conv_dict["plan"] = []

        return get_json_result(data=conv_dict)
        
    except Exception as e:
        logging.error(f"Error getting conversation: {str(e)}")
        return server_error_response(e)


@conversation_bp.route('/rm', methods=['POST'])
@login_required
def remove_conversations():
    """
    删除会话
    ---
    tags:
      - 对话管理
    summary: 删除会话
    description: 批量删除指定的会话
    parameters:
      - name: body
        in: body
        required: true
        schema:
          type: object
          properties:
            conversation_ids:
              type: array
              items:
                type: string
              description: 要删除的会话ID列表
          required:
            - conversation_ids
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
              example: "success"
            data:
              type: boolean
              example: true
      400:
        description: 请求参数错误
      500:
        description: 服务器内部错误
    """
    conv_ids = request.json.get("conversation_ids", [])
    if not conv_ids:
        return get_data_error_result(message="会话ID列表是必需的！")
        
    try:
        for cid in conv_ids:
            exists, conv = conversation_service.get_conversation_by_id(cid)
            if not exists or not conv:
                return get_data_error_result(message="会话未找到！")
                
            conversation_service.delete_conversation(cid)
            
        return get_json_result(data=True)
        
    except Exception as e:
        logging.error(f"Error removing conversations: {str(e)}")
        return server_error_response(e)


@conversation_bp.route('/list', methods=['GET'])
@login_required
def list_conversations():
    """
    获取会话列表
    ---
    tags:
      - 对话管理
    summary: 获取会话列表
    description: 获取会话列表，支持按dialog_id过滤
    parameters:
      - name: page
        in: query
        type: integer
        default: 1
        description: 页码
      - name: page_size
        in: query
        type: integer
        default: 10
        description: 每页数量
      - name: dialog_id
        in: query
        type: string
        description: 对话ID，用于过滤特定对话下的会话
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
              example: "success"
            data:
              type: array
              items:
                type: object
                properties:
                  id:
                    type: string
                    description: 会话ID
                  message:
                    type: string
                    description: 消息内容
                  reference:
                    type: string
                    description: 引用信息
                  dialog_id:
                    type: string
                    description: 对话ID
                  sort_order:
                    type: integer
                    description: 排序字段
                  scene:
                    type: string
                    description: 场景名称
                  plan:
                    type: string
                    description: 计划内容
                  create_date:
                    type: string
                    format: date-time
                    description: 创建日期
                  update_date:
                    type: string
                    format: date-time
                    description: 更新日期
      400:
        description: 请求参数错误
      500:
        description: 服务器内部错误
    """
    page = int(request.args.get("page", 1))
    page_size = int(request.args.get("page_size", 10))
    dialog_id = request.args.get("dialog_id")
    
    try:
        # 构建查询条件
        query_conditions = {}
        if dialog_id:
            query_conditions['dialog_id'] = dialog_id
            
        convs = conversation_service.get_list(
            page_number=page,
            items_per_page=page_size,
            orderby='create_date',
            desc=True,
            **query_conditions
        )
        
        # 统一格式化 plan 字段：仅支持字符串列表格式
        try:
            for conv in convs:
                plan_value = conv.get("plan")
                if plan_value:
                    if isinstance(plan_value, str):
                        s = plan_value.strip()
                        # 仅支持列表字符串 "[...]"（双引号或单引号）
                        if s.startswith("[") and s.endswith("]"):
                            try:
                                # 先尝试 JSON 解析（双引号）
                                conv["plan"] = json.loads(s)
                            except Exception:
                                try:
                                    # 回退到 Python 字面量解析（单引号）
                                    conv["plan"] = [str(item).strip() for item in ast.literal_eval(s)]
                                except Exception:
                                    conv["plan"] = [s]
                        else:
                            conv["plan"] = [s]
                    else:
                        conv["plan"] = []
                else:
                    conv["plan"] = []
        except Exception as e:
            logging.error(f"Format plan field error in /list: {str(e)}")

        return get_json_result(data=convs)
        
    except Exception as e:
        logging.error(f"Error listing conversations: {str(e)}")
        return server_error_response(e)


@conversation_bp.route('/save', methods=['POST'])
@login_required
@validate_request("conversation_id", "messages")
def save_conversation_data():
    """
    保存会话数据接口
    ---
    tags:
      - 对话管理
    summary: 保存会话数据
    description: 保存前端传输的会话数据，验证会话ID和排序是否重复
    parameters:
      - name: body
        in: body
        required: true
        schema:
          type: object
          properties:
            conversation_id:
              type: string
              description: 会话ID
            messages:
              type: array
              items:
                type: object
                properties:
                  role:
                    type: string
                    enum: ["user", "assistant", "system"]
                  content:
                    type: string
                  id:
                    type: string
                  sort_order:
                    type: integer
                    description: 消息排序
              description: 消息列表
            reference:
              type: array
              description: 引用信息
          required:
            - conversation_id
            - messages
    responses:
      200:
        description: 保存成功
        schema:
          type: object
          properties:
            code:
              type: integer
              example: 0
            message:
              type: string
              example: "success"
            data:
              type: boolean
              example: true
      400:
        description: 请求参数错误
      404:
        description: 会话不存在
      409:
        description: 会话ID或排序重复
      500:
        description: 服务器内部错误
    """
    req = request.json
    conversation_id = req["conversation_id"]
    messages = req["messages"]
    reference = req.get("reference", [])
    
    try:
        # 验证会话是否存在
        exists, conv = conversation_service.get_conversation_by_id(conversation_id)
        if not exists or not conv:
            return get_data_error_result(message="会话未找到！", retcode=404)
        
        # 验证消息ID是否重复
        message_ids = [msg.get("id") for msg in messages if msg.get("id")]
        if len(message_ids) != len(set(message_ids)):
            return get_data_error_result(message="发现重复的消息ID！", retcode=409)
        
        # 验证排序是否重复
        sort_orders = [msg.get("sort_order") for msg in messages if msg.get("sort_order") is not None]
        if len(sort_orders) != len(set(sort_orders)):
            return get_data_error_result(message="发现重复的排序值！", retcode=409)
        
        # 更新会话数据
        update_data = {
            'message': json.dumps(messages),
            'reference': json.dumps(reference)
        }
        
        success = conversation_service.update_conversation(conversation_id, update_data)
        if not success:
            return get_data_error_result(message="保存会话数据失败！")
        
        return get_json_result(data=True)
        
    except Exception as e:
        logging.error(f"Error in save_conversation_data: {str(e)}")
        return server_error_response(e)


@conversation_bp.route('/list_by_dialog', methods=['GET'])
@login_required
def list_conversations_by_dialog():
    """
    根据dialog_id获取对话列表
    ---
    tags:
      - 对话管理
    summary: 根据dialog_id获取对话列表
    description: 根据dialog_id获取对话列表，支持分页，按sort_order从大到小排序
    parameters:
      - name: dialog_id
        in: query
        type: string
        required: true
        description: 对话ID
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
              example: "success"
            data:
              type: array
              items:
                type: object
                properties:
                  id:
                    type: string
                    description: 会话ID
                  message:
                    type: string
                    description: 消息内容
                  reference:
                    type: string
                    description: 引用信息
                  dialog_id:
                    type: string
                    description: 对话ID
                  sort_order:
                    type: integer
                    description: 排序字段
                  create_date:
                    type: string
                    format: date-time
                    description: 创建日期
                  update_date:
                    type: string
                    format: date-time
                    description: 更新日期
      400:
        description: 请求参数错误
      500:
        description: 服务器内部错误
    """
    dialog_id = request.args.get("dialog_id")
    if not dialog_id:
        return get_data_error_result(message="dialog_id是必需的！")

    
    try:
        # 构建查询条件，按sort_order从大到小排序
        convs = conversation_service.get_all_list(
            orderby='sort_order',
            desc=False,
            dialog_id=dialog_id
        )
            
        # 统一格式化 plan 字段：仅支持字符串列表格式
        try:
            for conv in convs:
                plan_value = conv.get("plan")
                if plan_value:
                    if isinstance(plan_value, str):
                        s = plan_value.strip()
                        # 仅支持列表字符串 "[...]"（双引号或单引号）
                        if s.startswith("[") and s.endswith("]"):
                            try:
                                # 先尝试 JSON 解析（双引号）
                                conv["plan"] = json.loads(s)
                            except Exception:
                                try:
                                    # 回退到 Python 字面量解析（单引号）
                                    conv["plan"] = [str(item).strip() for item in ast.literal_eval(s)]
                                except Exception:
                                    conv["plan"] = [s]
                        else:
                            conv["plan"] = [s]
                    else:
                        conv["plan"] = []
                else:
                    conv["plan"] = []
        except Exception as e:
            logging.error(f"Format plan field error: {str(e)}")
        
        return get_json_result(data=convs)
        
    except Exception as e:
        logging.error(f"Error listing conversations by dialog: {str(e)}")
        return server_error_response(e)