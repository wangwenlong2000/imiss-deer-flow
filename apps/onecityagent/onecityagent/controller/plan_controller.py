# -*- coding: utf-8 -*-
# Plan Controller for OneCity Agent

import logging
from flask import Blueprint, request
from flask_login import login_required, current_user
from flasgger import swag_from

from onecityagent.service.tool_service.save_plan import PlanDataService
from onecityagent.utils.api_utils import get_json_result, server_error_response
from onecityagent.logger import logger

plan_bp = Blueprint('plan', __name__, url_prefix='/api/plan')

# 禁止使用的方案名称
FORBIDDEN_SCENE_NAMES = {"其他场景", "非内置场景"}


@plan_bp.route('/create', methods=['POST'])
@login_required
@swag_from({
    "parameters": [
        {
            "name": "body",
            "in": "body",
            "required": True,
            "schema": {
                "type": "object",
                "properties": {
                    "scene": {"type": "string", "description": "场景名称"},
                    "plan": {
                        "type": "array", 
                        "items": {"type": "string"}, 
                        "description": "方案内容列表"
                    }
                },
                "required": ["scene", "plan"]
            }
        }
    ],
    "responses": {
        200: {
            "description": "创建成功",
            "schema": {
                "type": "object",
                "properties": {
                    "code": {"type": "integer"},
                    "message": {"type": "string"},
                    "data": {"type": "object"}
                }
            }
        },
        400: {"description": "请求参数错误"},
        500: {"description": "服务器内部错误"}
    },
    "tags": ["方案管理"],
    "summary": "创建方案",
    "description": "创建新的方案数据"
})
def create_plan():
    """创建方案"""
    try:
        data = request.get_json()
        if not data:
            return get_json_result(data=None, code=400, message="请求数据不能为空")
        
        scene = data.get('scene')
        plan = data.get('plan')
        
        if not scene or not plan:
            return get_json_result(data=None, code=400, message="缺少必要参数: scene, plan")

        # 数量限制：与个人绑定的方案数量不能超过15条（仅统计owner='person'）
        person_plan_count = PlanDataService.count_person_plans_by_user(current_user.id)
        if person_plan_count >= 15:
            return get_json_result(data=None, code=400, message="个人绑定的方案数量超过15条")

        # 名称校验：禁止使用特定占位名称
        if (scene or "").strip() in FORBIDDEN_SCENE_NAMES:
            return get_json_result(data=None, code=400, message="请根据方案详情填写方案名称，不能命名为其他场景或非内置场景")
        
        # 检查同一用户下场景名称是否已存在
        existing_plan = PlanDataService.get_plan_by_scene_and_user(scene, current_user.id)
        if existing_plan:
            return get_json_result(data=None, code=400, message="该方案名称已存在，请使用其他名称")
        
        result = PlanDataService.create_plan(
            scene=scene,
            plan=plan,
            user_id=current_user.id,
            tenant_id=current_user.id,  # tenant_id复制user_id
            owner='person'  # owner默认为person
        )
        
        if result:
            return get_json_result(data={"id": result.id}, message="创建成功")
        else:
            return get_json_result(data=None, code=500, message="创建失败")
            
    except Exception as e:
        logger.exception(f"Create plan error: {str(e)}")
        return server_error_response(e)


@plan_bp.route('/update/<int:plan_id>', methods=['PUT'])
@login_required
@swag_from({
    "parameters": [
        {
            "name": "plan_id",
            "in": "path",
            "required": True,
            "type": "integer",
            "description": "方案ID"
        },
        {
            "name": "body",
            "in": "body",
            "required": True,
            "schema": {
                "type": "object",
                "properties": {
                    "scene": {"type": "string", "description": "场景名称"},
                    "plan": {
                        "type": "array", 
                        "items": {"type": "string"}, 
                        "description": "方案内容列表"
                    },
                    "owner": {"type": "string", "description": "所有者（只能设置为person）", "enum": ["person"]}
                }
            }
        }
    ],
    "responses": {
        200: {"description": "更新成功"},
        400: {"description": "请求参数错误"},
        404: {"description": "方案不存在"},
        500: {"description": "服务器内部错误"}
    },
    "tags": ["方案管理"],
    "summary": "更新方案",
    "description": "更新指定ID的方案数据"
})
def update_plan(plan_id):
    """更新方案"""
    try:
        data = request.get_json()
        if not data:
            return get_json_result(data=None, code=400, message="请求数据不能为空")
        
        # 先检查方案是否存在且属于当前用户
        existing_plan = PlanDataService.get_plan_by_id(plan_id)
        if not existing_plan:
            return get_json_result(data=None, code=404, message="方案不存在")
        
        if existing_plan['user_id'] != current_user.id:
            return get_json_result(data=None, code=403, message="无权限操作此方案")
        
        # 过滤允许更新的字段
        update_data = {}
        if 'scene' in data:
            # 名称校验：禁止使用特定占位名称
            if (data['scene'] or "").strip() in FORBIDDEN_SCENE_NAMES:
                return get_json_result(data=None, code=400, message="请根据方案详情填写方案名称，不能命名为其他场景或非内置场景")
            # 只有当scene值真正发生变化时才进行各种检查
            if data['scene'] != existing_plan.get('scene'):
                # 如果当前owner为public，不允许修改scene
                if existing_plan.get('owner') == 'public':
                    return get_json_result(data=None, code=400, message="owner为public时不允许修改scene")
                
                # 检查同一用户下场景名称是否已存在（排除当前方案）
                scene_check = PlanDataService.get_plan_by_scene_and_user(data['scene'], current_user.id)
                if scene_check and scene_check.get('id') != plan_id:
                    return get_json_result(data=None, code=400, message="该方案名称已存在，请使用其他名称")
            
            update_data['scene'] = data['scene']
        if 'plan' in data:
            update_data['plan'] = data['plan']
        if 'owner' in data:
            # 如果当前是person，不允许修改为public；如果当前是public，可以保持为public
            if data['owner'] == 'public' and existing_plan.get('owner') == 'person':
                return get_json_result(data=None, code=400, message="不允许将person方案修改为public")
            # 只允许设置为person或public
            if data['owner'] not in ['person', 'public']:
                return get_json_result(data=None, code=400, message="owner只能设置为person或public")
            update_data['owner'] = data['owner']
        
        if not update_data:
            return get_json_result(data=None, code=400, message="没有可更新的数据")
        
        result = PlanDataService.update_plan(plan_id, **update_data)
        
        if result:
            return get_json_result(data=None, message="更新成功")
        else:
            return get_json_result(data=None, code=500, message="更新失败")
            
    except Exception as e:
        logger.exception(f"Update plan error: {str(e)}")
        return server_error_response(e)


@plan_bp.route('/delete/<int:plan_id>', methods=['DELETE'])
@login_required
@swag_from({
    "parameters": [
        {
            "name": "plan_id",
            "in": "path",
            "required": True,
            "type": "integer",
            "description": "方案ID"
        }
    ],
    "responses": {
        200: {"description": "删除成功"},
        403: {"description": "无权限操作或公共方案不允许删除"},
        404: {"description": "方案不存在"},
        500: {"description": "服务器内部错误"}
    },
    "tags": ["方案管理"],
    "summary": "删除方案",
    "description": "删除指定ID的方案数据（软删除）"
})
def delete_plan(plan_id):
    """删除方案"""
    try:
        # 先检查方案是否存在且属于当前用户
        existing_plan = PlanDataService.get_plan_by_id(plan_id)
        if not existing_plan:
            return get_json_result(data=None, code=404, message="方案不存在")
        
        if existing_plan['user_id'] != current_user.id:
            return get_json_result(data=None, code=403, message="无权限操作此方案")
        
        # 检查owner是否为public，如果是则不允许删除
        if existing_plan.get('owner') == 'public':
            return get_json_result(data=None, code=403, message="公共方案不允许删除")
        
        result = PlanDataService.delete_plan(plan_id)
        
        if result:
            return get_json_result(data=None, message="删除成功")
        else:
            return get_json_result(data=None, code=500, message="删除失败")
            
    except Exception as e:
        logger.exception(f"Delete plan error: {str(e)}")
        return server_error_response(e)


@plan_bp.route('/get/<int:plan_id>', methods=['GET'])
@login_required
@swag_from({
    "parameters": [
        {
            "name": "plan_id",
            "in": "path",
            "required": True,
            "type": "integer",
            "description": "方案ID"
        }
    ],
    "responses": {
        200: {
            "description": "获取成功",
            "schema": {
                "type": "object",
                "properties": {
                    "code": {"type": "integer"},
                    "message": {"type": "string"},
                    "data": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "integer"},
                            "scene": {"type": "string"},
                            "owner": {"type": "string"},
                            "plan": {
                                "type": "array", 
                                "items": {"type": "string"}, 
                                "description": "方案内容列表"
                            },
                            "user_id": {"type": "string"},
                            "tenant_id": {"type": "integer"},
                            "status": {"type": "string"},
                            "create_date": {"type": "string"},
                            "update_date": {"type": "string"}
                        }
                    }
                }
            }
        },
        404: {"description": "方案不存在"},
        500: {"description": "服务器内部错误"}
    },
    "tags": ["方案管理"],
    "summary": "获取方案详情",
    "description": "根据ID获取方案的详细信息"
})
def get_plan(plan_id):
    """获取方案详情"""
    try:
        plan = PlanDataService.get_plan_by_id(plan_id)
        
        if not plan:
            return get_json_result(data=None, code=404, message="方案不存在")
        
        if plan['user_id'] != current_user.id:
            return get_json_result(data=None, code=403, message="无权限访问此方案")
        
        return get_json_result(data=plan, message="获取成功")
        
    except Exception as e:
        logger.exception(f"Get plan error: {str(e)}")
        return server_error_response(e)


@plan_bp.route('/list', methods=['GET'])
@login_required
@swag_from({
    "parameters": [
        {
            "name": "page",
            "in": "query",
            "type": "integer",
            "default": 1,
            "description": "页码"
        },
        {
            "name": "page_size",
            "in": "query",
            "type": "integer",
            "default": 20,
            "description": "每页数量"
        },
        {
            "name": "user_id",
            "in": "query",
            "type": "string",
            "required": True,
            "description": "用户ID"
        },
        {
            "name": "tenant_id",
            "in": "query",
            "type": "integer",
            "description": "租户ID"
        },
        {
            "name": "scene",
            "in": "query",
            "type": "string",
            "description": "场景名称（模糊搜索）"
        }
    ],
    "responses": {
        200: {
            "description": "获取成功",
            "schema": {
                "type": "object",
                "properties": {
                    "code": {"type": "integer"},
                    "message": {"type": "string"},
                    "data": {
                        "type": "object",
                        "properties": {
                            "total": {"type": "integer"},
                            "page": {"type": "integer"},
                            "page_size": {"type": "integer"},
                            "data": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "id": {"type": "integer"},
                                        "scene": {"type": "string"},
                                        "owner": {"type": "string"},
                                        "plan": {
                                            "type": "array", 
                                            "items": {"type": "string"}, 
                                            "description": "方案内容列表"
                                        },
                                        "user_id": {"type": "string"},
                                        "tenant_id": {"type": "integer"},
                                        "status": {"type": "string"},
                                        "create_date": {"type": "string"},
                                        "update_date": {"type": "string"}
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
        500: {"description": "服务器内部错误"}
    },
    "tags": ["方案管理"],
    "summary": "获取方案列表",
    "description": "获取当前用户的方案列表，支持分页和过滤"
})
def list_plans():
    """获取方案列表"""
    try:
        page = request.args.get('page', 1, type=int)
        page_size = request.args.get('page_size', 20, type=int)
        user_id = request.args.get('user_id', type=str)
        tenant_id = request.args.get('tenant_id', type=int)
        scene = request.args.get('scene')
        
        if not user_id:
            return get_json_result(data=None, code=400, message="缺少必要参数: user_id")
        
        result = PlanDataService.get_plans_by_user(
            user_id=user_id,
            tenant_id=tenant_id,
            scene=scene,
            page=page,
            page_size=page_size
        )
        
        return get_json_result(data=result, message="获取成功")
        
    except Exception as e:
        logger.exception(f"List plans error: {str(e)}")
        return server_error_response(e)


@plan_bp.route('/search', methods=['GET'])
@login_required
@swag_from({
    "parameters": [
        {
            "name": "keyword",
            "in": "query",
            "type": "string",
            "required": True,
            "description": "搜索关键词"
        },
        {
            "name": "page",
            "in": "query",
            "type": "integer",
            "default": 1,
            "description": "页码"
        },
        {
            "name": "page_size",
            "in": "query",
            "type": "integer",
            "default": 20,
            "description": "每页数量"
        },
        {
            "name": "tenant_id",
            "in": "query",
            "type": "integer",
            "description": "租户ID"
        }
    ],
    "responses": {
        200: {
            "description": "搜索成功",
            "schema": {
                "type": "object",
                "properties": {
                    "code": {"type": "integer"},
                    "message": {"type": "string"},
                    "data": {
                        "type": "object",
                        "properties": {
                            "total": {"type": "integer"},
                            "page": {"type": "integer"},
                            "page_size": {"type": "integer"},
                            "keyword": {"type": "string"},
                            "data": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "id": {"type": "integer"},
                                        "scene": {"type": "string"},
                                        "owner": {"type": "string"},
                                        "plan": {
                                            "type": "array", 
                                            "items": {"type": "string"}, 
                                            "description": "方案内容列表"
                                        },
                                        "user_id": {"type": "string"},
                                        "tenant_id": {"type": "integer"},
                                        "status": {"type": "string"},
                                        "create_date": {"type": "string"},
                                        "update_date": {"type": "string"}
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
        400: {"description": "请求参数错误"},
        500: {"description": "服务器内部错误"}
    },
    "tags": ["方案管理"],
    "summary": "搜索方案",
    "description": "根据关键词搜索用户的方案，支持在场景和方案内容中搜索"
})
def search_plans():
    """搜索方案"""
    try:
        keyword = request.args.get('keyword')
        if not keyword:
            return get_json_result(data=None, code=400, message="搜索关键词不能为空")
        
        page = request.args.get('page', 1, type=int)
        page_size = request.args.get('page_size', 20, type=int)
        tenant_id = request.args.get('tenant_id', type=int)
        
        result = PlanDataService.search_plans(
            user_id=current_user.id,
            keyword=keyword,
            tenant_id=tenant_id,
            page=page,
            page_size=page_size
        )
        
        return get_json_result(data=result, message="搜索成功")
        
    except Exception as e:
        logger.exception(f"Search plans error: {str(e)}")
        return server_error_response(e)


@plan_bp.route('/get_by_scene', methods=['GET'])
@login_required
@swag_from({
    "parameters": [
        {
            "name": "scene",
            "in": "query",
            "type": "string",
            "required": True,
            "description": "场景名称"
        }
    ],
    "responses": {
        200: {
            "description": "获取成功",
            "schema": {
                "type": "object",
                "properties": {
                    "code": {"type": "integer"},
                    "message": {"type": "string"},
                    "data": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "integer"},
                            "scene": {"type": "string"},
                            "owner": {"type": "string"},
                            "plan": {
                                "type": "array", 
                                "items": {"type": "string"}, 
                                "description": "方案内容列表"
                            },
                            "user_id": {"type": "string"},
                            "tenant_id": {"type": "integer"},
                            "status": {"type": "string"},
                            "create_date": {"type": "string"},
                            "update_date": {"type": "string"}
                        }
                    }
                }
            }
        },
        400: {"description": "请求参数错误"},
        404: {"description": "方案不存在"},
        500: {"description": "服务器内部错误"}
    },
    "tags": ["方案管理"],
    "summary": "根据场景获取方案",
    "description": "根据场景名称获取用户的特定方案"
})
def get_plan_by_scene():
    """根据场景获取方案"""
    try:
        scene = request.args.get('scene')
        if not scene:
            return get_json_result(data=None, code=400, message="方案名称不能为空")
        
        plan = PlanDataService.get_plan_by_scene_and_user(scene, current_user.id)
        
        if not plan:
            return get_json_result(data=None, code=404, message="未找到该场景的方案")
        
        return get_json_result(data=plan, message="获取成功")
        
    except Exception as e:
        logger.exception(f"Get plan by scene error: {str(e)}")
        return server_error_response(e)


@plan_bp.route('/get_id_by_scene', methods=['GET'])
@login_required
@swag_from({
    "parameters": [
        {
            "name": "scene",
            "in": "query",
            "type": "string",
            "required": True,
            "description": "场景名称"
        }
    ],
    "responses": {
        200: {
            "description": "获取成功",
            "schema": {
                "type": "object",
                "properties": {
                    "code": {"type": "integer"},
                    "message": {"type": "string"},
                    "data": {
                        "type": "object",
                        "properties": {
                            "id": {"type": ["integer", "null"], "description": "方案ID，如果不存在则为null"}
                        }
                    }
                }
            }
        },
        400: {"description": "请求参数错误"},
        500: {"description": "服务器内部错误"}
    },
    "tags": ["方案管理"],
    "summary": "根据场景获取方案ID",
    "description": "根据场景名称获取用户的特定方案ID，如果不存在则返回null"
})
def get_plan_id_by_scene():
    """根据场景获取方案ID"""
    try:
        scene = request.args.get('scene')
        if not scene:
            return get_json_result(data=None, code=400, message="方案名称不能为空")
        
        plan_id = PlanDataService.get_plan_id_by_scene_and_user(scene, current_user.id)
        
        return get_json_result(data={"id": plan_id}, message="获取成功")
        
    except Exception as e:
        logger.exception(f"Get plan id by scene error: {str(e)}")
        return server_error_response(e)