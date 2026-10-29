# -*- coding: utf-8 -*-
# 配置控制器 - 提供MCP和A2A配置的Web API接口

import logging
from flask import Blueprint, jsonify, request
from flasgger import swag_from

from onecityagent.service.mcp_service import mcp_config_server
from onecityagent.service.a2a_service import a2a_config_server
from onecityagent.utils.api_utils import get_json_result, server_error_response

logger = logging.getLogger(__name__)

# 创建Blueprint
config_controller = Blueprint('config_controller', __name__, url_prefix='/config')


# ==================== MCP配置相关API ====================

@config_controller.route('/mcp', methods=['POST'])
@swag_from({
    "tags": ["MCP配置"],
    "summary": "创建MCP配置",
    "parameters": [
        {
            "name": "body",
            "in": "body",
            "required": True,
            "schema": {
                "type": "object",
                "properties": {
                    "agent_id": {"type": "string", "description": "智能体ID"},
                    "mcp_server_name": {"type": "string", "description": "MCP服务器名称"},
                    "connection_type": {"type": "string", "description": "连接类型"},
                    "server_url": {"type": "string", "description": "服务器URL"},
                    "command": {"type": "string", "description": "执行命令"},
                    "args": {"type": "array", "items": {"type": "string"}, "description": "命令参数"},
                    "status": {"type": "integer", "description": "配置状态，1启用0关闭", "default": 1},
                    "mcp_tools_info": {"type": "string", "description": "MCP工具信息"}
                },
                "required": ["agent_id", "mcp_server_name", "connection_type"]
            }
        }
    ],
    "responses": {
        200: {"description": "创建成功"},
        400: {"description": "请求参数错误"},
        500: {"description": "服务器错误"}
    }
})
def create_mcp_config():
    """创建MCP配置"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'message': '请求数据不能为空'}), 400
        
        result = mcp_config_server.create_config(data)
        if result['success']:
            return get_json_result(result['data'])
        else:
            return jsonify(result), 400
            
    except Exception as e:
        logger.error(f"Error creating MCP config: {str(e)}", exc_info=True)
        return server_error_response(e)


@config_controller.route('/mcp/<string:config_id>', methods=['GET'])
@swag_from({
    "tags": ["MCP配置"],
    "summary": "获取MCP配置",
    "parameters": [
        {
            "name": "config_id",
            "in": "path",
            "type": "string",
            "required": True,
            "description": "配置ID（UUID）"
        }
    ],
    "responses": {
        200: {"description": "获取成功"},
        404: {"description": "配置不存在"},
        500: {"description": "服务器错误"}
    }
})
def get_mcp_config(config_id):
    """获取MCP配置"""
    try:
        result = mcp_config_server.get_config(config_id)
        if result['success']:
            return get_json_result(result['data'])
        else:
            return jsonify(result), 404
            
    except Exception as e:
        logger.error(f"Error getting MCP config: {str(e)}", exc_info=True)
        return server_error_response(e)


@config_controller.route('/mcp', methods=['GET'])
@swag_from({
    "tags": ["MCP配置"],
    "summary": "获取MCP配置列表",
    "parameters": [
        {"name": "page", "in": "query", "type": "integer", "default": 1, "description": "页码"},
        {"name": "page_size", "in": "query", "type": "integer", "default": 10, "description": "每页数量"},
        {"name": "agent_id", "in": "query", "type": "string", "description": "智能体ID过滤"},
        {"name": "status", "in": "query", "type": "integer", "description": "状态过滤，1启用0关闭"},
        {"name": "search", "in": "query", "type": "string", "description": "搜索关键词"}
    ],
    "responses": {
        200: {"description": "获取成功"},
        500: {"description": "服务器错误"}
    }
})
def get_mcp_configs():
    """获取MCP配置列表"""
    try:
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 10))
        agent_id = request.args.get('agent_id')
        status = request.args.get('status')
        search = request.args.get('search')
        
        # 转换status参数
        if status is not None:
            try:
                status = int(status)
            except ValueError:
                status = None
        
        result = mcp_config_server.get_configs(
            page=page, 
            page_size=page_size, 
            agent_id=agent_id, 
            status=status, 
            search=search
        )
        
        if result['success']:
            return get_json_result(result['data'])
        else:
            return jsonify(result), 500
            
    except Exception as e:
        logger.error(f"Error getting MCP configs: {str(e)}", exc_info=True)
        return server_error_response(e)


@config_controller.route('/mcp/<string:config_id>', methods=['PUT'])
@swag_from({
    "tags": ["MCP配置"],
    "summary": "更新MCP配置",
    "parameters": [
        {
            "name": "config_id",
            "in": "path",
            "type": "string",
            "required": True,
            "description": "配置ID（UUID）"
        },
        {
            "name": "body",
            "in": "body",
            "required": True,
            "schema": {
                "type": "object",
                "properties": {
                    "agent_id": {"type": "string"},
                    "mcp_server_name": {"type": "string"},
                    "connection_type": {"type": "string"},
                    "server_url": {"type": "string"},
                    "command": {"type": "string"},
                    "args": {"type": "array", "items": {"type": "string"}},
                    "status": {"type": "integer", "description": "配置状态，1启用0关闭"},
                    "mcp_tools_info": {"type": "string"}
                }
            }
        }
    ],
    "responses": {
        200: {"description": "更新成功"},
        404: {"description": "配置不存在"},
        500: {"description": "服务器错误"}
    }
})
def update_mcp_config(config_id):
    """更新MCP配置"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'message': '请求数据不能为空'}), 400
        
        result = mcp_config_server.update_config(config_id, data)
        if result['success']:
            return get_json_result(result['data'])
        else:
            return jsonify(result), 404 if '不存在' in result['message'] else 400
            
    except Exception as e:
        logger.error(f"Error updating MCP config: {str(e)}", exc_info=True)
        return server_error_response(e)


@config_controller.route('/mcp/<string:config_id>', methods=['DELETE'])
@swag_from({
    "tags": ["MCP配置"],
    "summary": "删除MCP配置",
    "parameters": [
        {
            "name": "config_id",
            "in": "path",
            "type": "string",
            "required": True,
            "description": "配置ID（UUID）"
        }
    ],
    "responses": {
        200: {"description": "删除成功"},
        404: {"description": "配置不存在"},
        500: {"description": "服务器错误"}
    }
})
def delete_mcp_config(config_id):
    """删除MCP配置"""
    try:
        result = mcp_config_server.delete_config(config_id)
        if result['success']:
            return get_json_result({'message': result['message']})
        else:
            return jsonify(result), 404
            
    except Exception as e:
        logger.error(f"Error deleting MCP config: {str(e)}", exc_info=True)
        return server_error_response(e)


# ==================== A2A配置相关API ====================

@config_controller.route('/a2a', methods=['POST'])
@swag_from({
    "tags": ["A2A配置"],
    "summary": "创建A2A智能体配置",
    "parameters": [
        {
            "name": "body",
            "in": "body",
            "required": True,
            "schema": {
                "type": "object",
                "properties": {
                    "agent_name": {"type": "string", "description": "智能体名称"},
                    "agent_url": {"type": "string", "description": "智能体URL"},
                    "status": {"type": "integer", "description": "智能体状态，1启用0关闭", "default": 1},
                    "type": {"type": "integer", "description": "智能体类型，0系统内置，1用户新增", "default": 1},
                    "description": {"type": "string", "description": "智能体描述"}
                },
                "required": ["agent_name", "agent_url"]
            }
        }
    ],
    "responses": {
        200: {"description": "创建成功"},
        400: {"description": "请求参数错误"},
        500: {"description": "服务器错误"}
    }
})
def create_a2a_agent():
    """创建A2A智能体配置"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'message': '请求数据不能为空'}), 400
        
        result = a2a_config_server.create_agent(data)
        if result['success']:
            return get_json_result(result['data'])
        else:
            return jsonify(result), 400
            
    except Exception as e:
        logger.error(f"Error creating A2A agent: {str(e)}", exc_info=True)
        return server_error_response(e)


@config_controller.route('/a2a/<string:agent_id>', methods=['GET'])
@swag_from({
    "tags": ["A2A配置"],
    "summary": "获取A2A智能体配置",
    "parameters": [
        {
            "name": "agent_id",
            "in": "path",
            "type": "string",
            "required": True,
            "description": "智能体ID"
        }
    ],
    "responses": {
        200: {"description": "获取成功"},
        404: {"description": "智能体不存在"},
        500: {"description": "服务器错误"}
    }
})
def get_a2a_agent(agent_id):
    """获取A2A智能体配置"""
    try:
        result = a2a_config_server.get_agent(agent_id)
        if result['success']:
            return get_json_result(result['data'])
        else:
            return jsonify(result), 404
            
    except Exception as e:
        logger.error(f"Error getting A2A agent: {str(e)}", exc_info=True)
        return server_error_response(e)


@config_controller.route('/a2a', methods=['GET'])
@swag_from({
    "tags": ["A2A配置"],
    "summary": "获取A2A智能体配置列表",
    "parameters": [
        {"name": "page", "in": "query", "type": "integer", "default": 1, "description": "页码"},
        {"name": "page_size", "in": "query", "type": "integer", "default": 10, "description": "每页数量"},
        {"name": "status", "in": "query", "type": "integer", "description": "状态过滤，1启用0关闭"},
        {"name": "type", "in": "query", "type": "integer", "description": "类型过滤，0系统内置，1用户新增"},
        {"name": "search", "in": "query", "type": "string", "description": "搜索关键词"},
    ],
    "responses": {
        200: {"description": "获取成功"},
        500: {"description": "服务器错误"}
    }
})
def get_a2a_agents():
    """获取A2A智能体配置列表"""
    try:
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 10))
        status = request.args.get('status')
        a_type = request.args.get('type')
        search = request.args.get('search')
        
        # 转换status参数
        if status is not None:
            try:
                status = int(status)
            except ValueError:
                status = None
        
        # 转换type参数
        if a_type is not None:
            try:
                a_type = int(a_type)
            except ValueError:
                a_type = None

        result = a2a_config_server.get_agents(
            page=page, 
            page_size=page_size, 
            status=status, 
            type=a_type,
            search=search
        )
        
        if result['success']:
            return get_json_result(result['data'])
        else:
            return jsonify(result), 500
            
    except Exception as e:
        logger.error(f"Error getting A2A agents: {str(e)}", exc_info=True)
        return server_error_response(e)


@config_controller.route('/a2a/<string:agent_id>', methods=['PUT'])
@swag_from({
    "tags": ["A2A配置"],
    "summary": "更新A2A智能体配置",
    "parameters": [
        {
            "name": "agent_id",
            "in": "path",
            "type": "string",
            "required": True,
            "description": "智能体ID"
        },
        {
            "name": "body",
            "in": "body",
            "required": True,
            "schema": {
                "type": "object",
                "properties": {
                    "agent_name": {"type": "string"},
                    "agent_url": {"type": "string"},
                    "status": {"type": "integer", "description": "智能体状态，1启用0关闭"},
                    "type": {"type": "integer", "description": "智能体类型，0系统内置，1用户新增"},
                    "description": {"type": "string"}
                }
            }
        }
    ],
    "responses": {
        200: {"description": "更新成功"},
        404: {"description": "智能体不存在"},
        500: {"description": "服务器错误"}
    }
})
def update_a2a_agent(agent_id):
    """更新A2A智能体配置"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'message': '请求数据不能为空'}), 400
        
        result = a2a_config_server.update_agent(agent_id, data)
        if result['success']:
            return get_json_result(result['data'])
        else:
            return jsonify(result), 404 if '不存在' in result['message'] else 400
            
    except Exception as e:
        logger.error(f"Error updating A2A agent: {str(e)}", exc_info=True)
        return server_error_response(e)


@config_controller.route('/a2a/<string:agent_id>', methods=['DELETE'])
@swag_from({
    "tags": ["A2A配置"],
    "summary": "删除A2A智能体配置",
    "parameters": [
        {
            "name": "agent_id",
            "in": "path",
            "type": "string",
            "required": True,
            "description": "智能体ID"
        }
    ],
    "responses": {
        200: {"description": "删除成功"},
        404: {"description": "智能体不存在"},
        500: {"description": "服务器错误"}
    }
})
def delete_a2a_agent(agent_id):
    """删除A2A智能体配置"""
    try:
        result = a2a_config_server.delete_agent(agent_id)
        if result['success']:
            return get_json_result({'message': result['message']})
        else:
            return jsonify(result), 404
            
    except Exception as e:
        logger.error(f"Error deleting A2A agent: {str(e)}", exc_info=True)
        return server_error_response(e)


@config_controller.route('/a2a/<string:agent_id>/toggle', methods=['POST'])
@swag_from({
    "tags": ["A2A配置"],
    "summary": "切换A2A智能体状态",
    "parameters": [
        {
            "name": "agent_id",
            "in": "path",
            "type": "string",
            "required": True,
            "description": "智能体ID"
        }
    ],
    "responses": {
        200: {"description": "切换成功"},
        404: {"description": "智能体不存在"},
        500: {"description": "服务器错误"}
    }
})
def toggle_a2a_agent_status(agent_id):
    """切换A2A智能体状态"""
    try:
        result = a2a_config_server.toggle_agent_status(agent_id)
        if result['success']:
            return get_json_result(result['data'])
        else:
            return jsonify(result), 404
            
    except Exception as e:
        logger.error(f"Error toggling A2A agent status: {str(e)}", exc_info=True)
        return server_error_response(e)