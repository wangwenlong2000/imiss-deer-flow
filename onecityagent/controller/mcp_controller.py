# -*- coding: utf-8 -*-
# MCP Controller for OneCity Agent

import json
import os
import asyncio
import logging
from pathlib import Path
from typing import Dict, Optional

from flask import Blueprint, jsonify, request
from flasgger import swag_from

from onecityagent.config import PROJECT_ROOT, MCPServerConfig
from onecityagent.utils.api_utils import get_json_result, server_error_response
from onecityagent.enums import McpConnectionType

# 设置日志记录器
logger = logging.getLogger(__name__)

# 创建Blueprint
mcp_controller = Blueprint('mcp_controller', __name__, url_prefix='/mcp')

# MCP配置文件路径
MCP_CONFIG_PATH = PROJECT_ROOT / "config" / "mcp.json"


def load_mcp_config() -> Dict:
    """加载MCP配置文件"""
    if not MCP_CONFIG_PATH.exists():
        return {"mcpServers": {}}
    
    try:
        with open(MCP_CONFIG_PATH, 'r') as f:
            return json.load(f)
    except Exception as e:
        return {"mcpServers": {}}


def save_mcp_config(config: Dict) -> bool:
    """保存MCP配置文件"""
    try:
        # 确保config目录存在
        os.makedirs(os.path.dirname(MCP_CONFIG_PATH), exist_ok=True)
        
        with open(MCP_CONFIG_PATH, 'w') as f:
            json.dump(config, f, indent=2)
        return True
    except Exception as e:
        return False


@mcp_controller.route('/', methods=['GET'])
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
            "name": "pageSize",
            "in": "query",
            "type": "integer",
            "default": 10,
            "description": "每页数量"
        },
        {
            "name": "search",
            "in": "query",
            "type": "string",
            "description": "搜索关键词"
        }
    ],
    "responses": {
        200: {
            "description": "MCP配置信息",
            "schema": {
                "type": "object",
                "properties": {
                    "data": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "id": {"type": "string"},
                                "type": {"type": "string"},
                                "url": {"type": "string"},
                                "command": {"type": "string"},
                                "args": {"type": "array", "items": {"type": "string"}}
                            }
                        }
                    },
                    "total": {"type": "integer"},
                    "page": {"type": "integer"},
                    "pageSize": {"type": "integer"}
                }
            }
        },
        500: {
            "description": "服务器错误",
            "schema": {
                "type": "object",
                "properties": {
                    "error": {"type": "string"}
                }
            }
        }
    }
})
def get_mcp_config():
    """获取MCP配置信息（支持分页和搜索）"""
    try:
        # 获取查询参数
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('pageSize', 10))
        search = request.args.get('search', '').strip()
        
        # 加载配置
        config = load_mcp_config()
        
        # 转换为列表格式，添加id字段并清理数据
        servers = []
        for server_id, server_config in config.get('mcpServers', {}).items():
            server = {
                'id': server_id,
                'type': server_config.get('type', ''),
                'url': server_config.get('url', ''),
                'command': server_config.get('command', ''),
                'args': server_config.get('args', [])
            }
            # 不再移除None值，确保所有字段都存在
            servers.append(server)
        
        # 搜索过滤
        if search:
            filtered_servers = []
            search_lower = search.lower()
            for server in servers:
                if (search_lower in server['id'].lower() or
                    search_lower in server.get('type', '').lower() or
                    search_lower in server.get('url', '').lower() or
                    search_lower in server.get('command', '').lower()):
                    filtered_servers.append(server)
            servers = filtered_servers
        
        # 计算分页
        total = len(servers)
        start_index = (page - 1) * page_size
        end_index = start_index + page_size
        paginated_servers = servers[start_index:end_index]
        
        return get_json_result({
            'data': paginated_servers,
            'total': total,
            'page': page,
            'pageSize': page_size
        })
    except Exception as e:
        logger.error(f"Error getting MCP config: {str(e)}", exc_info=True)
        return server_error_response(e)


@mcp_controller.route('/', methods=['PUT'])
@swag_from({
    "parameters": [
        {
            "name": "body",
            "in": "body",
            "required": True,
            "schema": {
                "type": "object",
                "properties": {
                    "mcpServers": {
                        "type": "object",
                        "additionalProperties": {
                            "type": "object",
                            "properties": {
                                "type": {"type": "string"},
                                "url": {"type": "string"},
                                "command": {"type": "string"},
                                "args": {"type": "array", "items": {"type": "string"}}
                            },
                            "required": ["type"]
                        }
                    }
                },
                "required": ["mcpServers"]
            }
        }
    ],
    "responses": {
        200: {
            "description": "更新成功",
            "schema": {
                "type": "object",
                "properties": {
                    "success": {"type": "boolean"},
                    "message": {"type": "string"}
                }
            }
        },
        400: {
            "description": "请求错误",
            "schema": {
                "type": "object",
                "properties": {
                    "error": {"type": "string"}
                }
            }
        },
        500: {
            "description": "服务器错误",
            "schema": {
                "type": "object",
                "properties": {
                    "error": {"type": "string"}
                }
            }
        }
    }
})
def update_mcp_config():
    """更新MCP配置信息"""
    try:
        data = request.json
        
        # 检查是否使用前端格式 mcp_servers
        if data and 'mcp_servers' in data:
            # 将前端格式转换为后端格式
            converted_data = {
                'mcpServers': data['mcp_servers']
            }
            data = converted_data
            logger.info("已将前端 mcp_servers 格式转换为后端 mcpServers 格式")
        
        if not data or 'mcpServers' not in data:
            logger.warning("Missing required parameter: mcpServers in MCP config update")
            return get_json_result(data=None, code=400, message="缺少必要参数: mcpServers 或 mcp_servers")
        
        # 验证配置格式（按连接类型枚举校验）
        for server_id, server_config in data['mcpServers'].items():
            conn_type = server_config.get('connection_type') or server_config.get('type')
            if not conn_type:
                logger.warning(f"Missing required parameter 'connection_type' for server {server_id}")
                return get_json_result(data=None, code=400, message=f"服务器 {server_id} 缺少必要参数: connection_type")
            if not McpConnectionType.is_valid(conn_type):
                logger.warning(f"Invalid connection_type '{conn_type}' for server {server_id}")
                return get_json_result(data=None, code=400, message=f"无效的连接类型: {conn_type}，只支持: {', '.join(McpConnectionType.get_valid_values())}")
            
            # 连接类型对应的必填项校验
            if conn_type == McpConnectionType.SSE.value:
                if 'url' not in server_config:
                    logger.warning(f"Missing required parameter 'url' for SSE server {server_id}")
                    return get_json_result(data=None, code=400, message=f"SSE类型服务器 {server_id} 缺少必要参数: url")
            if conn_type == McpConnectionType.STDIO.value:
                if 'command' not in server_config:
                    logger.warning(f"Missing required parameter 'command' for STDIO server {server_id}")
                    return get_json_result(data=None, code=400, message=f"STDIO类型服务器 {server_id} 缺少必要参数: command")
            
            # 可选参数填充
            if 'args' not in server_config:
                server_config['args'] = []
        
        # 加载当前配置
        config = load_mcp_config()
        
        # 检查服务器ID是否已存在
        for server_id in data['mcpServers']:
            if 'mcpServers' in config and server_id in config['mcpServers']:
                logger.warning(f"Server ID  already exists")
                return get_json_result(data=None, code=400, message=f"服务器ID {server_id} 已存在")
        
        # 如果mcpServers不存在，创建它
        if 'mcpServers' not in config:
            config['mcpServers'] = {}
        
        # 添加新的服务器配置
        for server_id, server_config in data['mcpServers'].items():
            config['mcpServers'][server_id] = server_config
        
        # 保存配置
        if save_mcp_config(config):
            return get_json_result(data={"success": True}, message="MCP配置更新成功")
        else:
            return get_json_result(data=None, code=500, message="保存配置失败")
    except Exception as e:
        logger.error(f"Error updating MCP config: {str(e)}", exc_info=True)
        return server_error_response(e)


@mcp_controller.route('/<server_id>', methods=['GET'])
@swag_from({
    "parameters": [
        {
            "name": "server_id",
            "in": "path",
            "required": True,
            "type": "string",
            "description": "MCP服务器ID"
        }
    ],
    "responses": {
        200: {
            "description": "MCP服务器配置",
            "schema": {
                "type": "object",
                "properties": {
                    "type": {"type": "string"},
                    "url": {"type": "string"},
                    "command": {"type": "string"},
                    "args": {"type": "array", "items": {"type": "string"}}
                }
            }
        },
        404: {
            "description": "服务器不存在",
            "schema": {
                "type": "object",
                "properties": {
                    "error": {"type": "string"}
                }
            }
        },
        500: {
            "description": "服务器错误",
            "schema": {
                "type": "object",
                "properties": {
                    "error": {"type": "string"}
                }
            }
        }
    }
})
def get_server_config(server_id):
    """获取指定MCP服务器配置"""
    try:
        config = load_mcp_config()
        
        if 'mcpServers' not in config or server_id not in config['mcpServers']:
            return get_json_result(data=None, code=404, message=f"服务器 {server_id} 不存在")
        
        return get_json_result(data=config['mcpServers'][server_id])
    except Exception as e:
        logger.error(f"Error getting MCP server {server_id}: {str(e)}", exc_info=True)
        return server_error_response(e)


@mcp_controller.route('/<server_id>', methods=['PUT'])
@swag_from({
    "parameters": [
        {
            "name": "server_id",
            "in": "path",
            "required": True,
            "type": "string",
            "description": "MCP服务器ID"
        },
        {
            "name": "body",
            "in": "body",
            "required": True,
            "schema": {
                "type": "object",
                "properties": {
                    "type": {
                        "type": "string",
                        "enum": ["sse", "stdio"],
                        "description": "MCP服务器类型"
                    },
                    "url": {
                        "type": "string",
                        "description": "MCP服务器URL (sse类型)"
                    },
                    "command": {
                        "type": "string",
                        "description": "要运行的命令 (stdio类型)"
                    },
                    "args": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "命令参数 (stdio类型)"
                    }
                },
                "required": ["type"]
            }
        }
    ],
    "responses": {
        200: {
            "description": "更新成功",
            "schema": {
                "type": "object",
                "properties": {
                    "message": {"type": "string"}
                }
            }
        },
        400: {
            "description": "请求错误",
            "schema": {
                "type": "object",
                "properties": {
                    "error": {"type": "string"}
                }
            }
        },
        404: {
            "description": "服务器不存在",
            "schema": {
                "type": "object",
                "properties": {
                    "error": {"type": "string"}
                }
            }
        },
        500: {
            "description": "服务器错误",
            "schema": {
                "type": "object",
                "properties": {
                    "error": {"type": "string"}
                }
            }
        }
    }
})
def update_server_config(server_id):
    """更新指定MCP服务器配置"""
    try:
        data = request.get_json()
        if not data:
            logger.warning(f"Invalid request data for server  update")
            return get_json_result(data=None, code=400, message="无效请求")

        # 验证连接类型（兼容旧字段type）
        conn_type = data.get("connection_type") or data.get("type")
        if not conn_type or not McpConnectionType.is_valid(conn_type):
            logger.warning(f"Invalid connection_type for server ")
            return get_json_result(data=None, code=400, message=f"无效的连接类型: {conn_type}，只支持: {', '.join(McpConnectionType.get_valid_values())}")
        
        # 必填项随连接类型校验
        if conn_type == McpConnectionType.SSE.value and not data.get("url"):
            logger.warning(f"Missing required parameter 'url' for SSE server {server_id}")
            return get_json_result(data=None, code=400, message=f"SSE类型服务器 {server_id} 缺少必要参数: url")
        if conn_type == McpConnectionType.STDIO.value and not data.get("command"):
            logger.warning(f"Missing required parameter 'command' for STDIO server {server_id}")
            return get_json_result(data=None, code=400, message=f"STDIO类型服务器 {server_id} 缺少必要参数: command")

        # 加载当前配置
        config = load_mcp_config()
        
        if 'mcpServers' not in config or server_id not in config['mcpServers']:
            return get_json_result(data=None, code=404, message=f"服务器 {server_id} 不存在")

        # 更新服务器配置
        config['mcpServers'][server_id] = {
            "type": conn_type,
            "url": data.get("url", ""),
            "command": data.get("command", ""),
            "args": data.get("args", [])
        }

        # 保存更新的配置
        if not save_mcp_config(config):
            return get_json_result(data=None, code=500, message="保存配置失败")

        return get_json_result(data={"success": True}, message=f"服务器 {server_id} 更新成功")
    except Exception as e:
        logger.error(f"Error updating MCP server {server_id}: {str(e)}", exc_info=True)
        return server_error_response(e)


@mcp_controller.route('/<server_id>', methods=['DELETE'])
@swag_from({
    "parameters": [
        {
            "name": "server_id",
            "in": "path",
            "required": True,
            "type": "string",
            "description": "MCP服务器ID"
        }
    ],
    "responses": {
        200: {
            "description": "删除成功",
            "schema": {
                "type": "object",
                "properties": {
                    "success": {"type": "boolean"},
                    "message": {"type": "string"}
                }
            }
        },
        404: {
            "description": "服务器不存在",
            "schema": {
                "type": "object",
                "properties": {
                    "error": {"type": "string"}
                }
            }
        },
        500: {
            "description": "服务器错误",
            "schema": {
                "type": "object",
                "properties": {
                    "error": {"type": "string"}
                }
            }
        }
    }
})
def delete_server_config(server_id):
    """删除指定MCP服务器配置"""
    try:
        config = load_mcp_config()
        
        if 'mcpServers' not in config or server_id not in config['mcpServers']:
            return get_json_result(data=None, code=404, message=f"服务器 {server_id} 不存在")
        
        # 删除服务器配置
        del config['mcpServers'][server_id]
        
        # 保存配置
        if save_mcp_config(config):
            return get_json_result(data={"success": True}, message=f"服务器 {server_id} 配置删除成功")
        else:
            return get_json_result(data=None, code=500, message="保存配置失败")
    except Exception as e:
        logger.error(f"Error deleting MCP server {server_id}: {str(e)}", exc_info=True)
        return server_error_response(e)


@mcp_controller.route('/create', methods=['POST'])
@swag_from({
    "parameters": [
        {
            "name": "body",
            "in": "body",
            "required": True,
            "schema": {
                "type": "object",
                "properties": {
                    "mcp_servers": {
                        "type": "object",
                        "additionalProperties": {
                            "type": "object",
                            "properties": {
                                "type": {"type": "string", "enum": ["sse", "stdio"]},
                                "url": {"type": "string"},
                                "command": {"type": "string"},
                                "args": {"type": "array", "items": {"type": "string"}}
                            },
                            "required": ["type"]
                        }
                    },
                    "mcpServers": {
                        "type": "object",
                        "additionalProperties": {
                            "type": "object",
                            "properties": {
                                "type": {"type": "string", "enum": ["sse", "stdio"]},
                                "url": {"type": "string"},
                                "command": {"type": "string"},
                                "args": {"type": "array", "items": {"type": "string"}}
                            },
                            "required": ["type"]
                        }
                    }
                }
            }
        }
    ],
    "responses": {
        200: {
            "description": "创建成功",
            "schema": {
                "type": "object",
                "properties": {
                    "success": {"type": "boolean"},
                    "message": {"type": "string"}
                }
            }
        },
        400: {
            "description": "请求错误",
            "schema": {
                "type": "object",
                "properties": {
                    "error": {"type": "string"}
                }
            }
        },
        500: {
            "description": "服务器错误",
            "schema": {
                "type": "object",
                "properties": {
                    "error": {"type": "string"}
                }
            }
        }
    }
})
def create_mcp_config():
    """新增MCP配置信息"""
    try:
        data = request.json
        
        # 检查是否使用前端格式 mcp_servers
        if data and 'mcp_servers' in data:
            # 将前端格式转换为后端格式
            converted_data = {
                'mcpServers': data['mcp_servers']
            }
            data = converted_data
            logger.info("已将前端 mcp_servers 格式转换为后端 mcpServers 格式")
        
        if not data or ('mcpServers' not in data):
            logger.warning("Missing required parameter: mcpServers in MCP config create")
            return get_json_result(data=None, code=400, message="缺少必要参数: mcpServers 或 mcp_servers")
        
        # 验证配置格式并处理连接类型（兼容旧字段type）
        for server_id, server_config in data['mcpServers'].items():
            conn_type = server_config.get('connection_type') or server_config.get('type')
            if not conn_type:
                logger.warning(f"Missing required parameter 'connection_type' for server {server_id}")
                return get_json_result(data=None, code=400, message=f"服务器 {server_id} 缺少必要参数: connection_type")
            if not McpConnectionType.is_valid(conn_type):
                logger.warning(f"Invalid connection_type  for server {server_id}")
                return get_json_result(data=None, code=400, message=f"无效的连接类型，只支持: {', '.join(McpConnectionType.get_valid_values())}")
            
            # 连接类型对应的必填项校验
            if conn_type == McpConnectionType.SSE.value:
                if 'url' not in server_config:
                    logger.warning(f"Missing required parameter 'url' for SSE server {server_id}")
                    return get_json_result(data=None, code=400, message=f"SSE类型服务器 {server_id} 缺少必要参数: url")
            if conn_type == McpConnectionType.STDIO.value:
                if 'command' not in server_config:
                    logger.warning(f"Missing required parameter 'command' for STDIO server {server_id}")
                    return get_json_result(data=None, code=400, message=f"STDIO类型服务器 {server_id} 缺少必要参数: command")
            
            # 可选参数填充
            if 'args' not in server_config:
                server_config['args'] = []
        
        # 加载当前配置
        config = load_mcp_config()
        
        # 检查服务器ID是否已存在
        for server_id in data['mcpServers']:
            if 'mcpServers' in config and server_id in config['mcpServers']:
                logger.warning(f"Server ID already exists")
                return get_json_result(data=None, code=400, message=f"服务器ID {server_id} 已存在")
        
        # 如果mcpServers不存在，创建它
        if 'mcpServers' not in config:
            config['mcpServers'] = {}
        
        # 添加新的服务器配置
        for server_id, server_config in data['mcpServers'].items():
            config['mcpServers'][server_id] = server_config
        
        # 保存配置
        if save_mcp_config(config):
            return get_json_result(data={"success": True}, message="MCP配置新增成功")
        else:
            return get_json_result(data=None, code=500, message="保存配置失败")
    except Exception as e:
        logger.error(f"Error creating MCP config: {str(e)}", exc_info=True)
        return server_error_response(e)


@mcp_controller.route('/reload', methods=['POST'])
@swag_from({
    "parameters": [
        {
            "name": "server_id",
            "in": "query",
            "required": False,
            "type": "string",
            "description": "要重新加载的MCP服务器ID，不提供则重新加载所有服务器"
        }
    ],
    "responses": {
        200: {
            "description": "重新加载成功",
            "schema": {
                "type": "object",
                "properties": {
                    "success": {"type": "boolean"},
                    "message": {"type": "string"}
                }
            }
        },
        404: {
            "description": "服务器不存在",
            "schema": {
                "type": "object",
                "properties": {
                    "error": {"type": "string"}
                }
            }
        },
        500: {
            "description": "服务器错误",
            "schema": {
                "type": "object",
                "properties": {
                    "error": {"type": "string"}
                }
            }
        }
    }
})
def reload_mcp_config():
    """重新加载MCP配置"""
    try:
        # 获取要重新加载的服务器ID，如果不提供则重新加载所有服务器
        server_id = request.args.get('server_id')
        
        # 如果指定了服务器ID，检查该服务器是否存在
        if server_id:
            config = load_mcp_config()
            if 'mcpServers' not in config or server_id not in config['mcpServers']:
                return get_json_result(data=None, code=404, message=f"服务器 {server_id} 不存在")
        
        try:
            # 尝试从app_dev.py导入runner实例
            try:
                from app import runner
                logger.info("从app.py导入runner实例成功")
            except ImportError:
                # 如果app_dev.py导入失败，尝试从app.py导入
                try:
                    from app_dev import runner
                    logger.info("从app_dev.py导入runner实例成功")
                except ImportError as app_import_error:
                    logger.error(f"导入runner实例失败: {app_import_error}")
                    return get_json_result(data=None, code=500, message="应用程序未正确初始化，无法重新加载MCP配置")
            
            # 异步调用reload_mcp_config方法
            try:
                asyncio.run(runner.reload_mcp_config(server_id))
                
                message = f"MCP服务器  配置已重新加载" if server_id else "所有MCP服务器配置已重新加载"
                logger.info(message)
                return get_json_result(data={"success": True}, message=message)
            except ConnectionError as conn_error:
                error_message = str(conn_error)
                logger.error(f"连接到MCP服务器时出错: {error_message}")
                return get_json_result(data=None, code=500, message=f"连接到MCP服务器失败: {error_message}")
        except Exception as reload_error:
            error_message = str(reload_error)
            logger.error(f"重新加载MCP配置时出错: {error_message}")
            
            # 根据错误类型提供更友好的错误信息
            if "Agent初始化失败" in error_message:
                return get_json_result(data=None, code=500, message="Agent初始化失败，请检查服务器配置和连接")
            elif "连接到SSE服务器" in error_message and "失败" in error_message:
                return get_json_result(data=None, code=500, message=f"连接到MCP服务器失败: {error_message}")
            else:
                return get_json_result(data=None, code=500, message=f"重新加载MCP配置失败: {error_message}")
    except Exception as e:
        logger.error(f"Error reloading MCP config: {str(e)}", exc_info=True)
        return server_error_response(e)