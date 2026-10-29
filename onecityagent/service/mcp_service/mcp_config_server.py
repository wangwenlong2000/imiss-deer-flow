# -*- coding: utf-8 -*-
# MCP配置服务器 - 提供MCP配置的增删改查功能

import logging
import time
from datetime import datetime
from typing import List, Dict, Optional, Any
from peewee import DoesNotExist

from onecityagent.models.db_models import McpConfig, A2aAgent, DB
from onecityagent.utils import common_utils
from onecityagent.enums import StatusEnum, McpConnectionType

logger = logging.getLogger(__name__)


class McpConfigServer:
    """MCP配置服务器类"""
    
    def __init__(self):
        self.db = DB.get_instance()
    
    def create_config(self, config_data: Dict[str, Any]) -> Dict[str, Any]:
        """创建MCP配置
        
        Args:
            config_data: 配置数据字典
            
        Returns:
            创建结果字典
        """
        try:
            with self.db.atomic():
                agent_id = config_data.get('agent_id')
                connection_type = config_data.get('connection_type')
                status = config_data.get('status', StatusEnum.ENABLED.value)
                
                # 验证连接类型
                if connection_type and not McpConnectionType.is_valid(connection_type):
                    return {
                        'success': False,
                        'message': f'无效的连接类型: {connection_type}，只支持: {", ".join(McpConnectionType.get_valid_values())}',
                        'data': None
                    }
                
                # 验证状态值
                if status not in [StatusEnum.DISABLED.value, StatusEnum.ENABLED.value]:
                    return {
                        'success': False,
                        'message': f'无效的状态值: {status}，只支持: {StatusEnum.DISABLED.value}(禁用), {StatusEnum.ENABLED.value}(启用)',
                        'data': None
                    }
                
                # 验证智能体是否存在
                if agent_id:
                    try:
                        A2aAgent.get_by_id(agent_id)
                    except DoesNotExist:
                        return {
                            'success': False,
                            'message': f'智能体不存在: {agent_id}',
                            'data': None
                        }
                
                # 数量限制：同一智能体下启用的MCP配置不能超过5个
                if agent_id:
                    enabled_count = McpConfig.select().where(
                        (McpConfig.agent_id == agent_id) & (McpConfig.status == StatusEnum.ENABLED.value)
                    ).count()
                    if enabled_count >= 5:
                        return {
                            'success': False,
                            'message': f'当前智能体绑定的MCP数量已达上限（5个）',
                            'data': None
                        }
                
                # 验证同一智能体下MCP服务器名称不能重复
                mcp_server_name = config_data.get('mcp_server_name')
                if agent_id and mcp_server_name:
                    existing_name = McpConfig.select().where(
                        (McpConfig.agent_id == agent_id) & 
                        (McpConfig.mcp_server_name == mcp_server_name)
                    ).exists()
                    if existing_name:
                        return {
                            'success': False,
                            'message': f'该智能体下已存在名称为 "{mcp_server_name}" 的MCP配置',
                            'data': None
                        }
                
                # 验证同一智能体下服务器地址不能重复
                server_url = config_data.get('server_url')
                if agent_id and server_url:
                    existing_url = McpConfig.select().where(
                        (McpConfig.agent_id == agent_id) & 
                        (McpConfig.server_url == server_url)
                    ).exists()
                    if existing_url:
                        return {
                            'success': False,
                            'message': f'该智能体下已存在URL为 "{server_url}" 的MCP配置',
                            'data': None
                        }
                
                current_time = int(time.time())
                current_date = datetime.now()
                
                # 创建配置记录，config_id将自动生成UUID
                config = McpConfig.create(
                    agent_id=agent_id,
                    mcp_server_name=config_data.get('mcp_server_name'),
                    connection_type=connection_type,
                    server_url=config_data.get('server_url'),
                    command=config_data.get('command'),
                    args=config_data.get('args'),
                    status=status,
                    mcp_tools_info=config_data.get('mcp_tools_info'),
                    headers=config_data.get('headers'),
                    create_date=current_date,
                    update_date=current_date
                )
                
                logger.info(f"Created MCP config with ID: {config.config_id}")
                return {
                    'success': True,
                    'message': 'MCP配置创建成功',
                    'data': config.to_dict()
                }
                
        except Exception as e:
            logger.error(f"Error creating MCP config: {str(e)}", exc_info=True)
            return {
                'success': False,
                'message': f'创建MCP配置失败: {str(e)}',
                'data': None
            }
    
    def get_config(self, config_id: str) -> Dict[str, Any]:
        """获取单个MCP配置
        
        Args:
            config_id: 配置ID（UUID）
            
        Returns:
            配置数据字典
        """
        try:
            config = McpConfig.get_by_id(config_id)
            return {
                'success': True,
                'message': '获取MCP配置成功',
                'data': config.to_dict()
            }
        except DoesNotExist:
            return {
                'success': False,
                'message': f'MCP配置不存在: {config_id}',
                'data': None
            }
        except Exception as e:
            logger.error(f"Error getting MCP config {config_id}: {str(e)}", exc_info=True)
            return {
                'success': False,
                'message': f'获取MCP配置失败: {str(e)}',
                'data': None
            }
    
    def get_configs(self, page: int = 1, page_size: int = 10, 
                   agent_id: Optional[str] = None, 
                   status: Optional[bool] = None,
                   search: Optional[str] = None) -> Dict[str, Any]:
        """获取MCP配置列表（支持分页和过滤）
        
        Args:
            page: 页码
            page_size: 每页数量
            agent_id: 智能体ID过滤
            status: 状态过滤
            search: 搜索关键词
            
        Returns:
            配置列表数据字典
        """
        try:
            query = McpConfig.select()
            
            # 应用过滤条件
            if agent_id:
                query = query.where(McpConfig.agent_id == agent_id)
            if status is not None:
                query = query.where(McpConfig.status == status)
            if search:
                search_term = f'%{search}%'
                query = query.where(
                    (McpConfig.mcp_server_name.contains(search)) |
                    (McpConfig.agent_id.contains(search)) |
                    (McpConfig.connection_type.contains(search))
                )
            
            # 计算总数
            total = query.count()
            
            # 应用分页
            offset = (page - 1) * page_size
            configs = query.order_by(McpConfig.create_date.desc()).offset(offset).limit(page_size)
            
            # 转换为字典列表
            config_list = [config.to_dict() for config in configs]
            
            return {
                'success': True,
                'message': '获取MCP配置列表成功',
                'data': {
                    'configs': config_list,
                    'total': total,
                    'page': page,
                    'page_size': page_size
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting MCP configs: {str(e)}", exc_info=True)
            return {
                'success': False,
                'message': f'获取MCP配置列表失败: {str(e)}',
                'data': None
            }
    
    def update_config(self, config_id: str, config_data: Dict[str, Any]) -> Dict[str, Any]:
        """更新MCP配置
        
        Args:
            config_id: 配置ID（UUID）
            config_data: 更新的配置数据字典
            
        Returns:
            更新结果字典
        """
        try:
            with self.db.atomic():
                config = McpConfig.get_by_id(config_id)
                
                # 验证连接类型（如果要更新connection_type）
                if 'connection_type' in config_data:
                    connection_type = config_data['connection_type']
                    if connection_type and not McpConnectionType.is_valid(connection_type):
                        return {
                            'success': False,
                            'message': f'无效的连接类型: {connection_type}，只支持: {", ".join(McpConnectionType.get_valid_values())}',
                            'data': None
                        }
                
                # 验证状态值（如果要更新status）
                if 'status' in config_data:
                    status = config_data['status']
                    if status not in [StatusEnum.DISABLED.value, StatusEnum.ENABLED.value]:
                        return {
                            'success': False,
                            'message': f'无效的状态值: {status}，只支持: {StatusEnum.DISABLED.value}(禁用), {StatusEnum.ENABLED.value}(启用)',
                            'data': None
                        }
                
                # 验证智能体是否存在（如果要更新agent_id）
                if 'agent_id' in config_data:
                    agent_id = config_data['agent_id']
                    if agent_id:
                        try:
                            A2aAgent.get_by_id(agent_id)
                        except DoesNotExist:
                            return {
                                'success': False,
                                'message': f'智能体不存在: {agent_id}',
                                'data': None
                            }
                    config.agent_id = agent_id
                
                # 获取当前或更新后的agent_id
                current_agent_id = config_data.get('agent_id', config.agent_id)
                
                # 验证同一智能体下MCP服务器名称不能重复（如果要更新mcp_server_name）
                if 'mcp_server_name' in config_data and config_data['mcp_server_name']:
                    existing_name = McpConfig.select().where(
                        (McpConfig.agent_id == current_agent_id) & 
                        (McpConfig.mcp_server_name == config_data['mcp_server_name']) &
                        (McpConfig.config_id != config_id)
                    ).first()
                    if existing_name:
                        return {
                            'success': False,
                            'message': f'该智能体下已存在名称为 "{config_data["mcp_server_name"]}" 的MCP配置',
                            'data': None
                        }
                
                # 验证同一智能体下服务器地址不能重复（如果要更新server_url）
                if 'server_url' in config_data and config_data['server_url']:
                    existing_url = McpConfig.select().where(
                        (McpConfig.agent_id == current_agent_id) & 
                        (McpConfig.server_url == config_data['server_url']) &
                        (McpConfig.config_id != config_id)
                    ).first()
                    if existing_url:
                        return {
                            'success': False,
                            'message': f'该智能体下已存在URL为 "{config_data["server_url"]}" 的MCP配置',
                            'data': None
                        }
                
                # 更新其他字段
                if 'mcp_server_name' in config_data:
                    config.mcp_server_name = config_data['mcp_server_name']
                if 'connection_type' in config_data:
                    config.connection_type = config_data['connection_type']
                if 'server_url' in config_data:
                    config.server_url = config_data['server_url']
                if 'command' in config_data:
                    config.command = config_data['command']
                if 'args' in config_data:
                    config.args = config_data['args']
                if 'status' in config_data:
                    config.status = config_data['status']
                if 'mcp_tools_info' in config_data:
                    config.mcp_tools_info = config_data['mcp_tools_info']
                if 'headers' in config_data:
                    config.headers = config_data['headers']
                
                # 更新时间戳
                config.update_date = datetime.now()
                
                config.save()
                
                logger.info(f"Updated MCP config with ID: {config_id}")
                return {
                    'success': True,
                    'message': 'MCP配置更新成功',
                    'data': config.to_dict()
                }
                
        except DoesNotExist:
            return {
                'success': False,
                'message': f'MCP配置不存在: {config_id}',
                'data': None
            }
        except Exception as e:
            logger.error(f"Error updating MCP config {config_id}: {str(e)}", exc_info=True)
            return {
                'success': False,
                'message': f'更新MCP配置失败: {str(e)}',
                'data': None
            }
    
    def delete_config(self, config_id: str) -> Dict[str, Any]:
        """删除MCP配置
        
        Args:
            config_id: 配置ID（UUID）
            
        Returns:
            删除结果字典
        """
        try:
            with self.db.atomic():
                config = McpConfig.get_by_id(config_id)
                config.delete_instance()
                
                logger.info(f"Deleted MCP config with ID: {config_id}")
                return {
                    'success': True,
                    'message': 'MCP配置删除成功',
                    'data': None
                }
                
        except DoesNotExist:
            return {
                'success': False,
                'message': f'MCP配置不存在: {config_id}',
                'data': None
            }
        except Exception as e:
            logger.error(f"Error deleting MCP config {config_id}: {str(e)}", exc_info=True)
            return {
                'success': False,
                'message': f'删除MCP配置失败: {str(e)}',
                'data': None
            }
    
    def get_configs_by_agent(self, agent_id: str) -> Dict[str, Any]:
        """根据智能体ID获取MCP配置列表
        
        Args:
            agent_id: 智能体ID
            
        Returns:
            配置列表数据字典
        """
        try:
            configs = McpConfig.select().where(
                (McpConfig.agent_id == agent_id) & 
                (McpConfig.status == 1)
            ).order_by(McpConfig.create_date.desc())
            
            config_list = [config.to_dict() for config in configs]
            
            return {
                'success': True,
                'message': f'获取智能体 {agent_id} 的MCP配置成功',
                'data': config_list
            }
            
        except Exception as e:
            logger.error(f"Error getting MCP configs for agent {agent_id}: {str(e)}", exc_info=True)
            return {
                'success': False,
                'message': f'获取智能体MCP配置失败: {str(e)}',
                'data': None
            }
    
    def update_tools_info(self, config_id: str, tools_info: str) -> Dict[str, Any]:
        """更新工具信息
        
        Args:
            config_id: 配置ID（UUID）
            tools_info: 工具信息JSON字符串
            
        Returns:
            更新结果字典
        """
        try:
            with self.db.atomic():
                config = McpConfig.get_by_id(config_id)
                config.mcp_tools_info = tools_info
                config.update_date = datetime.now()
                config.save()
                
                logger.info(f"Updated tools info for MCP config ID: {config_id}")
                return {
                    'success': True,
                    'message': 'MCP工具信息更新成功',
                    'data': config.to_dict()
                }
                
        except DoesNotExist:
            return {
                'success': False,
                'message': f'MCP配置不存在: {config_id}',
                'data': None
            }
        except Exception as e:
            logger.error(f"Error updating tools info for MCP config {config_id}: {str(e)}", exc_info=True)
            return {
                'success': False,
                'message': f'更新MCP工具信息失败: {str(e)}',
                'data': None
            }


# 创建全局实例
mcp_config_server = McpConfigServer()
