# -*- coding: utf-8 -*-
# A2A配置服务器 - 提供A2A智能体配置的增删改查功能

import logging
import time
import uuid
from datetime import datetime
from typing import List, Dict, Optional, Any
from peewee import DoesNotExist

from onecityagent.models.db_models import A2aAgent, McpConfig, DB
from onecityagent.utils import common_utils
from onecityagent.enums.agent_enums import AgentType, AgentRoleType
from onecityagent.enums import StatusEnum

logger = logging.getLogger(__name__)


class A2aConfigServer:
    """A2A配置服务器类"""
    
    def __init__(self):
        self.db = DB.get_instance()
    
    def create_agent(self, agent_data: Dict[str, Any]) -> Dict[str, Any]:
        """创建A2A智能体配置
        
        Args:
            agent_data: 智能体数据字典
            
        Returns:
            创建结果字典
        """
        try:
            with DB.get_instance().atomic():
                # 验证智能体类型，不允许创建系统内置类型
                agent_type = agent_data.get('type', AgentType.USER.value)
                if agent_type == AgentType.SYSTEM.value:
                    return {
                        'success': False,
                        'message': '不允许创建系统内置类型的智能体',
                        'data': None
                    }
                if agent_type not in [AgentType.SYSTEM.value, AgentType.USER.value]:
                    return {
                        'success': False,
                        'message': f'无效的类型值: {agent_type}，只支持: {AgentType.SYSTEM.value}(系统内置), {AgentType.USER.value}(用户新增)',
                        'data': None
                    }
                
                # 验证状态值
                status = agent_data.get('status', StatusEnum.ENABLED.value)
                if status not in [StatusEnum.DISABLED.value, StatusEnum.ENABLED.value]:
                    return {
                        'success': False,
                        'message': f'无效的状态值: {status}，只支持: {StatusEnum.DISABLED.value}(禁用), {StatusEnum.ENABLED.value}(启用)',
                        'data': None
                    }
                
                # 验证智能体角色类型
                agent_role_type = agent_data.get('agent_type', AgentRoleType.EXECUTION.value)
                valid_role_types = [AgentRoleType.INTENT_RECOGNITION.value, AgentRoleType.PLANNING.value, 
                                   AgentRoleType.EXECUTION.value, AgentRoleType.FEEDBACK.value, AgentRoleType.QA.value]
                if agent_role_type not in valid_role_types:
                    return {
                        'success': False,
                        'message': f'无效的智能体角色类型: {agent_role_type}，只支持: {valid_role_types}',
                        'data': None
                    }
                
                # 验证智能体名称是否重复
                agent_name = agent_data.get('agent_name')
                if agent_name:
                    existing_agent = A2aAgent.select().where(A2aAgent.agent_name == agent_name).first()
                    if existing_agent:
                        return {
                            'success': False,
                            'message': f'智能体名称已存在: {agent_name}',
                            'data': None
                        }
                
                current_time = int(time.time())
                current_date = datetime.now()
                
                # 创建智能体记录，agent_id将自动生成UUID
                agent = A2aAgent.create(
                    agent_name=agent_data.get('agent_name'),
                    agent_url=agent_data.get('agent_url'),
                    status=status,
                    type=agent_type,
                    description=agent_data.get('description'),
                    create_date=current_date,
                    update_date=current_date,
                    agent_type=agent_role_type
                )
                
                logger.info(f"Created A2A agent with ID: {agent.agent_id}")

                return {
                    'success': True,
                    'message': 'A2A智能体配置创建成功',
                    'data': agent.to_dict()
                }
                
        except Exception as e:
            logger.error(f"Error creating A2A agent: {str(e)}", exc_info=True)
            return {
                'success': False,
                'message': f'创建A2A智能体配置失败: {str(e)}',
                'data': None
            }
    
    def get_agent(self, agent_id: str) -> Dict[str, Any]:
        """获取单个A2A智能体配置
        
        Args:
            agent_id: 智能体ID
            
        Returns:
            智能体数据字典
        """
        try:
            with DB.connection_context():
                agent = A2aAgent.get_by_id(agent_id)
                agent_dict = agent.to_dict()
                
                # 查询该智能体绑定的MCP配置
                try:
                    mcp_configs = McpConfig.select().where(
                        McpConfig.agent_id == agent_id
                    ).order_by(McpConfig.create_date.desc())
                    
                    agent_dict['mcp_configs'] = [config.to_dict() for config in mcp_configs]
                except Exception as e:
                    logger.warning(f"Error getting MCP configs for agent {_safe_agent_id(agent.agent_id)}: {str(e)}")
                    agent_dict['mcp_configs'] = []
                
                return {
                    'success': True,
                    'message': '获取A2A智能体配置成功',
                    'data': agent_dict
                }
        except DoesNotExist:
            return {
                'success': False,
                'message': f'A2A智能体不存在: {agent_id}',
                'data': None
            }
        except Exception as e:
            logger.error(f"Error getting A2A agent {_safe_agent_id(agent_id)}: {str(e)}", exc_info=True)
            return {
                'success': False,
                'message': f'获取A2A智能体配置失败: {str(e)}',
                'data': None
            }
    
    def get_agents(self, page: int = 1, page_size: int = 10,
                   status: Optional[bool] = None,
                   type: Optional[int] = None,
                   agent_tpye: Optional[int] = None,
                   search: Optional[str] = None) -> Dict[str, Any]:
        """获取A2A智能体配置列表（支持分页和过滤）
        
        Args:
            page: 页码
            page_size: 每页数量
            status: 状态过滤
            type: 智能体类型过滤
            search: 搜索关键词
            
        Returns:
            智能体列表数据字典
        """
        try:
            with DB.connection_context():
                query = A2aAgent.select()
                
                # 应用过滤条件
                if status is not None:
                    query = query.where(A2aAgent.status == status)
                if type is not None:
                    query = query.where(A2aAgent.type == type)
                if agent_tpye is not None:
                    query = query.where(A2aAgent.agent_type == agent_tpye)
                if search:
                    search_term = f'%{search}%'
                    query = query.where(
                        (A2aAgent.agent_id.contains(search)) |
                        (A2aAgent.agent_name.contains(search)) |
                        (A2aAgent.agent_url.contains(search)) |
                        (A2aAgent.description.contains(search))
                    )
                
                # 计算总数
                total = query.count()
                
                # 应用分页
                offset = (page - 1) * page_size
                agents = query.order_by(A2aAgent.create_date.desc()).offset(offset).limit(page_size)
                
                # 转换为字典列表并查询每个智能体的MCP配置
                agent_list = []
                for agent in agents:
                    agent_dict = agent.to_dict()
                    
                    # 查询该智能体绑定的MCP配置
                    try:
                        mcp_configs = McpConfig.select().where(
                            McpConfig.agent_id == agent.agent_id
                        ).order_by(McpConfig.create_date.desc())
                        
                        agent_dict['mcp_configs'] = [config.to_dict() for config in mcp_configs]
                    except Exception as e:
                        logger.warning(f"Error getting MCP configs for agent {_safe_agent_id(agent.agent_id)}: {str(e)}")
                        agent_dict['mcp_configs'] = []
                    
                    agent_list.append(agent_dict)
                
                return {
                    'success': True,
                    'message': '获取A2A智能体配置列表成功',
                    'data': {
                        'agents': agent_list,
                        'total': total,
                        'page': page,
                        'page_size': page_size
                    }
                }
            
        except Exception as e:
            logger.error(f"Error getting A2A agents: {str(e)}", exc_info=True)
            return {
                'success': False,
                'message': f'获取A2A智能体配置列表失败: {str(e)}',
                'data': None
            }
    
    def update_agent(self, agent_id: str, agent_data: Dict[str, Any]) -> Dict[str, Any]:
        """更新A2A智能体配置
        
        Args:
            agent_id: 智能体ID
            agent_data: 更新的智能体数据
            
        Returns:
            更新结果字典
        """
        try:
            with DB.get_instance().atomic():
                agent = A2aAgent.get_by_id(agent_id)
                
                # 检查是否为系统内置智能体
                if agent.type == AgentType.SYSTEM.value:
                    return {
                        'success': False,
                        'message': '系统内置智能体不允许修改',
                        'data': None
                    }
                
                # 验证状态值（如果要更新status）
                if 'status' in agent_data:
                    status = agent_data['status']
                    if status not in [StatusEnum.DISABLED.value, StatusEnum.ENABLED.value]:
                        return {
                            'success': False,
                            'message': f'无效的状态值: {status}，只支持: {StatusEnum.DISABLED.value}(禁用), {StatusEnum.ENABLED.value}(启用)',
                            'data': None
                        }
                
                # 验证智能体类型（如果要更新type）
                if 'type' in agent_data:
                    agent_type = agent_data['type']
                    # 防止将智能体修改为系统内置类型
                    if agent_type == AgentType.SYSTEM.value:
                        return {
                            'success': False,
                            'message': '不允许将智能体修改为系统内置类型',
                            'data': None
                        }
                
                # 验证智能体名称是否重复（如果要更新agent_name）
                if 'agent_name' in agent_data:
                    new_agent_name = agent_data['agent_name']
                    if new_agent_name and new_agent_name != agent.agent_name:
                        existing_agent = A2aAgent.select().where(
                            (A2aAgent.agent_name == new_agent_name) & 
                            (A2aAgent.agent_id != agent_id)
                        ).first()
                        if existing_agent:
                            return {
                                'success': False,
                                'message': f'智能体名称已存在: {new_agent_name}',
                                'data': None
                            }
                
                # 更新字段
                if 'agent_name' in agent_data:
                    agent.agent_name = agent_data['agent_name']
                if 'agent_url' in agent_data:
                    agent.agent_url = agent_data['agent_url']
                if 'status' in agent_data:
                    agent.status = agent_data['status']
                if 'type' in agent_data:
                    # 防止将智能体修改为系统内置类型
                    if agent_data['type'] == AgentType.SYSTEM.value:
                        return {
                            'success': False,
                            'message': '不允许将智能体修改为系统内置类型',
                            'data': None
                        }
                    agent.type = agent_data['type']
                if 'description' in agent_data:
                    agent.description = agent_data['description']
                if 'agent_type' in agent_data:
                    # 验证智能体角色类型
                    agent_role_type = agent_data['agent_type']
                    valid_role_types = [AgentRoleType.INTENT_RECOGNITION.value, AgentRoleType.PLANNING.value, 
                                       AgentRoleType.EXECUTION.value, AgentRoleType.FEEDBACK.value, AgentRoleType.QA.value]
                    if agent_role_type not in valid_role_types:
                        return {
                            'success': False,
                            'message': f'无效的智能体角色类型: {agent_role_type}，只支持: {valid_role_types}',
                            'data': None
                        }
                    agent.agent_type = agent_role_type
                
                # 更新时间戳
                agent.update_date = datetime.now()
                
                agent.save()
                
                logger.info(f"Updated A2A agent with ID: {agent_id}")
                return {
                    'success': True,
                    'message': 'A2A智能体配置更新成功',
                    'data': agent.to_dict()
                }
                
        except DoesNotExist:
            return {
                'success': False,
                'message': f'A2A智能体不存在: {agent_id}',
                'data': None
            }
        except Exception as e:
            logger.error(f"Error updating A2A agent {agent_id}: {str(e)}", exc_info=True)
            return {
                'success': False,
                'message': f'更新A2A智能体配置失败: {str(e)}',
                'data': None
            }
    
    def delete_agent(self, agent_id: str) -> Dict[str, Any]:
        """删除A2A智能体配置
        
        Args:
            agent_id: 智能体ID
            
        Returns:
            删除结果字典
        """
        try:
            with DB.get_instance().atomic():
                agent = A2aAgent.get_by_id(agent_id)
                
                # 检查是否为系统内置智能体
                if agent.type == AgentType.SYSTEM.value:
                    return {
                        'success': False,
                        'message': '系统内置智能体不允许删除',
                        'data': None
                    }
                
                agent.delete_instance()
                
                logger.info(f"Deleted A2A agent with ID: {agent_id}")
                return {
                    'success': True,
                    'message': 'A2A智能体配置删除成功',
                    'data': None
                }
                
        except DoesNotExist:
            return {
                'success': False,
                'message': f'A2A智能体不存在: {agent_id}',
                'data': None
            }
        except Exception as e:
            logger.error(f"Error deleting A2A agent {agent_id}: {str(e)}", exc_info=True)
            return {
                'success': False,
                'message': f'删除A2A智能体配置失败: {str(e)}',
                'data': None
            }
    
    def get_active_agents(self) -> Dict[str, Any]:
        """获取所有启用的A2A智能体配置
        
        Returns:
            启用的智能体列表数据字典
        """
        try:
            agents = A2aAgent.select().where(
                A2aAgent.status == 1
            ).order_by(A2aAgent.create_date.desc())
            
            agent_list = [agent.to_dict() for agent in agents]
            
            return {
                'success': True,
                'message': '获取启用的A2A智能体配置成功',
                'data': agent_list
            }
            
        except Exception as e:
            logger.error(f"Error getting active A2A agents: {str(e)}", exc_info=True)
            return {
                'success': False,
                'message': f'获取启用的A2A智能体配置失败: {str(e)}',
                'data': None
            }
    
    def toggle_agent_status(self, agent_id: str) -> Dict[str, Any]:
        """切换A2A智能体状态
        
        Args:
            agent_id: 智能体ID
            
        Returns:
            切换结果字典
        """
        try:
            with DB.get_instance().atomic():
                agent = A2aAgent.get_by_id(agent_id)
                
                # 检查是否为系统内置智能体
                if agent.type == AgentType.SYSTEM.value:
                    return {
                        'success': False,
                        'message': '系统内置智能体状态不允许修改',
                        'data': None
                    }
                
                agent.status = 1 if agent.status == 0 else 0
                agent.update_date = datetime.now()
                agent.save()
                
                status_text = "启用" if agent.status == 1 else "禁用"
                logger.info(f"Toggled A2A agent {agent_id} status to: {agent.status}")
                return {
                    'success': True,
                    'message': f'A2A智能体已{status_text}',
                    'data': agent.to_dict()
                }
                
        except DoesNotExist:
            return {
                'success': False,
                'message': f'A2A智能体不存在: {agent_id}',
                'data': None
            }
        except Exception as e:
            logger.error(f"Error toggling A2A agent {agent_id} status: {str(e)}", exc_info=True)
            return {
                'success': False,
                'message': f'切换A2A智能体状态失败: {str(e)}',
                'data': None
            }
    
    def search_agents(self, keyword: str) -> Dict[str, Any]:
        """搜索A2A智能体配置
        
        Args:
            keyword: 搜索关键词
            
        Returns:
            搜索结果字典
        """
        try:
            agents = A2aAgent.select().where(
                (A2aAgent.agent_id.contains(keyword)) |
                (A2aAgent.agent_name.contains(keyword)) |
                (A2aAgent.agent_url.contains(keyword)) |
                (A2aAgent.description.contains(keyword))
            ).order_by(A2aAgent.create_date.desc())
            
            agent_list = [agent.to_dict() for agent in agents]
            
            return {
                'success': True,
                'message': f'搜索A2A智能体配置成功，找到 {len(agent_list)} 个结果',
                'data': agent_list
            }
            
        except Exception as e:
            logger.error(f"Error searching A2A agents with keyword '{keyword}': {str(e)}", exc_info=True)
            return {
                'success': False,
                'message': f'搜索A2A智能体配置失败: {str(e)}',
                'data': None
            }
    
    def get_execution_agents_with_mcp(self) -> Dict[str, Any]:
        """获取所有反馈智能体，并将所有执行智能体的MCP配置汇总到反馈智能体的mcp_configs中
        
        Returns:
            包含反馈智能体及汇总MCP配置的结果字典
        """
        try:
            # 查询反馈类型且状态为1的智能体
            thinking_agents = A2aAgent.select().where(
                (A2aAgent.agent_type == AgentRoleType.FEEDBACK.value) &
                (A2aAgent.status == 1)
            ).order_by(A2aAgent.create_date.desc())

            thinking_list = [agent.to_dict() for agent in thinking_agents]
            
            # 查询执行类型为2且状态为1的智能体
            execution_agents = A2aAgent.select().where(
                (A2aAgent.agent_type == AgentRoleType.EXECUTION.value) &
                (A2aAgent.status == 1)
            ).order_by(A2aAgent.create_date.desc())

            # 收集所有执行智能体的MCP配置
            all_mcp_configs = []
            
            for agent in execution_agents:
                # 查询该智能体绑定的状态为1的MCP配置
                try:
                    mcp_configs = McpConfig.select().where(
                        (McpConfig.agent_id == agent.agent_id) &
                        (McpConfig.status == 1)
                    ).order_by(McpConfig.create_date.desc())
                    
                    all_mcp_configs.extend([config.to_dict() for config in mcp_configs])
                    
                except Exception as e:
                    logger.warning(f"Error getting MCP configs for agent {agent.agent_id}: {str(e)}")
            
            # 将所有MCP配置添加到反馈智能体列表中
            for thinking_agent in thinking_list:
                thinking_agent['mcp_configs'] = all_mcp_configs
            
            return {
                'success': True,
                'message': f'获取反馈智能体及MCP配置成功，找到 {len(thinking_list)} 个反馈智能体，汇总了 {len(all_mcp_configs)} 个MCP配置',
                'data': thinking_list
            }
            
        except Exception as e:
            logger.error(f"Error getting execution agents with MCP configs: {str(e)}", exc_info=True)
            return {
                'success': False,
                'message': f'获取执行智能体及MCP配置失败: {str(e)}',
                'data': None
            }


    def _safe_agent_id(agent_id: str) -> str:
        try:
            if not isinstance(agent_id, str) or len(agent_id) > 50:
                return "INVALID_AGENT_ID"
            uuid.UUID(agent_id)
            return agent_id
        except Exception:
            return "INVALID_AGENT_ID"

# 创建全局实例
a2a_config_server = A2aConfigServer()

