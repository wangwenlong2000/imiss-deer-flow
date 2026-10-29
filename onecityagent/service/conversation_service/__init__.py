# -*- coding: utf-8 -*-
# Conversation Service for OneCity Agent

import json
from uuid import uuid4
from typing import List, Dict, Any, Optional, Tuple
from flask_login import current_user

from onecityagent.models.db_models import Conversation, DB
from onecityagent.constants import StatusEnum
from onecityagent.utils import common_utils
from onecityagent.logger import logger


class ConversationService:
    """会话服务类，提供会话管理的数据库操作封装"""
    
    def __init__(self):
        """初始化会话服务"""
        pass
    
    def save(self, **kwargs):
        """保存会话数据"""
        try:
            with DB.connection_context():
                conversation = Conversation.create(**kwargs)
                return True  # 如果没有异常，说明保存成功
        except Exception as e:
            logger.exception(f"保存失败 - 模型: Conversation, 数据: {kwargs}, 错误: {str(e)}")
            return False
    
    def get_by_id(self, id_):
        """根据ID获取对象"""
        try:
            with DB.connection_context():
                return Conversation.get_by_id(id_)
        except Conversation.DoesNotExist:
            return None
        except Exception as e:
            logger.exception(f"查询失败 - 模型: Conversation, ID: {id_}, 错误: {str(e)}")
            return None
    
    def delete(self, id_):
        """根据ID删除对象"""
        try:
            with DB.connection_context():
                obj = Conversation.get_by_id(id_)
                return obj.delete_instance() > 0
        except Exception as e:
            logger.exception(f"删除失败 - 模型: Conversation, ID: {id_}, 错误: {str(e)}")
            return False

    def update_by_id(self, id_, update_data: Dict[str, Any]) -> bool:
        """根据ID更新记录（白名单字段，防止批量赋值漏洞）"""
        try:
            with DB.connection_context():
                # 仅允许更新的安全字段
                allowed_fields = {"dialog_id", "message", "reference", "sort_order", "scene", "plan"}
                safe_updates = {k: v for k, v in update_data.items() if k in allowed_fields}

                # 记录被忽略的字段（如 id、dialog_id、create_date 等）
                for k in update_data.keys():
                    if k not in allowed_fields:
                        logger.warning(f"忽略不允许更新的字段: {k}")

                # 类型校验与规范化
                if "sort_order" in safe_updates and safe_updates["sort_order"] is not None:
                    try:
                        safe_updates["sort_order"] = int(safe_updates["sort_order"])
                    except (TypeError, ValueError):
                        logger.error(f"sort_order类型无效: {safe_updates['sort_order']}")
                        return False

                # 统一由服务端设置更新时间
                from datetime import datetime
                safe_updates["update_date"] = datetime.now()

                # 使用 UPDATE 语句避免对象属性被动态篡改
                rows = Conversation.update(**safe_updates).where(Conversation.id == id_).execute()
                if rows == 0:
                    logger.error(f"对象不存在 - 模型: Conversation, ID: {id_}")
                    return False
                return True
        except Exception as e:
            logger.exception(f"更新失败 - 模型: Conversation, ID: {id_}, 错误: {str(e)}")
            return False

    def delete_by_id(self, id_) -> bool:
        """根据ID删除记录"""
        try:
            with DB.connection_context():
                obj = Conversation.get_by_id(id_)
                obj.delete_instance()
                return True
        except Conversation.DoesNotExist:
            logger.error(f"对象不存在 - 模型: Conversation, ID: {id_}")
            return False
        except Exception as e:
            logger.exception(f"删除失败 - 模型: Conversation, ID: {id_}, 错误: {str(e)}")
            return False

    def get_list(self, page_number: int = 1, items_per_page: int = 10, 
                 orderby: str = 'create_date', desc: bool = True,
                 conversation_id: Optional[str] = None,
                 dialog_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        获取会话列表，支持分页和筛选
        
        Args:
            page_number: 页码
            items_per_page: 每页数量
            orderby: 排序字段
            desc: 是否降序
            conversation_id: 会话ID筛选
            dialog_id: 对话ID筛选
            
        Returns:
            会话列表
        """
        with DB.connection_context():
            sessions = Conversation.select()
            
            if conversation_id:
                sessions = sessions.where(Conversation.id == conversation_id)
            if dialog_id:
                sessions = sessions.where(Conversation.dialog_id == dialog_id)
                
            if desc:
                sessions = sessions.order_by(Conversation.getter_by(orderby).desc())
            else:
                sessions = sessions.order_by(Conversation.getter_by(orderby).asc())

            sessions = sessions.paginate(page_number, items_per_page)
            return list(sessions.dicts())

    def get_all_list(self, orderby: str = 'create_date', desc: bool = True,
                 conversation_id: Optional[str] = None,
                 dialog_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        获取会话列表，支持分页和筛选

        Args:
            orderby: 排序字段
            desc: 是否降序
            conversation_id: 会话ID筛选
            dialog_id: 对话ID筛选

        Returns:
            会话列表
        """
        with DB.connection_context():
            sessions = Conversation.select()

            if conversation_id:
                sessions = sessions.where(Conversation.id == conversation_id)
            if dialog_id:
                sessions = sessions.where(Conversation.dialog_id == dialog_id)

            if desc:
                sessions = sessions.order_by(Conversation.getter_by(orderby).desc())
            else:
                sessions = sessions.order_by(Conversation.getter_by(orderby).asc())

            return list(sessions.dicts())

    def create_conversation(self, conversation_id: Optional[str] = None,
                            dialog_id: Optional[str] = None,
                            sort_order: Optional[int] = None,
                            message: Optional[str] = None,
                            reference: Optional[str] = None,
                            scene: Optional[str] = None,
                            plan: Optional[str] = None) -> bool:
        """
        创建新会话
        
        Args:
            conversation_id: 指定会话ID（可选）
            dialog_id: 关联的对话ID（可选）
            sort_order: 排序字段（可选）
            message: 消息内容JSON字符串（可选）
            reference: 引用信息JSON字符串（可选）
            scene: 场景名称（可选）
            plan: 计划内容（可选）
            
        Returns:
            (是否成功, 会话对象)
        """
        try:
            if not conversation_id:
                conversation_id = common_utils.get_uuid()
                
            # 创建会话数据
            #current_time = common_utils.current_timestamp()
            current_date = common_utils.get_format_time()

            
            conv_data = {
                "id": conversation_id,
                "create_date": current_date,
                "update_date": current_date,
                "message": message,
                "reference": reference,
                "dialog_id": dialog_id,
                "sort_order": sort_order,
                "scene": scene or "",
                "plan": plan or ""
            }
            
            # 保存会话数据
            save_success = self.save(**conv_data)
            if not save_success:
                logger.error(f"Failed to save conversation data: {conv_data}")
                return False
       
            return True
            
        except Exception as e:
            logger.exception(f"Failed to create conversation: {str(e)}")
            return False

    def update_conversation(self, conversation_id: str, update_data: Dict[str, Any]) -> bool:
        """更新会话信息
        
        Args:
            conversation_id: 会话ID
            update_data: 更新字段字典
            
        Returns:
            是否更新成功
        """
        try:
            return self.update_by_id(conversation_id, update_data)
        except Exception as e:
            logger.exception(f"Failed to update conversation {conversation_id}: {str(e)}")
            return False

    def delete_conversation(self, conversation_id: str) -> bool:
        """删除会话
        
        Args:
            conversation_id: 会话ID
            
        Returns:
            是否删除成功
        """
        try:
            return self.delete_by_id(conversation_id)
        except Exception as e:
            logger.exception(f"Failed to delete conversation {conversation_id}: {str(e)}")
            return False

    def get_conversation_by_id(self, conversation_id: str) -> Tuple[bool, Optional[Conversation]]:
        """
        根据ID获取会话
        
        Args:
            conversation_id: 会话ID
            
        Returns:
            (是否成功, 会话对象)
        """
        try:
            conversation = self.get_by_id(conversation_id)
            if conversation:
                return True, conversation
            else:
                return False, None
        except Exception as e:
            logger.exception(f"Failed to get conversation by id {conversation_id}: {str(e)}")
            return False, None
    
    def get_conversation_by_dialog_and_sort(self, dialog_id: str, sort_order: int) -> Tuple[bool, Optional[Conversation]]:
        """
        根据dialog_id和sort_order获取会话
        
        Args:
            dialog_id: 对话ID
            sort_order: 排序字段
            
        Returns:
            (是否成功, 会话对象)
        """
        try:
            with DB.connection_context():
                query = Conversation.select().where(
                    (Conversation.dialog_id == dialog_id) & 
                    (Conversation.sort_order == sort_order)
                )
                conversation = query.first()
                if conversation:
                    return True, conversation
                else:
                    return False, None
        except Exception as e:
            logger.exception(f"Failed to get conversation by dialog_id {dialog_id} and sort_order {sort_order}: {str(e)}")
            return False, None

    def add_message(self, conversation_id: str, message: Dict[str, Any], 
                   reference: Optional[Dict[str, Any]] = None) -> bool:
        """
        向会话添加消息
        
        Args:
            conversation_id: 会话ID
            message: 消息内容
            reference: 引用信息
            
        Returns:
            是否添加成功
        """
        try:
            exists, conv = self.get_by_id(conversation_id)
            if not exists or not conv:
                return False
                
            conv_dict = conv.to_dict()
            
            # 解析JSON字符串
            messages = json.loads(conv_dict.get('message', '[]')) if conv_dict.get('message') else []
            references = json.loads(conv_dict.get('reference', '[]')) if conv_dict.get('reference') else []
                
            messages.append(message)
            if reference:
                references.append(reference)
            
            # 更新时间戳
            #current_time = common_utils.current_timestamp()
            current_date = common_utils.get_format_time()
            
            update_data = {
                'message': json.dumps(messages),
                'reference': json.dumps(references),
                'update_date': current_date
            }
                
            return self.update_by_id(conversation_id, update_data)
            
        except Exception as e:
            logger.exception(f"Failed to add message to conversation {conversation_id}: {str(e)}")
            return False


