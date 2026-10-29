import uuid
import time
from datetime import datetime
from typing import Dict, List, Optional
from peewee import fn

from onecityagent.models.db_models import DB, Dialog, Conversation


class DialogService:
    """对话服务类 - Dialog管理"""
    
    def __init__(self):
        self.db = DB.get_instance()
    
    def create_dialog(self, name: str, creator_id: str, tenant_id: str, description: str = None) -> Dict:
        """创建新的对话会话"""
        with DB.connection_context():
            dialog_id = str(uuid.uuid4())
            #current_time = int(time.time() * 1000)
            current_date = datetime.now()
            
            dialog = Dialog.create(
                id=dialog_id,
                name=name,
                creator_id=creator_id,
                tenant_id=tenant_id,
                description=description,
                create_date=current_date,
                update_date=current_date,
                status=1
            )
            
            return {
                'dialog_id': dialog.id,
                'name': dialog.name,
                'creator_id': dialog.creator_id,
                'tenant_id': dialog.tenant_id,
                'description': dialog.description,
                'status': dialog.status,
                'finished': dialog.finished
            }
    

    
    def get_dialog(self, dialog_id: str) -> Optional[Dict]:
        """获取对话会话信息"""
        with DB.connection_context():
            dialog = Dialog.get_or_none(Dialog.id == dialog_id)
            if dialog:
                return {
                    'dialog_id': dialog.id,
                    'name': dialog.name,
                    'creator_id': dialog.creator_id,
                    'tenant_id': dialog.tenant_id,
                    'description': dialog.description,
                    'create_date': dialog.create_date.isoformat() if dialog.create_date else None,
                    'update_date': dialog.update_date.isoformat() if dialog.update_date else None,
                    'status': dialog.status,
                    'finished': dialog.finished
                }
            return None
    
    def get_dialogs(self, creator_id: str = None, tenant_id: str = None, page: int = 1, page_size: int = 20) -> Dict:
        """分页获取对话会话列表"""
        with DB.connection_context():
            query = Dialog.select().where(Dialog.status == 1)
            
            if creator_id:
                query = query.where(Dialog.creator_id == creator_id)
            if tenant_id:
                query = query.where(Dialog.tenant_id == tenant_id)
            
            # 计算总数
            total_count = query.count()
            
            # 计算偏移量
            offset = (page - 1) * page_size
            
            # 查询对话会话列表，按创建时间倒序
            dialogs = query.order_by(Dialog.create_date.desc()).offset(offset).limit(page_size)
            
            dialog_list = []
            for dialog in dialogs:
                dialog_list.append({
                    'dialog_id': dialog.id,
                    'name': dialog.name,
                    'creator_id': dialog.creator_id,
                    'tenant_id': dialog.tenant_id,
                    'description': dialog.description,
                    'create_date': dialog.create_date.isoformat() if dialog.create_date else None,
                    'update_date': dialog.update_date.isoformat() if dialog.update_date else None,
                    'status': dialog.status,
                    'finished': dialog.finished
                })
            
            # 计算总页数
            total_pages = (total_count + page_size - 1) // page_size
            
            return {
                'dialogs': dialog_list,
                'pagination': {
                    'page': page,
                    'page_size': page_size,
                    'total_count': total_count,
                    'total_pages': total_pages,
                    'has_next': page < total_pages,
                    'has_prev': page > 1
                }
            }

    def get_all_dialogs(self, creator_id: str = None, tenant_id: str = None) -> Dict:
        """获取全部对话会话列表（不分页）"""
        with DB.connection_context():
            query = Dialog.select().where(Dialog.status == 1)

            if creator_id:
                query = query.where(Dialog.creator_id == creator_id)
            if tenant_id:
                query = query.where(Dialog.tenant_id == tenant_id)

            # 总数
            total_count = query.count()

            # 不分页，全部返回
            dialogs = query.order_by(Dialog.create_date.desc())

            dialog_list = []
            for dialog in dialogs:
                dialog_list.append({
                    'dialog_id': dialog.id,
                    'name': dialog.name,
                    'creator_id': dialog.creator_id,
                    'tenant_id': dialog.tenant_id,
                    'description': dialog.description,
                    'create_date': dialog.create_date.isoformat() if dialog.create_date else None,
                    'update_date': dialog.update_date.isoformat() if dialog.update_date else None,
                    'status': dialog.status,
                    'finished': dialog.finished
                })

            return {
                'dialogs': dialog_list,
                'pagination': {
                    'page': 1,
                    'page_size': total_count,
                    'total_count': total_count,
                    'total_pages': 1,
                    'has_next': False,
                    'has_prev': False
                }
            }

    def update_dialog(self, dialog_id: str, name: str = None, description: str = None) -> Optional[Dict]:
        """更新对话会话信息"""
        with DB.connection_context():
            with self.db.atomic():
                dialog = Dialog.get_or_none(Dialog.id == dialog_id)
                if dialog:
                    if name is not None:
                        dialog.name = name
                    if description is not None:
                        dialog.description = description
                    #dialog.update_time = int(time.time() * 1000)
                    dialog.update_date = datetime.now()
                    dialog.save()
                    
                    return {
                        'dialog_id': dialog.id,
                        'name': dialog.name,
                        'creator_id': dialog.creator_id,
                        'tenant_id': dialog.tenant_id,
                        'description': dialog.description,
                        'status': dialog.status,
                        'finished': dialog.finished
                    }
                return None
    
    def delete_dialog(self, dialog_id: str) -> bool:
        """删除对话会话（软删除）"""
        with DB.connection_context():
            with self.db.atomic():
                dialog = Dialog.get_or_none(Dialog.id == dialog_id)
                if dialog:
                    dialog.status = 0
                    #dialog.update_time = int(time.time() * 1000)
                    dialog.update_date = datetime.now()
                    dialog.save()
                    return True
                return False
                
    def update_dialog_finished(self, dialog_id: str, finished: int = 1) -> Optional[Dict]:
        """更新对话会话的完成状态"""
        with DB.connection_context():
            with self.db.atomic():
                dialog = Dialog.get_or_none(Dialog.id == dialog_id)
                if dialog:
                    dialog.finished = finished
                    dialog.update_date = datetime.now()
                    dialog.save()
                    
                    return {
                        'dialog_id': dialog.id,
                        'name': dialog.name,
                        'creator_id': dialog.creator_id,
                        'tenant_id': dialog.tenant_id,
                        'description': dialog.description,
                        'status': dialog.status,
                        'finished': dialog.finished
                    }
                return None
