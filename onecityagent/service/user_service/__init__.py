# -*- coding: utf-8 -*-
# User Service Implementation

from datetime import datetime
from werkzeug.security import check_password_hash

from onecityagent.utils.common_utils import datetime_format

from onecityagent.models.db_models import User, Role, Tenant, UserTenant, DB
from onecityagent.constants import StatusEnum, UserTenantRole
from onecityagent.utils.common_utils import delta_seconds
from onecityagent.logger import logger


class CommonService:
    model = None

    def __init__(self):
        self.db = DB.get_instance()
        
    @classmethod
    def save(cls, **kwargs):
        try:
            with DB.connection_context():
                # 创建模型实例
                obj = cls.model(**kwargs)
                
                # 使用 force_insert=True 强制执行 INSERT 操作
                save_result = obj.save(force_insert=True)
                
                return save_result > 0
        except Exception as e:
            logger.exception(f"保存失败 - {cls.model.__name__}: {str(e)}")
            return False

    @classmethod
    def get_by_id(cls, id_):
        try:
            with DB.connection_context():
                obj = cls.model.get_by_id(id_)
                return True, obj
        except cls.model.DoesNotExist:
            return False, None
        except Exception as e:
            logger.exception(f"Get by id error: {str(e)}")
            return False, None

    @classmethod
    def delete(cls, id_):
        try:
            with DB.connection_context():
                return cls.model.delete_by_id(id_) > 0
        except Exception as e:
            logger.exception(f"Delete error: {str(e)}")
            return False

    @classmethod
    def filter_update(cls, filters, update_dict):
        try:
            with DB.connection_context():
                query = cls.model.update(**update_dict).where(*filters)
                return query.execute() > 0
        except Exception as e:
            logger.exception(f"Filter update error: {str(e)}")
            return False

    @classmethod
    def filter_delete(cls, filters):
        try:
            with DB.connection_context():
                query = cls.model.delete().where(*filters)
                return query.execute() > 0
        except Exception as e:
            logger.exception(f"Filter delete error: {str(e)}")
            return False


class UserService(CommonService):
    model = User

    @classmethod
    def query_user(cls, email, password):
        try:
            with DB.connection_context():
                user = cls.model.get(cls.model.email == email, cls.model.status == StatusEnum.VALID.value)
                if check_password_hash(user.password, password):
                    return user
                return None
        except cls.model.DoesNotExist:
            return None
        except Exception as e:
            logger.exception(f"Query user error: {str(e)}")
            return None

    @classmethod
    def query(cls, email):
        """Query user by email, returns a list for compatibility"""
        try:
            with DB.connection_context():
                user = cls.model.get(cls.model.email == email, cls.model.status == StatusEnum.VALID.value)
                return [user] if user else []
        except cls.model.DoesNotExist:
            return []
        except Exception as e:
            logger.exception(f"Query user by email error: {str(e)}")
            return []

    @classmethod
    def get_user_role(cls, role_id):
        try:
            with DB.connection_context():
                return Role.get(Role.id == role_id, Role.status == StatusEnum.VALID.value)
        except Role.DoesNotExist:
            return None
        except Exception as e:
            logger.exception(f"Get user role error: {str(e)}")
            return None

    @classmethod
    def get_all_roles(cls):
        """Get all valid roles"""
        try:
            with DB.connection_context():
                return list(Role.select().where(Role.status == StatusEnum.VALID.value).dicts())
        except Exception as e:
            logger.exception(f"Get all roles error: {str(e)}")
            return []

    @classmethod
    def update_by_id(cls, user_id, update_dict):
        """Update user by id"""
        try:
            with DB.connection_context():
                return cls.model.update(**update_dict).where(cls.model.id == user_id).execute() > 0
        except Exception as e:
            logger.exception(f"Update user by id error: {str(e)}")
            return False


class TenantService(CommonService):
    model = Tenant

    @classmethod
    def update_by_id(cls, tenant_id, update_dict):
        """Update tenant by id"""
        try:
            with DB.connection_context():
                return cls.model.update(**update_dict).where(cls.model.id == tenant_id).execute() > 0
        except Exception as e:
            logger.exception(f"Update tenant by id error: {str(e)}")
            return False

    @classmethod
    def get_info_by(cls, user_id):
        try:
            with DB.connection_context():
                fields = [
                    cls.model.id.alias("tenant_id"),
                    cls.model.name,
                    UserTenant.role
                ]
                return list(cls.model.select(*fields)
                        .join(UserTenant, on=((cls.model.id == UserTenant.tenant_id) & 
                                             (UserTenant.user_id == user_id) & 
                                             (UserTenant.status == StatusEnum.VALID.value) & 
                                             (UserTenant.role == UserTenantRole.OWNER.value)))
                        .where(cls.model.status == StatusEnum.VALID.value).dicts())
        except Exception as e:
            logger.exception(f"Get tenant info error: {str(e)}")
            return []


class UserTenantService(CommonService):
    model = UserTenant

    @classmethod
    @DB.connection_context()
    def get_by_tenant_id(cls, tenant_id):
        fields = [
            User.id,
            User.email,
            User.nickname,
            User.avatar,
            User.create_date,
            User.update_date,
            cls.model.role,
            cls.model.invited_by
        ]
        return list(User.select(*fields)
                    .join(cls.model, on=((User.id == cls.model.user_id) & 
                                        (cls.model.tenant_id == tenant_id) & 
                                        (cls.model.status == StatusEnum.VALID.value)))
                    .where(User.status == StatusEnum.VALID.value).dicts())

    @classmethod
    @DB.connection_context()
    def get_tenants_by_user_id(cls, user_id):
        fields = [
            Tenant.id,
            Tenant.name,
            Tenant.create_date,
            Tenant.update_date,
            Tenant.llm_id,
            Tenant.embd_id,
            Tenant.asr_id,
            Tenant.img2txt_id,
            Tenant.rerank_id,
            Tenant.tts_id,
            Tenant.parser_ids,
            cls.model.role
        ]
        return list(Tenant.select(*fields)
                    .join(cls.model, on=((Tenant.id == cls.model.tenant_id) & 
                                        (cls.model.user_id == user_id) & 
                                        (cls.model.status == StatusEnum.VALID.value)))
                    .where(Tenant.status == StatusEnum.VALID.value).dicts())

    @classmethod
    def query(cls, user_id=None, tenant_id=None):
        """Query user tenant relationship"""
        try:
            with DB.connection_context():
                conditions = [cls.model.status == StatusEnum.VALID.value]
                if user_id is not None:
                    conditions.append(cls.model.user_id == user_id)
                if tenant_id is not None:
                    conditions.append(cls.model.tenant_id == tenant_id)
                
                return list(cls.model.select().where(*conditions))
        except Exception as e:
            logger.exception(f"Query user tenant error: {str(e)}")
            return []