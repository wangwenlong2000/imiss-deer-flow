# -*- coding: utf-8 -*-
# Role Service Implementation

import logging
from datetime import datetime

from onecityagent.models.db_models import Role, User, DB
from onecityagent.constants import StatusEnum, UserRole
from onecityagent.service.user_service import CommonService
from onecityagent.utils.common_utils import current_timestamp, datetime_format


class RoleService(CommonService):
    model = Role
    
    @classmethod
    def filter_by_id(cls, role_id):
        """
        根据ID获取角色信息
        """
        try:
            with DB.connection_context():
                role = cls.model.select().where(cls.model.id == role_id).get()
                return role
        except cls.model.DoesNotExist:
            return None
        except Exception as e:
            logging.exception(f"Filter role by id error: {str(e)}")
            return None
    
    @classmethod
    def get_role_list(cls):
        """
        获取角色列表
        """
        fields = [
            cls.model.id.alias("role_id"),
            cls.model.name,
            cls.model.code,
            cls.model.sort,
            cls.model.status,
            cls.model.type,
            cls.model.data_scope,
            cls.model.data_scope_dept_ids,
            cls.model.remark,
            cls.model.create_date,
            cls.model.update_date,
        ]
        try:
            with DB.connection_context():
                return list(cls.model.select(*fields)
                            .where((cls.model.status == StatusEnum.VALID) & 
                                   (cls.model.code != UserRole.ADMIN.value)).dicts())
        except Exception as e:
            logging.exception(f"Get role list error: {str(e)}")
            return []

    @classmethod
    def get_user_role_by_email(cls, email):
        """
        根据邮箱获取用户角色信息
        """
        fields = [
            cls.model.id.alias('role_id'),
            cls.model.name,
            cls.model.code,
            cls.model.sort,
            cls.model.status,
            cls.model.type,
            cls.model.data_scope,
            cls.model.data_scope_dept_ids,
            cls.model.remark
        ]

        try:
            with DB.connection_context():
                role = (Role.select(*fields)
                        .join(User, on=((User.role == Role.id) & (Role.status == StatusEnum.VALID) 
                                        & (User.email == email)))
                        .where((User.status == StatusEnum.VALID))
                        .dicts()
                        .get())
                return role
        except Exception as e:
            logging.exception(f"Get user role by email error: {str(e)}")
            return None

    @classmethod
    def get_user_role_by_token(cls, token):
        """
        根据token获取用户角色信息
        """
        fields = [
            cls.model.id.alias('role_id'),
            cls.model.name,
            cls.model.code,
            cls.model.sort,
            cls.model.status,
            cls.model.type,
            cls.model.data_scope,
            cls.model.data_scope_dept_ids,
            cls.model.remark
        ]

        try:
            with DB.connection_context():
                role = (Role.select(*fields)
                        .join(User, on=((User.role == Role.id) & (Role.status == StatusEnum.VALID) 
                                        & (User.access_token == token)))
                        .where((User.status == StatusEnum.VALID))
                        .dicts()
                        .get())
                return role
        except Exception as e:
            logging.exception(f"Get user role by token error: {str(e)}")
            return None