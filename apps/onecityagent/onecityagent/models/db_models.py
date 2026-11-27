# -*- coding: utf-8 -*-
# Database models for OneCity Agent

import logging
import inspect
import os
import sys
import typing
import operator
import uuid
from enum import Enum
from functools import wraps
from itsdangerous.url_safe import URLSafeTimedSerializer as Serializer
from flask_login import UserMixin
from peewee import (
    AutoField, BigIntegerField, BooleanField, CharField,
    CompositeKey, IntegerField, TextField, FloatField, DateTimeField,
    Field, Model, Metadata
)
from playhouse.pool import PooledMySQLDatabase, PooledPostgresqlDatabase
import pymysql

from onecityagent.constants import SerializedType, StatusEnum, UserTenantRole, UserRole
from onecityagent.utils import common_utils
from onecityagent.enums.agent_enums import AgentType


from onecityagent.application.database import DB, database_proxy


class TextFieldType(Enum):
    MYSQL = 'LONGTEXT'
    POSTGRES = 'TEXT'


class LongTextField(TextField):
    @property
    def field_type(self):
        # Preserve MySQL LONGTEXT while allowing the same model definitions to
        # serve independent SQLite/Postgres factory instances.
        from peewee import MySQLDatabase
        return "LONGTEXT" if isinstance(database_proxy.obj, MySQLDatabase) else "TEXT"


class JSONField(LongTextField):
    default_value = {}

    def __init__(self, object_hook=None, object_pairs_hook=None, **kwargs):
        self._object_hook = object_hook
        self._object_pairs_hook = object_pairs_hook
        super().__init__(**kwargs)

    def db_value(self, value):
        if value is None:
            value = self.default_value
        return common_utils.json_dumps(value)

    def python_value(self, value):
        if not value:
            return self.default_value
        return common_utils.json_loads(
            value, object_hook=self._object_hook, object_pairs_hook=self._object_pairs_hook)


class ListField(JSONField):
    default_value = []

# 在 BaseModel 类中添加连接管理装饰器
def with_database(func):
    """数据库连接管理装饰器"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        with DB.connection_context():
            return func(*args, **kwargs)
    return wrapper

class BaseModel(Model):
    create_date = DateTimeField(null=True, index=True)
    update_date = DateTimeField(null=True, index=True)

    def to_json(self):
        # 兼容旧代码
        return self.to_dict()

    def to_dict(self):
        return self.__dict__['__data__']

    @property
    def meta(self) -> Metadata:
        return self._meta

    @classmethod
    def get_primary_keys_name(cls):
        return cls._meta.primary_key.field_names if isinstance(cls._meta.primary_key, CompositeKey) else [
            cls._meta.primary_key.name]

    @classmethod
    def getter_by(cls, attr):
        return operator.attrgetter(attr)(cls)

    # 修改查询方法，确保连接正确管理
    @classmethod
    def query(cls, reverse=None, order_by=None, **kwargs):
        filters = []
        for f_n, f_v in kwargs.items():
            attr_name = '%s' % f_n
            if not hasattr(cls, attr_name) or f_v is None:
                continue
            if type(f_v) in {list, set}:
                f_v = list(f_v)
                if len(f_v) == 0:
                    continue
                filters.append(getattr(cls, attr_name).in_(f_v))
            else:
                filters.append(getattr(cls, attr_name) == f_v)

        if len(filters) == 0:
            query = cls.select()
        else:
            query = cls.select().where(*filters)

        if order_by:
            if reverse:
                query = query.order_by(getattr(cls, order_by).desc())
            else:
                query = query.order_by(getattr(cls, order_by))

        with DB.connection_context():
            return list(query)

    class Meta:
        database = database_proxy


class User(BaseModel, UserMixin):
    id = CharField(primary_key=True, max_length=32)  # 用户ID
    access_token = CharField(max_length=255, null=True, index=True)  # 访问令牌
    nickname = CharField(max_length=100, index=True)  # 昵称
    password = CharField(max_length=255, null=True, index=True)  # 密码
    email = CharField(max_length=255, index=True)  # 邮箱
    avatar = TextField(null=True)  # 头像
    language = CharField(max_length=32, null=True, index=True)  # 语言
    color_schema = CharField(max_length=32, null=True, index=True)  # 颜色方案
    timezone = CharField(max_length=64, null=True, index=True)  # 时区
    last_login_time = DateTimeField(null=True, index=True)  # 最后登录时间
    is_authenticated = CharField(max_length=1, index=True)  # 是否认证
    is_active = CharField(max_length=1, index=True)  # 是否激活
    is_anonymous = CharField(max_length=1, index=True)  # 是否匿名
    login_channel = CharField(max_length=255, null=True, index=True)  # 登录渠道
    status = CharField(max_length=1, null=True, index=True)  # 状态
    role = CharField(max_length=64, null=True)  # 角色
    is_superuser = BooleanField(null=True, index=True)  # 是否超级用户

    def get_id(self):
        return self.access_token

    def get_reset_token(self, expires_sec=1800):
        s = Serializer(__import__("flask").current_app.config["SECRET_KEY"])
        return s.dumps({"user_id": self.id}).decode("utf-8")

    @staticmethod
    def verify_reset_token(token):
        s = Serializer(__import__("flask").current_app.config["SECRET_KEY"])
        try:
            user_id = s.loads(token)["user_id"]
        except:
            return None
        return User.get_or_none(User.id == user_id)


class Role(BaseModel):
    id = CharField(primary_key=True, max_length=32)  # 角色ID
    name = CharField(max_length=30)  # 角色名称
    code = CharField(max_length=100)  # 角色权限字符串
    sort = IntegerField()  # 显示顺序
    data_scope = IntegerField(default=1)  # 数据范围（1：全部数据权限 2：自定数据权限 3：本部门数据权限 4：本部门及以下数据权限）
    data_scope_dept_ids = CharField(max_length=500, default='')  # 数据范围(指定部门数组)
    status = IntegerField()  # 角色状态（1正常 0停用）
    type = IntegerField()  # 角色类型
    remark = CharField(max_length=500, null=True)  # 备注


class Tenant(BaseModel):
    id = CharField(primary_key=True, max_length=32)  # 租户ID
    name = CharField(max_length=100, null=True)  # 租户名称
    public_key = CharField(max_length=255, null=True)  # 公钥
    llm_id = CharField(max_length=128)  # LLM模型ID
    embd_id = CharField(max_length=128)  # 嵌入模型ID
    asr_id = CharField(max_length=128)  # ASR模型ID
    img2txt_id = CharField(max_length=128)  # 图像转文本模型ID
    rerank_id = CharField(max_length=128)  # 重排序模型ID
    tts_id = CharField(max_length=256, null=True)  # TTS模型ID
    parser_ids = CharField(max_length=256)  # 解析器ID列表
    credit = IntegerField()  # 积分
    status = CharField(max_length=1, null=True)  # 状态


class UserTenant(BaseModel):
    id = CharField(primary_key=True, max_length=32)  # 用户租户关系ID
    user_id = CharField(max_length=32, index=True)  # 用户ID
    tenant_id = CharField(max_length=32, index=True)  # 租户ID
    role = CharField(max_length=32, index=True)  # 角色
    invited_by = CharField(max_length=32, index=True)  # 邀请人
    status = CharField(max_length=1, null=True, index=True)  # 状态

    class Meta:
        database = database_proxy
        table_name = 'user_tenant'


class UserRole(BaseModel):
    id = BigIntegerField(primary_key=True)  # 自增编号
    user_id = CharField(max_length=32)  # 用户ID
    role_id = CharField(max_length=32)  # 角色ID
    deleted = BooleanField(default=False)  # 是否删除

    class Meta:
        database = database_proxy
        table_name = 'user_role'


class TenantLlm(BaseModel):
    llm_id = CharField(max_length=32, primary_key=True)  # LLM ID
    tenant_id = CharField(max_length=32, null=False, index=True)  # 租户ID
    llm_factory = CharField(max_length=128, null=False, index=True)  # LLM厂商
    model_type = CharField(max_length=128, null=True, index=True)  # 模型类型
    model = CharField(max_length=128, null=False, index=True)  # 模型名称
    api_key = CharField(max_length=1024, null=True, index=True)  # API密钥
    base_url = CharField(max_length=255, null=True)  # API基础地址
    max_tokens = IntegerField(null=False, index=True)  # 最大令牌数
    used_tokens = IntegerField(null=False, index=True)  # 已使用令牌数
    status = CharField(max_length=255, null=True, default='0', index=True)  # 是否启用 0-不启用 1-启用

    class Meta:
        database = database_proxy
        table_name = 'tenant_llm'
        indexes = (
            (('tenant_id', 'llm_factory', 'model'), True),  # 保持唯一约束
            (('create_date',), False),
            (('update_date',), False),
            (('tenant_id',), False),
            (('llm_factory',), False),
            (('model_type',), False),
            (('model',), False),
            (('api_key',), False),
            (('max_tokens',), False),
            (('used_tokens',), False),
            (('status',), False),
        )


class McpConfig(Model):
    """MCP协议配置表"""
    config_id = CharField(max_length=50, primary_key=True, default=lambda: str(uuid.uuid4()))  # 配置唯一标识符，自动生成UUID
    agent_id = CharField(max_length=50)  # 关联的智能体ID
    mcp_server_name = CharField(max_length=100)  # MCP服务器名称
    connection_type = CharField(max_length=50)  # 连接类型
    server_url = CharField(max_length=255, null=True)  # 服务器URL地址
    command = CharField(max_length=255, null=True)  # 执行命令（stdio类型使用）
    args = JSONField(null=True)  # 命令参数列表
    status = IntegerField(default=1)  # 配置状态，1启用0关闭
    mcp_tools_info = TextField(null=True)  # MCP工具信息，保存返回的工具列表
    headers = TextField(null=True)  # headers字段
    create_date = DateTimeField(null=True)  # 创建时间
    update_date = DateTimeField(null=True)  # 更新时间

    def to_dict(self):
        return self.__dict__['__data__']

    class Meta:
        database = database_proxy
        table_name = 'mcp_configs'


class A2aAgent(Model):
    """A2A智能体配置表"""
    agent_id = CharField(max_length=50, primary_key=True, default=lambda: str(uuid.uuid4()))  # 智能体唯一标识符，自动生成UUID
    agent_name = CharField(max_length=100)  # 智能体名称
    agent_url = CharField(max_length=255)  # 智能体绑定的URL地址
    status = IntegerField(default=1)  # 智能体状态，1启用0关闭
    type = IntegerField(default=1)  # 智能体类型，0系统内置，1用户新增
    description = TextField(null=True)  # 智能体描述信息
    create_date = DateTimeField(null=True)  # 创建时间
    update_date = DateTimeField(null=True)  # 更新时间
    agent_type = IntegerField(default=2)

    def to_dict(self):
        return self.__dict__['__data__']

    class Meta:
        database = database_proxy
        table_name = 'a2a_agents'


class Dialog(BaseModel):
    """对话会话表"""
    id = CharField(primary_key=True, max_length=36, default=lambda: str(uuid.uuid4()))  # 会话ID，自动生成UUID
    name = CharField(max_length=255, index=True)  # 会话名称
    creator_id = CharField(max_length=32, index=True)  # 创建人ID
    tenant_id = CharField(max_length=32, index=True)  # 租户ID
    status = IntegerField(default=1)  # 会话状态，1正常0删除
    description = TextField()  # 会话描述（非必填）
    finished = IntegerField(default=0)  # 是否结束，0未结束，1已结束

    class Meta:
        database = database_proxy
        table_name = 'dialog'
        indexes = (
            (('create_date',), False),
            (('update_date',), False),
            (('name',), False),
            (('creator_id',), False),
            (('tenant_id',), False),
            (('status',), False),
        )


class Conversation(BaseModel):
    id = CharField(primary_key=True, max_length=64)  # 会话ID
    message = TextField(null=True)  # 消息内容
    reference = TextField(null=True)  # 引用信息
    dialog_id = CharField(max_length=64, null=False, index=True)  # 对话ID
    sort_order = IntegerField(null=True, index=True)  # 排序字段
    scene = CharField(max_length=100, null=False)  # 场景名称
    plan = LongTextField(null=False)  # 计划内容

    class Meta:
        database = database_proxy
        table_name = 'conversation'
        indexes = (
            (('dialog_id',), False),
            (('sort_order',), False),
        )


class PlanData(BaseModel):
    """计划数据表"""
    id = CharField(max_length=32, primary_key=True)  # 基于时间生成的ID
    scene = CharField(max_length=255, index=True)  # 场景
    owner = CharField(max_length=255, null=True)  # 拥有者
    plan = LongTextField()  # 计划内容
    user_id = CharField(max_length=32, index=True)  # 用户ID
    tenant_id = CharField(max_length=32, index=True)  # 租户ID
    status = CharField(max_length=1, default='1', index=True)  # 状态

    @staticmethod
    def generate_time_based_id():
        """生成基于时间的ID"""
        import time
        import random

        # 获取当前时间戳（毫秒级）
        timestamp = int(time.time() * 1000)

        # 添加随机数防止并发冲突
        random_num = random.randint(1000, 9999)

        # 组合生成ID: 时间戳 + 随机数
        plan_id = f"{timestamp}{random_num}"

        return plan_id

    class Meta:
        database = database_proxy
        table_name = 'plan_data'
        indexes = (
            (('scene', 'user_id'), True),  # 唯一索引
            (('user_id',), False),
            (('tenant_id',), False),
            (('status',), False),
        )


# 创建所有表
def create_tables():
    from .llm_models import LLMFactories, LLM, TenantLLM
    with DB.connection_context():
        DB.get_instance().create_tables([User, Role, Tenant, UserTenant, UserRole, TenantLlm, LLMFactories, LLM, TenantLLM, McpConfig, A2aAgent, Dialog, Conversation, PlanData], safe=True)