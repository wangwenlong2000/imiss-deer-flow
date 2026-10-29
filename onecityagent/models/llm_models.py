# -*- coding: utf-8 -*-
# LLM related database models for OneCity Agent

from peewee import (
    CharField, IntegerField, TextField, CompositeKey, BigIntegerField, DateTimeField
)
from onecityagent.models.db_models import BaseModel, DB


class LLMFactories(BaseModel):
    """LLM工厂模型"""
    name = CharField(
        max_length=128,
        null=False,
        help_text="LLM factory name",
        primary_key=True
    )
    logo = TextField(null=True, help_text="llm logo base64")
    tags = CharField(
        max_length=255,
        null=False,
        help_text="LLM, Text Embedding, Image2Text, ASR",
        index=True
    )
    status = CharField(
        max_length=1,
        null=True,
        help_text="is it validate(0: wasted, 1: validate)",
        default="1",
        index=True
    )

    def __str__(self):
        return self.name

    class Meta:
        database = DB.get_instance()
        table_name = "llm_factories"


class LLM(BaseModel):
    """LLM模型字典"""
    llm_name = CharField(
        max_length=128,
        null=False,
        help_text="LLM name",
        index=True
    )
    model_type = CharField(
        max_length=128,
        null=False,
        help_text="LLM, Text Embedding, Image2Text, ASR",
        index=True
    )
    fid = CharField(
        max_length=128,
        null=False,
        help_text="LLM factory id",
        index=True
    )
    max_tokens = IntegerField(null=False)
    tags = CharField(
        max_length=255,
        null=False,
        help_text="LLM, Text Embedding, Image2Text, Chat, 32k...",
        index=True
    )
    status = CharField(
        max_length=1,
        null=True,
        help_text="is it validate(0: wasted, 1: validate)",
        default="1",
        index=True
    )

    def __str__(self):
        return self.llm_name

    class Meta:
        database = DB.get_instance()
        table_name = "llm"
        primary_key = CompositeKey('llm_name', 'fid')


class TenantLLM(BaseModel):
    """租户LLM配置表"""
    llm_id = CharField(max_length=32, null=False, primary_key=True)  # LLM ID
    create_date = DateTimeField(null=True, index=True)  # 创建时间
    update_date = DateTimeField(null=True, index=True)  # 更新时间
    tenant_id = CharField(max_length=32, null=False, index=True)
    llm_factory = CharField(
        max_length=128,
        null=False,
        help_text="LLM factory name",
        index=True
    )
    model_type = CharField(
        max_length=128,
        null=True,
        help_text="LLM, Text Embedding, Image2Text, ASR",
        index=True
    )
    model = CharField(
        max_length=128,
        null=True,
        help_text="Model name",
        default="",
        index=True
    )
    api_key = CharField(
        max_length=1024,
        null=True,
        help_text="API KEY",
        index=True
    )
    base_url = CharField(
        max_length=255,
        null=True,
        help_text="API Base URL"
    )
    max_tokens = IntegerField(default=8192, index=True)
    used_tokens = IntegerField(default=0, index=True)
    status = CharField(
        max_length=255,
        null=True,
        help_text="是否启用 0-不启用 1-启用",
        default="0",
        index=True
    )

    def __str__(self):
        return self.model

    class Meta:
        database = DB.get_instance()
        table_name = "tenant_llm"
        indexes = (
            (('tenant_id', 'llm_factory', 'model'), True),  # 保持唯一约束
        )


# 创建LLM相关表
def create_llm_tables():
    """创建LLM相关的数据库表"""
    with DB.connection_context():
        DB.get_instance().create_tables([LLMFactories, LLM, TenantLLM], safe=True)