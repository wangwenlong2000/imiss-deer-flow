# -*- coding: utf-8 -*-
# LLM Service Layer for OneCity Agent

import json
import re
from typing import Dict, List, Optional, Any
from onecityagent.models.llm_models import LLMFactories, LLM, TenantLLM
from onecityagent.enums.common_enums import LLMStatusEnum
from onecityagent.models.db_models import DB
from onecityagent.service.llm_service.llm_models import (
    EmbeddingModel, ChatModel, RerankModel, CVModel, Seq2txtModel, TTSModel
)


class LLMFactoriesService:
    """LLM工厂服务"""
    
    @staticmethod
    def get_all():
        """获取所有LLM工厂"""
        with DB.connection_context():
            return list(LLMFactories.select())
    
    @staticmethod
    def get_by_name(name: str):
        """根据名称获取LLM工厂"""
        with DB.connection_context():
            return LLMFactories.get_or_none(LLMFactories.name == name)
    
    @staticmethod
    def filter_by_tags(tags: str):
        """根据标签过滤LLM工厂"""
        with DB.connection_context():
            return list(LLMFactories.select().where(LLMFactories.tags.contains(tags)))


class LLMService:
    """LLM模型服务"""
    
    @staticmethod
    def get_all():
        """获取所有LLM模型"""
        with DB.connection_context():
            return list(LLM.select())
    
    @staticmethod
    def get_by_factory(fid: str):
        """根据工厂ID获取LLM模型"""
        with DB.connection_context():
            return list(LLM.select().where(LLM.fid == fid))
    
    @staticmethod
    def get_by_type(model_type: str):
        """根据模型类型获取LLM模型"""
        with DB.connection_context():
            return list(LLM.select().where(LLM.model_type == model_type))
    
    @staticmethod
    def get_by_name_and_factory(llm_name: str, fid: str):
        """根据名称和工厂ID获取LLM模型"""
        with DB.connection_context():
            return LLM.get_or_none((LLM.llm_name == llm_name) & (LLM.fid == fid))


class TenantLLMService:
    """租户LLM服务"""
    
    @staticmethod
    def get_api_key(tenant_id: str, llm_factory: str, model: str) -> Optional[str]:
        """获取租户的API密钥"""
        with DB.connection_context():
            tenant_llm = TenantLLM.get_or_none(
                (TenantLLM.tenant_id == tenant_id) &
                (TenantLLM.llm_factory == llm_factory) &
                (TenantLLM.model == model)
            )
            return tenant_llm.api_key if tenant_llm else None
    
    @staticmethod
    def get_by_tenant(tenant_id: str):
        """根据租户ID获取所有LLM配置"""
        with DB.connection_context():
            return TenantLLM.select().where(TenantLLM.tenant_id == tenant_id)
    
    @staticmethod
    def get_by_llm_id(llm_id: str):
        """根据LLM ID获取配置"""
        with DB.connection_context():
            return TenantLLM.get_or_none(TenantLLM.llm_id == llm_id)
    
    @staticmethod
    def get_my_llms(tenant_id: str, model_type: str = None) -> List[Dict]:
        """获取租户的LLM列表"""
        with DB.connection_context():
            # 使用JOIN查询获取工厂信息
            query = (TenantLLM
                    .select(TenantLLM.llm_id,
                           TenantLLM.llm_factory, 
                           LLMFactories.logo,
                           LLMFactories.tags,
                           TenantLLM.model_type,
                           TenantLLM.model,
                           TenantLLM.used_tokens,
                           TenantLLM.status)
                    .join(LLMFactories, on=(TenantLLM.llm_factory == LLMFactories.name))
                    .where(TenantLLM.tenant_id == tenant_id, 
                           TenantLLM.api_key.is_null(False)))
            
            if model_type:
                query = query.where(TenantLLM.model_type == model_type)
            
            result = []
            for row in query.dicts():
                result.append({
                    'llm_id': row['llm_id'],
                    'llm_factory': row['llm_factory'],
                    'logo': row['logo'],
                    'tags': row['tags'],
                    'model_type': row['model_type'],
                    'model': row['model'],
                    'used_tokens': row['used_tokens'],
                    'status': row['status']
                })
            return result

    @staticmethod
    def get_all_llms(model_type: str = None) -> List[Dict]:
        """获取租户的LLM列表"""
        with DB.connection_context():
            # 使用JOIN查询获取工厂信息
            query = (TenantLLM
                     .select(TenantLLM.llm_factory,
                             LLMFactories.logo,
                             LLMFactories.tags,
                             TenantLLM.model_type,
                             TenantLLM.model,
                             TenantLLM.used_tokens,
                             TenantLLM.status)
                     .join(LLMFactories, on=(TenantLLM.llm_factory == LLMFactories.name))
                     .where(TenantLLM.api_key.is_null(False)))

            if model_type:
                query = query.where(TenantLLM.model_type == model_type)

            result = []
            for row in query.dicts():
                result.append({
                    'llm_factory': row['llm_factory'],
                    'logo': row['logo'],
                    'tags': row['tags'],
                    'model_type': row['model_type'],
                    'model': row['model'],
                    'used_tokens': row['used_tokens'],
                    'status': row['status']
                })
            return result


    @staticmethod
    def create(**kwargs):
        """创建新的租户LLM配置"""
        import uuid
        from datetime import datetime
        
        # 生成唯一的 llm_id
        if 'llm_id' not in kwargs:
            kwargs['llm_id'] = str(uuid.uuid4()).replace('-', '')[:32]
        
        # 设置创建时间
        if 'create_date' not in kwargs:
            kwargs['create_date'] = datetime.now()
        if 'update_date' not in kwargs:
            kwargs['update_date'] = datetime.now()
        
        with DB.connection_context():
            return TenantLLM.create(**kwargs)
    
    @staticmethod
    def update(llm_id: str, **kwargs):
        """更新租户LLM配置"""
        from datetime import datetime
        
        with DB.connection_context():
            tenant_llm = TenantLLM.get_or_none(TenantLLM.llm_id == llm_id)
            if not tenant_llm:
                raise ValueError(f"TenantLLM with llm_id {llm_id} not found")
                
            # 设置更新时间
            kwargs['update_date'] = datetime.now()
            
            for key, value in kwargs.items():
                setattr(tenant_llm, key, value)
            tenant_llm.save()
            return tenant_llm
    
    @staticmethod
    def save_or_update(tenant_id: str, llm_factory: str, model: str, **kwargs):
        """保存或更新租户LLM配置"""
        import uuid
        from datetime import datetime
        
        with DB.connection_context():
            # 先查找是否存在相同的配置（基于唯一约束）
            tenant_llm = TenantLLM.get_or_none(
                (TenantLLM.tenant_id == tenant_id) &
                (TenantLLM.llm_factory == llm_factory) &
                (TenantLLM.model == model)
            )
            
            if tenant_llm:
                # 更新现有记录
                kwargs['update_date'] = datetime.now()
                for key, value in kwargs.items():
                    setattr(tenant_llm, key, value)
                tenant_llm.save()
                created = False
            else:
                # 创建新记录
                if 'llm_id' not in kwargs:
                    kwargs['llm_id'] = str(uuid.uuid4()).replace('-', '')[:32]
                kwargs['tenant_id'] = tenant_id
                kwargs['llm_factory'] = llm_factory
                kwargs['model'] = model
                kwargs['create_date'] = datetime.now()
                kwargs['update_date'] = datetime.now()
                tenant_llm = TenantLLM.create(**kwargs)
                created = True
            
            return tenant_llm, created
    
    @staticmethod
    def get_factory_and_name(llm_factory: str, model: str):
        """根据租户ID、工厂和模型名称获取LLM配置"""
        with DB.connection_context():
            return TenantLLM.get_or_none(
                (TenantLLM.llm_factory == llm_factory) &
                (TenantLLM.model == model)
            )

    @staticmethod
    def get_by_status_on():
        """获取指定租户启用状态的LLM配置
        """
        with DB.connection_context():
            query = TenantLLM.select().where(
                (TenantLLM.status == str(LLMStatusEnum.ENABLED))
            )

            result = []
            for row in query.dicts():
                result.append({
                    'llm_factory': row['llm_factory'],
                    'api_key': row['api_key'],
                    'model_type': row['model_type'],
                    'base_url': row['base_url'],
                    'model': row['model'],
                    'max_tokens': row['max_tokens'],
                    'used_tokens': row['used_tokens'],
                    'status': row['status']
                })
            return list(result)

    @staticmethod
    def delete(tenant_id: str, llm_factory: str, model: str = None):
        """删除租户LLM配置"""
        with DB.connection_context():
            query = TenantLLM.delete().where(
                (TenantLLM.tenant_id == tenant_id) &
                (TenantLLM.llm_factory == llm_factory)
            )
            if model:
                query = query.where(TenantLLM.model == model)
            
            return query.execute()
    
    @staticmethod
    def model_instance(tenant_id: str, llm_factory: str, model: str, model_type: str):
        """获取模型实例"""
        with DB.connection_context():
            tenant_llm = TenantLLM.get_or_none(
                (TenantLLM.tenant_id == tenant_id) &
                (TenantLLM.llm_factory == llm_factory) &
                (TenantLLM.model == model)
            )
            
            if not tenant_llm:
                raise ValueError(f"No LLM configuration found for {llm_factory}/{model}")
            
            # 构建模型配置
            config = {
                'api_key': tenant_llm.api_key,
                'base_url': tenant_llm.base_url,
                'model_name': model
            }
        
        # 根据模型类型返回相应的模型实例
        if model_type == "EMBEDDING":
            return EmbeddingModel[llm_factory](key=tenant_llm.api_key, model_name=model, base_url=tenant_llm.base_url)
        elif model_type == "RERANK":
            return RerankModel[llm_factory](key=tenant_llm.api_key, model_name=model, base_url=tenant_llm.base_url)
        elif model_type == "IMAGE2TEXT":
            return CvModel[llm_factory](key=tenant_llm.api_key, model_name=model, base_url=tenant_llm.base_url)
        elif model_type == "CHAT":
            return ChatModel[llm_factory](key=tenant_llm.api_key, model_name=model, base_url=tenant_llm.base_url)
        elif model_type == "SPEECH2TEXT":
            return Seq2txtModel[llm_factory](key=tenant_llm.api_key, model_name=model, base_url=tenant_llm.base_url)
        elif model_type == "TTS":
            return TTSModel[llm_factory](key=tenant_llm.api_key, model_name=model, base_url=tenant_llm.base_url)
        else:
            raise ValueError(f"Unsupported model type: {model_type}")
    
    @staticmethod
    def increase_usage(tenant_id: str, llm_factory: str, model: str, used_tokens: int):
        """增加使用量"""
        with DB.connection_context():
            tenant_llm = TenantLLM.get_or_none(
                (TenantLLM.tenant_id == tenant_id) &
                (TenantLLM.llm_factory == llm_factory) &
                (TenantLLM.model == model)
            )
            
            if tenant_llm:
                tenant_llm.used_tokens += used_tokens
                tenant_llm.save()
                return tenant_llm.used_tokens
            return 0
    
    @staticmethod
    def get_openai_models():
        """获取OpenAI模型列表"""
        return [
            "gpt-3.5-turbo",
            "gpt-3.5-turbo-16k",
            "gpt-4",
            "gpt-4-32k",
            "gpt-4-turbo",
            "gpt-4o",
            "gpt-4o-mini",
            "text-embedding-ada-002",
            "text-embedding-3-small",
            "text-embedding-3-large"
        ]
    
    @staticmethod
    def get_by_tenant_and_factory_and_name(tenant_id: str, llm_factory: str, model: str):
        """根据租户ID、工厂和模型名称获取LLM配置"""
        with DB.connection_context():
            return TenantLLM.get_or_none(
                (TenantLLM.tenant_id == tenant_id) &
                (TenantLLM.llm_factory == llm_factory) &
                (TenantLLM.model == model)
            )
    
    @staticmethod
    def delete_by_tenant_and_factory_and_name(llm_factory: str, model: str):
        """根据租户ID、工厂和模型名称删除LLM配置"""
        with DB.connection_context():
            query = TenantLLM.delete().where(
                (TenantLLM.llm_factory == llm_factory) &
                (TenantLLM.model == model)
            )
            return query.execute()
    
    @staticmethod
    def delete_by_tenant_and_factory(tenant_id: str, llm_factory: str):
        """根据租户ID和工厂删除所有LLM配置"""
        with DB.connection_context():
            query = TenantLLM.delete().where(
                (TenantLLM.tenant_id == tenant_id) &
                (TenantLLM.llm_factory == llm_factory)
            )
            return query.execute()
    
    @staticmethod
    def disable_all_models_for_tenant():
        """禁用租户的所有模型"""
        with DB.connection_context():
            query = TenantLLM.update(status=str(LLMStatusEnum.DISABLED))
            return query.execute()


class LLMBundle:
    """LLM模型包装器，提供统一的接口"""
    
    def __init__(self, tenant_id: str, llm_type: str, model: str, llm_factory: str):
        self.tenant_id = tenant_id
        self.llm_type = llm_type
        self.model = model
        self.llm_factory = llm_factory
        self._model = None
    
    @property
    def model(self):
        """懒加载模型实例"""
        if self._model is None:
            self._model = TenantLLMService.model_instance(
                self.tenant_id, self.llm_factory, self.model, self.llm_type
            )
        return self._model
    
    def encode(self, texts: List[str], **kwargs):
        """文本编码"""
        if self.llm_type != "EMBEDDING":
            raise ValueError("This model is not an embedding model")
        
        result = self.model.encode(texts, **kwargs)
        
        # 计算token使用量
        used_tokens = sum(len(text.split()) for text in texts)
        TenantLLMService.increase_usage(self.tenant_id, self.llm_factory, self.model, used_tokens)
        
        return result
    
    def encode_queries(self, texts: List[str], **kwargs):
        """查询编码"""
        if self.llm_type != "EMBEDDING":
            raise ValueError("This model is not an embedding model")
        
        result = self.model.encode_queries(texts, **kwargs)
        
        # 计算token使用量
        used_tokens = sum(len(text.split()) for text in texts)
        TenantLLMService.increase_usage(self.tenant_id, self.llm_factory, self.llm_name, used_tokens)
        
        return result
    
    def similarity(self, query_embeddings, doc_embeddings):
        """计算相似度"""
        if self.llm_type != "EMBEDDING":
            raise ValueError("This model is not an embedding model")
        
        return self.model.similarity(query_embeddings, doc_embeddings)
    
    def describe(self, image, max_length: int = 300):
        """图像描述"""
        if self.llm_type != "IMAGE2TEXT":
            raise ValueError("This model is not an image-to-text model")
        
        result = self.model.describe(image, max_length)
        
        # 计算token使用量
        used_tokens = max_length // 4  # 估算
        TenantLLMService.increase_usage(self.tenant_id, self.llm_factory, self.llm_name, used_tokens)
        
        return result
    
    def transcription(self, audio):
        """语音转文本"""
        if self.llm_type != "SPEECH2TEXT":
            raise ValueError("This model is not a speech-to-text model")
        
        result = self.model.transcription(audio)
        
        # 计算token使用量（估算）
        used_tokens = len(result.split()) if isinstance(result, str) else 100
        TenantLLMService.increase_usage(self.tenant_id, self.llm_factory, self.llm_name, used_tokens)
        
        return result
    
    def tts(self, text: str):
        """文本转语音"""
        if self.llm_type != "TTS":
            raise ValueError("This model is not a text-to-speech model")
        
        result = self.model.tts(text)
        
        # 计算token使用量
        used_tokens = len(text.split())
        TenantLLMService.increase_usage(self.tenant_id, self.llm_factory, self.llm_name, used_tokens)
        
        return result
    
    def chat(self, system: str, history: List[List[str]], gen_conf: Dict = None):
        """聊天对话"""
        if self.llm_type != "CHAT":
            raise ValueError("This model is not a chat model")
        
        result = self.model.chat(system, history, gen_conf or {})
        
        # 计算token使用量
        total_text = system + " ".join([" ".join(turn) for turn in history])
        if isinstance(result, str):
            total_text += result
        used_tokens = len(total_text.split())
        TenantLLMService.increase_usage(self.tenant_id, self.llm_factory, self.llm_name, used_tokens)
        
        return result