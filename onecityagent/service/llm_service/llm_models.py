# -*- coding: utf-8 -*-
# LLM Models Implementation for OneCity Agent

import logging
import re
import threading
import requests
import os
import json
from abc import ABC
from typing import List, Dict, Any, Optional
import numpy as np

try:
    from openai import OpenAI, AzureOpenAI
except ImportError:
    OpenAI = None
    AzureOpenAI = None

try:
    from groq import Groq
except ImportError:
    Groq = None

try:
    import dashscope
except ImportError:
    dashscope = None

try:
    from zhipuai import ZhipuAI
except ImportError:
    ZhipuAI = None

try:
    from ollama import Client
except ImportError:
    Client = None

try:
    from groq import Groq
except ImportError:
    Groq = None

try:
    from openai.lib.azure import AzureOpenAI
except ImportError:
    AzureOpenAI = None

try:
    import asyncio
except ImportError:
    asyncio = None


def num_tokens_from_string(string: str, encoding_name: str = "cl100k_base") -> int:
    """Returns the number of tokens in a text string."""
    try:
        import tiktoken
        encoding = tiktoken.get_encoding(encoding_name)
        num_tokens = len(encoding.encode(string))
        return num_tokens
    except ImportError:
        # Fallback to simple word count if tiktoken is not available
        return len(string.split())


def is_english(texts):
    """Check if text is in English"""
    if not texts:
        return True
    text = texts[0] if isinstance(texts, list) else texts
    # Simple heuristic: if more than 80% are ASCII characters, consider it English
    ascii_count = sum(1 for c in text if ord(c) < 128)
    return ascii_count / len(text) > 0.8 if text else True


def truncate(text: str, max_length: int) -> str:
    """Truncate text to max_length characters."""
    if len(text) <= max_length:
        return text
    return text[:max_length]


class BaseEmbedding(ABC):
    """Base class for embedding models"""
    
    def __init__(self, key: str, model_name: str, **kwargs):
        self.key = key
        self.model_name = model_name
        self.base_url = kwargs.get('base_url')
    
    def encode(self, texts: List[str], batch_size: int = 16) -> tuple:
        """Encode texts to embeddings"""
        raise NotImplementedError("Please implement encode method!")
    
    def encode_queries(self, text: str) -> tuple:
        """Encode query text to embedding"""
        raise NotImplementedError("Please implement encode_queries method!")
    
    def similarity(self, query_embeddings, doc_embeddings):
        """Calculate similarity between query and document embeddings"""
        if isinstance(query_embeddings, list):
            query_embeddings = np.array(query_embeddings)
        if isinstance(doc_embeddings, list):
            doc_embeddings = np.array(doc_embeddings)
        
        # Normalize embeddings
        query_norm = query_embeddings / np.linalg.norm(query_embeddings)
        doc_norm = doc_embeddings / np.linalg.norm(doc_embeddings, axis=1, keepdims=True)
        
        # Calculate cosine similarity
        return np.dot(doc_norm, query_norm)


class OpenAIEmbedding(BaseEmbedding):
    """OpenAI embedding model"""
    
    def __init__(self, key: str, model_name: str = "text-embedding-ada-002", base_url: str = None, **kwargs):
        super().__init__(key, model_name, **kwargs)
        if not OpenAI:
            raise ImportError("OpenAI package is required for OpenAI embedding")
        
        if not base_url:
            base_url = "https://api.openai.com/v1"
        
        self.client = OpenAI(api_key=key, base_url=base_url)
    
    def encode(self, texts: List[str], batch_size: int = 16) -> tuple:
        texts = [truncate(t, 8191) for t in texts]
        res = self.client.embeddings.create(
            input=texts,
            model=self.model_name
        )
        return (
            np.array([d.embedding for d in res.data]),
            res.usage.total_tokens
        )
    
    def encode_queries(self, text: str) -> tuple:
        res = self.client.embeddings.create(
            input=[truncate(text, 8191)],
            model=self.model_name
        )
        return (
            np.array(res.data[0].embedding),
            res.usage.total_tokens
        )


class AzureEmbedding(BaseEmbedding):
    """Azure OpenAI embedding model"""
    
    def __init__(self, key: str, model_name: str, base_url: str = None, **kwargs):
        super().__init__(key, model_name, **kwargs)
        if not OpenAI:
            raise ImportError("OpenAI package is required for Azure embedding")
        
        from openai.lib.azure import AzureOpenAI
        
        # Parse key if it's JSON
        if key.startswith('{'):
            key_data = json.loads(key)
            api_key = key_data.get('api_key', '')
            api_version = key_data.get('api_version', '2024-02-01')
        else:
            api_key = key
            api_version = '2024-02-01'
        
        self.client = AzureOpenAI(
            api_key=api_key,
            azure_endpoint=base_url,
            api_version=api_version
        )
    
    def encode(self, texts: List[str], batch_size: int = 16) -> tuple:
        texts = [truncate(t, 8191) for t in texts]
        res = self.client.embeddings.create(
            input=texts,
            model=self.model_name
        )
        return (
            np.array([d.embedding for d in res.data]),
            res.usage.total_tokens
        )
    
    def encode_queries(self, text: str) -> tuple:
        res = self.client.embeddings.create(
            input=[truncate(text, 8191)],
            model=self.model_name
        )
        return (
            np.array(res.data[0].embedding),
            res.usage.total_tokens
        )


class QWenEmbedding(BaseEmbedding):
    """QWen embedding model"""
    
    def __init__(self, key: str, model_name: str = "text_embedding_v2", **kwargs):
        super().__init__(key, model_name, **kwargs)
        if not dashscope:
            raise ImportError("dashscope package is required for QWen embedding")
        
        dashscope.api_key = key
    
    def encode(self, texts: List[str], batch_size: int = 10) -> tuple:
        batch_size = min(batch_size, 4)
        try:
            res = []
            token_count = 0
            texts = [truncate(t, 2048) for t in texts]
            
            for i in range(0, len(texts), batch_size):
                resp = dashscope.TextEmbedding.call(
                    model=self.model_name,
                    input=texts[i:i + batch_size],
                    text_type="document"
                )
                embds = [[] for _ in range(len(resp["output"]["embeddings"]))]
                for e in resp["output"]["embeddings"]:
                    embds[e["text_index"]] = e["embedding"]
                res.extend(embds)
                token_count += resp["usage"]["total_tokens"]
            
            return np.array(res), token_count
        except Exception as e:
            raise Exception(f"QWen embedding error: {str(e)}")
    
    def encode_queries(self, text: str) -> tuple:
        try:
            resp = dashscope.TextEmbedding.call(
                model=self.model_name,
                input=text[:2048],
                text_type="query"
            )
            return (
                np.array(resp["output"]["embeddings"][0]["embedding"]),
                resp["usage"]["total_tokens"]
            )
        except Exception as e:
            raise Exception(f"QWen embedding error: {str(e)}")


class ZhipuEmbedding(BaseEmbedding):
    """Zhipu embedding model"""
    
    def __init__(self, key: str, model_name: str = "embedding-2", **kwargs):
        super().__init__(key, model_name, **kwargs)
        if not ZhipuAI:
            raise ImportError("zhipuai package is required for Zhipu embedding")
        
        self.client = ZhipuAI(api_key=key)
    
    def encode(self, texts: List[str], batch_size: int = 16) -> tuple:
        texts = [truncate(t, 2048) for t in texts]
        res = self.client.embeddings.create(
            model=self.model_name,
            input=texts
        )
        return (
            np.array([d.embedding for d in res.data]),
            res.usage.total_tokens
        )
    
    def encode_queries(self, text: str) -> tuple:
        res = self.client.embeddings.create(
            model=self.model_name,
            input=[truncate(text, 2048)]
        )
        return (
            np.array(res.data[0].embedding),
            res.usage.total_tokens
        )


class OllamaEmbedding(BaseEmbedding):
    """Ollama embedding model"""
    
    def __init__(self, key: str, model_name: str, base_url: str = "http://localhost:11434", **kwargs):
        super().__init__(key, model_name, **kwargs)
        if not Client:
            raise ImportError("ollama package is required for Ollama embedding")
        
        self.client = Client(host=base_url)
    
    def encode(self, texts: List[str], batch_size: int = 16) -> tuple:
        embeddings = []
        token_count = 0
        
        for text in texts:
            response = self.client.embeddings(
                model=self.model_name,
                prompt=text
            )
            embeddings.append(response['embedding'])
            token_count += num_tokens_from_string(text)
        
        return np.array(embeddings), token_count
    
    def encode_queries(self, text: str) -> tuple:
        response = self.client.embeddings(
            model=self.model_name,
            prompt=text
        )
        return (
            np.array(response['embedding']),
            num_tokens_from_string(text)
        )


# Base classes for other model types
class BaseChatModel(ABC):
    """Base class for chat models"""
    
    def __init__(self, key: str, model_name: str, **kwargs):
        self.key = key
        self.model_name = model_name
        self.base_url = kwargs.get('base_url')
        timeout = int(os.environ.get('LM_TIMEOUT_SECONDS', 600))
        if OpenAI:
            self.client = OpenAI(api_key=key, base_url=self.base_url, timeout=timeout)
    
    def chat(self, system: str, history: List[Dict[str, str]], gen_conf: Dict = None) -> tuple:
        """Generate chat response"""
        if system:
            history.insert(0, {"role": "system", "content": system})
        try:
            response = self.client.chat.completions.create(
                model=self.model_name,
                extra_body={
                    "repetition_penalty": 1.05
                },
                messages=history,
                **gen_conf)
            ans = response.choices[0].message.content.strip()
            if response.choices[0].finish_reason == "length":
                ans += "...\nFor the content length reason, it stopped, continue?" if is_english(
                    [ans]) else "······\n由于长度的原因，回答被截断了，要继续吗？"
            return ans, response.usage.total_tokens
        except Exception as e:
            return "**ERROR**: " + str(e), 0
    
    def chat_streamly(self, system: str, history: List[Dict[str, str]], gen_conf: Dict = None):
        """Generate chat response in streaming mode"""
        if system:
            history.insert(0, {"role": "system", "content": system})
        ans = ""
        total_tokens = 0
        try:
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=history,
                extra_body={
                    "repetition_penalty": 1.05
                },
                stream=True,
                **gen_conf)
            for resp in response:
                if not resp.choices: continue
                if not resp.choices[0].delta.content:
                    resp.choices[0].delta.content = ""
                ans += resp.choices[0].delta.content

                if not hasattr(resp, "usage") or not resp.usage:
                    total_tokens = (
                                total_tokens
                                + num_tokens_from_string(resp.choices[0].delta.content)
                        )
                elif isinstance(resp.usage, dict):
                    total_tokens = resp.usage.get("total_tokens", total_tokens)
                else: total_tokens = resp.usage.total_tokens

                if resp.choices[0].finish_reason == "length":
                    ans += "...\nFor the content length reason, it stopped, continue?" if is_english(
                        [ans]) else "······\n由于长度的原因，回答被截断了，要继续吗？"
                yield ans

        except Exception as e:
            yield ans + "\n**ERROR**: " + str(e)

        yield total_tokens


class OpenAIChat(BaseChatModel):
    """OpenAI chat model"""
    
    def __init__(self, key: str, model_name: str = "gpt-3.5-turbo", base_url: str = "https://api.openai.com/v1", **kwargs):
        if not base_url:
            base_url = "https://api.openai.com/v1"
        super().__init__(key, model_name, base_url=base_url, **kwargs)


class AzureChat(BaseChatModel):
    """Azure OpenAI chat model"""
    
    def __init__(self, key: str, model_name: str, **kwargs):
        if not AzureOpenAI:
            raise ImportError("Azure OpenAI package is required for Azure chat")
        
        # Parse key if it's JSON
        if key.startswith('{'):
            key_data = json.loads(key)
            api_key = key_data.get('api_key', '')
            api_version = key_data.get('api_version', '2024-02-01')
        else:
            api_key = key
            api_version = '2024-02-01'
        
        self.key = key
        self.model_name = model_name
        self.base_url = kwargs.get('base_url')
        self.client = AzureOpenAI(
            api_key=api_key,
            azure_endpoint=self.base_url,
            api_version=api_version
        )


class QWenChat(BaseChatModel):
    """QWen chat model"""
    
    def __init__(self, key: str, model_name: str = "qwen-turbo", **kwargs):
        if not dashscope:
            raise ImportError("dashscope package is required for QWen chat")
        
        import dashscope
        from dashscope import Generation
        dashscope.api_key = key
        self.key = key
        self.model_name = model_name
        self.base_url = kwargs.get('base_url')
        self.Generation = Generation
    
    def chat(self, system: str, history: List[Dict[str, str]], gen_conf: Dict = None) -> tuple:
        from http import HTTPStatus
        if system:
            history.insert(0, {"role": "system", "content": system})
        
        try:
            response = self.Generation.call(
                self.model_name,
                messages=history,
                result_format='message',
                **gen_conf
            )
            ans = ""
            tk_count = 0
            if response.status_code == HTTPStatus.OK:
                ans += response.output.choices[0]['message']['content']
                tk_count += response.usage.total_tokens
                if response.output.choices[0].get("finish_reason", "") == "length":
                    ans += "...\nFor the content length reason, it stopped, continue?" if is_english(
                        [ans]) else "······\n由于长度的原因，回答被截断了，要继续吗？"
                return ans, tk_count
            
            return "**ERROR**: " + response.message, tk_count
        except Exception as e:
            return "**ERROR**: " + str(e), 0
    
    def chat_streamly(self, system: str, history: List[Dict[str, str]], gen_conf: Dict = None):
        from http import HTTPStatus
        if system:
            history.insert(0, {"role": "system", "content": system})
        ans = ""
        tk_count = 0
        try:
            response = self.Generation.call(
                self.model_name,
                messages=history,
                result_format='message',
                stream=True,
                incremental_output=True,
                **gen_conf
            )
            for resp in response:
                if resp.status_code == HTTPStatus.OK:
                    ans = resp.output.choices[0]['message']['content']
                    tk_count = resp.usage.total_tokens
                    if resp.output.choices[0].get("finish_reason", "") == "length":
                        ans += "...\nFor the content length reason, it stopped, continue?" if is_english(
                            [ans]) else "······\n由于长度的原因，回答被截断了，要继续吗？"
                    yield ans
                else:
                    yield ans + "\n**ERROR**: " + resp.message
        except Exception as e:
            yield ans + "\n**ERROR**: " + str(e)
        
        yield tk_count


class ZhipuChat(BaseChatModel):
    """Zhipu chat model"""
    
    def __init__(self, key: str, model_name: str = "glm-3-turbo", **kwargs):
        if not ZhipuAI:
            raise ImportError("zhipuai package is required for Zhipu chat")
        
        self.key = key
        self.model_name = model_name
        self.base_url = kwargs.get('base_url')
        self.client = ZhipuAI(api_key=key)
    
    def chat(self, system: str, history: List[Dict[str, str]], gen_conf: Dict = None) -> tuple:
        if system:
            history.insert(0, {"role": "system", "content": system})
        try:
            if "presence_penalty" in gen_conf: del gen_conf["presence_penalty"]
            if "frequency_penalty" in gen_conf: del gen_conf["frequency_penalty"]
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=history,
                **gen_conf
            )
            ans = response.choices[0].message.content.strip()
            if response.choices[0].finish_reason == "length":
                ans += "...\nFor the content length reason, it stopped, continue?" if is_english(
                    [ans]) else "······\n由于长度的原因，回答被截断了，要继续吗？"
            return ans, response.usage.total_tokens
        except Exception as e:
            return "**ERROR**: " + str(e), 0
    
    def chat_streamly(self, system: str, history: List[Dict[str, str]], gen_conf: Dict = None):
        if system:
            history.insert(0, {"role": "system", "content": system})
        if "presence_penalty" in gen_conf: del gen_conf["presence_penalty"]
        if "frequency_penalty" in gen_conf: del gen_conf["frequency_penalty"]
        ans = ""
        tk_count = 0
        try:
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=history,
                stream=True,
                **gen_conf
            )
            for resp in response:
                if not resp.choices[0].delta.content: continue
                delta = resp.choices[0].delta.content
                ans += delta
                if resp.choices[0].finish_reason == "length":
                    ans += "...\nFor the content length reason, it stopped, continue?" if is_english(
                        [ans]) else "······\n由于长度的原因，回答被截断了，要继续吗？"
                    tk_count = resp.usage.total_tokens
                if resp.choices[0].finish_reason == "stop": tk_count = resp.usage.total_tokens
                yield ans
        except Exception as e:
            yield ans + "\n**ERROR**: " + str(e)
        
        yield tk_count


class OllamaChat(BaseChatModel):
    """Ollama chat model"""
    
    def __init__(self, key: str, model_name: str, **kwargs):
        if not Client:
            raise ImportError("ollama package is required for Ollama chat")
        
        self.key = key
        self.model_name = model_name
        self.base_url = kwargs.get('base_url', 'http://localhost:11434')
        self.client = Client(host=self.base_url)
    
    def chat(self, system: str, history: List[Dict[str, str]], gen_conf: Dict = None) -> tuple:
        if system:
            history.insert(0, {"role": "system", "content": system})
        try:
            options = {}
            if "temperature" in gen_conf: options["temperature"] = gen_conf["temperature"]
            if "max_tokens" in gen_conf: options["num_predict"] = gen_conf["max_tokens"]
            if "top_p" in gen_conf: options["top_k"] = gen_conf["top_p"]
            if "presence_penalty" in gen_conf: options["presence_penalty"] = gen_conf["presence_penalty"]
            if "frequency_penalty" in gen_conf: options["frequency_penalty"] = gen_conf["frequency_penalty"]
            response = self.client.chat(
                model=self.model_name,
                messages=history,
                options=options,
                keep_alive=-1
            )
            ans = response["message"]["content"].strip()
            return ans, response.get("eval_count", 0) + response.get("prompt_eval_count", 0)
        except Exception as e:
            return "**ERROR**: " + str(e), 0
    
    def chat_streamly(self, system: str, history: List[Dict[str, str]], gen_conf: Dict = None):
        if system:
            history.insert(0, {"role": "system", "content": system})
        options = {}
        if "temperature" in gen_conf: options["temperature"] = gen_conf["temperature"]
        if "max_tokens" in gen_conf: options["num_predict"] = gen_conf["max_tokens"]
        if "top_p" in gen_conf: options["top_k"] = gen_conf["top_p"]
        if "presence_penalty" in gen_conf: options["presence_penalty"] = gen_conf["presence_penalty"]
        if "frequency_penalty" in gen_conf: options["frequency_penalty"] = gen_conf["frequency_penalty"]
        ans = ""
        try:
            response = self.client.chat(
                model=self.model_name,
                messages=history,
                stream=True,
                options=options,
                keep_alive=-1
            )
            for resp in response:
                if resp["done"]:
                    yield resp.get("prompt_eval_count", 0) + resp.get("eval_count", 0)
                ans += resp["message"]["content"]
                yield ans
        except Exception as e:
            yield ans + "\n**ERROR**: " + str(e)
        yield 0


class MoonshotChat(BaseChatModel):
    """Moonshot chat model"""
    
    def __init__(self, key: str, model_name: str = "moonshot-v1-8k", base_url: str = "https://api.moonshot.cn/v1", **kwargs):
        if not base_url:
            base_url = "https://api.moonshot.cn/v1"
        super().__init__(key, model_name, base_url=base_url, **kwargs)


class DeepSeekChat(BaseChatModel):
    """DeepSeek chat model"""
    
    def __init__(self, key: str, model_name: str = "deepseek-chat", base_url: str = "https://api.deepseek.com/v1", **kwargs):
        if not base_url:
            base_url = "https://api.deepseek.com/v1"
        super().__init__(key, model_name, base_url=base_url, **kwargs)


class XinferenceChat(BaseChatModel):
    """Xinference chat model"""
    
    def __init__(self, key: str = None, model_name: str = "", base_url: str = "", **kwargs):
        if not base_url:
            raise ValueError("Local llm url cannot be None")
        if base_url.split("/")[-1] != "v1":
            base_url = os.path.join(base_url, "v1")
        super().__init__(key, model_name, base_url=base_url, **kwargs)


class HuggingFaceChat(BaseChatModel):
    """HuggingFace chat model"""
    
    def __init__(self, key: str = None, model_name: str = "", base_url: str = "", **kwargs):
        if not base_url:
            raise ValueError("Local llm url cannot be None")
        if base_url.split("/")[-1] != "v1":
            base_url = os.path.join(base_url, "v1")
        super().__init__(key, model_name, base_url=base_url, **kwargs)


class LocalAIChat(BaseChatModel):
    """LocalAI chat model"""
    
    def __init__(self, key: str, model_name: str, base_url: str, **kwargs):
        if not base_url:
            raise ValueError("Local llm url cannot be None")
        if base_url.split("/")[-1] != "v1":
            base_url = os.path.join(base_url, "v1")
        self.key = key
        self.model_name = model_name.split("___")[0]
        self.base_url = base_url
        if OpenAI:
            self.client = OpenAI(api_key="empty", base_url=base_url)


class OpenAI_APIChat(BaseChatModel):
    """OpenAI-API-Compatible chat model for any OpenAI compatible API"""
    
    def __init__(self, key: str, model_name: str, base_url: str, **kwargs):
        if not base_url:
            raise ValueError("OpenAI-API-Compatible requires a base_url")
        if base_url.split("/")[-1] != "v1":
            base_url = os.path.join(base_url, "v1")
        super().__init__(key, model_name, base_url=base_url, **kwargs)


class BaseRerankModel(ABC):
    """Base class for rerank models"""
    
    def __init__(self, key: str, model_name: str, **kwargs):
        self.key = key
        self.model_name = model_name
        self.base_url = kwargs.get('base_url')
    
    def similarity(self, query: str, texts: List[str]) -> List[float]:
        """Calculate similarity scores for reranking"""
        raise NotImplementedError("Please implement similarity method!")


class BaseCvModel(ABC):
    """Base class for computer vision models"""
    
    def __init__(self, key: str, model_name: str, **kwargs):
        self.key = key
        self.model_name = model_name
        self.base_url = kwargs.get('base_url')
    
    def describe(self, image, max_length: int = 300) -> str:
        """Describe image content"""
        raise NotImplementedError("Please implement describe method!")


class BaseSeq2txtModel(ABC):
    """Base class for sequence to text models (ASR)"""
    
    def __init__(self, key: str, model_name: str, **kwargs):
        self.key = key
        self.model_name = model_name
        self.base_url = kwargs.get('base_url')
    
    def transcription(self, audio) -> str:
        """Transcribe audio to text"""
        raise NotImplementedError("Please implement transcription method!")


class BaseTTSModel(ABC):
    """Base class for text-to-speech models"""
    
    def __init__(self, key: str, model_name: str, **kwargs):
        self.key = key
        self.model_name = model_name
        self.base_url = kwargs.get('base_url')
    
    def tts(self, text: str) -> bytes:
        """Convert text to speech"""
        raise NotImplementedError("Please implement tts method!")


# Model registry
ChatModel = {
    "OpenAI": OpenAIChat,
    "Azure-OpenAI": AzureChat,
    "ZHIPU-AI": ZhipuChat,
    "Tongyi-Qianwen": QWenChat,
    "Ollama": OllamaChat,
    "Moonshot": MoonshotChat,
    "DeepSeek": DeepSeekChat,
    "Xinference": XinferenceChat,
    "HuggingFace": HuggingFaceChat,
    "LocalAI": LocalAIChat,
    "OpenAI-API-Compatible": OpenAI_APIChat,
}

EmbeddingModel = {
    "OpenAI": OpenAIEmbedding,
    "Azure-OpenAI": AzureEmbedding,
    "Tongyi-Qianwen": QWenEmbedding,
    "ZHIPU-AI": ZhipuEmbedding,
    "Ollama": OllamaEmbedding,
}

RerankModel = {
    # TODO: Add rerank models
}

CVModel = {
    # TODO: Add CV models
}

Seq2txtModel = {
    # TODO: Add Seq2txt models
}

TTSModel = {
    # TODO: Add TTS models
}