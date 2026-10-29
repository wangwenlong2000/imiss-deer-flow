# -*- coding: utf-8 -*-
# Common utilities for OneCity Agent

import json
import uuid
import base64
import pickle
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Union


from Cryptodome.Cipher import PKCS1_OAEP
from Cryptodome.PublicKey import RSA
from Cryptodome.Hash import SHA256

from onecityagent import config


def get_uuid() -> str:
    """
    生成UUID
    """
    return str(uuid.uuid4())


def get_format_time() -> str:
    """
    获取格式化的当前时间
    """
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def current_timestamp() -> int:
    """
    获取当前时间戳（秒）
    """
    return int(datetime.now().timestamp())


def datetime_format(dt: datetime) -> str:
    """
    格式化日期时间对象
    """
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def delta_seconds(time_str: str) -> int:
    """
    计算给定时间字符串与当前时间的秒数差
    """
    try:
        dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
        return int((datetime.now() - dt).total_seconds())
    except Exception as e:
        logging.error(f"Delta seconds error: {str(e)}")
        return 0


def json_dumps(obj: Any, with_type: bool = False) -> str:
    """
    将对象转换为JSON字符串
    """
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception as e:
        logging.error(f"JSON dumps error: {str(e)}")
        return "{}"


def json_loads(json_str: str, object_hook=None, object_pairs_hook=None) -> Any:
    """
    将JSON字符串转换为对象
    """
    try:
        return json.loads(json_str, object_hook=object_hook, object_pairs_hook=object_pairs_hook)
    except Exception as e:
        logging.error(f"JSON loads error: {str(e)}")
        return {}


def serialize_b64(obj: Any, to_str: bool = False) -> Union[bytes, str]:
    """
    将对象序列化为base64编码
    """
    try:
        serialized = base64.b64encode(pickle.dumps(obj))
        return serialized.decode('utf-8') if to_str else serialized
    except Exception as e:
        logging.error(f"Serialize b64 error: {str(e)}")
        return "" if to_str else b""


def deserialize_b64(b64_str: Union[bytes, str]) -> Any:
    """
    将base64编码反序列化为对象
    """
    try:
        if isinstance(b64_str, str):
            b64_str = b64_str.encode('utf-8')
        return pickle.loads(base64.b64decode(b64_str))
    except Exception as e:
        logging.error(f"Deserialize b64 error: {str(e)}")
        return None


def encrypt(plain_text: str) -> str:
    """
    使用RSA公钥加密文本（RSA-OAEP，SHA-256）
    """
    try:
        with open(config.PRIVATE_KEY_PATH, 'r') as f:
            key_data = f.read()
        # 导入私钥，支持密码保护
        private_key = RSA.import_key(key_data, passphrase=config.PRIVATE_KEY_PASSPHRASE)
        # 从私钥中提取公钥
        public_key = private_key.publickey()
        cipher = PKCS1_OAEP.new(public_key, hashAlgo=SHA256)
        encrypted = cipher.encrypt(plain_text.encode('utf-8'))
        return base64.b64encode(encrypted).decode('utf-8')
    except Exception as e:
        logging.error(f"Encrypt error: {str(e)}")
        return ""


def decrypt(encrypted_text: str) -> str:
    """
    使用RSA私钥解密文本（RSA-OAEP，SHA-256）
    """
    try:
        with open(config.PRIVATE_KEY_PATH, 'r') as f:
            key_data = f.read()
        # 导入私钥，支持密码保护
        private_key = RSA.import_key(key_data, passphrase=config.PRIVATE_KEY_PASSPHRASE)
        cipher = PKCS1_OAEP.new(private_key, hashAlgo=SHA256)
        decrypted = cipher.decrypt(base64.b64decode(encrypted_text))
        return decrypted.decode('utf-8')
    except Exception as e:
        logging.error(f"Decrypt error: {str(e)}")
        return ""


def download_img(url: str) -> str:
    """
    下载图片并返回本地路径
    """
    try:
        import requests
        import os
        from urllib.parse import urlparse
        
        response = requests.get(url, stream=True)
        if response.status_code != 200:
            return ""
        
        # 解析URL获取文件名
        parsed_url = urlparse(url)
        filename = os.path.basename(parsed_url.path)
        if not filename:
            filename = f"{get_uuid()}.jpg"
        
        # 确保目录存在
        os.makedirs(config.UPLOAD_FOLDER, exist_ok=True)
        
        # 保存文件
        file_path = os.path.join(config.UPLOAD_FOLDER, filename)
        with open(file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
        
        return file_path
    except Exception as e:
        logging.error(f"Download image error: {str(e)}")
        return ""


# python
def contains_terminate(content: Any, terms: List[str]) -> bool:
    if not terms:
        return False

    # 字符串：直接检测子串（不区分大小写）
    if isinstance(content, str):
        text = content.lower()
        return any(term.lower() in text for term in terms)

    # 字典：优先检查显式的 terminate 字段，其次递归检查所有值
    if isinstance(content, dict):
        term_val = content.get("terminate")
        if isinstance(term_val, bool) and term_val:
            return True
        if isinstance(term_val, str) and term_val.lower() == "true":
            return True
        # 递归检查字典的所有值
        return any(contains_terminate(v, terms) for v in content.values())

    # 列表/元组/集合：递归检查每一项
    if isinstance(content, (list, tuple, set)):
        return any(contains_terminate(item, terms) for item in content)

    # 其他类型：转为字符串后检查
    try:
        return any(term.lower() in str(content).lower() for term in terms)
    except Exception:
        return False
