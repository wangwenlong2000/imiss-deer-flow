# -*- coding: utf-8 -*-
# LLM Controller for OneCity Agent

import json
import os
from typing import Dict, List, Optional
from flask import Blueprint, request, jsonify
from flask_login import login_required, current_user
from flasgger import swag_from
from onecityagent.logger import logger
from onecityagent.service.llm_service.llm_service import (
    LLMFactoriesService, LLMService, TenantLLMService, LLMBundle
)
from onecityagent.models.llm_models import LLMFactories, LLM, TenantLLM
from onecityagent.service.llm_service.llm_models import EmbeddingModel, ChatModel, RerankModel
from onecityagent.service.user_service import TenantService

# Create blueprint
llm_bp = Blueprint('llm', __name__, url_prefix='/llm')


def get_current_tenant_id():
    """Get current tenant ID from request context"""
    # 如果请求头中没有租户ID，尝试通过当前用户获取
    try:
        if current_user and current_user.is_authenticated:
            tenant_info = TenantService.get_info_by(current_user.id)
            if tenant_info and len(tenant_info) > 0:
                return tenant_info[0]['tenant_id']
    except Exception as e:
        logger.warning(f"Failed to get tenant info for user {current_user.id if current_user else 'unknown'}: {e}")
    
    # 如果都失败了，返回默认值
    return 'default'


def get_current_user_id():
    """Get current user ID from request context"""
    # This should be implemented based on your authentication system
    return request.headers.get('X-User-ID', 'default')


@llm_bp.route('/factories', methods=['GET'])
def get_factories():
    """
    获取所有LLM工厂列表
    ---
    tags:
      - LLM
    summary: 获取LLM工厂列表
    description: 获取所有可用的LLM工厂信息，包括名称、logo、标签和状态
    responses:
      200:
        description: 成功获取工厂列表
        schema:
          type: object
          properties:
            code:
              type: integer
              example: 0
            message:
              type: string
              example: success
            data:
              type: array
              items:
                type: object
                properties:
                  name:
                    type: string
                    description: 工厂名称
                  logo:
                    type: string
                    description: 工厂logo URL
                  tags:
                    type: array
                    items:
                      type: string
                    description: 工厂标签
                  status:
                    type: string
                    description: 工厂状态
      500:
        description: 服务器内部错误
    """
    try:
        factories = LLMFactoriesService.get_all()
        result = []
        for factory in factories:
            factory_data = {
                'name': factory.name,
                'logo': factory.logo,
                'tags': factory.tags.split(',') if factory.tags else [],
                'status': factory.status
            }
            result.append(factory_data)
        
        return jsonify({
            'code': 0,
            'message': 'success',
            'data': result
        })
    except Exception as e:
        logger.error(f"Error in get_llm_factories: {str(e)}", exc_info=True)
        return jsonify({
            'code': 500,
            'message': str(e),
            'data': None
        }), 500


@llm_bp.route('/enable_model', methods=['POST'])
def enable_model():
    """
    启用模型
    ---
    tags:
      - LLM
    summary: 启用模型
    description: 启用指定的模型，同时禁用其他所有模型，确保只有一个模型处于启用状态
    parameters:
      - name: body
        in: body
        required: true
        schema:
          type: object
          required:
            - llm_factory
            - model
          properties:
            llm_factory:
              type: string
              description: LLM工厂名称
            model:
              type: string
              description: 模型名称
    responses:
      200:
        description: 成功启用模型
        schema:
          type: object
          properties:
            code:
              type: integer
              example: 0
            message:
              type: string
              example: success
            data:
              type: boolean
              example: true
      400:
        description: 请求参数错误
      404:
        description: 模型不存在
      500:
        description: 服务器内部错误
    """
    try:
        req = request.json
        if not req:
            logger.warning("Request body is required in enable_model")
            return jsonify({
                'code': 400,
                'message': 'Request body is required',
                'data': None
            }), 400

        # 验证必需参数
        required_fields = ['llm_factory', 'model']
        for field in required_fields:
            if field not in req:
                logger.warning(f"Missing required field in enable_model: {field}")
                return jsonify({
                    'code': 400,
                    'message': f'Missing required field: {field}',
                    'data': None
                }), 400


        factory = req['llm_factory']
        model = req['model']

        # 检查目标模型是否存在
        target_model = TenantLLMService.get_factory_and_name(
            factory, model
        )
        
        if not target_model:
            logger.warning(f"Model not found in enable_model: {factory}/{model}")
            return jsonify({
                'code': 404,
                'message': 'Model not found',
                'data': None
            }), 404

        # 先将该租户的所有模型状态设置为0（禁用）
        TenantLLMService.disable_all_models_for_tenant()
        
        # 然后将目标模型状态设置为1（启用）
        TenantLLMService.update(target_model.llm_id, status='1')
        
        logger.info(f"Model enabled successfully: {factory}/{model}")
        return jsonify({
            'code': 0,
            'message': 'success',
            'data': True
        })
        
    except Exception as e:
        logger.error(f"Error in enable_model: {str(e)}", exc_info=True)
        return jsonify({
            'code': 500,
            'message': str(e),
            'data': None
        }), 500


@llm_bp.route('/delete_llm', methods=['POST'])
def delete_llm_specific():
    """删除指定的LLM模型"""
    try:
        data = request.get_json()
        if not data or 'llm_factory' not in data or 'model' not in data:
            return jsonify({
                'code': 400,
                'message': 'llm_factory and model are required'
            })
        
        #tenant_id = get_current_tenant_id()
        llm_factory = data['llm_factory']
        model = data['model']
        
        # 删除指定的LLM配置
        TenantLLMService.delete_by_tenant_and_factory_and_name(
            llm_factory=llm_factory,
            model=model
        )
        
        return jsonify({
            'code': 0,
            'message': 'LLM deleted successfully',
            'data': True
        })
    except Exception as e:
        return jsonify({
            'code': 500,
            'message': str(e)
        })


@llm_bp.route('/delete_factory', methods=['POST'])
def delete_factory():
    """删除指定工厂的所有LLM模型"""
    try:
        data = request.get_json()
        if not data or 'llm_factory' not in data:
            return jsonify({
                'code': 400,
                'message': 'llm_factory is required'
            })
        
        tenant_id = get_current_tenant_id()
        llm_factory = data['llm_factory']
        
        # 删除指定工厂的所有LLM配置
        TenantLLMService.delete_by_tenant_and_factory(
            tenant_id=tenant_id,
            llm_factory=llm_factory
        )
        
        return jsonify({
            'code': 0,
            'message': 'Factory deleted successfully',
            'data': True
        })
    except Exception as e:
        return jsonify({
            'code': 500,
            'message': str(e)
        })


@llm_bp.route('/models', methods=['GET'])
def get_models():
    """
    获取所有LLM模型列表
    ---
    tags:
      - LLM
    summary: 获取LLM模型列表
    description: 获取所有可用的LLM模型信息，支持按工厂和类型筛选
    parameters:
      - name: factory
        in: query
        type: string
        description: 工厂名称筛选
      - name: type
        in: query
        type: string
        description: 模型类型筛选（如：chat, embedding）
    responses:
      200:
        description: 成功获取模型列表
        schema:
          type: object
          properties:
            code:
              type: integer
              example: 0
            message:
              type: string
              example: success
            data:
              type: array
              items:
                type: object
                properties:
                  model:
                    type: string
                    description: 模型名称
                  model_type:
                    type: string
                    description: 模型类型
                  factory:
                    type: string
                    description: 所属工厂
                  max_tokens:
                    type: integer
                    description: 最大token数
                  tags:
                    type: array
                    items:
                      type: string
                    description: 模型标签
                  status:
                    type: string
                    description: 模型状态
      500:
        description: 服务器内部错误
    """
    try:
        factory_name = request.args.get('factory')
        model_type = request.args.get('type')
        
        if factory_name:
            models = LLMService.get_by_factory(factory_name)
        elif model_type:
            models = LLMService.get_by_type(model_type)
        else:
            models = LLMService.get_all()
        
        result = []
        for model in models:
            model_data = {
                'model': model.llm_name,
                'model_type': model.model_type,
                'factory': model.fid,
                'max_tokens': model.max_tokens,
                'tags': model.tags.split(',') if model.tags else [],
                'status': model.status
            }
            result.append(model_data)
        
        return jsonify({
            'code': 0,
            'message': 'success',
            'data': result
        })
    except Exception as e:
        logger.error(f"Error in get_models: {str(e)}", exc_info=True)
        return jsonify({
            'code': 500,
            'message': str(e),
            'data': None
        }), 500


@llm_bp.route('/list', methods=['GET'])
def list_llms():
    """
    获取所有可用的LLM列表
    ---
    tags:
      - LLM
    summary: 获取LLM列表
    description: 获取所有可用的LLM模型列表，按工厂分组
    parameters:
      - name: model_type
        in: query
        type: string
        description: 模型类型筛选（如：chat, embedding）
    responses:
      200:
        description: 成功获取LLM列表
        schema:
          type: object
          properties:
            code:
              type: integer
              example: 0
            message:
              type: string
              example: success
            data:
              type: object
              description: 按工厂分组的LLM列表
      500:
        description: 服务器内部错误
    """
    try:
        model_type = request.args.get('model_type')
        
        # 获取所有可用的LLM模型
        all_models = LLMService.get_all()
        llms = []
        for model in all_models:
            if hasattr(model, 'status') and model.status != 'valid':
                continue
            model_dict = {
                'model': model.llm_name,
                'model_type': model.model_type,
                'fid': model.fid,
                'max_tokens': model.max_tokens,
                'tags': model.tags.split(',') if model.tags else [],
                'status': model.status if hasattr(model, 'status') else None,
                'available': True  # 所有模型都显示为可用
            }
            llms.append(model_dict)
        
        # 获取所有已配置的LLM（不限制租户）
        all_tenant_llms = TenantLLMService.get_all_llms()
        llm_set = set([f"{m['model']}@{m['fid']}" for m in llms])
        for tenant_llm in all_tenant_llms:
            if f"{tenant_llm['model']}@{tenant_llm['llm_factory']}" in llm_set:
                continue
                
            llms.append({
                'model': tenant_llm['model'],
                'model_type': tenant_llm['model_type'],
                'fid': tenant_llm['llm_factory'],
                'available': True,
                'max_tokens': 0,
                'tags': [],
                'status': tenant_llm.get('status')
            })
        
        # 按工厂分组并筛选
        result = {}
        for m in llms:
            if model_type and m['model_type'].find(model_type) < 0:
                continue
            if m['fid'] not in result:
                result[m['fid']] = []
            result[m['fid']].append(m)
        
        return jsonify({
            'code': 0,
            'message': 'success',
            'data': result
        })
    except Exception as e:
        logger.error(f"Error in list_llms: {str(e)}", exc_info=True)
        return jsonify({
            'code': 500,
            'message': str(e),
            'data': None
        }), 500


@llm_bp.route('/add_llm', methods=['POST'])
def add_llm():
    """
    添加LLM配置
    ---
    tags:
      - LLM
    summary: 添加LLM配置
    description: 为当前租户添加新的LLM配置
    parameters:
      - name: body
        in: body
        required: true
        schema:
          type: object
          required:
            - llm_factory
            - llm_name
            - model_type
          properties:
            llm_factory:
              type: string
              description: LLM工厂名称
            model:
              type: string
              description: LLM模型名称
            model_type:
              type: string
              description: 模型类型
            api_key:
              type: string
              description: API密钥
            api_base:
              type: string
              description: API基础URL
            max_tokens:
              type: integer
              description: 最大token数
    responses:
      200:
        description: 成功添加LLM配置
        schema:
          type: object
          properties:
            code:
              type: integer
              example: 0
            message:
              type: string
              example: success
            data:
              type: boolean
              example: true
      400:
        description: 请求参数错误
      500:
        description: 服务器内部错误
    """
    try:
        req = request.json
        if not req:
            logger.warning("Request body is required in add_llm")
            return jsonify({
                'code': 400,
                'message': 'Request body is required',
                'data': None
            }), 400

        # 验证必需参数
        required_fields = ['llm_factory', 'model', 'model_type']
        for field in required_fields:
            if field not in req:
                logger.warning(f"Missing required field in add_llm: {field}")
                return jsonify({
                    'code': 400,
                    'message': f'Missing required field: {field}',
                    'data': None
                }), 400

        tenant_id = get_current_tenant_id()
        factory = req['llm_factory']
        model = req['model']
        model_type = req['model_type']
        api_key = req.get('api_key', '')
        base_url = req.get('base_url', '')
        max_tokens = req.get('max_tokens', 0)

        # 处理特殊工厂的API密钥格式
        def apikey_json(keys):
            return json.dumps({k: req.get(k, "") for k in keys})

        if factory == "VolcEngine":
            # For VolcEngine, due to its special authentication method
            # Assemble ark_api_key endpoint_id into api_key
            model = req["model"]
            api_key = apikey_json(["ark_api_key", "endpoint_id"])

        elif factory == "Tencent Hunyuan":
            api_key = apikey_json(["hunyuan_sid", "hunyuan_sk"])

        elif factory == "Tencent Cloud":
            api_key = apikey_json(["tencent_cloud_sid", "tencent_cloud_sk"])

        elif factory == "Bedrock":
            # For Bedrock, due to its special authentication method
            # Assemble bedrock_ak, bedrock_sk, bedrock_region
            model = req["model"]
            api_key = apikey_json(["bedrock_ak", "bedrock_sk", "bedrock_region"])

        elif factory == "LocalAI":
            model = req["model"] + "___LocalAI"
            api_key = "xxxxxxxxxxxxxxx"

        elif factory == "HuggingFace":
            model = req["model"] + "___HuggingFace"
            api_key = "xxxxxxxxxxxxxxx"

        elif factory == "OpenAI-API-Compatible":
            model = req["model"]
            api_key = req.get("api_key", "xxxxxxxxxxxxxxx")

        elif factory == "XunFei Spark":
            model = req["model"]
            if model_type == "chat":
                api_key = req.get("spark_api_password", "xxxxxxxxxxxxxxx")
            elif model_type == "tts":
                api_key = apikey_json(["spark_app_id", "spark_api_secret", "spark_api_key"])
            else:
                api_key = req.get("api_key", "xxxxxxxxxxxxxxx")

        elif factory == "BaiduYiyan":
            model = req["model"]
            api_key = apikey_json(["yiyan_ak", "yiyan_sk"])

        elif factory == "Fish Audio":
            model = req["model"]
            api_key = apikey_json(["fish_audio_ak", "fish_audio_refid"])

        elif factory == "Google Cloud":
            model = req["model"]
            api_key = apikey_json(["google_project_id", "google_region", "google_service_account_key"])

        elif factory == "Azure-OpenAI":
            model = req["model"]
            api_key = apikey_json(["api_key", "api_version"])

        elif factory in ["Ollama", "Xinference"]:
            api_key = "xxxxxxxxxxxxxxx"  # 自部署模型不需要真实API密钥

        else:
            model = req["model"]
            api_key = req.get("api_key", "xxxxxxxxxxxxxxx")

        # 测试API密钥是否有效
        msg = ""
        if api_key and api_key != "xxxxxxxxxxxxxxx":
            try:
                if model_type == "embedding":
                    model_instance = EmbeddingModel[factory](
                        key=api_key,
                        model_name=model,
                        base_url=base_url
                    )
                    arr, tc = model_instance.encode(["Test if the api key is available"])
                    if len(arr[0]) == 0 or tc == 0:
                        raise Exception("Embedding test failed")
                elif model_type == "chat":
                    model_instance = ChatModel[factory](
                        key=api_key,
                        model_name=model,
                        base_url=base_url
                    )
                    response, tc = model_instance.chat(
                        None,
                        [{"role": "user", "content": "Hello! How are you doing!"}],
                        {"temperature": 0.9, "max_tokens": 50}
                    )
                    if not tc or response.find("**ERROR**") >= 0:
                        raise Exception(response if response else "Chat test failed")
                elif model_type == "rerank":
                    model_instance = RerankModel[factory](
                        key=api_key,
                        model_name=model,
                        base_url=base_url
                    )
                    arr, tc = model_instance.similarity("What's the weather?", ["Is it sunny today?"])
                    if len(arr) == 0 or tc == 0:
                        raise Exception("Rerank test failed")
            except Exception as e:
                import traceback
                logger.error(f"Model test failed at line {traceback.extract_tb(e.__traceback__)[-1].lineno}: {str(e)}")
                logger.error(f"Full traceback: {traceback.format_exc()}")
                msg = f"Failed to access model ({model}): {str(e)}"

        if msg:
            logger.warning(f"API key validation failed in add_llm: {msg}")
            return jsonify({
                'code': 400,
                'message': msg,
                'data': None
            }), 400

        # 保存或更新LLM配置
        llm_config = {
            'tenant_id': tenant_id,
            'llm_factory': factory,
            'model_type': model_type,
            'model': model,
            'base_url': base_url,
            'api_key': api_key,
            'max_tokens': max_tokens,
            'status': '0'  # 新增时默认为不启用
        }

        # 检查是否已存在相同配置
        existing = TenantLLMService.get_by_tenant_and_factory_and_name(
            tenant_id, factory, model
        )

        if existing:
            # 更新现有配置
            TenantLLMService.update(existing.llm_id, **llm_config)
        else:
            # 创建新配置
            TenantLLMService.create(**llm_config)

        return jsonify({
            'code': 0,
            'message': 'success',
            'data': True
        })
    except Exception as e:
        logger.error(f"Error in add_llm: {str(e)}", exc_info=True)
        return jsonify({
            'code': 500,
            'message': str(e),
            'data': None
        }), 500


@llm_bp.route('/my_llms', methods=['GET'])
def get_my_llms():
    """
    获取所有可用的LLM配置列表
    ---
    tags:
      - LLM
    summary: 获取所有LLM配置
    description: 获取所有已配置的LLM列表，不再限制租户
    parameters:
      - name: type
        in: query
        type: string
        description: 模型类型筛选
    responses:
      200:
        description: 成功获取LLM配置列表
        schema:
          type: object
          properties:
            code:
              type: integer
              example: 0
            message:
              type: string
              example: success
            data:
              type: array
              description: LLM配置列表
      500:
        description: 服务器内部错误
    """
    try:
        model_type = request.args.get('type')
        
        llm_list = TenantLLMService.get_all_llms(model_type)
        res = {}
        for o in llm_list:
            if o["llm_factory"] not in res:
                res[o["llm_factory"]] = {
                    "tags": o["tags"],
                    "llm": []
                }
            res[o["llm_factory"]]["llm"].append({
                "type": o["model_type"],
                "name": o["model"],
                "used_token": o["used_tokens"],
                "status": o.get("status")
            })
        return jsonify({
            'code': 0,
            'message': 'success',
            'data': res
        })
    except Exception as e:
        logger.error(f"Error in get_my_llms: {str(e)}", exc_info=True)
        return jsonify({
            'code': 500,
            'message': str(e),
            'data': None
        }), 500


@llm_bp.route('/set-api-key', methods=['POST'])
def set_api_key():
    """
    设置LLM API密钥
    ---
    tags:
      - LLM
    summary: 设置API密钥
    description: 为指定的LLM模型设置API密钥和配置
    parameters:
      - name: X-Tenant-ID
        in: header
        type: string
        description: 租户ID
      - in: body
        name: body
        description: API密钥配置信息
        required: true
        schema:
          type: object
          required:
            - llm_factory
            - llm_name
            - api_key
          properties:
            llm_factory:
              type: string
              description: LLM工厂名称
            model:
              type: string
              description: LLM模型名称
            api_key:
              type: string
              description: API密钥
            base_url:
              type: string
              description: API基础URL（可选）
    responses:
      200:
        description: API密钥设置成功
        schema:
          type: object
          properties:
            code:
              type: integer
              example: 0
            message:
              type: string
              example: API key set successfully
            data:
              type: object
              properties:
                tenant_id:
                  type: string
                llm_factory:
                  type: string
                llm_name:
                  type: string
                model_type:
                  type: string
      400:
        description: 缺少必需参数
      404:
        description: LLM模型未找到
      500:
        description: 服务器内部错误
    """
    try:
        tenant_id = get_current_tenant_id()
        data = request.get_json()
        
        llm_factory = data.get('llm_factory')
        model = data.get('model')
        api_key = data.get('api_key')
        base_url = data.get('base_url')
        
        if not all([llm_factory, model, api_key]):
            logger.warning("Missing required parameters in set_api_key: llm_factory, model, or api_key")
            return jsonify({
                'code': 400,
                'message': 'Missing required parameters',
                'data': None
            }), 400
        
        # 验证LLM模型是否存在
        llm_model = LLMService.get_by_name_and_factory(model, llm_factory)
        if not llm_model:
            return jsonify({
                'code': 404,
                'message': 'LLM model not found',
                'data': None
            }), 404
        
        # 保存或更新配置
        config_data = {
            'model_type': llm_model.model_type,
            'api_key': api_key,
            'base_url': base_url,
            'max_tokens': llm_model.max_tokens,
            'used_tokens': 0
        }
        
        tenant_llm, created = TenantLLMService.save_or_update(
            tenant_id, llm_factory, model, **config_data
        )
        
        return jsonify({
            'code': 0,
            'message': 'API key set successfully',
            'data': {
                'llm_id': tenant_llm.llm_id,
                'tenant_id': tenant_id,
                'llm_factory': llm_factory,
                'model': model,
                'model_type': tenant_llm.model_type,
                'created': created
            }
        })
    except Exception as e:
        logger.error(f"Error in set_api_key: {str(e)}", exc_info=True)
        return jsonify({
            'code': 500,
            'message': str(e),
            'data': None
        }), 500


@llm_bp.route('/test-connection', methods=['POST'])
def test_connection():
    """
    测试LLM连接
    ---
    tags:
      - LLM
    summary: 测试LLM连接
    description: 测试指定LLM模型的连接是否正常
    parameters:
      - name: X-Tenant-ID
        in: header
        type: string
        description: 租户ID
      - in: body
        name: body
        description: 连接测试参数
        required: true
        schema:
          type: object
          required:
            - llm_factory
            - llm_name
          properties:
            llm_factory:
              type: string
              description: LLM工厂名称
            llm_name:
              type: string
              description: LLM模型名称
    responses:
      200:
        description: 连接测试成功
        schema:
          type: object
          properties:
            code:
              type: integer
              example: 0
            message:
              type: string
              example: Connection test successful
            data:
              type: object
              description: 测试结果
      400:
        description: 缺少必需参数
      404:
        description: LLM配置未找到
      500:
        description: 连接测试失败或服务器内部错误
    """
    try:
        tenant_id = get_current_tenant_id()
        data = request.get_json()
        
        llm_factory = data.get('llm_factory')
        model = data.get('model')
        
        if not all([llm_factory, model]):
            logger.warning("Missing required parameters in test_connection: llm_factory or model")
            return jsonify({
                'code': 400,
                'message': 'Missing required parameters',
                'data': None
            }), 400
        
        # 获取租户LLM配置
        tenant_llm = TenantLLM.get_or_none(
            (TenantLLM.tenant_id == tenant_id) &
            (TenantLLM.llm_factory == llm_factory) &
            (TenantLLM.model == model)
        )
        
        if not tenant_llm:
            return jsonify({
                'code': 404,
                'message': 'LLM configuration not found',
                'data': None
            }), 404
        
        # 根据模型类型进行测试
        try:
            if tenant_llm.model_type == 'embedding':
                model_instance = TenantLLMService.model_instance(
                    tenant_id, llm_factory, model, 'EMBEDDING'
                )
                # 测试编码一个简单文本
                result, tokens = model_instance.encode(['test'])
                test_result = {'status': 'success', 'tokens_used': tokens}
            
            elif tenant_llm.model_type == 'chat':
                model_instance = TenantLLMService.model_instance(
                    tenant_id, llm_factory, model, 'CHAT'
                )
                # 测试简单对话
                result = model_instance.chat('You are a helpful assistant.', [['Hello', 'Hi there!']])
                test_result = {'status': 'success', 'response': result[:100] if result else 'No response'}
            
            else:
                test_result = {'status': 'success', 'message': 'Model type not testable'}
            
            return jsonify({
                'code': 0,
                'message': 'Connection test successful',
                'data': test_result
            })
        
        except Exception as test_error:
            return jsonify({
                'code': 500,
                'message': f'Connection test failed: {str(test_error)}',
                'data': None
            }), 500
    
    except Exception as e:
        logger.error(f"Error in test_connection: {str(e)}", exc_info=True)
        return jsonify({
            'code': 500,
            'message': str(e),
            'data': None
        }), 500


@llm_bp.route('/delete', methods=['DELETE'])
def delete_llm():
    """
    删除LLM配置
    ---
    tags:
      - LLM
    summary: 删除LLM配置
    description: 删除指定的LLM配置
    parameters:
      - name: X-Tenant-ID
        in: header
        type: string
        description: 租户ID
      - in: body
        name: body
        description: 删除参数
        required: true
        schema:
          type: object
          required:
            - llm_factory
          properties:
            llm_factory:
              type: string
              description: LLM工厂名称
            model:
              type: string
              description: LLM模型名称（可选，不指定则删除该工厂下所有配置）
    responses:
      200:
        description: 删除成功
        schema:
          type: object
          properties:
            code:
              type: integer
              example: 0
            message:
              type: string
              example: Deleted 1 LLM configuration(s)
            data:
              type: object
              properties:
                deleted_count:
                  type: integer
                  description: 删除的配置数量
      400:
        description: 缺少必需参数
      500:
        description: 服务器内部错误
    """
    try:
        tenant_id = get_current_tenant_id()
        data = request.get_json()
        
        llm_factory = data.get('llm_factory')
        model = data.get('model')
        
        if not llm_factory:
            logger.warning("Missing llm_factory parameter in delete_llm")
            return jsonify({
                'code': 400,
                'message': 'Missing llm_factory parameter',
                'data': None
            }), 400
        
        deleted_count = TenantLLMService.delete(tenant_id, llm_factory, model)
        
        return jsonify({
            'code': 0,
            'message': f'Deleted {deleted_count} LLM configuration(s)',
            'data': {'deleted_count': deleted_count}
        })
    except Exception as e:
        logger.error(f"Error in delete_llm: {str(e)}", exc_info=True)
        return jsonify({
            'code': 500,
            'message': str(e),
            'data': None
        }), 500


@llm_bp.route('/usage', methods=['GET'])
def get_usage():
    """
    获取LLM使用统计
    ---
    tags:
      - LLM
    summary: 获取LLM使用统计
    description: 获取当前租户的LLM使用情况统计
    parameters:
      - name: X-Tenant-ID
        in: header
        type: string
        description: 租户ID
    responses:
      200:
        description: 成功获取使用统计
        schema:
          type: object
          properties:
            code:
              type: integer
              example: 0
            message:
              type: string
              example: success
            data:
              type: object
              properties:
                total_max_tokens:
                  type: integer
                  description: 总最大token数
                total_used_tokens:
                  type: integer
                  description: 总已使用token数
                models:
                  type: array
                  items:
                    type: object
                    properties:
                      factory:
                        type: string
                        description: 工厂名称
                      name:
                        type: string
                        description: 模型名称
                      type:
                        type: string
                        description: 模型类型
                      max_tokens:
                        type: integer
                        description: 最大token数
                      used_tokens:
                        type: integer
                        description: 已使用token数
                      usage_percentage:
                        type: number
                        description: 使用百分比
      500:
        description: 服务器内部错误
    """
    try:
        tenant_id = get_current_tenant_id()
        
        llms = TenantLLMService.get_my_llms(tenant_id)
        
        total_usage = {
            'total_max_tokens': 0,
            'total_used_tokens': 0,
            'models': []
        }
        
        for llm in llms:
            total_usage['total_max_tokens'] += llm['max_tokens']
            total_usage['total_used_tokens'] += llm['used_tokens']
            total_usage['models'].append({
                'factory': llm['llm_factory'],
                'name': llm['model'],
                'type': llm['model_type'],
                'max_tokens': llm['max_tokens'],
                'used_tokens': llm['used_tokens'],
                'usage_percentage': (llm['used_tokens'] / llm['max_tokens'] * 100) if llm['max_tokens'] > 0 else 0,
                'status': llm.get('status')
            })
        
        return jsonify({
            'code': 0,
            'message': 'success',
            'data': total_usage
        })
    except Exception as e:
        logger.error(f"Error in get_usage: {str(e)}", exc_info=True)
        return jsonify({
            'code': 500,
            'message': str(e),
            'data': None
        }), 500


@llm_bp.route('/init-factories', methods=['POST'])
def init_factories():
    """
    初始化LLM工厂数据
    ---
    tags:
      - LLM
    summary: 初始化LLM工厂数据
    description: 从配置文件初始化LLM工厂和模型数据
    responses:
      200:
        description: 初始化成功
        schema:
          type: object
          properties:
            code:
              type: integer
              example: 0
            message:
              type: string
              example: Factories initialized successfully
            data:
              type: object
              properties:
                created_factories:
                  type: integer
                  description: 创建的工厂数量
                created_models:
                  type: integer
                  description: 创建的模型数量
      404:
        description: 配置文件未找到
      500:
        description: 服务器内部错误
    """
    try:
        # 读取工厂配置文件
        config_file = os.path.join(
            os.path.dirname(__file__),
            '../service/llm_service/llm_factories.json'
        )
        
        if not os.path.exists(config_file):
            return jsonify({
                'code': 404,
                'message': 'Factory configuration file not found',
                'data': None
            }), 404
        
        with open(config_file, 'r', encoding='utf-8') as f:
            factories_data = json.load(f)
        
        created_factories = 0
        created_models = 0
        
        for factory_data in factories_data:
            # 创建或更新工厂
            factory, created = LLMFactories.get_or_create(
                name=factory_data['name'],
                defaults={
                    'logo': factory_data.get('logo', ''),
                    'tags': factory_data.get('tags', ''),
                    'status': factory_data.get('status', '1')
                }
            )
            if created:
                created_factories += 1
            
            # 创建或更新模型
            for llm_data in factory_data.get('llm', []):
                model, created = LLM.get_or_create(
                    fid=factory_data['name'],
                    llm_name=llm_data['llm_name'],
                    defaults={
                        'model_type': llm_data.get('model_type', ''),
                        'max_tokens': llm_data.get('max_tokens', 0),
                        'tags': llm_data.get('tags', ''),
                        'status': '1'
                    }
                )
                if created:
                    created_models += 1
        
        return jsonify({
            'code': 0,
            'message': 'Factories initialized successfully',
            'data': {
                'created_factories': created_factories,
                'created_models': created_models
            }
        })
    except Exception as e:
        logger.error(f"Error in init_factories: {str(e)}", exc_info=True)
        return jsonify({
            'code': 500,
            'message': str(e),
            'data': None
        }), 500