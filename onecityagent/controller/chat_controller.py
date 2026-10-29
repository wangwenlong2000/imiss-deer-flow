# -*- coding: utf-8 -*-
# Chat Controller for OneCity Agent

import asyncio
from typing import Dict, Optional
from functools import wraps

from flask import Blueprint, jsonify, request
from flasgger import swag_from

from onecityagent.logger import logger
from onecityagent.service.agent_service.agent import AgentState

# 定义异步路由装饰器
def async_route(route_function):
    @wraps(route_function)
    def wrapper(*args, **kwargs):
        try:
            # 尝试获取现有事件循环
            loop = asyncio.get_event_loop()
        except RuntimeError:
            # 如果没有当前事件循环，创建一个新的
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        return loop.run_until_complete(route_function(*args, **kwargs))
    return wrapper

# 创建Blueprint
chat_controller = Blueprint('chat_controller', __name__, url_prefix='')

# MCPRunner实例，将在应用初始化时设置
runner = None


def set_runner(mcp_runner):
    """设置MCPRunner实例"""
    global runner
    runner = mcp_runner


@chat_controller.route('/health', methods=['GET'])
@swag_from({
    "responses": {
        200: {
            "description": "Health check successful",
            "schema": {
                "type": "object",
                "properties": {
                    "status": {"type": "string"}
                }
            }
        }
    }
})
def health_check():
    """健康检查接口"""
    return jsonify({"status": "ok"})


@chat_controller.route('/chat', methods=['POST'])
@swag_from({
    "parameters": [
        {
            "name": "body",
            "in": "body",
            "required": True,
            "schema": {
                "type": "object",
                "properties": {
                    "prompt": {
                        "type": "string",
                        "description": "用户输入的提示"
                    },
                    "plan_strategy": {
                        "type": "string",
                        "description": "规划策略，可选值：step, outline, plan_kg",
                        "enum": ["step", "outline", "plan_kg"],
                        "default": "step"
                    },
                    "intent": {
                        "type": "boolean",
                        "description": "是否使用意图识别",
                        "default": False
                    },
                    "user_name": {
                        "type": "string",
                        "description": "用户名，用于个性化"
                    },
                    "follow_up_response": {
                        "type": "string",
                        "description": "意图识别的后续响应"
                    },
                    "original_input": {
                        "type": "string",
                        "description": "意图识别的原始输入"
                    }
                },
                "required": ["prompt"]
            }
        }
    ],
    "responses": {
        200: {
            "description": "Successful response",
            "schema": {
                "type": "object",
                "properties": {
                    "result": {"type": "string"},
                    "needs_follow_up": {"type": "boolean"},
                    "follow_up_message": {"type": "string"},
                    "original_input": {"type": "string"}
                }
            }
        },
        400: {
            "description": "Bad request",
            "schema": {
                "type": "object",
                "properties": {
                    "error": {"type": "string"}
                }
            }
        }
    }
})
@async_route
async def process_request():
    """处理Agent请求"""
    try:
        # 检查runner是否已初始化
        if runner is None:
            return jsonify({"error": "Agent未初始化"}), 500
            
        data = request.json
        
        # 验证必要参数
        if not data or 'prompt' not in data:
            logger.warning("Missing required parameter: prompt in chat request")
            return jsonify({"error": "Missing required parameter: prompt"}), 400
            
        plan_strategy = data.get('plan_strategy', 'step')
        intent = data.get('intent', False)
        user_name = data.get('user_name', '')
        prompt = data.get('prompt', '')
        follow_up_response = data.get('follow_up_response', None)
        original_input = data.get('original_input', None)
        
        # 验证plan_strategy参数
        if plan_strategy not in ["step", "outline", "plan_kg"]:
            logger.warning(f"Invalid plan_strategy value: {plan_strategy}")
            return jsonify({"error": "Invalid plan_strategy value"}), 400
            
        # 验证plan_kg策略需要intent为True
        if plan_strategy == "plan_kg" and not intent:
            logger.warning("plan_strategy is 'plan_kg' but intent is False")
            return jsonify({"error": "When plan_strategy is 'plan_kg', intent must be True"}), 400
        
        # 处理意图识别的后续交互
        if follow_up_response and original_input and intent:
            # 构建新的prompt，包含原始输入和后续响应
            combined_prompt = f"{original_input}\n{follow_up_response}"
            prompt = combined_prompt
        
        # 初始化并运行
        await runner.initialize()
        result = await runner.run_single_prompt(plan_strategy, intent, user_name, prompt)
        
        # 检查是否需要后续交互
        if isinstance(result, dict) and result.get("scene") == "需要更多信息":
            return jsonify({
                "result": "需要更多信息",
                "needs_follow_up": True,
                "follow_up_message": result.get("params", "请提供更多信息"),
                "original_input": result.get("original_input", prompt)
            })
        
        # 检查是否是意图识别失败的错误信息
        if isinstance(result, dict) and result.get("scene") == "意图识别失败":
            logger.warning(f"Intent recognition failed: {result.get('message', '意图识别失败')}")
            return jsonify({
                "result": result.get("message", "意图识别失败"),
                "needs_follow_up": False,
                "error": "意图识别失败"
            }), 400
        
        return jsonify({"result": result, "needs_follow_up": False})
    except Exception as e:
        # 确保在异常情况下重置Agent状态
        if runner and hasattr(runner, 'agent'):
            runner.agent.current_step = 0
            runner.agent.state = AgentState.IDLE
            
        logger.error(f"Error processing request: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@chat_controller.route('/shutdown', methods=['POST'])
@swag_from({
    "responses": {
        200: {
            "description": "Shutdown successful",
            "schema": {
                "type": "object",
                "properties": {
                    "status": {"type": "string"}
                }
            }
        }
    }
})
@async_route
async def shutdown():
    """关闭Agent连接"""
    if runner and runner.initialized:
        await runner.cleanup()
    return jsonify({"status": "Agent connection closed"})