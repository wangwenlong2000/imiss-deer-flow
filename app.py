# -*- coding: utf-8 -*-
# Main application file for OneCity Agent

import asyncio
import json
import os
import secrets
import sys
from functools import wraps

from flask import Flask, jsonify, request
from flasgger import Swagger
from flask_cors import CORS
from flask_login import LoginManager
from playhouse.flask_utils import FlaskDB
from onecityagent.models.db_models import DB

from onecityagent.logger import logger
from onecityagent.service.agent_service.agent import Agent, AgentState
from onecityagent.controller.chat_controller import chat_controller, set_runner
from onecityagent.controller.mcp_controller import mcp_controller
from onecityagent.controller.config_controller import config_controller
from onecityagent.controller.user_controller import user_bp, tenant_bp
from onecityagent.controller.llm_controller import llm_bp
from onecityagent.controller.role_controller import role_bp
from onecityagent.controller.conversation_controller import conversation_bp
from onecityagent.controller.dialog_controller import dialog_bp
from onecityagent.controller.plan_controller import plan_bp
from onecityagent.models import User, create_tables

# 创建Flask应用
app = Flask(__name__)
db = DB.get_instance() 
FlaskDB(app, db)

app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY') or secrets.token_urlsafe(32)
app.config['JSON_AS_ASCII'] = False

# 配置Swagger
swagger_config = {
    "title": "OneCity Agent API",
    "description": "OneCity Agent API Documentation",
    "version": "1.0.0",
    "termsOfService": "",
    "hide_top_bar": True,
    "specs": [
        {
            "endpoint": 'apispec',
            "route": '/apispec.json',
            "rule_filter": lambda rule: True,  # all in
            "model_filter": lambda tag: True,  # all in
        }
    ],
    "static_url_path": "/flasgger_static",
    "swagger_ui": True,
    "specs_route": "/swagger/",
    "headers": []  # 添加空的headers配置，避免TypeError
}
swagger = Swagger(app, config=swagger_config)

# 配置CORS
CORS(app, supports_credentials=True)

# 配置登录管理器
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'user.login'

@login_manager.user_loader
def load_user(user_id):
    return User.get_or_none(User.access_token == user_id)

# 注册蓝图，添加v1前缀
app.register_blueprint(config_controller, url_prefix='/v1/config')
app.register_blueprint(chat_controller, url_prefix='/v1')
app.register_blueprint(mcp_controller, url_prefix='/v1/mcp')
app.register_blueprint(user_bp, url_prefix='/v1/user')
app.register_blueprint(tenant_bp, url_prefix='/v1')
app.register_blueprint(llm_bp, url_prefix='/v1/llm')
app.register_blueprint(role_bp, url_prefix='/v1/role')
app.register_blueprint(conversation_bp, url_prefix='/v1/conversation')
app.register_blueprint(dialog_bp, url_prefix='/v1/dialog')
app.register_blueprint(plan_bp, url_prefix='/v1/plan')

# 创建数据库表
# create_tables()

# 异步路由装饰器
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


# MCPRunner类，用于管理Agent实例
class MCPRunner:
    def __init__(self):
        self.agent = Agent()
        self.initialized = False

    async def initialize(self):
        """初始化Agent连接"""
        if not self.initialized:
            await self.agent.initialize()
            self.initialized = True

    async def run_single_prompt(self, plan_strategy, intent, user_name, prompt):
        """运行单个prompt"""
        try:
            return await self.agent.run(
                strategy=plan_strategy,
                intent=intent,
                user_name=user_name,
                prompt=prompt
            )
        except Exception as e:
            # 确保在异常情况下重置Agent状态
            self.agent.current_step = 0
            self.agent.state = AgentState.IDLE
            logger.error(f"Error in run_single_prompt: {str(e)}", exc_info=True)
            raise

    async def cleanup(self):
        """清理资源"""
        await self.agent.cleanup()
        self.initialized = False
        
    async def reload_mcp_config(self, server_id=None) -> None:
        """重新加载MCP配置并重新连接MCP服务器
        
        Args:
            server_id: 要重新加载的服务器ID，如果为None则重新加载所有服务器
        """
        if not self.initialized:
            logger.info("Agent未初始化，正在初始化Agent...")
            try:
                await self.initialize()
                logger.info("Agent初始化成功")
            except Exception as e:
                logger.error(f"Agent初始化失败: {e}")
                return
            
        try:
            # 重新加载MCPSettings中的服务器配置
            from onecityagent.config import MCPSettings, IntentConfig, config
            from onecityagent.service.intent_service.intention import Intent

            # 重新加载服务器配置并更新config中的MCP配置
            updated_servers = MCPSettings.load_server_config()
            config.mcp_config.servers = updated_servers
            logger.info("MCP服务器配置已更新到config中")

            # 重新加载意图场景配置并更新Intent对象
            self.agent.intent = Intent(IntentConfig.load_all_scene_configs(), self.agent.llm)
            logger.info("意图场景配置重新加载完成")

            if server_id:
                # 只重新加载指定的服务器
                if server_id in updated_servers:
                    logger.info(f"正在重新加载MCP服务器 {server_id} 的配置")
                    # 断开指定服务器的连接
                    try:
                        await self.agent.mcp_clients.disconnect(server_id)
                    except Exception as e:
                        logger.warning(f"断开MCP服务器 {server_id} 连接时出错: {e}")
                        # 继续执行，不中断重新加载过程
                    
                    # 重新连接指定的服务器
                    server_config = updated_servers[server_id]
                    if server_config.type == "sse" and server_config.url:
                        try:
                            await self.agent.connect_mcp_server(server_config.url, server_id)
                            logger.info(f"已重新连接到MCP服务器 {server_id} 在 {server_config.url}")
                        except ConnectionError as e:
                            logger.error(f"连接到MCP服务器 {server_id} 失败: {e}")
                            # 继续执行，不中断整个重新加载过程
                    elif server_config.type == "stdio" and server_config.command:
                        await self.agent.connect_mcp_server(
                            server_config.command,
                            server_id,
                            use_stdio=True,
                            stdio_args=server_config.args
                        )
                        logger.info(f"已重新连接到MCP服务器 {server_id} 使用命令 {server_config.command}")
                else:
                    logger.warning(f"找不到MCP服务器 {server_id} 的配置")
            else:
                # 重新加载所有服务器
                logger.info("正在重新加载所有MCP服务器配置")
                # 断开所有服务器的连接
                try:
                    await self.agent.mcp_clients.disconnect()
                except Exception as e:
                    logger.warning(f"断开所有MCP服务器连接时出错: {e}")
                    # 继续执行，不中断重新加载过程
                
                # 重新连接所有服务器
                for srv_id, srv_config in updated_servers.items():
                    try:
                        if srv_config.type == "sse" and srv_config.url:
                            try:
                                await self.agent.connect_mcp_server(srv_config.url, srv_id)
                                logger.info(f"已重新连接到MCP服务器 {srv_id} 在 {srv_config.url}")
                            except ConnectionError as e:
                                logger.error(f"连接到MCP服务器 {srv_id} 失败: {e}")
                                # 继续执行，不中断整个重新加载过程
                        elif srv_config.type == "stdio" and srv_config.command:
                            await self.agent.connect_mcp_server(
                                srv_config.command,
                                srv_id,
                                use_stdio=True,
                                stdio_args=srv_config.args
                            )
                            logger.info(f"已重新连接到MCP服务器 {srv_id} 使用命令 {srv_config.command}")
                    except Exception as e:
                        logger.error(f"重新连接到MCP服务器 {srv_id} 失败: {e}")
            
            # 更新工具列表
            #await self.agent._refresh_tools()
            logger.info("MCP配置重新加载完成")
        except Exception as e:
            logger.error(f"重新加载MCP配置时出错: {e}")


# 创建MCPRunner实例
runner = MCPRunner()

# 设置runner到chat_controller
set_runner(runner)


# 请求日志中间件
@app.before_request
def log_request_info():
    logger.info(f"Request: {request.method} {request.url} - IP: {request.remote_addr}")
    if request.is_json and request.get_json():
        # 记录请求体，但要小心敏感信息
        request_data = request.get_json()
        # 过滤敏感字段
        filtered_data = {k: v if k not in ['password', 'api_key', 'secret'] else '***' for k, v in request_data.items()}
        logger.debug(f"Request body: {filtered_data}")


@app.after_request
def log_response_info(response):
    logger.info(f"Response: {response.status_code} - {request.method} {request.url}")
    return response


# 全局错误处理器
@app.errorhandler(404)
def not_found(error):
    logger.warning(f"404 Not Found: {request.url}")
    return jsonify({
        'code': 404,
        'message': 'Resource not found',
        'data': None
    }), 404


@app.errorhandler(500)
def internal_error(error):
    logger.error(f"500 Internal Server Error: {str(error)}", exc_info=True)
    return jsonify({
        'code': 500,
        'message': 'Internal server error',
        'data': None
    }), 500


@app.errorhandler(Exception)
def handle_exception(error):
    logger.error(f"Unhandled exception: {str(error)}", exc_info=True)
    return jsonify({
        'code': 500,
        'message': 'An unexpected error occurred',
        'data': None
    }), 500


# 根路由重定向到Swagger UI
@app.route('/')
def index():
    return app.send_static_file('swagger-ui/index.html')


if __name__ == "__main__":
    # 监听端口由部署环境提供；调试模式默认关闭。
    app_port = int(os.environ["ONECITY_APP_PORT"])
    app_debug = os.environ.get("ONECITY_DEBUG", "false").strip().lower() in {"1", "true", "yes"}
    app.run(host="0.0.0.0", port=app_port, debug=app_debug)
