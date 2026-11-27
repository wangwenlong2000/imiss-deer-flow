"""Optional MCP/chat and model-management routes; imported only in legacy mode."""
from onecityagent.logger import logger
from onecityagent.service.agent_service.agent import Agent, AgentState

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



def register_legacy_api(app):
    from onecityagent.controller.chat_controller import chat_controller, set_runner
    from onecityagent.controller.mcp_controller import mcp_controller
    from onecityagent.controller.config_controller import config_controller
    from onecityagent.controller.llm_controller import llm_bp
    set_runner(MCPRunner())
    app.register_blueprint(config_controller, url_prefix="/v1/config")
    app.register_blueprint(chat_controller, url_prefix="/v1")
    app.register_blueprint(mcp_controller, url_prefix="/v1/mcp")
    app.register_blueprint(llm_bp, url_prefix="/v1/llm")
