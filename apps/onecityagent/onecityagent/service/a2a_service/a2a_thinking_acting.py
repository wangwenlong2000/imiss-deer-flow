import asyncio
import copy
import json
import threading
import requests
import re
from flask import request, jsonify
from openai.types.chat import ChatCompletionMessageToolCall
from python_a2a.server.http import create_flask_app
from flask.json.provider import DefaultJSONProvider
from python_a2a import AgentCard, AgentSkill, A2AServer, TaskStatus, TaskState, run_server, A2AResponseError, \
    MessageRole, TextContent
from python_a2a import Message as A2AMessage
from typing import List, Optional, Any, Dict, Tuple
from pydantic import BaseModel, Field
from build.text_normalization import normalize_mcp_tools_ascii_sequences

from onecityagent.config import config, get_port_from_url, MCPServerConfig
from onecityagent.service.a2a_service import a2a_config_server
from onecityagent.logger import logger
from onecityagent.prompt import MCP_SYSTEM_PROMPT, MCP_NEXT_STEP_PROMPT
from onecityagent.schema import ToolChoice, Message, ToolCall, AgentState, Memory
from onecityagent.service.llm_service import LLM
from onecityagent.service.tool_service.mcp_tool import MCPClients
from onecityagent.service.llm_service.llm import extract_tool_calls
from fastmcp import Client
from fastmcp.client import SSETransport, StreamableHttpTransport

agent_url = config.a2a_config.actor

# 创建Acting智能体Card
acting_agent_card = AgentCard(
    name="执行智能体",
    description="一个集成思考和执行功能的智能体，能够思考并执行工具调用。",
    url=agent_url,
    version="1.0.0",
    skills=[
        AgentSkill(
            name="思考和执行",
            description="能够分析问题、生成工具调用计划并执行工具调用。",
            examples=["查询西安的经纬度"]
        )
    ],
    capabilities={
        "streaming": False,
        "pushNotifications": False,
        "stateTransitionHistory": False
    }
)


class NoUnicodeEscapeJSONProvider(DefaultJSONProvider):
    ensure_ascii = False


class ActingAgent(A2AServer):
    name: str = "acting_agent"
    description: Optional[str] = "一个集成思考和执行功能的智能体，能够思考并执行工具调用。"
    terminate: Optional[list] = ["terminate", "终止"]
    system_prompt: Optional[str] = MCP_SYSTEM_PROMPT
    next_step_prompt: Optional[str] = MCP_NEXT_STEP_PROMPT

    # 思考相关
    llm: LLM = Field(default_factory=LLM, description="Language model instance")
    tool_choices: str = ToolChoice.AUTO
    max_steps: int = 20

    # 执行相关
    mcp_clients: MCPClients = Field(default_factory=MCPClients)
    available_tools: MCPClients = None
    max_observe: int = 10000
    connected_servers: Dict[str, str] = Field(default_factory=dict)

    # 共享状态
    state: AgentState = Field(default=AgentState.IDLE, description="Current agent state")
    _current_base64_image = None

    def __init__(self):
        super().__init__(agent_card=acting_agent_card)
        self.llm = LLM(config_name="default", llm_config=config.llm)
        self.mcp_clients = MCPClients()
        self.available_tools = None
        self.connected_servers = {}
        self.state = AgentState.IDLE
        self._current_base64_image = None

    def add_custom_routes(self, app):
        """添加自定义路由"""

        @app.route("/update_llm_config", methods=["POST"])
        def update_llm_config():
            try:
                logger.info("[A2A_CONFIG] 收到LLM配置更新请求")
                # LLM配置更新逻辑
                from onecityagent.service.llm_service import TenantLLMService
                models = TenantLLMService.get_by_status_on()
                if not models[0]:
                    logger.warning("[A2A_CONFIG] LLM配置更新失败：缺少llm更新配置")
                    return jsonify({"code": 400, "data": {"message": "缺少llm更新配置"}, "message": "failure"}), 400
                else:
                    new_llm_config = {"llm": {
                        "default": {
                            "model": models[0]["model"],
                            "base_url": models[0]["base_url"],
                            "api_key": models[0]["api_key"],
                            "max_tokens": models[0]["max_tokens"],
                        }
                    }}
                    config.update_config(new_llm_config)
                    self.llm = LLM(config_name="default", llm_config=config.llm)
                    logger.info(f"[A2A_CONFIG] LLM配置更新成功: {models[0]['model']}")
                    return jsonify({"code": 0, "data": {"message": "LLM配置已更新"}, "message": "success"}), 200
            except Exception as e:
                logger.error(f"[A2A_CONFIG] LLM配置更新异常: {str(e)}")
                return jsonify({"code": 500, "data": {"message": str(e)}, "message": "failure"}), 500

    def add_custom_mcp_routes(self, app):
        """添加自定义路由"""
        @app.route("/update_mcp_config", methods=["post"])
        def update_config():
            unconnect_mcp = []
            """自定义接口：更新 mcp 配置"""
            data = request.get_json()
            agent_id = data.get("agent_id") or None
            if not agent_id:
                return jsonify({"code": 400, "data": {"message": f"缺少参数 agent id"}, "message": "failure"}), 400
            else:
                new_config = a2a_config_server.get_agent(agent_id)
            new_llm_config = new_config.get('data') or {}
            if not new_llm_config:
                return jsonify({"code": 400, "data": {"message": f"缺少更新配置"}, "message": "failure"}), 400
            try:
                config_mcp_config_servers = new_config['data']['mcp_configs']
                config.mcp_config.servers = {}
                for server_config in config_mcp_config_servers:
                    try:
                        headers_string = server_config.get("headers", None)
                        if headers_string:
                            headers = json.loads(headers_string)
                        else:
                            headers = None
                        server_id = server_config['mcp_server_name']
                        config.mcp_config.servers[server_id] = MCPServerConfig(
                            type=server_config["connection_type"],
                            url=server_config.get("server_url"),
                            command=server_config.get("command"),
                            args=server_config.get("args", []),
                            headers=headers,
                        )
                    except Exception as e:
                        unconnect_mcp.append(server_config['mcp_server_name'])
                        continue

                # 更新 MCP config 配置
                self.mcp_clients.sessions = {}
                ini_result = run_setup()

                def _get_agent_info(agent_type: int) -> tuple[str, str]:
                    data = a2a_config_server.get_agents(agent_tpye=agent_type)
                    agent = data["data"]["agents"][0]
                    return agent["agent_url"], agent["agent_id"]

                def _notify_agent(url: str, agent_id: str, label: str):
                    if not url:
                        return
                    try:
                        response = requests.post(
                            f"{url}/update_mcp_config",
                            json={"agent_id": agent_id, "tools": self.mcp_clients.to_params()},
                            timeout=30,
                        )
                        if response.status_code == 200:
                            logger.info(f"通知{label}更新MCP配置成功: {response.json()}")
                        else:
                            logger.error(
                                f"通知{label}更新MCP配置失败，状态码: {response.status_code}, 响应: {response.text}")
                    except requests.RequestException as e:
                        logger.error(f"通知{label}更新MCP配置时出错: {str(e)}")

                planning_agent_url, planning_agent_id = _get_agent_info(agent_type=1)
                reflecting_agent_url, reflecting_agent_id = _get_agent_info(agent_type=3)

                _notify_agent(planning_agent_url, planning_agent_id, "规划智能体")
                _notify_agent(reflecting_agent_url, reflecting_agent_id, "反馈智能体")

                ini_result.get('unconnect_mcp_list', []).extend(unconnect_mcp)
                unconnect_mcp_tools = ini_result.get('unconnect_mcp_list', [])
                logger.info(f"未连接的工具{unconnect_mcp_tools}")
                return jsonify(
                    {"code": 0, "data": {"message": f"MCP 配置已更新", "unconnect_mcp_tools": unconnect_mcp_tools},
                                "message": "success"}), 200
            except Exception as e:
                return jsonify({"code": 500, "data": {"message": str(e)}, "message": "failure"}), 500

    async def connect_mcp_server(self, server_url: str, server_id: str = "", stdio_args: List[str] = None,
                                 headers: Dict[str, str] = None, *, conn_type: str = "sse") -> None:
        """连接MCP服务器"""
        try:
            logger.info(f"[A2A_MCP] 尝试连接MCP服务器: server_id={server_id}, url={server_url}, type={conn_type}")

            if conn_type == "stdio":
                await self.mcp_clients.connect_stdio(server_url, stdio_args or [], server_id)
            elif conn_type == "streamableHttp":
                await self.mcp_clients.connect_http(base_url=server_url, server_id=server_id, headers=headers)
            else:
                await self.mcp_clients.connect_sse(server_url, server_id, headers=headers)

            self.connected_servers[server_id or server_url] = server_url
            self.connected_servers[server_id + "-headers"] = headers
            self.connected_servers[server_id + "-type"] = conn_type
            new_tools = [tool for tool in self.mcp_clients.tools if tool.server_id == server_id]
            self.mcp_clients.add_tools(*new_tools)
            logger.info(
                f"[A2A_MCP] 成功连接到MCP服务器: {server_id or server_url} ({conn_type}), 工具数量: {len(new_tools)}")
        except Exception as e:
            logger.error(f"[A2A_MCP] 连接MCP服务器失败: server_id={server_id}, error={str(e)}")
            raise

    async def initialize(self):
        """初始化MCP服务器连接"""
        try:
            logger.info("[A2A_INIT] 开始初始化MCP服务器连接")
            server_count = len(config.mcp_config.servers)
            await self.mcp_clients.disconnect()
            logger.info(config.mcp_config.servers)
            logger.info(f"[A2A_INIT] 配置的MCP服务器数量: {server_count}")

            unconnect_server_id = []
            for server_id, server_config in config.mcp_config.servers.items():
                try:
                    if server_config.type == "sse" and server_config.url:
                        await self.connect_mcp_server(server_config.url, server_id,
                                                      headers=server_config.headers, conn_type="sse")
                    elif server_config.type == "streamableHttp" and server_config.url:
                        await self.connect_mcp_server(server_config.url, server_id,
                                                      headers=server_config.headers, conn_type="streamableHttp")
                    elif server_config.type == "stdio" and server_config.command:
                        await self.connect_mcp_server(server_config.command, server_id,
                                                      stdio_args=server_config.args, conn_type="stdio")
                except Exception as e:
                    unconnect_server_id.append(server_id)
                    logger.error(f"[A2A_INIT] 连接到MCP服务器失败: {server_id}, error={str(e)}")
                    continue

            self.available_tools = self.mcp_clients
            logger.info(f"[A2A_INIT] 已连接的MCP工具详细信息: {self.available_tools.to_params()}")
            # await self._refresh_tools()
            tool_names = list(self.mcp_clients.tool_map.keys())
            tools_info = ", ".join(tool_names)
            logger.info(f"[A2A_INIT] 初始化完成，可用工具: {tools_info}")
            # unconnect tools
            return {"code": 200, "unconnect_mcp_list": unconnect_server_id}
        except Exception as e:
            logger.error(f"[A2A_INIT] 初始化MCP服务器连接失败: {str(e)}")
            return {"code": 500, "message": str(e)}

    async def _refresh_tools(self):
        """刷新工具列表"""
        if not self.mcp_clients.sessions:
            return [], []

        added_tools = []
        removed_tools = []
        for (key, session) in self.mcp_clients.sessions.items():
            response = await session.list_tools()
            current_tools = {tool.name: tool.inputSchema for tool in response.tools}
            previous_tools = getattr(session, "_previous_tools", {})

            previous_names = set(previous_tools.keys())
            current_names = set(current_tools.keys())
            added = list(current_names - previous_names)
            removed = list(previous_names - current_names)

            added_tools.extend(added)
            removed_tools.extend(removed)
            session._previous_tools = current_tools

        return added_tools, removed_tools

    @staticmethod
    def _clean_response(content: Any) -> Any:
        # 如果响应内容是字符串，检查是否包含长度超过60且没有任何标点的片段，若有则删除该部分
        if isinstance(content, str):
            # 中英标点集合
            punct_pattern = r'[\.，。,!！\?？;；:：、\-—"“”\'‘’\(\)\[\]\{\}<>]'
            # 按标点分割，并保留分隔符
            parts = re.split(f'({punct_pattern})', content)
            filtered = []
            for part in parts:
                if not part:
                    continue
                # 如果该片段包含标点，直接保留
                if re.search(punct_pattern, part):
                    filtered.append(part)
                    continue
                # 去掉首尾空白后判断长度，超过60且无标点的片段将被删除
                if len(part.strip()) > 60:
                    continue
                filtered.append(part)
            cleaned = ''.join(filtered).strip()
            # 将清理后的内容写回响应对
        else:
            cleaned = content
        return cleaned

    def _apply_scene_filter(self, memory: Memory) -> None:
        """
        根据首条消息判断场景（优先读取 JSON 中的 "场景" 字段），
        并从 self.mcp_clients 中筛除与当前场景无关的工具，结果赋给 self.available_tools。
        """
        try:
            if not getattr(self, "mcp_clients", None) or not getattr(self.mcp_clients, "tools", None):
                return
            if not memory.messages:
                return

            first_msg = memory.messages[0]
            first_text = (first_msg.content or "").strip()
            scene = None

            # 优先尝试解析为 JSON，读取 "场景" 字段
            try:
                parsed = json.loads(first_text)
                if isinstance(parsed, dict):
                    scene_val = parsed.get("场景") or parsed.get("scene")
                    if isinstance(scene_val, str):
                        scene = scene_val
            except Exception:
                pass

            # 关键词回退判断
            lower = first_text.lower()
            fire_keywords = ["消防", "火警", "火灾","fire"]
            flood_keywords = ["洪涝", "洪水", "水灾", "内涝", "flood"]

            if not scene:
                if any(k in lower for k in fire_keywords) and not any(k in lower for k in flood_keywords):
                    scene = "消防告警"
                elif any(k in lower for k in flood_keywords) and not any(k in lower for k in fire_keywords):
                    scene = "洪涝灾害告警"

            if not scene:
                return
            logger.info(f"[A2A_SCENE] 检测到场景: {scene}")
            return scene
        except Exception as e:
            logger.error(f"[A2A_SCENE] 场景识别出错: {str(e)}")
            return


    async def think(self, memory: Memory) -> Tuple[bool, Memory, list, str]:
        """思考阶段：分析问题并生成工具调用计划"""
        logger.info("[A2A_THINK] 开始思考阶段")

        if memory.messages[-1].role != MessageRole.USER:
            if self.next_step_prompt:
                memory.messages.append(Message.user_message(self.next_step_prompt))

        try:
            # 判断第一条消息中是否包含了场景：消防告警，如果是，则将self.available_tools设置去除洪涝灾害告警的工具
            # 判断第一条消息是否包含了场景：洪涝灾害告警，如果是，则将self.available_tools设置去除消防告警的工具
            scene = self._apply_scene_filter(memory)
            all_tools_list = self.available_tools.to_params()
            if scene == "消防告警":
                # 从all_tools_list中过滤并删除name为flooding_alarm的工具
                filtered_tools = [tool for tool in all_tools_list if tool["function"]["name"] != 'flooding_alarm']
            elif scene == "洪涝灾害告警":
                # 从all_tools_list中过滤并删除name为fire_alarm的工具
                filtered_tools = [tool for tool in all_tools_list if tool["function"]["name"] != 'fire_alarm']
            else:
                filtered_tools = all_tools_list

            acting_prompt = self.system_prompt
            # 判断工具中是否存在maps_geo, maps_direction_driving
            if not any(tool["function"]["name"] == "maps_geo" for tool in filtered_tools):
                if self.system_prompt:
                    acting_prompt = self.system_prompt + ("\n*重点注意：1.目前没有可用的地理位置查询工具，请不要尝试任何查询经纬度信息方式。同时输出中不允许包含任何经纬度信息，第一步中查询经纬度可以直接结束任务。\n"
                                           "2.如果要求发送简报给相关部门，可以将不包含经纬度地址的简报使用工具发送简报给相关部门*")

            logger.info(f"[A2A_THINK] 输入消息数量: {len(memory.messages)}")
            for i, msg in enumerate(memory.messages[-3:]):  # 只记录最后3条消息
                logger.debug(f"[A2A_THINK] 消息[{i}]: role={msg.role}, content={msg.content[:100]}...")
            try:
                response = await self.llm.ask_tool(
                    messages=memory.messages,
                    system_msgs=[Message.system_message(acting_prompt)] if self.system_prompt else None,
                    # tools=self.available_tools.to_params(),
                    tools=filtered_tools,
                    tool_choice=self.tool_choices
                )
            except Exception as e:
                logger.error(f"[A2A_THINK] LLM思考阶段调用失败: {str(e)}")

            tool_calls = response.tool_calls or []
            if not tool_calls:
                raw_calls = extract_tool_calls(response.content)
                tool_calls = [ChatCompletionMessageToolCall.model_validate(item) for item in raw_calls]

            content = response.content or ""
            content = self._clean_response(content)
            content, tool_calls = normalize_mcp_tools_ascii_sequences(content, tool_calls)

            # 添加思考结果到内存
            thinker_msg = (
                Message.from_tool_calls(content=content, tool_calls=tool_calls)
                if tool_calls else Message.assistant_message(content)
            )
            memory.add_message(thinker_msg)

            logger.info(f"[A2A_THINK] 思考阶段完成: {content[:200]}...")
            if tool_calls:
                tool_names = [call.function.name for call in tool_calls]
                logger.info(f"[A2A_THINK] 计划执行工具: {tool_names}")
                for call in tool_calls:
                    logger.info(f"[A2A_THINK] 工具调用详情: {call.function.name}({call.function.arguments})")

            return bool(tool_calls or content), memory, tool_calls, content

        except Exception as e:
            logger.error(f"[A2A_THINK] 思考阶段出错: {str(e)}")
            memory.add_message(Message.assistant_message(f"思考过程出错: {str(e)}"))
            return False, memory, []

    async def execute_tool(self, command: ToolCall) -> str:
        """执行单个工具调用"""
        if not command or not command.function or not command.function.name:
            logger.error("[A2A_EXECUTE] 工具调用格式无效")
            return "Error: Invalid command format"

        name = command.function.name
        if name not in self.available_tools.tool_map:
            logger.error(f"[A2A_EXECUTE] 未知工具: {name}")
            return f"Error: Unknown tool '{name}'"

        try:
            args = json.loads(command.function.arguments or "{}")
            logger.info(f"[A2A_EXECUTE] 开始执行工具: {name}")
            logger.info(f"[A2A_EXECUTE] 工具参数: {json.dumps(args, ensure_ascii=False)}")

            tool_info = self.available_tools.tool_map.get(name)
            original_name = tool_info.original_name if tool_info else name
            server_id = tool_info.server_id if tool_info else ""

            server_url = self.connected_servers.get(server_id, "")
            headers = self.connected_servers.get(server_id + "-headers", "")
            conn_type = self.connected_servers[server_id + "-type"]
            if not server_url:
                logger.error(f"[A2A_EXECUTE] 找不到工具服务器URL: tool={name}, server_id={server_id}")
                return f"Error: Cannot find server URL for tool '{name}'"

            # 创建与特定服务器的客户端连接，支持 SSE 和 HTTP
            logger.info(f"[A2A_EXECUTE] 使用服务器URL: {server_url} (type={conn_type})")
            if conn_type == "streamableHttp":
                transport = StreamableHttpTransport(url=server_url, headers=headers or {})
            else:  # 默认使用 SSE
                transport = SSETransport(url=server_url, headers=headers or {})

            client = Client(transport)

            async with client:
                await client.ping()
                logger.info(f"[A2A_EXECUTE] 执行工具{original_name}, {args}")
                result = await client.call_tool(original_name, args)
                logger.info(f"[A2A_EXECUTE] 工具执行完成: {name}")
                logger.debug(f"[A2A_EXECUTE] 工具执行结果: {str(result)[:500]}...")


            observation = (
                f"执行工具 `{original_name}` 获得结果:\n{str(result)}"
                if result else f"工具 `{original_name}` 执行成功，但没有返回结果"
            )
            return observation

        except Exception as e:
            error_msg = f"工具 '{name}' 执行失败: {str(e)}"
            logger.error(f"[A2A_EXECUTE] {error_msg}")
            return f"工具执行遇到网络波动: {error_msg}"

    async def act(self, tool_calls: List[ToolCall], memory: Memory) -> Memory:
        """执行阶段：执行工具调用并处理结果"""
        if not tool_calls:
            logger.info("[A2A_ACT] 无工具调用需要执行")
            return memory

        logger.info(f"[A2A_ACT] 开始执行阶段，工具数量: {len(tool_calls)}")

        for i, command in enumerate(tool_calls):
            logger.info(f"[A2A_ACT] 执行工具 {i + 1}/{len(tool_calls)}: {command.function.name}")

            self._current_base64_image = None
            result = await self.execute_tool(command)

            if self.max_observe:
                result = result[:self.max_observe]

            # 添加工具执行结果到内存
            tool_msg = Message.tool_message(
                content=result,
                tool_call_id=command.id,
                name=command.function.name,
                base64_image=self._current_base64_image,
            )
            memory.add_message(tool_msg)
            logger.info(f"[A2A_ACT] 工具执行完成: {command.function.name}")

        logger.info("[A2A_ACT] 执行阶段完成")
        return memory

    async def handle_message_async(self, message: A2AMessage) -> A2AMessage:
        """处理输入消息，整合思考和执行过程"""
        try:
            logger.info(f"收到A2A消息: {message}")

            message_text = message.content.text if hasattr(message.content, "text") else "No query provided"
            logger.info(f"[A2A_INPUT] - content: {message_text[:500]}...")

            memory = Memory()
            message_data = json.loads(message_text) if isinstance(message_text, str) else message_text

            # 重建内存
            if "messages" in message_data:
                logger.info(f"[A2A_PROCESS] 重建内存，消息数量: {len(message_data['messages'])}")
                for msg in message_data["messages"]:
                    if msg["role"] == "system":
                        memory.add_message(Message.system_message(msg["content"]))
                    elif msg["role"] == "user":
                        memory.add_message(Message.user_message(msg["content"]))
                    elif msg["role"] == "assistant":
                        if msg.get("tool_calls"):
                            tool_calls = [ToolCall.model_validate(call) for call in msg["tool_calls"]]
                            memory.add_message(Message.from_tool_calls(tool_calls=tool_calls, content=msg["content"]))
                        else:
                            memory.add_message(Message.assistant_message(msg["content"]))
                    elif msg["role"] == "tool":
                        memory.add_message(Message.tool_message(msg["content"], msg["name"], msg.get("tool_call_id")))
            else:
                user_content = message_data.get("content", "")
                logger.info(f"[A2A_PROCESS] 添加用户消息，仅部分展示: {user_content[:200]}...")
                memory.add_message(Message.user_message(user_content))

            # 思考阶段
            logger.info("[A2A_PROCESS] 进入思考阶段")
            try:
                result_bool, memory, tool_calls, think_result = await self.think(memory)
            except Exception as e:
                result_json = json.dumps({
                    "memory": memory.to_dict_list(),
                    "status": "error",
                    "llm_output": f"大模型请求错误，超出最大Token数，请尝试更换模型或联系管理员"
                }, ensure_ascii=False, indent=2)
                response_message = A2AMessage(
                    content=TextContent(text=result_json),
                    role=MessageRole.AGENT,
                    parent_message_id=message.message_id,
                    conversation_id=message.conversation_id
                )
                return response_message

            tool_results = []
            if tool_calls:
                for i, command in enumerate(tool_calls):
                    result = await self.execute_tool(command)
                    # 记录每个工具调用的结果，用于前端显示
                    tool_results.append({
                        "tool_name": command.function.name,
                        "tool_args": json.loads(command.function.arguments or "{}"),
                        "result": result,
                        "tool_call_id": command.id,
                        # "base64_image": self._current_base64_image,
                    })
                    # 添加工具执行结果到内存
                    tool_msg = Message.tool_message(
                        content=result,
                        tool_call_id=command.id,
                        name=command.function.name,
                        base64_image=self._current_base64_image,
                    )
                    memory.add_message(tool_msg)
            else:
                logger.info("[A2A_PROCESS] 跳过执行阶段，无工具调用")

            integrated_content = {
                "acting": think_result,
                "tool_calls": tool_results if tool_results else []
            }

            result = {
                "memory": memory.to_dict_list(),
                "status": "completed",
                "llm_output": integrated_content
            }

            result_json = json.dumps(result, ensure_ascii=False, indent=2)

            response_message = A2AMessage(
                content=TextContent(text=result_json),
                role=MessageRole.ACTOR_AGENT,
                parent_message_id=message.message_id,
                conversation_id=message.conversation_id
            )

            # logger.info(f"[A2A_OUTPUT] 返回消息ID: {response_message.message_id}")
            return response_message

        except Exception as e:
            if 'memory' not in locals():
                memory = Memory()
            result_json = json.dumps({
                "memory": memory.to_dict_list(),
                "status": "error",
                "llm_output": f"大模型请求错误，请尝试更换模型或联系管理员"
            }, ensure_ascii=False, indent=2)
            response_message = A2AMessage(
                content=TextContent(text=result_json),
                role=MessageRole.AGENT,
                parent_message_id=message.message_id,
                conversation_id=message.conversation_id
            )
            return response_message

    def handle_message(self, message: A2AMessage) -> A2AMessage:
        logger.info("[A2A_ENTRY] 进入A2A消息处理")
        try:
            result = asyncio.run(self.handle_message_async(message))
            logger.info("[A2A_ENTRY] A2A消息处理成功完成")
            return result
        except Exception as e:
            logger.error(f"[A2A_ENTRY] A2A消息处理失败: {str(e)}")
            raise


def run_setup():
    logger.info("[A2A_SETUP] 开始运行设置")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    initial_result = loop.run_until_complete(agent.initialize())
    logger.info(f"[A2A_SETUP] 未连接成功的 server: {initial_result.get('unconnect_mcp_list', [])}")

    loop.close()
    logger.info("[A2A_SETUP] 设置完成")

    return initial_result


if __name__ == '__main__':
    port = get_port_from_url(agent_url)
    logger.info(f"[A2A_MAIN] 启动ActingAgent，端口: {port}")
    agent = ActingAgent()
    run_setup()
    app = create_flask_app(agent)
    app.json = NoUnicodeEscapeJSONProvider(app)
    agent.add_custom_routes(app)
    agent.add_custom_mcp_routes(app)
    logger.info(f"[A2A_MAIN] Flask应用已配置，开始监听端口 {port}")
    app.run(host="0.0.0.0", port=port)


