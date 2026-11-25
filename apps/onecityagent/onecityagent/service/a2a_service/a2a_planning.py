import asyncio
import re
from typing import Dict, List

from onecityagent.service.tool_service.mcp_tool import MCPClients
from python_a2a import AgentCard, AgentSkill, A2AServer, TaskStatus, TaskState, run_server, MessageRole, TextContent
from python_a2a import Message as A2AMessage
import json
from flask import request, jsonify
from python_a2a.server.http import create_flask_app
from flask.json.provider import DefaultJSONProvider

from onecityagent.service.llm_service import LLM
from onecityagent.logger import logger
from onecityagent.schema import Message, ToolChoice
from onecityagent.prompt import OUTLINE_PROMPT, USER_REQUEST_PROMPT
from onecityagent.service.tool_service.mysql_tool import query_plan_by_scene, query_user_scenes
from onecityagent.config import config, get_port_from_url
from onecityagent.service.llm_service import TenantLLMService
from onecityagent.service.a2a_service import a2a_config_server
from onecityagent.config import MCPServerConfig

# 定义一个自定义的JSON提供者，关闭ensure_ascii选项
class NoUnicodeEscapeJSONProvider(DefaultJSONProvider):
    # 关键：关闭 ensure_ascii
    ensure_ascii = False

agent_url = config.a2a_config.planner
# 创建Agent Card
agent_card = AgentCard(
    name="规划智能体",
    description="一个用于规划和任务分解的智能体，能够检索方案库或处理复杂任务并提供清晰的执行大纲。",
    url=agent_url,
    version="1.0.0",
    skills=[
        AgentSkill(
            name="规划与任务分解",
            description="能够根据用户请求规划任务大纲，并进行任务分解。",
            examples=['从信息中分析出洪涝地点经纬度，持续的时间，灾情涉及范围，输出灾情研判简报，控制在200字以内',
                      '结合洪涝等级，输出洪涝处置方案', '调用工具，将灾情通知相关部门处置，并包含研判简报和处置方案']
        )
    ],
    capabilities={
        "streaming": False,
        "pushNotifications": False,
        "stateTransitionHistory": False
    }
)

class Planner(A2AServer):
    def __init__(self, config):
        super().__init__(agent_card=agent_card)
        self.config = config
        self.llm = LLM(config_name="default", llm_config=self.config.llm)
        self.outline_plan_list = []
        self.db_config = {
        "host": self.config.database_config.host,
        "port": self.config.database_config.port,
        "user": self.config.database_config.user,
        "password": self.config.database_config.password,
        "database": self.config.database_config.name
    }
        self.mcp_clients = MCPClients()
        self.available_tools = None
        self.connected_servers = {}
        self.tool_choices = ToolChoice.NONE
        self.tools = []

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
            # raise
            return {"code": 500, "message": str(e)}

    def add_custom_llm_routes(self, app):
        """添加自定义路由"""
        @app.route("/update_llm_config", methods=["POST"])
        def update_llm_config():
            """自定义接口：更新 LLM 配置"""
            try:
                models = TenantLLMService.get_by_status_on()
                if not models or not models[0]:
                    return jsonify({"code":400, "data":{"message": f"缺少llm更新配置"}, "message":"failure"}), 400
                else:
                    new_llm_config = {"llm":
                        {
                            "default": {
                                "model": models[0]["model"],
                                "base_url": models[0]["base_url"],
                                "api_key": models[0]["api_key"],
                                "max_tokens": models[0]["max_tokens"],
                            }
                        }
                    }
                    self.config.update_config(new_llm_config)
                    logger.info(f"更新 Planning LLM 配置{new_llm_config}")
                    # 更新 LLM 配置
                    self.llm = LLM(config_name="default", llm_config=self.config.llm)
                    return jsonify({"code":0, "data":{"message": f"LLM 配置已更新"}, "message":"success"}), 200
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
            new_tools = [tool for tool in self.mcp_clients.tools if tool.server_id == server_id]
            self.mcp_clients.add_tools(*new_tools)
            logger.info(f"[A2A_MCP] 成功连接到MCP服务器: {server_id or server_url} ({conn_type}), 工具数量: {len(new_tools)}")
        except Exception as e:
            logger.error(f"[A2A_MCP] 连接MCP服务器失败: server_id={server_id}, error={str(e)}")
            raise

    def add_custom_mcp_routes(self, app):
        """添加自定义路由"""
        @app.route("/update_mcp_config", methods=["post"])
        def update_config():
            """自定义接口：更新 MCP 配置"""
            data = request.get_json()
            self.tools = data["tools"]
            return jsonify({"code": 0, "data": {"message": f"MCP 配置已更新"}, "message": "success"}), 200

    @staticmethod
    def extract_steps(kg_plan):
        """
        容错提取步骤：
        - 优先匹配完整的 <|begin_step|>...<|end_step|>
        - 如果缺失一侧标记，通过标记流状态机尽量收集片段（支持缺失结束或开始标记）
        - 若上面都未能获取到内容，使用换行/编号/Day/第N天 等启发式拆分
        返回清洗后的步骤字符串列表（按出现顺序，去重并剔除空串）
        """
        if not kg_plan:
            return []

        text = str(kg_plan)
        steps = []

        # 1) 完整配对的优先提取
        full_pat = re.compile(r'<\|begin_step\|>(.*?)<\|end_step\|>', re.DOTALL)
        found = [s.strip() for s in full_pat.findall(text) if s and s.strip()]
        if found:
            # 直接返回，保留顺序
            return found

        # 2) 标记流状态机，兼容单侧缺失的情况
        tokens = re.split(r'(<\|begin_step\|>|<\|end_step\|>)', text)
        buffer = None
        prev_content = None
        for tok in tokens:
            if tok == '<|begin_step|>':
                buffer = ''
            elif tok == '<|end_step|>':
                if buffer is not None:
                    candidate = buffer.strip()
                    if candidate:
                        steps.append(candidate)
                else:
                    # 没有 begin，却遇到 end，尝试把前一段内容作为候选
                    if prev_content and prev_content.strip():
                        steps.append(prev_content.strip())
                buffer = None
            else:
                # 普通内容片段
                if buffer is not None:
                    buffer += tok
                prev_content = tok

        # 如果最后遇到 begin 但没有 end，则把 buffer 内容作为一个步骤
        if buffer is not None and buffer.strip():
            steps.append(buffer.strip())

        # 3) 若仍为空，使用启发式拆分（换行、编号、Day、中文“第N天”等）
        if not steps:
            # 先按换行拆分并保留可能的长行
            lines = [ln.strip() for ln in re.split(r'\r?\n', text) if ln.strip()]
            # 如果行本身不多，尝试按常见分隔符拆分整段文本
            if len(lines) <= 1:
                # 以常见分隔关键词或符号尝试拆分
                heur_splits = re.split(
                    r'(?:<\|begin_step\|>|<\|end_step\|>|\n-{2,}|\n\*+|\n\s*\d+\.[\s]|(?:(?<=\n)|^)\s*Day\s*\d+|第\s*\d+\s*天)',
                    text, flags=re.IGNORECASE)
                lines = [s.strip() for s in heur_splits if s and s.strip()]

            # 筛选出像步骤的行（长度阈值或包含关键字）
            for ln in lines:
                if len(ln) >= 10 or re.search(r'\bDay\b|\bday\b|第\s*\d+\s*天|步骤|Day\s*\d+', ln,
                                              flags=re.IGNORECASE):
                    steps.append(ln)

        # 最后清洗：去除多余的标记、首尾空白，合并连续空白为单空格，去重并保持第一次出现顺序
        cleaned = []
        seen = set()
        for s in steps:
            s_clean = re.sub(r'<\|begin_step\|>|<\|end_step\|>', '', s)
            s_clean = re.sub(r'\s+', ' ', s_clean).strip()
            if s_clean and s_clean not in seen:
                cleaned.append(s_clean)
                seen.add(s_clean)

        return cleaned

    @staticmethod
    def get_sim_req(requests_response, sim_threshold):
        try:
            reqs_response = json.loads(requests_response)
            valid_results = []
            for item in reqs_response:
                if "scene" in item and "score" in item and isinstance(item["score"], (int, float)):
                    valid_results.append(item)
            if not valid_results:
                return None
            sorted_reqs_response = sorted(valid_results, key=lambda x: x["score"], reverse=True)
            if sorted_reqs_response[0]["score"] >= sim_threshold:
                return sorted_reqs_response[0]["scene"]
            return None
        except json.JSONDecodeError:
            logger.error("JSON解析失败")
            return None

    @staticmethod
    def build_request_message(cur_request, scenes):
        """构建用户请求消息"""
        saved_scenes_str = "\n".join([f"{idx + 1}. {req}" for idx, req in enumerate(scenes)])
        return f"用户任务: {cur_request}\n\n场景列表:\n{saved_scenes_str}\n\n"

    async def generate_outline(self, request):
        """生成大纲"""
        logger.info(f"使用LLM生成方案：{request}")

        # # 使用ask_tool方法调用LLM并传入可用工具
        # outline_response = await self.llm.ask_tool(
        #     messages=[Message.user_message(str(request))],
        #     system_msgs=[Message.system_message(OUTLINE_PROMPT)] if OUTLINE_PROMPT else None,
        #     tools=self.available_tools.to_params(),
        #     tool_choice="none"
        # )

        # # 仅在使用ask_tool时需要
        # outline_response = outline_response.content
        # tools = self.available_tools.to_params()
        tools = self.tools
        system_prompt = str(OUTLINE_PROMPT) + "\n可用工具列表如下：" + str(tools)

        # 使用ask方法调用LLM
        outline_response = await self.llm.ask(
            messages=[Message.user_message(str(request))],
            system_msgs=[Message.system_message(system_prompt)] if OUTLINE_PROMPT else None,
        )
        logger.info(f"大模型生成的大纲内容: {outline_response}")
        return outline_response

    # 异步规划整体任务执行大纲
    async def outline_plan(self, request) -> list:
        """
        根据 request 确定整体任务执行的大纲，再根据大纲进行任务分解执行。
        """
        outline_response = await self.llm.ask(
            messages=[Message.user_message(request)],
            system_msgs=(
                [Message.system_message(OUTLINE_PROMPT)]
                if OUTLINE_PROMPT
                else None
            ),
        )
        outline_list = self.extract_steps(outline_response)
        logger.info(f"任务大纲：{outline_list}")
        self.outline_plan_list = outline_list
        return outline_list

    # 检索知识库内容中的任务大纲
    async def plan_kg(self, request, scene, user_id=""):
        """
        根据 request 确定整体任务执行的大纲，再根据大纲进行任务分解执行。
        scene_type: 0-内置方案库 1-个人方案库 2-大模型生成
        """
        try:
            logger.info(f"开始进行内置方案库检索，场景: {scene}, 用户ID: {user_id}")
            scene_name = ""
            scene_type = "2"
            retrieval_chunk = query_plan_by_scene(scene, self.db_config, user_id, 'public')
            if retrieval_chunk:
                outline_response = retrieval_chunk
                logger.info(f"检索到内置方案库内容: {outline_response}")
                scene_name = scene
                scene_type = "0"
            else:
                logger.info("未检索到内置方案库内容，开始进行个人方案库检索")
                saved_scenes = query_user_scenes(self.db_config, user_id)
                if scene != config.intent_config.OTHER_SCENE:
                    request_sim = f"{scene}, {request}"
                else:
                    request_sim = request
                if not saved_scenes:
                    logger.info("个人方案库为空，开始使用LLM生成大纲")
                    outline_response = await self.generate_outline(request)
                    outline_list = self.extract_steps(outline_response)
                    logger.info(f"任务大纲：{outline_list}")
                    self.outline_plan_list = outline_list
                    planner_info = {
                        "scene_name": scene_name,
                        "scene_type": scene_type,
                        "outline_plan": outline_list
                    }
                    return planner_info
                user_message = self.build_request_message(request_sim, saved_scenes)

                plan_user_response = await self.llm.ask(
                    messages=[Message.user_message(user_message)],
                    system_msgs=(
                        [Message.system_message(USER_REQUEST_PROMPT)]
                        if USER_REQUEST_PROMPT
                        else None
                    )
                )
                similar_scene = self.get_sim_req(plan_user_response, 0.5)
                if similar_scene:
                    if outline_response := query_plan_by_scene(similar_scene, self.db_config, user_id, 'person'):
                        logger.info(f"从个人方案库检索到方案: {outline_response}")
                        scene_name = similar_scene
                        scene_type = "1"
                    else:
                        logger.info(f"个人方案库未检索到方案: {similar_scene}，开始使用LLM生成大纲")
                        outline_response = await self.generate_outline(request)
                else:
                    outline_response = await self.generate_outline(request)

            outline_list = self.extract_steps(outline_response)
            logger.info(f"任务大纲：{outline_list}")
            self.outline_plan_list = outline_list
            planner_info = {
                "scene_name": scene_name,
                "scene_type": scene_type,
                "outline_plan": outline_list
            }
            return planner_info
        except Exception as e:
            # 获取底层异常信息
            cause = getattr(e, '__cause__', None)
            if cause:
                error_msg = f"{str(e)}; 原因: {str(cause)}"
            else:
                error_msg = str(e)
            logger.error(f"出错: {error_msg}")
            logger.error(f"检索知识库任务失败: {e}")
            outline_response = await self.llm.ask(
                messages=[Message.user_message(request)],
                system_msgs=(
                    [Message.system_message(OUTLINE_PROMPT)]
                    if OUTLINE_PROMPT
                    else None
                ),
            )
            outline_list = self.extract_steps(outline_response)
            logger.info(f"任务大纲：{outline_list}")
            self.outline_plan_list = outline_list
            scene_name = ""
            scene_type = "2"
            planner_info = {
                "scene_name": scene_name,
                "scene_type": scene_type,
                "outline_plan": outline_list
            }
            return planner_info

    def handle_message(self, message: Message) -> Message:
        # Extract message text
        # 提取消息内容
        logger.info(f"收到A2A消息: {message}")
        message_text = message.content.text if hasattr(message.content, "text") else "No query provided"
        message_data = json.loads(message_text) if isinstance(message_text, str) else message_text
        scene = message_data.get("scene", "") if isinstance(message_data, dict) else ""
        user_id = message_data.get("user_id", "public") if isinstance(message_data, dict) else "public"
        # 根据场景提取请求内容
        if scene == self.config.intent_config.OTHER_SCENE:
            request = message_data.get("question", "") if isinstance(message_data, dict) else ""
        else:
            question = message_data.get("question", "") if isinstance(message_data, dict) else ""
            param = message_data.get("params", "") if isinstance(message_data, dict) else ""
            request = f"用户问题：{question}, 参数：{param}"
        logger.info(f"Received request: {request} for scene: {scene}")
        outline_list = asyncio.run(self.plan_kg(request, scene, user_id))
        return A2AMessage(
            content=TextContent(json.dumps(outline_list, ensure_ascii=False)),
            role=MessageRole.AGENT,
            parent_message_id=message.message_id,
            conversation_id=message.conversation_id)

def run_setup():
    logger.info("[A2A_SETUP] 开始运行设置")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    initial_result = loop.run_until_complete(plan_server.initialize())
    logger.info(f"[A2A_SETUP] 未连接成功的 server: {initial_result.get('unconnect_mcp_list', [])}")
    loop.close()
    logger.info("[A2A_SETUP] 设置完成")

    return initial_result

if __name__ == "__main__":
    port = get_port_from_url(agent_url)
    plan_server = Planner(config)
    first_initial_result = run_setup()
    app = create_flask_app(plan_server)
    app.json = NoUnicodeEscapeJSONProvider(app)
    plan_server.add_custom_llm_routes(app)  # 添加自定义路由
    plan_server.add_custom_mcp_routes(app)  # 添加自定义路由
    app.run(host="0.0.0.0", port=port)

