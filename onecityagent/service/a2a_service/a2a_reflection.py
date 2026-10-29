import asyncio
import copy
import json
import re

from flask import request, jsonify

from python_a2a.server.http import create_flask_app
from flask.json.provider import DefaultJSONProvider

from python_a2a import AgentCard, AgentSkill, A2AServer, TaskStatus, TaskState, run_server, A2AResponseError, \
    MessageRole, TextContent
from python_a2a import Message as A2AMessage
from typing import List, Optional, Any, Dict, Tuple, Iterable
from pydantic import BaseModel, Field

from onecityagent.config import config, get_port_from_url
from onecityagent.config import MCPServerConfig
from onecityagent.service.a2a_service import a2a_config_server

from onecityagent.exceptions import TokenLimitExceeded
from onecityagent.logger import logger
from onecityagent.prompt import MCP_SYSTEM_PROMPT, MCP_FEEDBACK_PROMPT
from onecityagent.schema import Message, AgentState, Memory, ToolChoice
from onecityagent.service.llm_service import LLM
from onecityagent.service.tool_service.mcp_tool import MCPClients
from onecityagent.utils.common_utils import contains_terminate

agent_url = config.a2a_config.feedback  # 需要在配置中添加feedback智能体的URL

# 创建反馈智能体卡片
feedback_agent_card = AgentCard(
    name="反馈智能体",
    description="一个专门提供执行结果反馈和建议的智能体。",
    url=agent_url,
    version="1.0.0",
    skills=[
        AgentSkill(
            name="提供执行反馈",
            description="能够评估执行结果并提供反馈与建议。",
            examples=[
                """'执行是否正确': True, '反馈建议': '已经获取到了三个受灾地点的经纬度信息。现在可以编写灾情研判简报了。根据提供的信息，此次洪涝灾害发生在湖北省随州市随县的柳林镇、洪山镇和三里岗镇，持续时间为12小时，其中柳林镇的最大雨量达到了519毫米，其他地区也超过了300毫米。此次灾害导致28人遇难，5人失联，158万余人受灾，1700余间房屋受损，公路、桥梁、供电、通信等基础设施严重受损。'"""]
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

def find_latest_user_execution_step(memory) -> Tuple[Optional[str], Optional[int]]:
    """
    在 memory 中查找最新的 role == 'user' 且内容以 '执行第X步' 开头的消息。
    支持被 JSON 转义或包裹的字符串（例如: "\"执行第1步: ...\""）。
    返回 (message_text, step_number)；找不到则返回 (None, None)。
    """
    # 尝试获取消息列表的各种常见属性/方法
    candidates = None
    for attr in ("messages", "_messages", "history", "msgs"):
        if hasattr(memory, attr):
            candidates = getattr(memory, attr)
            break
    if candidates is None and hasattr(memory, "get_messages"):
        try:
            candidates = memory.get_messages()
        except Exception:
            candidates = None
    if candidates is None:
        return None, None

    # 确保是可迭代的并从最新往前查
    if isinstance(candidates, Iterable):
        try:
            messages_iter = reversed(list(candidates))
        except Exception:
            try:
                messages_iter = reversed(candidates)
            except Exception:
                return None, None
    else:
        return None, None

    def _normalize_text(raw: Any) -> str:
        """把各种可能的 content 归一为可匹配的纯文本（尝试 json.loads 解码转义字符串）。"""
        if raw is None:
            return ""
        s = str(raw).strip()
        # 如果看起来像被 JSON 字符串化（以双引号或单引号包裹或包含转义），尝试 json.loads
        # 多次解码以处理双重转义情况
        for _ in range(2):
            if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")) or ('\\n' in s or '\\"' in s or "\\'" in s):
                try:
                    decoded = json.loads(s)
                    if isinstance(decoded, str):
                        s = decoded.strip()
                        continue
                except Exception:
                    # 若解码失败，跳出循环保留原始 s
                    break
            break
        return s

    for msg in messages_iter:
        # 支持 dict 或 对象 两种形式
        role = None
        content = None

        if isinstance(msg, dict):
            role = msg.get("role") or msg.get("type")
            content = msg.get("content") or msg.get("text") or msg.get("body")
        else:
            # 对象形式，尽量读取常见属性
            role = getattr(msg, "role", None) or getattr(msg, "type", None)
            content = getattr(msg, "content", None) or getattr(msg, "text", None)

        if not role or str(role).lower() != "user":
            continue

        if content is None:
            continue

        text = _normalize_text(content)
        # 使用更宽松的正则：允许前导的引号、反斜杠或空白
        m = re.search(r'^[\s"\\\']*执行第\s*(\d+)\s*步', text)
        if m:
            try:
                step_num = int(m.group(1))
            except Exception:
                step_num = None
            return text, step_num

    return None, None


def extract_and_modify_jsons(text: str) -> Dict[str, Any]:
    """
    从文本中提取并解析第一个最外层 JSON 对象（dict）。
    使用括号计数，忽略字符串中的花括号，支持多层嵌套。
    如果 JSON 解析成功，则把 JSON 之前和之后的非 JSON 文本（若有）合并到返回 dict 的 "content" 字段。
    解析失败返回空 dict。
    """
    start = -1
    depth = 0
    in_string = False
    escape = False

    for i, ch in enumerate(text):
        if escape:
            escape = False
            continue

        if ch == '\\':
            if in_string:
                escape = True
            continue

        if ch == '"':
            in_string = not in_string
            continue

        if in_string:
            continue

        if ch == '{':
            if depth == 0:
                start = i
            depth += 1
        elif ch == '}':
            if depth > 0:
                depth -= 1
                if depth == 0 and start != -1:
                    json_str = text[start:i + 1]
                    try:
                        data = json.loads(json_str)
                        if isinstance(data, dict):
                            # JSON 之前和之后的非 JSON 文本
                            leading_text = text[:start].strip()
                            trailing_text = text[i + 1:].strip()

                            existing = data.get("content")
                            # 将 existing 转为字符串（若存在且非字符串）
                            if existing is not None and not isinstance(existing, str):
                                try:
                                    existing = json.dumps(existing, ensure_ascii=False)
                                except Exception:
                                    existing = str(existing)

                            parts: List[str] = []
                            if leading_text:
                                parts.append(leading_text)
                            if existing:
                                parts.append(existing)
                            if trailing_text:
                                parts.append(trailing_text)

                            if parts:
                                data["content"] = "\n\n".join(p.rstrip() for p in parts if p)

                            return data
                    except Exception as e:
                        logger.debug(f"JSON解析失败: {json_str}, error: {e}")
                    # 重置，继续寻找下一个候选
                    start = -1

    return {}

class FeedbackAgent(A2AServer):
    name: str = "feedback_agent"
    description: Optional[str] = "An agent that provides feedback on execution results."

    # 配置项
    terminate: Optional[list] = ["terminate", "终止"]
    system_prompt: Optional[str] = MCP_SYSTEM_PROMPT
    feedback_prompt: Optional[str] = MCP_FEEDBACK_PROMPT

    # 状态与依赖
    memory: Memory = Field(default_factory=Memory, description="Agent's memory store")
    state: AgentState = Field(default=AgentState.IDLE, description="Current agent state")
    llm: LLM = Field(default_factory=LLM, description="Language model instance")
    mcp_clients: MCPClients = Field(default_factory=MCPClients)

    def __init__(self):
        super().__init__(agent_card=feedback_agent_card)
        self.memory = Memory()
        self.llm = LLM(config_name="default", llm_config=config.llm)
        self.mcp_clients = MCPClients()
        self.state = AgentState.IDLE
        self.tools = []

    def add_custom_routes(self, app):
        """添加自定义路由"""

        @app.route("/update_llm_config", methods=["POST"])
        def update_llm_config():
            """自定义接口：更新 LLM 配置"""
            try:
                from onecityagent.service.llm_service import TenantLLMService
                models = TenantLLMService.get_by_status_on()
                if not models[0]:
                    return jsonify({"code": 400, "data": {"message": "缺少llm更新配置"}, "message": "failure"}), 400
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
                    config.update_config(new_llm_config)
                    logger.info(f"更新反馈智能体LLM 配置{new_llm_config}")
                    # 更新 LLM 配置
                    self.llm = LLM(config_name="default", llm_config=config.llm)
                    return jsonify({"code": 0, "data": {"message": "LLM 配置已更新"}, "message": "success"}), 200
            except Exception as e:
                return jsonify({"code": 500, "data": {"message": str(e)}, "message": "failure"}), 500

    def add_custom_mcp_routes(self, app):
        """添加自定义路由"""
        @app.route("/update_mcp_config", methods=["post"])
        def update_config():
            """自定义接口：更新 MCP 配置"""
            data = request.get_json()
            self.tools = data["tools"]
            return jsonify({"code": 0, "data": {"message": f"MCP 配置已更新"}, "message": "success"}), 200

    async def provide_feedback(self, memory: Memory) -> Tuple[bool, memory, str, bool]:
        """生成对当前执行状态的反馈，决定下一步操作"""
        try:

            # 使用示例（在原函数中插入）
            latest_text, latest_step = find_latest_user_execution_step(memory)
            step_info = f"当前任务: {latest_text}"

            # 添加反馈提示词
            if self.tools:
                tool_names = [tool.get('function', {}).get('name') for tool in self.tools]
                if 'maps_geo' not in tool_names:
                    wrong_prompt =  ("\n*重点注意：1.目前没有可用的地理位置查询工具，请不要尝试任何查询经纬度信息方式。同时输出中不允许包含任何经纬度信息，第一步中无法查询经纬度可以输出action_status为false，直接结束任务。\n"
                                         "2.判断处置方案步骤是否正确执行时，不需要考虑经纬度查询步骤是否正确\n"
                                         "3.如果要求发送简报给相关部门，不需要考虑经纬度查询步骤是否正确，可以将不包含经纬度地址的简报使用工具发送简报给相关部门*")
                    feedback_prompt = f"{step_info}\n" + (
                                self.feedback_prompt or "请根据当前助手返回内容，判断是否正确执行") + wrong_prompt
                else:
                    feedback_prompt = f"{step_info}\n" + (self.feedback_prompt or "请根据当前助手返回内容，判断是否正确执行")

            else:
                feedback_prompt = f"{step_info}\n" + (self.feedback_prompt or "请根据当前助手返回内容，判断是否正确执行")

            try:
                # 使用LLM生成反馈
                response = await self.llm.ask(
                    messages=memory.messages,
                    system_msgs=[Message.system_message(feedback_prompt)] if feedback_prompt else None
                )
            except Exception as e:
                # 获取底层异常信息
                cause = getattr(e, '__cause__', None)
                if cause:
                    error_msg = f"{str(e)}; 原因: {str(cause)}"
                else:
                    error_msg = str(e)
                logger.error(f"生成反馈时出错: {error_msg}")
                error_msg = f"大模型请求错误，超出最大Token数，请尝试更换模型或联系管理员"
                memory.add_message(
                    Message.assistant_message(f"大模型请求错误，超出最大Token数，请尝试更换模型或联系管理员"))
                return True, memory, error_msg, False

            content = response or ""
            json_res = extract_and_modify_jsons(content)

            # 解析内容，若解析失败则兜底返回原始内容并视为已完成，继续下一步
            if not json_res or "content" not in json_res:
                feedback_content = content  # 兜底返回原始内容
                # 解析失败时，视为动作已完成，继续执行下一步
                action_status = True
            else:
                feedback_content = json_res["content"]
                action_status_raw = json_res.get("action_status", False)
                if isinstance(action_status_raw, str):
                    action_status = action_status_raw.lower() == "true"
                else:
                    action_status = bool(action_status_raw)
            logger.info(f"反馈智能体生成的内容: {feedback_content}")
            # 检查是否需要终止
            should_terminate = False
            content = json_res.get("content")
            # 1) 顶层 terminate 字段优先判断（支持 bool 或常见字符串）
            term_flag = json_res.get("terminate")
            if isinstance(term_flag, bool):
                should_terminate = term_flag
            elif isinstance(term_flag, str) and term_flag.strip().lower() in ("true", "1", "yes", "terminate", "终止"):
                should_terminate = True

            # 2) 如果 content 本身是 dict，检查其内部的 terminate 字段
            if not should_terminate and isinstance(content, dict):
                c_term = content.get("terminate")
                if isinstance(c_term, bool):
                    should_terminate = c_term
                elif isinstance(c_term, str) and c_term.strip().lower() in ("true", "1", "yes", "terminate", "终止"):
                    should_terminate = True

            # 3) 最后使用 contains_terminate 做递归/容错匹配（支持 str/list/dict 等）
            if not should_terminate and contains_terminate(content, self.terminate):
                should_terminate = True

            # 判断是否生成重复内容
            last_question = memory.messages[-1].to_dict()["content"] if memory.messages else ""
            # 判断最后一条消息和反馈内容85%以上相似则认为没有生成有效反馈
            similarity = 0.0
            if last_question and content:
                from difflib import SequenceMatcher
                similarity = SequenceMatcher(None, last_question, content).ratio()
            if similarity >= 0.85:
                if should_terminate:
                    feedback_content = "该步骤已结束。"
                else:
                    feedback_content = feedback_content + "\n请继续完善内容。"
            
            # 判断如果是还有现在可以执行、继续完善内容等，则不终止
            if re.search(r'继续执行|现在可以编写|现在可以', feedback_content):
                return False, memory, feedback_content, action_status

            return should_terminate, memory, feedback_content, action_status
        except Exception as e:
            # 获取底层异常信息
            cause = getattr(e, '__cause__', None)
            if cause:
                error_msg = f"{str(e)}; 原因: {str(cause)}"
            else:
                error_msg = str(e)
            logger.error(f"生成反馈时出错: {error_msg}")
            return False, memory, str(e), False

    async def handle_message_async(self, message: A2AMessage) -> A2AMessage:
        try:
            # 初始化记忆
            memory = Memory()
            logger.info(f"收到A2A消息: {message}")
            message_text = message.content.text if hasattr(message.content, "text") else "No query provided"
            message_data = json.loads(message_text) if isinstance(message_text, str) else message_text

            # 重建记忆
            if "memory" in message_data:
                for msg_dict in message_data["memory"]:
                    role = msg_dict.get("role")
                    content = msg_dict.get("content")

                    if role == "system":
                        memory.add_message(Message.system_message(content))
                    elif role == "user":
                        memory.add_message(Message.user_message(content))
                    elif role == "assistant":
                        memory.add_message(Message.assistant_message(content))
                    elif role == "thinker":
                        memory.add_message(Message.thinker_message(content))
                    elif role == "actor":
                        memory.add_message(Message.actor_message(content))
                    elif role == "tool":
                        memory.add_message(Message.tool_message(
                            msg_dict["content"],
                            msg_dict["name"],
                            msg_dict.get("tool_call_id")
                        ))
            else:
                # 如果没有消息历史，使用内容作为用户消息
                memory.add_message(Message.user_message(message_data.get("content", "")))

            #获取最新的角色为user，内容为执行第X步开头的信息


            # 生成反馈
            should_terminate, memory, feedback, action_status = await self.provide_feedback(memory)

            # 准备返回数据
            result = {
                "memory": memory.to_dict_list(),
                "should_terminate": should_terminate,
                "feedback": feedback,
                "action_status": action_status
            }
            logger.info(f"反馈智能体目前状态结果: {result}")

            result_json = json.dumps(result, ensure_ascii=False, indent=2)

            return A2AMessage(
                content=TextContent(text=result_json),
                role=MessageRole.AGENT,
                parent_message_id=message.message_id,
                conversation_id=message.conversation_id
            )

        except Exception as e:
            logger.error(f"FeedbackAgent处理任务时出错: {e}")
            if not result_json:
                result_json = json.dumps({
                    "memory": memory.to_dict_list() if 'memory' in locals() else [],
                    "should_terminate": False,
                    "feedback": f"反馈智能体处理任务时出错,请联系管理员: {str(e)}",
                    "action_status": False
                }, ensure_ascii=False, indent=2)
            return A2AMessage(
                content=TextContent(text=result_json),
                role=MessageRole.AGENT,
                parent_message_id=message.message_id,
                conversation_id=message.conversation_id
            )

    def handle_message(self, message: A2AMessage) -> A2AMessage:
        return asyncio.run(self.handle_message_async(message))


def run_setup():
    # 反馈智能体不需要连接MCP工具，所以这里可以留空或进行其他初始化
    pass


if __name__ == '__main__':
    """主函数，用于启动反馈智能体服务"""
    port = get_port_from_url(agent_url)
    agent = FeedbackAgent()
    run_setup()
    app = create_flask_app(agent)
    app.json = NoUnicodeEscapeJSONProvider(app)
    agent.add_custom_routes(app)  # 添加自定义路由
    agent.add_custom_mcp_routes(app)
    app.run(host="0.0.0.0", port=port)


