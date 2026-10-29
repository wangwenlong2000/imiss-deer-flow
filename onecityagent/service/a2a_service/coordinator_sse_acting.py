from flask import Flask, request, Response, jsonify, make_response
import json
import uuid
import time
import os
import requests
from functools import wraps
from urllib.parse import urlparse
from loguru import logger
from python_a2a import AgentNetwork, A2AClient, TextContent
from onecityagent.config import config, Config, get_port_from_url
from onecityagent.service.a2a_service.a2a_router import AIAgentRouter
from onecityagent.service.a2a_service.intent_a2a.intent_client import IntentClient
from python_a2a.models.message import Message, MessageRole
from onecityagent.service.tool_service.mysql_tool import query_a2a_agent
from flasgger import Swagger, swag_from

swagger_config = {
    "headers": [],
    "specs": [
        {
            "endpoint": 'apispec_1',
            "route": '/apispec_1.json',
            "rule_filter": lambda rule: True,
            "model_filter": lambda tag: True,
        }
    ],
    "static_url_path": "/flasgger_static",
    "swagger_ui": True,
    "specs_route": "/apidocs/"
}

app = Flask(__name__)
swagger = Swagger(app, config=swagger_config)


class Scheduler:
    network = AgentNetwork(name="Assistant Network")
    for agent_name, agent_url in config.a2a_config.servers.items():
        network.add(agent_name, agent_url.url)
    router = AIAgentRouter(
        llm_client=A2AClient(config.a2a_config.llm_qa),
        agent_network=network,
    )
    intent_client = IntentClient(config.a2a_config.intent)
    planner = A2AClient(config.a2a_config.planner)
    acting_agent = A2AClient(config.a2a_config.actor)  # 整合的Acting智能体
    feedback_agent = A2AClient(config.a2a_config.feedback)  # 独立的反馈智能体

    def __init__(self):
        self.terminate = ["terminate", "终止", "停止", "完成", "结束", "任务完成", "任务结束"]

    @staticmethod
    def reload_network():
        db_config = {
            "host": config.database_config.host,
            "port": config.database_config.port,
            "user": config.database_config.user,
            "password": config.database_config.password,
            "database": config.database_config.name
        }
        Scheduler.network = AgentNetwork(name="Assistant Network")
        acting_agents = query_a2a_agent(db_config, agent_type=2)
        for agent_dict in acting_agents:
            for agent_name, agent_url in agent_dict.items():
                Scheduler.network.add(agent_name, agent_url)
        Scheduler.router = AIAgentRouter(
            llm_client=A2AClient(config.a2a_config.llm_qa),
            agent_network=Scheduler.network,
        )

        # 重载所有智能体的大模型配置
        agents_with_llm = query_a2a_agent(db_config, agent_type=0)
        agents_with_llm.extend(query_a2a_agent(db_config, agent_type=1))
        agents_with_llm.extend(query_a2a_agent(db_config, agent_type=3))
        agents_with_llm.extend(query_a2a_agent(db_config, agent_type=4))
        for agent_dict in agents_with_llm:
            for agent_name, agent_url in agent_dict.items():
                url = agent_url + "/update_llm_config"
                headers = {"Content-Type": "application/json"}
                response = requests.post(url, headers=headers)
                if response.status_code == 200:
                    logger.info(f"智能体 {agent_name} 的 LLM 配置已更新")
                else:
                    logger.error(f"更新智能体 {agent_name} 的 LLM 配置失败: {response.text}")

    def intent_query_route_stream(self, query: str, conversation_id=uuid.uuid4().hex, user_id="public"):
        """流式处理查询并生成SSE事件"""
        acting_agent = self.acting_agent
        res_content = {}
        if not conversation_id:
            conversation_id = uuid.uuid4().hex

        # 使用意图识别智能体处理查询
        intent_response = self.intent_client.get_intent_params(query, conversation_id)

        # 发送意图识别结果
        while isinstance(intent_response, str):
            yield f"data: {json.dumps({'type': 'agent', 'role': '意图识别智能体', 'content': intent_response, 'conversation_id': conversation_id})}\n\n"
            yield f"data: {json.dumps({'type': '[DONE]', 'conversation_id': conversation_id, 'finished': False})}\n\n"
            intent_response = self.intent_client.get_intent_params(query, conversation_id)
            return

        yield f"data: {json.dumps({'type': 'agent', 'role': '意图识别智能体', 'content': str(intent_response), 'conversation_id': conversation_id})}\n\n"

        res_content['参数'] = intent_response.get('params', {})
        res_content['场景'] = intent_response.get('scene', '')
        res_content['原始输入'] = intent_response.get('question', '')

        if res_content['场景'] == config.intent_config.OTHER_SCENE:
            if intent_response.get('dialogue'):
                memory = {
                    "messages": [
                        {
                            "role": msg.get("role", "user"),
                            "content": json.dumps(msg.get("content", ""), ensure_ascii=False, indent=2)
                        } for msg in intent_response.get('dialogue')
                    ]
                }
            else:
                memory = {
                    "messages": [
                        {
                            "role": "user",
                            "content": json.dumps(intent_response.get('question'), ensure_ascii=False, indent=2)
                        }
                    ]
                }
        else:
            memory = {
                "messages": [
                    {
                        "role": "user",
                        "content": json.dumps(res_content, ensure_ascii=False, indent=2)
                    }
                ]
            }

        logger.info("初始内存: {}".format(memory))

        intent_response['user_id'] = user_id

        # 将意图识别结果发送到规划智能体
        planner_response = self.planner.send_message(
            Message(role=MessageRole.INTENT_AGENT,
                    content=TextContent(text=json.dumps(intent_response, ensure_ascii=False, indent=2)),
                    conversation_id=conversation_id)
        )
        planner_info = json.loads(planner_response.content.text)
        logger.info(f"规划智能体返回结果: {planner_info}")
        outline = planner_info.get("outline_plan", [])
        scene_name = planner_info.get("scene_name", "")
        scene_type = str(planner_info.get("scene_type", ""))
        # 判断是否为空列表或者为空
        if outline == [] or (isinstance(outline, dict) and not outline):
            yield f"data: {json.dumps({'type': '系统', 'role': '系统', 'content': '该问题可能存在乱码、无意义、缺少信息或无工具完成的情况，规划智能体无法生成有效大纲，无法继续执行任务。', 'conversation_id': conversation_id})}\n\n"
            yield f"data: {json.dumps({'type': '[DONE]', 'conversation_id': conversation_id, 'finished': True})}\n\n"
            return
        yield f"data: {json.dumps({'type': 'agent', 'role': '规划智能体', 'content': outline, 'scene_name': scene_name, 'scene_type': scene_type, 'conversation_id': conversation_id})}\n\n"

        tmp_memory = {}
        for index in range(len(outline)):
            step_info = f"执行第{index + 1}步: {outline[index]}"
            yield f"data: {json.dumps({'type': '系统', 'role': '系统', 'content': step_info, 'conversation_id': conversation_id})}\n\n"

            memory["messages"].append({
                "role": "user",
                "content": json.dumps(outline[index], ensure_ascii=False, indent=2)
            })

            # 开始新的协调循环：Acting智能体 + 反馈智能体
            step_count = 0
            max_steps_per_outline = 10  # 每个大纲步骤的最大执行次数

            while (not any(term in (memory["messages"][-1]["content"] or "") for term in self.terminate)
                   and memory["messages"][-1]["content"]
                   and step_count < max_steps_per_outline):

                step_count += 1
                logger.info(f"执行第{index + 1}步的第{step_count}轮Acting-Feedback循环")
                # agent_name, confidence = self.router.route_query(memory["messages"][-1]["content"])
                # actor = self.network.get_agent(agent_name)
                # 1. 调用Acting智能体（整合思考和执行）

                # 判断该使用哪个Acting智能体
                # agent_name, confidence = self.router.route_query(memory["messages"][-1]["content"])
                # acting_agent = self.network.get_agent(agent_name)

                # 如果第一步则role为planner，后续为reflection
                role = MessageRole.PLANNER_AGENT if step_count == 1 else MessageRole.REFLECTION_AGENT
                acting_result = acting_agent.send_message(Message(
                    role=role,
                    content=TextContent(text=json.dumps(memory, ensure_ascii=False, indent=2)),
                    conversation_id=conversation_id
                ))

                acting_result = json.loads(acting_result.content.text)
                acting_memory = acting_result.get("memory", [])
                acting_status = acting_result.get("status", "unknown")
                llm_output = acting_result.get("llm_output", "")

                # 输出Acting智能体的结果
                if llm_output:
                    yield f"data: {json.dumps({'type': 'agent', 'role': '执行智能体', 'content': str(llm_output), 'conversation_id': conversation_id}, ensure_ascii=False)}\n\n"

                # 显示新增的工具执行结果
                new_messages = [msg for msg in acting_memory if msg.get("role") == "tool"]
                if "last_shown_tool_messages" not in tmp_memory:
                    tmp_memory["last_shown_tool_messages"] = []

                for msg in new_messages:
                    if msg not in tmp_memory["last_shown_tool_messages"]:
                        if msg.get("content"):
                            content = msg.get("content").replace("terminate", "").strip()
                            tool_name = msg.get("name", "未知工具")
                            # yield f"data: {json.dumps({'type': 'agent', 'role': f'工具执行({tool_name})', 'content': content, 'conversation_id': conversation_id}, ensure_ascii=False)}\n\n"
                        tmp_memory["last_shown_tool_messages"].append(msg)

                # 更新内存为Acting智能体的输出
                memory["messages"] = acting_memory

                # 2. 调用反馈智能体进行分析和决策
                feedback_payload = {
                    "memory": acting_memory,
                    "acting_status": acting_status
                }

                feedback_result = self.feedback_agent.send_message(Message(
                    role=MessageRole.ACTOR_AGENT,
                    content=TextContent(text=json.dumps(feedback_payload, ensure_ascii=False, indent=2)),
                    conversation_id=conversation_id
                ))

                feedback_result = json.loads(feedback_result.content.text)
                feedback_content = feedback_result.get("feedback", "")
                should_terminate = feedback_result.get("should_terminate", False)
                action_status = feedback_result.get("action_status", False)
                logger.info("反馈智能体返回结果: {}".format(feedback_result))

                # 如果反馈结果与之前的结果有两次相同，说明反馈智能体可能陷入循环无法执行，输出该任务无法继续执行，仅提取feedback字段进行重复检测
                if "last_feedback" not in tmp_memory:
                    tmp_memory["last_feedback"] = ""
                    tmp_memory["same_feedback_count"] = 0
                if feedback_content == tmp_memory["last_feedback"]:
                    tmp_memory["same_feedback_count"] += 1
                else:
                    tmp_memory["same_feedback_count"] = 0

                tmp_memory["last_feedback"] = feedback_content

                if tmp_memory["same_feedback_count"] >= 1:
                    should_terminate = True

                # 输出反馈智能体的分析
                if feedback_content:
                    yield f"data: {json.dumps({'type': 'agent', 'role': '反馈智能体', 'content': str({'执行是否正确': action_status, '反馈建议': feedback_content}), 'conversation_id': conversation_id}, ensure_ascii=False)}\n\n"
                # 更新内存为反馈智能体的输出
                memory["messages"].append({"role": "user", "content": feedback_content})

                # 3. 根据反馈决定是否继续
                if should_terminate or any(term in feedback_content.lower() for term in self.terminate):
                    logger.info(f"反馈智能体建议终止，结束第{index + 1}步的执行")
                    # yield f"data: {json.dumps({'type': '系统', 'role': '系统', 'content': f'第{index + 1}步已执行完成，无需下一步操作', 'conversation_id': conversation_id}, ensure_ascii=False)}\n\n"
                    break

                # 检查是否达到最大步骤数
                if step_count >= max_steps_per_outline:
                    if index != len(outline) - 1:
                        yield f"data: {json.dumps({'type': '系统', 'role': '系统', 'content': f'第{index + 1}步已达到最大执行轮数，继续下一步', 'conversation_id': conversation_id}, ensure_ascii=False)}\n\n"
                    else:
                        yield f"data: {json.dumps({'type': '系统', 'role': '系统', 'content': f'第{index + 1}步已达到最大执行轮数，且为最后一步，结束执行', 'conversation_id': conversation_id}, ensure_ascii=False)}\n\n"
                    break

        # 发送会话结束信号
        yield f"data: {json.dumps({'type': '[DONE]', 'conversation_id': conversation_id, 'finished': True}, ensure_ascii=False)}\n\n"


# ===== DeerFlow mobile integration: begin =====
# 说明：
# 1. 原 /query 接口保持不变，继续服务“事件处置”。
# 2. 新增 /query/deerflow 接口，专门服务“城市超脑平台”。
# 3. 默认优先以 SSE 流式方式调用 B 侧 DeerFlow mobile adapter：B 每返回一个 segment，A 侧就立即转成移动侧 SSE 卡片事件。
# 4. 如果未配置 DEERFLOW_API_URL，则返回 mock segments，用于验证前端展示链路。
# 5. 如果需要临时回退到非流式调用，可设置 DEERFLOW_STREAM=false。
# 6. 本文件以“移动方最新版本”的 /query 底层逻辑为基线，只额外加入城市超脑平台专用逻辑。

_ALLOWED_DEERFLOW_ROLES = {
    "意图识别智能体",
    "规划智能体",
    "系统",
    "执行智能体",
    "反馈智能体",
}

# DeerFlow manual_ui 模式使用的固定测试身份。这里故意只接受身份键，
# 场景、角色和组织信息均由 OneCityAgent 后端生成，不能由浏览器任意拼装。
_DEERFLOW_COMPLIANCE_PROFILES = {
    "ordinary_user": {
        "scene": "self_use",
        "role": "user",
        "org_id": "personal",
    },
    "internal_operator": {
        "scene": "internal_org",
        "role": "internal_operator",
        "org_id": "city-governance",
    },
    "cross_org_operator": {
        "scene": "cross_org",
        "role": "cross_org_operator",
        "org_id": "partner-org",
    },
    "publisher": {
        "scene": "public_release",
        "role": "publisher",
        "org_id": "publicity",
    },
    "researcher": {
        "scene": "research_anon",
        "role": "researcher",
        "org_id": "research",
    },
}


def _build_deerflow_compliance_profile(identity):
    """Build the exact manual profile accepted by DeerFlow's server whitelist."""
    identity_key = str(identity or "ordinary_user").strip()
    profile = _DEERFLOW_COMPLIANCE_PROFILES.get(identity_key)
    if profile is None:
        raise ValueError(f"不支持的合规身份: {identity_key}")

    user_context = {
        "user_id": f"manual-{identity_key}",
        "roles": [profile["role"]],
        "org_id": profile["org_id"],
    }
    compliance_context = {
        "source": "manual_ui",
        "enabled": True,
        "identity": identity_key,
        "scene": profile["scene"],
        "user_context": user_context,
    }
    return user_context, compliance_context


def _build_sse_payload(role, content, conversation_id, status=None, metadata=None):
    """把 DeerFlow segment 转成移动侧前端已支持的 SSE payload。"""
    event_type = "系统" if role == "系统" else "agent"
    payload = {
        "type": event_type,
        "role": role,
        "content": content,
        "conversation_id": conversation_id,
    }
    if status:
        payload["status"] = status
    if metadata:
        payload["metadata"] = metadata
    return payload


def _yield_sse(payload):
    """统一输出移动侧 SSE data 行。"""
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"


def _yield_sse_comment(content):
    """输出 SSE 注释行，用于 heartbeat/keepalive。"""
    safe_content = str(content).replace("\n", " ").strip()
    return f": {safe_content}\n\n"


def _get_deerflow_public_base_url():
    """获取附件下载链接的公开前缀；未配置时不改写相对链接。"""
    public_base = os.environ.get("DEERFLOW_PUBLIC_BASE_URL", "").strip().rstrip("/")
    if public_base:
        return public_base
    return ""


def _rewrite_artifact_url(url):
    """把 B 侧返回的 /api/threads/... 相对链接按需改写为完整链接。"""
    if not isinstance(url, str) or not url:
        return url
    if not url.startswith("/api/"):
        return url
    public_base = _get_deerflow_public_base_url()
    if not public_base:
        return url
    return public_base + url


def _rewrite_artifact_links_in_text(content):
    """按需把文本里的 /api/threads/... 相对下载链接改为完整链接。"""
    if not isinstance(content, str):
        return content
    public_base = _get_deerflow_public_base_url()
    if not public_base or "/api/threads/" not in content:
        return content
    # 常见 content 里每个下载链接都是独立一行，因此按空白边界做保守替换。
    return content.replace(" /api/threads/", f" {public_base}/api/threads/").replace("\n  /api/threads/", f"\n  {public_base}/api/threads/")


def _rewrite_artifact_links_in_metadata(metadata):
    """按需改写 metadata.files[].artifact_url。"""
    if not isinstance(metadata, dict):
        return metadata
    files = metadata.get("files")
    if isinstance(files, list):
        for item in files:
            if isinstance(item, dict) and "artifact_url" in item:
                item["artifact_url"] = _rewrite_artifact_url(item.get("artifact_url"))
    return metadata



def _derive_deerflow_url_from_run(suffix):
    """基于 DEERFLOW_API_URL 推导同一 DeerFlow 服务下的其他接口地址。"""
    api_url = os.environ.get("DEERFLOW_API_URL", "").strip()
    if not api_url:
        return ""

    parsed = urlparse(api_url)
    if not parsed.scheme or not parsed.netloc:
        return ""

    base = f"{parsed.scheme}://{parsed.netloc}"
    return f"{base}/{suffix.lstrip('/')}"



def _normalize_deerflow_thread_id(raw_thread_id=None, conversation_id=None):
    """
    将 A 侧城市超脑会话 ID 稳定映射为 UUID。

    说明：新版链路优先使用 B 侧 ensure 接口返回的真实 thread_id；
    这个函数只作为上传 URL 等兜底场景使用，不能替代 ensure。
    """
    raw_value = str(raw_thread_id or conversation_id or "").strip()

    if raw_value:
        try:
            return str(uuid.UUID(raw_value))
        except Exception:
            return str(uuid.uuid5(uuid.NAMESPACE_URL, f"onecityagent-citybrain-thread:{raw_value}"))

    return str(uuid.uuid4())


_DEERFLOW_THREAD_CACHE = {}


def _is_uuid_like(value):
    """判断一个值是否是 DeerFlow/LangGraph 可用的 UUID thread_id。"""
    try:
        uuid.UUID(str(value or "").strip())
        return True
    except Exception:
        return False


def _cache_deerflow_thread_id(conversation_id, thread_id):
    """在 A 侧进程内缓存移动端会话与 DeerFlow 真实 thread_id 的绑定。"""
    safe_conversation_id = str(conversation_id or "").strip()
    safe_thread_id = str(thread_id or "").strip()
    if safe_conversation_id and _is_uuid_like(safe_thread_id):
        _DEERFLOW_THREAD_CACHE[safe_conversation_id] = safe_thread_id


def _get_cached_deerflow_thread_id(conversation_id):
    safe_conversation_id = str(conversation_id or "").strip()
    cached = _DEERFLOW_THREAD_CACHE.get(safe_conversation_id)
    return cached if _is_uuid_like(cached) else ""


def _resolve_deerflow_thread(conversation_id, user_id=None, explicit_thread_id=None, source="citybrain"):
    """
    统一解析当前移动端会话应该使用的 DeerFlow 真实 thread_id。

    优先级：
    1. A 侧已缓存的 conversation_id -> thread_id 绑定，防止同一会话中途切到新 thread；
    2. 前端显式传回的真实 UUID thread_id，通常来自上一轮 start 事件或上传返回；
    3. B 侧 /api/deerflow/mobile/threads/ensure 返回的真实 thread_id。

    这样可以保证：同一个 OneCityAgent 对话框内，多轮问答、上传文件、继续分析都稳定落到同一个 DeerFlow thread。
    """
    safe_conversation_id = str(conversation_id or "").strip() or uuid.uuid4().hex
    explicit = str(explicit_thread_id or "").strip()

    cached_thread_id = _get_cached_deerflow_thread_id(safe_conversation_id)
    if cached_thread_id:
        if _is_uuid_like(explicit) and explicit != cached_thread_id:
            logger.warning(
                f"忽略同一 conversation_id 下不一致的显式 DeerFlow thread_id: "
                f"conversation_id={safe_conversation_id}, cached={cached_thread_id}, explicit={explicit}"
            )
        return {
            "conversation_id": safe_conversation_id,
            "thread_id": cached_thread_id,
            "assistant_id": None,
            "created": False,
            "source": "a_side_cache",
        }

    if _is_uuid_like(explicit):
        _cache_deerflow_thread_id(safe_conversation_id, explicit)
        return {
            "conversation_id": safe_conversation_id,
            "thread_id": explicit,
            "assistant_id": None,
            "created": False,
            "source": "frontend_explicit_thread_id",
        }

    thread_info = _ensure_deerflow_thread(safe_conversation_id, user_id=user_id, source=source)
    _cache_deerflow_thread_id(safe_conversation_id, thread_info.get("thread_id"))
    thread_info["source"] = "deerflow_ensure"
    return thread_info



def _get_deerflow_thread_ensure_url():
    """获取 B 侧 DeerFlow mobile 真实 thread 确保接口。"""
    explicit = os.environ.get("DEERFLOW_THREAD_ENSURE_URL", "").strip()
    if explicit:
        return explicit
    return _derive_deerflow_url_from_run("api/deerflow/mobile/threads/ensure")


def _ensure_deerflow_thread(conversation_id, user_id=None, source="citybrain"):
    """
    确保当前 citybrain 会话对应一个真实存在的 DeerFlow/LangGraph thread。

    注意：
    - 不再由 A 侧自己生成 UUID 当 DeerFlow thread_id；
    - 必须调用 B 侧 /api/deerflow/mobile/threads/ensure；
    - 返回的 thread_id 才能用于 /api/threads/{thread_id}/uploads 和 /api/deerflow/mobile/run。
    """
    ensure_url = _get_deerflow_thread_ensure_url()
    safe_conversation_id = str(conversation_id or "").strip() or uuid.uuid4().hex
    payload = {
        "conversation_id": safe_conversation_id,
        "source": source,
        "user_id": str(user_id or "public"),
    }

    timeout = int(os.environ.get("DEERFLOW_ENSURE_TIMEOUT", "120"))
    logger.info(f"确保 DeerFlow 真实 thread: url={ensure_url}, conversation_id={safe_conversation_id}, user_id={payload['user_id']}")

    response = requests.post(
        ensure_url,
        json=payload,
        timeout=(10, timeout),
    )
    response.raise_for_status()

    try:
        raw_data = response.json()
    except Exception as e:
        raise RuntimeError(f"DeerFlow ensure thread 接口未返回 JSON: {str(e)}")

    data = raw_data.get("data") if isinstance(raw_data, dict) else None
    if not isinstance(data, dict):
        data = raw_data if isinstance(raw_data, dict) else {}

    thread_id = data.get("thread_id")
    if not thread_id and isinstance(raw_data, dict):
        thread_id = raw_data.get("thread_id")
    assistant_id = data.get("assistant_id") if isinstance(data, dict) else None
    created = data.get("created") if isinstance(data, dict) else None

    if not thread_id:
        raise RuntimeError(f"DeerFlow ensure thread 接口未返回 thread_id: {raw_data}")

    logger.info(
        f"DeerFlow 真实 thread 已确认: conversation_id={safe_conversation_id}, "
        f"thread_id={thread_id}, assistant_id={assistant_id}, created={created}"
    )

    return {
        "conversation_id": safe_conversation_id,
        "thread_id": str(thread_id),
        "assistant_id": assistant_id,
        "created": created,
        "raw": raw_data,
    }


def _get_deerflow_thread_upload_url(thread_id):
    """DeerFlow 原生线程级文件上传接口：/api/threads/{thread_id}/uploads。"""
    explicit = os.environ.get("DEERFLOW_THREAD_UPLOAD_URL", "").strip()
    safe_thread_id = _normalize_deerflow_thread_id(thread_id, thread_id)
    if explicit:
        return explicit.format(thread_id=safe_thread_id)
    return _derive_deerflow_url_from_run(f"api/threads/{safe_thread_id}/uploads")


def _get_deerflow_public_base_url():
    """获取浏览器可访问的 DeerFlow 公网基地址。"""
    explicit = os.environ.get("DEERFLOW_PUBLIC_BASE_URL", "").strip().rstrip("/")
    if explicit:
        return explicit

    api_url = os.environ.get("DEERFLOW_API_URL", "").strip()
    parsed = urlparse(api_url) if api_url else None
    if parsed and parsed.scheme and parsed.netloc:
        host = parsed.hostname or ""
        if host.startswith("127.") or host == "localhost" or host.startswith("172.") or host.startswith("10."):
            return ""
        return f"{parsed.scheme}://{parsed.netloc}"

    return ""


def _normalize_deerflow_artifact_url(url_value):
    """把 B 侧返回的相对 artifact_url 转成可给浏览器点击的完整 URL。"""
    if not isinstance(url_value, str) or not url_value:
        return ""
    if url_value.startswith("http://") or url_value.startswith("https://"):
        return url_value
    if url_value.startswith("/"):
        return _get_deerflow_public_base_url() + url_value
    return url_value


def _get_deerflow_internal_file_url(file_url):
    """把浏览器可访问的 DeerFlow 文件 URL 转成 A 容器内可访问的 URL。"""
    if not isinstance(file_url, str) or not file_url:
        return file_url

    public_url = _normalize_deerflow_artifact_url(file_url)
    internal_base = os.environ.get("DEERFLOW_FILE_INTERNAL_BASE", "").strip().rstrip("/")
    parsed_file_url = urlparse(public_url)

    if internal_base and parsed_file_url.path:
        return internal_base + parsed_file_url.path + (f"?{parsed_file_url.query}" if parsed_file_url.query else "")

    api_url = os.environ.get("DEERFLOW_API_URL", "").strip()
    parsed_api_url = urlparse(api_url) if api_url else None
    if parsed_api_url and parsed_api_url.scheme and parsed_api_url.netloc and parsed_file_url.path:
        return f"{parsed_api_url.scheme}://{parsed_api_url.netloc}{parsed_file_url.path}" + (f"?{parsed_file_url.query}" if parsed_file_url.query else "")

    return public_url


def _get_file_extension(filename_or_url):
    raw = str(filename_or_url or "").split("?")[0].split("#")[0]
    name = raw.rsplit("/", 1)[-1]
    if "." not in name:
        return ""
    return name.rsplit(".", 1)[-1].lower()


def _normalize_uploaded_files_response(raw_data, thread_id=None):
    """兼容 DeerFlow 原生线程上传返回结构，统一提取 files。"""
    if not isinstance(raw_data, dict):
        return []

    files = raw_data.get("files")
    if not isinstance(files, list) and isinstance(raw_data.get("data"), dict):
        files = raw_data.get("data", {}).get("files")
    if not isinstance(files, list):
        return []

    normalized = []
    for item in files:
        if not isinstance(item, dict):
            continue

        artifact_url_raw = item.get("artifact_url") or item.get("url") or ""
        markdown_artifact_url_raw = item.get("markdown_artifact_url") or ""
        artifact_url_public = _normalize_deerflow_artifact_url(artifact_url_raw)
        markdown_artifact_url_public = _normalize_deerflow_artifact_url(markdown_artifact_url_raw)
        filename = item.get("filename") or item.get("original_filename") or ""
        markdown_file = item.get("markdown_file") or ""

        normalized.append({
            "filename": filename,
            "original_filename": item.get("original_filename") or filename,
            "size": item.get("size"),
            "mime_type": item.get("mime_type") or item.get("content_type") or "",
            "path": item.get("path") or "",
            "virtual_path": item.get("virtual_path") or "",
            "artifact_url": artifact_url_raw,
            "markdown_file": markdown_file,
            "markdown_path": item.get("markdown_path") or "",
            "markdown_virtual_path": item.get("markdown_virtual_path") or "",
            "markdown_artifact_url": markdown_artifact_url_raw,
            "url": artifact_url_public,
            "preview_url": markdown_artifact_url_public or artifact_url_public,
            "thread_id": thread_id or item.get("thread_id") or "",
        })
    return normalized


def _normalize_attachments_param(raw_value):
    """解析前端通过 query 参数传来的 attachments JSON，保留 B 侧需要的字段。"""
    if not raw_value:
        return []
    try:
        attachments = json.loads(raw_value)
    except Exception as e:
        logger.warning(f"attachments 参数不是合法 JSON，已忽略: {e}")
        return []

    if not isinstance(attachments, list):
        return []

    normalized = []
    for item in attachments:
        if not isinstance(item, dict):
            continue

        filename = item.get("filename") or item.get("original_filename") or ""
        normalized_item = {
            "filename": filename,
            "size": item.get("size"),
            "virtual_path": item.get("virtual_path") or "",
            "artifact_url": item.get("artifact_url") or item.get("url") or "",
            "markdown_virtual_path": item.get("markdown_virtual_path") or "",
            "markdown_artifact_url": item.get("markdown_artifact_url") or item.get("preview_url") or "",
        }

        if item.get("mime_type"):
            normalized_item["mime_type"] = item.get("mime_type")
        if item.get("path"):
            normalized_item["path"] = item.get("path")
        if item.get("markdown_path"):
            normalized_item["markdown_path"] = item.get("markdown_path")
        if item.get("thread_id"):
            normalized_item["thread_id"] = item.get("thread_id")
        if item.get("file_id"):
            normalized_item["file_id"] = item.get("file_id")

        if any(normalized_item.values()):
            normalized.append(normalized_item)

    return normalized


def _attach_files_to_payload(payload, attachments, thread_id=None):
    """按 B 侧最新约定把上传文件信息放入 attachments，并显式携带 thread_id。"""
    if thread_id:
        payload["thread_id"] = thread_id
    if attachments:
        payload["attachments"] = attachments
    return payload


def _mock_deerflow_segments(query):
    """没有真实 DeerFlow adapter 时，用固定 mock 数据验证链路。"""
    return [
        {
            "agent_name": "意图识别智能体",
            "content": (
                "路由任务：城市超脑平台已接收用户问题。\n"
                f"用户问题：{query}\n"
                "识别场景：DeerFlow 集成测试；场景模式：multi；"
                "候选场景：phone_network, policy_regulation。"
            ),
            "status": "completed",
        },
        {
            "agent_name": "规划智能体",
            "content": [
                "解析用户问题并确定需要调用 DeerFlow",
                "由 DeerFlow 侧完成意图识别、任务规划、Skill/RAG/工具调用",
                "接收 DeerFlow adapter 返回的 ordered segments",
                "将 segments 转换为移动侧 SSE 智能体卡片格式",
            ],
            "status": "completed",
        },
        {
            "agent_name": "系统",
            "content": "执行第1步：调用 DeerFlow adapter，获取意图识别和规划结果。",
            "status": "running",
        },
        {
            "agent_name": "执行智能体",
            "content": "模拟执行：已从 DeerFlow adapter 获取意图识别结果和任务规划结果。",
            "status": "completed",
        },
        {
            "agent_name": "反馈智能体",
            "content": str({
                "执行是否正确": True,
                "反馈建议": "第1步完成，继续模拟 Skill/RAG 执行过程。"
            }),
            "status": "completed",
        },
        {
            "agent_name": "系统",
            "content": "执行第2步：模拟 DeerFlow 调用电话网络分析 Skill 和法规检索 Skill。",
            "status": "running",
        },
        {
            "agent_name": "执行智能体",
            "content": (
                "模拟执行：已完成重点号码筛选、异常通信特征分析、政策法规检索，"
                "并整理为移动侧可展示的执行摘要。"
            ),
            "status": "completed",
        },
        {
            "agent_name": "反馈智能体",
            "content": str({
                "执行是否正确": True,
                "反馈建议": "第2步完成，DeerFlow mock segments 已按顺序返回。"
            }),
            "status": "completed",
        },
        {
            "agent_name": "系统",
            "content": "执行第3步：汇总 DeerFlow 输出，生成最终反馈。",
            "status": "running",
        },
        {
            "agent_name": "执行智能体",
            "content": "模拟执行：已完成 DeerFlow 结果汇总，并准备结束城市超脑平台任务。",
            "status": "completed",
        },
        {
            "agent_name": "反馈智能体",
            "content": str({
                "执行是否正确": True,
                "反馈建议": "城市超脑平台 mock 链路已跑通。后续可将 mock 替换为真实 DeerFlow adapter 返回的 segments。"
            }),
            "status": "completed",
        },
    ]


def _extract_deerflow_segments(raw_data):
    """兼容 B 侧 DeerFlow adapter 可能返回的几种非流式外层结构。"""
    if isinstance(raw_data, list):
        return raw_data

    if not isinstance(raw_data, dict):
        return []

    for key in ("ordered_segments", "segments", "events", "messages", "data", "result"):
        value = raw_data.get(key)
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            nested = _extract_deerflow_segments(value)
            if nested:
                return nested
    return []


def _segment_sort_key(item):
    """优先按 order 排序；没有 order 的保持相对靠后。"""
    if isinstance(item, dict):
        order = item.get("order")
        try:
            return int(order)
        except (TypeError, ValueError):
            return 10 ** 9
    return 10 ** 9


def _normalize_single_deerflow_segment(item, query=None):
    """把 B 侧单条 segment 统一成 A 侧移动端可展示的 segment。"""
    if not isinstance(item, dict):
        return {
            "agent_name": "执行智能体",
            "content": str(item),
            "status": "completed",
            "metadata": {"source": "deerflow"},
        }

    # 兼容 event: segment 的 data 外层可能包一层 segment/data。
    if isinstance(item.get("segment"), dict):
        item = item.get("segment")
    elif isinstance(item.get("data"), dict) and not (item.get("role") or item.get("agent_name")):
        item = item.get("data")

    role = item.get("agent_name") or item.get("role") or item.get("agent") or item.get("type") or "执行智能体"
    if role == "agent":
        role = "执行智能体"
    if role not in _ALLOWED_DEERFLOW_ROLES:
        logger.warning(f"DeerFlow segment role 不在移动侧允许范围内: {role}，已降级为执行智能体")
        role = "执行智能体"

    content = _rewrite_artifact_links_in_text(item.get("content", ""))
    metadata = item.get("metadata") or {}
    if isinstance(metadata, dict):
        metadata.setdefault("source", "deerflow")
        metadata = _rewrite_artifact_links_in_metadata(metadata)

    return {
        "agent_name": role,
        "content": content,
        "status": item.get("status") or "completed",
        "metadata": metadata,
    }


def _normalize_deerflow_segments(raw_data, query):
    """兼容 B 侧 DeerFlow adapter 非流式返回的几种格式，统一成 ordered segments。"""
    if raw_data is None:
        return _mock_deerflow_segments(query)

    segments = _extract_deerflow_segments(raw_data)
    if not isinstance(segments, list) or not segments:
        logger.warning("DeerFlow adapter 未返回有效 segments，使用 mock segments 兜底")
        return _mock_deerflow_segments(query)

    segments = sorted(segments, key=_segment_sort_key)
    return [_normalize_single_deerflow_segment(item, query=query) for item in segments]


def _load_deerflow_segments(
    query,
    conversation_id,
    user_id,
    attachments=None,
    thread_id=None,
    compliance_identity="ordinary_user",
):
    """非流式兜底调用；默认 /query/deerflow 优先使用 _stream_deerflow_sse_events。"""
    api_url = os.environ.get("DEERFLOW_API_URL", "").strip()
    timeout = int(os.environ.get("DEERFLOW_TIMEOUT", "1800"))

    if not api_url:
        logger.info("未配置 DEERFLOW_API_URL，/query/deerflow 使用 mock segments")
        return _mock_deerflow_segments(query)

    identity_context, compliance_context = _build_deerflow_compliance_profile(compliance_identity)
    payload = _attach_files_to_payload({
        "query": query,
        "conversation_id": conversation_id,
        "session_id": conversation_id,
        "user_id": user_id,
        "stream": False,
        "compliance_identity": identity_context,
        "compliance_context": compliance_context,
    }, attachments or [], thread_id or conversation_id)
    logger.info(f"调用 DeerFlow adapter 非流式接口: {api_url}, conversation_id={conversation_id}, user_id={user_id}")
    response = requests.post(api_url, json=payload, timeout=(10, timeout))
    response.raise_for_status()
    return _normalize_deerflow_segments(response.json(), query)


def _yield_mock_deerflow_events(query, conversation_id):
    """未配置真实 DeerFlow 时输出 mock SSE。"""
    yield _yield_sse({
        "type": "系统",
        "role": "系统",
        "content": "未配置 DEERFLOW_API_URL，当前使用城市超脑 mock 数据。",
        "conversation_id": conversation_id,
        "status": "running",
    })
    for seg in _mock_deerflow_segments(query):
        role = seg.get("agent_name") or seg.get("role") or "执行智能体"
        yield _yield_sse(_build_sse_payload(
            role=role,
            content=seg.get("content", ""),
            conversation_id=conversation_id,
            status=seg.get("status"),
            metadata=seg.get("metadata"),
        ))
        time.sleep(0.01)
    yield _yield_sse({
        "type": "[DONE]",
        "conversation_id": conversation_id,
        "finished": True,
    })


def _stream_deerflow_sse_events(
    query,
    conversation_id,
    user_id,
    attachments=None,
    thread_id=None,
    compliance_identity="ordinary_user",
):
    """调用 B 侧 DeerFlow SSE 流式接口，并实时转成移动侧前端可消费的 SSE。"""
    api_url = os.environ.get("DEERFLOW_API_URL", "").strip()
    timeout = int(os.environ.get("DEERFLOW_TIMEOUT", "1800"))

    if not api_url:
        logger.info("未配置 DEERFLOW_API_URL，/query/deerflow 使用 mock segments")
        yield from _yield_mock_deerflow_events(query, conversation_id)
        return

    identity_context, compliance_context = _build_deerflow_compliance_profile(compliance_identity)
    payload = _attach_files_to_payload({
        "query": query,
        "conversation_id": conversation_id,
        "session_id": conversation_id,
        "user_id": user_id,
        "stream": True,
        "compliance_identity": identity_context,
        "compliance_context": compliance_context,
    }, attachments or [], thread_id or conversation_id)

    logger.info(
        f"调用 DeerFlow adapter 流式接口: {api_url}, "
        f"conversation_id={conversation_id}, thread_id={payload.get('thread_id')}, "
        f"user_id={user_id}, compliance_identity={compliance_identity}, "
        f"compliance_scene={compliance_context.get('scene')}, "
        f"attachments={len(payload.get('attachments', []))}"
    )

    yield _yield_sse({
        "type": "系统",
        "role": "系统",
        "content": "城市超脑平台已接收请求，正在调用 DeerFlow 流式分析链路。",
        "conversation_id": conversation_id,
        "status": "running",
    })

    current_event = None
    saw_done = False

    with requests.post(
        api_url,
        json=payload,
        stream=True,
        timeout=(10, timeout),
    ) as resp:
        resp.raise_for_status()

        for line in resp.iter_lines(decode_unicode=True):
            if line is None:
                continue

            line = line.strip()
            if not line:
                continue

            # B 侧如果直接输出 SSE 注释行，A 侧原样转换为注释行，保证链路活跃。
            if line.startswith(":"):
                yield _yield_sse_comment(line[1:].strip())
                continue

            if line.startswith("event:"):
                current_event = line[len("event:"):].strip()
                continue

            if not line.startswith("data:"):
                continue

            raw_data = line[len("data:"):].strip()

            # DeerFlow / LangGraph 常见结束标记：data: [DONE]
            # 这不是普通文本，不能展示到前端；应转换为 A 侧标准 DONE 事件。
            if raw_data in {"[DONE]", "DONE"}:
                saw_done = True
                yield _yield_sse({
                    "type": "[DONE]",
                    "conversation_id": conversation_id,
                    "finished": True,
                })
                break

            try:
                data = json.loads(raw_data) if raw_data else {}
            except Exception as e:
                logger.warning(f"DeerFlow stream data 不是合法 JSON: {raw_data[:200]}, err={e}")
                yield _yield_sse({
                    "type": "系统",
                    "role": "系统",
                    "content": f"收到 DeerFlow 非 JSON 流式数据：{raw_data[:200]}",
                    "conversation_id": conversation_id,
                    "status": "running",
                })
                current_event = None
                continue

            if current_event == "accepted":
                yield _yield_sse({
                    "type": "系统",
                    "role": "系统",
                    "content": "DeerFlow 已接收任务，正在排队或准备执行。",
                    "conversation_id": conversation_id,
                    "status": data.get("status") or "running",
                    "metadata": {"source": "deerflow", "source_event": "accepted"},
                })
                current_event = None
                continue

            if current_event == "start":
                metadata = {"source": "deerflow", "source_event": "start"}
                if isinstance(data, dict) and data.get("thread_id"):
                    metadata["thread_id"] = data.get("thread_id")
                yield _yield_sse({
                    "type": "系统",
                    "role": "系统",
                    "content": f"DeerFlow 已启动真实分析任务。thread_id={data.get('thread_id', '')}".strip(),
                    "conversation_id": conversation_id,
                    "status": data.get("status") or "running",
                    "metadata": metadata,
                })
                current_event = None
                continue

            if current_event == "heartbeat":
                thread_id = data.get("thread_id") if isinstance(data, dict) else None
                comment = f"deerflow heartbeat thread_id={thread_id}" if thread_id else "deerflow heartbeat"
                yield _yield_sse_comment(comment)
                current_event = None
                continue

            if current_event == "compliance":
                yield _yield_sse({
                    "type": "compliance",
                    "role": "合规检测",
                    "content": "",
                    "conversation_id": conversation_id,
                    "status": "completed",
                    "compliance_result": data,
                    "metadata": {
                        "source": "deerflow",
                        "source_event": "compliance",
                    },
                })
                current_event = None
                continue

            if current_event == "segment" or (
                isinstance(data, dict) and (data.get("role") or data.get("agent_name") or data.get("segment"))
            ):
                seg = _normalize_single_deerflow_segment(data, query=query)
                role = seg.get("agent_name") or "执行智能体"
                yield _yield_sse(_build_sse_payload(
                    role=role,
                    content=seg.get("content", ""),
                    conversation_id=conversation_id,
                    status=seg.get("status"),
                    metadata=seg.get("metadata"),
                ))
                current_event = None
                continue

            if current_event == "done":
                saw_done = True
                yield _yield_sse({
                    "type": "[DONE]",
                    "conversation_id": conversation_id,
                    "finished": True,
                    "thread_id": data.get("thread_id") if isinstance(data, dict) else None,
                    "compliance_results": data.get("compliance_results", []) if isinstance(data, dict) else [],
                })
                break

            if current_event == "error":
                message = data.get("message") or data.get("error") or data.get("content") or str(data)
                yield _yield_sse({
                    "type": "error",
                    "role": "系统",
                    "content": f"DeerFlow 执行失败：{message}",
                    "conversation_id": conversation_id,
                })
                yield _yield_sse({
                    "type": "[DONE]",
                    "conversation_id": conversation_id,
                    "finished": False,
                })
                saw_done = True
                break

            current_event = None

    if not saw_done:
        yield _yield_sse({
            "type": "[DONE]",
            "conversation_id": conversation_id,
            "finished": True,
        })




@app.route('/query/deerflow/upload', methods=['POST'])
def upload_deerflow_files():
    """城市超脑平台文件上传代理：A 侧接收文件后转发到 DeerFlow 原生线程级上传接口。"""
    raw_thread_param = request.args.get('thread_id') or request.form.get('thread_id') or ''
    raw_conversation_id = (
        request.args.get('conversation_id')
        or request.form.get('conversation_id')
        or (raw_thread_param if raw_thread_param and not _is_uuid_like(raw_thread_param) else '')
        or uuid.uuid4().hex
    )
    explicit_thread_id = raw_thread_param if _is_uuid_like(raw_thread_param) else ''
    user_id = request.args.get('user_id') or request.form.get('user_id') or 'public'
    thread_info = _resolve_deerflow_thread(raw_conversation_id, user_id=user_id, explicit_thread_id=explicit_thread_id)
    conversation_id = thread_info["conversation_id"]
    thread_id = thread_info["thread_id"]
    upload_url = _get_deerflow_thread_upload_url(thread_id)
    uploaded_files = request.files.getlist('files')

    if not uploaded_files:
        return jsonify({"code": 400, "message": "未收到上传文件", "data": {"files": []}}), 400

    multipart_files = []
    try:
        for file_storage in uploaded_files:
            multipart_files.append((
                'files',
                (
                    file_storage.filename,
                    file_storage.stream,
                    file_storage.mimetype or 'application/octet-stream',
                )
            ))

        logger.info(f"转发城市超脑上传文件到 DeerFlow: {upload_url}, thread_id={thread_id}, files={len(multipart_files)}")
        response = requests.post(
            upload_url,
            files=multipart_files,
            timeout=(10, int(os.environ.get("DEERFLOW_UPLOAD_TIMEOUT", "600"))),
        )
        response.raise_for_status()

        try:
            raw_data = response.json()
        except Exception:
            logger.warning("DeerFlow 上传接口未返回 JSON")
            return jsonify({"code": 502, "message": "DeerFlow 上传接口返回格式不是 JSON", "data": {"files": [], "thread_id": thread_id}}), 502

        files = _normalize_uploaded_files_response(raw_data, thread_id=thread_id)
        return jsonify({
            "code": 0,
            "message": "success",
            "data": {
                "files": files,
                "conversation_id": conversation_id,
                "thread_id": thread_id,
                "assistant_id": thread_info.get("assistant_id"),
                "created": thread_info.get("created"),
            }
        })
    except Exception as e:
        logger.exception(f"上传文件到 DeerFlow 失败: {str(e)}")
        return jsonify({"code": 500, "message": f"上传文件到 DeerFlow 失败: {str(e)}", "data": {"files": [], "thread_id": thread_id}}), 500


@app.route('/query/deerflow/file/preview', methods=['GET'])
def preview_deerflow_file():
    """城市超脑平台结果文件预览代理：md/txt/csv 预览；xlsx 第一阶段提示下载。"""
    file_url = request.args.get('url', '').strip()
    filename = request.args.get('filename', '').strip()

    if not file_url:
        return jsonify({"code": 400, "message": "缺少文件 URL", "data": {}}), 400

    public_url = _normalize_deerflow_artifact_url(file_url)
    ext = _get_file_extension(filename or public_url)

    if ext in {"xlsx", "xls"}:
        return jsonify({
            "code": 0,
            "message": "success",
            "data": {
                "filename": filename or public_url.rsplit('/', 1)[-1],
                "file_type": ext,
                "preview_type": "unsupported",
                "content": "Excel 文件暂不支持在线预览，请下载后查看。",
                "truncated": False,
                "url": public_url,
            }
        })

    if ext not in {"md", "txt", "csv", "json", "log"}:
        return jsonify({
            "code": 0,
            "message": "success",
            "data": {
                "filename": filename or public_url.rsplit('/', 1)[-1],
                "file_type": ext or "unknown",
                "preview_type": "unsupported",
                "content": "该类型文件暂不支持在线预览，请下载后查看。",
                "truncated": False,
                "url": public_url,
            }
        })

    internal_url = _get_deerflow_internal_file_url(public_url)
    max_chars = int(os.environ.get("DEERFLOW_PREVIEW_MAX_CHARS", "200000"))

    try:
        logger.info(f"预览 DeerFlow 文件: public_url={public_url}, internal_url={internal_url}")
        response = requests.get(
            internal_url,
            timeout=(10, int(os.environ.get("DEERFLOW_PREVIEW_TIMEOUT", "120"))),
        )
        response.raise_for_status()

        content_type = response.headers.get('Content-Type', '')
        if 'application/json' in content_type:
            try:
                data = response.json()
                if isinstance(data, dict) and isinstance(data.get('data'), dict) and 'content' in data.get('data', {}):
                    return jsonify(data)
            except Exception:
                pass

        response.encoding = response.encoding or 'utf-8'
        content = response.text
        truncated = False
        if len(content) > max_chars:
            content = content[:max_chars] + "\n\n……内容过长，已截断预览，请下载完整文件查看。"
            truncated = True

        preview_type = "markdown" if ext == "md" else "csv" if ext == "csv" else "text"
        return jsonify({
            "code": 0,
            "message": "success",
            "data": {
                "filename": filename or public_url.rsplit('/', 1)[-1],
                "file_type": ext,
                "preview_type": preview_type,
                "content": content,
                "truncated": truncated,
                "url": public_url,
            }
        })
    except Exception as e:
        logger.exception(f"预览 DeerFlow 文件失败: {str(e)}")
        return jsonify({"code": 500, "message": f"预览文件失败: {str(e)}", "data": {"url": public_url}}), 500

@app.route('/query/deerflow', methods=['GET'])
@swag_from({
    'tags': ['Query'],
    'description': '城市超脑平台专用查询接口。接收 query 后调用 DeerFlow adapter，并返回移动侧兼容的 SSE 事件流。',
    'parameters': [
        {
            'name': 'query',
            'in': 'query',
            'type': 'string',
            'required': True,
            'description': '查询内容'
        },
        {
            'name': 'conversation_id',
            'in': 'query',
            'type': 'string',
            'required': False,
            'description': '会话ID'
        },
        {
            'name': 'user_id',
            'in': 'query',
            'type': 'string',
            'required': False,
            'description': '用户ID'
        }
    ],
    'responses': {
        200: {
            'description': 'SSE事件流',
        },
        400: {
            'description': '缺少查询参数',
        }
    }
})
def query_deerflow_sse():
    logger.info(f"[DeerFlow SSE] request args: {request.args}")
    query = request.args.get('query')
    raw_thread_param = request.args.get('thread_id') or ''
    conversation_id = (
        request.args.get('conversation_id')
        or (raw_thread_param if raw_thread_param and not _is_uuid_like(raw_thread_param) else '')
        or uuid.uuid4().hex
    )
    user_id = request.args.get('user_id', 'public')
    compliance_identity = request.args.get('compliance_identity', 'ordinary_user')
    try:
        _build_deerflow_compliance_profile(compliance_identity)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    explicit_thread_id = raw_thread_param if _is_uuid_like(raw_thread_param) else ''
    thread_info = _resolve_deerflow_thread(conversation_id, user_id=user_id, explicit_thread_id=explicit_thread_id)
    conversation_id = thread_info["conversation_id"]
    thread_id = thread_info["thread_id"]
    attachments = _normalize_attachments_param(request.args.get('attachments', ''))

    if not query:
        return jsonify({"error": "缺少查询参数"}), 400

    def generate():
        try:
            use_stream = os.environ.get("DEERFLOW_STREAM", "true").strip().lower() not in {"0", "false", "no"}
            if use_stream:
                for event in _stream_deerflow_sse_events(
                    query,
                    conversation_id,
                    user_id,
                    attachments,
                    thread_id,
                    compliance_identity,
                ):
                    yield event
            else:
                segments = _load_deerflow_segments(
                    query,
                    conversation_id,
                    user_id,
                    attachments,
                    thread_id,
                    compliance_identity,
                )
                for seg in segments:
                    role = seg.get("agent_name") or seg.get("role") or "执行智能体"
                    if role not in _ALLOWED_DEERFLOW_ROLES:
                        role = "执行智能体"
                    payload = _build_sse_payload(
                        role=role,
                        content=seg.get("content", ""),
                        conversation_id=conversation_id,
                        status=seg.get("status"),
                        metadata=seg.get("metadata"),
                    )
                    yield _yield_sse(payload)
                    time.sleep(0.01)

                yield _yield_sse({
                    "type": "[DONE]",
                    "conversation_id": conversation_id,
                    "finished": True,
                })
        except Exception as e:
            logger.exception(f"调用 DeerFlow 或生成 SSE 时出错: {str(e)}")
            yield _yield_sse({
                "type": "error",
                "role": "系统",
                "content": f"调用城市超脑平台失败：{str(e)}",
                "conversation_id": conversation_id,
            })
            yield _yield_sse({
                "type": "[DONE]",
                "conversation_id": conversation_id,
                "finished": False,
            })

    response = Response(generate(), mimetype='text/event-stream')
    response.headers['Cache-Control'] = 'no-cache'
    response.headers['X-Accel-Buffering'] = 'no'
    return response

# ===== DeerFlow mobile integration: end =====


@app.route('/query', methods=['GET'])
@swag_from({
    'tags': ['Query'],
    'description': '处理查询请求并返回SSE事件流',
    'parameters': [
        {
            'name': 'query',
            'in': 'query',
            'type': 'string',
            'required': True,
            'description': '查询内容'
        },
        {
            'name': 'conversation_id',
            'in': 'query',
            'type': 'string',
            'required': False,
            'description': '会话ID'
        },
        {
            'name': 'user_id',
            'in': 'query',
            'type': 'string',
            'required': True,
            'description': '用户ID'
        }
    ],
    'responses': {
        200: {
            'description': 'SSE事件流',
        },
        400: {
            'description': '缺少查询参数',
        }
    }
})
def query_sse():
    logger.info(request.args)
    query = request.args.get('query')
    conversation_id = request.args.get('conversation_id', uuid.uuid4().hex)
    user_id = request.args.get('user_id', 'public')
    if not query:
        return jsonify({"error": "缺少查询参数"}), 400

    def generate():
        try:
            local_scheduler = Scheduler()
            for event in local_scheduler.intent_query_route_stream(query, conversation_id=conversation_id,
                                                                   user_id=user_id):
                yield event
                time.sleep(0.01)
        except Exception as e:
            logger.error(f"处理查询时出错: {str(e)}")
            yield f"data: {json.dumps({'type': 'error', 'role': '系统', 'content': '执行中遇到网络波动无法成功调用智能体，请进行对MCP和智能体进行重载或联系管理员'})}\n\n"

    return Response(generate(), mimetype='text/event-stream')


@app.route('/a2a_reload', methods=['GET'])
@swag_from({
    'tags': ['Reload'],
    'description': '重载执行智能体Agent网络配置',
    'responses': {
        200: {
            'description': '重载成功',
        },
        500: {
            'description': '重载失败',
        }
    }
})
def a2a_reload():
    try:
        Scheduler.reload_network()
        return jsonify({"code": 0, "data": {"message": "Agent network reloaded."}, "message": "success"}), 200
    except Exception as e:
        logger.error(f"重载 agent network 失败: {str(e)}")
        return jsonify({"code": 500, "data": {}, "message": f"Failed to reload agent network: {str(e)}"}), 500


if __name__ == '__main__':
    port = get_port_from_url(config.a2a_config.router)
    app.run(host='0.0.0.0', port=port, threaded=True)
