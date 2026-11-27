"""Legacy event-response A2A workflow. Explicitly enabled by the app factory."""
from flask import Blueprint, request, Response, jsonify, make_response
import json
import uuid
import time
import os
import re
import requests
from datetime import datetime
from functools import wraps
from threading import Lock
from urllib.parse import quote, unquote, urlparse
from loguru import logger
from python_a2a import AgentNetwork, A2AClient, TextContent
from onecityagent.config import config, Config, get_port_from_url
from onecityagent.constants import StatusEnum
from onecityagent.utils.http_logging import PathOnlyRequestHandler
from onecityagent.models.db_models import DB, Dialog, Role, Tenant, User, UserTenant
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

app = Blueprint("legacy_a2a", __name__)


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


