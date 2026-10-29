#!/usr/bin/env python3 
# -*- coding: utf-8 -*- 
# @Time : 2025/7/14 10:24 
# @Author : Shang Shuhan 
# @File : a2a_intent.py


import asyncio
from collections import defaultdict
from typing import Dict, Any, List
from flask import request, jsonify
from flask.json.provider import DefaultJSONProvider

from python_a2a import A2AServer, run_server, AgentCard, AgentSkill, TaskStatus, TaskState
from python_a2a.server.http import create_flask_app

# 添加项目根目录到Python路径
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from onecityagent import config
from onecityagent.config import IntentConfig, get_port_from_url
from onecityagent.service.intent_service.intention_multi_conversation import Intent
from onecityagent.service.llm_service.llm import LLM
from onecityagent.schema import Message as MessageSchema
from onecityagent.logger import logger
from onecityagent.service.llm_service import TenantLLMService

agent_url = config.a2a_config.intent
agent_card = AgentCard(
    name="意图识别智能体",
    description="一个意图识别 A2A 服务，识别当前意图并将关键参数返回给客户端",
    url=agent_url,
    version="1.0.0",
    skills=[
        AgentSkill(
            name="意图识别",
            description="一个意图识别 A2A 服务，识别当前意图并将关键参数",
            examples=["西安的经纬度是多少？"]
        )
    ]
)

# 定义一个自定义的JSON提供者，关闭ensure_ascii选项
class NoUnicodeEscapeJSONProvider(DefaultJSONProvider):
    # 关键：关闭 ensure_ascii
    ensure_ascii = False

class IntentAgent(A2AServer):
    def __init__(self):
        super().__init__(agent_card=agent_card)
        self.llm = LLM("default")
        self.intent = Intent(IntentConfig.load_all_scene_configs(), self.llm)
        # 会话级隔离：conversation_id -> {dialogue, questions}
        self._sessions: Dict[str, Dict[str, List]] = defaultdict(lambda: {"dialogue": [], "questions": []})
        self._lock = asyncio.Lock()

    def add_custom_routes(self, app):
        """添加自定义路由"""
        @app.route("/update_llm_config", methods=["POST"])
        def update_llm_config():
            """自定义接口：更新 LLM 配置"""
            try:
                models = TenantLLMService.get_by_status_on()
                if not models[0]:
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
                    config.update_config(new_llm_config)
                    logger.info(f"更新 Intent LLM 配置{new_llm_config}")
                # 更新 LLM 配置
                self.llm = LLM(config_name="default", llm_config=config.llm)
                self.intent = Intent(IntentConfig.load_all_scene_configs(), self.llm)
                return jsonify({"code": 0, "data": {"message": f"LLM 配置已更新"}, "message": "success"}), 200
            except Exception as e:
                return jsonify({"code": 500, "data": {"message": str(e)}, "message": "failure"}), 500


    # ---------- 内部工具 ----------
    async def _rewrite_question(self, questions: List[str]) -> str:
        """把多轮问题合并、重写为一句话"""
        if len(questions) == 1:
            return questions[0]
        prompt = (
            "请根据以下内容，以第一人称总结，不要遗漏信息，确保后续模型可以执行该任务。\n"
            + "\n".join(questions)
        )
        return await self.llm.ask(
            messages=[MessageSchema.user_message("\n".join(questions))],
            system_msgs=[MessageSchema.system_message(prompt)]
        )

    # ---------- 主处理 ----------
    async def _process(self, conversation_id: str, question: str) -> Dict[str, Any]:
        """真正处理单个会话的意图识别"""
        session = self._sessions[conversation_id]
        session["dialogue"].append({"role": "user", "content": question})
        session["questions"].append(question)
        # 调用意图识别
        response = await self.intent.process_multi_question(question, session)

        # 如果是最终 dict 结果
        if isinstance(response, dict):
            self.intent.current_purpose = ""
            rewrite = await self._rewrite_question(session["questions"])
            response.update({
                "question": rewrite,
                "dialogue": session["dialogue"]
            })
            # 该会话结束，清空数据
            del self._sessions[conversation_id]
            return response

        # 否则是追问，保存 assistant 回复
        session["dialogue"].append({"role": "assistant", "content": response})
        return response

    def handle_task(self, task):
        return asyncio.run(self.handle_task_async(task))

    async def handle_task_async(self, task):
        # 提取参数
        message_data = task.message or {}
        logger.info(f"收到A2A消息: {message_data}")
        content = message_data.get("content", {})
        conversation_id = message_data.get("conversation_id", "")
        question = content.get("text", "") if isinstance(content, dict) else ""

        if not question:
            task.status = TaskStatus(state=TaskState.FAILED)
            return task

        response = await self._process(conversation_id, question)

        task.artifacts = [{"parts": [{"type": "text", "text": response}]}]
        task.status = TaskStatus(state=TaskState.COMPLETED)
        return task


if __name__ == "__main__":
    port = get_port_from_url(agent_url)
    agent = IntentAgent()
    app = create_flask_app(agent)
    app.json = NoUnicodeEscapeJSONProvider(app)
    agent.add_custom_routes(app)  # 添加自定义路由
    app.run(host="0.0.0.0", port=port)

