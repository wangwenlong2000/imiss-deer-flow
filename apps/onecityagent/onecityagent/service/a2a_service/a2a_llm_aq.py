from flask import Flask, jsonify
from flask.json.provider import DefaultJSONProvider
from python_a2a.server.http import create_flask_app

from onecityagent.service.llm_service import TenantLLMService
from onecityagent.config import config, get_port_from_url
from onecityagent.logger import logger
from python_a2a import AgentCard, AgentSkill, A2AServer, TextContent, MessageRole, TaskStatus, TaskState, Message
from onecityagent.service.llm_service.a2a_llm_openai import OpenAIA2AServer

class NoUnicodeEscapeJSONProvider(DefaultJSONProvider):
    ensure_ascii = False

class OpenAIAgent(A2AServer):
    name: str = "大模型问答助手"
    description: str = "大模型问答助手"

    def __init__(self, agent_card):
        super().__init__(agent_card=agent_card)
        self.openai_server = OpenAIA2AServer(
                            api_key=config.llm["default"].api_key,
                            base_url=config.llm["default"].base_url,
                            model=config.llm["default"].model,
                            temperature=config.llm["default"].temperature,
                            system_prompt="你是一个有帮助的AI助手，提供准确、简洁和有用的信息。"
    )

    def add_custom_llm_routes(self, app):
        @app.route("/update_llm_config", methods=["POST"])
        def update_llm_config():
            try:
                models = TenantLLMService.get_by_status_on()
                if not models or not models[0]:
                    return jsonify({"error": "缺少llm更新配置"}), 400
                new_llm_config = {"llm":
                    {
                        "default": {
                            "model": models[0]["model"],
                            "base_url": models[0]["base_url"],
                            "api_key": models[0]["api_key"],
                            "temperature": models[0].get("temperature", 0.7),
                        }
                    }
                }
                config.update_config(new_llm_config)
                logger.info(f"更新 LLM 配置: {new_llm_config}")
                self.openai_server = OpenAIA2AServer(
                    api_key=config.llm["default"].api_key,
                    base_url=config.llm["default"].base_url,
                    model=config.llm["default"].model,
                    temperature=config.llm["default"].temperature,
                    system_prompt="你是一个有帮助的AI助手，提供准确、简洁和有用的信息。"
                )
                return jsonify({"code":0, "data":{"message": f"LLM 配置已更新"}, "message":"success"}), 200
            except Exception as e:
                return jsonify({"code": 500, "data": {"message": str(e)}, "message": "failure"}), 500

    def handle_task(self, task):
        message_data = task.message or {}

        # 将消息转换为Message对象
        if isinstance(message_data, dict):
            try:
                message = Message.from_dict(message_data)
            except:
                content = message_data.get("content", {})
                text = content.get("text", "") if isinstance(content, dict) else ""
                message = Message(content=TextContent(text=text), role=MessageRole.USER)
        else:
            message = message_data

        try:
            response = self.openai_server.handle_message(message)
            task.artifacts = [{
                "parts": [{"type": "text", "text": response.content.text}]
            }]
            task.status = TaskStatus(state=TaskState.COMPLETED)
        except Exception as e:
            logger.error(f"LLM处理错误: {e}")
            task.artifacts = [{
                "parts": [{"type": "text", "text": f"处理请求出错: {str(e)}"}]
            }]
            task.status = TaskStatus(state=TaskState.FAILED)
        return task

if __name__ == "__main__":
    prot = get_port_from_url(config.a2a_config.llm_qa)
    agent_card = AgentCard(
        name="大模型问答助手",
        description="大模型问答助手，支持通用问答、内容创作、问题解决",
        url=config.a2a_config.llm_qa,
        version="1.0.0",
        skills=[
            AgentSkill(name="通用问答", description="能够回答各种问题",
                       examples=["中国在什么大洲?", "美国第一任总统是谁?"]),
            AgentSkill(name="创作内容", description="帮助创作文章、诗歌等",
                       examples=["写一首关于秋天的短诗", "为咖啡店创作一个广告语"]),
            AgentSkill(name="问题解决", description="帮助解决问题并提供解决方案",
                       examples=["如何提高时间管理能力?", "学习新语言的好策略是什么?"])
        ]
    )
    agent = OpenAIAgent(agent_card)
    app = create_flask_app(agent)
    app.json = NoUnicodeEscapeJSONProvider(app)
    agent.add_custom_llm_routes(app)
    app.run(host="0.0.0.0", port=prot)