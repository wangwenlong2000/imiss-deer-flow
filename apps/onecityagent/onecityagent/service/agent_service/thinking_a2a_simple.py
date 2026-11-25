#!/usr/bin/env python3 
# -*- coding: utf-8 -*- 
# @Time : 2025/7/10 16:19 
# @Author : Shang Shuhan 
# @File : thinking_a2a_simple.py

from python_a2a import AgentCard, AgentSkill, A2AServer, TaskStatus, TaskState, run_server
import asyncio
import json


from onecityagent.service.llm_service import LLM
from onecityagent.schema import Message as MessageSchema
from onecityagent.schema import Memory


agent_card = AgentCard(
    name="思考智能体",
    description="一个用于思考判断的智能体",
    url=f"http://localhost:5006",
    version="1.0.0",
    skills=[
        AgentSkill(
            name="思考",
            description="根据当前所有的步骤内容，判断是否合理",
            examples=[""]
        )
    ]
)

class Thinker(A2AServer):
    def __init__(self):
        super().__init__(agent_card=agent_card)
        self.llm = LLM(config_name="mcp_agent")
        self.judge_prompt = """根据提供的所有信息，判断最后一条信息内容是否合理且正确，给出是或否的结果，若结果为是，返回合理，无需给出原因，若结果为否，返回不合理并给出对应建议。"""


    def handle_task(self, task):
        message_data = task.message or {}
        content = message_data.get("content", {})
        all_message = content.get("text", "") if isinstance(content, dict) else ""
        all_message_dict = json.loads(all_message)
        memory = Memory.model_validate(all_message_dict)
        messages = memory.messages

        result = asyncio.run(self.llm.ask(messages=messages,
                                          system_msgs=([MessageSchema.system_message(self.judge_prompt)])))
        print(result)
        if result == "合理":
            response = {"result": True}
        else:
            response = {"result": False, "message": result}
        # response = result
        task.artifacts = [{
            "parts": [{"type": "text", "text": response}]
        }]
        task.status = TaskStatus(state=TaskState.COMPLETED)
        return task


if __name__ == "__main__":
    think_server = Thinker()
    run_server(think_server, port=5006, host="0.0.0.0")
