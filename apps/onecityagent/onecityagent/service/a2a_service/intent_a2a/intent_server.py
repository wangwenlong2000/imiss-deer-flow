#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time : 2025/6/17 11:14
# @Author : Shang Shuhan
# @File : intent_server.py


import asyncio
from python_a2a import A2AServer, run_server, AgentCard, AgentSkill, TaskStatus, TaskState

from onecityagent.config import IntentConfig
from onecityagent.service.intent_service.intention import Intent
from onecityagent.service.llm_service.llm import LLM
from onecityagent.schema import Message as MessageSchema


agent_card = AgentCard(
    name="Echo Server",
    description="An intent A2A server that identify the current intention back to the client with key parameters",
    url=f"http://localhost:{5001}",
    version="1.0.0",
    skills=[
        AgentSkill(
            name="intent recognition",
            description="Identify the current intention and analyze key parameters",
            examples=["西安今天的天气怎么样"]
        )
    ]
)


# Create a custom A2A server
class IntentAgent(A2AServer):
    def __init__(self):
        # Initialize with our agent card
        super().__init__(agent_card=agent_card)
        self.llm = LLM("llm")
        self.intent = Intent(IntentConfig.load_all_scene_configs(), self.llm)
        self.question_list = []
        self.dialogue = []

    def handle_task(self, task):
        # Extract message text
        message_data = task.message or {}
        content = message_data.get("content", {})
        question = content.get("text", "") if isinstance(content, dict) else ""
        if question:
            self.question_list.append(question)
            self.dialogue.append({"role": "user", "content": question})
        response = asyncio.run(self.intent.process_multi_question(question))
        if isinstance(response, dict):
            self.intent.current_purpose = ""
            question_clips = ",".join(self.question_list)
            if len(self.question_list) == 1:
                rewrite_question = self.question_list[0]
            else:
                prompt = f"请根据以下内容，以第一人称总结，不要遗漏信息，确保后续模型可以执行该任务。\n{question_clips}\n"
                rewrite_question = asyncio.run(self.llm.ask(messages=[MessageSchema.user_message(question_clips)],
                                        system_msgs=([MessageSchema.system_message(prompt)])))
            self.question_list = []
            response["question"] = rewrite_question
            response["dialogue"] = self.dialogue
            self.dialogue = []
        else:
            self.dialogue.append({"role": "assistant", "content": response})

        # Create response artifact
        task.artifacts = [{
            "parts": [{"type": "text", "text": response}]
        }]

        task.status = TaskStatus(state=TaskState.COMPLETED)
        return task


if __name__ == "__main__":
    intent_agent = IntentAgent()
    run_server(intent_agent, host="0.0.0.0", port=5001)


