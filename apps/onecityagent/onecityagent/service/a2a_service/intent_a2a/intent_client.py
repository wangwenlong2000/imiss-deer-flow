#!/usr/bin/env python3 
# -*- coding: utf-8 -*- 
# @Time : 2025/6/18 10:43 
# @Author : Shang Shuhan 
# @File : intent_client.py
import uuid
from python_a2a import A2AClient, Message, TextContent, MessageRole

class IntentClient:
    def __init__(self, client_url="http://localhost:5001"):
        self.client = A2AClient(client_url)

    def get_intent_params(self, query, conversation_id=uuid.uuid4().hex):
        message = Message(
            content=TextContent(text=query),
            role=MessageRole.USER,
            conversation_id=conversation_id
        )
        response = self.client.send_message(message)
        return response.content.text

if __name__ == '__main__':
    # 多轮对话
    intent_client = IntentClient()
    # q0 = "2025-07-10西安国寿金融中心20楼以上发生了火灾，持续了半小时。"
    # rtn_data0 = intent_client.get_intent_params(q0)
    # print("rtn_data0", rtn_data0, type(rtn_data0 ))

    q0 = "自驾去西安怎么走"
    rtn_data0 = intent_client.get_intent_params(q0)
    print("rtn_data0", rtn_data0)
    q1 = "北京"
    conversation_id = 11
    rtn_data1 = intent_client.get_intent_params(q1)
    rtn_data0 = intent_client.get_intent_params(q0, conversation_id)
    print("rtn_data0", rtn_data0, type(rtn_data0 ))
    conversation_id = 12
    q2 = "2025-06-20上海天气怎么样"
    rtn_data2 = intent_client.get_intent_params(q2, conversation_id)
    print("rtn_data2", rtn_data2, type(rtn_data2))
    conversation_id = 13
    q1 = "起点是钟楼"
    rtn_data1 = intent_client.get_intent_params(q1, conversation_id)
    print("rtn_data1", rtn_data1, type(rtn_data1))

    conversation_id = 11
    q1 = "起点是北京"
    rtn_data1 = intent_client.get_intent_params(q1, conversation_id)
    print("rtn_data1", rtn_data1, type(rtn_data1))
    # q2 = "2025-06-20上海天气怎么样"
    # rtn_data2 = intent_client.get_intent_params(q2)
    # print("rtn_data2", rtn_data2, type(rtn_data2))
