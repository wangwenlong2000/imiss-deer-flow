#!/usr/bin/env python3 
# -*- coding: utf-8 -*- 
# @Time : 2025/7/14 10:31 
# @Author : Shang Shuhan 
# @File : intention_multi_conversation.py

from onecityagent.logger import logger

from onecityagent.service.intent_service.scene_processor import CommonProcessor
from onecityagent.service.intent_service.utils.data_format_utils import extract_continuous_digits, extract_float
from onecityagent.service.intent_service.utils.helpers import Intent_RELATED_INTENT_THRESHOLD, OTHER_SCENE
from onecityagent.schema import Message


class Intent:
    def __init__(self, scene_templates, llm):
        self.scene_templates = scene_templates
        self.llm = llm
        self._processors = {}

    # 所有方法增加 session 参数
    async def is_related_to_last_intent(self, user_input, session):
        if not session.get("current_purpose"):
            session["current_purpose"] = 0
            return False
        prompt = (
            f"判断当前用户输入内容与当前对话场景的关联性:\n\n"
            f"当前对话场景: {self.scene_templates[session['current_purpose']]['description']}\n"
            f"当前用户输入: {user_input}\n\n"
            f"这两次输入是否关联（仅用小数回答关联度，得分范围0.0至1.0）"
        )
        result = await self.llm.ask(
            messages=[Message.user_message(None)],
            system_msgs=[Message.system_message(prompt)],
        )
        return extract_float(result) >= Intent_RELATED_INTENT_THRESHOLD

    async def recognize_intent(self, user_input, session):
        purpose_options = {}
        purpose_description = {}
        index = 1
        for template_key, template_info in self.scene_templates.items():
            purpose_options[str(index)] = template_key
            purpose_description[str(index)] = template_info["description"]
            index += 1
        options_prompt = "\n".join([f"{key}. {value} - 请回复{key}" for key, value in purpose_description.items()])
        options_prompt += "\n0. 其他场景 - 请回复0"

        user_choice = await self.llm.ask(messages=[Message.user_message(user_input)],
                                         system_msgs=([Message.system_message(f"有下面多种场景，需要你根据用户输入进行判断，只答选项\n{options_prompt}\n用户输入：{user_input}\n请回复序号：")]))

        logger.debug(f'purpose_options: %s', purpose_options)
        logger.debug(f'user_choice: %s', user_choice)

        user_choices = extract_continuous_digits(user_choice)
        # 根据用户选择获取对应场景
        if user_choices and user_choices[0] != '0':
            session["current_purpose"] = purpose_options[user_choices[0]]
        # else:
        #     session["current_purpose"] = 0
        if session["current_purpose"]:
            selected_purpose = session["current_purpose"]
            logger.info(f"用户选择了场景：{self.scene_templates[selected_purpose]['name']}")
            # 这里可以继续处理其他逻辑
        # else:
        #     # 用户输入的选项无效的情况，可以进行相应的处理
        #     logger.info("其他场景")

    def get_processor_for_scene(self, scene_name, session):
        # 缓存放在 Intent 实例里
        if scene_name in self._processors:
            return self._processors[scene_name]

        scene_config = self.scene_templates.get(scene_name)
        if not scene_config:
            raise ValueError(f"未找到名为{scene_name}的场景配置")

        processor = CommonProcessor(scene_config, self.llm)
        self._processors[scene_name] = processor
        return processor

    async def process_multi_question(self, user_input, session):
        # 使用 session 中的 current_purpose
        if await self.is_related_to_last_intent(user_input, session):
            pass
        else:
            await self.recognize_intent(user_input, session)
        current = session["current_purpose"]

        if current in self.scene_templates:
            processor = self.get_processor_for_scene(current, session)
            return await processor.process(user_input, None)
        return {"scene": OTHER_SCENE}
