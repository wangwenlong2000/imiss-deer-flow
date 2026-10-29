# encoding=utf-8
from onecityagent.logger import logger

from onecityagent.service.intent_service.scene_processor import CommonProcessor
from onecityagent.service.intent_service.utils.data_format_utils import extract_continuous_digits, extract_float
from onecityagent.service.intent_service.utils.helpers import Intent_RELATED_INTENT_THRESHOLD
from onecityagent.schema import Message

class Intent:
    def __init__(self, scene_templates, llm):
        self.scene_templates = scene_templates
        self.current_purpose: str = ''
        self.processors = {}
        self.llm = llm


    async def is_related_to_last_intent(self, user_input):
        """
        判断当前输入是否与上一次意图场景相关
        """
        if not self.current_purpose:
            return False
        prompt = f"判断当前用户输入内容与当前对话场景的关联性:\n\n当前对话场景: {self.scene_templates[self.current_purpose]['description']}\n当前用户输入: {user_input}\n\n这两次输入是否关联（仅用小数回答关联度，得分范围0.0至1.0）"

        result = await self.llm.ask(messages=[Message.user_message(None)],
                                    system_msgs=([Message.system_message(prompt)]))

        return extract_float(result) > Intent_RELATED_INTENT_THRESHOLD

    async def recognize_intent(self, user_input):
        # 根据场景模板生成选项
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
            self.current_purpose = purpose_options[user_choices[0]]

        if self.current_purpose:
            print(f"用户选择了场景：{self.scene_templates[self.current_purpose]['name']}")
            # 这里可以继续处理其他逻辑
        else:
            # 用户输入的选项无效的情况，可以进行相应的处理
            print("无效的选项，请重新选择")

    def get_processor_for_scene(self, scene_name):
        if scene_name in self.processors:
            return self.processors[scene_name]

        scene_config = self.scene_templates.get(scene_name)
        if not scene_config:
            raise ValueError(f"未找到名为{scene_name}的场景配置")

        processor_class = CommonProcessor(scene_config, self.llm)
        self.processors[scene_name] = processor_class
        return self.processors[scene_name]

    async def process_multi_question(self, user_input):
        """
        处理多轮问答
        :param user_input:
        :return:
        """
        # 检查当前输入是否与上一次的意图场景相关
        if await self.is_related_to_last_intent(user_input):
            pass
        else:
            # 不相关时，重新识别意图
            await self.recognize_intent(user_input)
        logger.info('current_purpose: %s', self.current_purpose)

        if self.current_purpose in self.scene_templates:
            # 根据场景模板调用相应场景的处理逻辑
            self.get_processor_for_scene(self.current_purpose)
            # 调用抽象类process方法
            return await self.processors[self.current_purpose].process(user_input, None)
        return {"scene": "非内置方案"}

    async def run_intent_interactive(self, prompt=None):
        """Run the intent agent in interactive mode.
        
        Args:
            prompt (str, optional): The initial prompt from request. Defaults to None.
        
        Returns:
            dict: The intent recognition result.
        """
        try:
            # 每次新请求开始时重置当前意图和处理器
            self.current_purpose = ''
            self.processors = {}
            
            user_input = ""
            question_list = []
            
            # 如果提供了初始prompt，直接使用它
            if prompt and prompt.strip():
                user_input = prompt
                question_list.append(user_input)
                response = await self.process_multi_question(user_input)
                
                if isinstance(response, dict):
                    question_clips = ",".join(question_list)
                    prompt = f"请根据以下内容，以第一人称总结，不要遗漏信息，确保后续模型可以执行该任务。\n{question_clips}\n"
                    question = await self.llm.ask(messages=[Message.user_message(question_clips)],
                                                  system_msgs=([Message.system_message(prompt)]))

                    response["question"] = question
                    return response
                else:
                    # 如果需要更多信息，在Web服务环境中返回特殊标记
                    return {"scene": "需要更多信息", "params": response, "original_input": user_input}
            
            # 如果没有提供初始prompt，使用传统的交互模式
            while user_input.lower() not in ["exit", "quit", "q"]:
                user_input = input("\nEnter your request: ")
                question_list.append(user_input)
                response = await self.process_multi_question(user_input)
                if isinstance(response, dict):
                    question_clips = ",".join(question_list)
                    prompt = f"请根据以下内容，以第一人称总结，不要遗漏信息，确保后续模型可以执行该任务。\n{question_clips}\n"
                    question = await self.llm.ask(messages=[Message.user_message(question_clips)],
                                                  system_msgs=([Message.system_message(prompt)]))

                    response["question"] = question
                    print(f"\nAgent: {response}")
                    return response
                else:
                    question_list.append(response)
            return None
        except Exception as e:
            logger.error(f"意图识别失败: {str(e)}", exc_info=True)
            # 返回更明确的错误信息
            return {"scene": "意图识别失败", "params": str(e), "error": True}