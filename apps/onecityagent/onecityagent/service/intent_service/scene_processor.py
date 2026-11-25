# encoding=utf-8
import logging

from onecityagent.service.intent_service.utils.helpers import get_raw_slot, update_slot, format_name_value,format_name_value_for_logging, is_slot_fully_filled, \
    extract_json_from_string, get_dynamic_example
from onecityagent.service.intent_service.utils.prompt_utils import get_slot_update_message, get_slot_query_user_message
from onecityagent.schema import Message

class CommonProcessor():
    def __init__(self, scene_config, llm):
        parameters = scene_config["parameters"]
        self.scene_config = scene_config
        self.scene_name = scene_config["name"]
        self.slot_template = get_raw_slot(parameters)
        self.slot_dynamic_example = get_dynamic_example(scene_config)
        self.slot = get_raw_slot(parameters)
        self.llm = llm

    async def process(self, user_input, context):
        # 处理用户输入，更新槽位，检查完整性，以及与用户交互
        # 先检查本次用户输入是否有信息补充，保存补充后的结果   编写程序进行字符串value值diff对比，判断是否有更新

        message = get_slot_update_message(self.scene_name, self.slot_dynamic_example, self.slot_template,
                                          user_input)  # 优化封装一下 .format  入参只要填input
        new_info_json_raw = await self.llm.ask(messages=[Message.user_message(user_input)],
                                               system_msgs=([Message.system_message(message)]))
        current_values = extract_json_from_string(new_info_json_raw)
        logging.debug('current_values: %s', current_values)
        logging.debug('slot update before: %s', self.slot)
        update_slot(current_values, self.slot)
        logging.debug('slot update after: %s', self.slot)

        ## 修改代码，不进行多轮询问
        rtn_data = {}
        params = format_name_value(self.slot)
        rtn_data["params"] = {k: v for k, v in params.items() if v }
        rtn_data["scene"] = self.scene_name
        self.reset()
        return rtn_data
        # 判断参数是否已经全部补全
        # if is_slot_fully_filled(self.slot):
        #     rtn_data = {}
        #     rtn_data["params"] = format_name_value(self.slot)
        #     rtn_data["scene"] = self.scene_name
        #     self.reset()
        #     return rtn_data
        # else:
        #     message = get_slot_query_user_message(self.scene_name, self.slot, user_input)
        #     result = await self.llm.ask(messages=[Message.user_message(user_input)],
        #                                 system_msgs=([Message.system_message(message)]))
        #     return result

    def reset(self):
        """重置槽位"""
        self.slot = get_raw_slot(self.scene_config["parameters"])


