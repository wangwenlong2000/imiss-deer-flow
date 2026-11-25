import re
import ast

from onecityagent.config import config
from onecityagent.exceptions import TokenLimitExceeded
from onecityagent.logger import logger
from onecityagent.schema import AgentState, Message, ToolChoice
from onecityagent.prompt import OUTLINE_PROMPT
from onecityagent.service.tool_service.ragflow import RAGFlowService, DatasetType


class Planner:
    def __init__(self, agent):
        self.agent = agent
        self.outline_plan_list = []

    @staticmethod
    def extract_steps(kg_plan):
        # Regular expression to find content between <|begin_step|> and <|end_step|>
        pattern = re.compile(r'<\|begin_step\|>(.*?)<\|end_step\|>', re.DOTALL)
        steps = pattern.findall(kg_plan)
        return steps

    # async def think(self) -> bool:
    #     """Process current state and decide next action."""
    #     if not self.agent.mcp_clients.sessions or not self.agent.mcp_clients.tool_map:
    #         logger.info("MCP service is no longer available, ending interaction")
    #         self.agent.state = AgentState.FINISHED
    #         return False
    #
    #     if self.agent.current_step % self.agent._refresh_tools_interval == 0:
    #         await self.agent._refresh_tools()
    #         if not self.agent.mcp_clients.tool_map:
    #             logger.info("MCP service has shut down, ending interaction")
    #             self.agent.state = AgentState.FINISHED
    #             return False
    #
    #     if self.agent.next_step_prompt:
    #         user_msg = Message.user_message(self.agent.next_step_prompt)
    #         self.agent.messages += [user_msg]
    #
    #     try:
    #         response = await self.agent.llm.ask_tool(
    #             messages=self.agent.messages,
    #             system_msgs=(
    #                 [Message.system_message(self.agent.system_prompt)]
    #                 if self.agent.system_prompt
    #                 else None
    #             ),
    #             tools=self.agent.available_tools.to_params(),
    #             tool_choice=self.agent.tool_choices,
    #         )
    #     except Exception as e:
    #         if hasattr(e, "__cause__") and isinstance(e.__cause__, TokenLimitExceeded):
    #             token_limit_error = e.__cause__
    #             logger.error(
    #                 f"🚨 Token limit error (from RetryError): {token_limit_error}"
    #             )
    #             self.agent.memory.add_message(
    #                 Message.assistant_message(
    #                     f"Maximum token limit reached, cannot continue execution: {str(token_limit_error)}"
    #                 )
    #             )
    #             self.agent.state = AgentState.FINISHED
    #             return False
    #         raise
    #
    #     self.agent.tool_calls = tool_calls = (
    #         response.tool_calls if response and response.tool_calls else []
    #     )
    #     content = response.content if response and response.content else ""
    #
    #     logger.info(f"✨ {self.agent.name}'s thoughts: {content}")
    #     logger.info(
    #         f"🛠️ {self.agent.name} selected {len(tool_calls) if tool_calls else 0} tools to use"
    #     )
    #     if tool_calls:
    #         logger.info(
    #             f"🧰 Tools being prepared: {[call.function.name for call in tool_calls]}"
    #         )
    #         logger.info(f"🔧 Tool arguments: {tool_calls[0].function.arguments}")
    #
    #     try:
    #         if response is None:
    #             raise RuntimeError("No response received from the LLM")
    #
    #         if self.agent.tool_choices == ToolChoice.NONE:
    #             if tool_calls:
    #                 logger.warning(
    #                     f"🤔 Hmm, {self.agent.name} tried to use tools when they weren't available!"
    #                 )
    #             if content:
    #                 self.agent.memory.add_message(Message.assistant_message(content))
    #                 return True
    #             return False
    #
    #         assistant_msg = (
    #             Message.from_tool_calls(content=content, tool_calls=self.agent.tool_calls)
    #             if self.agent.tool_calls
    #             else Message.assistant_message(content)
    #         )
    #         self.agent.memory.add_message(assistant_msg)
    #
    #         if self.agent.tool_choices == ToolChoice.REQUIRED and not self.agent.tool_calls:
    #             return True
    #
    #         if self.agent.tool_choices == ToolChoice.AUTO and not self.agent.tool_calls:
    #             return bool(content)
    #
    #         return bool(self.agent.tool_calls)
    #     except Exception as e:
    #         logger.error(f"🚨 Oops! The {self.agent.name}'s thinking process hit a snag: {e}")
    #         self.agent.memory.add_message(
    #             Message.assistant_message(
    #                 f"Error encountered while processing: {str(e)}"
    #             )
    #         )
    #         return False

    def is_stuck(self) -> bool:
        """Check if the agent is stuck in a loop by detecting duplicate content"""
        if len(self.agent.memory.messages) < 2:
            return False

        last_message = self.agent.memory.messages[-1]
        if not last_message.content:
            return False

        duplicate_count = sum(
            1
            for msg in reversed(self.agent.memory.messages[:-1])
            if msg.role == "assistant" and msg.content == last_message.content
        )

        return duplicate_count >= self.agent.duplicate_threshold

    def handle_stuck_state(self):
        """Handle stuck state by adding a prompt to change strategy"""
        stuck_prompt = "\
        无效循环状态已被检测到。观察到重复的响应。请考虑新的策略，避免重复尝试已被证明无效的路径。"
        self.agent.next_step_prompt = f"{stuck_prompt}\n{self.agent.next_step_prompt}"
        logger.warning(f"Agent detected stuck state. Added prompt: {stuck_prompt}")

    # 规划整体任务执行大纲，进行任务分解
    async def outline_plan(self, request) -> list:
        """
        根据 request 确定整体任务执行的大纲，再根据大纲进行任务分解执行。
        """
        outline_response = await self.agent.llm.ask(
            messages=[Message.user_message(request)],
            system_msgs=(
                [Message.system_message(OUTLINE_PROMPT)]
                if OUTLINE_PROMPT
                else None
            ),
        )
        outline_list = self.extract_steps(outline_response)
        logger.info(f"任务大纲：{outline_list}")
        self.outline_plan_list = outline_list
        return outline_list

    # 检索知识库内容中的任务大纲
    async def plan_kg(self, request, scene) -> list:
        """
        根据 request 确定整体任务执行的大纲，再根据大纲进行任务分解执行。
        """
        try:
            ragflow_service = RAGFlowService(DatasetType.PUBLIC)
            retrieval_result = await ragflow_service.call_retrieval(
                question=scene,
                sim_threshold=config.rag_config.public["similarity_threshold"]
            )
            retrieval_chunk = ast.literal_eval(retrieval_result)
            retrieval_chunk = retrieval_chunk.get("content", None)
            if retrieval_chunk:
                outline_response =retrieval_chunk
                logger.info("检索到知识库内容: {}".format(outline_response))
            else:
                outline_response = await self.agent.llm.ask(
                    messages=[Message.user_message(str(request))],
                    system_msgs=(
                        [Message.system_message(OUTLINE_PROMPT)]
                        if OUTLINE_PROMPT
                        else None
                    ),
                )
            outline_list = self.extract_steps(outline_response)
            logger.info(f"任务大纲：{outline_list}")
            self.outline_plan_list = outline_list
            return outline_list
        except Exception as e:
            logger.error(f"Error to connect to knowledge database mcp: {e}")
            outline_response = await self.agent.llm.ask(
                messages=[Message.user_message(request)],
                system_msgs=(
                    [Message.system_message(OUTLINE_PROMPT)]
                    if OUTLINE_PROMPT
                    else None
                ),
            )
            outline_list = self.extract_steps(outline_response)
            logger.info(f"任务大纲：{outline_list}")
            self.outline_plan_list = outline_list
            return outline_list

    # async def think_outline(self) -> bool:
    #     """Process current state and decide next action."""
    #     if not self.agent.mcp_clients.sessions or not self.agent.mcp_clients.tool_map:
    #         logger.info("MCP service is no longer available, ending interaction")
    #         self.agent.state = AgentState.FINISHED
    #         return False
    #
    #     if self.agent.current_step % self.agent._refresh_tools_interval == 0:
    #         await self.agent._refresh_tools()
    #         if not self.agent.mcp_clients.tool_map:
    #             logger.info("MCP service has shut down, ending interaction")
    #             self.agent.state = AgentState.FINISHED
    #             return False
    #
    #     if self.outline_plan_list[self.agent.current_step - 1]:
    #         user_msg = Message.user_message(self.outline_plan_list[self.agent.current_step - 1])
    #         self.agent.messages += [user_msg]
    #
    #     try:
    #         response = await self.agent.llm.ask_tool(
    #             messages=self.agent.messages,
    #             system_msgs=(
    #                 [Message.system_message(self.agent.system_prompt)]
    #                 if self.agent.system_prompt
    #                 else None
    #             ),
    #             tools=self.agent.available_tools.to_params(),
    #             tool_choice=self.agent.tool_choices,
    #         )
    #     except Exception as e:
    #         if hasattr(e, "__cause__") and isinstance(e.__cause__, TokenLimitExceeded):
    #             token_limit_error = e.__cause__
    #             logger.error(
    #                 f"🚨 Token limit error (from RetryError): {token_limit_error}"
    #             )
    #             self.agent.memory.add_message(
    #                 Message.assistant_message(
    #                     f"Maximum token limit reached, cannot continue execution: {str(token_limit_error)}"
    #                 )
    #             )
    #             self.agent.state = AgentState.FINISHED
    #             return False
    #         raise
    #
    #     self.agent.tool_calls = tool_calls = (
    #         response.tool_calls if response and response.tool_calls else []
    #     )
    #     content = response.content if response and response.content else ""
    #
    #     logger.info(f"{self.agent.name}'s thoughts: {content}")
    #     logger.info(
    #         f"{self.agent.name} selected {len(tool_calls) if tool_calls else 0} tools to use"
    #     )
    #     if tool_calls:
    #         logger.info(
    #             f"🧰 Tools being prepared: {[call.function.name for call in tool_calls]}"
    #         )
    #         logger.info(f"🔧 Tool arguments: {tool_calls[0].function.arguments}")
    #
    #     try:
    #         if response is None:
    #             raise RuntimeError("No response received from the LLM")
    #
    #         if self.agent.tool_choices == ToolChoice.NONE:
    #             if tool_calls:
    #                 logger.warning(
    #                     f"🤔 Hmm, {self.agent.name} tried to use tools when they weren't available!"
    #                 )
    #             if content:
    #                 self.agent.memory.add_message(Message.assistant_message(content))
    #                 return True
    #             return False
    #
    #         assistant_msg = (
    #             Message.from_tool_calls(content=content, tool_calls=self.agent.tool_calls)
    #             if self.agent.tool_calls
    #             else Message.assistant_message(content)
    #         )
    #         self.agent.memory.add_message(assistant_msg)
    #
    #         if self.agent.tool_choices == ToolChoice.REQUIRED and not self.agent.tool_calls:
    #             return True
    #
    #         if self.agent.tool_choices == ToolChoice.AUTO and not self.agent.tool_calls:
    #             return bool(content)
    #
    #         return bool(self.agent.tool_calls)
    #     except Exception as e:
    #         logger.error(f"🚨 Oops! The {self.agent.name}'s thinking process hit a snag: {e}")
    #         self.agent.memory.add_message(
    #             Message.assistant_message(
    #                 f"Error encountered while processing: {str(e)}"
    #             )
    #         )
    #         return False
