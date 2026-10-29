import json

from onecityagent.exceptions import TokenLimitExceeded
from onecityagent.logger import logger
from onecityagent.schema import AgentState, Message, ToolChoice


class Thinker:
    def __init__(self, agent, outline_plan_list=None):
        self.agent = agent
        self.outline_plan_list = outline_plan_list

    async def think(self) -> bool:
        """Process current state and decide next action."""
        if not await self._validate_mcp_service():
            return False

        if self.agent.next_step_prompt:
            self.agent.messages.append(Message.user_message(self.agent.next_step_prompt))

        response = await self._ask_tool_with_error_handling()
        if not response:
            return False

        self._log_tool_usage(response)
        return self._process_response(response)

    async def think_outline(self) -> bool:
        """Process current state and decide next action based on outline."""
        if not await self._validate_mcp_service():
            return False

        if self.outline_plan_list[self.agent.current_step - 1]:
            self.agent.messages.append(
                Message.user_message(self.outline_plan_list[self.agent.current_step - 1])
            )

        response = await self._ask_tool_with_error_handling()
        if not response:
            return False

        self._log_tool_usage(response)
        return self._process_response(response)

    async def think_kg(self) -> bool:
        """Process current state and decide next action based on outline."""
        if not await self._validate_mcp_service():
            return False

        idx = self.agent.current_step - 1 - self.agent.tools_num
        if (
            self.outline_plan_list
            and 0 <= idx < len(self.outline_plan_list)
            and self.outline_plan_list[idx]
        ):
            self.agent.messages.append(
                Message.user_message(self.outline_plan_list[idx])
            )

        response = await self._ask_tool_with_error_handling()
        if not response:
            return False

        self._log_tool_usage(response)
        return self._process_response(response)

    async def _validate_mcp_service(self) -> bool:
        """Validate MCP service availability."""
        if not self.agent.mcp_clients.sessions or not self.agent.mcp_clients.tool_map:
            logger.info("MCP service is no longer available, ending interaction")
            self.agent.state = AgentState.FINISHED
            return False

        if self.agent.current_step % self.agent._refresh_tools_interval == 0:
            await self.agent._refresh_tools()
            if not self.agent.mcp_clients.tool_map:
                logger.info("MCP service has shut down, ending interaction")
                self.agent.state = AgentState.FINISHED
                return False
        return True

    async def _ask_tool_with_error_handling(self):
        """Handle tool request and manage errors."""
        try:
            return await self.agent.llm.ask_tool(
                messages=self.agent.messages,
                system_msgs=(
                    [Message.system_message(self.agent.system_prompt)]
                    if self.agent.system_prompt
                    else None
                ),
                tools=self.agent.available_tools.to_params(),
                tool_choice=self.agent.tool_choices,
            )
        except Exception as e:
            if hasattr(e, "__cause__") and isinstance(e.__cause__, TokenLimitExceeded):
                token_limit_error = e.__cause__
                logger.error(f"🚨 Token limit error: {token_limit_error}")
                self.agent.memory.add_message(
                    Message.assistant_message(
                        f"Maximum token limit reached: {str(token_limit_error)}"
                    )
                )
                self.agent.state = AgentState.FINISHED
                return None
            raise

    def _log_tool_usage(self, response):
        """Log tool usage details."""
        self.agent.tool_calls = response.tool_calls or []
        content = response.content or ""

        logger.info(f"{self.agent.name}'s thoughts: {content}")
        logger.info(
            f"{self.agent.name} selected {len(self.agent.tool_calls)} tools to use"
        )
        if self.agent.tool_calls:
            logger.info(
                f"🧰 Tools: {[call.function.name for call in self.agent.tool_calls]}"
            )
            logger.info(f"🔧 Arguments: {json.loads(self.agent.tool_calls[0].function.arguments)}")
            # 对于每个工具调用进行计数
            self.agent.tools_num += 1

    def _process_response(self, response) -> bool:
        """Process the response and decide next action."""
        if response is None:
            raise RuntimeError("No response received from the LLM")

        content = response.content or ""
        assistant_msg = (
            Message.from_tool_calls(content=content, tool_calls=self.agent.tool_calls)
            if self.agent.tool_calls
            else Message.assistant_message(content)
        )
        self.agent.memory.add_message(assistant_msg)

        if self.agent.tool_choices == ToolChoice.NONE:
            if self.agent.tool_calls:
                logger.warning(
                    f"🤔 {self.agent.name} tried to use tools when unavailable!"
                )
            return bool(content)

        if self.agent.tool_choices in {ToolChoice.REQUIRED, ToolChoice.AUTO}:
            return bool(self.agent.tool_calls or content)

        return False
