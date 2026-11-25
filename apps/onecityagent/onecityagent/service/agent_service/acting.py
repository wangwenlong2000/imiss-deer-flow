import json
from typing import Any

from onecityagent.logger import logger
from onecityagent.prompt import MCP_MULTIMEDIA_RESPONSE_PROMPT
from onecityagent.schema import ToolChoice, Message, ToolCall, AgentState
from onecityagent.service.tool_service.base import ToolResult


class Actor:
    def __init__(self, agent):
        self.agent = agent

    async def act(self) -> str:
        """Execute tool calls and handle their results"""
        if not self.agent.tool_calls:
            if self.agent.tool_choices == ToolChoice.REQUIRED:
                raise ValueError("Tool calls required but none provided")

            # Return last message content if no tool calls
            return self.agent.messages[-1].content or "No content or commands to execute"

        results = []
        for command in self.agent.tool_calls:
            self.agent._current_base64_image = None
            result = await self.execute_tool(command)

            if self.agent.max_observe:
                result = result[: self.agent.max_observe]

            logger.info(
                f"🎯 Tool '{command.function.name}' completed its mission! Result: {result}"
            )

            tool_msg = Message.tool_message(
                content=result,
                tool_call_id=command.id,
                name=command.function.name,
                base64_image=self.agent._current_base64_image,
            )
            self.agent.memory.add_message(tool_msg)
            results.append(result)

        return "\n\n".join(results)

    async def execute_tool(self, command: ToolCall) -> str:
        """Execute a single tool call with robust error handling"""
        if not command or not command.function or not command.function.name:
            return "Error: Invalid command format"

        name = command.function.name
        if name not in self.agent.available_tools.tool_map:
            return f"Error: Unknown tool '{name}'"

        try:
            args = json.loads(command.function.arguments or "{}")
            logger.info(f"🔧 Activating tool: '{name}'...")
            result = await self.agent.available_tools.execute(name=name, tool_input=args)
            await self._handle_special_tool(name=name, result=result)

            if hasattr(result, "base64_image") and result.base64_image:
                self.agent._current_base64_image = result.base64_image

            observation = (
                f"Observed output of cmd `{name}` executed:\n{str(result)}"
                if result
                else f"Cmd `{name}` completed with no output"
            )
            return observation
        except json.JSONDecodeError:
            error_msg = f"Error parsing arguments for {name}: Invalid JSON format"
            logger.error(
                f"📝 Oops! The arguments for '{name}' don't make sense - invalid JSON, arguments:{command.function.arguments}"
            )
            return f"Error: {error_msg}"
        except Exception as e:
            error_msg = f"⚠️ Tool '{name}' encountered a problem: {str(e)}"
            logger.exception(error_msg)
            return f"Error: {error_msg}"

    async def _handle_special_tool(self, name: str, result: Any, **kwargs) -> None:
        """Handle special tool execution and state changes"""
        if not self.agent._is_special_tool(name):
            return

        if self.agent._should_finish_execution(name=name, result=result, **kwargs):
            logger.info(f"🏁 Special tool '{name}' has completed the task!")
            self.agent.state = AgentState.FINISHED

        if isinstance(result, ToolResult) and result.base64_image:
            self.agent.memory.add_message(
                Message.system_message(
                    MCP_MULTIMEDIA_RESPONSE_PROMPT.format(tool_name=name)
                )
            )