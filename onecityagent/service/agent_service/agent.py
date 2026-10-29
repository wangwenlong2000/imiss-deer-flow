import ast
from abc import ABC
from contextlib import asynccontextmanager
from typing import List, Optional, Any, Dict, Tuple

from pydantic import BaseModel, Field, model_validator

from onecityagent.service.agent_service.thinking import Thinker
from onecityagent.service.llm_service.llm import LLM
from onecityagent.service.agent_service.planning import Planner
from onecityagent.service.intent_service.intention import Intent
from onecityagent.config import IntentConfig
from onecityagent.service.agent_service.acting import Actor
from onecityagent.logger import logger
from onecityagent.prompt import MCP_NEXT_STEP_PROMPT, MCP_SYSTEM_PROMPT, USER_PREFERENCES_PROMPT
from onecityagent.schema import ROLE_TYPE, AgentState, Memory, Message, TOOL_CHOICE_TYPE, ToolChoice
from onecityagent.service.tool_service.mcp_tool import MCPClients
from onecityagent.config import config
from onecityagent.service.tool_service.ragflow import RAGFlowService, DatasetType

class Function(BaseModel):
    name: str
    arguments: str

class ToolCall(BaseModel):
    """Represents a tool/function call in a message"""

    id: str
    type: str = "function"
    function: Function

class Agent(BaseModel, ABC):
    # Core attributes
    name: str = "mcp_agent"
    description: Optional[str] = "An agent that connects to an MCP server and uses its tools."

    # terminate signal
    terminate: Optional[list] = ["terminate", "终止"]  # Special tool name for termination

    # last question
    last_question: Optional[str] = "<last>"  # Last question asked by the user

    # Prompts
    system_prompt: Optional[str] = MCP_SYSTEM_PROMPT
    next_step_prompt: Optional[str] = MCP_NEXT_STEP_PROMPT

    # Dependencies
    llm: LLM = Field(default_factory=LLM, description="Language model instance")
    memory: Memory = Field(default_factory=Memory, description="Agent's memory store")
    state: AgentState = Field(
        default=AgentState.IDLE, description="Current agent state"
    )

    mcp_clients: MCPClients = Field(default_factory=MCPClients)
    available_tools: MCPClients = None  # Will be set in initialize()

    max_steps: int = 20
    max_observe: int = 10000
    tools_num: int = Field(default=0, description="Current step in execution")  # Count of tools used in the current session
    connection_type: str = "stdio"  # "stdio" or "sse"
    current_step: int = Field(default=0, description="Current step in execution")

    tool_schemas: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    _refresh_tools_interval: int = 10  # Refresh tools every N steps

    special_tool_names: List[str] = Field(default_factory=lambda: ["terminate"])
    tool_choices: TOOL_CHOICE_TYPE = ToolChoice.AUTO  # type: ignore

    duplicate_threshold: int = 2

    # Track connected MCP servers
    connected_servers: Dict[str, str] = Field(
        default_factory=dict
    )  # server_id -> url/command

    class Config:
        arbitrary_types_allowed = True
        extra = "allow"  # Allow extra fields for flexibility in subclasses

    @model_validator(mode="after")
    def initialize_agent(self):
        """Initialize agent with default settings if not provided."""
        if self.llm is None or not isinstance(self.llm, LLM):
            self.llm = LLM(config_name=self.name.lower())
        if not isinstance(self.memory, Memory):
            self.memory = Memory()

        self.intent = Intent(IntentConfig.load_all_scene_configs(), self.llm)
        self.planner = Planner(self)
        self.actor = Actor(self)
        self.thinker = Thinker(self)
        logger.info("Agent initialized with LLM and memory.")
        return self

    @asynccontextmanager
    async def state_context(self, new_state: AgentState):
        """Context manager for safe agent state transitions.

        Args:
            new_state: The state to transition to during the context.

        Yields:
            None: Allows execution within the new state.

        Raises:
            ValueError: If the new_state is invalid.
        """
        if not isinstance(new_state, AgentState):
            raise ValueError(f"Invalid state: {new_state}")

        previous_state = self.state
        self.state = new_state
        try:
            yield
        except Exception as e:
            self.state = AgentState.ERROR  # Transition to ERROR on failure
            raise e
        finally:
            self.state = previous_state  # Revert to previous state

    async def connect_mcp_server(
        self,
        server_url: str,
        server_id: str = "",
        use_stdio: bool = False,
        stdio_args: List[str] = None,
    ) -> None:
        """Connect to an MCP server and add its tools."""
        if use_stdio:
            await self.mcp_clients.connect_stdio(
                server_url, stdio_args or [], server_id
            )
            self.connected_servers[server_id or server_url] = server_url
        else:
            await self.mcp_clients.connect_sse(server_url, server_id)
            self.connected_servers[server_id or server_url] = server_url

        # Update available tools with only the new tools from this server
        new_tools = [
            tool for tool in self.mcp_clients.tools if tool.server_id == server_id
        ]
        self.mcp_clients.add_tools(*new_tools)

    async def initialize(
        self
    ) -> None:
        """Initialize connections to configured MCP servers."""
        for server_id, server_config in config.mcp_config.servers.items():
            try:
                if server_config.type == "sse":
                    if server_config.url:
                        await self.connect_mcp_server(server_config.url, server_id)
                        logger.info(
                            f"Connected to MCP server {server_id} at {server_config.url}"
                        )
                elif server_config.type == "stdio":
                    if server_config.command:
                        await self.connect_mcp_server(
                            server_config.command,
                            server_id,
                            use_stdio=True,
                            stdio_args=server_config.args,
                        )
                        logger.info(
                            f"Connected to MCP server {server_id} using command {server_config.command}"
                        )
            except Exception as e:
                logger.error(f"Failed to connect to MCP server {server_id}: {e}")
                continue
        # Set available_tools to our MCP instance
        self.available_tools = self.mcp_clients

        # Store initial tool schemas
        await self._refresh_tools()

        # Add system message about available tools
        tool_names = list(self.mcp_clients.tool_map.keys())
        tools_info = ", ".join(tool_names)

        # Add system prompt and available tools information
        self.memory.add_message(
            Message.system_message(
                f"{self.system_prompt}\n\nAvailable MCP tools: {tools_info}"
            )
        )

    async def step(self) -> str:
        """Execute a single step: think and act."""
        should_act = await self.thinker.think()
        if not should_act:
            return "Thinking complete - no action needed"
        return await self.actor.act()

    async def step_outline(self):
        # should_act = await self.planner.think_outline()
        self.thinker.outline_plan_list = self.planner.outline_plan_list
        should_act = await self.thinker.think_outline()
        if not should_act:
            return "Thinking complete - no action needed"
        return await self.actor.act()

    async def step_kg(self):
        # should_act = await self.planner.think_outline()
        self.thinker.outline_plan_list = self.planner.outline_plan_list
        should_act = await self.thinker.think_kg()
        if not should_act:
            return "Thinking complete - no action needed"
        return await self.actor.act()

    async def _refresh_tools(self) -> Tuple[List[str], List[str]]:
        """Refresh the list of available tools from the MCP server.

        Returns:
            A tuple of (added_tools, removed_tools)
        """
        if not self.mcp_clients.sessions:
            return [], []

        # modify by houzhe
        for (key, session) in self.mcp_clients.sessions.items():
            # Get current tool schemas directly from the server
            # response = await self.mcp_clients.sessions.list_tools()
            response = await session.list_tools()
            current_tools = {tool.name: tool.inputSchema for tool in response.tools}

            # Determine added, removed, and changed tools
            current_names = set(current_tools.keys())
            previous_names = set(self.tool_schemas.keys())

            added_tools = list(current_names - previous_names)
            removed_tools = list(previous_names - current_names)

            # Check for schema changes in existing tools
            changed_tools = []
            for name in current_names.intersection(previous_names):
                if current_tools[name] != self.tool_schemas.get(name):
                    changed_tools.append(name)

            # Update stored schemas
            self.tool_schemas = current_tools

            # Log and notify about changes
            if added_tools:
                logger.info(f"Added MCP tools: {added_tools}")
                self.memory.add_message(
                    Message.system_message(f"New tools available: {', '.join(added_tools)}")
                )
            if removed_tools:
                logger.info(f"Removed MCP tools: {removed_tools}")
                self.memory.add_message(
                    Message.system_message(
                        f"Tools no longer available: {', '.join(removed_tools)}"
                    )
                )
            if changed_tools:
                logger.info(f"Changed MCP tools: {changed_tools}")
        return added_tools, removed_tools

    async def cleanup(self) -> None:
        """Clean up MCP connection when done."""
        if self.mcp_clients.sessions:
            try:
                await self.mcp_clients.disconnect()
                logger.info("MCP connection closed")
            except Exception as e:
                logger.warning(f"Error during MCP connection cleanup: {e}")
                # 即使清理过程中出现错误，也确保状态被重置
            # 确保重置初始化状态
            self.initialized = False

    async def run(self, strategy: str = "step", intent: bool = False, user_name: str = "", prompt: str = None) -> str:
        """
        Run the agent with the specified strategy.

        Args:
            strategy: The planning strategy to use ("step", "outline", or "plan_kg").
            intent: Whether to use intent recognition.
            user_name: The user name for personalization.
            prompt: The prompt text from request.

        Returns:
            str: A string summarizing the execution results.

        Raises:
            RuntimeError: If the agent is not in the IDLE state at the start.
        """

        # 每次请求开始时重置current_step和state，确保多轮交互时步数不会累加
        self.current_step = 0
        self.state = AgentState.IDLE

        if intent:
            try:
                intent_res = await self.intent.run_intent_interactive(prompt)
                # 如果需要更多信息，直接返回，不进行后续流程
                if isinstance(intent_res, dict) and intent_res.get("scene") == "需要更多信息":
                    return intent_res
                # 如果意图识别失败，返回错误信息
                if isinstance(intent_res, dict) and intent_res.get("scene") == "意图识别失败":
                    logger.error(f"意图识别失败: {intent_res.get('params', '')}", exc_info=True)
                    # 确保在意图识别失败时也能正确重置状态
                    self.current_step = 0
                    self.state = AgentState.IDLE
                    return {"scene": "意图识别失败", "message": f"意图识别失败: {intent_res.get('params', '')}"}
                # 只有在成功识别意图后才继续
                request = "{}:{}".format(intent_res["scene"], intent_res["params"])
                
                # 重置所有场景处理器的上下文，避免不同用户请求之间的上下文混淆
                for processor in self.intent.processors.values():
                    if hasattr(processor, 'reset'):
                        processor.reset()
            except Exception as e:
                logger.error(f"意图识别失败: {str(e)}", exc_info=True)
                # 确保在意图识别失败时也能正确重置状态
                self.current_step = 0
                self.state = AgentState.IDLE
                return {"scene": "意图识别失败", "message": f"意图识别失败: {str(e)}"}
        else:
            """Run the agent with user prompt."""
            if prompt is None:
                prompt = input("Enter your prompt: ")
            if not prompt.strip():
                logger.warning("Empty prompt provided.")
                return "Empty prompt provided."
            request = prompt

        # TODO 从用户库中获取个人画像
        if user_name:
            personal_service = RAGFlowService(DatasetType.PERSONAL)
            retrieval_result = await personal_service.call_retrieval(user_name,
                                                                     config.rag_config.personal["similarity_threshold"])
            user_preferences = ast.literal_eval(retrieval_result).get("content", None)
            if user_preferences:
                request += USER_PREFERENCES_PROMPT + user_preferences

        try:
            if self.state != AgentState.IDLE:
                raise RuntimeError(f"Cannot run agent from state: {self.state}")

            if request and strategy != "step":
                self.update_memory("user", request)

            results: List[str] = []
            if strategy == "step":
                # 为用户输入自动添加self.last_question标志位
                self.update_memory("user", request + f" {self.last_question}")
                async with self.state_context(AgentState.RUNNING):
                    while (
                        self.current_step < self.max_steps and self.state != AgentState.FINISHED
                    ):
                        self.current_step += 1
                        logger.info(f"Executing step {self.current_step}/{self.max_steps}")
                        step_result = await self.step()

                        # Check for stuck state
                        if self.is_stuck():
                            self.planner.handle_stuck_state()

                        results.append(f"Step {self.current_step}: {step_result}")

                        # 任务完成检查，通过“terminate”判断任务是否完成，完成则修改AgentState状态为FINISHED
                        if "terminate" in results[-1].lower():
                            self.state = AgentState.FINISHED
                            results.append("Terminated: Received termination command")

                    if self.current_step >= self.max_steps:
                        self.current_step = 0
                        self.state = AgentState.IDLE
                        results.append(f"Terminated: Reached max steps ({self.max_steps})")
                return "\n".join(results) if results else "No steps executed"

            elif strategy == "outline" or (strategy == "plan_kg" and intent_res["scene"] == "其他场景"):
                # 读取outline步骤进行执行
                await self.planner.outline_plan(request)
                async with self.state_context(AgentState.RUNNING):
                    while (
                            self.current_step < self.max_steps and self.state != AgentState.FINISHED
                    ):
                        self.current_step += 1
                        logger.info(f"Executing step {self.current_step}/{self.max_steps}")
                        step_result = await self.step_outline()
                        # Check for stuck state
                        if self.is_stuck():
                            self.planner.handle_stuck_state()

                        results.append(f"Step {self.current_step}: {step_result}")

                        # 任务完成检查
                        step_num = len(self.thinker.outline_plan_list) + self.tools_num
                        last_user_message = next((msg for msg in reversed(self.messages) if msg.role == "user"), "")
                        last_user_content = last_user_message.content if last_user_message else ""
                        if self.current_step > step_num or (any(
                                term in results[-1].lower() for term in self.terminate)
                                                             and self.last_question in last_user_content):
                            self.state = AgentState.FINISHED
                            results.append("Terminated: Received termination command")

                    if self.current_step >= self.max_steps:
                        self.current_step = 0
                        self.state = AgentState.IDLE
                        results.append(f"Terminated: Reached max steps ({self.max_steps})")
                return "\n".join(results) if results else "No steps executed"

            elif strategy == "plan_kg" and (intent_res["scene"] != "其他场景"):
                # 读取知识库中的步骤进行执行
                await self.planner.plan_kg(intent_res["params"], intent_res["scene"])
                async with self.state_context(AgentState.RUNNING):
                    while (
                            self.current_step < self.max_steps and self.state != AgentState.FINISHED
                    ):
                        # kg step执行
                        self.current_step += 1
                        logger.info(f"Executing step {self.current_step}/{self.max_steps}")
                        step_result = await self.step_kg()
                        # Check for stuck state
                        if self.is_stuck():
                            self.planner.handle_stuck_state()

                        results.append(f"Step {self.current_step}: {step_result}")

                        # 任务完成检查
                        step_num = len(self.thinker.outline_plan_list) + self.tools_num
                        last_user_message = next((msg for msg in reversed(self.messages) if msg.role == "user"), "")
                        last_user_content = last_user_message.content if last_user_message else ""
                        if self.current_step > step_num or (any(
                                term in results[-1].lower() for term in self.terminate)
                                                             and self.last_question in last_user_content):
                            self.state = AgentState.FINISHED
                            results.append("Terminated: Received termination command")

                    if self.current_step >= self.max_steps:
                        self.current_step = 0
                        self.state = AgentState.IDLE
                        results.append(f"Terminated: Reached max steps ({self.max_steps})")
                return "\n".join(results) if results else "No steps executed"
        finally:
            await self.cleanup()

    def update_memory(
            self,
            role: ROLE_TYPE,  # type: ignore
            content: str,
            base64_image: Optional[str] = None,
            **kwargs,
    ) -> None:
        """Add a message to the agent's memory.

        Args:
            role: The role of the message sender (user, system, assistant, tool).
            content: The message content.
            base64_image: Optional base64 encoded image.
            **kwargs: Additional arguments (e.g., tool_call_id for tool messages).

        Raises:
            ValueError: If the role is unsupported.
        """
        message_map = {
            "user": Message.user_message,
            "system": Message.system_message,
            "assistant": Message.assistant_message,
            "tool": lambda content, **kw: Message.tool_message(content, **kw),
        }

        if role not in message_map:
            raise ValueError(f"Unsupported message role: {role}")

        # Create message with appropriate parameters based on role
        kwargs = {"base64_image": base64_image, **(kwargs if role == "tool" else {})}
        self.memory.add_message(message_map[role](content, **kwargs))

    def is_stuck(self) -> bool:
        """Check if the agent is stuck in a loop by detecting duplicate content"""
        if len(self.memory.messages) < 2:
            return False

        last_message = self.memory.messages[-1]
        if not last_message.content:
            return False

        # Count identical content occurrences
        duplicate_count = sum(
            1
            for msg in reversed(self.memory.messages[:-1])
            if msg.role == "assistant" and msg.content == last_message.content
        )

        return duplicate_count >= self.duplicate_threshold

    @property
    def messages(self) -> List[Message]:
        """Retrieve a list of messages from the agent's memory."""
        return self.memory.messages

    @messages.setter
    def messages(self, new_messages: List[Message]) -> None:
        """Set a new list of messages in the agent's memory."""
        if not isinstance(new_messages, list):
            raise ValueError("messages must be a list of Message objects")
        self.memory.messages = new_messages

    def _is_special_tool(self, name: str) -> bool:
        """Check if tool name is in special tools list"""
        return name.lower() in [n.lower() for n in self.special_tool_names]