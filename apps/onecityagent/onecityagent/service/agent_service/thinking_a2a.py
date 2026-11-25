from python_a2a import AgentCard, AgentSkill, A2AServer, TaskStatus, TaskState, run_server
import asyncio
import json
from fastmcp import Client
from fastmcp.client import SSETransport
from typing import List, Tuple

from onecityagent.service.llm_service import LLM
from onecityagent.schema import Message as MessageSchema
from onecityagent.schema import ToolCall, Memory
from onecityagent.logger import logger
from onecityagent.service.tool_service.mcp_tool import MCPClients
from onecityagent.config import config
from onecityagent.prompt import MCP_SYSTEM_PROMPT

agent_card = AgentCard(
    name="思考智能体",
    description="一个用于思考的智能体",
    url=f"http://localhost:5004",
    version="1.0.0",
    skills=[
        AgentSkill(
            name="思考",
            description="根据当前状态和可用的工具，判断下一步是什么",
            examples=["西安的经纬度？"]
        )
    ]
)

# asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

class Thinker(A2AServer):
    def __init__(self):
        super().__init__(agent_card=agent_card)
        self.llm = LLM(config_name="mcp_agent")
        self.tools = [{'type': 'function', 'function': {'name': 'mcp_amap-amap-sse_maps_direction_bicycling', 'description': '骑行路径规划用于规划骑行通勤方案，规划时会考虑天桥、单行线、封路等情况。最大支持 500km 的骑行路线规划', 'parameters': {'type': 'object', 'properties': {'origin': {'type': 'string', 'description': '出发点经纬度，坐标格式为：经度，纬度'}, 'destination': {'type': 'string', 'description': '目的地经纬度，坐标格式为：经度，纬度'}}, 'required': ['origin', 'destination']}}}, {'type': 'function', 'function': {'name': 'mcp_amap-amap-sse_maps_direction_driving', 'description': '驾车路径规划 API 可以根据用户起终点经纬度坐标规划以小客车、轿车通勤出行的方案，并且返回通勤方案的数据。', 'parameters': {'type': 'object', 'properties': {'origin': {'type': 'string', 'description': '出发点经纬度，坐标格式为：经度，纬度'}, 'destination': {'type': 'string', 'description': '目的地经纬度，坐标格式为：经度，纬度'}}, 'required': ['origin', 'destination']}}}, {'type': 'function', 'function': {'name': 'mcp_amap-amap-sse_maps_direction_transit_integrated', 'description': '根据用户起终点经纬度坐标规划综合各类公共（火车、公交、地铁）交通方式的通勤方案，并且返回通勤方案的数据，跨城场景下必须传起点城市与终点城市', 'parameters': {'type': 'object', 'properties': {'origin': {'type': 'string', 'description': '出发点经纬度，坐标格式为：经度，纬度'}, 'destination': {'type': 'string', 'description': '目的地经纬度，坐标格式为：经度，纬度'}, 'city': {'type': 'string', 'description': '公共交通规划起点城市'}, 'cityd': {'type': 'string', 'description': '公共交通规划终点城市'}}, 'required': ['origin', 'destination', 'city', 'cityd']}}}, {'type': 'function', 'function': {'name': 'mcp_amap-amap-sse_maps_direction_walking', 'description': '根据输入起点终点经纬度坐标规划100km 以内的步行通勤方案，并且返回通勤方案的数据', 'parameters': {'type': 'object', 'properties': {'origin': {'type': 'string', 'description': '出发点经度，纬度，坐标格式为：经度，纬度'}, 'destination': {'type': 'string', 'description': '目的地经度，纬度，坐标格式为：经度，纬度'}}, 'required': ['origin', 'destination']}}}, {'type': 'function', 'function': {'name': 'mcp_amap-amap-sse_maps_distance', 'description': '测量两个经纬度坐标之间的距离,支持驾车、步行以及球面距离测量', 'parameters': {'type': 'object', 'properties': {'origins': {'type': 'string', 'description': '起点经度，纬度，可以传多个坐标，使用竖线隔离，比如120,30|120,31，坐标格式为：经度，纬度'}, 'destination': {'type': 'string', 'description': '终点经度，纬度，坐标格式为：经度，纬度'}, 'type': {'type': 'string', 'description': '距离测量类型,1代表驾车距离测量，0代表直线距离测量，3步行距离测量'}}, 'required': ['origins', 'destination']}}}, {'type': 'function', 'function': {'name': 'mcp_amap-amap-sse_maps_geo', 'description': '将详细的结构化地址转换为经纬度坐标。支持对地标性名胜景区、建筑物名称解析为经纬度坐标', 'parameters': {'type': 'object', 'properties': {'address': {'type': 'string', 'description': '待解析的结构化地址信息'}, 'city': {'type': 'string', 'description': '指定查询的城市'}}, 'required': ['address']}}}, {'type': 'function', 'function': {'name': 'mcp_amap-amap-sse_maps_regeocode', 'description': '将一个高德经纬度坐标转换为行政区划地址信息', 'parameters': {'type': 'object', 'properties': {'location': {'type': 'string', 'description': '经纬度'}}, 'required': ['location']}}}, {'type': 'function', 'function': {'name': 'mcp_amap-amap-sse_maps_ip_location', 'description': 'IP 定位根据用户输入的 IP 地址，定位 IP 的所在位置', 'parameters': {'type': 'object', 'properties': {'ip': {'type': 'string', 'description': 'IP地址'}}, 'required': ['ip']}}}, {'type': 'function', 'function': {'name': 'mcp_amap-amap-sse_maps_schema_personal_map', 'description': '用于行程规划结果在高德地图展示。将行程规划位置点按照行程顺序填入lineList，返回结果为高德地图打开的URI链接，该结果不需总结，直接返回！', 'parameters': {'type': 'object', 'properties': {'orgName': {'type': 'string', 'description': '行程规划地图小程序名称'}, 'lineList': {'type': 'array', 'description': '行程列表', 'items': {'type': 'object', 'properties': {'title': {'type': 'string', 'description': '行程名称描述（按行程顺序）'}, 'pointInfoList': {'type': 'array', 'description': '行程目标位置点描述', 'items': {'type': 'object', 'properties': {'name': {'type': 'string', 'description': '行程目标位置点名称'}, 'lon': {'type': 'number', 'description': '行程目标位置点经度'}, 'lat': {'type': 'number', 'description': '行程目标位置点纬度'}, 'poiId': {'type': 'string', 'description': '行程目标位置点POIID'}}, 'required': ['name', 'lon', 'lat', 'poiId']}}}, 'required': ['title', 'pointInfoList']}}}, 'required': ['orgName', 'lineList']}}}, {'type': 'function', 'function': {'name': 'mcp_amap-amap-sse_maps_around_search', 'description': '周边搜，根据用户传入关键词以及坐标location，搜索出radius半径范围的POI', 'parameters': {'type': 'object', 'properties': {'keywords': {'type': 'string', 'description': '搜索关键词'}, 'location': {'type': 'string', 'description': '中心点经度纬度'}, 'radius': {'type': 'string', 'description': '搜索半径'}}, 'required': ['keywords', 'location']}}}, {'type': 'function', 'function': {'name': 'mcp_amap-amap-sse_maps_search_detail', 'description': '查询关键词搜或者周边搜获取到的POI ID的详细信息', 'parameters': {'type': 'object', 'properties': {'id': {'type': 'string', 'description': '关键词搜或者周边搜获取到的POI ID'}}, 'required': ['id']}}}, {'type': 'function', 'function': {'name': 'mcp_amap-amap-sse_maps_text_search', 'description': '关键字搜索 API 根据用户输入的关键字进行 POI 搜索，并返回相关的信息', 'parameters': {'type': 'object', 'properties': {'keywords': {'type': 'string', 'description': '查询关键字'}, 'city': {'type': 'string', 'description': '查询城市'}, 'citylimit': {'type': 'boolean', 'default': False, 'description': '是否限制城市范围内搜索，默认不限制'}}, 'required': ['keywords']}}}, {'type': 'function', 'function': {'name': 'mcp_amap-amap-sse_maps_schema_navi', 'description': ' Schema唤醒客户端-导航页面，用于根据用户输入终点信息，返回一个拼装好的客户端唤醒URI，用户点击该URI即可唤起对应的客户端APP。唤起客户端后，会自动跳转到导航页面。', 'parameters': {'type': 'object', 'properties': {'lon': {'type': 'string', 'description': '终点经度'}, 'lat': {'type': 'string', 'description': '终点纬度'}}, 'required': ['lon', 'lat']}}}, {'type': 'function', 'function': {'name': 'mcp_amap-amap-sse_maps_schema_take_taxi', 'description': '根据用户输入的起点和终点信息，返回一个拼装好的客户端唤醒URI，直接唤起高德地图进行打车。直接展示生成的链接，不需要总结', 'parameters': {'type': 'object', 'properties': {'slon': {'type': 'string', 'description': '起点经度'}, 'slat': {'type': 'string', 'description': '起点纬度'}, 'sname': {'type': 'string', 'description': '起点名称'}, 'dlon': {'type': 'string', 'description': '终点经度'}, 'dlat': {'type': 'string', 'description': '终点纬度'}, 'dname': {'type': 'string', 'description': '终点名称'}}, 'required': ['dlon', 'dlat', 'dname']}}}, {'type': 'function', 'function': {'name': 'mcp_amap-amap-sse_maps_weather', 'description': '根据城市名称或者标准adcode查询指定城市的天气', 'parameters': {'type': 'object', 'properties': {'city': {'type': 'string', 'description': '城市名称或者adcode'}}, 'required': ['city']}}}, {'type': 'function', 'function': {'name': 'mcp_mcp-alarm_mock_fire_alarm', 'description': '将火警报告发送到消防部门', 'parameters': {'properties': {'report': {'title': 'Report', 'type': 'string'}}, 'required': ['report'], 'type': 'object'}}}, {'type': 'function', 'function': {'name': 'mcp_mcp-alarm_mock_flooding_alarm', 'description': '将洪涝报告发送到相关部门', 'parameters': {'properties': {'report': {'title': 'Report', 'type': 'string'}}, 'required': ['report'], 'type': 'object'}}}]
        self.mcp_clients = MCPClients()
        self.connected_servers = {}
        self.memory = Memory()
        self.tool_schemas = {}
        self.system_prompt = MCP_SYSTEM_PROMPT

        self.available_tools = None  # 将在initialize()中设置
        self.initialize_sync()


    def initialize_sync(self):
        """同步初始化方法，适用于需要在事件循环外部调用的场景"""
        try:
            loop_init = asyncio.new_event_loop()
            asyncio.set_event_loop(loop_init)
            return loop_init.run_until_complete(self.initialize())
            # return self.initialize()
        except Exception as e:
            logger.error(f"同步初始化MCP服务器连接失败: {str(e)}")
            raise

    async def connect_mcp_server(
            self,
            server_url: str,
            server_id: str = "",
            use_stdio: bool = False,
            stdio_args: List[str] = None,
    ) -> None:
        """安全地连接到MCP服务器并添加其工具"""
        try:
            if use_stdio:
                await self.mcp_clients.connect_stdio(
                    server_url, stdio_args or [], server_id
                )
            else:
                # 直接调用connect_sse，不使用额外的任务组包装
                await self.mcp_clients.connect_sse(server_url, server_id)

            # 记录连接
            self.connected_servers[server_id or server_url] = server_url

            # 更新可用工具，只添加来自此服务器的新工具
            new_tools = [
                tool for tool in self.mcp_clients.tools if tool.server_id == server_id
            ]
            self.mcp_clients.add_tools(*new_tools)

            logger.info(f"成功连接到MCP服务器 {server_id or server_url}")
        except Exception as e:
            logger.error(f"连接MCP服务器 {server_id or server_url} 失败: {str(e)}")
            raise

    async def execute_tool(self, command: ToolCall) -> str:
        """Execute a single tool call with robust error handling"""
        if not command or not command.function or not command.function.name:
            return "Error: Invalid command format"

        name = command.function.name
        if name not in self.available_tools.tool_map:
            return f"Error: Unknown tool '{name}'"

        try:
            args = json.loads(command.function.arguments or "{}")
            logger.info(f"🔧 Activating tool: '{name}'...")

            # 获取工具所属的服务器ID
            tool_info = self.available_tools.tool_map.get(name)
            original_name = tool_info.original_name if tool_info else name
            logger.info(f"<UNK> Activating tool: '{original_name}'...")
            server_id = tool_info.server_id if tool_info else ""

            # 获取对应服务器的URL
            server_url = self.connected_servers.get(server_id, "")
            if not server_url:
                return f"Error: Cannot find server URL for tool '{name}'"

            # 创建与特定服务器的客户端连接
            transport = SSETransport(url=server_url)
            client = Client(transport)

            # 执行工具调用
            async with client:
                await client.ping()
                result = await client.call_tool(original_name, args)
                logger.info(f"Tool '{name}' executed with result: {result}")

            if hasattr(result, "base64_image") and result.base64_image:
                self._current_base64_image = result.base64_image

            observation = (
                f"Observed output of cmd `{original_name}` executed:\n{str(result)}"
                if result
                else f"Cmd `{original_name}` completed with no output"
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

    async def _refresh_tools(self) -> Tuple[List[str], List[str]]:
        """Refresh the list of available tools from the MCP server.

        Returns:
            A tuple of (added_tools, removed_tools)
        """
        if not self.mcp_clients.sessions:
            return [], []

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
                    MessageSchema.system_message(f"New tools available: {', '.join(added_tools)}")
                )
            if removed_tools:
                logger.info(f"Removed MCP tools: {removed_tools}")
                self.memory.add_message(
                    MessageSchema.system_message(
                        f"Tools no longer available: {', '.join(removed_tools)}"
                    )
                )
            if changed_tools:
                logger.info(f"Changed MCP tools: {changed_tools}")
        return added_tools, removed_tools

    async def initialize(self) -> None:
        """初始化与配置的MCP服务器的连接"""
        try:
            for server_id, server_config in config.mcp_config.servers.items():
                try:
                    if server_config.type == "sse":
                        if server_config.url:
                            # 使用专用任务进行连接
                            await self.connect_mcp_server(server_config.url, server_id)
                            logger.info(f"已连接到MCP服务器 {server_id} ({server_config.url})")
                    elif server_config.type == "stdio":
                        if server_config.command:
                            await self.connect_mcp_server(
                                server_config.command,
                                server_id,
                                use_stdio=True,
                                stdio_args=server_config.args,
                            )
                            logger.info(f"已通过命令连接到MCP服务器 {server_id} ({server_config.command})")
                except Exception as e:
                    logger.error(f"连接到MCP服务器 {server_id} 失败: {str(e)}")
                    continue

            # 设置可用工具为我们的MCP实例
            self.available_tools = self.mcp_clients

            # 存储初始工具架构
            added_tools, _ = await self._refresh_tools()

            # 添加关于可用工具的系统消息
            tool_names = list(self.mcp_clients.tool_map.keys())
            tools_info = ", ".join(tool_names)

            # 添加系统提示和可用工具信息
            self.memory.add_message(
                MessageSchema.system_message(
                    f"{self.system_prompt}\n\n可用的MCP工具: {tools_info}"
                )
            )
        except Exception as e:
            logger.error(f"初始化MCP服务器连接失败: {str(e)}")
            raise

    async def act(self, tool_calls) -> str:
        """Execute tool calls and handle their results"""
        results = []

        for command in tool_calls:
            self._current_base64_image = None
            result = await self.execute_tool(command)
            logger.info(
                f"🎯 Tool '{command.function.name}' completed its mission! Result: {result}"
            )

            tool_msg = MessageSchema.tool_message(
                content=result,
                tool_call_id=command.id,
                name=command.function.name,
                base64_image=self._current_base64_image,
            )
            self.memory.add_message(tool_msg)
            results.append(result)

        return "\n\n".join(results)

    def handle_task(self, task):
        message_data = task.message or {}
        content = message_data.get("content", {})
        all_message = content.get("text", "") if isinstance(content, dict) else ""
        MCP_NEXT_STEP_PROMPT = """Based on the current state and available tools, what should be done next?
        Think step by step about the problem and identify which MCP tool would be most helpful for the current stage.
        If you've already made progress, consider what additional information you need or what actions would move you closer to completing the task.
        """
        all_message_dict = json.loads(all_message)
        memory = Memory.model_validate(all_message_dict)
        messages = memory.messages
        next_step_prompt = {
            "role": "user",
            "content": MCP_NEXT_STEP_PROMPT
        }
        all_message_dict["messages"].append(next_step_prompt)
        last_message = MessageSchema.user_message(MCP_NEXT_STEP_PROMPT)
        messages.append(last_message)
        result = asyncio.run(self.llm.ask_tool(messages=messages,
                                               tools=self.tools,
                                               tool_choice = "auto"))
        # content = result.content or ""
        # tool_calls = result.tool_calls or []
        # act_result = asyncio.run(self.act(tool_calls))
        # print("act_result", act_result)
        # print(act_result[0].to_dict())
        result_dict = result.to_dict()
        all_message_dict["messages"].append(result_dict)
        response = all_message_dict
        task.artifacts = [{
            "parts": [{"type": "text", "text": response}]
        }]
        task.status = TaskStatus(state=TaskState.COMPLETED)
        return task

if __name__ == "__main__":
    think_server = Thinker()
    run_server(think_server, port=5004, host="0.0.0.0")