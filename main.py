#!/usr/bin/env python
import argparse
import asyncio
import sys

from onecityagent.service.agent_service.agent import Agent
from onecityagent.config import config
from onecityagent.logger import logger


class MCPRunner:
    """Runner class for MCP Agent with proper path handling and configuration."""

    def __init__(self):
        self.root_path = config.root_path
        self.server_reference = config.mcp_config.server_reference
        self.mcp_config = config.mcp_config
        self.agent = Agent()

    async def initialize(
        self,
    ) -> None:
        """Initialize the MCP agent with the appropriate connection."""
        logger.info(f"Initializing MCPAgent connection...")
        await self.agent.initialize()
        logger.info(f"Connected to MCP server")

    async def run_single_prompt(self, strategy, intent, user_name) -> None:
        # TODO demo测试，使用user name进行内容获取
        """Run the agent with a single prompt."""
        await self.agent.run(strategy, intent, user_name)

    async def cleanup(self) -> None:
        """Clean up agent resources."""
        await self.agent.cleanup()
        logger.info("Session ended")


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Run the MCP Agent")
    parser.add_argument("--intent", "-t", default=False, required=False, help="Run with intent recognition or not")
    parser.add_argument("--prompt", "-p", help="Single prompt to execute and exit")
    parser.add_argument("--plan-strategy", "-s",
                        choices=["step", "outline", "plan_kg"],
                        default="step",
                        help="Plan strategy to use")
    parser.add_argument("--user_name", "-u", default="", required=False, help="User name")
    return parser.parse_args()


async def run_mcp() -> None:
    """Main entry point for the MCP runner."""
    args = parse_args()
    runner = MCPRunner()

    if args.plan_strategy == "plan_kg" and not args.intent:
        logger.error("""错误：当 "plan-strategy" 使用 "plan_kg" 时，"intent" 参数必须为 True""")
        sys.exit(1)

    try:
        # await runner.initialize(args.connection, args.server_url)
        await runner.initialize()
        await runner.run_single_prompt(args.plan_strategy, args.intent, args.user_name)
    except KeyboardInterrupt:
        logger.info("Program interrupted by user")
    except Exception as e:
        logger.error(f"Error running MCPAgent: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(run_mcp())
