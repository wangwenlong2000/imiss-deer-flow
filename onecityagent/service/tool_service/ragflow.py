import asyncio
import json
from copy import deepcopy
from enum import Enum
from typing import Dict, List, Optional
from dataclasses import dataclass

from onecityagent.config import RagSettings, config
from onecityagent.service.tool_service.mcp_tool import MCPClients


@dataclass
class DatasetConfig:
    """Configuration for a dataset"""
    dataset_name: str
    document_name: str


class DatasetType(Enum):
    """Enum for different dataset types"""
    PUBLIC = "public"
    PERSONAL = "personal"
    # To add new types in future:
    # THIRD = "third"


class RAGFlowClient:
    """Core RAGFlow client handling direct API operations"""

    def __init__(self, rag_settings: RagSettings, dataset_cfg: DatasetConfig):
        self.base_url = rag_settings.base_url.rstrip("/")
        self.api_key = rag_settings.api_key
        self.dataset_cfg = dataset_cfg
        self.dataset_ids: Optional[List[str]] = None
        self.mcp_client = None

    async def _get_mcp_client(self):
        """Initialize MCP client if not already done"""
        if self.mcp_client is None:
            self.mcp_client = deepcopy(MCPClients())
            await self.mcp_client.connect_sse(
                f"{self.base_url}/sse?apikey={self.api_key}",
                server_id="rag")
        return self.mcp_client

    async def _ensure_initialized(self):
        """Ensure the client is initialized with dataset IDs"""
        if self.dataset_ids is None:
            self.dataset_ids = await self._get_dataset_ids()

    async def _get_dataset_ids(self) -> List[str]:
        """Get dataset IDs for the configured dataset"""
        list_res = await self._execute("ragflow_list_datasets", {})

        datasets = [json.loads(line) for line in
                    list_res.split('\n')
                    if line.strip()]
        matching_datasets = [ds["id"] for ds in datasets
                             if ds["dataset"] == self.dataset_cfg.dataset_name]

        if not matching_datasets:  # Empty list indicates no matching dataset_name
            raise ValueError(f"The dataset does not exist.: {self.dataset_cfg.dataset_name}")
        return matching_datasets

    async def _execute(self, tool_name: str, args: Dict) -> Dict:
        """Execute a RAGFlow operation"""
        mcp_client = await self._get_mcp_client()
        full_tool_name = f"mcp_rag_{tool_name}"

        if full_tool_name not in mcp_client.tool_map:
            raise RuntimeError(f"Tool {full_tool_name} not found")

        response = await mcp_client.tool_map[full_tool_name].execute(**args)

        if response.error:
            raise RuntimeError(response.error)
        # return {"content": [{"text": response.output}]}
        return response.output

    async def close_session(self) -> None:
        """Clean up MCP connection when done."""
        if self.mcp_client.sessions:
            await self.mcp_client.disconnect()

    async def retrieval(self, question: str, sim_threshold: float) -> Dict:
        """Perform retrieval operation"""
        await self._ensure_initialized()
        return await self._execute("ragflow_retrieval", {
            "dataset_ids": self.dataset_ids,
            "question": question,
            "similarity_threshold": sim_threshold
        })

    async def write_chunk(self, content: str) -> Dict:
        """Write content to the dataset"""
        await self._ensure_initialized()
        doc_id = await self._get_or_create_doc()
        return await self._execute("ragflow_write_chunk", {
            "dataset_ids": self.dataset_ids,
            "document_ids": [doc_id],
            "content": content
        })

    async def _get_or_create_doc(self) -> str:
        """Get existing document or create a new one"""
        res = await self._execute("ragflow_list_doc", {
            "dataset_ids": self.dataset_ids
        })
        docs = [json.loads(line) for line in
                res.split('\n')
                if line.strip() and '"doc_id"' in line]

        for doc in docs:
            if doc["doc_name"] == self.dataset_cfg.document_name:
                return doc["doc_id"]

        # Create new document if not found
        create_res = await self._execute("ragflow_create_doc", {
            "dataset_ids": self.dataset_ids,
            "document_name": self.dataset_cfg.document_name
        })
        return create_res


class RAGFlowService:
    """Service layer providing simplified interface for dataset operations"""

    def __init__(self, dataset_type: DatasetType):
        self.dataset_type = dataset_type
        self._client = None

    async def _get_client(self) -> RAGFlowClient:
        """Initialize client with proper configuration"""
        if self._client is None:
            dataset_cfg = self._get_dataset_config()
            self._client = RAGFlowClient(config.rag_config, dataset_cfg)
        return self._client

    def _get_dataset_config(self) -> DatasetConfig:
        """Get configuration for the specified dataset type"""
        if self.dataset_type == DatasetType.PUBLIC:
            return DatasetConfig(
                config.rag_config.public["dataset"],
                config.rag_config.public["document"]
            )
        elif self.dataset_type == DatasetType.PERSONAL:
            return DatasetConfig(
                config.rag_config.personal["dataset"],
                config.rag_config.personal["document"]
            )
        # Example for future extension:
        # elif dataset_type == DatasetType.THIRD:
        #     return DatasetConfig(...)
        else:
            raise ValueError(f"Unknown dataset type: {self.dataset_type}")

    async def call_retrieval(self, question: str, sim_threshold: float = 0.5) -> Dict:
        """Perform retrieval from the configured dataset"""
        client = await self._get_client()
        return await client.retrieval(question, sim_threshold)

    async def call_write_chunk(self, content: str) -> Dict:
        """Write content to the configured dataset"""
        client = await self._get_client()
        return await client.write_chunk(content)

    async def cleanup(self) -> None:
        """Clean up MCP connection when done."""
        if self._client is not None:
            await self._client.close_session()


# Usage example
async def main():
    # Using public dataset through service layer

    # Example 1: Call retrieval
    print("Calling retrieval...")
    public_service = RAGFlowService(DatasetType.PUBLIC)
    retrieval_result = await public_service.call_retrieval(
        question="今天是星期几",
        sim_threshold=0.1
    )
    print("Retrieval result:", retrieval_result)

    # Example 2: Call write chunk (will create doc if none exists)
    print("\nCalling write chunk...")
    write_result = await public_service.call_write_chunk(
        content="今天是星期三."
    )
    print("Write result:", write_result)
    await public_service.cleanup()

    # Example 3: Call write chunk
    print("\nCalling write chunk...")
    personal_service = RAGFlowService(DatasetType.PERSONAL)
    print(await personal_service.call_write_chunk("Personal data example"))

    # Example 4: Call retrieval
    print("Calling retrieval...")
    print(await personal_service.call_retrieval("Personal data"))
    await personal_service.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
