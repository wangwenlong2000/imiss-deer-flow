import json
import threading
import tomllib
from pathlib import Path
from typing import Dict, List, Optional, Any
import re
from pydantic import BaseModel, Field


def get_project_root() -> Path:
    """Get the project root directory"""
    return Path(__file__).resolve().parent.parent


PROJECT_ROOT = get_project_root()
WORKSPACE_ROOT = PROJECT_ROOT / "workspace"

def get_port_from_url(url):
    """
    从给定的URL中提取端口号。
    如果URL中没有端口号，则返回None。
    """
    match = re.search(r':(\d+)', url)
    if match:
        return int(match.group(1))
    return None

class MockMCPSettings(BaseModel):
    """Mock MCP 服务器配置"""
    construction_management: int = Field(8001, description="住建部门施工管理服务端口")
    fire_safety: int = Field(8002, description="消防服务端口")
    safety_food_pharmaceuticals: int = Field(8003, description="市场监管局食药品安全服务端口")
    street_order: int = Field(8004, description="街面秩序服务端口")
    public_security_alarm: int = Field(8005, description="公共安全服务端口")
    flooding_alarm: int = Field(8006, description="洪涝报警服务端口")

class LLMSettings(BaseModel):
    model: str = Field(..., description="Model name")
    base_url: str = Field(..., description="API base URL")
    api_key: str = Field(..., description="API key")
    max_tokens: int = Field(4096, description="Maximum number of tokens per request")
    max_input_tokens: Optional[int] = Field(
        None,
        description="Maximum input tokens to use across all requests (None for unlimited)",
    )
    temperature: float = Field(1.0, description="Sampling temperature")
    api_type: str = Field(..., description="Azure, Openai, or Ollama")
    api_version: str = Field(..., description="Azure Openai version if AzureOpenai")


class ProxySettings(BaseModel):
    server: str = Field(None, description="Proxy server address")
    username: Optional[str] = Field(None, description="Proxy username")
    password: Optional[str] = Field(None, description="Proxy password")


class SearchSettings(BaseModel):
    engine: str = Field(default="Google", description="Search engine the llm to use")
    fallback_engines: List[str] = Field(
        default_factory=lambda: ["DuckDuckGo", "Baidu", "Bing"],
        description="Fallback search engines to try if the primary engine fails",
    )
    retry_delay: int = Field(
        default=60,
        description="Seconds to wait before retrying all engines again after they all fail",
    )
    max_retries: int = Field(
        default=3,
        description="Maximum number of times to retry all engines when all fail",
    )
    lang: str = Field(
        default="en",
        description="Language code for search results (e.g., en, zh, fr)",
    )
    country: str = Field(
        default="us",
        description="Country code for search results (e.g., us, cn, uk)",
    )


class BrowserSettings(BaseModel):
    headless: bool = Field(False, description="Whether to run browser in headless mode")
    disable_security: bool = Field(
        True, description="Disable browser security features"
    )
    extra_chromium_args: List[str] = Field(
        default_factory=list, description="Extra arguments to pass to the browser"
    )
    chrome_instance_path: Optional[str] = Field(
        None, description="Path to a Chrome instance to use"
    )
    wss_url: Optional[str] = Field(
        None, description="Connect to a browser instance via WebSocket"
    )
    cdp_url: Optional[str] = Field(
        None, description="Connect to a browser instance via CDP"
    )
    proxy: Optional[ProxySettings] = Field(
        None, description="Proxy settings for the browser"
    )
    max_content_length: int = Field(
        2000, description="Maximum length for content retrieval operations"
    )


class SandboxSettings(BaseModel):
    """Configuration for the execution sandbox"""

    use_sandbox: bool = Field(False, description="Whether to use the sandbox")
    image: str = Field("python:3.12-slim", description="Base image")
    work_dir: str = Field("/workspace", description="Container working directory")
    memory_limit: str = Field("512m", description="Memory limit")
    cpu_limit: float = Field(1.0, description="CPU limit")
    timeout: int = Field(300, description="Default command timeout (seconds)")
    network_enabled: bool = Field(
        False, description="Whether network access is allowed"
    )


class RagSettings(BaseModel):
    base_url: str = Field("", description="ragflow server url")
    api_key: str = Field("", description="ragflow server api key")
    public: dict = Field({}, description="ragflow public dataset infos")
    personal: dict = Field({}, description="ragflow public dataset infos")


class MCPServerConfig(BaseModel):
    """Configuration for a single MCP server"""

    type: str = Field(..., description="Server connection type (sse or stdio)")
    url: Optional[str] = Field(None, description="Server URL for SSE connections")
    command: Optional[str] = Field(None, description="Command for stdio connections")
    args: List[str] = Field(
        default_factory=list, description="Arguments for stdio command"
    )
    headers: Optional[Dict[str, Any]] = Field(
        default=None, description="Optional headers for SSE connections"
    )

class MCPSettings(BaseModel):
    """Configuration for MCP (Model Context Protocol)"""

    server_reference: str = Field(
        "onecityagent.mcp_service.server", description="Module reference for the MCP server"
    )
    servers: Dict[str, MCPServerConfig] = Field(
        default_factory=dict, description="MCP server configurations"
    )

    @classmethod
    def load_server_config(cls) -> Dict[str, MCPServerConfig]:
        """Load MCP server configuration from JSON file"""
        config_path = PROJECT_ROOT / "config" / "mcp.json"

        try:
            config_file = config_path if config_path.exists() else None
            if not config_file:
                return {}

            with config_file.open() as f:
                data = json.load(f)
                servers = {}

                for server_id, server_config in data.get("mcpServers", {}).items():
                    servers[server_id] = MCPServerConfig(
                        type=server_config["type"],
                        url=server_config.get("url"),
                        command=server_config.get("command"),
                        args=server_config.get("args", []),
                        headers=server_config.get("headers", None)
                    )
                return servers
        except Exception as e:
            raise ValueError(f"Failed to load MCP server config: {e}")

class A2AServerConfig(BaseModel):
    """A2A 单个服务器配置"""
    url: Optional[str] = Field(None, description="连接的服务器 URL")
class A2ASettings(BaseModel):
    """A2A 配置"""
    server_reference: str = Field(
        "onecityagent.a2a_service.server", description="A2A 服务器模块引用"
    )
    servers: Dict[str, A2AServerConfig] = Field(
        default_factory=dict, description="A2A 服务器配置"
    )
    router: str = Field(default_factory=str, description="A2A 路由配置")
    planner: str = Field(default_factory=str, description="A2A 规划智能体配置")
    llm_qa: str = Field(default_factory=str, description="A2A LLM 问答智能体配置")
    thinker: str = Field(default_factory=str, description="A2A 思考智能体配置")
    intent: str = Field(default_factory=str, description="A2A 意图识别智能体配置")
    actor: str = Field(default_factory=str, description="A2A 执行智能体配置")
    feedback: str = Field(default_factory=str, description="A2A 反馈智能体配置")
    config_crud_url: str = Field(default_factory=str, description="A2A 配置增删改查路由")
    system_prompt: str = Field(default_factory=str, description="A2A系统提示")

    @classmethod
    def load_server_config(cls) -> Dict[str, A2AServerConfig]:
        """从 JSON 文件加载 A2A 服务器配置"""
        config_path = PROJECT_ROOT / "config" / "a2a.json"
        try:
            config_file = config_path if config_path.exists() else None
            if not config_file:
                return {}
            with config_file.open(encoding='utf-8') as f:
                data = json.load(f)
                servers = {}
                for server_id, server_config in data.get("a2aServers", {}).items():
                    servers[server_id] = A2AServerConfig(
                        url=server_config.get("url"),
                    )
                return servers
        except Exception as e:
            raise ValueError(f"Failed to load A2A server config: {e}")

class IntentConfig():

    @classmethod
    def load_all_scene_configs(cls):
        config_path = PROJECT_ROOT / "config" / "intent_scene_templates.json"

        all_scene_configs = {}
        with open(config_path, 'r', encoding='utf-8') as file:
            for key, value in json.load(file).items():
                # todo 可以有加载优先级
                # 只有当键不存在时，才添加到all_scene_configs中
                if key not in all_scene_configs:
                    all_scene_configs[key] = value
        return all_scene_configs

    # 数据库配置现在直接从config.toml加载

class IntentSettings(BaseModel):
    """Configuration for Intent """
    DEBUG: bool = Field(
        False, description="whether debug mode is on"
    )
    RELATED_INTENT_THRESHOLD: float = Field(0.5, description="intent relation threshold")
    OTHER_SCENE: str = Field("", description="other intent scene name")

class DatabaseSettings(BaseModel):
    """数据库配置"""
    type: str = Field("mysql", description="数据库类型")
    host: str = Field("localhost", description="数据库主机")
    port: int = Field(3306, description="数据库端口")
    name: str = Field("onecityagent", description="数据库名称")
    user: str = Field("root", description="数据库用户名")
    password: str = Field("", description="数据库密码")
    max_connections: int = Field(500, description="最大连接数")
    timeout: int = Field(300, description="连接超时时间")

class AppConfig(BaseModel):
    llm: Dict[str, LLMSettings]
    sandbox: Optional[SandboxSettings] = Field(
        None, description="Sandbox configuration"
    )
    browser_config: Optional[BrowserSettings] = Field(
        None, description="Browser configuration"
    )
    search_config: Optional[SearchSettings] = Field(
        None, description="Search configuration"
    )
    mcp_config: Optional[MCPSettings] = Field(None, description="MCP configuration")

    rag_config: Optional[RagSettings] = Field(None, description="RAG configuration")

    intent_config: Optional[IntentSettings] = Field(None, description="INTENT configuration")

    a2a_config: Optional[A2ASettings] = Field(None, description="A2A 配置")
    database_config: Optional[DatabaseSettings] = Field(None, description="数据库配置")
    mock_mcp_config: Optional[MockMCPSettings] = Field(None, description="Mock MCP 配置")
    class Config:
        arbitrary_types_allowed = True


class Config:
    _instance = None
    _lock = threading.Lock()
    _initialized = False

    # RSA私钥文件路径
    PRIVATE_KEY_PATH = PROJECT_ROOT / "config" / "private.pem"
    # RSA私钥密码（如果私钥文件被加密）
    # 从配置或环境读取

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not self._initialized:
            with self._lock:
                if not self._initialized:
                    self._config = None
                    self._load_initial_config()
                    self._initialized = True

    @staticmethod
    def _get_config_path() -> Path:
        root = PROJECT_ROOT
        config_path = root / "config" / "config.toml"
        if config_path.exists():
            return config_path
        example_path = root / "config" / "config.example.toml"
        if example_path.exists():
            return example_path
        raise FileNotFoundError("No configuration file found in config directory")

    def _load_config(self) -> dict:
        config_path = self._get_config_path()
        with config_path.open("rb") as f:
            return tomllib.load(f)

    def _load_initial_config(self):
        raw_config = self._load_config()
        base_llm = raw_config.get("llm", {})
        llm_overrides = {
            k: v for k, v in raw_config.get("llm", {}).items() if isinstance(v, dict)
        }

        default_settings = {
            "model": base_llm.get("model"),
            "base_url": base_llm.get("base_url"),
            "api_key": base_llm.get("api_key"),
            "max_tokens": base_llm.get("max_tokens", 4096),
            "max_input_tokens": base_llm.get("max_input_tokens"),
            "temperature": base_llm.get("temperature", 1.0),
            "api_type": base_llm.get("api_type", ""),
            "api_version": base_llm.get("api_version", ""),
        }

        # handle browser config.
        browser_config = raw_config.get("browser", {})
        browser_settings = None

        if browser_config:
            # handle proxy settings.
            proxy_config = browser_config.get("proxy", {})
            proxy_settings = None

            if proxy_config and proxy_config.get("server"):
                proxy_settings = ProxySettings(
                    **{
                        k: v
                        for k, v in proxy_config.items()
                        if k in ["server", "username", "password"] and v
                    }
                )

            # filter valid browser config parameters.
            valid_browser_params = {
                k: v
                for k, v in browser_config.items()
                if k in BrowserSettings.__annotations__ and v is not None
            }

            # if there is proxy settings, add it to the parameters.
            if proxy_settings:
                valid_browser_params["proxy"] = proxy_settings

            # only create BrowserSettings when there are valid parameters.
            if valid_browser_params:
                browser_settings = BrowserSettings(**valid_browser_params)

        search_config = raw_config.get("search", {})
        search_settings = None
        if search_config:
            search_settings = SearchSettings(**search_config)

        sandbox_config = raw_config.get("sandbox", {})
        if sandbox_config:
            sandbox_settings = SandboxSettings(**sandbox_config)
        else:
            sandbox_settings = SandboxSettings()

        mcp_config = raw_config.get("mcp", {})
        mcp_settings = None
        if mcp_config:
            # Load server configurations from JSON
            mcp_config["servers"] = MCPSettings.load_server_config()
            mcp_settings = MCPSettings(**mcp_config)
        else:
            mcp_settings = MCPSettings(servers=MCPSettings.load_server_config())

        a2a_config = raw_config.get("a2a", {})
        if a2a_config:
            a2a_config["servers"] = A2ASettings.load_server_config()
            a2a_settings = A2ASettings(**a2a_config)
        else:
            a2a_settings = A2ASettings(servers=A2ASettings.load_server_config())

        rag_config = raw_config.get("ragflow", {})
        rag_settings = None
        if rag_config:
            rag_settings = RagSettings(**rag_config)

        intent_config = raw_config.get("intent", {})
        intent_settings = None
        if intent_config:
            intent_settings = IntentSettings(**intent_config)

        # 读取数据库配置
        database_settings = None
        db_config = raw_config.get("agent", {}).get("database", {})
        if db_config:
            # 将config.toml中的配置键名转换为DatabaseSettings模型的键名
            db_params = {
                "type": db_config.get("DATABASE_TYPE", "mysql"),
                "host": db_config.get("DATABASE_HOST", "localhost"),
                "port": db_config.get("DATABASE_PORT", 3306),
                "name": db_config.get("DATABASE_NAME", "onecityagent"),
                "user": db_config.get("DATABASE_USER", "root"),
                "password": db_config.get("DATABASE_PASSWORD", ""),
                "max_connections": db_config.get("DATABASE_MAX_CONNECTIONS", 500),
                "timeout": db_config.get("DATABASE_TIMEOUT", 300),
            }
            database_settings = DatabaseSettings(**db_params)
        else:
            database_settings = DatabaseSettings()

        # 读取安全配置（支持顶层 [security] 或 [agent.security]）
        security_config = raw_config.get("security", {})
        if security_config:
            pk_value = security_config.get("PRIVATE_KEY")
            if pk_value is not None:
                self.PRIVATE_KEY_PASSPHRASE = pk_value
            pk_path_value = security_config.get("PRIVATE_KEY_PATH")
            if pk_path_value:
                p = Path(pk_path_value)
                if not p.is_absolute():
                    p = PROJECT_ROOT / pk_path_value
                self.PRIVATE_KEY_PATH = p

        config_dict = {
            "llm": {
                "default": default_settings,
                **{
                    name: {**default_settings, **override_config}
                    for name, override_config in llm_overrides.items()
                },
            },
            "sandbox": sandbox_settings,
            "browser_config": browser_settings,
            "search_config": search_settings,
            "mcp_config": mcp_settings,
            "a2a_config": a2a_settings,
            "rag_config": rag_settings,
            "intent_config": intent_settings,
            "database_config": database_settings
        }

        # 读取 Mock MCP 配置
        mock_mcp_config = raw_config.get("mock_mcp", {})
        mock_mcp_settings = None
        if mock_mcp_config:
            mock_mcp_settings = MockMCPSettings(**mock_mcp_config)
        else:
            mock_mcp_settings = MockMCPSettings()

        config_dict = {
            "llm": {
                "default": default_settings,
                **{
                    name: {**default_settings, **override_config}
                    for name, override_config in llm_overrides.items()
                },
            },
            "sandbox": sandbox_settings,
            "browser_config": browser_settings,
            "search_config": search_settings,
            "mcp_config": mcp_settings,
            "a2a_config": a2a_settings,
            "rag_config": rag_settings,
            "intent_config": intent_settings,
            "database_config": database_settings,
            "mock_mcp_config": mock_mcp_settings
        }

        self._config = AppConfig(**config_dict)

    @property
    def llm(self) -> Dict[str, LLMSettings]:
        return self._config.llm

    @property
    def sandbox(self) -> SandboxSettings:
        return self._config.sandbox

    @property
    def browser_config(self) -> Optional[BrowserSettings]:
        return self._config.browser_config

    @property
    def search_config(self) -> Optional[SearchSettings]:
        return self._config.search_config

    @property
    def mcp_config(self) -> MCPSettings:
        """Get the MCP configuration"""
        return self._config.mcp_config

    @property
    def rag_config(self) -> RagSettings:
        return self._config.rag_config

    @property
    def intent_config(self) -> IntentSettings:
        return self._config.intent_config

    @property
    def workspace_root(self) -> Path:
        """Get the workspace root directory"""
        return WORKSPACE_ROOT

    @property
    def root_path(self) -> Path:
        """Get the root path of the application"""
        return PROJECT_ROOT

    @property
    def a2a_config(self) -> A2ASettings:
        return self._config.a2a_config

    @property
    def database_config(self) -> DatabaseSettings:
        """获取数据库配置"""
        return self._config.database_config

    # 为了向后兼容，保留这些属性
    @property
    def DATABASE_TYPE(self) -> str:
        return self.database_config.type

    @property
    def DATABASE_HOST(self) -> str:
        return self.database_config.host

    @property
    def DATABASE_PORT(self) -> int:
        return self.database_config.port

    @property
    def DATABASE_NAME(self) -> str:
        return self.database_config.name

    @property
    def DATABASE_USER(self) -> str:
        return self.database_config.user

    @property
    def DATABASE_PASSWORD(self) -> str:
        return self.database_config.password

    @property
    def DATABASE_MAX_CONNECTIONS(self) -> int:
        return self.database_config.max_connections

    @property
    def DATABASE_TIMEOUT(self) -> int:
        return self.database_config.timeout

    @property
    def mock_mcp_config(self) -> MockMCPSettings:
        """获取 Mock MCP 配置"""
        return self._config.mock_mcp_config

    # 动态更新所有config中的配置
    def update_config(self, config_dict: Dict):
        """
        根据提供的字典更新配置。这是一个通用函数，可以更新任何配置部分。

        Args:
            config_dict: 包含配置的字典，格式如下:
            {
                "llm": {
                    "default": {...},  # 默认LLM配置
                    "model1": {...}    # 其他模型配置
                },
                "browser": {...},      # 浏览器配置
                "search": {...},       # 搜索配置
                "sandbox": {...},      # 沙箱配置
                "mcp": {...},          # MCP配置
                "ragflow": {...},      # RAG配置
                "intent": {...},       # 意图配置
                "a2a": {...},          # A2A配置
                "database": {...}      # 数据库配置
            }
        """
        if not config_dict:
            return

        # 通用更新函数，用于处理基础配置模型
        def update_basic_config(field_name, config_class):
            if field_name in config_dict:
                new_config = config_dict[field_name]
                # 获取当前配置
                config_attr = f"{field_name}_config"
                current_config = getattr(self._config, config_attr) or config_class()
                # 构建当前配置字典
                current_dict = {k: getattr(current_config, k) for k in current_config.model_fields}
                # 合并配置
                updated_config = {**current_dict, **new_config}
                # 更新配置
                setattr(self._config, config_attr, config_class(**updated_config))

        # 处理LLM配置
        if "llm" in config_dict:
            llm_config = config_dict["llm"]
            if not llm_config:
                return

            # 创建新的llm配置字典
            new_llm_config = {}

            # 处理默认配置
            if "default" in llm_config:
                default_config = llm_config["default"]
                # 获取当前默认配置并更新
                current_default = self._config.llm.get("default", {})
                current_default_dict = {k: getattr(current_default, k) for k in
                                        current_default.model_fields} if current_default else {}
                # 合并配置
                updated_default = {**current_default_dict, **default_config}
                # 确保所有必需字段都存在
                for field in ["model", "base_url", "api_key"]:
                    if field not in updated_default or not updated_default[field]:
                        raise ValueError(f"缺少必需的LLM配置字段: {field}")
                # 创建新的LLMSettings对象
                new_llm_config["default"] = LLMSettings(**updated_default)
            else:
                # 保留现有的默认配置
                if "default" in self._config.llm:
                    new_llm_config["default"] = self._config.llm["default"]

            # 处理其他模型配置
            for model_name, model_config in llm_config.items():
                if model_name == "default":
                    continue

                # 获取当前模型配置(如果存在)
                current_model = self._config.llm.get(model_name)

                # 如果当前模型配置存在，只使用当前模型的配置作为基础，不继承其他模型的配置
                if current_model:
                    base_config_dict = {k: getattr(current_model, k) for k in current_model.model_fields}
                # 如果是新模型，则使用默认配置作为基础
                elif "default" in new_llm_config:
                    base_config_dict = {k: getattr(new_llm_config["default"], k) for k in
                                        new_llm_config["default"].model_fields}
                else:
                    base_config_dict = {}

                # 合并配置
                updated_model = {**base_config_dict, **model_config}

                # 确保所有必需字段都存在
                for field in ["model", "base_url", "api_key"]:
                    if field not in updated_model or not updated_model[field]:
                        if "default" in new_llm_config and getattr(new_llm_config["default"], field):
                            updated_model[field] = getattr(new_llm_config["default"], field)
                        else:
                            raise ValueError(f"模型 {model_name} 缺少必需的配置字段: {field}")

                # 创建新的LLMSettings对象
                new_llm_config[model_name] = LLMSettings(**updated_model)

            # 保留现有的未更新的模型配置
            for model_name, model_config in self._config.llm.items():
                if model_name not in new_llm_config:
                    new_llm_config[model_name] = model_config

            # 更新配置对象中的llm属性
            self._config.llm = new_llm_config

        # 处理浏览器配置
        if "browser" in config_dict:
            browser_config = config_dict["browser"]
            # 特殊处理代理配置
            if "proxy" in browser_config and isinstance(browser_config["proxy"], dict):
                browser_config["proxy"] = ProxySettings(**browser_config["proxy"])
            update_basic_config("browser", BrowserSettings)

        # 处理搜索配置
        update_basic_config("search", SearchSettings)

        # 处理沙箱配置
        update_basic_config("sandbox", SandboxSettings)

        # 处理MCP配置
        if "mcp" in config_dict:
            mcp_config = config_dict["mcp"]
            current_mcp = self._config.mcp_config or MCPSettings()
            current_dict = {k: getattr(current_mcp, k) for k in current_mcp.model_fields}

            # 特殊处理servers字段
            if "servers" in mcp_config and isinstance(mcp_config["servers"], dict):
                updated_servers = {}
                for server_id, server_config in mcp_config["servers"].items():
                    updated_servers[server_id] = MCPServerConfig(**server_config)
                mcp_config["servers"] = updated_servers

            updated_mcp = {**current_dict, **mcp_config}
            self._config.mcp_config = MCPSettings(**updated_mcp)

        # 处理RAG配置
        if "ragflow" in config_dict:
            update_basic_config("rag", RagSettings)

        # 处理意图配置
        if "intent" in config_dict:
            update_basic_config("intent", IntentSettings)

        # 处理A2A配置
        if "a2a" in config_dict:
            a2a_config = config_dict["a2a"]
            current_a2a = self._config.a2a_config or A2ASettings()
            current_dict = {k: getattr(current_a2a, k) for k in current_a2a.model_fields}

            # 特殊处理servers字段
            if "servers" in a2a_config and isinstance(a2a_config["servers"], dict):
                updated_servers = {}
                for server_id, server_config in a2a_config["servers"].items():
                    updated_servers[server_id] = A2AServerConfig(**server_config)
                a2a_config["servers"] = updated_servers

            updated_a2a = {**current_dict, **a2a_config}
            self._config.a2a_config = A2ASettings(**updated_a2a)

        # 处理数据库配置
        if "database" in config_dict:
            update_basic_config("database", DatabaseSettings)

config = Config()

if __name__ == '__main__':
    print(config.database_config)
    config.update_config({
        "database": {
            "host": "db.example.com",
            "max_connections": 500,
            "port": 3307

        }
    })
    print(config.database_config.host)
    print(config.a2a_config)

