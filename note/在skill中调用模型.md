# DeerFlow Skill 中调用大模型的最佳实践总结

## 核心原则

### 1. 优先使用 DeerFlow 内置模型
- **推荐**：`from deerflow.models import create_chat_model`
- **优势**：与平台深度集成，配置统一，维护简单
- **适用场景**：大多数问答需求

### 2. 根据需求选择合适的方法
| 方法 | 适用场景 | 注意事项 |
|------|---------|----------|
| **DeerFlow 内置模型** | 一般问答，平台集成 | 无需额外配置 |
| **LangGraph API** | 分布式部署，微服务架构 | 需要网络连接，注意超时 |
| **外部 API** | 需要特定模型能力 | 管理 API 密钥，控制成本 |
| **本地模型** | 数据隐私，离线使用 | 硬件要求高，性能有限 |

## 关键实现要点

### 1. 错误处理机制
```python
def safe_qa_call(question: str, method: str) -> str:
    try:
        return answer_question(question, method)
    except requests.exceptions.Timeout:
        return "请求超时，请稍后重试"
    except requests.exceptions.ConnectionError:
        return "网络连接失败，请检查网络"
    except Exception as e:
        return f"系统错误：{str(e)[:100]}..."
```

### 2. 超时控制
```python
import signal

def with_timeout(func, timeout_sec=30):
    """函数执行超时控制"""
    def handler(signum, frame):
        raise TimeoutError(f"函数执行超时 ({timeout_sec}秒)")
    
    signal.signal(signal.SIGALRM, handler)
    signal.alarm(timeout_sec)
    
    try:
        result = func()
        signal.alarm(0)
        return result
    except TimeoutError:
        raise
```

### 3. 缓存策略
```python
from functools import lru_cache
import hashlib

@lru_cache(maxsize=100)
def cached_answer(question: str, method: str) -> str:
    """LRU缓存常用问答"""
    return answer_question(question, method)
```

## 安全注意事项

### 1. API 密钥安全
- 永远不要将 API 密钥硬编码在代码中
- 使用环境变量或密钥管理服务
- 考虑加密存储敏感密钥

### 2. 输入验证
```python
def validate_input(question: str) -> bool:
    """验证用户输入"""
    # 长度限制
    if len(question) > 2000:
        return False
    
    # 防止注入攻击
    dangerous_patterns = ["eval(", "exec(", "import os", "__import__"]
    for pattern in dangerous_patterns:
        if pattern in question.lower():
            return False
    
    return True
```

### 3. 输出过滤
```python
def sanitize_output(text: str) -> str:
    """过滤敏感或不当内容"""
    # 移除可能的敏感信息
    sensitive_patterns = [
        r"API_KEY=[A-Za-z0-9]+",
        r"Bearer\s+[A-Za-z0-9\._\-]+",
        r"密码[:：]\s*[\w@#$%^&*]+"
    ]
    
    for pattern in sensitive_patterns:
        text = re.sub(pattern, "[FILTERED]", text)
    
    return text
```

## 性能优化建议

### 1. 批量处理
```python
def batch_process(questions: list, method: str) -> list:
    """批量处理问题"""
    results = []
    for q in questions:
        results.append(answer_question(q, method))
    return results
```

### 2. 异步处理
```python
import asyncio
import aiohttp

async def async_qa(question: str, method: str) -> str:
    """异步问答"""
    # 实现异步调用逻辑
    pass
```

### 3. 资源管理
```python
class QAResourceManager:
    """问答资源管理器"""
    def __init__(self):
        self.session = None
    
    def __enter__(self):
        self.session = requests.Session()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            self.session.close()
```

## 监控与日志

### 1. 详细日志
```python
import logging

logger = logging.getLogger(__name__)

def answer_with_logging(question: str, method: str) -> str:
    """带日志的问答函数"""
    logger.info(f"开始回答问题：{question[:50]}...")
    start_time = time.time()
    
    try:
        result = answer_question(question, method)
        elapsed = time.time() - start_time
        
        logger.info(f"回答完成，用时：{elapsed:.2f}秒")
        logger.debug(f"回答内容：{result[:100]}...")
        
        return result
    except Exception as e:
        logger.error(f"回答问题失败：{str(e)}")
        raise
```

### 2. 性能监控
```python
class QAMonitor:
    """问答性能监控器"""
    def __init__(self):
        self.stats = {
            "total_calls": 0,
            "success_calls": 0,
            "failed_calls": 0,
            "total_time": 0
        }
    
    def record_call(self, success: bool, elapsed: float):
        self.stats["total_calls"] += 1
        if success:
            self.stats["success_calls"] += 1
        else:
            self.stats["failed_calls"] += 1
        self.stats["total_time"] += elapsed
```

## 测试策略

### 1. 单元测试
```python
import pytest
from unittest.mock import Mock, patch

def test_answer_question():
    """测试问答函数"""
    with patch("requests.post") as mock_post:
        # 模拟 API 响应
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "choices": [{"message": {"content": "测试回答"}}]
        }
        mock_post.return_value = mock_response
        
        result = answer_question("测试问题", "deepseek")
        assert "测试回答" in result
```

### 2. 集成测试
```python
def test_integration():
    """集成测试"""
    # 测试真实环境下的问答
    question = "什么是机器学习？"
    methods = ["deerflow", "langgraph", "deepseek"]
    
    for method in methods:
        try:
            result = answer_question(question, method)
            assert result and len(result) > 10
        except Exception as e:
            # 某些方法可能不可用，这是正常的
            print(f"方法 {method} 不可用：{e}")
```

## 部署配置

### 1. 环境变量配置
```bash
# .env 文件
DEERFLOW_GATEWAY_URL=http://localhost:8000
DEEPSEEK_API_KEY=your_key_here
OPENAI_API_KEY=your_key_here
OLLAMA_HOST=http://localhost:11434
QA_TIMEOUT=30
QA_MAX_RETRIES=3
QA_CACHE_SIZE=100
```

### 2. Docker 配置
```dockerfile
FROM python:3.11-slim

# 安装依赖
RUN pip install deerflow requests

# 复制代码
COPY . /app
WORKDIR /app

# 运行技能
CMD ["python", "scripts/qa_skill.py"]
```

## 故障排除

### 常见问题及解决方案

1. **网络连接失败**
   - 检查网络连接
   - 验证 API 端点 URL
   - 检查防火墙设置

2. **API 密钥无效**
   - 验证环境变量设置
   - 检查密钥权限
   - 确认服务订阅状态

3. **响应超时**
   - 增加超时时间
   - 优化提示词长度
   - 考虑使用更快的模型

4. **内存不足**
   - 减少批量处理大小
   - 优化缓存策略
   - 考虑使用流式响应

## 总结

在 DeerFlow Skill 中集成大模型进行问答，关键在于：

1. **选择合适的调用方式**：根据需求、预算和技术环境选择
2. **实现健壮的错误处理**：网络错误、超时、API限制等
3. **确保安全性**：API密钥管理、输入验证、输出过滤
4. **优化性能**：缓存、批量处理、异步调用
5. **完善的监控和日志**：问题追踪、性能分析
6. **充分的测试**：单元测试、集成测试

遵循这些最佳实践，可以构建出稳定、安全、高效的问答技能，为用户提供优质的问答体验。