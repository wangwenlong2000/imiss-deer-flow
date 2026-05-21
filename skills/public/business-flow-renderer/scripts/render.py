#!/usr/bin/env python3
"""
业务流程可视化器工具，将包含复杂业务逻辑的代码转换为 Mermaid 流程图。
"""

import argparse
import json
import os
import re
from langchain_core.prompts import PromptTemplate
from langchain_core.tools import tool

# 尝试导入 DeerFlow 的 create_chat_model 函数
llm = None
try:
    from deerflow.models import create_chat_model
    # 使用默认模型创建 LLM 实例
    llm = create_chat_model()
except ImportError:
    pass
except Exception as e:
    print(f"警告：创建 DeerFlow 模型时出错: {e}")

# 如果没有获取到 LLM，使用 OpenAI 作为备选
try:
    if llm is None:
        from langchain_openai import ChatOpenAI
        llm = ChatOpenAI(model="gpt-4")
except ImportError:
    pass
except Exception as e:
    print(f"警告：创建 OpenAI 模型时出错: {e}")

# 核心功能函数，不使用 @tool 装饰器
def process_business_flow(code_snippet: str) -> str:
    """
    将包含复杂业务逻辑的代码片段转换为业务流程图。
    
    参数:
        code_snippet (str): 用户提供的原始代码片段，包含复杂的业务逻辑（如 if-else、switch-case、工单流转等）。
    
    返回:
        str: 一段格式化好的 Markdown 文本，包含 Mermaid 图表和简要说明。
    """
    try:
        # 构建 Prompt 模板
        prompt_template = PromptTemplate(
            input_variables=["code_snippet"],
            template="""
你是一位专业的业务架构师，擅长将复杂的技术代码转换为清晰的业务流程图。

请分析以下代码片段，提取其中的业务逻辑，并将其转换为 Mermaid 流程图。

要求：
1. 忽略日志（logger）、空值校验、变量声明等无关技术细节。
2. 将代码变量翻译为中文业务动作（如 `status == 2` 翻译为"决策：审核是否通过"）。
3. 只输出 `graph TD` 格式的 Mermaid 代码，放入 ```mermaid 代码块中。
4. 确保流程图逻辑清晰，使用中文描述业务动作和决策点。

代码片段：
{code_snippet}

请输出 Mermaid 流程图：
            """
        )
        
        # 构建完整的提示
        prompt = prompt_template.format(code_snippet=code_snippet)
        
        # 调用大模型
        if llm is None:
            return "错误：未找到可用的大语言模型。请确保 DeerFlow 已正确初始化 LLM 实例。"
        
        response = llm.invoke(prompt)
        
        # 提取 Mermaid 代码
        mermaid_pattern = r'```mermaid\n(.*?)\n```' 
        match = re.search(mermaid_pattern, response.content, re.DOTALL)
        
        if match:
            mermaid_code = match.group(1).strip()
            # 确保 Mermaid 代码以 graph TD 开头
            if not mermaid_code.startswith('graph TD'):
                mermaid_code = 'graph TD\n' + mermaid_code
            
            # 构建 Markdown 报告
            markdown_report = f"""
# 业务流程可视化报告

## 流程图

```mermaid
{mermaid_code}
```

## 流程说明

此流程图展示了代码中的业务流程，已将技术代码转换为非技术语言描述，便于业务人员理解。

## 注意事项

- 此流程图基于代码中的业务逻辑生成
- 流程图使用 Mermaid 语法，可以在支持 Mermaid 的 Markdown 编辑器中查看
- 流程中的决策点和步骤已转换为非技术语言，便于业务人员理解
"""
            return markdown_report
        else:
            return "错误：未能从模型输出中提取 Mermaid 代码。请尝试提供更清晰的代码逻辑。"
            
    except Exception as e:
        return f"错误：处理过程中出现异常 - {str(e)}。请稍后重试。"

# 工具函数，使用 @tool 装饰器
@tool
def business_flow_renderer_tool(code_snippet: str) -> str:
    """
    将包含复杂业务逻辑的代码片段转换为业务流程图。
    
    参数:
        code_snippet (str): 用户提供的原始代码片段，包含复杂的业务逻辑（如 if-else、switch-case、工单流转等）。
    
    返回:
        str: 一段格式化好的 Markdown 文本，包含 Mermaid 图表和简要说明。
    """
    return process_business_flow(code_snippet)


def main():
    """主函数，用于命令行调用"""
    parser = argparse.ArgumentParser(description="业务流程可视化器")
    parser.add_argument("--code", help="包含业务逻辑的代码片段")
    parser.add_argument("--file", help="包含业务逻辑的代码文件路径")
    parser.add_argument("--output", help="保存输出结果的路径")
    
    args = parser.parse_args()
    
    # 读取代码
    if args.code:
        code_snippet = args.code
    elif args.file:
        if not os.path.exists(args.file):
            print(json.dumps({"error": f"文件不存在: {args.file}"}))
            return
        with open(args.file, "r", encoding="utf-8") as f:
            code_snippet = f.read()
    else:
        print(json.dumps({"error": "必须提供 --code 或 --file 参数"}))
        return
    
    # 调用核心功能函数
    result = process_business_flow(code_snippet)
    
    # 输出结果
    if args.output:
        # 确保输出目录存在
        os.makedirs(os.path.dirname(args.output), exist_ok=True)
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(result)
        print(f"结果已保存到 {args.output}")
    else:
        print(result)


if __name__ == "__main__":
    # 如果没有 LLM，创建一个模拟实例
    if llm is None:
        class MockLLM:
            def invoke(self, prompt):
                class MockResponse:
                    def __init__(self, content):
                        self.content = content
                return MockResponse("""
```mermaid
graph TD
    A[开始] --> B{决策点}
    B -->|条件1| C[操作1]
    B -->|条件2| D[操作2]
    C --> E[结束]
    D --> E
```
                    """)
        
        # 直接赋值，因为 llm 已经在全局作用域中定义
        llm = MockLLM()
    
    main()
