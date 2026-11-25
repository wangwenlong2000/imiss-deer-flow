from typing import Dict, List, Any, Optional, Tuple, Union

from python_a2a.models import Message, TextContent, MessageRole
from python_a2a.client.base import BaseA2AClient


class AIAgentRouter:
    """
    使用大型语言模型智能地将查询路由到最合适的代理。

    此路由器分析查询和对话上下文，确定网络中哪个智能体最适合处理请求。
    通过使用简洁的提示和缓存智能体描述来优化token使用。
    """

    def __init__(
            self,
            llm_client: BaseA2AClient,
            agent_network: 'AgentNetwork',
            system_prompt: Optional[str] = None,
            max_history_tokens: int = 500
    ):
        """
        初始化AI智能体路由器。

        参数:
            llm_client: 用于做路由决策的LLM客户端
            agent_network: 可用智能体的网络
            system_prompt: 路由器的自定义系统提示
            max_history_tokens: 从对话历史中包含的最大token数
        """
        self.llm = llm_client
        self.agent_network = agent_network
        self.agent_descriptions = self._gather_agent_descriptions()
        self.max_history_tokens = max_history_tokens
        self.system_prompt = system_prompt or self._create_default_system_prompt()

        # 智能体选择的缓存，避免对类似查询重复调用LLM
        self._selection_cache = {}

    def _create_default_system_prompt(self) -> str:
        """创建路由器的默认系统提示。"""
        return (
            "You are an agent router that determines which specialized agent "
            "should handle a user query. Analyze the query's intent and select "
            "the most appropriate agent based on its capabilities. Respond with "
            "only the agent name, nothing else."
        )

    def _gather_agent_descriptions(self) -> Dict[str, Dict[str, Any]]:
        """从所有智能体收集简明的描述和能力。"""
        descriptions = {}

        for name, agent in self.agent_network.agents.items():
            # 只提取必要信息以保持提示简短
            try:
                agent_card = getattr(agent, 'agent_card', None)
                if agent_card:
                    descriptions[name] = {
                        "description": agent_card.description,
                        "skills": [{"name": skill.name, "description": skill.description}
                                   for skill in agent_card.skills[:3]],  # 限制为前3个技能
                        "tags": list(set(tag for skill in agent_card.skills
                                         for tag in skill.tags[:5]))  # 限制标签，移除重复
                    }
                else:
                    # 对没有卡片的智能体的后备方案
                    descriptions[name] = {
                        "description": f"一般查询的'{name}'智能体",
                        "skills": [],
                        "tags": []
                    }
            except Exception as e:
                # 优雅地处理任何错误
                descriptions[name] = {
                    "description": f"'{name}'智能体（详情不可用）",
                    "skills": [],
                    "tags": []
                }

        return descriptions

    def _create_routing_prompt(self, query: str, conversation_history: Optional[List[Dict]] = None) -> str:
        """
        为路由器LLM创建提示。

        参数:
            query: 要路由的用户输入
            conversation_history: 可选的对话历史上下文

        返回:
            给LLM的简洁提示
        """
        # 创建可用智能体的紧凑描述
        agent_descriptions = "\n".join([
            f"智能体: {name}\n描述: {info['description']}\n"
            f"技能: {', '.join(skill['name'] for skill in info['skills'])}\n"
            f"标签: {', '.join(info['tags'])}"
            for name, info in self.agent_descriptions.items()
        ])

        # 如果提供了历史记录，则包含截断的历史
        history_text = ""
        if conversation_history:
            # 只包含最近和最相关的消息
            recent_history = conversation_history[-3:]  # 最多最后3条消息
            history_text = "\n最近对话:\n" + "\n".join([
                f"{'用户' if msg.get('role') == 'user' else '智能体'}: {msg.get('content', {}).get('text', '')}"
                for msg in recent_history
                if isinstance(msg, dict) and 'role' in msg and 'content' in msg
            ])

        # 构建最终提示
        prompt = (
            f"可用智能体:\n{agent_descriptions}\n\n"
            f"用户查询: {query}\n{history_text}\n\n"
            "哪个智能体应该处理这个查询？只回复智能体名称。"
        )

        return prompt

    def _parse_agent_selection(self, response: str) -> str:
        """
        解析LLM的响应以提取所选智能体名称。

        参数:
            response: LLM的响应文本

        返回:
            所选智能体的名称
        """
        # 清理响应并仅提取智能体名称
        agent_name = response.strip().lower()

        # 处理多行响应，取第一行
        if "\n" in agent_name:
            agent_name = agent_name.split("\n")[0].strip()

        # 删除LLM可能添加的常见前缀
        for prefix in ["agent:", "agent name:", "the agent", "i recommend", "recommended agent:"]:
            if agent_name.startswith(prefix):
                agent_name = agent_name[len(prefix):].strip()

        # 如果存在引号则移除
        agent_name = agent_name.strip('"\'')

        # 检查是否与我们的任何智能体名称匹配（不区分大小写）
        for name in self.agent_descriptions:
            if name.lower() == agent_name or name.lower() in agent_name:
                return name

        # 如果没有精确匹配，尝试找到最接近的匹配
        for name in self.agent_descriptions:
            if name.lower() in agent_name or agent_name in name.lower():
                return name

        # 如果没有找到匹配项，默认使用第一个智能体
        if self.agent_descriptions:
            return next(iter(self.agent_descriptions.keys()))

        # 最后的手段
        return agent_name

    def route_query(
            self,
            query: str,
            conversation_history: Optional[List[Dict]] = None,
            use_cache: bool = True
    ) -> Tuple[str, float]:
        """
        将查询路由到最合适的智能体。

        参数:
            query: 要路由的查询
            conversation_history: 可选的对话历史上下文
            use_cache: 是否对类似查询使用缓存结果

        返回:
            (agent_name, confidence_score)的元组
        """
        # 检查缓存中是否有类似查询，以节省token
        if use_cache:
            cache_key = query.lower().strip()
            if cache_key in self._selection_cache:
                return self._selection_cache[cache_key]

        # 创建路由提示
        prompt = self._create_routing_prompt(query, conversation_history)

        # 让LLM选择最佳智能体
        message = Message(
            content=TextContent(text=prompt),
            role=MessageRole.USER
        )

        try:
            response = self.llm.send_message(message)
            agent_name = self._parse_agent_selection(response.content.text)
            confidence = 0.9  # 默认置信度分数

            # 存入缓存
            if use_cache:
                self._selection_cache[query.lower().strip()] = (agent_name, confidence)

            return agent_name, confidence

        except Exception as e:
            # LLM失败时的后备逻辑
            # 使用基本关键词匹配作为备用
            return self._fallback_routing(query)

    def _fallback_routing(self, query: str) -> Tuple[str, float]:
        """
        LLM失败时路由查询的后备方法。
        针对智能体描述使用基本关键词匹配。

        参数:
            query: 要路由的查询

        返回:
            (agent_name, confidence_score)的元组
        """
        query_lower = query.lower()
        best_match = None
        best_score = 0

        for name, info in self.agent_descriptions.items():
            score = 0

            # 检查描述
            if any(keyword in query_lower for keyword in info['description'].lower().split()):
                score += 1

            # 检查技能名称和描述
            for skill in info['skills']:
                if skill['name'].lower() in query_lower:
                    score += 2
                if any(keyword in query_lower for keyword in skill['description'].lower().split()):
                    score += 1

            # 检查标签
            for tag in info['tags']:
                if tag.lower() in query_lower:
                    score += 3

            if score > best_score:
                best_score = score
                best_match = name

        # 如果没有好的匹配，返回第一个智能体
        if best_match is None and self.agent_descriptions:
            best_match = next(iter(self.agent_descriptions.keys()))
            best_score = 0

        # 将分数归一化为0到1之间的置信度值
        max_possible_score = 10  # 近似最大可能分数
        confidence = min(best_score / max_possible_score, 1.0) if best_score > 0 else 0.1

        return best_match, confidence
