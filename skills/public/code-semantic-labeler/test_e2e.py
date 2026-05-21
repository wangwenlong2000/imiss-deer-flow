#!/usr/bin/env python3
"""C++ 语义标注端到端测试"""

import sys
import os
import json

# 配置路径
sys.path.insert(0, '/mnt/skills/public/code-semantic-labeler')
sys.path.insert(0, '/mnt/skills/public/code-semantic-labeler/scripts')
os.environ['DEER_FLOW_CONFIG_PATH'] = '/mnt/skills/public/code-semantic-labeler/config.yaml'

from semantic_labeler import SemanticLabelingPipeline, RuleBasedLabeler, LLMFallbackLabeler, CodeSummarizer

# ====== 1. 测试 RuleBasedLabeler（本体规则）=====
print("=" * 60)
print("1. RuleBasedLabeler 单元测试")
print("=" * 60)

with open('/mnt/skills/public/code-semantic-labeler/ontology_rules.json', 'r') as f:
    rules = json.load(f)

print(f"   ontology_rules.json 加载成功，共 {len(rules)} 条规则")

# 测试具体匹配
ruler = RuleBasedLabeler(rules)

test_cases = [
    ("std::vector", "容器操作"),
    ("std::map", "容器操作"),
    ("std::sort", "算法应用"),
    ("backtrack", "回溯算法"),
    ("findCombinations", "回溯算法"),
    ("Solution", "算法实现"),
    ("combinationSum", "算法实现"),
    ("std::cout", "标准输出"),
    ("std::cin", "标准输入"),
    ("std::fstream", "文件读写"),
    ("std::unique_ptr", "内存管理"),
    ("std::thread", "并发编程"),
    ("int main", "程序入口"),
    ("test_function", "通用工具"),  # 非C++特定，应走默认
]

for name, expected_type in test_cases:
    result = ruler.label(name)
    match = result["matched_rule"]
    label = result["label"]
    status = "✓" if result["matched"] else "✗"
    expected = label == expected_type
    exp_mark = "✓" if expected else f"✗(期望{expected_type})"
    print(f"   [{status}][{exp_mark}] '{name}' -> 标签: {label}, 规则: {match}")

# ====== 2. 测试 LLMFallbackLabeler 的 fallback 标注 ======
print()
print("=" * 60)
print("2. LLMFallbackLabeler._fallback_label() 单元测试")
print("=" * 60)

# 初始化一个 LLM=None 的 FallbackLabeler —— 强制走 fallback
fallback = LLMFallbackLabeler(rules, labeler=ruler, llm=None)

cpp_names = [
    "std::vector<int>", "std::map<std::string, int>", "std::sort",
    "std::cout", "std::cin", "std::unique_ptr", "std::thread",
    "std::mutex", "backtrack", "combinationSum", "Solution",
    "findCombinations", "int main", "dfs", "bfs",
    "mystery_var", "some_function",  # 不匹配 C++ 模式
]

for name in cpp_names:
    label = fallback._fallback_label(name)
    if label:
        print(f"   '{name}' -> {label}")
    else:
        print(f"   '{name}' -> General (无匹配)")

# ====== 3. 测试 _fallback_summarize（回退摘要）=====
print()
print("=" * 60)
print("3. CodeSummarizer._fallback_summarize() 单元测试")
print("=" * 60)

summarizer = CodeSummarizer(None, None)
cpp_code = """
#include <vector>
#include <iostream>
#include <algorithm>

class Solution {
public:
    std::vector<std::vector<int>> combinationSum(std::vector<int>& candidates, int target) {
        std::vector<std::vector<int>> result;
        std::vector<int> current;
        std::sort(candidates.begin(), candidates.end());
        backtrack(candidates, target, 0, current, result);
        return result;
    }
    
private:
    void backtrack(std::vector<int>& candidates, int target, int start,
                   std::vector<int>& current, std::vector<std::vector<int>>& result) {
        if (target == 0) {
            result.push_back(current);
            return;
        }
        for (int i = start; i < candidates.size(); i++) {
            if (candidates[i] > target) break;
            current.push_back(candidates[i]);
            backtrack(candidates, target - candidates[i], i, current, result);
            current.pop_back();
        }
    }
};
"""
summary = summarizer._fallback_summarize(cpp_code)
print(f"   摘要结果: {summary}")
print(f"   预期包含 'C++': {'C++' in summary or 'c++' in summary.lower()}")
print(f"   预期包含 'combinationSum' 或 'backtrack': {'combinationSum' in summary or 'backtrack' in summary}")

# ====== 4. 测试 LLMFallbackLabeler.label() 完整链路 ======
print()
print("=" * 60)
print("4. LLMFallbackLabeler.label() 完整链路测试")
print("=" * 60)

for name in cpp_names:
    label = fallback.label(name)
    print(f"   '{name}' -> {label}")

# ====== 5. 测试 pipeline process_nodes ======
print()
print("=" * 60)
print("5. SemanticLabelingPipeline.process_nodes 测试")
print("=" * 60)

pipeline = SemanticLabelingPipeline([ruler, fallback])

test_nodes = [
    {"name": "std::vector<int>", "code": "std::vector<int>"},
    {"name": "std::sort", "code": "std::sort(candidates.begin(), candidates.end())"},
    {"name": "backtrack", "code": "void backtrack(...)"},
    {"name": "combinationSum", "code": "vector<vector<int>> combinationSum(...)"},
    {"name": "Solution", "code": "class Solution {...}"},
    {"name": "std::cout", "code": 'std::cout << "hello"'},
    {"name": "int main", "code": "int main() {...}"},
    {"name": "dfs", "code": "void dfs(...)"},
    {"name": "std::unique_ptr", "code": "std::unique_ptr<int> ptr"},
    {"name": "std::thread", "code": "std::thread t(fn)"},
]

labeled = pipeline.process_nodes(test_nodes)
for node in labeled:
    print(f"   '{node['name']}' -> {node['type']}")

# ====== 6. 测试 CodeSummarizer._fallback_summarize ======
print()
print("=" * 60)
print("6. CodeSummarizer 摘要测试")
print("=" * 60)

# 测试 Python 代码也走 fallback
py_code = """
def quicksort(arr):
    if len(arr) <= 1:
        return arr
    pivot = arr[0]
    left = [x for x in arr[1:] if x <= pivot]
    right = [x for x in arr[1:] if x > pivot]
    return quicksort(left) + [pivot] + quicksort(right)
"""
py_summary = summarizer._fallback_summarize(py_code)
print(f"   Python 摘要: {py_summary}")

# 测试空代码
empty_summary = summarizer._fallback_summarize("")
print(f"   空代码摘要: {empty_summary}")

# 测试短代码
short_summary = summarizer._fallback_summarize("int main() { return 0; }")
print(f"   短代码摘要: {short_summary}")

print()
print("=" * 60)
print("所有 C++ 语义标注测试完成！")
print("=" * 60)
