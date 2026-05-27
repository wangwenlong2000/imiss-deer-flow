#!/usr/bin/env python3
"""
命令行工具，用于调用代码语义标注器。
"""

import argparse
import json
import os
import sys

# 添加父目录到路径，以便导入 semantic_labeler 模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from semantic_labeler import (
    RuleBasedLabeler,
    LLMFallbackLabeler,
    SemanticLabelingPipeline,
    DeerFlowOrchestrator,
    CodeSummarizer,
    OntologyManager
)


def main():
    """
    主函数，处理命令行参数并调用代码语义标注器。
    """
    parser = argparse.ArgumentParser(description="代码语义标注器")

    # 输入参数
    parser.add_argument("--code", help="要分析的 Python 代码字符串")
    parser.add_argument("--file", help="包含要分析的代码的文件路径")

    # 输出参数
    parser.add_argument("--output", help="保存输出结果的文件路径")

    # 本体规则参数
    parser.add_argument("--rules-file", help="本体规则文件路径")
    parser.add_argument("--update-rules", action="store_true", help="更新本体规则")
    parser.add_argument("--add-rule", nargs=2, metavar=('PATTERN', 'LABEL'), help="添加本体规则")
    parser.add_argument("--language", default="python", choices=["python", "cpp"], help="代码语言 (默认: python)")

    args = parser.parse_args()

    # 读取代码
    if args.code:
        code = args.code
    elif args.file:
        if not os.path.exists(args.file):
            print(json.dumps({"error": f"文件不存在: {args.file}"}))
            return
        with open(args.file, "r", encoding="utf-8") as f:
            code = f.read()
    else:
        print(json.dumps({"error": "必须提供 --code 或 --file 参数"}))
        return

    try:
        # 初始化本体管理器
        rules_file = args.rules_file or os.path.join(os.path.dirname(__file__), "..", "ontology_rules.json")
        ontology_manager = OntologyManager(rules_file)

        # 处理规则更新
        if args.update_rules:
            pass

        # 处理添加规则
        if args.add_rule:
            pattern, label = args.add_rule
            ontology_manager.add_rule(pattern, label)
            ontology_manager.save_rules()
            print(f"已添加规则: {pattern} -> {label}")
            return

        # 获取本体规则
        ontology_rules = ontology_manager.get_rules()

        # 如果规则为空，使用默认规则
        if not ontology_rules:
            ontology_rules = {
                "cv2": "Camera",
                "model": "Detection",
                "tracker": "Tracking",
                "db": "Database",
                "numpy": "DataProcessing",
                "pandas": "DataProcessing",
                "tensorflow": "MachineLearning",
                "pytorch": "MachineLearning",
            }
            # 保存默认规则
            ontology_manager.update_rules(ontology_rules)
            ontology_manager.save_rules()

        # 创建标注器
        rule_labeler = RuleBasedLabeler(ontology_rules)
        llm_labeler = LLMFallbackLabeler()

        # 创建语义标注管道
        pipeline = SemanticLabelingPipeline([rule_labeler, llm_labeler])

        # 创建代码总结器
        code_summarizer = CodeSummarizer()

        # 创建工作流引擎
        orchestrator = DeerFlowOrchestrator(pipeline, code_summarizer)

        # 处理原始代码
        result = orchestrator.process_raw_code(code, language=args.language)

        # 输出结果
        if args.output:
            # 确保输出目录存在
            os.makedirs(os.path.dirname(args.output), exist_ok=True)
            with open(args.output, "w", encoding="utf-8") as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
            print(f"结果已保存到 {args.output}")
        else:
            print(json.dumps(result, ensure_ascii=False, indent=2))

    except Exception as e:
        print(json.dumps({"error": str(e)}))


if __name__ == "__main__":
    main()
