#!/usr/bin/env python3
"""
命令行工具，用于调用代码切分器。
"""

import argparse
import json
import os
import sys

# 添加父目录到路径，以便导入 code_splitter 模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from code_splitter import get_code_splitter


def main():
    """
    主函数，处理命令行参数并调用代码切分器。
    """
    parser = argparse.ArgumentParser(description="代码切分工具")
    
    # 基本参数
    parser.add_argument("--strategy", choices=["langchain", "llamaindex"], default="langchain",
                      help="切分策略")
    parser.add_argument("--code", help="要切分的代码字符串")
    parser.add_argument("--file", help="包含要切分的代码的文件路径")
    parser.add_argument("--language", required=True, help="代码的编程语言")
    parser.add_argument("--metadata", type=str, help="附加的元数据，JSON 格式")
    
    # LangChain 特定参数
    parser.add_argument("--chunk-size", type=int, default=1000, help="切分后的代码块大小")
    parser.add_argument("--chunk-overlap", type=int, default=200, help="代码块之间的重叠大小")
    
    # 输出参数
    parser.add_argument("--output", help="保存输出结果的文件路径")
    
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
    
    # 解析元数据
    metadata = None
    if args.metadata:
        try:
            metadata = json.loads(args.metadata)
        except json.JSONDecodeError:
            print(json.dumps({"error": "元数据格式无效，必须是 JSON 格式"}))
            return
    
    # 准备切分器参数
    splitter_kwargs = {}
    if args.strategy == "langchain":
        splitter_kwargs["chunk_size"] = args.chunk_size
        splitter_kwargs["chunk_overlap"] = args.chunk_overlap
    
    try:
        # 获取切分器实例
        splitter = get_code_splitter(strategy=args.strategy, **splitter_kwargs)
        
        # 切分代码
        result = splitter.split_code(code=code, language=args.language, metadata=metadata)
        
        # 输出结果
        if args.output:
            # 确保输出目录存在
            output_dir = os.path.dirname(args.output)
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)
            with open(args.output, "w", encoding="utf-8") as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
            print(f"结果已保存到 {args.output}")
        else:
            print(json.dumps(result, ensure_ascii=False, indent=2))
            
    except Exception as e:
        print(json.dumps({"error": str(e)}))


if __name__ == "__main__":
    main()
