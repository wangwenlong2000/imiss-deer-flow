#!/usr/bin/env python3
"""
命令行工具，用于调用数据流提取器。
"""

import argparse
import json
import os
import sys

# 添加父目录到路径，以便导入 dataflow_extractor 模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dataflow_extractor import ASTDataflowExtractor


def main():
    """
    主函数，处理命令行参数并调用数据流提取器。
    """
    parser = argparse.ArgumentParser(description="数据流提取器")
    
    # 输入参数
    parser.add_argument("--code", help="要分析的 Python 代码字符串")
    parser.add_argument("--file", help="包含要分析的代码的文件路径")
    
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
    
    try:
        # 创建提取器实例
        extractor = ASTDataflowExtractor()
        
        # 提取数据流
        result = extractor.extract(code)
        
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
