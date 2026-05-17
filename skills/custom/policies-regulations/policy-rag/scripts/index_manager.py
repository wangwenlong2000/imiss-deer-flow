#!/usr/bin/env python3
"""
索引管理器 - 支持缓存和增量更新

解决每次检索都重新计算 embedding 的性能问题
"""

import hashlib
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from rag_system import (
    DashScopeEmbeddingClient,
    INDEX_CACHE_DIR,
    READONLY_FLOWS_DIR,
    log,
)


DEFAULT_FLOWS_DIR = Path(os.getenv("RAG_FLOWS_DIR", str(READONLY_FLOWS_DIR)))


class FlowIndexManager:
    """审批流程索引管理器 - 支持缓存和增量更新"""

    def __init__(self, flows_dir: str, embedder: DashScopeEmbeddingClient):
        self.flows_dir = Path(flows_dir)
        self.embedder = embedder
        self.cache_dir = INDEX_CACHE_DIR
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        self.index_file = self.cache_dir / "flow_index_cache.json"
        self.metadata_file = self.cache_dir / "flow_index_metadata.json"

        self.flows: List[Dict] = []
        self.flow_embeddings: List[np.ndarray] = []
        self.level3_groups: Dict[str, List[Dict]] = {}

    def _get_file_hash(self, filepath: Path) -> str:
        stat = filepath.stat()
        content = f"{filepath.name}-{stat.st_size}-{stat.st_mtime}"
        return hashlib.md5(content.encode()).hexdigest()

    def _load_cache_metadata(self) -> Dict:
        if self.metadata_file.exists():
            with open(self.metadata_file, "r", encoding="utf-8") as f:
                return json.load(f)
        return {"files": {}, "last_update": None}

    def _save_cache_metadata(self, metadata: Dict) -> None:
        with open(self.metadata_file, "w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)

    def _load_cached_embeddings(self) -> Optional[List[np.ndarray]]:
        npz_file = self.cache_dir / "flow_embeddings.npz"
        if npz_file.exists():
            data = np.load(npz_file, allow_pickle=True)
            return [data[f"arr_{i}"] for i in range(len(data.files))]
        return None

    def _save_cached_embeddings(self, embeddings: List[np.ndarray]) -> None:
        npz_file = self.cache_dir / "flow_embeddings.npz"
        save_dict = {f"arr_{i}": emb for i, emb in enumerate(embeddings)}
        np.savez_compressed(npz_file, **save_dict)

    def _flow_signature(self, flow: Dict) -> str:
        parts = [
            flow.get("流程名称", ""),
            flow.get("具体事项", ""),
            flow.get("适用范围", ""),
            flow.get("部门", ""),
            flow.get("审批路径", ""),
            flow.get("_embedding_text", ""),
        ]
        return hashlib.md5("|".join(parts).encode("utf-8")).hexdigest()

    def _load_cached_flow_pairs(self) -> List[tuple]:
        if not self.index_file.exists():
            return []
        try:
            with open(self.index_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            cached_flows = data.get("flows", [])
            cached_embeddings = self._load_cached_embeddings() or []
            if len(cached_flows) != len(cached_embeddings):
                return []
            return list(zip(cached_flows, cached_embeddings))
        except Exception as e:
            log(f"读取缓存配对失败: {e}", "WARN")
            return []

    def _detect_changes(self) -> tuple:
        metadata = self._load_cache_metadata()
        current_files = {}
        json_files = list(self.flows_dir.glob("*_flows.json")) + list(self.flows_dir.glob("*_v2.json"))
        for filepath in json_files:
            current_files[str(filepath)] = self._get_file_hash(filepath)
        cached_files = metadata.get("files", {})

        changed_files = []
        new_files = []
        for filepath, file_hash in current_files.items():
            if filepath not in cached_files:
                new_files.append(filepath)
            elif cached_files[filepath] != file_hash:
                changed_files.append(filepath)
        deleted_files = [f for f in cached_files if f not in current_files]
        return changed_files, new_files, deleted_files

    def build_or_update_index(self, force_rebuild: bool = False) -> bool:
        changed_files, new_files, deleted_files = self._detect_changes()
        has_changes = bool(changed_files or new_files or deleted_files)
        if not force_rebuild and not has_changes and self.index_file.exists():
            log("使用缓存的索引（文件未变化）")
            return self._load_from_cache()

        log(f"检测到文件变化: {len(changed_files)} 修改, {len(new_files)} 新增, {len(deleted_files)} 删除")
        self._load_all_flows()

        log("正在计算 embedding（优先复用缓存）...")
        cached_emb_map: Dict[str, np.ndarray] = {}
        if not force_rebuild:
            for cached_flow, cached_emb in self._load_cached_flow_pairs():
                cached_emb_map[self._flow_signature(cached_flow)] = cached_emb

        self.flow_embeddings = [None] * len(self.flows)  # type: ignore
        missing_indexes: List[int] = []
        missing_texts: List[str] = []
        reused_count = 0

        for idx, flow in enumerate(self.flows):
            signature = self._flow_signature(flow)
            cached = cached_emb_map.get(signature)
            if cached is not None:
                self.flow_embeddings[idx] = cached
                reused_count += 1
            else:
                missing_indexes.append(idx)
                missing_texts.append(flow.get("_embedding_text", ""))

        if missing_texts:
            log(f"重算 {len(missing_texts)} 个流程向量，复用 {reused_count} 个")
            new_embeddings = self.embedder.get_embeddings_batch(missing_texts, batch_size=10)
            for idx, emb in zip(missing_indexes, new_embeddings):
                self.flow_embeddings[idx] = emb
        else:
            log(f"全部复用缓存向量：{reused_count} 个")

        default_dim = next((emb.shape[0] for emb in self.flow_embeddings if emb is not None), self.embedder.dimension)
        self.flow_embeddings = [emb if emb is not None else np.zeros(default_dim, dtype=np.float32) for emb in self.flow_embeddings]

        self._build_level3_groups()
        self._save_to_cache()

        metadata = {"files": {}, "last_update": datetime.now().isoformat(), "flow_count": len(self.flows)}
        json_files = list(self.flows_dir.glob("*_flows.json")) + list(self.flows_dir.glob("*_v2.json"))
        for filepath in json_files:
            metadata["files"][str(filepath)] = self._get_file_hash(filepath)
        self._save_cache_metadata(metadata)

        log(f"索引更新完成：共 {len(self.flows)} 个流程", "SUCCESS")
        return True

    def _load_all_flows(self) -> None:
        self.flows = []
        json_files = list(self.flows_dir.glob("*_flows.json")) + list(self.flows_dir.glob("*_v2.json"))
        log(f"找到 {len(json_files)} 个流程文件")
        for file in json_files:
            try:
                with open(file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        for flow in data:
                            if "_embedding_text" not in flow:
                                flow["_embedding_text"] = self._build_embedding_text(flow)
                            self.flows.append(flow)
            except Exception as e:
                log(f"加载文件失败 {file}: {e}", "WARN")
        log(f"已加载 {len(self.flows)} 个流程")

    def _build_embedding_text(self, flow: Dict) -> str:
        parts = [
            f"流程：{flow.get('流程名称', '')}",
            f"事项：{flow.get('具体事项', '')}",
            f"适用范围：{flow.get('适用范围', '')}",
            f"部门：{flow.get('部门', '')}",
        ]
        for node in flow.get("审批节点", []):
            roles = "、".join(node.get("roles", []))
            parts.append(f"{node['level']}{node['type']}：{roles}")
        if flow.get("备注") and flow["备注"] not in ["✔", "nan", ""]:
            parts.append(f"备注：{flow['备注']}")
        return "\n".join(parts)

    def _build_level3_groups(self) -> None:
        from collections import defaultdict

        self.level3_groups = defaultdict(list)
        for flow in self.flows:
            level3_key = self._get_level3_key(flow)
            if level3_key:
                self.level3_groups[level3_key].append(flow)
        log(f"发现 {len(self.level3_groups)} 个三级流程分组")

    def _get_level3_key(self, flow: Dict) -> str:
        name = flow.get("流程名称", "")
        parts = name.split(" / ")
        return parts[2] if len(parts) >= 3 else name

    def _save_to_cache(self) -> None:
        flows_data = []
        for flow in self.flows:
            flow_copy = {k: v for k, v in flow.items() if k != "embedding"}
            flows_data.append(flow_copy)
        with open(self.index_file, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "flows": flows_data,
                    "level3_groups": {k: [item.get("流程名称", "") for item in v] for k, v in self.level3_groups.items()},
                },
                f,
                ensure_ascii=False,
                indent=2,
            )
        self._save_cached_embeddings(self.flow_embeddings)
        log(f"索引已缓存到 {self.cache_dir}")

    def _load_from_cache(self) -> bool:
        try:
            with open(self.index_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                self.flows = data["flows"]
            self.flow_embeddings = self._load_cached_embeddings()
            if not self.flow_embeddings or len(self.flow_embeddings) != len(self.flows):
                log("缓存数据不完整，需要重建索引")
                return False
            self._build_level3_groups()
            log(f"从缓存加载：{len(self.flows)} 个流程")
            return True
        except Exception as e:
            log(f"加载缓存失败: {e}", "WARN")
            return False

    def get_stats(self) -> Dict:
        metadata = self._load_cache_metadata()
        return {
            "flow_count": len(self.flows),
            "level3_groups": len(self.level3_groups),
            "last_update": metadata.get("last_update", "从未"),
            "cached_files": len(metadata.get("files", {})),
            "cache_dir": str(self.cache_dir),
        }


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="索引管理工具")
    parser.add_argument("command", choices=["build", "stats", "clear"], help="命令")
    parser.add_argument("--flows-dir", default=str(DEFAULT_FLOWS_DIR), help="流程数据目录")
    parser.add_argument("--force", action="store_true", help="强制重建")
    args = parser.parse_args()

    if args.command == "build":
        embedder = DashScopeEmbeddingClient()
        manager = FlowIndexManager(args.flows_dir, embedder)
        manager.build_or_update_index(force_rebuild=args.force)
    elif args.command == "stats":
        embedder = DashScopeEmbeddingClient()
        manager = FlowIndexManager(args.flows_dir, embedder)
        if manager._load_from_cache():
            stats = manager.get_stats()
            print("\n索引统计信息:")
            print(f"  流程总数: {stats['flow_count']}")
            print(f"  三级分组: {stats['level3_groups']}")
            print(f"  最后更新: {stats['last_update']}")
            print(f"  缓存文件: {stats['cached_files']}")
            print(f"  缓存目录: {stats['cache_dir']}")
        else:
            print("没有找到缓存索引，请先运行 build 命令")
    elif args.command == "clear":
        if INDEX_CACHE_DIR.exists():
            import shutil
            shutil.rmtree(INDEX_CACHE_DIR)
            print(f"已清除缓存: {INDEX_CACHE_DIR}")
        else:
            print("缓存目录不存在")


if __name__ == "__main__":
    main()
