# ShanghaiTech ES Skill Agent Test Summary

- started_at: `2026-06-02T01:06:55+0800`
- finished_at: `2026-06-02T01:10:29+0800`
- sample_source: `/home/huangxiao/City_brain/shanghaitech/shanghaitech/training/videos/07_007.avi`
- sandbox_video: `/mnt/datasets/shanghaitech-agent-eval/07_007.avi`
- sample_sha256: `6c7f67d1353ce7d6b914b4ff0809397c9c9348cab376e868a39f726f2220cba1`
- source_index: `huangxiao-shanghaitech-es-agent-test`
- vector_index: `huangxiao-shanghaitech-vector-agent-test`
- agent_chain_passed: `7/7`
- functional_success: `6/7`

## Results

| Case | Skill | Agent Chain | Result | Error | Notes |
| --- | --- | --- | --- | --- | --- |
| `batch-video-ingestion-shanghaitech` | `batch-video-ingestion` | `True` | `success` | `` | {"ingested_count": 1, "failed_count": 0, "index": "huangxiao-shanghaitech-es-agent-test", "documents": [{"video_id": "shanghaitech-07-007-agent-test", "camera_id": "CAM_SHANGHAITECH_07_007", "filename": "07_007.avi", "labels": ["person"]... |
| `video-search-shanghaitech` | `video-search` | `True` | `success` | `` | {"total": 1, "query_mode": "keyword_filter", "index": "huangxiao-shanghaitech-es-agent-test", "vector_field": "vector", "hits": [{"score": 1.8144113, "video_id": "shanghaitech-07-007-agent-test", "camera_id": "CAM_SHANGHAITECH_07_007", "... |
| `object-statistics-shanghaitech` | `object-statistics` | `True` | `success` | `` | {"index": "huangxiao-shanghaitech-es-agent-test", "matched_videos": 1, "total_objects": 4, "total_tracks": 2, "by_label": {"person": 4}} |
| `evidence-package-generation-shanghaitech` | `evidence-package-generation` | `True` | `success` | `` | {"package_id": "PKG_1c5931e913", "manifest_uri": "/mnt/user-data/outputs/evidence-package-generation-shanghaitech/package/manifest.json", "evidence": [{"video_id": "shanghaitech-07-007-agent-test", "event_id": "EVT_SHANGHAITECH_07_007_AG... |
| `video-embedding-index-deterministic-shanghaitech` | `video-embedding-index` | `True` | `success` | `` | {"failed_count": 0, "source_index": "huangxiao-shanghaitech-es-agent-test", "target_index": "huangxiao-shanghaitech-vector-agent-test", "embedded_count": 1, "embedding_provider": "deterministic-hash", "embedding_model": "BAAI/bge-m3", "v... |
| `video-embedding-index-streetmodel-query` | `video-embedding-index` | `True` | `success` | `` | {"embedding_provider": "streetmodel", "embedding_model": "Qwen3-VL-Embedding-2B", "dimensions": 2048, "vector_field": "video_vector-Qwen3-VL-Embedding-2B_urban_governance"} |
| `video-embedding-index-streetmodel-video` | `video-embedding-index` | `True` | `failed` | `VIDEO_COPY_TO_SHARED_FAILED` | {} |
