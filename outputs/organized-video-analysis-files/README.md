# Video Agent Files Bundle

This directory collects the files needed for the requested video-agent checks.

## Input Videos

- `input_videos/Vedio-demo/fight.mp4`
  - Requested runtime path: `/mnt/datasets/Vedio-demo/fight.mp4`
  - Use: `single-video-event-analysis` for fight, fall-down, and abnormal-event review.
- `input_videos/Vedio-demo/Trafic.mp4`
  - Requested runtime path: `/mnt/datasets/Vedio-demo/Trafic.mp4`
  - Use: ingest into `citybrain-video-library` with `camera_id=CAM_DEERFLOW_001`, then search by `Trafic` and `person/car`.
- `input_videos/shanghaitech-agent-eval/07_007.avi`
  - Requested runtime path: `/mnt/datasets/shanghaitech-agent-eval/07_007.avi`
  - Use: ShanghaiTech true-chain checks and StreetModel embedding tests.

Checksums are in `manifests/video_sha256.txt`.

## Skill Files

`skills_custom/` contains the relevant local custom skills:

- `single-video-event-analysis`
- `batch-video-ingestion`
- `video-search`
- `object-statistics`
- `evidence-package-generation`
- `video-embedding-index`
- `ffmpeg-utils`
- `deerflow_config.json`

In the original test prompts these are referenced as `/mnt/skills/custom/...`.
On this host, the source copies came from `skills/custom/...`.

## Test Drivers

- `test_drivers/run_agent_skill_tests.py`
  - Covers `fight.mp4`, `Trafic.mp4`, `citybrain-video-library`, and the standard video skills.
- `test_drivers/run_shanghaitech_es_agent_tests.py`
  - Covers ShanghaiTech `07_007.avi`, ES ingestion/search/statistics/evidence, and StreetModel query/video embedding cases.

## Historical Results

- `historical_results/single-video-event-analysis/`
  - Existing analysis output for `fight.mp4`, including timeline, event candidate, evidence frame ids, and human-review decision.
- `historical_results/20260602-010655-shanghaitech-es/`
  - Existing ShanghaiTech true-chain results for ingestion, search, object statistics, evidence package generation, deterministic embedding, and StreetModel query/video tests.
- `historical_results/20260602-011116-shanghaitech-es/`
  - Existing StreetModel video embedding path-mapping failure case.

Large raw SSE logs were intentionally excluded.

## Current Path Note

The requested `/mnt/datasets/...` and `/mnt/skills/custom/...` paths were not present in the current host view when checked. The usable local copies are organized here and can be mapped or copied into those runtime paths as needed.

Full file list: `manifests/file_list.txt`.
