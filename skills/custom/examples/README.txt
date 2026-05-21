This bundle is intended to be mounted or copied as DeerFlow's custom skills root.

Recommended mount target:
  /mnt/skills/custom

Expected video input directory:
  /mnt/data/videos

Expected output directory:
  /mnt/data/video-monitoring-runs

Example command inside the DeerFlow sandbox:
  python /mnt/skills/custom/frame-sampling/scripts/run.py \
    --input /mnt/data/video-monitoring-runs/run_001/frame_sampling_input.json \
    --config /mnt/skills/custom/configs/deerflow_config.json \
    --output /mnt/data/video-monitoring-runs/run_001/frame_sampling_result.json

