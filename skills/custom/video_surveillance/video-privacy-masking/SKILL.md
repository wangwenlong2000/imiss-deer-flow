---
name: video-privacy-masking
description: Blur, pixelate, or black out sensitive regions across an entire video and write a redacted video file that can be released externally. Use when the user asks for a 脱敏视频 / masked video / 打码后的视频 rather than a single masked image — for example "把人脸和车牌打码后导出一份可以公开的视频". Regions must be supplied by the caller as bbox coordinates, optionally with a start/end time window. For masking one evidence still image, use privacy-masking instead. This skill does not detect faces or plates, and never recognizes identity or reads plate numbers.
---

# Video Privacy Masking

## Purpose

Produce a redacted **video** for external release. `privacy-masking` covers a single
evidence image; this skill covers the video itself and preserves the audio track.

Masking a region does not require recognizing what is inside it. This skill blurs
coordinates it is given — it does not perform face recognition, identity matching,
or license-plate OCR, and it must not be described as doing so.

## Execution Priority

Run this skill's `scripts/run.py` first. Only write custom ffmpeg or OpenCV code if
the script cannot cover the request.

Do not edit this skill's files during business execution. If the script fails,
report the concrete failure and stop unless the user explicitly asked for debugging.

## Input Resolution

This skill requires explicit `sensitive_regions`. It has no detector. If the user
has not supplied coordinates:

1. Run `video-object-analytics` or `object-detection` to obtain bounding boxes for
   the objects to be masked, then pass those boxes here.
2. If no coordinates can be obtained, ask the user for the regions instead of
   guessing. Do not mask the whole frame as a substitute.

## Atomic CLI

```bash
python video-privacy-masking/scripts/run.py \
  --video-uri <input.mp4> \
  --sensitive-regions-json <regions.json> \
  --method gaussian_blur \
  --output-video <masked.mp4> \
  --output <result.json>
```

`<regions.json>`:

```json
{
  "sensitive_regions": [
    {"type": "face", "bbox": [100, 100, 300, 260]},
    {"type": "plate", "bbox": [500, 300, 760, 380], "start_seconds": 2, "end_seconds": 8}
  ]
}
```

A region without `start_seconds`/`end_seconds` is masked for the whole clip. A region
with a time window is masked only inside that window.

Parameters: `--video-uri`, `--sensitive-regions-json`, `--method`, `--strength`,
`--output-video`, `--disabled`, `--config`, `--output`.

## Methods

| method | Effect |
| --- | --- |
| `gaussian_blur` (default) | Gaussian blur, `--strength` is the sigma (default 20) |
| `pixelate` | Mosaic; `--strength` controls block size |
| `solid` | Fill the region with solid black |

## Workflow

1. Validate the video path and the caller-supplied region coordinates.
2. Build an ffmpeg `filter_complex` that crops each region, applies the effect, and
   overlays it back at the same position.
3. Write the masked video, copying the audio stream unchanged.
4. Return the masked video URI, regions, method, duration, size, and SHA-256 hash.

## Outputs

Returns `video_uri`, `source_video_uri`, `privacy_masked`, `masked_regions`,
`method`, `strength`, `duration_seconds`, `file_size_bytes`, `sha256`, and
`audio_preserved`.

## Failure Modes

- `MISSING_URI` — no input video given
- `VIDEO_NOT_FOUND` — the input video path does not exist
- `MISSING_SENSITIVE_REGIONS` — no valid bbox coordinates supplied
- `UNSUPPORTED_METHOD` — method is not one of gaussian_blur/pixelate/solid
- `FFMPEG_MISSING` — ffmpeg is not installed in the runtime
- `MASKING_FAILED` — ffmpeg could not write the masked video
- `INPUT_NOT_FOUND` / `INPUT_UNREADABLE` / `INPUT_INVALID_JSON` / `INPUT_INVALID_YAML`

## Constraints

- Never delete or overwrite the original video; the masked file is written alongside it.
- Do not claim a video was masked unless this script returned `status=success`.
- Do not present this skill as identity recognition or plate recognition.

## Dependencies

```yaml
system_packages:
  - ffmpeg
```
