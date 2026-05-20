#!/usr/bin/env python3
"""
Multi-pass video frame extraction for forensic analysis.

Extracts frames from video files (e.g., body cam footage) using a three-pass
approach: scene detection, coarse sweep, and dense extraction around key
moments. All processing uses ffmpeg/ffprobe -- no third-party Python packages.

Usage:
    python3 extract_frames.py <video_file> [options]

Options:
    --output-dir DIR          Where to save frames (default: auto-generated)
    --coarse-fps FLOAT        FPS for coarse pass (default: 1.0)
    --dense-fps FLOAT         FPS for dense pass (default: 4.0)
    --scene-threshold FLOAT   Scene detection sensitivity 0-1 (default: 0.3)
    --dense-window FLOAT      Seconds around scene changes for dense pass (default: 2.0)
    --chapter-duration INT    Seconds per chapter (default: 300)
    --max-frames INT          Safety limit (default: 2000)
    --offset-pass             Enable Pass 3 -- offset extraction for double coverage
"""
import argparse
import json
import math
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
VIDEO_EXTENSIONS = {".mp4", ".mov", ".avi", ".mkv", ".webm", ".wmv", ".flv", ".ts", ".m4v"}
JPEG_QUALITY = 95
DEDUP_THRESHOLD_MS = 100  # ms proximity for timestamp deduplication


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------
def log(msg: str):
    """Print progress to stderr."""
    print(msg, file=sys.stderr, flush=True)


def format_timestamp(seconds: float) -> str:
    """Convert seconds to H:MM:SS.mmm display format."""
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = seconds % 60
    return f"{h}:{m:02d}:{s:06.3f}"


def ts_to_ms(seconds: float) -> int:
    """Convert seconds to integer milliseconds."""
    return int(round(seconds * 1000))


def frame_filename(timestamp_ms: int) -> str:
    """Generate frame filename from timestamp in ms."""
    return f"frame_{timestamp_ms:09d}ms.jpg"


def check_tools():
    """Verify ffmpeg and ffprobe are available."""
    for tool in ("ffmpeg", "ffprobe"):
        if not shutil.which(tool):
            log(f"ERROR: {tool} is required but not found in PATH.")
            if sys.platform == "darwin":
                log(f"  Install with: brew install ffmpeg")
            else:
                log(f"  Install with: sudo apt-get install -y ffmpeg")
            sys.exit(2)


# ---------------------------------------------------------------------------
# Video metadata via ffprobe
# ---------------------------------------------------------------------------
def get_video_metadata(video_path: str) -> dict:
    """Get video metadata using ffprobe. Returns dict with video info."""
    cmd = [
        "ffprobe", "-v", "quiet",
        "-print_format", "json",
        "-show_format", "-show_streams",
        video_path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        log(f"ERROR: ffprobe failed: {result.stderr}")
        sys.exit(1)

    probe = json.loads(result.stdout)

    # Find the video stream
    video_stream = None
    for stream in probe.get("streams", []):
        if stream.get("codec_type") == "video":
            video_stream = stream
            break

    if not video_stream:
        log("ERROR: No video stream found in file.")
        sys.exit(1)

    # Parse FPS from r_frame_rate (e.g., "30000/1001")
    fps_str = video_stream.get("r_frame_rate", "30/1")
    try:
        num, den = fps_str.split("/")
        fps = float(num) / float(den)
    except (ValueError, ZeroDivisionError):
        fps = 30.0

    # Duration from format or stream
    duration = float(probe.get("format", {}).get("duration", 0))
    if duration == 0:
        duration = float(video_stream.get("duration", 0))

    # Resolution
    width = int(video_stream.get("width", 0))
    height = int(video_stream.get("height", 0))

    # File size
    file_size_bytes = int(probe.get("format", {}).get("size", 0))

    return {
        "path": os.path.abspath(video_path),
        "filename": os.path.basename(video_path),
        "duration_seconds": round(duration, 2),
        "fps": round(fps, 2),
        "resolution": f"{width}x{height}",
        "codec": video_stream.get("codec_name", "unknown"),
        "file_size_mb": round(file_size_bytes / (1024 * 1024), 1),
    }


# ---------------------------------------------------------------------------
# Pass 0: Scene Detection
# ---------------------------------------------------------------------------
def detect_scenes(video_path: str, threshold: float) -> list[dict]:
    """
    Use ffmpeg scene detection to find timestamps of scene changes.
    Returns list of {timestamp_seconds, score}.
    """
    log(f"Pass 0: Scene detection (threshold={threshold})...")

    cmd = [
        "ffmpeg", "-i", video_path,
        "-vf", f"select='gt(scene,{threshold})',showinfo",
        "-vsync", "vfr",
        "-f", "null", "-",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    # ffmpeg writes showinfo to stderr
    output = result.stderr

    scenes = []
    # Parse showinfo lines for pts_time values
    # Example: [Parsed_showinfo_1 @ 0x...] n:   0 pts:   1356 pts_time:45.2 ...
    pattern = re.compile(r"pts_time:\s*([\d.]+)")
    # Also try to grab scene score from select filter debug
    score_pattern = re.compile(r"scene:([\d.]+)")

    for line in output.split("\n"):
        if "pts_time:" not in line:
            continue
        match = pattern.search(line)
        if match:
            ts = float(match.group(1))
            # Try to extract scene score
            score = threshold  # default to threshold if we can't parse
            score_match = score_pattern.search(line)
            if score_match:
                score = float(score_match.group(1))
            scenes.append({
                "timestamp_seconds": round(ts, 3),
                "score": round(score, 3),
            })

    log(f"  Detected {len(scenes)} scene changes.")
    return scenes


# ---------------------------------------------------------------------------
# Pass 1: Coarse Sweep
# ---------------------------------------------------------------------------
def extract_coarse_frames(
    video_path: str,
    output_dir: str,
    fps: float,
    duration: float,
    chapter_duration: int,
    max_frames: int,
) -> tuple[list[dict], int]:
    """
    Extract frames at regular intervals (coarse sweep).
    Returns (list of frame records, total frame count).
    """
    log(f"Pass 1: Coarse sweep at {fps} fps...")

    total_expected = int(math.ceil(duration * fps))
    if total_expected > max_frames:
        log(f"  WARNING: Coarse pass alone would produce {total_expected} frames "
            f"(limit: {max_frames}). Reducing effective duration.")

    frames = []
    frame_count = 0
    num_chapters = max(1, int(math.ceil(duration / chapter_duration)))

    for ch_idx in range(num_chapters):
        if frame_count >= max_frames:
            log(f"  Max frames limit ({max_frames}) reached. Stopping coarse pass.")
            break

        ch_start = ch_idx * chapter_duration
        ch_end = min((ch_idx + 1) * chapter_duration, duration)
        ch_duration = ch_end - ch_start
        ch_num = ch_idx + 1
        ch_dir = os.path.join(output_dir, "chapters", f"chapter_{ch_num:03d}", "frames")
        os.makedirs(ch_dir, exist_ok=True)

        # Use ffmpeg to extract frames for this chapter
        cmd = [
            "ffmpeg", "-y",
            "-ss", str(ch_start),
            "-i", video_path,
            "-t", str(ch_duration),
            "-vf", f"fps={fps}",
            "-qmin", "1", "-q:v", "2",
            os.path.join(ch_dir, "coarse_%06d.jpg"),
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            log(f"  WARNING: ffmpeg failed for chapter {ch_num}: {result.stderr[:200]}")
            continue

        # Rename extracted frames to timestamp-based names
        extracted = sorted(Path(ch_dir).glob("coarse_*.jpg"))
        for i, fpath in enumerate(extracted):
            if frame_count >= max_frames:
                fpath.unlink()
                continue
            ts_seconds = ch_start + (i / fps)
            if ts_seconds > duration:
                fpath.unlink()
                continue
            ts_ms = ts_to_ms(ts_seconds)
            new_name = frame_filename(ts_ms)
            new_path = os.path.join(ch_dir, new_name)
            fpath.rename(new_path)
            frames.append({
                "path": os.path.relpath(new_path, output_dir),
                "timestamp_seconds": round(ts_seconds, 3),
                "timestamp_ms": ts_ms,
                "timestamp_display": format_timestamp(ts_seconds),
                "pass": "coarse",
                "chapter": ch_num,
            })
            frame_count += 1

        # Clean up any remaining coarse_*.jpg that weren't renamed
        for leftover in Path(ch_dir).glob("coarse_*.jpg"):
            leftover.unlink()

        log(f"  Chapter {ch_num}/{num_chapters}: {len(extracted)} frames "
            f"({format_timestamp(ch_start)} - {format_timestamp(ch_end)})")

    log(f"  Coarse pass complete: {frame_count} frames.")
    return frames, frame_count


# ---------------------------------------------------------------------------
# Pass 2: Dense Extraction around Scene Changes
# ---------------------------------------------------------------------------
def merge_windows(scene_times: list[float], window: float, duration: float) -> list[tuple]:
    """
    Merge overlapping dense extraction windows around scene change timestamps.
    Returns list of (start, end) tuples clamped to [0, duration].
    """
    if not scene_times:
        return []

    intervals = []
    for ts in sorted(scene_times):
        start = max(0.0, ts - window)
        end = min(duration, ts + window)
        intervals.append((start, end))

    # Merge overlapping intervals
    merged = [intervals[0]]
    for start, end in intervals[1:]:
        prev_start, prev_end = merged[-1]
        if start <= prev_end:
            merged[-1] = (prev_start, max(prev_end, end))
        else:
            merged.append((start, end))

    return merged


def extract_dense_frames(
    video_path: str,
    output_dir: str,
    scenes: list[dict],
    dense_fps: float,
    window: float,
    duration: float,
    chapter_duration: int,
    existing_timestamps_ms: set,
    max_frames: int,
    current_count: int,
) -> tuple[list[dict], int]:
    """
    Extract frames at higher FPS around scene change points.
    Deduplicates against existing coarse frames by timestamp proximity.
    Returns (list of frame records, new frame count added).
    """
    if not scenes:
        log("Pass 2: No scene changes -- skipping dense extraction.")
        return [], 0

    scene_times = [s["timestamp_seconds"] for s in scenes]
    windows = merge_windows(scene_times, window, duration)

    log(f"Pass 2: Dense extraction at {dense_fps} fps around {len(scenes)} "
        f"scene changes ({len(windows)} merged windows)...")

    frames = []
    added = 0

    for win_start, win_end in windows:
        if current_count + added >= max_frames:
            log(f"  Max frames limit ({max_frames}) reached. Stopping dense pass.")
            break

        win_duration = win_end - win_start

        # Determine which chapter this window primarily falls in
        ch_num = int(win_start // chapter_duration) + 1
        ch_dir = os.path.join(output_dir, "chapters", f"chapter_{ch_num:03d}", "frames")
        os.makedirs(ch_dir, exist_ok=True)

        # Use a temp prefix for dense frames
        cmd = [
            "ffmpeg", "-y",
            "-ss", str(win_start),
            "-i", video_path,
            "-t", str(win_duration),
            "-vf", f"fps={dense_fps}",
            "-qmin", "1", "-q:v", "2",
            os.path.join(ch_dir, "dense_%06d.jpg"),
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            log(f"  WARNING: ffmpeg failed for window "
                f"{format_timestamp(win_start)}-{format_timestamp(win_end)}: "
                f"{result.stderr[:200]}")
            continue

        # Rename and deduplicate
        extracted = sorted(Path(ch_dir).glob("dense_*.jpg"))
        for i, fpath in enumerate(extracted):
            if current_count + added >= max_frames:
                fpath.unlink()
                continue

            ts_seconds = win_start + (i / dense_fps)
            if ts_seconds > duration:
                fpath.unlink()
                continue

            ts_ms = ts_to_ms(ts_seconds)

            # Deduplicate: skip if within DEDUP_THRESHOLD_MS of an existing frame
            is_dup = False
            for existing_ms in existing_timestamps_ms:
                if abs(ts_ms - existing_ms) <= DEDUP_THRESHOLD_MS:
                    is_dup = True
                    break

            if is_dup:
                fpath.unlink()
                continue

            new_name = frame_filename(ts_ms)
            new_path = os.path.join(ch_dir, new_name)
            # Handle potential name collision with coarse frame
            if os.path.exists(new_path):
                fpath.unlink()
                continue

            fpath.rename(new_path)
            existing_timestamps_ms.add(ts_ms)
            frames.append({
                "path": os.path.relpath(new_path, output_dir),
                "timestamp_seconds": round(ts_seconds, 3),
                "timestamp_ms": ts_ms,
                "timestamp_display": format_timestamp(ts_seconds),
                "pass": "dense",
                "chapter": ch_num,
            })
            added += 1

        # Clean up any remaining dense_*.jpg
        for leftover in Path(ch_dir).glob("dense_*.jpg"):
            leftover.unlink()

    log(f"  Dense pass complete: {added} new frames (after dedup).")
    return frames, added


# ---------------------------------------------------------------------------
# Pass 3 (Optional): Offset Extraction
# ---------------------------------------------------------------------------
def extract_offset_frames(
    video_path: str,
    output_dir: str,
    coarse_fps: float,
    duration: float,
    chapter_duration: int,
    existing_timestamps_ms: set,
    max_frames: int,
    current_count: int,
) -> tuple[list[dict], int]:
    """
    Extract at 0.5s offset from coarse timestamps for double coverage.
    Returns (list of frame records, new frame count added).
    """
    log(f"Pass 3: Offset extraction (0.5s offset from coarse)...")

    offset = 0.5 / coarse_fps  # half-step offset
    frames = []
    added = 0
    num_chapters = max(1, int(math.ceil(duration / chapter_duration)))

    for ch_idx in range(num_chapters):
        if current_count + added >= max_frames:
            log(f"  Max frames limit ({max_frames}) reached. Stopping offset pass.")
            break

        ch_start = ch_idx * chapter_duration + offset
        ch_end = min((ch_idx + 1) * chapter_duration, duration)
        if ch_start >= ch_end:
            continue
        ch_duration = ch_end - ch_start
        ch_num = ch_idx + 1
        ch_dir = os.path.join(output_dir, "chapters", f"chapter_{ch_num:03d}", "frames")
        os.makedirs(ch_dir, exist_ok=True)

        cmd = [
            "ffmpeg", "-y",
            "-ss", str(ch_start),
            "-i", video_path,
            "-t", str(ch_duration),
            "-vf", f"fps={coarse_fps}",
            "-qmin", "1", "-q:v", "2",
            os.path.join(ch_dir, "offset_%06d.jpg"),
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            continue

        extracted = sorted(Path(ch_dir).glob("offset_*.jpg"))
        for i, fpath in enumerate(extracted):
            if current_count + added >= max_frames:
                fpath.unlink()
                continue

            ts_seconds = ch_start + (i / coarse_fps)
            if ts_seconds > duration:
                fpath.unlink()
                continue

            ts_ms = ts_to_ms(ts_seconds)

            # Deduplicate
            is_dup = False
            for existing_ms in existing_timestamps_ms:
                if abs(ts_ms - existing_ms) <= DEDUP_THRESHOLD_MS:
                    is_dup = True
                    break

            if is_dup:
                fpath.unlink()
                continue

            new_name = frame_filename(ts_ms)
            new_path = os.path.join(ch_dir, new_name)
            if os.path.exists(new_path):
                fpath.unlink()
                continue

            fpath.rename(new_path)
            existing_timestamps_ms.add(ts_ms)
            frames.append({
                "path": os.path.relpath(new_path, output_dir),
                "timestamp_seconds": round(ts_seconds, 3),
                "timestamp_ms": ts_ms,
                "timestamp_display": format_timestamp(ts_seconds),
                "pass": "offset",
                "chapter": ch_num,
            })
            added += 1

        # Clean up leftovers
        for leftover in Path(ch_dir).glob("offset_*.jpg"):
            leftover.unlink()

    log(f"  Offset pass complete: {added} new frames.")
    return frames, added


# ---------------------------------------------------------------------------
# Metadata assembly
# ---------------------------------------------------------------------------
def build_metadata(
    video_meta: dict,
    scenes: list[dict],
    all_frames: list[dict],
    chapter_duration: int,
    passes_run: list[str],
    args,
) -> dict:
    """Build the metadata.json structure."""
    duration = video_meta["duration_seconds"]
    num_chapters = max(1, int(math.ceil(duration / chapter_duration)))

    # Assign chapter indices to scene changes
    scene_changes_with_chapters = []
    for sc in scenes:
        ch = int(sc["timestamp_seconds"] // chapter_duration) + 1
        scene_changes_with_chapters.append({
            "timestamp_seconds": sc["timestamp_seconds"],
            "score": sc["score"],
            "chapter": ch,
        })

    # Build chapter data
    chapters = []
    for ch_idx in range(num_chapters):
        ch_num = ch_idx + 1
        ch_start = ch_idx * chapter_duration
        ch_end = min((ch_idx + 1) * chapter_duration, duration)

        ch_frames = [f for f in all_frames if f["chapter"] == ch_num]
        ch_frames.sort(key=lambda f: f["timestamp_ms"])
        ch_scene_count = sum(
            1 for sc in scene_changes_with_chapters if sc["chapter"] == ch_num
        )

        # Strip chapter-specific fields for cleaner output
        frame_records = [
            {
                "path": f["path"],
                "timestamp_seconds": f["timestamp_seconds"],
                "timestamp_display": f["timestamp_display"],
                "pass": f["pass"],
            }
            for f in ch_frames
        ]

        chapters.append({
            "index": ch_num,
            "start_seconds": round(ch_start, 2),
            "end_seconds": round(ch_end, 2),
            "frame_count": len(ch_frames),
            "scene_changes": ch_scene_count,
            "frames": frame_records,
        })

    return {
        "video": video_meta,
        "scene_changes": scene_changes_with_chapters,
        "chapters": chapters,
        "summary": {
            "total_frames": len(all_frames),
            "total_scene_changes": len(scenes),
            "passes_run": passes_run,
            "coarse_fps": args.coarse_fps,
            "dense_fps": args.dense_fps,
            "scene_threshold": args.scene_threshold,
        },
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Multi-pass video frame extraction for forensic analysis."
    )
    parser.add_argument("video_file", help="Path to video file")
    parser.add_argument("--output-dir", default=None,
                        help="Where to save frames (default: auto-generated)")
    parser.add_argument("--coarse-fps", type=float, default=1.0,
                        help="FPS for coarse pass (default: 1.0)")
    parser.add_argument("--dense-fps", type=float, default=4.0,
                        help="FPS for dense pass (default: 4.0)")
    parser.add_argument("--scene-threshold", type=float, default=0.3,
                        help="Scene detection sensitivity 0-1 (default: 0.3, lower = more sensitive)")
    parser.add_argument("--dense-window", type=float, default=2.0,
                        help="Seconds before/after scene change for dense pass (default: 2.0)")
    parser.add_argument("--chapter-duration", type=int, default=300,
                        help="Seconds per chapter for chunked processing (default: 300)")
    parser.add_argument("--max-frames", type=int, default=2000,
                        help="Safety limit to avoid extracting too many (default: 2000)")
    parser.add_argument("--offset-pass", action="store_true",
                        help="Enable Pass 3: extract at 0.5s offset for double coverage")
    args = parser.parse_args()

    # --- Validate prerequisites ---
    check_tools()

    video_path = os.path.abspath(args.video_file)
    if not os.path.isfile(video_path):
        log(f"ERROR: File not found: {video_path}")
        sys.exit(1)

    ext = Path(video_path).suffix.lower()
    if ext not in VIDEO_EXTENSIONS:
        log(f"ERROR: Unsupported video format '{ext}'")
        log(f"Supported: {', '.join(sorted(VIDEO_EXTENSIONS))}")
        sys.exit(1)

    # --- Set up output directory ---
    if args.output_dir:
        output_dir = os.path.abspath(args.output_dir)
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = os.path.abspath(f"video_analysis_{timestamp}")

    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(os.path.join(output_dir, "chapters"), exist_ok=True)

    log(f"Video: {video_path}")
    log(f"Output: {output_dir}")

    # --- Get video metadata ---
    log("Reading video metadata...")
    video_meta = get_video_metadata(video_path)

    duration = video_meta["duration_seconds"]
    if duration <= 0:
        log("ERROR: Could not determine video duration.")
        sys.exit(1)

    log(f"  Duration: {format_timestamp(duration)} ({duration:.1f}s)")
    log(f"  FPS: {video_meta['fps']}")
    log(f"  Resolution: {video_meta['resolution']}")
    log(f"  Codec: {video_meta['codec']}")
    log(f"  File size: {video_meta['file_size_mb']} MB")

    start_time = time.time()
    all_frames = []
    total_count = 0
    passes_run = []

    try:
        # --- Pass 0: Scene Detection ---
        scenes = detect_scenes(video_path, args.scene_threshold)
        passes_run.append("scene_detect")

        # --- Pass 1: Coarse Sweep ---
        coarse_frames, coarse_count = extract_coarse_frames(
            video_path, output_dir, args.coarse_fps, duration,
            args.chapter_duration, args.max_frames,
        )
        all_frames.extend(coarse_frames)
        total_count += coarse_count
        passes_run.append("coarse")

        # Build set of existing timestamps for dedup
        existing_ts_ms = {f["timestamp_ms"] for f in all_frames}

        # --- Pass 2: Dense Extraction ---
        dense_frames, dense_count = extract_dense_frames(
            video_path, output_dir, scenes, args.dense_fps, args.dense_window,
            duration, args.chapter_duration, existing_ts_ms,
            args.max_frames, total_count,
        )
        all_frames.extend(dense_frames)
        total_count += dense_count
        passes_run.append("dense")

        # --- Pass 3: Offset (optional) ---
        if args.offset_pass:
            offset_frames, offset_count = extract_offset_frames(
                video_path, output_dir, args.coarse_fps, duration,
                args.chapter_duration, existing_ts_ms,
                args.max_frames, total_count,
            )
            all_frames.extend(offset_frames)
            total_count += offset_count
            passes_run.append("offset")

    except KeyboardInterrupt:
        log("\nInterrupted. Partial extraction may remain in output directory.")
        sys.exit(130)
    except Exception as e:
        log(f"ERROR: Extraction failed: {e}")
        # Clean up partial output
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)

    elapsed = time.time() - start_time

    # --- Sort all frames by timestamp ---
    all_frames.sort(key=lambda f: f["timestamp_ms"])

    # --- Build and write metadata ---
    metadata = build_metadata(
        video_meta, scenes, all_frames, args.chapter_duration, passes_run, args,
    )

    metadata_path = os.path.join(output_dir, "metadata.json")
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)

    # --- Summary ---
    num_chapters = len(metadata["chapters"])
    log(f"\nExtraction complete in {elapsed:.1f}s:")
    log(f"  Total frames: {total_count}")
    log(f"  Scene changes: {len(scenes)}")
    log(f"  Chapters: {num_chapters}")
    log(f"  Passes: {', '.join(passes_run)}")
    log(f"  Output: {output_dir}")
    log(f"  Metadata: {metadata_path}")

    if total_count >= args.max_frames:
        log(f"  WARNING: Frame limit ({args.max_frames}) was reached. "
            f"Some frames may have been skipped.")

    # --- JSON result to stdout ---
    result = {
        "status": "success",
        "output_dir": output_dir,
        "metadata": metadata_path,
        "total_frames": total_count,
        "total_scene_changes": len(scenes),
        "chapters": num_chapters,
        "video_duration": duration,
        "passes": passes_run,
    }
    print(json.dumps(result))


if __name__ == "__main__":
    main()
