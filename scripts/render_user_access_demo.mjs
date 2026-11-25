#!/usr/bin/env node
/** Render the checked browser timeline into a captioned H.264 review video. */

import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import { spawn } from "node:child_process";

function parseArgs(argv) {
  const options = {};
  for (let i = 0; i < argv.length; i++) {
    const key = argv[i];
    if (!key.startsWith("--") || !argv[i + 1]) throw new Error(`Invalid argument: ${key}`);
    options[key.slice(2)] = argv[++i];
  }
  return options;
}

const options = parseArgs(process.argv.slice(2));
assert(options.timeline && options.output, "Usage: node scripts/render_user_access_demo.mjs --timeline PATH --output PATH [--ffmpeg PATH]");
const timelinePath = path.resolve(options.timeline);
const outputPath = path.resolve(options.output);
const root = path.dirname(timelinePath);
const timeline = JSON.parse(await fs.readFile(timelinePath, "utf8"));
assert.equal(timeline.success, true, "Refusing to render a failed recording");
assert(timeline.assertions.length > 0 && timeline.assertions.every(item => item.passed), "Refusing to render a recording with failed assertions");

const actors = [
  { id: "alice", label: "用户 A · Alice" },
  { id: "bob", label: "用户 B · Bob" },
  { id: "alice_b", label: "用户 C · Alice · B组织" },
];
const scenes = timeline.clips;
assert(scenes.length >= 10, "Recording timeline is missing expected scenes");

function run(executable, args) {
  return new Promise((resolve, reject) => {
    const child = spawn(executable, args, { stdio: ["ignore", "ignore", "pipe"] });
    let stderr = "";
    child.stderr.on("data", data => { stderr = (stderr + data).slice(-3000); });
    child.on("error", reject);
    child.on("exit", code => code === 0 ? resolve() : reject(new Error(`${executable} exited ${code}: ${stderr}`)));
  });
}

const ffmpeg = options.ffmpeg ?? "/usr/bin/ffmpeg";
const editing = path.join(root, "editing");
await fs.mkdir(editing, { recursive: true });
const segmentFiles = [];

// Add a genuinely synchronized three-user view. The selected wall-clock
// instant is after all three HTTP runs were accepted and before the 45s jobs
// finish, so these panels show recorded states from the same time.
const runStarts = actors.map(actor => {
  const first = timeline.actors[actor.id].run_requests?.find(request => request.status === 200);
  assert(first, `No accepted run is recorded for ${actor.id}`);
  return Date.parse(first.at);
});
const syncWallTime = Math.max(...runStarts) + 2800;
const syncDuration = 8;
const montage = path.join(editing, "concurrent-three-user.mp4");
const actorLabelFiles = [];
for (const actor of actors) {
  const label = actor.id === "alice_b" ? "用户 C · Alice（B组织）| 排队中" : `${actor.label} | 正在处理`;
  const labelFile = path.join(editing, `label-${actor.id}.txt`);
  await fs.writeFile(labelFile, label);
  actorLabelFiles.push(labelFile);
}
const inputArgs = [];
for (const actor of actors) {
  const recordingStart = timeline.actors[actor.id].recording_started_epoch_ms;
  const seek = Math.max(0, (syncWallTime - recordingStart) / 1000);
  inputArgs.push("-ss", seek.toFixed(3), "-i", path.resolve(root, timeline.actors[actor.id].video));
}
const fontPath = "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc";
const filters = actors.map((actor, index) => {
  const name = String.fromCharCode(97 + index);
  const label = actorLabelFiles[index];
  return `[${index}:v]fps=25,crop=1120:630:400:160,scale=960:540,drawtext=fontfile=${fontPath}:textfile=${label}:expansion=none:fontsize=28:fontcolor=white:box=1:boxcolor=0x15243d@0.92:boxborderw=12:x=16:y=16[${name}]`;
});
const info = path.join(editing, "concurrent-info.txt");
await fs.writeFile(info, "同一时刻的实际前端画面\n2 个任务执行中   ·   1 个任务排队\n" + new Date(syncWallTime).toISOString().replace("T", "  ").replace(".000Z", " UTC"));
const graph = [
  ...filters.slice(0, 3),
  `color=c=0xf4f6f9:s=960x540:r=25:d=${syncDuration},drawtext=fontfile=${fontPath}:textfile=${info}:expansion=none:fontsize=32:line_spacing=24:fontcolor=0x15243d:x=(w-text_w)/2:y=(h-text_h)/2[d]`,
  `[a][b]hstack=inputs=2[top]`,
  `[c][d]hstack=inputs=2[bottom]`,
  `[top][bottom]vstack=inputs=2[out]`,
].join(";");
await run(ffmpeg, ["-hide_banner", "-loglevel", "error", "-y", ...inputArgs, "-filter_complex", graph, "-map", "[out]", "-t", String(syncDuration), "-c:v", "libx264", "-preset", "veryfast", "-crf", "21", "-pix_fmt", "yuv420p", "-an", montage]);
segmentFiles.push(montage);

for (let index = 0; index < scenes.length; index++) {
  const scene = scenes[index];
  const actor = actors.find(item => item.id === scene.actor);
  assert(actor, `Unknown recording actor: ${scene.actor}`);
  const actorVideo = path.resolve(root, timeline.actors[actor.id].video);
  const name = `segment-${String(index + 1).padStart(3, "0")}`;
  const captionFile = path.join(editing, `${name}.txt`);
  const title = scene.title.includes("HTTP 429")
    ? "同用户重复提交：服务端已拒绝 HTTP 429；当前前端仍显示等待状态"
    : scene.title;
  await fs.writeFile(captionFile, `${actor.label}  |  ${title}`);
  const segment = path.join(editing, `${name}.mp4`);
  const filter = `fps=25,scale=1920:1000:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:80:color=0xf4f6f9,drawtext=fontfile=${fontPath}:textfile=${captionFile}:expansion=none:fontsize=28:fontcolor=0x15243d:x=40:y=23`;
  await run(ffmpeg, ["-hide_banner", "-loglevel", "error", "-y", "-ss", Math.max(0, scene.start_seconds).toFixed(3), "-i", actorVideo, "-t", Math.max(1, scene.end_seconds - scene.start_seconds).toFixed(3), "-vf", filter, "-c:v", "libx264", "-preset", "veryfast", "-crf", "21", "-pix_fmt", "yuv420p", "-an", segment]);
  segmentFiles.push(segment);
}

const concatFile = path.join(editing, "segments.txt");
await fs.writeFile(concatFile, segmentFiles.map(file => `file '${file.replaceAll("'", "'\\''")}'`).join("\n") + "\n");
await fs.mkdir(path.dirname(outputPath), { recursive: true });
await run(ffmpeg, ["-hide_banner", "-loglevel", "error", "-y", "-f", "concat", "-safe", "0", "-i", concatFile, "-c", "copy", "-movflags", "+faststart", outputPath]);
console.log(JSON.stringify({ output: outputPath, scenes: scenes.length, duration_seconds: syncDuration + scenes.reduce((sum, scene) => sum + scene.end_seconds - scene.start_seconds, 0) }));
