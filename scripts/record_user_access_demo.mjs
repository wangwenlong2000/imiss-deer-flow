#!/usr/bin/env node
/**
 * Record real browser interactions with an isolated DeerFlow demo deployment.
 *
 * Run in the disposable browser image; no browser routing, mocked responses,
 * fabricated task states, or production credentials are used by this script.
 * Each actor gets an independent cookie jar and a native Playwright recording.
 * A timeline and screenshots accompany the raw recordings and edited MP4.
 */

import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";

const require = createRequire(import.meta.url);
const repository = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

function argumentsOf(argv) {
  const result = { "base-url": "http://127.0.0.1:38860", output: "/recordings", hold: "3", "ready-timeout": "180" };
  for (let index = 0; index < argv.length; index++) {
    const arg = argv[index];
    if (arg === "--help" || arg === "-h") result.help = true;
    else if (arg === "--probe") result.probe = true;
    else if (arg === "--skip-render") result["skip-render"] = true;
    else if (arg.startsWith("--") && argv[index + 1] && !argv[index + 1].startsWith("--")) result[arg.slice(2)] = argv[++index];
    else throw new Error(`Unknown argument: ${arg}`);
  }
  return result;
}

const options = argumentsOf(process.argv.slice(2));
if (options.help) {
  console.log("Usage: node scripts/record_user_access_demo.mjs --base-url http://127.0.0.1:38860 --output /recordings [--probe] [--skip-render] [--hold 3]");
  process.exit(0);
}
const baseURL = new URL(options["base-url"]).origin;
assert(["127.0.0.1", "localhost", "host.docker.internal"].includes(new URL(baseURL).hostname), "Only an isolated local demo origin may be recorded");
const output = path.resolve(options.output);
const holdMilliseconds = Math.max(500, Number(options.hold) * 1000);
const timeline = { started_at: new Date().toISOString(), origin: baseURL, environment: "isolated demo; test identities and synthetic CSV; real frontend and runtime", viewport: { width: 1920, height: 1080 }, recording_epoch_ms: Date.now(), steps: [], clips: [], actors: {}, assertions: [], errors: [] };
const actors = [
  { id: "alice", label: "用户 A · Alice", source: "A-private.csv", expected: 11, content: "value\n1\n10\n" },
  { id: "bob", label: "用户 B · Bob", source: "B-private.csv", expected: 22, content: "value\n2\n20\n" },
  { id: "alice_b", label: "用户 C · Alice · B组织", source: "C-private.csv", expected: 33, content: "value\n3\n30\n" },
];
const contexts = [];
const pageOffsets = new Map();
let browser;
let screenshotSequence = 0;

const pause = (milliseconds = holdMilliseconds) => new Promise(resolve => setTimeout(resolve, milliseconds));
const safeFilename = value => value.replace(/[^\w.-]+/g, "-").replace(/^-|-$/g, "").slice(0, 100);
const shortURL = value => { try { return new URL(value).pathname; } catch { return "unknown"; } };
const safeError = value => String(value?.message ?? value).replace(/Bearer\s+\S+/gi, "Bearer [redacted]").replace(/([?&](?:token|key|secret|password)=)[^\s&]+/gi, "$1[redacted]").slice(0, 1300);

async function flushTimeline() {
  await fs.writeFile(path.join(output, "timeline.json"), JSON.stringify(timeline, null, 2) + "\n");
}

async function milestone(name, details = {}) {
  const item = { at: new Date().toISOString(), elapsed_ms: Date.now() - Date.parse(timeline.started_at), name, ...details };
  timeline.steps.push(item);
  console.log(JSON.stringify(item));
  await flushTimeline();
}

async function screenshot(actor, name) {
  const filename = `${String(++screenshotSequence).padStart(3, "0")}-${actor.id}-${safeFilename(name)}.png`;
  await actor.page.screenshot({ path: path.join(output, "screenshots", filename), fullPage: false });
  return `screenshots/${filename}`;
}

async function clip(actor, title, operation, { after = holdMilliseconds } = {}) {
  await actor.page.bringToFront();
  const started = Date.now();
  await milestone("scene_started", { actor: actor.id, title });
  try {
    await operation();
    await pause(after);
    const image = await screenshot(actor, title);
    timeline.clips.push({ actor: actor.id, title, started_at: new Date(started).toISOString(), ended_at: new Date().toISOString(), wall_start_ms: started, wall_end_ms: Date.now(), start_seconds: (started - pageOffsets.get(actor.page)) / 1000, end_seconds: (Date.now() - pageOffsets.get(actor.page)) / 1000, screenshot: image });
    await milestone("scene_completed", { actor: actor.id, title, screenshot: image });
  } catch (error) {
    const image = await screenshot(actor, "FAILED-" + title).catch(() => null);
    await milestone("scene_failed", { actor: actor.id, title, error: safeError(error), screenshot: image });
    throw error;
  }
}

async function visiblePointer(context) {
  await context.addInitScript(() => {
    document.addEventListener("DOMContentLoaded", () => {
      const style = document.createElement("style");
      style.textContent = `*{font-family:'Noto Sans CJK SC',sans-serif!important}#__recording_pointer{position:fixed;left:0;top:0;width:23px;height:29px;z-index:2147483647;pointer-events:none;filter:drop-shadow(0 2px 3px #0006);transform:translate(50px,100px)}.__recording_click{position:fixed;width:50px;height:50px;border-radius:50%;border:3px solid #2563eb;z-index:2147483646;pointer-events:none;animation:recording-click .55s ease-out forwards}@keyframes recording-click{from{opacity:.8;scale:.3}to{opacity:0;scale:1.1}}`;
      document.head.append(style);
      const pointer = document.createElement("div");
      pointer.id = "__recording_pointer";
      pointer.innerHTML = '<svg viewBox="0 0 24 30"><path d="M2 2v23l6-6 5 9 5-3-5-8h9Z" fill="#ffffff" stroke="#111827" stroke-width="1.6"/></svg>';
      document.body.append(pointer);
      document.addEventListener("mousemove", event => { pointer.style.transform = `translate(${event.clientX}px,${event.clientY}px)`; });
      document.addEventListener("mousedown", event => {
        const ring = document.createElement("div"); ring.className = "__recording_click";
        ring.style.left = `${event.clientX - 25}px`; ring.style.top = `${event.clientY - 25}px`;
        document.body.append(ring); setTimeout(() => ring.remove(), 650);
      });
    });
  });
}

async function moveTo(actor, locator) {
  await locator.scrollIntoViewIfNeeded();
  const box = await locator.boundingBox();
  assert(box, "Expected an interactable visible control");
  await actor.page.mouse.move(box.x + box.width / 2, box.y + box.height / 2, { steps: 22 });
  await pause(220);
}

async function click(actor, locator) {
  await moveTo(actor, locator);
  await locator.click();
}

async function type(actor, locator, value) {
  await click(actor, locator);
  await locator.fill("");
  await locator.pressSequentially(value, { delay: 58 });
}

async function waitReady() {
  const deadline = Date.now() + Number(options["ready-timeout"]) * 1000;
  while (Date.now() < deadline) {
    try {
      const response = await fetch(new URL("/demo/login", baseURL), { signal: AbortSignal.timeout(4000) });
      if (response.ok) return;
    } catch { /* Only local service startup is retried. */ }
    await pause(700);
  }
  throw new Error("The isolated demo login did not become ready");
}

async function login(actor) {
  await actor.page.goto(`${baseURL}/demo/login`, { waitUntil: "domcontentloaded" });
  const accountButton = actor.page.locator(`[data-demo-user="${actor.id}"], button[name="identity"][value="${actor.id}"], button[name="user"][value="${actor.id}"]`).first();
  const accountSelect = actor.page.locator('select[name="user"], select[name="username"]');
  if (await accountButton.count()) await click(actor, accountButton);
  else if (await accountSelect.count()) {
    await moveTo(actor, accountSelect.first());
    await accountSelect.first().selectOption(actor.id);
    await click(actor, actor.page.getByRole("button", { name: /登录|进入|Login|Sign in/i }).first());
  } else {
    await type(actor, actor.page.locator('input[name="user"], input[name="username"]').first(), actor.id);
    const password = actor.page.locator('input[type="password"]');
    if (await password.count()) await type(actor, password.first(), options.password ?? "demo-only");
    await click(actor, actor.page.getByRole("button", { name: /登录|进入|Login|Sign in/i }).first());
  }
  await actor.page.waitForURL(url => url.pathname !== "/demo/login", { timeout: 60000 });
  if (!new URL(actor.page.url()).pathname.startsWith("/workspace")) await actor.page.goto(`${baseURL}/workspace/chats/new`, { waitUntil: "domcontentloaded" });
  await actor.page.getByPlaceholder("今天我能为你做些什么？").waitFor({ state: "visible", timeout: 60000 });
  await pause(500);
}

function chatInput(actor) { return actor.page.getByPlaceholder("今天我能为你做些什么？"); }
function runStatus(actor, expression) { return actor.page.locator('[role="status"]').filter({ hasText: expression }).first(); }

async function prepareChat(actor) {
  await actor.page.goto(`${baseURL}/workspace/chats/new`, { waitUntil: "domcontentloaded" });
  await chatInput(actor).waitFor({ state: "visible", timeout: 60000 });
  await actor.page.locator('input[type="file"][aria-label="Upload files"]').first().setInputFiles({ name: "same.csv", mimeType: "text/csv", buffer: Buffer.from(actor.content) });
  await actor.page.getByText("same.csv", { exact: true }).first().waitFor({ state: "visible" });
  await type(actor, chatInput(actor), "请计算上传 CSV 中 value 列的总和，并生成 report.csv 供下载。");
}

async function submitChat(actor) {
  actor.runRequests = [];
  actor.page.on("response", response => {
    if (response.request().method() === "POST" && /\/runs(?:\/stream|\/wait)?\/?$/.test(new URL(response.url()).pathname)) {
      actor.runRequests.push({ path: shortURL(response.url()), status: response.status(), at: new Date().toISOString() });
    }
  });
  await click(actor, actor.page.getByRole("button", { name: "Submit", exact: true }));
  await actor.page.waitForURL(/\/workspace\/chats\/(?!new(?:\?|$))[^/?]+/, { timeout: 60000 });
  actor.threadURL = actor.page.url();
  actor.threadId = new URL(actor.threadURL).pathname.split("/").at(-1);
  timeline.actors[actor.id].thread = actor.threadId;
}

async function waitResult(actor) {
  await actor.page.getByText("report.csv", { exact: true }).first().waitFor({ state: "visible", timeout: 180000 });
  await actor.page.waitForFunction(expected => {
    const text = document.body.innerText;
    return new RegExp(`(?:^|\\D)${expected}(?:\\D|$)`).test(text) && !/排队中，轮到后|正在处理你的问题/.test(text);
  }, actor.expected, { timeout: 180000 });
  await actor.page.getByText("本次使用当前会话上传的数据。", { exact: false }).first().waitFor({ state: "visible" });
  const uploads = await actor.page.request.get(`${baseURL}/api/threads/${actor.threadId}/uploads/list`);
  assert.equal(uploads.status(), 200, "The task's uploaded inputs must be readable by its owner");
  assert((await uploads.json()).files.some(file => file.filename === "same.csv"), "The demonstrated task must use its actual uploaded same.csv");
  const downloadLink = actor.page.locator('a[href*="/artifacts/"][href*="report.csv"][href*="download=true"]').first();
  await downloadLink.waitFor({ state: "visible" });
  actor.downloadURL = await downloadLink.getAttribute("href");
  timeline.assertions.push({ assertion: "result_visible", actor: actor.id, expected_total: actor.expected, passed: true });
}

async function rejectSecondRun(actor) {
  assert(actor.threadURL, "The first task must already have a real thread URL");
  const originalURL = actor.threadURL;
  await actor.page.goto(`${baseURL}/workspace/chats/new`, { waitUntil: "domcontentloaded" });
  await chatInput(actor).waitFor({ state: "visible" });
  await type(actor, chatInput(actor), "请再执行一个任务。");
  const responsePromise = actor.page.waitForResponse(response => response.request().method() === "POST" && /\/runs(?:\/stream|\/wait)?\/?$/.test(new URL(response.url()).pathname), { timeout: 20000 });
  await click(actor, actor.page.getByRole("button", { name: "Submit", exact: true }));
  const response = await responsePromise;
  assert.equal(response.status(), 429, "A second active task from the same user must be refused");
  timeline.assertions.push({ assertion: "same_user_second_task_rejected", actor: actor.id, status: response.status(), response_body: safeError(await response.text()), passed: true });
  await pause(2200);
  await screenshot(actor, "same-user-quota-429");
  await actor.page.goto(originalURL, { waitUntil: "domcontentloaded" });
  await chatInput(actor).waitFor({ state: "visible" });
}

async function downloadResult(actor) {
  const link = actor.page.locator('a[href*="/artifacts/"][href*="report.csv"][href*="download=true"]').first();
  const downloadPromise = actor.page.waitForEvent("download", { timeout: 20000 });
  await click(actor, link);
  const download = await downloadPromise;
  const saved = path.join(output, "downloads", `${actor.id}-report.csv`);
  await download.saveAs(saved);
  const csv = await fs.readFile(saved, "utf8");
  assert(new RegExp(`(?:^|[^0-9])${actor.expected}(?:[^0-9]|$)`).test(csv), `${actor.id} downloaded another user's result`);
  timeline.assertions.push({ assertion: "download_matches_own_input", actor: actor.id, expected_total: actor.expected, passed: true });
}

async function dataCenter(actor) {
  await actor.page.goto(`${baseURL}/workspace/data-center`, { waitUntil: "domcontentloaded" });
  const addDataButton = actor.page.getByRole("button", { name: "添加数据", exact: true }).last();
  await addDataButton.waitFor({ state: "visible", timeout: 60000 });
  await moveTo(actor, addDataButton);
  const responsePromise = actor.page.waitForResponse(response => new URL(response.url()).pathname.endsWith("/api/data-center/sources/upload") && response.request().method() === "POST");
  await actor.page.locator('input[type="file"]').first().setInputFiles({ name: actor.source, mimeType: "text/csv", buffer: Buffer.from(actor.content) });
  const response = await responsePromise;
  assert.equal(response.status(), 200, "Data source upload was rejected");
  const result = await response.json();
  actor.sourceId = result.sources[0].id;
  await actor.page.getByText(path.parse(actor.source).name, { exact: true }).first().waitFor({ state: "visible" });
  const text = await actor.page.locator("body").innerText();
  for (const other of actors.filter(other => other.id !== actor.id)) assert(!text.includes(path.parse(other.source).name), "Another user's data source is visible");
  timeline.assertions.push({ assertion: "catalogue_shows_only_own_sources", actor: actor.id, passed: true });
}

async function checkPermissions(actor, target, status) {
  await actor.page.goto(`${baseURL}/demo/permissions`, { waitUntil: "domcontentloaded" });
  await type(actor, actor.page.locator('input[name="thread_id"]'), target.threadId);
  await click(actor, actor.page.getByRole("button", { name: "检查访问权限", exact: true }));
  await actor.page.getByText(new RegExp(`HTTP ${status}.*${status === 200 ? "允许访问" : "拒绝访问"}`)).first().waitFor({ state: "visible" });
  const count = await actor.page.getByText(new RegExp(`HTTP ${status}.*${status === 200 ? "允许访问" : "拒绝访问"}`)).count();
  assert.equal(count, 2, "Both conversation state and artifact requests must use the expected authorization decision");
  timeline.assertions.push({ assertion: status === 200 ? "own_thread_and_artifact_access_allowed" : "foreign_thread_and_artifact_access_denied", actor: actor.id, target_actor: target.id, status, passed: true });
}

async function command(executable, argv) {
  await new Promise((resolve, reject) => {
    const process = spawn(executable, argv, { stdio: ["ignore", "ignore", "pipe"] });
    let stderr = "";
    process.stderr.on("data", data => { stderr = (stderr + data).slice(-2500); });
    process.on("error", reject);
    process.on("exit", code => code === 0 ? resolve() : reject(new Error(`${executable} exited ${code}: ${stderr}`)));
  });
}

async function renderVideo() {
  const font = options.font ?? "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc";
  const segmentFiles = [];
  for (let index = 0; index < timeline.clips.length; index++) {
    const scene = timeline.clips[index];
    const actor = actors.find(candidate => candidate.id === scene.actor);
    const name = `segment-${String(index + 1).padStart(3, "0")}`;
    const captionFile = path.join(output, "editing", `${name}.txt`);
    await fs.writeFile(captionFile, `${actor.label}  |  ${scene.title}`);
    const target = path.join(output, "editing", `${name}.mp4`);
    const filter = `fps=25,scale=1920:1000:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:80:color=0xf4f6f9,drawtext=fontfile=${font}:textfile=${captionFile}:expansion=none:fontsize=30:fontcolor=0x15243d:x=40:y=23`;
    await command(options.ffmpeg ?? "/usr/bin/ffmpeg", ["-hide_banner", "-loglevel", "error", "-y", "-ss", Math.max(0, scene.start_seconds).toFixed(3), "-i", actor.videoPath, "-t", Math.max(1, scene.end_seconds - scene.start_seconds).toFixed(3), "-vf", filter, "-c:v", "libx264", "-preset", "fast", "-crf", "20", "-pix_fmt", "yuv420p", "-an", target]);
    segmentFiles.push(target);
  }
  const concat = path.join(output, "editing", "segments.txt");
  await fs.writeFile(concat, segmentFiles.map(filename => `file '${filename.replace(/'/g, "'\\''")}'`).join("\n") + "\n");
  const target = path.join(output, "user-access-concurrency-demo.mp4");
  await command(options.ffmpeg ?? "/usr/bin/ffmpeg", ["-hide_banner", "-loglevel", "error", "-y", "-f", "concat", "-safe", "0", "-i", concat, "-c", "copy", "-movflags", "+faststart", target]);
  timeline.output_video = path.basename(target);
  timeline.output_duration_seconds = timeline.clips.reduce((sum, scene) => sum + scene.end_seconds - scene.start_seconds, 0);
  await milestone("video_rendered", { file: timeline.output_video, duration_seconds: timeline.output_duration_seconds });
}

async function configureRecorder() {
  for (const folder of [output, path.join(output, "raw"), path.join(output, "screenshots"), path.join(output, "downloads"), path.join(output, "editing")]) await fs.mkdir(folder, { recursive: true });
  if (!process.env.PLAYWRIGHT_BROWSERS_PATH) {
    const cache = await fs.mkdtemp(path.join(os.tmpdir(), "deerflow-recording-pw-"));
    process.env.PLAYWRIGHT_BROWSERS_PATH = cache;
    const ffmpeg = path.join(cache, "ffmpeg-1011");
    await fs.mkdir(ffmpeg);
    await fs.symlink(options.ffmpeg ?? "/usr/bin/ffmpeg", path.join(ffmpeg, "ffmpeg-linux"));
  }
  const { chromium } = require(options.playwright ?? path.join(repository, "frontend/node_modules/.pnpm/playwright-core@1.58.2/node_modules/playwright-core"));
  browser = await chromium.launch({ executablePath: options.chromium ?? "/usr/bin/chromium-browser", headless: true, args: ["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"] });
  for (const actor of actors) {
    const context = await browser.newContext({ viewport: { width: 1920, height: 1080 }, locale: "zh-CN", timezoneId: "Asia/Shanghai", colorScheme: "light", reducedMotion: "reduce", acceptDownloads: true, recordVideo: { dir: path.join(output, "raw", actor.id), size: { width: 1920, height: 1080 } } });
    contexts.push(context);
    actor.context = context;
    await context.addCookies([{ name: "locale", value: "zh-CN", url: baseURL }]);
    await context.addInitScript(() => {
      localStorage.setItem("deerflow.local-settings", JSON.stringify({ layout: { sidebar_collapsed: false }, notification: { enabled: false }, context: { mode: "flash" } }));
    });
    await visiblePointer(context);
    const recordingStart = Date.now();
    actor.page = await context.newPage();
    pageOffsets.set(actor.page, recordingStart);
    actor.page.setDefaultTimeout(30000);
    actor.page.on("pageerror", error => { timeline.errors.push({ actor: actor.id, type: "pageerror", error: safeError(error) }); });
    actor.page.on("requestfailed", request => { timeline.errors.push({ actor: actor.id, type: "requestfailed", path: shortURL(request.url()), error: request.failure()?.errorText }); });
    actor.video = actor.page.video();
    timeline.actors[actor.id] = { label: actor.label, expected_total: actor.expected, recording_started_at: new Date(recordingStart).toISOString(), recording_started_epoch_ms: recordingStart, timing_offset_seconds: (recordingStart - timeline.recording_epoch_ms) / 1000 };
  }
}

let succeeded = false;
try {
  await configureRecorder();
  await waitReady();
  await milestone("demo_ready");
  if (options.probe) {
    await actors[0].page.goto(`${baseURL}/demo/login`, { waitUntil: "domcontentloaded" });
    await screenshot(actors[0], "login-probe");
    await milestone("probe_completed", { title: await actors[0].page.title() });
  } else {
    for (const actor of actors) await clip(actor, "独立浏览器会话登录测试账号", () => login(actor));
    for (const actor of actors) await clip(actor, `上传同名 same.csv，独立输入总和为 ${actor.expected}`, () => prepareChat(actor));
    await clip(actors[0], "提交任务 A：开始执行", async () => { await submitChat(actors[0]); await runStatus(actors[0], /正在处理/).waitFor({ state: "visible" }); });
    await clip(actors[1], "提交任务 B：与 A 同时执行", async () => { await submitChat(actors[1]); await runStatus(actors[1], /正在处理/).waitFor({ state: "visible" }); });
    await clip(actors[2], "提交任务 C：两个执行名额占用，自动排队", async () => { await submitChat(actors[2]); await runStatus(actors[2], /排队中/).waitFor({ state: "visible" }); });
    for (const actor of actors.slice(0, 2)) await clip(actor, "任务进行中：不同用户互不阻塞", async () => { await runStatus(actor, /正在处理/).waitFor({ state: "visible" }); });
    await clip(actors[0], "同一用户再提交任务：HTTP 429，原任务继续执行", () => rejectSecondRun(actors[0]), { after: 1000 });
    for (const actor of actors) {
      await waitResult(actor);
      await clip(actor, `任务完成：结果为 ${actor.expected}，下载自己的 report.csv`, () => downloadResult(actor), { after: 4500 });
    }
    await clip(actors[0], "访问自己的会话与报告：两个接口均允许", () => checkPermissions(actors[0], actors[0], 200), { after: 4500 });
    await clip(actors[0], "尝试访问用户 B 的会话与报告：两个接口均拒绝", () => checkPermissions(actors[0], actors[1], 404), { after: 5500 });
    await clip(actors[2], "不同组织的同名 Alice：仍不能访问 A组织的数据", () => checkPermissions(actors[2], actors[0], 404), { after: 5500 });
    for (const actor of actors.slice(0, 2)) await clip(actor, "数据中心：上传私有数据，列表按登录用户隔离", () => dataCenter(actor), { after: 4500 });
    await milestone("scenario_completed", { assertion_count: timeline.assertions.length });
  }
  succeeded = true;
} catch (error) {
  timeline.failure = safeError(error);
  console.error(timeline.failure);
} finally {
  for (const context of contexts) await context.close().catch(error => { timeline.errors.push({ type: "context_close", error: safeError(error) }); });
  for (const actor of actors) {
    if (actor.video) {
      actor.videoPath = await actor.video.path().catch(() => null);
      timeline.actors[actor.id].video = actor.videoPath ? path.relative(output, actor.videoPath) : null;
      timeline.actors[actor.id].run_requests = actor.runRequests ?? [];
    }
  }
  await browser?.close();
  timeline.finished_at = new Date().toISOString();
  timeline.success = succeeded;
  await flushTimeline();
}
if (succeeded && !options.probe && !options["skip-render"]) {
  try { await renderVideo(); }
  catch (error) { timeline.render_error = safeError(error); await flushTimeline(); console.error(timeline.render_error); process.exitCode = 1; }
}
if (!succeeded) process.exitCode = 1;
