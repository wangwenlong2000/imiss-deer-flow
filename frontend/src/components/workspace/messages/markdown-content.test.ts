import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import test from "node:test";
import { runInNewContext } from "node:vm";

import { createElement, type ComponentType } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import ts from "typescript";

const require = createRequire(import.meta.url);
function loadSource(relative: string, dependencies: Record<string, unknown>) {
  const { outputText } = ts.transpileModule(readFileSync(new URL(relative, import.meta.url), "utf8"), {
    compilerOptions: { module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX },
  });
  const exports: Record<string, unknown> = {};
  runInNewContext(outputText, {
    exports,
    require: (name: string) => {
      if (name in dependencies) return dependencies[name];
      if (name === "react" || name === "react/jsx-runtime") return require(name);
      throw new Error(`Unexpected dependency: ${name}`);
    },
  });
  return exports;
}
const artifacts = loadSource("../../../core/artifacts/utils.ts", {
  "../config": { getBackendBaseURL: () => "" },
});
const loaded = loadSource("./markdown-content.tsx", {
  "@/components/ai-elements/message": {
    MessageResponse: ({ children, components }: { children: string; components: { a: ComponentType<Record<string, unknown>> } }) =>
      createElement(components.a, { href: children }, "Download"),
  },
  "@/core/artifacts/utils": artifacts,
  "@/core/streamdown": { streamdownPlugins: { remarkPlugins: [] } },
  "../citations/citation-link": { CitationLink: () => createElement("a", null, "Citation") },
});
const MarkdownContent = loaded.MarkdownContent as ComponentType<Record<string, unknown>>;
function render(path: string, threadId?: string) {
  return renderToStaticMarkup(createElement(MarkdownContent, { content: path, threadId, isLoading: false, rehypePlugins: [] }));
}

void test("thread output links use the same thread artifact API as downloadable files", () => {
  const html = render("/mnt/user-data/outputs/street-location/run/evidence.json", "thread-a");
  assert.ok(html.includes('href="/api/threads/thread-a/artifacts/mnt/user-data/outputs/street-location/run/evidence.json"'));
});
void test("uploaded file links resolve but unrelated external or NAS links are not exposed as artifacts", () => {
  assert.ok(render("/mnt/user-data/uploads/image.png", "thread-b").includes("/api/threads/thread-b/artifacts/"));
  assert.ok(render("https://example.com/report", "thread-b").includes('href="https://example.com/report"'));
  assert.ok(!render("/mnt/nas/private.png", "thread-b").includes("/api/threads/"));
  assert.ok(render("/mnt/user-data/outputs/report.csv").includes('href="/mnt/user-data/outputs/report.csv"'));
});
