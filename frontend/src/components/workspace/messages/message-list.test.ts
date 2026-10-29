import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import test from "node:test";
import { runInNewContext } from "node:vm";

import { createElement, type ComponentType, type ReactNode } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import ts from "typescript";

const require = createRequire(import.meta.url);
const messageUtils = await import(
  new URL("../../../core/messages/utils.ts", import.meta.url).href
);
const compliance = await import(
  new URL("../../../core/threads/compliance.ts", import.meta.url).href
);

// Render the real MessageList and use the real grouping/merge functions. Only
// visual primitives and context hooks are replaced, so no browser, network or
// additional test framework is required to catch duplicate body rendering.
function loadSource(relative: string, dependencies: Record<string, unknown>) {
  const source = readFileSync(new URL(relative, import.meta.url), "utf8");
  const { outputText } = ts.transpileModule(source, {
    compilerOptions: {
      module: ts.ModuleKind.CommonJS,
      jsx: ts.JsxEmit.ReactJSX,
      target: ts.ScriptTarget.ES2022,
    },
  });
  const exports: Record<string, unknown> = {};
  const resolve = (name: string) => {
    if (name in dependencies) return dependencies[name];
    if (name === "react/jsx-runtime") return require(name);
    throw new Error(`Unexpected dependency: ${name}`);
  };
  runInNewContext(outputText, { require: resolve, exports });
  return exports;
}

const threadUtils = loadSource("../../../core/threads/utils.ts", {
  "../messages/utils": messageUtils,
  "./compliance": compliance,
});
const Container = ({ children }: { children?: ReactNode }) =>
  createElement("div", null, children);
const { MessageList: LoadedMessageList } = loadSource("./message-list.tsx", {
  "@/components/ai-elements/conversation": {
    Conversation: Container,
    ConversationContent: Container,
  },
  "@/core/i18n/hooks": { useI18n: () => ({ t: { common: { taskWaiting: "Waiting" } } }) },
  "@/core/messages/utils": messageUtils,
  "@/core/rehype": { useRehypeSplitWordsIntoSpans: () => [] },
  "@/core/tasks/context": { useUpdateSubtask: () => () => undefined },
  "@/core/threads/compliance": compliance,
  "@/core/threads/utils": threadUtils,
  "@/lib/utils": { cn: () => "" },
  "../artifacts/artifact-file-list": {
    ArtifactFileList: ({ files }: { files: string[] }) =>
      createElement("ul", null, ...files.map((file) => createElement("li", { key: file, "data-file": file }, "Download"))),
  },
  "../streaming-indicator": { StreamingIndicator: () => createElement("span", null, "Loading") },
  "./compliance-notice": { ComplianceNotice: () => createElement("span", null, "Compliance") },
  "./markdown-content": { MarkdownContent: ({ content }: { content: string }) => createElement("section", null, content) },
  "./message-group": { MessageGroup: () => createElement("details", null, "Steps") },
  "./message-list-item": {
    MessageListItem: ({ message }: { message: { id: string; content: string } }) =>
      createElement("article", { "data-message": message.id }, message.content),
  },
  "./skeleton": { MessageListSkeleton: () => createElement("span", null, "Skeleton") },
  "./subtask-card": { SubtaskCard: () => null },
});
const MessageList = LoadedMessageList as ComponentType<Record<string, unknown>>;

const user = { type: "human", id: "question", content: "Analyze the uploaded CSV" };
const delivery = {
  type: "ai",
  id: "delivery",
  content: "Analysis result: flow total 800, mean 400.",
  tool_calls: [{ name: "present_files", id: "call-1", args: { filepaths: ["/mnt/user-data/outputs/report.csv"] } }],
};
const delivered = { type: "tool", id: "delivered", name: "present_files", tool_call_id: "call-1", content: "Successfully presented files" };
const answer = { type: "ai", id: "answer", content: delivery.content };

export function render(messages: unknown[], options: Record<string, unknown> = {}) {
  return renderToStaticMarkup(createElement(MessageList, {
    threadId: "thread-b",
    thread: { messages, values: {}, isLoading: false, ...options },
  }));
}

void test("a file-delivery draft plus the final answer renders one body and one download", () => {
  const html = render([user, delivery, delivered, answer]);
  assert.equal(html.split(answer.content).length - 1, 1);
  assert.equal(html.split('data-file="').length - 1, 1);
  assert.ok(html.includes('data-message="answer"'));
});

void test("delivery text stays hidden while the final answer is still streaming", () => {
  const html = render([user, delivery, delivered], { isLoading: true });
  assert.ok(!html.includes(delivery.content));
  assert.ok(html.includes('data-file="/mnt/user-data/outputs/report.csv"'));
});

void test("a revised/refused final answer cannot expose the delivery draft", () => {
  const html = render([user, delivery, delivered, { ...answer, content: "This answer was refused." }]);
  assert.ok(!html.includes(delivery.content));
  assert.ok(html.includes("This answer was refused."));
});

void test("refresh merging raw and canonical messages does not duplicate answers or cards", () => {
  const messages = [user, delivery, delivered, answer];
  const html = render(messages, { values: { raw_messages: messages } });
  assert.equal(html.split(answer.content).length - 1, 1);
  assert.equal(html.split('data-file="').length - 1, 1);
});

void test("separate user turns may legitimately receive the same answer", () => {
  const html = render([user, answer, { ...user, id: "question-2" }, { ...answer, id: "answer-2" }]);
  assert.equal(html.split(answer.content).length - 1, 2);
});

void test("multiple attachments survive hiding the file-delivery body", () => {
  const html = render([user, {
    ...delivery,
    tool_calls: [{ name: "present_files", id: "call-1", args: { filepaths: ["/mnt/user-data/outputs/report.csv", "/mnt/user-data/outputs/chart.png"] } }],
  }, delivered, answer]);
  assert.equal(html.split('data-file="').length - 1, 2);
  assert.equal(html.split(answer.content).length - 1, 1);
});
