import type { ToolCall } from "@langchain/core/messages";
import type { AIMessage } from "@langchain/langgraph-sdk";

import type { Translations } from "../i18n";
import { hasToolCalls } from "../messages/utils";

export function explainLastToolCall(message: AIMessage, t: Translations) {
  if (hasToolCalls(message)) {
    const lastToolCall = message.tool_calls![message.tool_calls!.length - 1]!;
    return explainToolCall(lastToolCall, t);
  }
  return t.common.thinking;
}


function getInvokeSkillDisplayName(args: unknown) {
  if (typeof args !== "object" || args === null) {
    return "调用 Skill";
  }

  const skillName = (args as { skill_name?: unknown }).skill_name;
  const mode = (args as { mode?: unknown }).mode;

  if (typeof skillName !== "string" || !skillName.trim()) {
    return "调用 Skill";
  }

  if (mode === "prepare") {
    return `准备调用 Skill：${skillName}`;
  }

  if (mode === "wrap_output") {
    return `整理 Skill 输出：${skillName}`;
  }

  return `调用 Skill：${skillName}`;
}


export function explainToolCall(toolCall: ToolCall, t: Translations) {
  if (toolCall.name === "web_search" || toolCall.name === "image_search") {
    return t.toolCalls.searchFor(toolCall.args.query);
  } else if (toolCall.name === "web_fetch") {
    return t.toolCalls.viewWebPage;
  } else if (toolCall.name === "present_files") {
    return t.toolCalls.presentFiles;
  } else if (toolCall.name === "write_todos") {
    return t.toolCalls.writeTodos;
  } else if (toolCall.name === "invoke_skill") {
    return getInvokeSkillDisplayName(toolCall.args);
  } else if (toolCall.args.description) {
    return toolCall.args.description;
  } else {
    return t.toolCalls.useTool(toolCall.name);
  }
}
