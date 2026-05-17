import type { Message } from "@langchain/langgraph-sdk";
import type { BaseStream } from "@langchain/langgraph-sdk/react";

import { isInternalMessage } from "../messages/utils";

import type { AgentThreadState } from "./types";

type MessageMetadataLookup = {
  getMessagesMetadata?: (
    message: Message,
    index?: number,
  ) =>
    | {
        streamMetadata?: Record<string, unknown>;
      }
    | undefined;
};

type ThreadTitleSource = {
  values?: {
    title?: string;
  };
};

type ThreadDisplaySource =
  | (Pick<BaseStream<AgentThreadState>, "messages" | "values"> &
      MessageMetadataLookup)
  | {
      messages: Message[];
      values?: {
        title?: string;
        raw_messages?: Message[];
      };
    } & MessageMetadataLookup;

export function pathOfThread(threadId: string) {
  return `/workspace/chats/${threadId}`;
}

export function textOfMessage(message: Message) {
  if (typeof message.content === "string") {
    return message.content;
  } else if (Array.isArray(message.content)) {
    for (const part of message.content) {
      if (part.type === "text") {
        return part.text;
      }
    }
  }
  return null;
}

const COMPACTION_SUMMARY_PREFIXES = [
  "Here is a summary of the conversation to date:",
  "This is a summary of the conversation to date:",
  "Conversation summary:",
];

const INTERNAL_STREAM_NODE_PREFIXES = ["IntentRecognitionMiddleware."];

function isCompactionSummaryMessage(message: Message) {
  const text = textOfMessage(message)?.trimStart();
  return text
    ? COMPACTION_SUMMARY_PREFIXES.some((prefix) => text.startsWith(prefix))
    : false;
}

function isInternalStreamMessage(
  message: Message,
  source?: MessageMetadataLookup,
  index?: number,
) {
  if (message.type !== "ai") {
    return false;
  }

  const metadata = source?.getMessagesMetadata?.(message, index)?.streamMetadata;
  const node = metadata?.langgraph_node;
  return (
    typeof node === "string" &&
    INTERNAL_STREAM_NODE_PREFIXES.some((prefix) => node.startsWith(prefix))
  );
}

function visibleMessages(messages: Message[], source?: MessageMetadataLookup) {
  return messages.filter(
    (message, index) =>
      !isCompactionSummaryMessage(message) &&
      !isInternalMessage(message) &&
      !isInternalStreamMessage(message, source, index),
  );
}

export function titleOfThread(thread: ThreadTitleSource) {
  return thread.values?.title ?? "Untitled";
}

export function displayMessagesOfThread(thread: ThreadDisplaySource) {
  const rawMessages = visibleMessages(thread.values?.raw_messages ?? []);
  const streamMessages = visibleMessages(thread.messages ?? [], thread);

  return mergeDisplayMessages(rawMessages, streamMessages);
}

function mergeDisplayMessages(
  rawMessages: Message[],
  streamMessages: Message[],
): Message[] {
  const merged: Message[] = [];
  const seen = new Set<string>();

  function messageKey(message: Message) {
    if (message.id) {
      return `id:${message.id}`;
    }

    const content =
      typeof message.content === "string"
        ? message.content
        : JSON.stringify(message.content ?? "");
    const name =
      "name" in message && typeof message.name === "string"
        ? message.name
        : "";
    const toolCallId =
      "tool_call_id" in message && typeof message.tool_call_id === "string"
        ? message.tool_call_id
        : "";

    return ["fallback", message.type, name, toolCallId, content].join("|");
  }

  function add(message: Message) {
    const key = messageKey(message);
    if (seen.has(key)) {
      return;
    }
    seen.add(key);
    merged.push(message);
  }

  for (const message of rawMessages) {
    add(message);
  }
  for (const message of streamMessages) {
    add(message);
  }

  return merged;
}
