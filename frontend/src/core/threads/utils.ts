import type { Message } from "@langchain/langgraph-sdk";
import type { BaseStream } from "@langchain/langgraph-sdk/react";

import type { AgentThreadState } from "./types";

type ThreadTitleSource = {
  values?: {
    title?: string;
  };
};

type ThreadDisplaySource =
  | Pick<BaseStream<AgentThreadState>, "messages" | "values">
  | {
      messages: Message[];
      values?: {
        title?: string;
        raw_messages?: Message[];
      };
    };

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

function isCompactionSummaryMessage(message: Message) {
  const text = textOfMessage(message)?.trimStart();
  return text
    ? COMPACTION_SUMMARY_PREFIXES.some((prefix) => text.startsWith(prefix))
    : false;
}

function visibleMessages(messages: Message[]) {
  return messages.filter((message) => !isCompactionSummaryMessage(message));
}

export function titleOfThread(thread: ThreadTitleSource) {
  return thread.values?.title ?? "Untitled";
}

export function displayMessagesOfThread(thread: ThreadDisplaySource) {
  const rawMessages = visibleMessages(thread.values?.raw_messages ?? []);
  return rawMessages.length ? rawMessages : visibleMessages(thread.messages);
}
