"use client";

import { Client as LangGraphClient } from "@langchain/langgraph-sdk/client";

import { getLangGraphBaseURL } from "../config";

import { sanitizeRunStreamOptions } from "./stream-mode";

const BUFFERED_COMPLIANCE_STREAM_MODES = ["values", "updates"] as const;
const BUFFERED_COMPLIANCE_STREAM_MODE_SET = new Set<string>(BUFFERED_COMPLIANCE_STREAM_MODES);

function complianceSafeStreamOptions<T>(options: T): T {
  const payload = typeof options === "object" && options !== null
    ? options as Record<string, unknown>
    : {};
  const rawModes = payload.streamMode;
  const requestedModes = rawModes == null
    ? []
    : Array.isArray(rawModes)
      ? rawModes
      : [rawModes];
  const allowedModes = requestedModes.filter(
    (mode): mode is string => typeof mode === "string" && BUFFERED_COMPLIANCE_STREAM_MODE_SET.has(mode),
  );
  const safeModes = requestedModes.length === 0 || allowedModes.length === 0
    ? [...BUFFERED_COMPLIANCE_STREAM_MODES]
    : allowedModes;

  return sanitizeRunStreamOptions({
    ...payload,
    streamMode: safeModes,
  }) as T;
}

function createCompatibleClient(isMock?: boolean): LangGraphClient {
  const client = new LangGraphClient({
    apiUrl: getLangGraphBaseURL(isMock),
  });

  const originalRunStream = client.runs.stream.bind(client.runs);
  client.runs.stream = ((threadId, assistantId, payload) =>
    originalRunStream(
      threadId,
      assistantId,
      complianceSafeStreamOptions(payload),
    )) as typeof client.runs.stream;

  const originalJoinStream = client.runs.joinStream.bind(client.runs);
  client.runs.joinStream = ((threadId, runId, options) =>
    originalJoinStream(
      threadId,
      runId,
      complianceSafeStreamOptions(options),
    )) as typeof client.runs.joinStream;

  return client;
}

let _singleton: LangGraphClient | null = null;
export function getAPIClient(isMock?: boolean): LangGraphClient {
  _singleton ??= createCompatibleClient(isMock);
  return _singleton;
}
