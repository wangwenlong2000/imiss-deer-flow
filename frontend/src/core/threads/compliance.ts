import type { Message } from "@langchain/langgraph-sdk";

/**
 * Compliance OutputGate retraction.
 *
 * The backend keeps streaming and retracts on a hit rather than buffering the
 * whole answer. When a violation lands it does two things:
 *
 *   1. emits a `compliance_retract` custom stream event — handled here, so the
 *      offending text leaves the screen immediately;
 *   2. rewrites the `AIMessage` in `after_model` — which arrives through the
 *      normal `values` / `messages` stream a moment later.
 *
 * Step 2 alone would leave the violation visible until the run finishes; step 1
 * alone would let a page refresh bring it back. Both are needed.
 *
 * Retractions are held in a module-level store because `displayMessagesOfThread`
 * is a pure function called from several places (message list, artifact loader).
 * Routing the replacement through the store means every caller sees the
 * retracted content without threading extra props through the tree.
 *
 * Honest limitation: this is after-the-fact. Between a violating token being
 * rendered and this event arriving there is a visible window — anything the user
 * screenshots or reads inside it cannot be recalled. Narrowing the window is
 * what `compliance.gates.output.incremental_scan.interval_chars` controls.
 */
export type ComplianceRetractEvent = {
  type: "compliance_retract";
  message_id: string;
  violation_types: string[];
  action: string[];
  replacement: string;
  notice?: string;
  audit_ref?: string | null;
  basis?: string[];
};

export type ComplianceRetraction = {
  messageId: string;
  violationTypes: string[];
  actions: string[];
  replacement: string;
  notice?: string;
  auditRef?: string | null;
  basis?: string[];
};

const retractions = new Map<string, ComplianceRetraction>();

export function isComplianceRetractEvent(
  event: unknown,
): event is ComplianceRetractEvent {
  return (
    typeof event === "object" &&
    event !== null &&
    "type" in event &&
    (event as { type?: unknown }).type === "compliance_retract" &&
    "message_id" in event &&
    typeof (event as { message_id?: unknown }).message_id === "string" &&
    "replacement" in event &&
    typeof (event as { replacement?: unknown }).replacement === "string"
  );
}

export function recordComplianceRetraction(event: ComplianceRetractEvent) {
  const retraction: ComplianceRetraction = {
    messageId: event.message_id,
    violationTypes: Array.isArray(event.violation_types)
      ? event.violation_types
      : [],
    actions: Array.isArray(event.action) ? event.action : [],
    replacement: event.replacement,
    notice: event.notice,
    auditRef: event.audit_ref ?? null,
    basis: Array.isArray(event.basis) ? event.basis : [],
  };
  retractions.set(event.message_id, retraction);
  return retraction;
}

export function complianceRetractionOf(
  messageId: string | undefined | null,
): ComplianceRetraction | undefined {
  if (!messageId) {
    return undefined;
  }
  return retractions.get(messageId);
}

/**
 * Replace the content of any message that has been retracted.
 *
 * Matching is by message id, which the backend deliberately preserves when it
 * rewrites the message — that is what makes the LangGraph reducer replace the
 * original rather than append a second copy.
 */
export function applyComplianceRetractions(messages: Message[]): Message[] {
  if (retractions.size === 0) {
    return messages;
  }

  let changed = false;
  const result = messages.map((message) => {
    const retraction = complianceRetractionOf(message.id);
    if (!retraction) {
      return message;
    }
    if (message.content === retraction.replacement) {
      return message;
    }
    changed = true;
    return { ...message, content: retraction.replacement };
  });

  return changed ? result : messages;
}

/** Clear stored retractions. Used when switching threads. */
export function clearComplianceRetractions() {
  retractions.clear();
}
