import type { Message } from "@langchain/langgraph-sdk";

/**
 * Compliance OutputGate retraction.
 *
 * Strict scenes buffer and inspect before delivery. Compatibility scenes keep
 * streaming and retract on a hit. When a violation lands the backend:
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
 * The after-the-fact window applies only to compatibility scenes. Strict
 * `cross_org`, `public_release`, and `research_anon` suppress original model
 * chunks before this parser can observe them.
 */
export type ComplianceRetractEvent = {
  type: "compliance_retract";
  message_id: string;
  scene?: string;
  streaming_mode?: string;
  violation_types: string[];
  action: string[];
  replacement: string;
  notice?: string;
  audit_ref?: string | null;
  basis?: string[];
};

export type ComplianceRetraction = {
  messageId: string;
  scene?: string;
  streamingMode?: string;
  violationTypes: string[];
  actions: string[];
  replacement: string;
  notice?: string;
  auditRef?: string | null;
  basis?: string[];
};

const retractions = new Map<string, ComplianceRetraction>();

/** The ten violation types, mirroring backend `contract.VIOLATION_TYPES`. */
export const COMPLIANCE_VIOLATION_TYPES = [
  "struct_id",
  "geo_loc",
  "hardcoded_cred",
  "illegal_content",
  "political",
  "text_id",
  "confidential",
  "video_meta_leak",
  "re_identify",
  "domain",
] as const;
export type ComplianceViolationType =
  (typeof COMPLIANCE_VIOLATION_TYPES)[number];

/** Disposition codes, weakest first — mirrors backend `contract.ACTIONS`. */
export const COMPLIANCE_ACTIONS = [
  "allow",
  "warn",
  "report",
  "role_check",
  "manual_review",
  "aggregate",
  "desensitize",
  "rewrite",
  "block_storage",
  "refuse",
] as const;
export type ComplianceAction = (typeof COMPLIANCE_ACTIONS)[number];

export const COMPLIANCE_GATES = [
  "InputGate",
  "ContextGate",
  "OutputGate",
] as const;
export type ComplianceGate = (typeof COMPLIANCE_GATES)[number];

export const COMPLIANCE_SCENES = [
  "self_use",
  "internal_org",
  "cross_org",
  "public_release",
  "research_anon",
  "_unknown",
] as const;
export type ComplianceScene = (typeof COMPLIANCE_SCENES)[number];

export const COMPLIANCE_CHECK_STATUSES = [
  "passed",
  "allowed",
  "handled",
] as const;
export type ComplianceCheckStatus =
  (typeof COMPLIANCE_CHECK_STATUSES)[number];

/**
 * Actions that changed or withheld the answer — mirrors backend
 * `contract.MUTATING_ACTIONS`.
 *
 * This drives whether the banner reads as an alert or as a note. The trusted
 * scene now comes from response metadata; the banner reflects the matrix action,
 * while `_unknown` is delivered conservatively by the backend.
 */
const MUTATING_ACTIONS = new Set<string>([
  "aggregate",
  "desensitize",
  "rewrite",
  "block_storage",
  "refuse",
]);

/** What the UI needs to explain a compliance disposition. */
export type ComplianceCheck = {
  gate: string | null;
  scene: string | null;
  status: ComplianceCheckStatus;
  violationTypes: string[];
  actions: string[];
  basis: string[];
  auditRef: string | null;
};

export type ComplianceDisposition = {
  gate: string | null;
  scene: string | null;
  violationTypes: string[];
  actions: string[];
  basis: string[];
  auditRef: string | null;
  notice: string | null;
  checks: ComplianceCheck[];
  /** True when the answer itself was altered or withheld. */
  mutated: boolean;
};

function strings(value: unknown): string[] {
  return Array.isArray(value)
    ? value.filter((item): item is string => typeof item === "string")
    : [];
}

function checkOf(value: unknown): ComplianceCheck | null {
  if (typeof value !== "object" || value === null) {
    return null;
  }
  const raw = value as Record<string, unknown>;
  const rawStatus = raw.status;
  const status = COMPLIANCE_CHECK_STATUSES.includes(
    rawStatus as ComplianceCheckStatus,
  )
    ? (rawStatus as ComplianceCheckStatus)
    : "handled";
  return {
    gate: typeof raw.gate === "string" ? raw.gate : null,
    scene: typeof raw.scene === "string" ? raw.scene : null,
    status,
    violationTypes: strings(raw.violation_types),
    actions: strings(raw.actions),
    basis: strings(raw.basis),
    auditRef: typeof raw.audit_ref === "string" ? raw.audit_ref : null,
  };
}

/**
 * Read the durable disposition off a message.
 *
 * The backend stamps `response_metadata.compliance` onto the rewritten
 * AIMessage, so this survives a page reload — unlike the live retract event,
 * which only exists for the lifetime of the stream. Components read this and
 * nothing else, which keeps them pure and keeps `memo()` honest.
 */
export function complianceDispositionOf(
  message: Message,
): ComplianceDisposition | null {
  const raw = (message as { response_metadata?: Record<string, unknown> })
    .response_metadata?.compliance;
  if (typeof raw !== "object" || raw === null) {
    return null;
  }
  const meta = raw as Record<string, unknown>;
  if (meta.evaluated !== true && meta.retracted !== true) {
    return null;
  }

  const checks = Array.isArray(meta.checks)
    ? meta.checks.map(checkOf).filter((item): item is ComplianceCheck => item !== null)
    : [];
  if (checks.length === 0) {
    checks.push(
      checkOf({
        gate: meta.gate,
        scene: meta.scene,
        status: "handled",
        violation_types: meta.violation_types,
        actions: meta.actions,
        basis: meta.basis,
        audit_ref: meta.audit_ref,
      })!,
    );
  }

  const actions = Array.from(
    new Set(checks.flatMap((check) => check.actions)),
  );
  const violationTypes = Array.from(
    new Set(checks.flatMap((check) => check.violationTypes)),
  );
  const basis = Array.from(new Set(checks.flatMap((check) => check.basis)));
  return {
    gate: typeof meta.gate === "string" ? meta.gate : null,
    scene: typeof meta.scene === "string" ? meta.scene : null,
    violationTypes,
    actions,
    basis,
    auditRef: typeof meta.audit_ref === "string" ? meta.audit_ref : null,
    notice: typeof meta.notice === "string" ? meta.notice : null,
    checks,
    mutated: actions.some((action) => MUTATING_ACTIONS.has(action)),
  };
}

/**
 * The `【合规提示】` paragraph the backend appends on warn / manual_review.
 *
 * On those dispositions the answer is kept and a notice is glued to the bottom.
 * The banner already shows that information, so rendering both means the user
 * reads it twice. Stripped at render time only — `message.content` stays intact
 * so Copy still yields the complete, compliant answer.
 *
 * Requires the leading blank line, so a *standalone* notice (a refusal, where
 * the notice IS the body) is never stripped to nothing.
 */
const APPENDED_NOTICE = /\n{2,}【合规提示】[\s\S]*$/;

export function stripAppendedComplianceNotice(
  text: string,
  standaloneNotice?: string | null,
): string {
  if (text.trim() === standaloneNotice?.trim()) {
    return "";
  }
  return text.replace(APPENDED_NOTICE, "").trimEnd();
}

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
    scene: event.scene,
    streamingMode: event.streaming_mode,
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

function complianceRetractionOf(
  messageId: string | undefined | null,
): ComplianceRetraction | undefined {
  if (!messageId) {
    return undefined;
  }
  return retractions.get(messageId);
}

/**
 * Stable identity per input message, so `memo(MessageContent)` does not thrash.
 * Keyed on the input object, so repeated passes return the same output object.
 */
const stamped = new WeakMap<object, Message>();

/**
 * Fold a live retraction into the message's durable compliance metadata.
 *
 * The two sources are complementary: the event arrives first and carries
 * `basis`/`notice`, the metadata arrives with the rewritten message and survives
 * a reload. Stamping the event onto the metadata field means components read one
 * shape and never touch the module store.
 */
function stamp(message: Message, retraction: ComplianceRetraction): Message {
  const cached = stamped.get(message);
  if (cached) {
    return cached;
  }

  const previous =
    ((message as { response_metadata?: Record<string, unknown> })
      .response_metadata?.compliance as Record<string, unknown>) ?? {};

  const next = {
    ...message,
    content: retraction.replacement,
    response_metadata: {
      ...((message as { response_metadata?: Record<string, unknown> })
        .response_metadata ?? {}),
      compliance: {
        ...previous,
        retracted: true,
        gate: previous.gate ?? "OutputGate",
        scene: retraction.scene ?? previous.scene ?? null,
        streaming_mode:
          retraction.streamingMode ?? previous.streaming_mode ?? null,
        violation_types: retraction.violationTypes.length
          ? retraction.violationTypes
          : (previous.violation_types ?? []),
        actions: retraction.actions.length
          ? retraction.actions
          : (previous.actions ?? []),
        audit_ref: retraction.auditRef ?? previous.audit_ref ?? null,
        basis: retraction.basis?.length
          ? retraction.basis
          : (previous.basis ?? []),
        notice: retraction.notice ?? previous.notice ?? null,
      },
    },
  } as Message;

  stamped.set(message, next);
  return next;
}

/**
 * Apply live retractions on top of whatever the server has sent so far.
 *
 * Deliberately no "content already matches, skip" early return: once the
 * rewritten server message arrives the content does match, but it may lack the
 * `basis`/`notice` the event carried. The WeakMap provides identity stability
 * instead.
 */
export function applyComplianceRetractions(messages: Message[]): Message[] {
  if (retractions.size === 0) {
    // Post-reload fast path: the durable metadata already stands on its own.
    return messages;
  }

  let changed = false;
  const result = messages.map((message) => {
    const retraction = complianceRetractionOf(message.id);
    if (!retraction) {
      return message;
    }
    const next = stamp(message, retraction);
    if (next !== message) {
      changed = true;
    }
    return next;
  });

  return changed ? result : messages;
}

/** Clear stored retractions. */
export function clearComplianceRetractions() {
  retractions.clear();
}

let ownerThreadId: string | null = null;

/**
 * Bind the retraction store to a thread.
 *
 * Message ids are only unique within a run and the store is module-level, so
 * without this it grows for the lifetime of the tab and a retraction recorded in
 * one thread could match a message in another.
 *
 * The falsy guard is load-bearing: a new conversation transitions
 * `undefined -> uuid` mid-session, and clearing on that transition would discard
 * a retraction belonging to the conversation in progress.
 */
export function bindComplianceThread(threadId: string | null | undefined) {
  if (!threadId || threadId === ownerThreadId) {
    return;
  }
  ownerThreadId = threadId;
  clearComplianceRetractions();
}
