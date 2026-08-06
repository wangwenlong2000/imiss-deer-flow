import assert from "node:assert/strict";
import test from "node:test";

const {
  applyComplianceRetractions,
  bindComplianceThread,
  clearComplianceRetractions,
  complianceDispositionOf,
  isComplianceRetractEvent,
  recordComplianceRetraction,
  stripAppendedComplianceNotice,
} = await import(new URL("./compliance.ts", import.meta.url).href);

/**
 * Captured verbatim from a real retraction produced by the running service
 * (thread a4a8418a, DashScope qwen3.6-35b-a3b). Using the real shape rather than
 * a hand-written one is the point: it is what actually arrives over the wire.
 */
const REAL_METADATA = {
  gate: "OutputGate",
  scene: "public_release",
  streaming_mode: "strict_buffered",
  actions: ["warn", "manual_review"],
  violation_types: ["re_identify"],
  audit_ref: "audit-45e2ac94302e459a",
  basis: [
    "个人信息保护法 §73(4) 去标识化/匿名化",
    "标注指南 7.23 §1.2.1(9)",
  ],
  notice:
    "【合规提示】 检测到合规风险类型：re_identify。 处置动作：warn, manual_review。 依据：个人信息保护法 §73(4) 去标识化/匿名化；标注指南 7.23 §1.2.1(9)。 审计编号：audit-45e2ac94302e459a。",
  retracted: true,
};

const realMessage = () => ({
  type: "ai",
  id: "lc_run--real",
  content:
    "报告显示，某对象的 sub_label=risk，user_id 尾部特征与设备 IMEI 存在稳定关联。\n\n【合规提示】 检测到合规风险类型：re_identify。 处置动作：warn, manual_review。",
  response_metadata: { compliance: REAL_METADATA },
});

// ── the durable read path (survives a page refresh) ─────────────────────────

void test("parses a real retraction off response_metadata", () => {
  const d = complianceDispositionOf(realMessage());
  assert.ok(d);
  assert.equal(d.gate, "OutputGate");
  assert.equal(d.scene, "public_release");
  assert.deepEqual(d.violationTypes, ["re_identify"]);
  assert.deepEqual(d.actions, ["warn", "manual_review"]);
  assert.equal(d.auditRef, "audit-45e2ac94302e459a");
  assert.equal(d.basis.length, 2, "guide requirement 5: the basis must survive");
});

void test("parses an InputGate block for the durable compliance UI", () => {
  const message = {
    type: "ai",
    id: "input-blocked",
    content: "【合规提示】 检测到合规风险类型：hardcoded_cred。",
    response_metadata: {
      compliance: {
        gate: "InputGate",
        scene: "public_release",
        streaming_mode: "input_block",
        transient_exposure_possible: false,
        actions: ["refuse", "warn"],
        violation_types: ["hardcoded_cred"],
        audit_ref: "audit-input",
        basis: ["网络安全法 §21 数据安全保护"],
        notice: "【合规提示】 检测到合规风险类型：hardcoded_cred。",
        retracted: true,
      },
    },
  };

  const disposition = complianceDispositionOf(message);
  assert.ok(disposition);
  assert.equal(disposition.gate, "InputGate");
  assert.equal(disposition.scene, "public_release");
  assert.deepEqual(disposition.actions, ["refuse", "warn"]);
  assert.deepEqual(disposition.violationTypes, ["hardcoded_cred"]);
  assert.equal(disposition.auditRef, "audit-input");
});

void test("warn/manual_review is not treated as a mutation", () => {
  // The phase-1 common case. Marking it destructive would cry wolf on every
  // flagged-but-intact answer.
  assert.equal(complianceDispositionOf(realMessage()).mutated, false);
});

void test("preserves simultaneous struct_id and geo_loc findings", () => {
  const message = realMessage();
  message.response_metadata.compliance = {
    ...REAL_METADATA,
    violation_types: ["struct_id", "geo_loc"],
  };
  const disposition = complianceDispositionOf(message);
  assert.ok(disposition);
  assert.deepEqual(disposition.violationTypes, ["struct_id", "geo_loc"]);
});

void test("refuse/rewrite is treated as a mutation", () => {
  const message = realMessage();
  message.response_metadata.compliance = {
    ...REAL_METADATA,
    actions: ["rewrite", "refuse"],
  };
  assert.equal(complianceDispositionOf(message).mutated, true);
});

void test("ignores messages with no or incomplete compliance metadata", () => {
  assert.equal(complianceDispositionOf({ type: "ai", id: "x", content: "hi" }), null);
  assert.equal(
    complianceDispositionOf({
      type: "ai",
      id: "x",
      content: "hi",
      response_metadata: { compliance: { retracted: false } },
    }),
    null,
  );
});

// ── the appended-notice strip ──────────────────────────────────────────────

void test("strips the appended notice so it is not shown twice", () => {
  const stripped = stripAppendedComplianceNotice(realMessage().content);
  assert.ok(!stripped.includes("【合规提示】"));
  assert.ok(stripped.includes("sub_label=risk"), "the answer itself must survive");
});

void test("never strips a standalone refusal to nothing", () => {
  // On refuse, the notice IS the body — stripping it would leave a blank bubble.
  const refusal = "【合规拦截】该内容包含合规风险，已被安全策略拦截，无法展示。";
  assert.equal(stripAppendedComplianceNotice(refusal), refusal);
  const standaloneNotice = "【合规提示】 检测到合规风险类型：re_identify。";
  assert.equal(stripAppendedComplianceNotice(standaloneNotice), standaloneNotice);
});

// ── the live event path ────────────────────────────────────────────────────

const EVENT = {
  type: "compliance_retract",
  message_id: "m-1",
  scene: "public_release",
  streaming_mode: "strict_buffered",
  violation_types: ["re_identify"],
  action: ["refuse"],
  replacement: "【合规拦截】内容已拦截。",
  notice: "【合规提示】 ...",
  audit_ref: "audit-live",
  basis: ["个人信息保护法 §73(4)"],
};

void test("recognizes the retract event shape", () => {
  assert.equal(isComplianceRetractEvent(EVENT), true);
  assert.equal(isComplianceRetractEvent({ type: "task_running" }), false);
  assert.equal(isComplianceRetractEvent(null), false);
});

void test("a live event stamps durable metadata onto the message", () => {
  clearComplianceRetractions();
  recordComplianceRetraction(EVENT);

  const [out] = applyComplianceRetractions([
    { type: "ai", id: "m-1", content: "violating original" },
  ]);

  assert.equal(out.content, EVENT.replacement, "content is replaced");
  const d = complianceDispositionOf(out);
  assert.ok(d, "the component reads metadata, so the event must stamp it");
  assert.equal(d.auditRef, "audit-live");
  assert.equal(d.scene, "public_release");
  assert.deepEqual(d.basis, ["个人信息保护法 §73(4)"]);
  assert.equal(d.mutated, true);
  clearComplianceRetractions();
});

void test("stamping is identity-stable so memo() does not thrash", () => {
  clearComplianceRetractions();
  recordComplianceRetraction(EVENT);
  const input = [{ type: "ai", id: "m-1", content: "violating original" }];
  assert.equal(
    applyComplianceRetractions(input)[0],
    applyComplianceRetractions(input)[0],
  );
  clearComplianceRetractions();
});

void test("messages without a retraction pass through untouched", () => {
  clearComplianceRetractions();
  const input = [{ type: "ai", id: "other", content: "fine" }];
  assert.equal(applyComplianceRetractions(input), input);
});

// ── thread binding (the cross-thread id collision guard) ───────────────────

void test("switching threads clears retractions", () => {
  clearComplianceRetractions();
  bindComplianceThread("thread-A");
  recordComplianceRetraction(EVENT);

  bindComplianceThread("thread-B");
  const input = [{ type: "ai", id: "m-1", content: "unrelated answer" }];
  assert.equal(
    applyComplianceRetractions(input),
    input,
    "a retraction from thread A must not apply in thread B",
  );
});

void test("a new conversation's undefined -> id transition keeps retractions", () => {
  // ChatPage passes `isNewThread ? undefined : threadId`, so this transition
  // happens mid-conversation. Clearing on it would discard a live retraction.
  clearComplianceRetractions();
  bindComplianceThread("thread-C");
  recordComplianceRetraction(EVENT);

  bindComplianceThread(undefined);
  const [out] = applyComplianceRetractions([
    { type: "ai", id: "m-1", content: "violating original" },
  ]);
  assert.equal(out.content, EVENT.replacement);
  clearComplianceRetractions();
});
