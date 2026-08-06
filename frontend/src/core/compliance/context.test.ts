import assert from "node:assert/strict";
import test from "node:test";

const {
  MANUAL_COMPLIANCE_IDENTITIES,
  normalizeManualComplianceContext,
  withManualIdentity,
} = await import(new URL("./context.ts", import.meta.url).href);

const BASE = normalizeManualComplianceContext(undefined);

void test("all guide 7.23 profiles bind a unique identity to the intended scene", () => {
  assert.deepEqual(
    MANUAL_COMPLIANCE_IDENTITIES.map((profile) => [
      profile.value,
      profile.scene,
    ]),
    [
      ["ordinary_user", "self_use"],
      ["internal_operator", "internal_org"],
      ["cross_org_operator", "cross_org"],
      ["publisher", "public_release"],
      ["researcher", "research_anon"],
    ],
  );
});

void test("selecting an identity atomically replaces scene, role, user and organization", () => {
  const context = withManualIdentity(BASE, "publisher");
  assert.equal(context.enabled, true);
  assert.equal(context.scene, "public_release");
  assert.deepEqual(context.user_context, {
    user_id: "manual-publisher",
    roles: ["publisher"],
    org_id: "publicity",
  });
});

void test("stored mismatched context is migrated to the identity's guide scene", () => {
  const context = normalizeManualComplianceContext({
    ...BASE,
    enabled: true,
    identity: "researcher",
    scene: "self_use",
    user_context: {
      user_id: "spoofed",
      roles: ["publisher"],
      org_id: "publicity",
    },
  });
  assert.equal(context.scene, "research_anon");
  assert.deepEqual(context.user_context, {
    user_id: "manual-researcher",
    roles: ["researcher"],
    org_id: "research",
  });
});
