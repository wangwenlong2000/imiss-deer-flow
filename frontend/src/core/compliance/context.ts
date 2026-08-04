export type ManualComplianceIdentity =
  | "ordinary_user"
  | "internal_operator"
  | "cross_org_operator"
  | "researcher"
  | "publisher";

export type ManualComplianceScene =
  | "self_use"
  | "internal_org"
  | "cross_org"
  | "public_release"
  | "research_anon";

export interface ManualComplianceContext {
  source: "manual_ui";
  enabled: boolean;
  identity: ManualComplianceIdentity;
  scene: ManualComplianceScene;
  user_context: {
    user_id: string;
    roles: string[];
    org_id: string;
  };
}

export const DEFAULT_MANUAL_COMPLIANCE_CONTEXT: ManualComplianceContext = {
  source: "manual_ui",
  enabled: false,
  identity: "ordinary_user",
  scene: "self_use",
  user_context: {
    user_id: "manual-user",
    roles: ["user"],
    org_id: "personal",
  },
};

export const MANUAL_COMPLIANCE_IDENTITIES: ReadonlyArray<{
  value: ManualComplianceIdentity;
  label: string;
  role: string;
  orgId: string;
}> = [
  {
    value: "ordinary_user",
    label: "普通用户",
    role: "user",
    orgId: "personal",
  },
  {
    value: "internal_operator",
    label: "内部工作人员",
    role: "internal_operator",
    orgId: "city-governance",
  },
  {
    value: "cross_org_operator",
    label: "跨组织协作人员",
    role: "cross_org_operator",
    orgId: "partner-org",
  },
  {
    value: "researcher",
    label: "科研人员",
    role: "researcher",
    orgId: "research",
  },
  {
    value: "publisher",
    label: "公开发布人员",
    role: "publisher",
    orgId: "publicity",
  },
];

export const MANUAL_COMPLIANCE_SCENES: ReadonlyArray<{
  value: ManualComplianceScene;
  label: string;
}> = [
  { value: "self_use", label: "自用" },
  { value: "internal_org", label: "组织内部" },
  { value: "cross_org", label: "跨组织" },
  { value: "public_release", label: "公开发布" },
  { value: "research_anon", label: "科研匿名化" },
];

export function getManualIdentity(value: ManualComplianceIdentity) {
  return (
    MANUAL_COMPLIANCE_IDENTITIES.find((item) => item.value === value) ??
    MANUAL_COMPLIANCE_IDENTITIES[0]!
  );
}

export function getManualScene(value: ManualComplianceScene) {
  return (
    MANUAL_COMPLIANCE_SCENES.find((item) => item.value === value) ??
    MANUAL_COMPLIANCE_SCENES[0]!
  );
}

export function withManualIdentity(
  context: ManualComplianceContext,
  identity: ManualComplianceIdentity,
): ManualComplianceContext {
  const selected = getManualIdentity(identity);
  return {
    ...context,
    enabled: true,
    identity,
    user_context: {
      ...context.user_context,
      user_id: `manual-${identity}`,
      roles: [selected.role],
      org_id: selected.orgId,
    },
  };
}
