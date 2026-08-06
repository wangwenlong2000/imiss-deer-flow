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
    user_id: "manual-ordinary_user",
    roles: ["user"],
    org_id: "personal",
  },
};

export const MANUAL_COMPLIANCE_IDENTITIES: ReadonlyArray<{
  value: ManualComplianceIdentity;
  label: string;
  role: string;
  orgId: string;
  scene: ManualComplianceScene;
  sceneLabel: string;
  audience: string;
  whenToUse: string;
  description: string;
}> = [
  {
    value: "ordinary_user",
    label: "个人自查用户",
    role: "user",
    orgId: "personal",
    scene: "self_use",
    sceneLabel: "自用",
    audience: "本人数据的所有者或上传者",
    whenToUse: "只查询、核对或分析自己上传且有权访问的数据",
    description:
      "适合本人数据的所有者或上传者，在只查询、核对或分析自己上传且有权访问的数据时选择。该场景相对宽松，但凭证、违法有害、政治敏感等底线风险仍会拦截。",
  },
  {
    value: "internal_operator",
    label: "同组织内部工作人员",
    role: "internal_operator",
    orgId: "city-governance",
    scene: "internal_org",
    sceneLabel: "组织内部",
    audience: "同一单位、部门或项目组工作人员",
    whenToUse: "数据只在本组织内部分析、协作或形成内部报告",
    description:
      "适合同一单位、部门或项目组的工作人员，在数据只用于组织内部分析、协作或内部报告时选择。敏感内容仍可能触发角色校验、告警或降低精度。",
  },
  {
    value: "cross_org_operator",
    label: "已授权跨组织协作人员",
    role: "cross_org_operator",
    orgId: "partner-org",
    scene: "cross_org",
    sceneLabel: "跨组织",
    audience: "已获授权的外部协作或跨部门人员",
    whenToUse: "数据需要提供给另一组织、委办局、审计方或合作伙伴",
    description:
      "适合已获得协作授权的跨组织人员，在数据需要提供给另一组织、委办局、审计方或合作伙伴时选择。因为越过组织信任边界，标识符和精确位置应脱敏，并保留审计记录。",
  },
  {
    value: "publisher",
    label: "公开发布操作人员",
    role: "publisher",
    orgId: "publicity",
    scene: "public_release",
    sceneLabel: "公开发布",
    audience: "负责面向公众发布内容的操作人员",
    whenToUse: "结果将进入公开报告、新闻、公众查询服务或互联网",
    description:
      "适合负责公开报告、新闻、公众查询服务或互联网发布的操作人员。该场景面向不特定公众，合规要求最严格，不会因为具有发布岗位而允许公开敏感原始值。",
  },
  {
    value: "researcher",
    label: "科研人员（匿名化数据）",
    role: "researcher",
    orgId: "research",
    scene: "research_anon",
    sceneLabel: "科研匿名化",
    audience: "使用已匿名化数据的科研或统计人员",
    whenToUse: "开展宏观规律、统计分析、模型训练或论文研究",
    description:
      "适合科研机构、高校或内部统计人员，在数据已经匿名化并用于宏观规律、统计分析、模型训练或论文研究时选择。结果应聚合展示，原始标识符不得直接输出。",
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
    scene: selected.scene,
    user_context: {
      ...context.user_context,
      user_id: `manual-${identity}`,
      roles: [selected.role],
      org_id: selected.orgId,
    },
  };
}

export function normalizeManualComplianceContext(
  context: Partial<ManualComplianceContext> | null | undefined,
): ManualComplianceContext {
  const identity = getManualIdentity(
    context?.identity ?? DEFAULT_MANUAL_COMPLIANCE_CONTEXT.identity,
  );
  return {
    source: "manual_ui",
    enabled: context?.enabled === true,
    identity: identity.value,
    scene: identity.scene,
    user_context: {
      user_id: `manual-${identity.value}`,
      roles: [identity.role],
      org_id: identity.orgId,
    },
  };
}
