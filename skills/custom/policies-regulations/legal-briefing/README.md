# legal-briefing

## 目录结构

```text
legal-briefing/
├── SKILL.md
├── README.md
└── references/
    ├── 10-labor-dispute.md
    ├── 11-contract-dispute.md
    ├── 12-tort-dispute.md
    ├── 13-family-and-inheritance.md
    └── 14-enterprise-compliance.md
```

## 定位
- 本 skill 负责把已有法律材料或已有分析结果整理成可交付文本。
- 本 skill 不承担首次法律检索职责。
- 本 skill 不承担深度案件分析、证据策略设计或合同逐条审查职责。

## 与其他法律 skill 的分工
- `yuandian-law-research`：找法、找依据、找规则来源。
- `china-case-analysis`：分析案件、拆争点、做合同审查和证据清单。
- `legal-briefing`：整理成备忘录、摘要、报告、风险提示和简报。

## 设计原则
- 主文件保留边界、协作方式、通用规则、默认流程和禁止事项。
- `references/` 只保留具体业务场景文件。
- 默认输出为 Markdown。
- 若后续需要 Word / `.docx` 导出，建议通过独立 docx 或文档导出 skill 完成，而不是并入本 skill。
