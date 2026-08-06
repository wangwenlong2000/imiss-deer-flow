

1. `Input Envelope` 和 `SkillResult` 不应该被设计成另一套独立协议，而是放进 `Skill Schema` 的 retrieval profile 里。这样 `Skill Schema` 仍然是唯一契约来源，retrieve 输入输出只是它的一个剖面，后续字段校验、版本演进、文档维护都收敛到同一套 Schema 上。

2. `budget` 建议放在 `Envelope` 顶层，并且做成可选字段。`Planner` 在生成 Envelope 时主动给出 `max_evidence_count` 和 `max_token_estimate`，各 retrieve 按这个预算返回结果；如果一次计划里并行调多个 retrieve，Planner 需要先拆分总预算，聚合层再做一次兜底裁剪，避免每个 retrieve 都按满额返回，最后把 32k 上下文撑爆。

3. Plan 模板这里需要承担“拆解和编排”的职责。它先把复合 query 拆成若干子问题，再为每个 retrieval task 生成对应的 Envelope，并决定哪些 retrieve 可以并行调度。多个 retrieve 返回后，结果统一进入聚合层，由聚合层按 `query_id` 做对齐、去重、排序和预算裁剪，最后再把收敛后的证据交给回答生成或后续执行型 Skill。
