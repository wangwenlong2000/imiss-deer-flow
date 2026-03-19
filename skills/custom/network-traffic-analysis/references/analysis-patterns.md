# Analysis Patterns

Use these patterns after `inspect` confirms the available canonical fields.

## Traffic overview

- Total records
- Time range
- Sum of `bytes`
- Sum of `packets`
- Unique `src_ip`
- Unique `dst_ip`
- Protocol distribution

## Suggested operator mapping

- "最活跃 / top / 最多" -> `topn`
- "趋势 / 按小时 / 按天 / 峰值" -> `timeseries`
- "占比 / 分布 / 协议分布 / 端口分布" -> `distribution`
- "筛出 / 导出 / 某时间窗 / 某网段" -> `filter` or `export`
- "分组统计" -> `aggregate`
- "异常 / 突增 / 扫描 / 稀有端口" -> `detect-anomaly`
- "精确自定义计算" -> `query`

