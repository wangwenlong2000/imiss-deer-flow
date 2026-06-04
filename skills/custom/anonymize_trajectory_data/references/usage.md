# anonymize_trajectory_data 使用说明

## 目的
在共享或对外分析前对原始轨迹做去标识，降低再识别风险。

## 三种泛化手段
1. 伪名化：`user_id` → `hash_uid(salt+user_id)`（12 位 sha256，不可逆）。
2. 空间泛化：`geohash` 截断到 `--coarse-precision`（默认 5 位，约 ±2.4km 网格）。
3. 时间泛化：时间戳按 `--time-bucket`（hour/day）分桶。

## k 值参考
`summary.json` 里的 `min_cell_k` 是脱敏后最小粗化网格的记录数。值越大，单个网格越难被再识别。若 `min_cell_k` 偏小，可调大 `--coarse-precision` 的粗度（更小的位数）或改用 `--time-bucket day`。
