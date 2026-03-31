# Law Regulation Dataset Schema

This folder stores local, tracked configuration for the law regulation retrieval and analysis workflow.

Files:

- `field_mapping.yaml`: canonical field aliases used by the skill script

Recommended raw dataset shape:

- One row per legal provision, article, clause, interpretation item, or policy entry
- Prefer at least these columns when available:
  - `id`
  - `name`
  - `content`

Recommended extended columns when available:

- `law_title`
- `article_no`
- `chapter`
- `section`
- `document_type`
- `issuer`
- `publish_date`
- `effective_date`
- `effect_status`
- `jurisdiction`
- `topic`
- `source`
- `source_url`

Supported input formats in phase 1:

- `.json`
- `.jsonl`
- `.csv`
- `.parquet`
- `.xlsx`
- `.xls`
- `.txt`

LeCoQA-style input example:

```json
{"id": 0, "name": "中华人民共和国民法典第四百六十三条", "content": "本编调整因合同产生的民事关系。\n"}