# Xi'an Traffic Annual Report RAG

The LLM decides from the user request whether RAG is needed. `rag_xian2024_min.py` only executes retrieval; it does not classify intent.

Use `rag_xian2024_min.py` when the user needs cited background facts from the 2024 Xi'an traffic annual report, such as citywide traffic indicators, annual trends, benchmark values, planning context, or source-backed wording for a governance report.

Do not use RAG as a substitute for realtime Baidu road condition, weather, route, forecast, or anomaly results.

## Dependency

The RAG script needs optional Python packages in the same environment used by the agent:

```bash
pip install -r /mnt/skills/custom/road-traffic-analysis/scripts/requirements-rag.txt
```

Equivalent packages:

```bash
pip install chromadb==1.5.8 sentence-transformers==5.4.1 PyMuPDF==1.27.2.3
```

## Data Scope

- Vector DB: `scripts/rag_db/chroma`
- Collection: `xian_traffic_2024_min`
- Source document: `西安2024.pdf`
- Current coverage: 2024 Xi'an city traffic annual-report chunks

## Query Command

```bash
python /mnt/skills/custom/road-traffic-analysis/scripts/rag_xian2024_min.py \
  query "<question>" \
  --top-k 5
```

The output is a JSON list. Each item includes:

- `section_path`: source section hierarchy
- `pages`: page range in the source report
- `preview`: retrieved evidence text
- `distance` and `rerank_score`: retrieval scores

## When To Use

Use RAG for:

- `2024年西安市交通发展情况`
- `中心城区高峰速度/道路网密度/绿色出行比例等指标`
- `和年度报告指标做对比`
- `生成带依据的交通治理研判报告`
- `解释实时路况背后的城市交通背景`

Do not use RAG for:

- a named road's current congestion status
- today's weather
- route planning
- forecast from uploaded CSV
- anomaly detection from uploaded CSV

## Mixed Workflow Pattern

For governance-style questions:

1. Use realtime tools for current facts:
   - `weather`
   - `road-traffic`
   - `geocode -> around-traffic` when nearby traffic is needed
2. Use RAG for annual-report evidence and benchmark context.
3. Write the answer with separate evidence types:
   - realtime measurements from Baidu API output
   - annual-report context from RAG `section_path` and `pages`
4. Avoid claiming a causal relationship unless both current data and cited context support it.

## Example

User: `今天早高峰西安市朱雀大街是否需要临时疏导？请结合实时路况、天气、周边道路情况和西安年度交通指标给出研判。`

Workflow:

- `weather(location="西安市")`
- `road-traffic(road_name="朱雀大街", city="西安市")`
- `geocode(address="朱雀大街", city="西安市")`
- `around-traffic(center="<geocode.location.center>", radius=500)`
- `rag_xian2024_min.py query "西安市中心城区主干路高峰期平均速度 交通体检指标 2024"`

Reply:

- current condition summary first
- annual-report benchmark/context with `section_path` and `pages`
- risk judgment and action suggestions
