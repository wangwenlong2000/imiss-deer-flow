---
name: law-regulations-rag
description: Use this skill when the user wants to preprocess law/regulation datasets, build or rebuild BM25/vector indexes, retrieve legal evidence through dual-path retrieval from built indexes, or answer grounded law/regulation questions from the indexed corpus.
metadata:
  short-description: Preprocess, build indexes, run dual-path retrieval, and answer grounded law/regulation questions with a strict script-driven workflow.
---

# Law Regulations RAG Skill

This skill is a strict, script-driven workflow for preprocessing law/regulation datasets, building BM25/vector indexes, retrieving legal evidence through dual-path retrieval from built indexes, and answering grounded law/regulation questions from the indexed corpus.

Use it when the user asks to:
- 构建索引 / 重建索引 / 更新索引
- 预处理政策法规数据
- 同时构建 BM25 和向量索引
- 为法规数据集建立可检索索引
- 检索相关法条 / 查询法规依据 / 查找法律条文
- 回答基于当前法规语料的简单法律问答
- 根据用户问题，从现有法规索引中查找支持证据

Do not use it for:
- 开放域法律咨询且没有对应语料或索引支撑的场景
- 需要联网获取最新法律法规内容的场景
- 与当前法规语料库无关的一般性闲聊
- 代替真实律师提供确定性法律结论且没有检索证据支撑的场景

## Hard rules

These rules are mandatory for this skill:
- Always use this skill workflow for law/regulation corpus preprocessing, indexing, and retrieval
- Always run the provided scripts through `bash`
- Never replace this workflow with ad hoc Python or one-off code
- Never read the full corpus or full index files into model context with `read_file`
- Never skip preprocessing if the dataset has not yet been converted into the processed format required by the indexers
- Never silently switch to unrelated directories
- If the input dataset or index path cannot be resolved cleanly, stop and report the exact reason
- If the user asks a law/regulation question grounded in the indexed corpus, prefer retrieval over rebuilding indexes
- Retrieval must always use dual-path retrieval over both BM25 and vector indexes
- Never switch to BM25-only or vector-only retrieval unless the skill files themselves have been explicitly changed to support that workflow
- If retrieval evidence is insufficient, explicitly say so instead of fabricating an answer
- Retrieval must use the built indexes; do not answer as if evidence was retrieved when no retrieval command was actually run

## Input resolution order

Resolve input in this exact order:
1. Uploaded files under `/mnt/user-data/uploads`
2. Local datasets under:
   - `/mnt/datasets/law-regulations/raw`
   - `/mnt/datasets/law-regulations/processed`
   - `/mnt/datasets/law-regulations/index`

Rules:
- If the user uploaded new dataset files in the current thread, prefer uploaded files
- If there is no uploaded match, use the local dataset under `/mnt/datasets/law-regulations/...`
- If both uploaded and local files match, prefer uploaded files unless the user explicitly asks for the local dataset

## Dataset convention

Expected directories:

- Raw data:
  - `/mnt/datasets/law-regulations/raw/corpus.jsonl`
  - `/mnt/datasets/law-regulations/raw/queries.json`

- Processed data:
  - `/mnt/datasets/law-regulations/processed/corpus_clean.jsonl`
  - `/mnt/datasets/law-regulations/processed/queries_clean.jsonl`
  - `/mnt/datasets/law-regulations/processed/rag_train.jsonl`
  - `/mnt/datasets/law-regulations/processed/qrels.tsv`

- Lexicon:
  - `/mnt/datasets/law-regulations/lexicon/thuocl_law_user_dict.txt`

- Index output:
  - `/mnt/datasets/law-regulations/index/law_bm25.pkl`
  - `/mnt/datasets/law-regulations/index/law_vector.index`
  - `/mnt/datasets/law-regulations/index/law_vector_metadata.pkl`

## Allowed tools and assets

Use only these assets for execution:
- `read_file` for:
  - this `SKILL.md`
  - small reference/config files
  - short previews when strictly necessary
- `bash` for:
  - `/mnt/skills/custom/law-regulations-rag/scripts/preprocess.py`
  - `/mnt/skills/custom/law-regulations-rag/scripts/build_index.py`
  - `/mnt/skills/custom/law-regulations-rag/scripts/retrieve.py`

Do not use other scripts unless the user explicitly changes the workflow.

## Workflow

### A. Preprocess raw dataset

Run preprocessing when:
- only raw files exist
- the user explicitly asks to preprocess / clean / standardize the dataset
- processed files are missing

Command:
```bash
cd /mnt/skills/custom/law-regulations-rag/scripts && \
python preprocess.py \
  --corpus-path /mnt/datasets/law-regulations/raw/corpus.jsonl \
  --queries-path /mnt/datasets/law-regulations/raw/queries.json \
  --out-dir /mnt/datasets/law-regulations/processed
```

### B. Build or rebuild indexes
Run index building when:
the user explicitly asks to build / rebuild / refresh indexes
the required index files are missing
the processed corpus has changed and the existing indexes are stale

Default build command for both BM25 and vector indexes:
```bash
cd /mnt/skills/custom/law-regulations-rag/scripts && \
python build_index.py \
  --corpus-path /mnt/datasets/law-regulations/processed/corpus_clean.jsonl \
  --queries-path /mnt/datasets/law-regulations/processed/queries_clean.jsonl \
  --output-dir /mnt/datasets/law-regulations/index \
  --mode both \
  --user-dict-path /mnt/datasets/law-regulations/lexicon/thuocl_law_user_dict.txt \
  --model-config /mnt/skills/custom/law-regulations-rag/config/models.yaml \
  --embedding-config-name law-embedding
```
Force rebuild command:
```bash
cd /mnt/skills/custom/law-regulations-rag/scripts && \
python build_index.py \
  --corpus-path /mnt/datasets/law-regulations/processed/corpus_clean.jsonl \
  --queries-path /mnt/datasets/law-regulations/processed/queries_clean.jsonl \
  --output-dir /mnt/datasets/law-regulations/index \
  --mode both \
  --force-rebuild \
  --user-dict-path /mnt/datasets/law-regulations/lexicon/thuocl_law_user_dict.txt \
  --model-config /mnt/skills/custom/law-regulations-rag/config/models.yaml \
  --embedding-config-name law-embedding
```

### C. Retrieve legal evidence from built indexes
Run retrieval when:
- the user asks a law/regulation question that should be answered from the indexed corpus
- the user asks to find relevant legal articles, regulations, or supporting evidence
- the user asks for grounded legal QA based on the existing corpus
- the user wants to know which legal provision supports a conclusion

- If the user asks a legal question that should be answered from the indexed corpus, run retrieve.py instead of rebuilding indexes
- If indexes are missing when retrieval is requested, build indexes first, then run retrieve.py
- If processed files are missing but raw files exist, preprocess first, then build indexes, then run retrieve.py

Command:
```bash
cd /mnt/skills/custom/law-regulations-rag/scripts && \
python retrieve.py \
  --query "<USER_QUERY>" \
  --index-dir /mnt/datasets/law-regulations/index \
  --top-k 5 \
  --user-dict-path /mnt/datasets/law-regulations/lexicon/thuocl_law_user_dict.txt \
  --model-config /mnt/skills/custom/law-regulations-rag/config/models.yaml \
  --embedding-config-name law-embedding
```

Notes:
- retrieve.py loads the built index files on each run; this is expected and acceptable for this skill
- Retrieval should not trigger a rebuild unless the required index files are missing
- If the required index files are missing, build both indexes first, then run retrieval
- If vector retrieval dependencies or API credentials are unavailable, report the exact reason because dual-path retrieval cannot be completed
- Since this skill only allows dual-path retrieval, a failure on either retrieval path should be treated as a retrieval failure for the whole workflow unless the user explicitly changes the workflow


## Decision rules
- If raw files exist but processed files do not exist, preprocess first
- If the user asks to preprocess data, run preprocess.py
- If the user asks to build indexes, run build_index.py
- If the user asks to rebuild or refresh indexes, run build_index.py with --force-rebuild
- If the user only asks for “构建索引” without specifying type, default to --mode both
- If the user asks a legal question that should be answered from the indexed corpus, run retrieve.py with --mode both instead of rebuilding indexes
- If indexes are missing when retrieval is requested, build indexes first, then run retrieve.py with --mode both
- If processed files are missing but raw files exist, preprocess first, then build indexes, then run retrieve.py with --mode both
- If the user asks for “同时构建索引” without specifying type, default to --mode both
- If neither raw files, processed files, nor indexes can be resolved, stop and report the exact missing paths
- If retrieval returns weak or empty results, do not fabricate legal support; say that the current indexed corpus did not return sufficient evidence
- Do not perform runtime routing between BM25 and vector retrieval; routing is fixed to dual-path retrieval only


## Output discipline
Always state:
- which dataset path was used
- whether the source came from uploads or local datasets
- whether preprocessing was run
- whether an existing index was reused or a new one was built
- which index mode was used: both
- where the index files were written

For retrieval runs, always state:
- which index path was used
- that dual-path retrieval over BM25 and vector indexes was used
- the top returned legal evidences
- whether the answer is directly supported by the retrieved evidence
- if evidence is insufficient, explicitly say so instead of fabricating

## Response style for grounded legal QA
When answering a law/regulation question based on retrieved evidence:
- First answer the question briefly and directly
- Then present the most relevant retrieved legal evidence
- Cite the regulation title and quote or summarize only the necessary supporting content
- Make it clear that the answer is grounded in the indexed corpus, not in external legal research
- Make it clear that the evidence came from dual-path retrieval over both BM25 and vector indexes
- If the evidence only partially supports the answer, explicitly say the support is partial
- If the corpus lacks enough support, say that the current indexed corpus does not provide sufficient basis for a complete answer

## Failure handling

Stop and report the exact reason if:
- raw dataset files cannot be found
- processed dataset files are required but missing
- index files are required but missing
- the user dictionary path is required but missing
- dual-path retrieval is requested but either BM25 or vector index files are missing
- vector retrieval is required by the dual-path workflow but the embedding API key is missing
- model configuration is invalid or unreadable
- any script returns a clear path/config/runtime error

Never hide these failures behind generic language.



