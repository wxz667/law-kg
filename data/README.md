# Data Layout

## Current structure

```text
data/
  raw/
    constitution/
    law/
    regulation/
    interpretation/
  normalized/
    documents/
    manifests/
  intermediate/
    01_ingest/
    02_segment/
    03_link/
  exports/
    neo4j/
    json/
  archive/
    legacy_pipeline/
```

## Conventions

- `raw/` stores original source files grouped by document category.
- Top-level raw categories are currently `constitution / law / regulation / interpretation`.
- Amendments and decisions are normalized as document subtypes during `ingest`, not separate top-level structure targets.
- CLI batch builds always discover files from `data/raw/`; use `--category` to limit traversal to a subdirectory.
- CLI single-file builds resolve `--source` relative to `data/raw/` unless an absolute path is given.
- `normalized/documents/` is reserved for future normalized document payloads.
- `intermediate/01_ingest/` stores flattened `SourceDocumentRecord` files named `<scope>.source_document.json`.
- `intermediate/02_segment/` stores only aggregated `graph.bundle-*.json` chunks and `graph.index.json`.
- `intermediate/03_link/` stores only aggregated `graph.bundle-*.json` chunks and `graph.index.json`.
- `exports/json/` stores the latest public aggregated delivery graph chunks and graph index, written by the pipeline finish step.
- `exports/neo4j/` is reserved for graph export artifacts.
- `data/.cache/kg-build/` stores internal manifests and per-document cache artifacts for incremental rebuilds.
- `archive/legacy_pipeline/` preserves old TreeKG / semantic-stage outputs.

## Current canonical sample

- [中华人民共和国刑法.docx](/home/zephyr/law-kg/data/raw/law/中华人民共和国刑法.docx)
