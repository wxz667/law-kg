# kg-build

`kg-build` is the delivery-grade build framework for constructing a knowledge
graph from the `docx` text of the Criminal Law of the People's Republic of
China.

The structural hierarchy is:

- `article`
- `paragraph`
- `item`
- `sub_item`

## Public entrypoint

The only public build command is:

```bash
cd /home/zephyr/law-kg/kg-build
PYTHONPATH=src /home/zephyr/law-kg/.venv/bin/python -m kg_build.cli build \
  --source /home/zephyr/law-kg/data/raw/statutes/中华人民共和国刑法.docx \
  --data-root /home/zephyr/law-kg/data
```

You can also run a partial range:

```bash
cd /home/zephyr/law-kg/kg-build
PYTHONPATH=src /home/zephyr/law-kg/.venv/bin/python -m kg_build.cli build \
  --source /home/zephyr/law-kg/data/raw/statutes/中华人民共和国刑法.docx \
  --data-root /home/zephyr/law-kg/data \
  --start-stage extract \
  --end-stage pred
```

## Output model

The canonical graph artifact is:

- `data/graph/graph.bundle.json`

Embedding outputs are reserved under:

- `data/intermediate/07_embed/embeddings.jsonl`

Intermediate stage artifacts are written under `data/intermediate/`.

## Notes

- `ingest`, `segment`, and `serialize` are implemented as production framework stages.
- `summarize`, `extract`, `aggr`, `conv`, `embed`, `dedup`, and `pred` are preserved as stable TODO interfaces.
- Neo4j is an optional downstream target and is not part of the core build pipeline.
