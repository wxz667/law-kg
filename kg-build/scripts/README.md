# Scripts

This directory contains downstream operational helpers that should not
pollute the core `kg_build` package.

Operational entrypoints are exposed through the package CLI:

```bash
python -m kg_build.cli build --source <docx> --data-root <data-dir>
```

Neo4j import helpers live here as downstream adapters:

```bash
./kg-build/scripts/import_into_neo4j.sh \
  ./data/intermediate/04_extract/graph.bundle.json
```

This script will:

- export `graph.bundle.json` into `data/graph/neo4j/nodes.csv` and `edges.csv`
- start the `neo4j` service from `compose.yaml`
- run the Cypher import script inside the container

After import, open:

```text
http://localhost:7474
```
