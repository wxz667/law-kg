# Data layout

- `raw/statutes/`: canonical statute input files in `.docx`
- `raw/judicial-interpretations/`: reserved for future legal interpretation sources
- `raw/cases/`: reserved for future case sources
- `raw/annotations/`: reserved for future manual labels
- `intermediate/`: stage-by-stage JSON artifacts written by the build pipeline
- `graph/`: canonical graph serialization outputs
- `manifest/`: build manifests and stage execution records

The canonical raw input for this project is:

- [中华人民共和国刑法.docx](/home/zephyr/law-kg/data/raw/statutes/中华人民共和国刑法.docx)

The canonical graph output is:

- `data/graph/graph.bundle.json`
