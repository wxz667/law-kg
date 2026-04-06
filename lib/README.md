# Infrastructure

This directory contains infrastructure adapters and optional downstream assets.

LLM-related infrastructure lives under `infra/llm/`.
Its responsibility is limited to explicit vendor adaptation:

- normalize each configured vendor into a shared client interface
- read provider connection settings from `.env`
- handle authentication, transport, retries, and provider response parsing
- avoid any coupling with Tree-KG stages, prompts, or graph contracts

Provider names should be explicit vendor or platform identifiers such as
`bigmodel`, `deepseek`, or `openrouter`, not broad compatibility labels.

Current provider-specific requirements:

- `bigmodel` uses the `zai-sdk` package
- `deepseek` uses the `openai` package with DeepSeek's OpenAI-compatible API

Core task semantics such as `summarize`, `extract`, and `pred` remain in
the corresponding stage modules under `kg-build/src/kg_build/stages/`.

Neo4j-related downstream assets remain under:

- `neo4j/data/`: runtime database volume
- `neo4j/logs/`: runtime logs
- `neo4j/plugins/`: optional plugins
