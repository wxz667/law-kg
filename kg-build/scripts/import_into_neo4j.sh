#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

BUNDLE_PATH="${1:-${REPO_ROOT}/data/intermediate/04_extract/graph.bundle.json}"
EXPORT_DIR="${REPO_ROOT}/data/graph/neo4j"

if [[ ! -f "${BUNDLE_PATH}" ]]; then
  echo "Bundle not found: ${BUNDLE_PATH}" >&2
  exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required to import into Neo4j. Enable Docker Desktop WSL integration first." >&2
  exit 1
fi

if [[ ! -x "${REPO_ROOT}/.venv/bin/python" ]]; then
  echo "Expected virtualenv python at ${REPO_ROOT}/.venv/bin/python" >&2
  exit 1
fi

mkdir -p "${EXPORT_DIR}"

echo "[neo4j] exporting CSV from ${BUNDLE_PATH}"
"${REPO_ROOT}/.venv/bin/python" "${SCRIPT_DIR}/export_neo4j_csv.py" \
  --bundle "${BUNDLE_PATH}" \
  --output-dir "${EXPORT_DIR}"

echo "[neo4j] starting container"
docker compose -f "${REPO_ROOT}/compose.yaml" up -d neo4j

CONTAINER_NAME="${NEO4J_CONTAINER_NAME:-law-kg-neo4j}"
NEO4J_USER="${NEO4J_USER:-neo4j}"
NEO4J_PASSWORD="${NEO4J_PASSWORD:-change_this_password}"

echo "[neo4j] waiting for cypher-shell"
for _ in $(seq 1 60); do
  if docker exec "${CONTAINER_NAME}" cypher-shell -u "${NEO4J_USER}" -p "${NEO4J_PASSWORD}" "RETURN 1;" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

if ! docker exec "${CONTAINER_NAME}" cypher-shell -u "${NEO4J_USER}" -p "${NEO4J_PASSWORD}" "RETURN 1;" >/dev/null 2>&1; then
  echo "Neo4j did not become ready in time." >&2
  exit 1
fi

echo "[neo4j] importing graph"
docker exec -i "${CONTAINER_NAME}" cypher-shell -u "${NEO4J_USER}" -p "${NEO4J_PASSWORD}" \
  < "${SCRIPT_DIR}/import_neo4j.cypher"

HTTP_PORT="${NEO4J_HTTP_PORT:-7474}"
echo "[neo4j] import completed"
echo "[neo4j] open http://localhost:${HTTP_PORT} and log in with ${NEO4J_USER}"
