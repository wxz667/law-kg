CREATE CONSTRAINT lawkg_node_id IF NOT EXISTS
FOR (n:LawKG)
REQUIRE n.id IS UNIQUE;

MATCH (n:LawKG)
DETACH DELETE n;

CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///neo4j/nodes.csv' AS row RETURN row",
  "
  MERGE (n:LawKG {id: row.id})
  SET n.type = row.type,
      n.name = CASE row.name WHEN '' THEN null ELSE row.name END,
      n.level = CASE row.level WHEN '' THEN null ELSE row.level END,
      n.source_id = CASE row.source_id WHEN '' THEN null ELSE row.source_id END,
      n.text = CASE row.text WHEN '' THEN null ELSE row.text END,
      n.summary = CASE row.summary WHEN '' THEN null ELSE row.summary END,
      n.description = CASE row.description WHEN '' THEN null ELSE row.description END,
      n.embedding_ref = CASE row.embedding_ref WHEN '' THEN null ELSE row.embedding_ref END,
      n.address = CASE row.address WHEN '' THEN null ELSE row.address END,
      n.display_label = CASE row.display_label WHEN '' THEN row.id ELSE row.display_label END,
      n.metadata_json = CASE row.metadata_json WHEN '' THEN null ELSE row.metadata_json END
  WITH n, row
  CALL apoc.create.addLabels(n, [row.type]) YIELD node
  RETURN count(*)
  ",
  {batchSize: 1000, parallel: false}
);

CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///neo4j/edges.csv' AS row RETURN row",
  "
  MATCH (source:LawKG {id: row.source})
  MATCH (target:LawKG {id: row.target})
  CALL apoc.merge.relationship(
    source,
    row.type,
    {id: row.id},
    {
      type: row.type,
      weight: CASE row.weight WHEN '' THEN null ELSE toFloat(row.weight) END,
      evidence_text: CASE row.evidence_text WHEN '' THEN null ELSE row.evidence_text END,
      evidence_json: CASE row.evidence_json WHEN '' THEN null ELSE row.evidence_json END,
      metadata_json: CASE row.metadata_json WHEN '' THEN null ELSE row.metadata_json END
    },
    target
  ) YIELD rel
  RETURN count(*)
  ",
  {batchSize: 1000, parallel: false}
);

CREATE INDEX lawkg_type IF NOT EXISTS
FOR (n:LawKG)
ON (n.type);

CREATE INDEX lawkg_name IF NOT EXISTS
FOR (n:LawKG)
ON (n.name);
