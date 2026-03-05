// 1) Node counts
MATCH (n)
RETURN labels(n)[0] AS label, count(*) AS cnt
ORDER BY cnt DESC;

// 2) Relationship counts
MATCH ()-[r]->()
RETURN type(r) AS rel, count(*) AS cnt
ORDER BY cnt DESC;

// 3) Poet -> Poem
MATCH (p:Poet)-[:WROTE]->(m:Poem)
RETURN p.name AS poet, m.title AS poem
ORDER BY poet;

// 4) Poem -> Era
MATCH (m:Poem)-[:CREATED_IN]->(e:Era)
RETURN m.title AS poem, e.name AS era;

// 5) Poem -> Place -> City
MATCH (m:Poem)-[:MENTIONS_PLACE]->(pl:Place)-[:LOCATED_IN]->(c:City)
RETURN m.title AS poem, pl.name AS place, c.name AS city
ORDER BY city;

// 6) Poem -> Image
MATCH (m:Poem)-[r:USES_IMAGE]->(i:Image)
RETURN m.title AS poem, i.name AS image, r.evidence_text AS evidence;

// 7) Poem -> NarrativeType
MATCH (m:Poem)-[:HAS_NARRATIVE]->(n:NarrativeType)
RETURN m.title AS poem, n.name AS narrative_type;

// 8) Poem -> DiscourseConcept
MATCH (m:Poem)-[:EMBODIES_DISCOURSE]->(d:DiscourseConcept)
RETURN m.title AS poem, d.name AS discourse_concept;

// 9) Poem -> Paper
MATCH (m:Poem)-[:DISCUSSED_IN]->(p:Paper)
RETURN m.title AS poem, p.title AS paper, p.year AS year
ORDER BY year DESC;

// 10) End-to-end path for one poem
MATCH (poet:Poet)-[:WROTE]->(m:Poem)-[:MENTIONS_PLACE]->(pl:Place)-[:LOCATED_IN]->(c:City)
MATCH (m)-[:HAS_NARRATIVE]->(n:NarrativeType)
MATCH (m)-[:EMBODIES_DISCOURSE]->(d:DiscourseConcept)
RETURN poet.name AS poet, m.title AS poem, c.name AS city, n.name AS narrative, d.name AS discourse;

// 11) Historical place normalization
MATCH (pl:Place)-[:NORMALIZED_TO]->(pc:PlaceCanonical)-[:CANON_LOCATED_IN]->(c:City)
RETURN pl.name AS mention_place, pc.name AS canonical_place, pc.period AS period, c.name AS city
ORDER BY canonical_place
LIMIT 50;

// 12) Auditable evidence chain for place extraction
MATCH (m:Poem)-[:HAS_EVIDENCE]->(e:Evidence)-[:SUPPORTS_PLACE]->(pl:Place)
RETURN
  m.title AS poem,
  pl.name AS place,
  e.span_text AS evidence_text,
  e.span_start AS span_start,
  e.span_end AS span_end,
  e.rule_version AS rule_version
LIMIT 50;

// 13) Auditable evidence chain for image extraction
MATCH (m:Poem)-[:HAS_EVIDENCE]->(e:Evidence)-[:SUPPORTS_IMAGE]->(img:Image)
RETURN
  m.title AS poem,
  img.name AS image,
  e.span_text AS evidence_text,
  e.span_start AS span_start,
  e.span_end AS span_end,
  e.extractor AS extractor
LIMIT 50;
