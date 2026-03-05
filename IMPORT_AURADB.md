# Import CSV Into AuraDB

## 1) Prepare data files

Fill CSV files under `data/input/`:

- `eras.csv`
- `poets.csv`
- `poems.csv`
- `cities.csv`
- `places.csv`
- `canonical_places.csv`
- `evidences.csv`
- `images.csv`
- `narrative_types.csv`
- `discourse_concepts.csv`
- `papers.csv`
- `rel_wrote.csv`
- `rel_created_in.csv`
- `rel_mentions_place.csv`
- `rel_uses_image.csv`
- `rel_has_narrative.csv`
- `rel_embodies_discourse.csv`
- `rel_discussed_in.csv`
- `rel_located_in.csv`
- `rel_normalized_to.csv`
- `rel_canon_located_in.csv`
- `rel_has_evidence.csv`
- `rel_evidence_supports_place.csv`
- `rel_evidence_supports_image.csv`

## 2) Run import

```powershell
conda run -n litkg python scripts/import_csv_to_auradb.py
```

Optional custom data folder:

```powershell
conda run -n litkg python scripts/import_csv_to_auradb.py --data-dir data/input
```

## 3) Check results in Aura Query

```cypher
MATCH (n) RETURN labels(n)[0] AS label, count(*) AS cnt ORDER BY cnt DESC;
```

```cypher
MATCH ()-[r]->() RETURN type(r) AS rel, count(*) AS cnt ORDER BY cnt DESC;
```

## 4) Quick demo queries

Poet -> Poem:

```cypher
MATCH (p:Poet)-[:WROTE]->(m:Poem)
RETURN p.name, m.title
LIMIT 20;
```

Poem -> Place -> City:

```cypher
MATCH (m:Poem)-[:MENTIONS_PLACE]->(pl:Place)-[:LOCATED_IN]->(c:City)
RETURN m.title, pl.name, c.name
LIMIT 20;
```

Poem with narrative and discourse tags:

```cypher
MATCH (m:Poem)-[:HAS_NARRATIVE]->(n:NarrativeType),
      (m)-[:EMBODIES_DISCOURSE]->(d:DiscourseConcept)
RETURN m.title, n.name, d.name
LIMIT 20;
```

Place normalization layer:

```cypher
MATCH (pl:Place)-[:NORMALIZED_TO]->(pc:PlaceCanonical)-[:CANON_LOCATED_IN]->(c:City)
RETURN pl.name AS mention_place, pc.name AS canonical_place, pc.period AS period, c.name AS city
LIMIT 30;
```

Auditable evidence chain:

```cypher
MATCH (m:Poem)-[:HAS_EVIDENCE]->(e:Evidence)-[:SUPPORTS_PLACE]->(pl:Place)
RETURN m.title, pl.name, e.span_text, e.span_start, e.span_end, e.rule_version
LIMIT 30;
```

You can also use prepared file:

- `cypher/demo_queries.cql`
