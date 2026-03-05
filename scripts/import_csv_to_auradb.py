from __future__ import annotations

import argparse
import csv
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from neo4j import GraphDatabase


@dataclass(frozen=True)
class NodeConfig:
    filename: str
    label: str
    id_col: str
    prop_cols: tuple[str, ...]


@dataclass(frozen=True)
class RelationConfig:
    filename: str
    start_label: str
    start_col: str
    rel_type: str
    end_label: str
    end_col: str
    prop_cols: tuple[str, ...]


NODE_CONFIGS: tuple[NodeConfig, ...] = (
    NodeConfig("eras.csv", "Era", "id", ("name", "start_year", "end_year")),
    NodeConfig(
        "poets.csv",
        "Poet",
        "id",
        ("name", "era", "birth_year", "death_year", "notes"),
    ),
    NodeConfig("poems.csv", "Poem", "id", ("title", "content", "genre", "source")),
    NodeConfig("cities.csv", "City", "id", ("name", "modern_location")),
    NodeConfig("places.csv", "Place", "id", ("name", "type", "notes")),
    NodeConfig(
        "canonical_places.csv",
        "PlaceCanonical",
        "id",
        ("name", "period", "geo_id", "type", "notes"),
    ),
    NodeConfig(
        "evidences.csv",
        "Evidence",
        "id",
        (
            "evidence_type",
            "span_text",
            "span_start",
            "span_end",
            "rule_version",
            "extractor",
            "source_file",
            "source_record_id",
            "confidence",
        ),
    ),
    NodeConfig("images.csv", "Image", "id", ("name", "category")),
    NodeConfig(
        "narrative_types.csv", "NarrativeType", "id", ("name", "description")
    ),
    NodeConfig(
        "discourse_concepts.csv", "DiscourseConcept", "id", ("name", "description")
    ),
    NodeConfig("papers.csv", "Paper", "id", ("title", "year", "authors", "journal")),
)


REL_CONFIGS: tuple[RelationConfig, ...] = (
    RelationConfig(
        "rel_wrote.csv",
        "Poet",
        "poet_id",
        "WROTE",
        "Poem",
        "poem_id",
        ("source", "confidence"),
    ),
    RelationConfig(
        "rel_created_in.csv",
        "Poem",
        "poem_id",
        "CREATED_IN",
        "Era",
        "era_id",
        ("source", "confidence"),
    ),
    RelationConfig(
        "rel_mentions_place.csv",
        "Poem",
        "poem_id",
        "MENTIONS_PLACE",
        "Place",
        "place_id",
        (
            "evidence_text",
            "source",
            "confidence",
            "evidence_id",
            "rule_version",
            "extractor",
            "source_file",
            "source_record_id",
            "match_span_start",
            "match_span_end",
        ),
    ),
    RelationConfig(
        "rel_uses_image.csv",
        "Poem",
        "poem_id",
        "USES_IMAGE",
        "Image",
        "image_id",
        (
            "evidence_text",
            "source",
            "confidence",
            "evidence_id",
            "rule_version",
            "extractor",
            "source_file",
            "source_record_id",
            "match_span_start",
            "match_span_end",
        ),
    ),
    RelationConfig(
        "rel_has_narrative.csv",
        "Poem",
        "poem_id",
        "HAS_NARRATIVE",
        "NarrativeType",
        "narrative_type_id",
        ("source", "confidence"),
    ),
    RelationConfig(
        "rel_embodies_discourse.csv",
        "Poem",
        "poem_id",
        "EMBODIES_DISCOURSE",
        "DiscourseConcept",
        "concept_id",
        ("evidence_text", "source", "confidence"),
    ),
    RelationConfig(
        "rel_discussed_in.csv",
        "Poem",
        "poem_id",
        "DISCUSSED_IN",
        "Paper",
        "paper_id",
        ("source", "confidence"),
    ),
    RelationConfig(
        "rel_located_in.csv",
        "Place",
        "place_id",
        "LOCATED_IN",
        "City",
        "city_id",
        ("source", "confidence"),
    ),
    RelationConfig(
        "rel_normalized_to.csv",
        "Place",
        "place_id",
        "NORMALIZED_TO",
        "PlaceCanonical",
        "canonical_place_id",
        ("method", "source", "confidence"),
    ),
    RelationConfig(
        "rel_canon_located_in.csv",
        "PlaceCanonical",
        "canonical_place_id",
        "CANON_LOCATED_IN",
        "City",
        "city_id",
        ("source", "confidence"),
    ),
    RelationConfig(
        "rel_has_evidence.csv",
        "Poem",
        "poem_id",
        "HAS_EVIDENCE",
        "Evidence",
        "evidence_id",
        ("source",),
    ),
    RelationConfig(
        "rel_evidence_supports_place.csv",
        "Evidence",
        "evidence_id",
        "SUPPORTS_PLACE",
        "Place",
        "place_id",
        ("source",),
    ),
    RelationConfig(
        "rel_evidence_supports_image.csv",
        "Evidence",
        "evidence_id",
        "SUPPORTS_IMAGE",
        "Image",
        "image_id",
        ("source",),
    ),
)


def get_env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise ValueError(f"Missing required env var: {name}")
    return value


def coerce_value(raw: str) -> Any:
    value = raw.strip()
    if value == "":
        return None

    low = value.lower()
    if low == "true":
        return True
    if low == "false":
        return False

    if re.fullmatch(r"-?\d+", value):
        return int(value)
    if re.fullmatch(r"-?\d+\.\d+", value):
        return float(value)

    return value


def read_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            return []
        return [dict(row) for row in reader]


def validate_headers(
    filename: str, rows: list[dict[str, str]], required_cols: tuple[str, ...]
) -> None:
    if not rows:
        return
    headers = set(rows[0].keys())
    missing = [col for col in required_cols if col not in headers]
    if missing:
        raise ValueError(f"{filename}: missing columns {missing}")


def create_constraints(session: Any) -> None:
    labels = [
        "Era",
        "Poet",
        "Poem",
        "City",
        "Place",
        "PlaceCanonical",
        "Evidence",
        "Image",
        "NarrativeType",
        "DiscourseConcept",
        "Paper",
    ]
    for label in labels:
        constraint_name = f"{label.lower()}_id_unique"
        query = (
            f"CREATE CONSTRAINT {constraint_name} IF NOT EXISTS "
            f"FOR (n:{label}) REQUIRE n.id IS UNIQUE"
        )
        session.run(query).consume()


def chunked(items: list[dict[str, Any]], batch_size: int) -> list[list[dict[str, Any]]]:
    if batch_size <= 0:
        batch_size = 500
    return [items[i : i + batch_size] for i in range(0, len(items), batch_size)]


def import_nodes(session: Any, data_dir: Path, batch_size: int) -> tuple[int, int]:
    imported = 0
    skipped = 0
    for cfg in NODE_CONFIGS:
        path = data_dir / cfg.filename
        rows = read_rows(path)
        validate_headers(cfg.filename, rows, (cfg.id_col, *cfg.prop_cols))
        if not rows:
            print(f"[WARN] Skip {cfg.filename}: file missing or empty.")
            continue

        query = (
            f"UNWIND $rows AS row "
            f"MERGE (n:{cfg.label} {{id: row.id}}) "
            f"SET n += row.props "
            f"RETURN count(*) AS c"
        )
        local_imported = 0
        local_skipped = 0
        payload: list[dict[str, Any]] = []
        for row in rows:
            node_id = (row.get(cfg.id_col) or "").strip()
            if node_id == "":
                local_skipped += 1
                continue

            props: dict[str, Any] = {}
            for col in cfg.prop_cols:
                raw = row.get(col, "")
                if raw is None:
                    continue
                value = coerce_value(raw)
                if value is not None:
                    props[col] = value

            payload.append({"id": node_id, "props": props})

        batches = chunked(payload, batch_size)
        for batch in batches:
            rec = session.run(query, rows=batch).single()
            local_imported += int(rec["c"]) if rec else 0

        imported += local_imported
        skipped += local_skipped
        print(
            f"[NODE] {cfg.filename:<24} imported={local_imported:<4} "
            f"skipped={local_skipped} batches={len(batches)}"
        )
    return imported, skipped


def import_relationships(session: Any, data_dir: Path, batch_size: int) -> tuple[int, int]:
    imported = 0
    skipped = 0
    for cfg in REL_CONFIGS:
        path = data_dir / cfg.filename
        rows = read_rows(path)
        validate_headers(cfg.filename, rows, (cfg.start_col, cfg.end_col, *cfg.prop_cols))
        if not rows:
            print(f"[WARN] Skip {cfg.filename}: file missing or empty.")
            continue

        query = (
            f"UNWIND $rows AS row "
            f"MATCH (a:{cfg.start_label} {{id: row.start_id}}) "
            f"MATCH (b:{cfg.end_label} {{id: row.end_id}}) "
            f"MERGE (a)-[r:{cfg.rel_type}]->(b) "
            f"SET r += row.props "
            f"RETURN count(r) AS c"
        )

        local_imported = 0
        local_skipped = 0
        payload: list[dict[str, Any]] = []
        for row in rows:
            start_id = (row.get(cfg.start_col) or "").strip()
            end_id = (row.get(cfg.end_col) or "").strip()
            if start_id == "" or end_id == "":
                local_skipped += 1
                continue

            props: dict[str, Any] = {}
            for col in cfg.prop_cols:
                raw = row.get(col, "")
                if raw is None:
                    continue
                value = coerce_value(raw)
                if value is not None:
                    props[col] = value

            payload.append({"start_id": start_id, "end_id": end_id, "props": props})

        batches = chunked(payload, batch_size)
        for batch in batches:
            result = session.run(query, rows=batch).single()
            created = int(result["c"]) if result else 0
            local_imported += created
            local_skipped += max(0, len(batch) - created)

        imported += local_imported
        skipped += local_skipped
        print(
            f"[REL ] {cfg.filename:<24} imported={local_imported:<4} "
            f"skipped={local_skipped} batches={len(batches)}"
        )
    return imported, skipped


def print_graph_summary(session: Any) -> None:
    print("\nGraph summary:")
    node_rows = session.run(
        "MATCH (n) RETURN labels(n)[0] AS label, count(*) AS cnt ORDER BY cnt DESC, label"
    ).data()
    rel_rows = session.run(
        "MATCH ()-[r]->() RETURN type(r) AS rel, count(*) AS cnt ORDER BY cnt DESC, rel"
    ).data()

    if not node_rows:
        print("  (no nodes)")
    else:
        for row in node_rows:
            print(f"  NODE {row['label']:<16} {row['cnt']}")

    if not rel_rows:
        print("  (no relationships)")
    else:
        for row in rel_rows:
            print(f"  REL  {row['rel']:<16} {row['cnt']}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import LiteratureKG CSV files into Neo4j AuraDB."
    )
    parser.add_argument(
        "--data-dir",
        default="data/input",
        help="Directory that contains CSV files (default: data/input).",
    )
    parser.add_argument(
        "--database",
        default=None,
        help="Neo4j database name. If omitted, use NEO4J_DATABASE from .env.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Batch size for UNWIND import (default: 500).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_dotenv()

    uri = get_env("NEO4J_URI")
    username = get_env("NEO4J_USERNAME")
    password = get_env("NEO4J_PASSWORD")
    database = args.database or get_env("NEO4J_DATABASE")

    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        raise FileNotFoundError(f"Data directory not found: {data_dir}")

    print(f"Using URI: {uri}")
    print(f"Using Username: {username}")
    print(f"Using Database: {database}")
    print(f"Using Data Dir: {data_dir.resolve()}")

    driver = GraphDatabase.driver(uri, auth=(username, password))
    try:
        driver.verify_connectivity()
        with driver.session(database=database) as session:
            create_constraints(session)
            node_imported, node_skipped = import_nodes(
                session, data_dir, batch_size=args.batch_size
            )
            rel_imported, rel_skipped = import_relationships(
                session, data_dir, batch_size=args.batch_size
            )
            print("\nImport done.")
            print(f"Nodes: imported={node_imported}, skipped={node_skipped}")
            print(f"Rels : imported={rel_imported}, skipped={rel_skipped}")
            print_graph_summary(session)
    finally:
        driver.close()

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        raise SystemExit(1)
