from __future__ import annotations

import csv
import datetime as dt
from collections import Counter
from pathlib import Path


AMBIGUOUS_REGION_TERMS = {
    "江南",
    "江北",
    "中原",
    "关中",
    "关东",
    "塞北",
    "塞外",
    "燕赵",
    "吴越",
    "两京",
    "巴陵",
    "梁园",
    "广陵",
    "洛阳城",
    "长安道",
}

INVALID_PLACE_TERMS = {
    # Keep empty unless we confirm clear extraction errors.
}


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, str]], headers: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        w.writerows(rows)


def main() -> int:
    root = Path(r"d:\pythonProjects\LiteratureKG")
    input_dir = root / "data" / "input"
    out_dir = root / "data" / "annotation"

    poems = read_csv(input_dir / "poems.csv")
    places = read_csv(input_dir / "places.csv")
    rel_mentions = read_csv(input_dir / "rel_mentions_place.csv")

    poem_by_id = {r["id"]: r for r in poems}
    place_by_id = {r["id"]: r for r in places}

    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows: list[dict[str, str]] = []

    strict_pos = 0
    strict_neg = 0
    literary_pos = 0
    literary_neg = 0
    span_bad = 0
    source_counter = Counter()
    place_counter = Counter()

    for i, rel in enumerate(rel_mentions, start=1):
        poem = poem_by_id.get(rel["poem_id"], {})
        place = place_by_id.get(rel["place_id"], {})

        place_name = place.get("name", "")
        evidence_text = rel.get("evidence_text", "")
        poem_content = poem.get("content", "")
        source_counter[rel.get("source", "unknown")] += 1
        place_counter[place_name] += 1

        span_start = rel.get("match_span_start", "")
        span_end = rel.get("match_span_end", "")

        span_valid = True
        if span_start.isdigit() and span_end.isdigit():
            s = int(span_start)
            e = int(span_end)
            if not (0 <= s <= e <= len(poem_content)):
                span_valid = False
            else:
                if poem_content[s:e] != evidence_text:
                    span_valid = False
        else:
            if evidence_text and evidence_text not in poem_content:
                span_valid = False

        if not span_valid:
            span_bad += 1

        # literary label: whether this mention is meaningful for文学空间研究
        if place_name in INVALID_PLACE_TERMS or not span_valid:
            literary_label = "0"
            literary_note = "invalid_extraction"
            literary_neg += 1
        else:
            literary_label = "1"
            literary_note = "valid_literary_place_mention"
            literary_pos += 1

        # strict_geo label: whether this mention is suitable for strict geocoding
        if place_name in INVALID_PLACE_TERMS or not span_valid:
            strict_label = "0"
            strict_note = "invalid_extraction"
            strict_neg += 1
        elif place_name in AMBIGUOUS_REGION_TERMS:
            strict_label = "0"
            strict_note = "macro_or_cultural_region"
            strict_neg += 1
        else:
            strict_label = "1"
            strict_note = "specific_historical_toponym"
            strict_pos += 1

        rows.append(
            {
                "annotation_id": f"ann_place_{i:04d}",
                "poem_id": rel.get("poem_id", ""),
                "poem_title": poem.get("title", ""),
                "place_id": rel.get("place_id", ""),
                "place_name": place_name,
                "place_type": place.get("type", ""),
                "evidence_text": evidence_text,
                "match_span_start": span_start,
                "match_span_end": span_end,
                "source": rel.get("source", ""),
                "model_confidence": rel.get("confidence", ""),
                "rule_version": rel.get("rule_version", ""),
                "extractor": rel.get("extractor", ""),
                "source_file": rel.get("source_file", ""),
                "source_record_id": rel.get("source_record_id", ""),
                "label_literary": literary_label,
                "label_literary_note": literary_note,
                "label_strict_geo": strict_label,
                "label_strict_geo_note": strict_note,
                "annotator": "codex_assistant",
                "annotation_version": "gold_place_v1",
                "annotated_at": now,
            }
        )

    out_csv = out_dir / "gold_rel_mentions_place_v1.csv"
    write_csv(
        out_csv,
        rows,
        [
            "annotation_id",
            "poem_id",
            "poem_title",
            "place_id",
            "place_name",
            "place_type",
            "evidence_text",
            "match_span_start",
            "match_span_end",
            "source",
            "model_confidence",
            "rule_version",
            "extractor",
            "source_file",
            "source_record_id",
            "label_literary",
            "label_literary_note",
            "label_strict_geo",
            "label_strict_geo_note",
            "annotator",
            "annotation_version",
            "annotated_at",
        ],
    )

    summary_path = out_dir / "gold_rel_mentions_place_v1_summary.txt"
    summary_lines = [
        f"rows={len(rows)}",
        f"span_bad={span_bad}",
        f"literary_positive={literary_pos}",
        f"literary_negative={literary_neg}",
        f"strict_geo_positive={strict_pos}",
        f"strict_geo_negative={strict_neg}",
        f"source_breakdown={dict(source_counter)}",
        "top_places=",
    ]
    for name, c in place_counter.most_common(20):
        summary_lines.append(f"  {name}: {c}")
    summary_path.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    print(f"saved: {out_csv}")
    print(f"saved: {summary_path}")
    print(f"rows={len(rows)} strict_geo_pos={strict_pos} strict_geo_neg={strict_neg}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
