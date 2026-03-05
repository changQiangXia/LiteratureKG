from __future__ import annotations

import csv
import datetime as dt
from collections import Counter
from pathlib import Path


STRICT_CONTEXT_AMBIGUOUS: dict[str, set[str]] = {
    "日": {"今日", "明日", "何日", "终日", "日日", "来日"},
    "月": {
        "岁月",
        "年月",
        "正月",
        "二月",
        "三月",
        "四月",
        "五月",
        "六月",
        "七月",
        "八月",
        "九月",
        "十月",
        "十一月",
        "十二月",
    },
    "风": {"风流", "风俗", "风教", "风骨", "风雅"},
    "云": {"云云", "云尔", "云何"},
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


def context_around(text: str, start: int, end: int, window: int = 2) -> str:
    left = max(0, start - window)
    right = min(len(text), end + window)
    return text[left:right]


def is_strict_ambiguous(token: str, text: str, start: int, end: int) -> tuple[bool, str]:
    phrases = STRICT_CONTEXT_AMBIGUOUS.get(token, set())
    if not phrases:
        return False, ""
    local = context_around(text, start, end, window=3)
    for p in phrases:
        if p in local:
            return True, p
    return False, ""


def main() -> int:
    root = Path(r"d:\pythonProjects\LiteratureKG")
    input_dir = root / "data" / "input"
    out_dir = root / "data" / "annotation"

    poems = read_csv(input_dir / "poems.csv")
    images = read_csv(input_dir / "images.csv")
    rel_uses = read_csv(input_dir / "rel_uses_image.csv")

    poem_by_id = {r["id"]: r for r in poems}
    image_by_id = {r["id"]: r for r in images}

    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows: list[dict[str, str]] = []

    literary_pos = 0
    literary_neg = 0
    strict_pos = 0
    strict_neg = 0
    span_bad = 0
    source_counter = Counter()
    image_counter = Counter()
    strict_note_counter = Counter()

    for i, rel in enumerate(rel_uses, start=1):
        poem = poem_by_id.get(rel["poem_id"], {})
        image = image_by_id.get(rel["image_id"], {})

        image_name = image.get("name", "")
        image_category = image.get("category", "")
        evidence_text = rel.get("evidence_text", "")
        poem_content = poem.get("content", "")
        source_counter[rel.get("source", "unknown")] += 1
        image_counter[image_name] += 1

        span_start_raw = rel.get("match_span_start", "")
        span_end_raw = rel.get("match_span_end", "")
        span_valid = True
        span_start = -1
        span_end = -1
        if span_start_raw.isdigit() and span_end_raw.isdigit():
            span_start = int(span_start_raw)
            span_end = int(span_end_raw)
            if not (0 <= span_start <= span_end <= len(poem_content)):
                span_valid = False
            else:
                if poem_content[span_start:span_end] != evidence_text:
                    span_valid = False
        else:
            if evidence_text:
                pos = poem_content.find(evidence_text)
                if pos >= 0:
                    span_start = pos
                    span_end = pos + len(evidence_text)
                else:
                    span_valid = False

        if not span_valid:
            span_bad += 1

        if not span_valid:
            literary_label = "0"
            literary_note = "invalid_extraction"
            literary_neg += 1
        else:
            literary_label = "1"
            literary_note = "valid_literary_image_mention"
            literary_pos += 1

        if not span_valid:
            strict_label = "0"
            strict_note = "invalid_extraction"
            strict_neg += 1
        else:
            ambiguous, phrase = is_strict_ambiguous(
                token=image_name, text=poem_content, start=span_start, end=span_end
            )
            if ambiguous:
                strict_label = "0"
                strict_note = f"ambiguous_context:{phrase}"
                strict_neg += 1
                strict_note_counter[strict_note] += 1
            else:
                strict_label = "1"
                strict_note = "specific_image_mention"
                strict_pos += 1

        rows.append(
            {
                "annotation_id": f"ann_image_{i:05d}",
                "poem_id": rel.get("poem_id", ""),
                "poem_title": poem.get("title", ""),
                "image_id": rel.get("image_id", ""),
                "image_name": image_name,
                "image_category": image_category,
                "evidence_text": evidence_text,
                "match_span_start": str(span_start),
                "match_span_end": str(span_end),
                "source": rel.get("source", ""),
                "model_confidence": rel.get("confidence", ""),
                "rule_version": rel.get("rule_version", ""),
                "extractor": rel.get("extractor", ""),
                "source_file": rel.get("source_file", ""),
                "source_record_id": rel.get("source_record_id", ""),
                "label_literary_image": literary_label,
                "label_literary_image_note": literary_note,
                "label_strict_image": strict_label,
                "label_strict_image_note": strict_note,
                "annotator": "codex_assistant",
                "annotation_version": "gold_image_v1",
                "annotated_at": now,
            }
        )

    out_csv = out_dir / "gold_rel_uses_image_v1.csv"
    write_csv(
        out_csv,
        rows,
        [
            "annotation_id",
            "poem_id",
            "poem_title",
            "image_id",
            "image_name",
            "image_category",
            "evidence_text",
            "match_span_start",
            "match_span_end",
            "source",
            "model_confidence",
            "rule_version",
            "extractor",
            "source_file",
            "source_record_id",
            "label_literary_image",
            "label_literary_image_note",
            "label_strict_image",
            "label_strict_image_note",
            "annotator",
            "annotation_version",
            "annotated_at",
        ],
    )

    summary_path = out_dir / "gold_rel_uses_image_v1_summary.txt"
    lines = [
        f"rows={len(rows)}",
        f"span_bad={span_bad}",
        f"literary_image_positive={literary_pos}",
        f"literary_image_negative={literary_neg}",
        f"strict_image_positive={strict_pos}",
        f"strict_image_negative={strict_neg}",
        f"source_breakdown={dict(source_counter)}",
        "strict_negative_reason_breakdown=",
    ]
    for k, v in strict_note_counter.most_common():
        lines.append(f"  {k}: {v}")
    lines.append("top_images=")
    for name, c in image_counter.most_common(20):
        lines.append(f"  {name}: {c}")
    summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"saved: {out_csv}")
    print(f"saved: {summary_path}")
    print(f"rows={len(rows)} strict_image_pos={strict_pos} strict_image_neg={strict_neg}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
