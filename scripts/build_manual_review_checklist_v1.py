from __future__ import annotations

import csv
from collections import Counter
from pathlib import Path


ROOT = Path(r"d:\pythonProjects\LiteratureKG")
ANNOTATION_DIR = ROOT / "data" / "annotation"
INPUT_DIR = ROOT / "data" / "input"

AMBIGUOUS_IMAGE_TOKENS = {"日", "月", "风", "云"}


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, str]], headers: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        w.writerows(rows)


def to_int(value: str, default: int = -1) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def build_context(content: str, start_raw: str, end_raw: str, evidence: str, window: int = 12) -> str:
    start = to_int(start_raw, -1)
    end = to_int(end_raw, -1)
    if 0 <= start <= end <= len(content):
        left = max(0, start - window)
        right = min(len(content), end + window)
        return f"{content[left:start]}【{content[start:end]}】{content[end:right]}"

    if evidence:
        pos = content.find(evidence)
        if pos >= 0:
            left = max(0, pos - window)
            right = min(len(content), pos + len(evidence) + window)
            return f"{content[left:pos]}【{evidence}】{content[pos + len(evidence):right]}"

    return content[: min(24, len(content))]


def place_risk(
    row: dict[str, str],
    place_freq: Counter[str],
) -> tuple[int, list[str]]:
    score = 0
    reasons: list[str] = []

    strict = row.get("label_strict_geo", "")
    strict_note = row.get("label_strict_geo_note", "")
    place_name = row.get("place_name", "")
    place_type = row.get("place_type", "")
    evidence = row.get("evidence_text", "")
    freq = place_freq.get(place_name, 0)

    if strict == "0":
        score += 100
        reasons.append("strict_geo_negative")
    if strict_note:
        score += 20
        reasons.append(f"note:{strict_note}")
    if place_type == "region":
        score += 12
        reasons.append("region_type")
    if len(place_name) <= 2:
        score += 8
        reasons.append("short_name")
    if len(evidence) <= 1:
        score += 6
        reasons.append("single_char_evidence")
    if freq >= 10:
        score += 16
        reasons.append(f"high_freq:{freq}")
    elif freq >= 5:
        score += 10
        reasons.append(f"mid_freq:{freq}")
    elif freq >= 3:
        score += 6
        reasons.append(f"repeat_freq:{freq}")

    return score, reasons


def image_risk(
    row: dict[str, str],
    image_freq: Counter[str],
) -> tuple[int, list[str]]:
    score = 0
    reasons: list[str] = []

    strict = row.get("label_strict_image", "")
    strict_note = row.get("label_strict_image_note", "")
    image_name = row.get("image_name", "")
    image_category = row.get("image_category", "")
    evidence = row.get("evidence_text", "")
    freq = image_freq.get(image_name, 0)

    if strict == "0":
        score += 100
        reasons.append("strict_image_negative")
    if strict_note.startswith("ambiguous_context:"):
        score += 24
        reasons.append(strict_note)
    if image_name in AMBIGUOUS_IMAGE_TOKENS:
        score += 16
        reasons.append("ambiguous_token")
    if image_category in {"weather", "astral"}:
        score += 8
        reasons.append("semantic_ambiguous_category")
    if len(evidence) <= 1:
        score += 5
        reasons.append("single_char_evidence")
    if freq >= 300:
        score += 18
        reasons.append(f"very_high_freq:{freq}")
    elif freq >= 150:
        score += 12
        reasons.append(f"high_freq:{freq}")
    elif freq >= 80:
        score += 8
        reasons.append(f"mid_freq:{freq}")

    return score, reasons


def select_top(
    rows: list[dict[str, str]],
    target_n: int,
    strict_key: str,
    score_key: str,
) -> list[dict[str, str]]:
    negatives = [r for r in rows if r.get(strict_key) == "0"]
    positives = [r for r in rows if r.get(strict_key) == "1"]
    negatives_sorted = sorted(
        negatives,
        key=lambda r: (
            -to_int(r.get(score_key, "0"), 0),
            r.get("risk_reasons", ""),
            r.get("annotation_id", ""),
        ),
    )
    positives_sorted = sorted(
        positives,
        key=lambda r: (
            -to_int(r.get(score_key, "0"), 0),
            r.get("risk_reasons", ""),
            r.get("annotation_id", ""),
        ),
    )

    out = negatives_sorted[:target_n]
    if len(out) < target_n:
        out.extend(positives_sorted[: target_n - len(out)])
    return out


def build_place_tasks(
    place_rows: list[dict[str, str]],
    poem_by_id: dict[str, dict[str, str]],
    top_n: int = 50,
) -> list[dict[str, str]]:
    place_freq = Counter(r.get("place_name", "") for r in place_rows)
    enriched: list[dict[str, str]] = []

    for r in place_rows:
        score, reasons = place_risk(r, place_freq)
        poem = poem_by_id.get(r.get("poem_id", ""), {})
        context = build_context(
            content=poem.get("content", ""),
            start_raw=r.get("match_span_start", ""),
            end_raw=r.get("match_span_end", ""),
            evidence=r.get("evidence_text", ""),
        )

        enriched.append(
            {
                "annotation_id": r.get("annotation_id", ""),
                "domain": "place",
                "poem_id": r.get("poem_id", ""),
                "poem_title": r.get("poem_title", ""),
                "target_id": r.get("place_id", ""),
                "target_name": r.get("place_name", ""),
                "target_type": r.get("place_type", ""),
                "evidence_text": r.get("evidence_text", ""),
                "context_excerpt": context,
                "current_literary_label": r.get("label_literary", ""),
                "current_strict_label": r.get("label_strict_geo", ""),
                "current_strict_note": r.get("label_strict_geo_note", ""),
                "risk_score": str(score),
                "risk_reasons": "|".join(reasons),
                "source_file": r.get("source_file", ""),
                "source_record_id": r.get("source_record_id", ""),
            }
        )

    top = select_top(enriched, top_n, strict_key="current_strict_label", score_key="risk_score")
    for i, r in enumerate(top, start=1):
        r["review_task_id"] = f"review_place_{i:03d}"
        r["priority_rank"] = str(i)
        r["review_status"] = "todo"
        r["review_decision"] = ""
        r["review_comment"] = ""
    return top


def build_image_tasks(
    image_rows: list[dict[str, str]],
    poem_by_id: dict[str, dict[str, str]],
    top_n: int = 50,
) -> list[dict[str, str]]:
    image_freq = Counter(r.get("image_name", "") for r in image_rows)
    enriched: list[dict[str, str]] = []

    for r in image_rows:
        score, reasons = image_risk(r, image_freq)
        poem = poem_by_id.get(r.get("poem_id", ""), {})
        context = build_context(
            content=poem.get("content", ""),
            start_raw=r.get("match_span_start", ""),
            end_raw=r.get("match_span_end", ""),
            evidence=r.get("evidence_text", ""),
        )

        enriched.append(
            {
                "annotation_id": r.get("annotation_id", ""),
                "domain": "image",
                "poem_id": r.get("poem_id", ""),
                "poem_title": r.get("poem_title", ""),
                "target_id": r.get("image_id", ""),
                "target_name": r.get("image_name", ""),
                "target_type": r.get("image_category", ""),
                "evidence_text": r.get("evidence_text", ""),
                "context_excerpt": context,
                "current_literary_label": r.get("label_literary_image", ""),
                "current_strict_label": r.get("label_strict_image", ""),
                "current_strict_note": r.get("label_strict_image_note", ""),
                "risk_score": str(score),
                "risk_reasons": "|".join(reasons),
                "source_file": r.get("source_file", ""),
                "source_record_id": r.get("source_record_id", ""),
            }
        )

    top = select_top(enriched, top_n, strict_key="current_strict_label", score_key="risk_score")
    for i, r in enumerate(top, start=1):
        r["review_task_id"] = f"review_image_{i:03d}"
        r["priority_rank"] = str(i)
        r["review_status"] = "todo"
        r["review_decision"] = ""
        r["review_comment"] = ""
    return top


def write_summary(path: Path, place_top: list[dict[str, str]], image_top: list[dict[str, str]]) -> None:
    place_neg = sum(1 for r in place_top if r.get("current_strict_label") == "0")
    image_neg = sum(1 for r in image_top if r.get("current_strict_label") == "0")
    place_reason = Counter()
    image_reason = Counter()
    for r in place_top:
        for item in r.get("risk_reasons", "").split("|"):
            if item:
                place_reason[item] += 1
    for r in image_top:
        for item in r.get("risk_reasons", "").split("|"):
            if item:
                image_reason[item] += 1

    lines = [
        "manual_review_checklist_v1",
        f"place_tasks={len(place_top)}",
        f"place_strict_negative_included={place_neg}",
        f"image_tasks={len(image_top)}",
        f"image_strict_negative_included={image_neg}",
        "top_place_risk_reasons=",
    ]
    for k, v in place_reason.most_common(12):
        lines.append(f"  {k}: {v}")
    lines.append("top_image_risk_reasons=")
    for k, v in image_reason.most_common(12):
        lines.append(f"  {k}: {v}")

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    poems = read_csv(INPUT_DIR / "poems.csv")
    place_gold = read_csv(ANNOTATION_DIR / "gold_rel_mentions_place_v1.csv")
    image_gold = read_csv(ANNOTATION_DIR / "gold_rel_uses_image_v1.csv")
    poem_by_id = {r.get("id", ""): r for r in poems}

    place_top = build_place_tasks(place_gold, poem_by_id, top_n=50)
    image_top = build_image_tasks(image_gold, poem_by_id, top_n=50)

    headers = [
        "review_task_id",
        "priority_rank",
        "domain",
        "annotation_id",
        "poem_id",
        "poem_title",
        "target_id",
        "target_name",
        "target_type",
        "evidence_text",
        "context_excerpt",
        "current_literary_label",
        "current_strict_label",
        "current_strict_note",
        "risk_score",
        "risk_reasons",
        "source_file",
        "source_record_id",
        "review_status",
        "review_decision",
        "review_comment",
    ]

    place_out = ANNOTATION_DIR / "manual_review_place_top50_v1.csv"
    image_out = ANNOTATION_DIR / "manual_review_image_top50_v1.csv"
    combined_out = ANNOTATION_DIR / "manual_review_combined_top100_v1.csv"
    summary_out = ANNOTATION_DIR / "manual_review_summary_v1.txt"

    write_csv(place_out, place_top, headers)
    write_csv(image_out, image_top, headers)
    write_csv(combined_out, place_top + image_top, headers)
    write_summary(summary_out, place_top, image_top)

    print(f"saved: {place_out}")
    print(f"saved: {image_out}")
    print(f"saved: {combined_out}")
    print(f"saved: {summary_out}")
    print(
        "counts:",
        f"place={len(place_top)}",
        f"image={len(image_top)}",
        f"combined={len(place_top) + len(image_top)}",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
