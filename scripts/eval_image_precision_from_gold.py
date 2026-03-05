from __future__ import annotations

import csv
from pathlib import Path


def main() -> int:
    gold_path = Path(r"d:\pythonProjects\LiteratureKG\data\annotation\gold_rel_uses_image_v1.csv")
    if not gold_path.exists():
        raise FileNotFoundError(f"Gold file not found: {gold_path}")

    with gold_path.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))

    total = len(rows)
    if total == 0:
        print("No rows.")
        return 0

    literary_pos = sum(1 for r in rows if r.get("label_literary_image") == "1")
    strict_pos = sum(1 for r in rows if r.get("label_strict_image") == "1")

    literary_precision = literary_pos / total
    strict_precision = strict_pos / total

    print(f"rows={total}")
    print(f"literary_image_precision={literary_precision:.4f} ({literary_pos}/{total})")
    print(f"strict_image_precision={strict_precision:.4f} ({strict_pos}/{total})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
