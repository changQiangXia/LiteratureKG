# Annotation Outputs

- `gold_rel_mentions_place_v1.csv`
  - Assistant-annotated labels for all `rel_mentions_place` rows.
  - Includes two standards:
    - `label_literary`: literary research perspective (region/place mentions are valid).
    - `label_strict_geo`: strict geocoding perspective (macro regions marked as 0).

- `gold_rel_mentions_place_v1_summary.txt`
  - Summary counts and top place mentions.

- `gold_rel_uses_image_v1.csv`
  - Assistant-annotated labels for all `rel_uses_image` rows.
  - Includes two standards:
    - `label_literary_image`: literary research perspective (imagery mention is valid).
    - `label_strict_image`: strict disambiguation perspective (ambiguous temporal/customary usages marked as 0).

- `gold_rel_uses_image_v1_summary.txt`
  - Summary counts, ambiguity reasons, and top image mentions.

- `manual_review_place_top50_v1.csv`
  - High-risk-first manual review checklist for place relations.
  - Prioritizes strict negative rows, then fills with top borderline rows.

- `manual_review_image_top50_v1.csv`
  - High-risk-first manual review checklist for image relations.
  - Prioritizes strict negative rows under ambiguous contexts.

- `manual_review_combined_top100_v1.csv`
  - Combined checklist (`50 place + 50 image`) for one-round fast human QA.

- `manual_review_summary_v1.txt`
  - Coverage summary and risk-reason breakdown for the generated checklist.

## Re-run

```powershell
python scripts/annotate_place_gold_v1.py
python scripts/eval_place_precision_from_gold.py
python scripts/annotate_image_gold_v1.py
python scripts/eval_image_precision_from_gold.py
python scripts/build_manual_review_checklist_v1.py
```
