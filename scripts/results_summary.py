#!/usr/bin/env python3
"""
Utilities to summarize MapReduce outputs (wordcount + timings).
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List


def summarize_output_dir(output_dir: Path) -> dict | None:
    if not output_dir.exists():
        return None

    worker_files = sorted(output_dir.glob("*_wordcount.json"))
    if not worker_files:
        return None

    aggregated = Counter()
    per_worker: List[Dict[str, Any]] = []
    lang_counter: Counter[str] = Counter()
    total_words = 0

    for path in worker_files:
        try:
            with path.open("r", encoding="utf-8") as fh:
                data = json.load(fh)
        except (OSError, json.JSONDecodeError):
            continue

        counts = data.get("counts")
        if isinstance(counts, dict):
            try:
                aggregated.update({k: int(v) for k, v in counts.items()})
            except (TypeError, ValueError):
                pass

        langs = data.get("languages")
        if isinstance(langs, dict):
            try:
                lang_counter.update({k: int(v) for k, v in langs.items()})
            except (TypeError, ValueError):
                pass

        worker_total = int(data.get("total_words", 0))
        worker_unique = int(data.get("unique_words", 0))
        total_words += worker_total
        per_worker.append(
            {
                "file": path.name,
                "total_words": worker_total,
                "unique_words": worker_unique,
            }
        )

    metrics_path = output_dir / "master_metrics.json"
    master_metrics = None
    if metrics_path.exists():
        try:
            with metrics_path.open("r", encoding="utf-8") as fh:
                master_metrics = json.load(fh)
        except (OSError, json.JSONDecodeError):
            master_metrics = None

    return {
        "total_words": total_words,
        "unique_words": len(aggregated),
        "top_words": [{"word": w, "count": c} for w, c in aggregated.most_common(20)],
        "per_worker": per_worker,
        "master_metrics": master_metrics,
        "languages": [{"lang": lang, "count": count} for lang, count in lang_counter.most_common()],
        "output_dir": str(output_dir),
    }


def format_summary(summary: dict) -> str:
    lines: List[str] = []
    lines.append("[launcher] Statistiques globales:")
    lines.append(f"  - Total mots : {summary['total_words']}")
    lines.append(f"  - Mots uniques : {summary['unique_words']}")
    lines.append("  - Top 20 global :")
    for item in summary["top_words"]:
        lines.append(f"      {item['word']}: {item['count']}")
    if summary.get("languages"):
        lines.append("  - Langues les plus détectées :")
        for entry in summary["languages"][:10]:
            lines.append(f"      {entry['lang']}: {entry['count']}")
    lines.append("  - Détail par worker :")
    for worker in summary["per_worker"]:
        lines.append(
            f"      {worker['file']}: {worker['total_words']} mots, "
            f"{worker['unique_words']} uniques"
        )
    metrics = summary.get("master_metrics")
    if metrics and "duration_seconds" in metrics:
        lines.append(
            f"[launcher] Temps Map+Shuffle+Reduce (depuis le master): "
            f"{metrics['duration_seconds']:.2f} s "
            f"(workers={metrics.get('workers')}, splits={metrics.get('splits')})"
        )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Résumé des sorties MapReduce")
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Répertoire contenant les fichiers *_wordcount.json",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Affiche le résumé au format JSON (une seule ligne)",
    )
    args = parser.parse_args()

    summary = summarize_output_dir(Path(args.output_dir))
    if summary is None:
        print(f"Aucun résultat trouvé dans {args.output_dir}")
        return 1

    if args.json:
        print(json.dumps(summary, ensure_ascii=False))
    else:
        print(format_summary(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
