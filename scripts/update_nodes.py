#!/usr/bin/env python3
"""
Récupère les machines *disponibles* depuis https://tp.telecom-paris.fr/ajax.php
et écrit leurs noms dans config/nodes.txt (un par ligne).

Le JSON retourné a la forme :

    {"data": [["tp-1a201-00", true, 0, 2, 0], ["tp-1a201-01", false, ...], ...]}

Seules les machines dont le deuxième champ vaut true sont conservées.

Utilisation (depuis la racine du dépôt) :

    python scripts/update_nodes.py
ou
    uv run scripts/update_nodes.py
"""

import json
import random
import sys
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlopen


URL = "https://tp.telecom-paris.fr/ajax.php"

# Ordre de priorité des groupes de machines
PRIORITY_PREFIXES = (
    "tp-1d23",
    "tp-3a101",
    "tp-3a107",
    "tp-3b01",
    "tp-1a226",
)


def main() -> int:
    # Téléchargement
    try:
        with urlopen(URL, timeout=10) as resp:  # type: ignore[arg-type]
            charset = resp.headers.get_content_charset() or "utf-8"
            text = resp.read().decode(charset, errors="replace")
    except (URLError, HTTPError) as e:
        print(f"Erreur lors de la récupération de {URL} : {e}", file=sys.stderr)
        return 1

    # Parsing JSON attendu : {"data": [[name, available, ...], ...]}
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as e:
        print(f"Réponse JSON invalide : {e}", file=sys.stderr)
        return 1

    data = payload.get("data")
    if not isinstance(data, list):
        print("Format inattendu : clé 'data' manquante ou non-liste.", file=sys.stderr)
        return 1

    nodes: list[str] = []
    for entry in data:
        # Chaque entrée doit être une liste du type [name, available, ...]
        if not isinstance(entry, list) or len(entry) < 2:
            continue
        name, available = entry[0], entry[1]
        if not isinstance(name, str):
            continue
        if available is True:
            nodes.append(name.strip())

    if not nodes:
        print("Aucune machine disponible trouvée dans la réponse.", file=sys.stderr)
        return 1

    # Réorganisation : d'abord les groupes prioritaires, ensuite les autres en ordre aléatoire
    buckets: dict[str, list[str]] = {p: [] for p in PRIORITY_PREFIXES}
    others: list[str] = []

    for node in nodes:
        matched_prefix = None
        for prefix in PRIORITY_PREFIXES:
            if node.startswith(prefix):
                matched_prefix = prefix
                break
        if matched_prefix is not None:
            buckets[matched_prefix].append(node)
        else:
            others.append(node)

    random.shuffle(others)

    ordered_nodes: list[str] = []
    for prefix in PRIORITY_PREFIXES:
        ordered_nodes.extend(buckets[prefix])
    ordered_nodes.extend(others)

    # Écriture dans config/nodes.txt à la racine du projet
    root = Path(__file__).resolve().parents[1]
    config_dir = root / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    out_path = config_dir / "nodes.txt"
    with out_path.open("w", encoding="utf-8") as f:
        for node in ordered_nodes:
            f.write(f"{node}\n")

    print(f"{len(ordered_nodes)} machines disponibles écrites dans {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
