#!/usr/bin/env python3
"""
Lance un master et plusieurs workers sur les machines TP via SSH.

Exemple d'utilisation (depuis la racine du dépôt sur une machine TP) :

    uv run python scripts/run_cluster.py \
        --nodes-file config/nodes.txt \
        --workers 3 \
        --splits 3 \
        --remote-path ~/hadoop_homemade

Le script :
- lit la liste de machines dans nodes.txt (ou utilise les options fournies),
- démarre le master via SSH sur la machine maître,
- démarre chaque worker sur les machines cibles (commande `uv run python worker.py`).

Il suppose que le répertoire du projet est partagé via NFS et déjà présent sur
toutes les machines listées (même chemin `remote-path`).
"""

from __future__ import annotations

import argparse
import shlex
import signal
import time
import subprocess
import sys
from pathlib import Path
from typing import List

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from results_summary import format_summary, summarize_output_dir


def read_nodes(file_path: Path) -> List[str]:
    if not file_path.exists():
        raise FileNotFoundError(f"Fichier de nodes introuvable: {file_path}")
    nodes: List[str] = []
    with file_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            stripped = line.strip()
            if stripped:
                nodes.append(stripped)
    if not nodes:
        raise ValueError(f"Aucune machine valide dans {file_path}")
    return nodes


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Lancer master + workers via SSH")
    parser.add_argument(
        "--nodes-file",
        default="config/nodes.txt",
        help="Fichier listant les machines disponibles (defaut: config/nodes.txt)",
    )
    parser.add_argument(
        "--master-node",
        help="Machine sur laquelle lancer le master (defaut: premiere du fichier)",
    )
    parser.add_argument(
        "--worker-nodes",
        nargs="+",
        help="Liste explicite des machines workers (defaut: suivant le master)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=3,
        help="Nombre de workers a lancer (si worker-nodes non fourni)",
    )
    parser.add_argument("--splits", type=int, default=3, help="Nombre de splits a traiter")
    parser.add_argument(
        "--remote-path",
        default="/cal/exterieurs/blepourt-25/hadoop_homemade",
        help="Chemin du projet sur les machines distantes",
    )
    parser.add_argument(
        "--master-port",
        type=int,
        default=5374,
        help="Port du master (defaut: 5374)",
    )
    parser.add_argument(
        "--base-shuffle-port",
        type=int,
        default=6001,
        help="Port de base pour le shuffle des workers (defaut: 6001)",
    )
    parser.add_argument(
        "--data-dir",
        default="/cal/commoncrawl",
        help="Repertoire contenant les fichiers CommonCrawl",
    )
    parser.add_argument(
        "--file-prefix",
        default="CC-MAIN-20230320083513-20230320113513-",
        help="Prefixe des fichiers (defaut: campagne 20230320...)",
    )
    parser.add_argument(
        "--file-suffix",
        default=".warc.wet",
        help="Suffixe des fichiers (defaut: .warc.wet)",
    )
    parser.add_argument(
        "--split-padding",
        type=int,
        default=5,
        help="Zero padding de l'index de split (ex: 00000)",
    )
    parser.add_argument(
        "--frequency-step",
        type=int,
        default=100,
        help="Taille des intervalles de frequence pour la phase de tri",
    )
    parser.add_argument(
        "--uv-bin",
        default=str(Path.home() / ".local/bin/uv"),
        help="Chemin vers l'exécutable uv (defaut: ~/.local/bin/uv)",
    )
    return parser.parse_args()


def build_ssh_command(host: str, remote_path: str, cmd: List[str]) -> List[str]:
    quoted_cmd = " ".join(shlex.quote(part) for part in cmd)
    if remote_path.startswith(("~/", "$")):
        cd_target = remote_path
    else:
        cd_target = shlex.quote(remote_path)
    remote = f"cd {cd_target} && {quoted_cmd}"
    return ["ssh", host, "bash", "-lc", remote]


def launch(cmd: List[str]) -> subprocess.Popen:
    print(" ".join(cmd))
    return subprocess.Popen(cmd)


def main() -> int:
    args = parse_args()
    nodes = read_nodes(Path(args.nodes_file))
    remote_root = args.remote_path.rstrip("/")
    remote_master = f"{remote_root}/master.py"
    remote_worker = f"{remote_root}/worker.py"

    master_node = args.master_node or nodes[0]
    remaining_nodes = [n for n in nodes if n != master_node]

    if args.worker_nodes:
        worker_nodes = args.worker_nodes
    else:
        if len(remaining_nodes) < args.workers:
            raise ValueError("Pas assez de machines dans le fichier pour lancer les workers demandes.")
        worker_nodes = remaining_nodes[: args.workers]

    num_workers = len(worker_nodes)
    print(f"[launcher] Master: {master_node}")
    print(f"[launcher] Workers: {', '.join(worker_nodes)}")

    processes: List[subprocess.Popen] = []
    uv_bin = args.uv_bin

    master_cmd = [
        uv_bin,
        "run",
        "python",
        remote_master,
        "--host",
        master_node,
        "--port",
        str(args.master_port),
        "--workers",
        str(num_workers),
        "--splits",
        str(args.splits),
        "--frequency-step",
        str(args.frequency_step),
    ]
    master_ssh = build_ssh_command(master_node, remote_root, master_cmd)
    try:
        print("[launcher] Démarrage du master...")
        processes.append(launch(master_ssh))
        time.sleep(1.0)

        for idx, node in enumerate(worker_nodes):
            shuffle_port = args.base_shuffle_port + idx
            worker_cmd = [
                uv_bin,
                "run",
                "python",
                remote_worker,
                "--worker-id",
                node,
                "--master-host",
                master_node,
                "--master-port",
                str(args.master_port),
                "--shuffle-host",
                node,
                "--shuffle-port",
                str(shuffle_port),
                "--data-dir",
                args.data_dir,
                "--file-prefix",
                args.file_prefix,
                "--file-suffix",
                args.file_suffix,
                "--split-padding",
                str(args.split_padding),
                "--output-dir",
                str(Path(remote_root) / "output"),
            ]
            worker_ssh = build_ssh_command(node, remote_root, worker_cmd)
            print(f"[launcher] Démarrage du worker sur {node} (shuffle {shuffle_port})...")
            processes.append(launch(worker_ssh))
            time.sleep(0.5)

        if processes:
            processes[0].wait()
            for proc in processes[1:]:
                proc.wait()
        summary = summarize_output_dir(Path(remote_root) / "output")
        if summary is None:
            print(f"[launcher] Aucun résultat trouvé dans {remote_root}/output")
        else:
            print(format_summary(summary))
    except KeyboardInterrupt:
        print("[launcher] Interruption reçue, arrêt des processus...")
    finally:
        for proc in processes:
            if proc.poll() is None:
                proc.send_signal(signal.SIGINT)
        time.sleep(1.0)
        for proc in processes:
            if proc.poll() is None:
                proc.terminate()
        for proc in processes:
            if proc.poll() is None:
                proc.kill()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
