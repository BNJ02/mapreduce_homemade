#!/usr/bin/env python3
"""
Script local : copie les fichiers nécessaires sur une machine TP et lance plusieurs
runs de run_cluster.py avec des nombres de workers différents. Chaque run est ajouté
à un fichier de benchmark (JSON lines).
"""

from __future__ import annotations

import argparse
import json
import shlex
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List

SSH_BASE_OPTS: List[str] = []
SCP_BASE_OPTS: List[str] = []
CONTROL_PATH: Path | None = None


def remote_join(remote_root: str, path: str) -> str:
    rel = Path(path)
    if rel.is_absolute():
        return rel.as_posix()
    return f"{remote_root}/{rel.as_posix()}"


def configure_ssh_options(remote_host: str) -> None:
    global CONTROL_PATH
    safe_host = remote_host.replace("@", "_").replace(":", "_")
    CONTROL_PATH = Path.home() / f".ssh/benchmark-{safe_host}.sock"
    base_opts = [
        "-4",
        "-o",
        "ConnectTimeout=10",
        "-o",
        "ConnectionAttempts=3",
        "-o",
        "ControlMaster=auto",
        "-o",
        f"ControlPath={CONTROL_PATH}",
        "-o",
        "ControlPersist=300",
    ]
    SSH_BASE_OPTS.clear()
    SSH_BASE_OPTS.extend(base_opts)
    SCP_BASE_OPTS.clear()
    SCP_BASE_OPTS.extend(base_opts)


def close_master_connection(remote_host: str) -> None:
    if CONTROL_PATH is None:
        return
    try:
        run_cmd(
            [
                "ssh",
                "-S",
                str(CONTROL_PATH),
                remote_host,
                "-O",
                "exit",
            ],
            retries=0,
        )
    except subprocess.CalledProcessError:
        pass


def run_cmd(
    cmd: List[str],
    capture: bool = False,
    retries: int = 0,
    retry_delay: float = 2.0,
) -> subprocess.CompletedProcess:
    attempts = retries + 1
    last_exc: subprocess.CalledProcessError | None = None
    for attempt in range(1, attempts + 1):
        try:
            if capture:
                return subprocess.run(
                    cmd,
                    check=True,
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
            subprocess.run(cmd, check=True)
            return subprocess.CompletedProcess(cmd, 0)
        except subprocess.CalledProcessError as exc:
            last_exc = exc
            if attempt >= attempts:
                break
            print(f"[benchmark] Commande {' '.join(cmd)} a echoue ({exc}), nouvel essai dans {retry_delay}s...")
            time.sleep(retry_delay)
    assert last_exc is not None
    raise last_exc


def ssh_cmd(host: str, remote_path: str, command: str, capture: bool = False) -> subprocess.CompletedProcess:
    remote = f"cd {shlex.quote(remote_path)} && {command}"
    full_cmd = ["ssh", *SSH_BASE_OPTS, host, "bash", "-lc", remote]
    return run_cmd(full_cmd, capture=capture, retries=3)


def use_shell(path: str) -> bool:
    return any(ch in path for ch in ("$", "~")) or " " in path


def ensure_remote_dir(remote_host: str, path: str) -> None:
    if use_shell(path):
        run_cmd(
            ["ssh", *SSH_BASE_OPTS, remote_host, "bash", "-lc", f"mkdir -p {shlex.quote(path)}"],
            retries=3,
        )
    else:
        run_cmd(["ssh", *SSH_BASE_OPTS, remote_host, "mkdir", "-p", path], retries=3)


def remove_remote_path(remote_host: str, path: str) -> None:
    if use_shell(path):
        run_cmd(
            ["ssh", *SSH_BASE_OPTS, remote_host, "bash", "-lc", f"rm -rf {shlex.quote(path)}"],
            retries=3,
        )
    else:
        run_cmd(["ssh", *SSH_BASE_OPTS, remote_host, "rm", "-rf", path], retries=3)


def scp_paths(paths: Iterable[Path], base_dir: Path, remote_host: str, remote_path: str) -> None:
    for path in paths:
        rel = path.relative_to(base_dir)
        remote_parent = remote_path
        if rel.parent != Path("."):
            remote_parent = f"{remote_path}/{rel.parent.as_posix()}"
            ensure_remote_dir(remote_host, remote_parent)
        target_path = f"{remote_path}/{rel.as_posix()}"
        remove_remote_path(remote_host, target_path)
        target = f"{remote_host}:{remote_path}/{rel.as_posix()}"
        cmd = ["scp", *SCP_BASE_OPTS, "-r", str(path), target]
        run_cmd(cmd, retries=3)


def append_benchmark_row(file_path: Path, row: dict) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def fetch_remote_output(
    remote_host: str,
    remote_output: str,
    local_base: Path,
    label: str,
) -> Path:
    destination = local_base / label
    if destination.exists():
        shutil.rmtree(destination)
    destination.mkdir(parents=True, exist_ok=True)
    rsync_cmd = [
        "rsync",
        "-az",
        "--delete",
        "-e",
        f"ssh {' '.join(SSH_BASE_OPTS)}",
        f"{remote_host}:{remote_output.rstrip('/')}/",
        f"{destination}/",
    ]
    run_cmd(rsync_cmd, retries=3)
    return destination


def print_compact_log(stdout: str, stderr: str) -> None:
    lines = []
    for line in stdout.splitlines():
        if "[launcher]" in line:
            lines.append(line)
    if not lines:
        lines = stdout.splitlines()[-20:]
    print("\n".join(lines))
    if stderr.strip():
        print("--- stderr ---")
        print(stderr.strip())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark MapReduce sur cluster TP")
    parser.add_argument("--remote-host", required=True, help="Machine de référence (ex: blepourt-25@tp-1a226-10)")
    parser.add_argument(
        "--remote-path",
        default="/cal/exterieurs/blepourt-25/hadoop_homemade",
        help="Répertoire distant pour le projet",
    )
    parser.add_argument(
        "--remote-uv-bin",
        default="/cal/exterieurs/blepourt-25/.local/bin/uv",
        help="Chemin distant vers uv",
    )
    parser.add_argument(
        "--paths",
        nargs="+",
        default=["master.py", "worker.py", "config", "scripts/run_cluster.py", "scripts/results_summary.py"],
        help="Fichiers/dossiers à copier via scp",
    )
    parser.add_argument("--splits", type=int, required=True, help="Nombre de splits pour chaque run")
    parser.add_argument(
        "--worker-counts",
        nargs="+",
        type=int,
        required=True,
        help="Liste des valeurs de workers à tester",
    )
    parser.add_argument(
        "--benchmark-file",
        default="benchmarks/results.jsonl",
        help="Fichier local où ajouter les résultats (JSON Lines)",
    )
    parser.add_argument(
        "--nodes-file",
        default="config/nodes.txt",
        help="Chemin (distant) vers nodes.txt utilisé par run_cluster",
    )
    parser.add_argument(
        "--export-dir",
        default="benchmarks/exports",
        help="Dossier local où rapatrier les fichiers output/ de chaque run",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    configure_ssh_options(args.remote_host)
    repo_root = Path(__file__).resolve().parents[1]
    remote_path = args.remote_path.rstrip("/")
    remote_output_base = f"{remote_path}/output"
    remote_nodes_file = remote_join(remote_path, args.nodes_file)
    remote_run_cluster = remote_join(remote_path, "scripts/run_cluster.py")
    remote_results_summary = remote_join(remote_path, "scripts/results_summary.py")
    benchmark_file = repo_root / args.benchmark_file
    export_root = repo_root / args.export_dir

    # Préparer la machine distante
    ensure_remote_dir(args.remote_host, remote_path)
    ensure_remote_dir(args.remote_host, f"{remote_path}/output")

    paths_to_copy = [repo_root / Path(p) for p in args.paths]
    scp_paths(paths_to_copy, repo_root, args.remote_host, remote_path)

    for workers in args.worker_counts:
        print(f"[benchmark] Run avec {workers} workers, {args.splits} splits")
        remote_output_dir = f"{remote_output_base}/workers{workers}_splits{args.splits}"
        purge_cmd = f"rm -rf {shlex.quote(remote_output_dir)} && mkdir -p {shlex.quote(remote_output_dir)}"
        ssh_cmd(args.remote_host, remote_path, purge_cmd)

        run_cmd_for_cluster = (
            f"{shlex.quote(args.remote_uv_bin)} run python {shlex.quote(remote_run_cluster)} "
            f"--nodes-file {shlex.quote(remote_nodes_file)} "
            f"--workers {workers} "
            f"--splits {args.splits} "
            f"--remote-path {shlex.quote(remote_path)} "
            f"--uv-bin {shlex.quote(args.remote_uv_bin)} "
            f"--data-dir /cal/commoncrawl "
            f"--file-prefix CC-MAIN-20230320083513-20230320113513- "
            f"--file-suffix .warc.wet "
            f"--split-padding 5 "
            f"--output-dir {shlex.quote(remote_output_dir)}"
        )
        try:
            run_proc = ssh_cmd(args.remote_host, remote_path, run_cmd_for_cluster, capture=True)
        except subprocess.CalledProcessError as exc:
            stdout = exc.stdout or ""
            stderr = exc.stderr or ""
            if stdout:
                print("[benchmark] run_cluster stdout:")
                print(stdout)
            if stderr:
                print("[benchmark] run_cluster stderr:", file=sys.stderr)
                print(stderr, file=sys.stderr)
            raise
        print_compact_log(run_proc.stdout, run_proc.stderr)

        summary_cmd = (
            f"python3 {shlex.quote(remote_results_summary)} "
            f"--output-dir {shlex.quote(remote_output_dir)} --json"
        )
        summary_proc = ssh_cmd(
            args.remote_host,
            remote_path,
            summary_cmd,
            capture=True,
        )
        summary = json.loads(summary_proc.stdout.strip())
        metrics = summary.get("master_metrics") or {}

        row = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "workers": workers,
            "splits": args.splits,
            "duration_seconds": metrics.get("duration_seconds"),
            "total_words": summary.get("total_words"),
            "unique_words": summary.get("unique_words"),
            "top_words": summary.get("top_words"),
            "per_worker": summary.get("per_worker"),
            "output_dir": summary.get("output_dir"),
        }
        append_benchmark_row(benchmark_file, row)
        print(f"[benchmark] Résultats ajoutés à {benchmark_file}")

        label = f"run_workers{workers}_splits{args.splits}"
        exported_path = fetch_remote_output(
            args.remote_host,
            remote_output_dir,
            export_root,
            label,
        )
        print(f"[benchmark] Fichiers output rapatriés dans {exported_path}")

    close_master_connection(args.remote_host)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
