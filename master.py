# serveur.py
import argparse
import json
import math
import signal
import socket
import struct
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Tuple, Optional


"""
Ce fichier implémente le rôle de master MapReduce.

Le master :
- écoute les connexions des workers (protocole TCP longueur + JSON),
- enregistre les workers (id, adresse, port de shuffle),
- leur envoie la liste complète des workers et leurs splits à traiter,
- ne transporte jamais les données applicatives (aucun mot, aucune paire clé/valeur),
- attend les notifications de fin de travail.

Le shuffle des données se fait directement entre workers.
"""


HOST = "0.0.0.0"
PORT = 5374
OUTPUT_DIR = Path(__file__).resolve().parent / "output"

running_event = threading.Event()
running_event.set()


def handle_stop(signum, frame):
    if running_event.is_set():
        print("Signal recu, arret propre du master...")
    running_event.clear()


signal.signal(signal.SIGINT, handle_stop)
signal.signal(signal.SIGTERM, handle_stop)


def recv_all(conn: socket.socket, expected: int) -> bytes | None:
    data = b""
    while len(data) < expected:
        try:
            chunk = conn.recv(expected - len(data))
        except socket.timeout:
            if not running_event.is_set():
                return None
            continue
        if not chunk:
            return None
        data += chunk
    return data


def recv_msg(conn: socket.socket) -> dict | None:
    header = recv_all(conn, 4)
    if header is None:
        return None
    size = struct.unpack(">I", header)[0]
    payload = recv_all(conn, size)
    if payload is None:
        return None
    try:
        return json.loads(payload.decode("utf-8"))
    except json.JSONDecodeError:
        return None


def send_msg(conn: socket.socket, message: dict) -> None:
    payload = json.dumps(message).encode("utf-8")
    header = struct.pack(">I", len(payload))
    conn.sendall(header + payload)


@dataclass
class WorkerState:
    worker_id: str
    host: str
    shuffle_port: int
    conn: socket.socket
    wordcount_done: bool = False
    sort_done: bool = False
    interval: Optional[Tuple[int, int]] = None
    min_freq: int = 0
    max_freq: int = 0


workers: Dict[str, WorkerState] = {}
workers_lock = threading.Lock()
all_registered = threading.Event()
all_done = threading.Event()
wordcount_done_event = threading.Event()
job_start_time: float | None = None


def handle_worker(conn: socket.socket, addr, expected_workers: int) -> None:
    conn.settimeout(1.0)
    worker_id: str | None = None
    try:
        while running_event.is_set():
            msg = recv_msg(conn)
            if msg is None:
                break
            mtype = msg.get("type")

            if mtype == "REGISTER":
                worker_id = msg.get("worker_id")
                shuffle_port = msg.get("shuffle_port")
                if not isinstance(worker_id, str) or not isinstance(shuffle_port, int):
                    continue

                host = msg.get("host")
                if not isinstance(host, str):
                    host = addr[0]

                with workers_lock:
                    if worker_id in workers:
                        continue
                    workers[worker_id] = WorkerState(
                        worker_id=worker_id,
                        host=host,
                        shuffle_port=shuffle_port,
                        conn=conn,
                    )
                    print(f"Worker enregistre: {worker_id} ({host}:{shuffle_port})")
                    if len(workers) >= expected_workers:
                        all_registered.set()

            elif mtype == "WORDCOUNT_DONE":
                wid = msg.get("worker_id")
                if not isinstance(wid, str):
                    continue
                with workers_lock:
                    w = workers.get(wid)
                    if w:
                        w.wordcount_done = True
                        w.min_freq = int(msg.get("min_freq", 0))
                        w.max_freq = int(msg.get("max_freq", 0))
                        print(f"Worker {wid} a terminé le wordcount.")
                    if workers and all(w.wordcount_done for w in workers.values()):
                        print("Tous les wordcounts sont terminés.")
                        wordcount_done_event.set()

            elif mtype == "SORT_DONE":
                wid = msg.get("worker_id")
                if not isinstance(wid, str):
                    continue
                with workers_lock:
                    w = workers.get(wid)
                    if w:
                        w.sort_done = True
                        print(f"Worker {wid} a terminé le tri.")
                    if workers and all(w.sort_done for w in workers.values()):
                        all_done.set()

            elif mtype == "SHUTDOWN":
                running_event.clear()
                break

    finally:
        if worker_id:
            with workers_lock:
                if worker_id in workers:
                    print(f"Deconnexion du worker {worker_id}")
        conn.close()


def distribute_splits(num_splits: int, worker_ids: List[str]) -> Dict[str, List[int]]:
    assignments: Dict[str, List[int]] = {wid: [] for wid in worker_ids}
    if not worker_ids or num_splits <= 0:
        return assignments

    wid_count = len(worker_ids)
    for idx in range(num_splits):
        wid = worker_ids[idx % wid_count]
        assignments[wid].append(idx)
    return assignments


def run_master(host: str, port: int, expected_workers: int, num_splits: int, freq_step: int) -> None:
    print(
        f"Master en attente de {expected_workers} workers sur {host}:{port} "
        f"(nombre de splits = {num_splits})"
    )

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((host, port))
        server_socket.listen()
        server_socket.settimeout(1.0)

        accept_threads: list[threading.Thread] = []

        try:
            while running_event.is_set() and not all_registered.is_set():
                try:
                    conn, addr = server_socket.accept()
                except socket.timeout:
                    continue
                print("Connexion worker depuis", addr)
                t = threading.Thread(
                    target=handle_worker,
                    args=(conn, addr, expected_workers),
                    daemon=True,
                )
                t.start()
                accept_threads.append(t)

            if not all_registered.is_set():
                print("Arret du master avant enregistrement de tous les workers.")
                return

            with workers_lock:
                worker_ids = list(workers.keys())
                worker_list = [
                    {
                        "worker_id": w.worker_id,
                        "host": w.host,
                        "shuffle_port": w.shuffle_port,
                    }
                    for w in workers.values()
                ]

            assignments = distribute_splits(num_splits, worker_ids)
            print("Distribution des splits (round-robin) :")
            for wid, splits in assignments.items():
                print(f"  {wid}: {splits}")

            global job_start_time
            job_start_time = time.perf_counter()
            all_done.clear()
            wordcount_done_event.clear()

            def assign_sort_intervals() -> None:
                with workers_lock:
                    if not workers:
                        return
                    min_freq = min(w.min_freq for w in workers.values())
                    max_freq = max(w.max_freq for w in workers.values())
                    if max_freq < min_freq:
                        max_freq = min_freq
                    total_range = max_freq - min_freq + 1
                    if freq_step and freq_step > 0:
                        step = freq_step
                    else:
                        step = max(1, math.ceil(total_range / len(worker_ids)))
                    current_min = min_freq
                    for idx, wid in enumerate(worker_ids):
                        high = current_min + step - 1
                        if idx == len(worker_ids) - 1:
                            high = max_freq
                        workers[wid].interval = (current_min, high)
                        current_min = high + 1

            def notify_sort_phase() -> None:
                with workers_lock:
                    for wid in worker_ids:
                        w = workers[wid]
                        interval = w.interval or (0, 0)
                        send_msg(
                            w.conn,
                            {
                                "type": "START_SORT",
                                "interval": {"min": interval[0], "max": interval[1]},
                                "intervals": [
                                    {
                                        "worker_id": wid2,
                                        "min": workers[wid2].interval[0]
                                        if workers[wid2].interval
                                        else 0,
                                        "max": workers[wid2].interval[1]
                                        if workers[wid2].interval
                                        else 0,
                                    }
                                    for wid2 in worker_ids
                                ],
                                "all_workers": worker_list,
                            },
                        )

            for wid in worker_ids:
                w = workers[wid]
                msg = {
                    "type": "START_WORDCOUNT",
                    "worker_id": wid,
                    "all_workers": worker_list,
                    "splits": assignments[wid],
                }
                send_msg(w.conn, msg)

            print("Phase 1 (wordcount) lancee. Attente de la phase 2 (tri repartie)...")
            wordcount_done_event.wait()
            assign_sort_intervals()
            notify_sort_phase()
            print("Phase 2 (tri) lancee...")
            all_done.wait()
            duration = None
            if job_start_time is not None:
                duration = time.perf_counter() - job_start_time
                print(
                    f"Phases Map+Shuffle+Reduce terminees en {duration:.2f} s "
                    f"(workers={len(worker_ids)}, splits={num_splits})"
                )
                write_master_metrics(duration, len(worker_ids), num_splits, OUTPUT_DIR)
            print("Tous les workers ont signale la fin du job.")

        finally:
            running_event.clear()
            for w in list(workers.values()):
                try:
                    send_msg(w.conn, {"type": "SHUTDOWN"})
                except OSError:
                    pass
            print("Arret du master.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Master MapReduce (serveur)")
    parser.add_argument("--host", default=HOST, help="Adresse d'ecoute (defaut: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=PORT, help="Port d'ecoute (defaut: 5374)")
    parser.add_argument(
        "--workers",
        type=int,
        required=True,
        help="Nombre de workers attendus",
    )
    parser.add_argument(
        "--splits",
        type=int,
        required=True,
        help="Nombre total de splits logiques a distribuer",
    )
    parser.add_argument(
        "--frequency-step",
        type=int,
        default=0,
        help="Pas fixe pour les intervalles (0 = repartition automatique)",
    )
    parser.add_argument(
        "--output-dir",
        default=str(OUTPUT_DIR),
        help="Repertoire de sortie pour les resultats du master (defaut: ./output)",
    )
    return parser.parse_args()


def write_master_metrics(duration: float, workers: int, splits: int, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "duration_seconds": duration,
        "workers": workers,
        "splits": splits,
        "timestamp": time.time(),
    }
    out_path = output_dir / "master_metrics.json"
    with out_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    args = parse_args()
    OUTPUT_DIR = Path(args.output_dir)
    run_master(args.host, args.port, args.workers, args.splits, args.frequency_step)
