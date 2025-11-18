# client.py
import argparse
import json
import socket
import struct
import threading
from collections import defaultdict
from typing import Dict, List, Tuple


"""
Ce fichier implémente un worker MapReduce.

Le worker :
- se connecte au master (serveur.py) pour s'enregistrer,
- écoute sur un port de shuffle pour recevoir les paires clé/valeur des autres workers,
- réalise la phase Map sur ses splits, puis le shuffle direct entre workers
  (hash(cle) % nb_workers) et enfin la phase Reduce locale,
- notifie le master une fois son travail terminé.

Les données applicatives (texte, clés/valeurs) ne transitent jamais par le master.
"""


MASTER_PORT = 5374
DEFAULT_SHUFFLE_PORT = 6000


def recv_all(sock: socket.socket, expected: int) -> bytes:
    data = b""
    while len(data) < expected:
        chunk = sock.recv(expected - len(data))
        if not chunk:
            raise ConnectionError(
                f"Connexion fermee (attendu {expected} octets, recu {len(data)})"
            )
        data += chunk
    return data


def recv_msg(sock: socket.socket) -> dict | None:
    try:
        header = recv_all(sock, 4)
    except ConnectionError:
        return None
    size = struct.unpack(">I", header)[0]
    try:
        payload = recv_all(sock, size)
    except ConnectionError:
        return None
    try:
        return json.loads(payload.decode("utf-8"))
    except json.JSONDecodeError:
        return None


def send_msg(sock: socket.socket, message: dict) -> None:
    payload = json.dumps(message).encode("utf-8")
    header = struct.pack(">I", len(payload))
    sock.sendall(header + payload)


class ShuffleState:
    def __init__(self, expected_peers: List[str]) -> None:
        self.lock = threading.Lock()
        self.pending_senders = set(expected_peers)
        self.data: Dict[str, int] = defaultdict(int)
        self.done_event = threading.Event()

    def add_pairs(self, pairs: List[Tuple[str, int]]) -> None:
        with self.lock:
            for key, value in pairs:
                self.data[key] += int(value)

    def mark_sender_done(self, sender_id: str) -> None:
        with self.lock:
            self.pending_senders.discard(sender_id)
            if not self.pending_senders:
                self.done_event.set()


def shuffle_listener(host: str, port: int, shuffle_state: ShuffleState) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((host, port))
        s.listen()
        s.settimeout(1.0)
        print(f"[worker] Shuffle en ecoute sur {host}:{port}")

        while not shuffle_state.done_event.is_set():
            try:
                conn, addr = s.accept()
            except socket.timeout:
                continue
            t = threading.Thread(
                target=handle_shuffle_connection,
                args=(conn, addr, shuffle_state),
                daemon=True,
            )
            t.start()


def handle_shuffle_connection(
    conn: socket.socket, addr, shuffle_state: ShuffleState
) -> None:
    with conn:
        while not shuffle_state.done_event.is_set():
            msg = recv_msg(conn)
            if msg is None:
                break
            if msg.get("type") != "SHUFFLE_DATA":
                continue

            sender = msg.get("from")
            pairs = msg.get("pairs") or []
            done = msg.get("done", False)

            if isinstance(pairs, list):
                normalized_pairs: List[Tuple[str, int]] = []
                for item in pairs:
                    if (
                        isinstance(item, list)
                        and len(item) == 2
                        and isinstance(item[0], str)
                    ):
                        normalized_pairs.append((item[0], int(item[1])))
                shuffle_state.add_pairs(normalized_pairs)

            if done and isinstance(sender, str):
                shuffle_state.mark_sender_done(sender)


def simple_map_for_split(split_id: int) -> Dict[str, int]:
    """
    Map simplifiee pour un split.

    Cette fonction est volontairement generique : elle ne lit pas de fichier.
    Elle simule des cles derivees de l'identifiant du split.
    """
    base = f"split{split_id}"
    keys = [base, base + "_a", base + "_b"]
    counts: Dict[str, int] = defaultdict(int)
    for k in keys:
        counts[k] += 1
    return dict(counts)


def run_map_shuffle_reduce(
    worker_id: str,
    splits: List[int],
    all_workers: List[dict],
    shuffle_host: str,
    shuffle_port: int,
) -> Dict[str, int]:
    worker_ids = [w["worker_id"] for w in all_workers]
    num_workers = len(worker_ids)
    my_index = worker_ids.index(worker_id)

    # Etat du shuffle : on attend les autres workers
    peers = [wid for wid in worker_ids if wid != worker_id]
    shuffle_state = ShuffleState(expected_peers=peers)

    listener_thread = threading.Thread(
        target=shuffle_listener,
        args=(shuffle_host, shuffle_port, shuffle_state),
        daemon=True,
    )
    listener_thread.start()

    aggregated_local: Dict[str, int] = defaultdict(int)
    outgoing: Dict[str, Dict[str, int]] = {
        wid: defaultdict(int) for wid in worker_ids if wid != worker_id
    }

    for split_id in splits:
        local_counts = simple_map_for_split(split_id)
        for key, value in local_counts.items():
            target_index = hash(key) % num_workers
            target_id = worker_ids[target_index]
            if target_id == worker_id:
                aggregated_local[key] += value
            else:
                outgoing[target_id][key] += value

    for target in outgoing:
        pairs = [[k, v] for k, v in outgoing[target].items()]
        if not pairs and peers:
            msg = {"type": "SHUFFLE_DATA", "from": worker_id, "pairs": [], "done": True}
        else:
            msg = {
                "type": "SHUFFLE_DATA",
                "from": worker_id,
                "pairs": pairs,
                "done": True,
            }
        target_info = next(w for w in all_workers if w["worker_id"] == target)
        try:
            with socket.create_connection(
                (target_info["host"], target_info["shuffle_port"])
            ) as sock:
                send_msg(sock, msg)
        except OSError as exc:
            print(f"[worker {worker_id}] Erreur en envoyant au worker {target}: {exc}")

    if peers:
        shuffle_state.mark_sender_done(worker_id)
        shuffle_state.done_event.wait()

    final_counts: Dict[str, int] = defaultdict(int)
    for key, value in aggregated_local.items():
        final_counts[key] += value
    for key, value in shuffle_state.data.items():
        final_counts[key] += value

    return dict(final_counts)


def run_worker(
    worker_id: str,
    master_host: str,
    master_port: int,
    shuffle_host: str,
    shuffle_port: int,
) -> None:
    with socket.create_connection((master_host, master_port)) as sock:
        send_msg(
            sock,
            {
                "type": "REGISTER",
                "worker_id": worker_id,
                "host": shuffle_host,
                "shuffle_port": shuffle_port,
            },
        )

        while True:
            msg = recv_msg(sock)
            if msg is None:
                break
            mtype = msg.get("type")

            if mtype == "START_JOB":
                splits = msg.get("splits") or []
                all_workers = msg.get("all_workers") or []
                if not isinstance(splits, list) or not isinstance(all_workers, list):
                    continue

                print(
                    f"[worker {worker_id}] Debut du job "
                    f"(splits assignes: {splits}, nb workers = {len(all_workers)})"
                )
                final_counts = run_map_shuffle_reduce(
                    worker_id=worker_id,
                    splits=[int(s) for s in splits],
                    all_workers=all_workers,
                    shuffle_host=shuffle_host,
                    shuffle_port=shuffle_port,
                )
                print(
                    f"[worker {worker_id}] Job termine, "
                    f"{len(final_counts)} cles reduites."
                )
                send_msg(
                    sock,
                    {"type": "JOB_DONE", "worker_id": worker_id, "num_keys": len(final_counts)},
                )

            elif mtype == "SHUTDOWN":
                print(f"[worker {worker_id}] Shutdown recu du master.")
                break


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Worker MapReduce (client)")
    parser.add_argument("--worker-id", required=True, help="Identifiant du worker")
    parser.add_argument(
        "--master-host", required=True, help="Adresse du master (serveur.py)"
    )
    parser.add_argument(
        "--master-port",
        type=int,
        default=MASTER_PORT,
        help="Port du master (defaut: 5374)",
    )
    parser.add_argument(
        "--shuffle-host",
        default="0.0.0.0",
        help="Adresse locale pour le shuffle (defaut: 0.0.0.0)",
    )
    parser.add_argument(
        "--shuffle-port",
        type=int,
        default=DEFAULT_SHUFFLE_PORT,
        help="Port local pour le shuffle (defaut: 6000)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_worker(
        worker_id=args.worker_id,
        master_host=args.master_host,
        master_port=args.master_port,
        shuffle_host=args.shuffle_host,
        shuffle_port=args.shuffle_port,
    )
