# client.py
import argparse
import hashlib
import json
import re
import socket
import struct
import threading
from concurrent.futures import ProcessPoolExecutor
from collections import defaultdict
import os
from pathlib import Path
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
DEFAULT_DATA_DIR = "/cal/commoncrawl"
DEFAULT_FILE_PREFIX = "CC-MAIN-20230320083513-20230320113513-"
DEFAULT_FILE_SUFFIX = ".warc.wet"
DEFAULT_SPLIT_PADDING = 5
DEFAULT_OUTPUT_DIR = str(Path(__file__).resolve().parent / "output")
CHUNK_LINE_COUNT = 5000
WORD_RE = re.compile(r"[A-Za-z0-9']+")


def deterministic_hash(key: str) -> int:
    digest = hashlib.sha256(key.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big", signed=False)


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


def build_split_path(
    split_id: int,
    data_dir: str,
    file_prefix: str,
    file_suffix: str,
    split_padding: int,
) -> Path:
    filename = f"{file_prefix}{split_id:0{split_padding}d}{file_suffix}"
    return Path(data_dir) / filename


def count_words_in_lines(lines: List[str]) -> Dict[str, int]:
    local_counts: Dict[str, int] = defaultdict(int)
    for line in lines:
        for match in WORD_RE.findall(line.lower()):
            local_counts[match] += 1
    return local_counts


def wordcount_map_for_split(
    split_id: int,
    data_dir: str,
    file_prefix: str,
    file_suffix: str,
    split_padding: int,
    process_pool: ProcessPoolExecutor,
) -> Dict[str, int]:
    """
    Lit un fichier CommonCrawl (format WET) et renvoie le wordcount du split.

    Les fichiers sont nommes comme suit :
    <file_prefix><numero zero-pad><file_suffix>
    Exemple : CC-...-00000.warc.wet
    """
    counts: Dict[str, int] = defaultdict(int)
    file_path = build_split_path(split_id, data_dir, file_prefix, file_suffix, split_padding)

    try:
        chunk_futures = []
        current_chunk: List[str] = []

        with file_path.open("r", encoding="utf-8", errors="ignore") as fh:
            for line in fh:
                current_chunk.append(line)
                if len(current_chunk) >= CHUNK_LINE_COUNT:
                    chunk = current_chunk
                    current_chunk = []
                    chunk_futures.append(process_pool.submit(count_words_in_lines, chunk))

        if current_chunk:
            chunk_futures.append(process_pool.submit(count_words_in_lines, current_chunk))

        for future in chunk_futures:
            local_counts = future.result()
            for key, value in local_counts.items():
                counts[key] += value
    except FileNotFoundError:
        print(f"[worker] Fichier introuvable pour le split {split_id}: {file_path}")
    except OSError as exc:
        print(f"[worker] Erreur lecture {file_path}: {exc}")

    return dict(counts)


def persist_wordcount_results(
    worker_id: str,
    splits: List[int],
    counts: Dict[str, int],
    output_dir: str,
) -> Path:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    total_words = sum(counts.values())
    unique_words = len(counts)
    vocab_density = (unique_words / total_words) if total_words else 0.0
    top_items = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)[:20]

    result = {
        "worker_id": worker_id,
        "splits": splits,
        "total_words": total_words,
        "unique_words": unique_words,
        "vocab_density": vocab_density,
        "top_words": [{"word": k, "count": v} for k, v in top_items],
        "counts": counts,
    }

    output_path = out_dir / f"{worker_id}_wordcount.json"
    with output_path.open("w", encoding="utf-8") as fh:
        json.dump(result, fh, ensure_ascii=False, indent=2)
    return output_path


def persist_sorted_results(
    worker_id: str,
    intervals: Tuple[int, int],
    sorted_items: List[Tuple[str, int]],
    output_dir: str,
) -> Path:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    result = {
        "worker_id": worker_id,
        "interval": intervals,
        "count": len(sorted_items),
        "items": [{"word": w, "count": c} for w, c in sorted_items],
    }
    output_path = out_dir / f"{worker_id}_sorted.json"
    with output_path.open("w", encoding="utf-8") as fh:
        json.dump(result, fh, ensure_ascii=False, indent=2)
    return output_path


def run_map_shuffle_reduce(
    worker_id: str,
    splits: List[int],
    all_workers: List[dict],
    shuffle_host: str,
    shuffle_port: int,
    data_dir: str,
    file_prefix: str,
    file_suffix: str,
    split_padding: int,
    map_threads: int,
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

    total_threads = max(1, map_threads)
    process_pool: ProcessPoolExecutor | None = None
    if splits:
        process_pool = ProcessPoolExecutor(max_workers=total_threads)

    try:
        for split_id in splits:
            if process_pool is None:
                local_counts = {}
            else:
                try:
                    local_counts = wordcount_map_for_split(
                        split_id=split_id,
                        data_dir=data_dir,
                        file_prefix=file_prefix,
                        file_suffix=file_suffix,
                        split_padding=split_padding,
                        process_pool=process_pool,
                    )
                except Exception as exc:  # pragma: no cover
                    print(f"[worker {worker_id}] Erreur Map split {split_id}: {exc}")
                    continue
            for key, value in local_counts.items():
                target_index = deterministic_hash(key) % num_workers
                target_id = worker_ids[target_index]
                if target_id == worker_id:
                    aggregated_local[key] += value
                else:
                    outgoing[target_id][key] += value
    finally:
        if process_pool:
            process_pool.shutdown()

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


def assign_interval_owner(
    count: int, intervals: List[dict], default_worker: str
) -> str:
    for interval in intervals:
        if not isinstance(interval, dict):
            continue
        wid = interval.get("worker_id")
        min_freq = int(interval.get("min", 0))
        max_freq = int(interval.get("max", 0))
        if min_freq <= count <= max_freq:
            return wid
    return default_worker


def distribute_chunk(
    chunk: List[Tuple[str, int]],
    intervals: List[dict],
    worker_id: str,
    worker_ids: List[str],
) -> Tuple[Dict[str, int], Dict[str, List[Tuple[str, int]]]]:
    local_data: Dict[str, int] = {}
    outgoing: Dict[str, List[Tuple[str, int]]] = {wid: [] for wid in worker_ids if wid != worker_id}
    for word, freq in chunk:
        target = assign_interval_owner(freq, intervals, worker_id)
        if target == worker_id:
            local_data[word] = freq
        else:
            outgoing.setdefault(target, []).append((word, freq))
    return local_data, outgoing


def run_sort_phase(
    worker_id: str,
    wordcounts: Dict[str, int],
    intervals: List[dict],
    my_interval: Tuple[int, int],
    all_workers: List[dict],
    shuffle_host: str,
    shuffle_port: int,
    map_threads: int,
) -> List[Tuple[str, int]]:
    worker_ids = [w["worker_id"] for w in all_workers]
    peers = [wid for wid in worker_ids if wid != worker_id]
    shuffle_state = ShuffleState(expected_peers=peers)
    listener_thread = threading.Thread(
        target=shuffle_listener,
        args=(shuffle_host, shuffle_port, shuffle_state),
        daemon=True,
    )
    listener_thread.start()

    local_bucket: Dict[str, int] = {}
    outgoing_pairs: Dict[str, List[Tuple[str, int]]] = {
        wid: [] for wid in worker_ids if wid != worker_id
    }

    items = list(wordcounts.items())
    max_workers = max(1, map_threads)
    chunk_size = max(1, len(items) // max_workers)
    chunks: List[List[Tuple[str, int]]] = [
        items[i : i + chunk_size] for i in range(0, len(items), chunk_size)
    ]

    if len(chunks) > 1:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(distribute_chunk, chunk, intervals, worker_id, worker_ids)
                for chunk in chunks
            ]
            for future in futures:
                chunk_local, chunk_outgoing = future.result()
                local_bucket.update(chunk_local)
                for target, pairs in chunk_outgoing.items():
                    if not pairs:
                        continue
                    outgoing_pairs.setdefault(target, []).extend(pairs)
    else:
        for word, freq in items:
            target = assign_interval_owner(freq, intervals, worker_id)
            if target == worker_id:
                local_bucket[word] = freq
            else:
                outgoing_pairs.setdefault(target, []).append((word, freq))

    for target in outgoing_pairs:
        pairs = [[word, freq] for word, freq in outgoing_pairs[target]]
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
            print(f"[worker {worker_id}] Erreur tri vers {target}: {exc}")

    if peers:
        shuffle_state.mark_sender_done(worker_id)
        shuffle_state.done_event.wait()

    final_bucket: Dict[str, int] = dict(local_bucket)
    for word, freq in shuffle_state.data.items():
        final_bucket[word] = freq

    sorted_items = sorted(
        final_bucket.items(),
        key=lambda kv: (-kv[1], kv[0]),
    )
    return sorted_items


def all_workers_done(state: Dict[str, int], nb_workers: int) -> bool:
    return len(state) >= nb_workers


def run_worker(
    worker_id: str,
    master_host: str,
    master_port: int,
    shuffle_host: str,
    shuffle_port: int,
    data_dir: str,
    file_prefix: str,
    file_suffix: str,
    split_padding: int,
    map_threads: int,
    output_dir: str,
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

        wordcounts: Dict[str, int] = {}
        all_workers_meta: List[dict] = []

        while True:
            msg = recv_msg(sock)
            if msg is None:
                break
            mtype = msg.get("type")

            if mtype == "START_WORDCOUNT":
                splits = msg.get("splits") or []
                all_workers_meta = msg.get("all_workers") or []
                if not isinstance(splits, list) or not isinstance(all_workers_meta, list):
                    continue
                print(
                    f"[worker {worker_id}] Job 1/2 wordcount "
                    f"(splits: {splits}, nb workers = {len(all_workers_meta)})"
                )
                split_ids = [int(s) for s in splits]
                wordcounts = run_map_shuffle_reduce(
                    worker_id=worker_id,
                    splits=split_ids,
                    all_workers=all_workers_meta,
                    shuffle_host=shuffle_host,
                    shuffle_port=shuffle_port,
                    data_dir=data_dir,
                    file_prefix=file_prefix,
                    file_suffix=file_suffix,
                    split_padding=split_padding,
                    map_threads=map_threads,
                )
                output_path = persist_wordcount_results(
                    worker_id=worker_id,
                    splits=split_ids,
                    counts=wordcounts,
                    output_dir=output_dir,
                )
                min_freq = min(wordcounts.values()) if wordcounts else 0
                max_freq = max(wordcounts.values()) if wordcounts else 0
                send_msg(
                    sock,
                    {
                        "type": "WORDCOUNT_DONE",
                        "worker_id": worker_id,
                        "num_keys": len(wordcounts),
                        "min_freq": min_freq,
                        "max_freq": max_freq,
                    },
                )
                print(f"[worker {worker_id}] Wordcount OK ({len(wordcounts)} cles). {output_path}")

            elif mtype == "START_SORT":
                interval = msg.get("interval") or {}
                intervals = msg.get("intervals") or []
                all_workers_sort = msg.get("all_workers") or all_workers_meta
                min_freq = int(interval.get("min", 0))
                max_freq = int(interval.get("max", 0))
                print(
                    f"[worker {worker_id}] Job 2/2 tri "
                    f"(intervalle [{min_freq}, {max_freq}])"
                )
                sorted_items = run_sort_phase(
                    worker_id=worker_id,
                    wordcounts=wordcounts,
                    intervals=intervals,
                    my_interval=(min_freq, max_freq),
                    all_workers=all_workers_sort,
                    shuffle_host=shuffle_host,
                    shuffle_port=shuffle_port,
                    map_threads=map_threads,
                )
                output_path = persist_sorted_results(
                    worker_id=worker_id,
                    intervals=(min_freq, max_freq),
                    sorted_items=sorted_items,
                    output_dir=output_dir,
                )
                send_msg(
                    sock,
                    {
                        "type": "SORT_DONE",
                        "worker_id": worker_id,
                        "count": len(sorted_items),
                    },
                )
                print(
                    f"[worker {worker_id}] Tri OK ({len(sorted_items)} elements) "
                    f"-> {output_path}"
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
    parser.add_argument(
        "--data-dir",
        default=DEFAULT_DATA_DIR,
        help="Repertoire contenant les fichiers CommonCrawl (defaut: /cal/commoncrawl)",
    )
    parser.add_argument(
        "--file-prefix",
        default=DEFAULT_FILE_PREFIX,
        help="Prefixe des fichiers WET (defaut: CC-MAIN-20230320083513-20230320113513-)",
    )
    parser.add_argument(
        "--file-suffix",
        default=DEFAULT_FILE_SUFFIX,
        help="Suffixe des fichiers WET (defaut: .warc.wet)",
    )
    parser.add_argument(
        "--split-padding",
        type=int,
        default=DEFAULT_SPLIT_PADDING,
        help="Nombre de chiffres pour zero-pad l'index de split (defaut: 5)",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Repertoire de sortie pour les resultats du worker (defaut: output/)",
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
        data_dir=args.data_dir,
        file_prefix=args.file_prefix,
        file_suffix=args.file_suffix,
        split_padding=args.split_padding,
        map_threads=max(1, os.cpu_count() or 1),
        output_dir=args.output_dir,
    )
