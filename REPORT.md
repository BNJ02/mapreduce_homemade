# Rapport de projet – MapReduce « fait maison » & loi d’Amdahl

## 1. Objectifs et contexte

Ce projet implémente un système de type MapReduce « fait maison » déployé sur les machines TP, avec pour objectifs principaux :

- Construire un pipeline complet Master/Workers sur TCP, sans transporter de données applicatives via le master.
- Mettre en place deux jobs MapReduce :
  - un **wordcount** sur CommonCrawl (`/cal/commoncrawl`) ;
  - un **tri réparti** des mots par fréquence (puis par ordre alphabétique).
- Réaliser une expérience de **loi d’Amdahl** en chronométrant uniquement les phases Map + Shuffle + Reduce.
- Ajouter une **analyse de la répartition des langues** via la bibliothèque Python `langdetect`.
- Automatiser les benchmarks et l’analyse via des scripts et des notebooks.

Le dépôt comporte principalement :

- `master.py` : master MapReduce.
- `worker.py` : worker MapReduce.
- `scripts/run_cluster.py` : lancement d’un job (master + workers).
- `scripts/benchmark_cluster.py` : benchmarks multi‑runs (Amdahl).
- `scripts/results_summary.py` : agrégation des résultats.
- `amdahl_analysis.ipynb`, `amdahl_report.ipynb` : analyse Amdahl + langues.
- `benchmarks/results.jsonl` : log JSONL des runs de benchmark.


## 2. Architecture générale

### 2.1. Protocole et rôles

#### Master (`master.py`)

- Ouvre un socket TCP (`HOST = 0.0.0.0`, `PORT = 5374`).
- Accepte des connexions de workers.
- Protocole d’échange :
  - message = `uint32 big-endian` (taille) + payload JSON (UTF‑8).
  - fonctions utilitaires : `recv_all`, `recv_msg`, `send_msg`.
- Pour chaque worker :
  - enregistre `worker_id`, `host`, `shuffle_port`.
- Garanties :
  - **ne manipule jamais de données applicatives** (ni texte, ni (clé, valeur)) ;
  - se contente de distribuer les splits, de coordonner les phases et de collecter les métriques.
- Rôle dans les phases :
  - Phase 1 (wordcount) :
    - reçoit les `WORDCOUNT_DONE` avec min/max de fréquences et un échantillonnage des fréquences ;
  - Phase 2 (tri) :
    - calcule les intervalles de fréquence globaux à partir des min/max + échantillons ;
    - envoie `START_SORT` avec l’intervalle propre + la liste des intervalles ;
  - Chronométrage :
    - démarre un timer juste avant `START_WORDCOUNT` ;
    - l’arrête après tous les `SORT_DONE` ;
    - écrit `master_metrics.json` dans le répertoire d’output (une entrée par run).

#### Workers (`worker.py`)

- Au démarrage :
  - se connectent au master (`MASTER_PORT = 5374`) ;
  - envoient :
    ```json
    {
      "type": "REGISTER",
      "worker_id": "...",
      "host": "...",
      "shuffle_port": ...
    }
    ```
  - ouvrent un serveur TCP pour le shuffle sur `shuffle_port`.
- Phases :
  1. **Wordcount** : lecture et comptage des fichiers WET + langues.
  2. **Tri réparti** : redistribution des mots par fréquences et tri.

- Événements envoyés au master :
  - `WORDCOUNT_DONE` (avec min/max de fréquence et échantillons) ;
  - `SORT_DONE` (fin du tri pour un intervalle donné).

- Persistances :
  - `*_wordcount.json` (résultats wordcount + stats) ;
  - `*_sorted.json` (résultats triés, intervalle couvert et liste triée).


### 2.2. Organisation du cluster

- `config/nodes.txt` : liste des machines TP disponibles (générée par `scripts/update_nodes.py` à partir de `https://tp.telecom-paris.fr/ajax.php`).
- Les machines partagent le code et les résultats via le **NFS** (`/cal/exterieurs/blepourt-25/hadoop_homemade`).

#### `scripts/run_cluster.py`

- Paramètres principaux :
  - `--nodes-file` : fichier de nœuds (typiquement `config/nodes.txt`).
  - `--master-node` / `--worker-nodes` (facultatif) ;
  - `--workers`, `--splits` ;
  - `--remote-path` : chemin du dépôt sur le NFS ;
  - `--uv-bin` : chemin vers `uv` distant ;
  - `--output-dir` : répertoire de sortie pour ce run (souvent `output/workersX_splitsY`).

- Comportement :
  - choisit un master (par défaut le premier nœud), puis `workers` suivants ;
  - lance le master :
    ```bash
    uv run python master.py --host <master_node> --workers <N> --splits <S> --output-dir <output_dir> ...
    ```
  - lance les workers par `ssh` :
    ```bash
    uv run python worker.py --worker-id <machine> --master-host <master_node> --shuffle-port <port> --output-dir <output_dir> ...
    ```

  - attend la fin du master + des workers ;
  - appelle `results_summary.py --output-dir <output_dir>` pour afficher le résumé ;
  - enrichit ensuite `master_metrics.json` avec le champ `languages` agrégé.

#### `scripts/benchmark_cluster.py`

- Paramètres :
  - `--remote-host` : login + machine de référence (ex. `blepourt-25@tp-1a226-10`) ;
  - `--remote-path` : chemin du dépôt sur le NFS ;
  - `--remote-uv-bin` : chemin vers `uv` ;
  - `--paths` : fichiers/répertoires à déployer (par défaut `master.py`, `worker.py`, `config`, `scripts/run_cluster.py`, `scripts/results_summary.py`) ;
  - `--splits` : nombre de splits ;
  - `--worker-counts` : liste des valeurs de workers à tester (expérience Amdahl) ;
  - `--benchmark-file` : fichier JSONL local (par défaut `benchmarks/results.jsonl`) ;
  - `--export-dir` : dossier racine local pour rapatriement optionnel ;
  - `--fetch-output` : si présent, rapatrie les fichiers `output/...` de chaque run.

- Logique :
  1. Crée `remote_path` et `remote_path/output` sur le NFS distant.
  2. Copie les fichiers nécessaires via `scp` (sans supprimer les fichiers individuels, mais en recréant les répertoires).
  3. Pour chaque `workers` dans `worker-counts` :
     - définit `remote_output_dir = <remote_path>/output/workers{workers}_splits{splits}` ;
     - purge ce répertoire (`rm -rf && mkdir -p`) ;
     - lance `run_cluster.py` avec `--output-dir remote_output_dir` ;
     - appelle `results_summary.py --output-dir remote_output_dir --json` à distance pour obtenir un résumé JSON ;
     - ajoute une ligne dans `benchmarks/results.jsonl` contenant :
       - `workers`, `splits`, `duration_seconds` (Amdahl),
       - `total_words`, `unique_words`, `top_words`,
       - `per_worker`, `languages`, `output_dir` ;
     - si `--fetch-output` est utilisé, rapatrie `remote_output_dir` dans `benchmarks/exports/run_workers{workers}_splits{splits}` via `rsync`.


## 3. Premier MapReduce : Wordcount sur CommonCrawl

### 3.1. Splits et lecture des fichiers

- Les splits logiques sont des indices `0..num_splits-1` mappés sur les fichiers WET de CommonCrawl :
  ```python
  filename = f"{DEFAULT_FILE_PREFIX}{split_id:05d}{DEFAULT_FILE_SUFFIX}"
  path = Path(DEFAULT_DATA_DIR) / filename
  ```
- Cette logique est encapsulée dans `build_split_path(split_id, data_dir, prefix, suffix, padding)`.

### 3.2. Phase Map locale (wordcount + langues)

Pour chaque split affecté à un worker :

- `wordcount_map_for_split` :
  - ouvre le fichier WET ;
  - lit les lignes par chunks (`CHUNK_LINE_COUNT = 5000`) ;
  - pour chaque chunk :
    - soumet le chunk au `ProcessPoolExecutor` :
      ```python
      chunk_futures.append(pool.submit(count_words_in_lines, chunk))
      ```
    - `count_words_in_lines` accumule les occurrences dans un dict ;
    - `detect_language` prend quelques lignes du chunk, appelle `langdetect.detect` (seed fixée) et incrémente `lang_counts` pour ce code.
  - fusionne tous les résultats :
    ```python
    counts[k] += v
    lang_counts[code] += 1
    ```
  - retourne `(counts, lang_counts)`.

Les mots sont extraits avec la regexp `WORD_RE = re.compile(r"[A-Za-z0-9']+")`.  
Le `ProcessPoolExecutor` (et non un pool de threads) permet de contourner le GIL pour le comptage de mots sur des gros fichiers.


### 3.3. Shuffle/Reduce (wordcount)

- Chaque worker connaît la liste des `worker_ids` et des ports de shuffle.
- Pour assigner une clé `word` à un worker, on utilise :
  ```python
  target_index = deterministic_hash(word) % num_workers
  target_id = worker_ids[target_index]
  ```
  avec `deterministic_hash` basé sur SHA‑256.

- Trois cas :
  - `target_id == me` → accumulation locale ;
  - sinon → accumulation dans `outgoing[target_id][word] += count`, puis envoi via TCP d’un message `SHUFFLE_DATA`.

- Côté réception (`handle_shuffle_connection` / `ShuffleState`) :
  - accumulation dans `shuffle_state.data` ;
  - un compteur de senders (`pending_senders`) permet de savoir quand tout le monde a terminé.

- Réduction finale :
  - `final_counts = aggregated_local + shuffle_state.data`.


### 3.4. Sorties `*_wordcount.json` & stat langues

Après la réduction :

- chaque worker appelle `persist_wordcount_results` :
  - calcule :
    - `total_words = sum(counts.values())` ;
    - `unique_words = len(counts)` ;
    - `vocab_density = unique_words / total_words` ;
    - `top_words` (top 20) ;
  - écrit le JSON :
    ```json
    {
      "worker_id": "...",
      "splits": [ ... ],
      "total_words": ...,
      "unique_words": ...,
      "vocab_density": ...,
      "top_words": [ {"word": "the", "count": 4613558}, ... ],
      "counts": { "the": 4613558, ... },
      "languages": { "en": nb_chunks, "fr": nb_chunks, ... }
    }
    ```

- envoie au master :
  ```json
  {
    "type": "WORDCOUNT_DONE",
    "worker_id": "...",
    "num_keys": len(wordcounts),
    "min_freq": min(wordcounts.values()),
    "max_freq": max(wordcounts.values()),
    "samples": [échantillons de fréquences]
  }
  ```

Ces informations serviront à construire les intervalles de la seconde phase.


## 4. Deuxième MapReduce : tri réparti par fréquence

### 4.1. Construction des intervalles de fréquences

Une fois tous les `WORDCOUNT_DONE` reçus :

- le master calcule :
  - `min_freq = min(w.min_freq for w in workers.values())` ;
  - `max_freq = max(w.max_freq for w in workers.values())` ;
- si `--frequency-step` est non nul :
  - `step = freq_step` ;
  - les intervalles `[min, min+step-1], ..., [last, max_freq]` sont attribués séquentiellement aux workers ;
- sinon (mode par défaut) :
  - construit `combined_samples = sorted(sum(w.samples for w in workers))` ;
  - pour `k` workers, utilise des quantiles sur `combined_samples` :
    - pour le worker i, on prend un segment de `combined_samples` correspondant à ~1/k des échantillons ;
    - le dernier intervalle est étendu jusqu’à `max_freq` ;
  - attribue ainsi `interval = (start_val, end_val)` à chaque worker.

Ces intervalles sont ensuite envoyés aux workers dans le message `START_SORT`, avec la liste complète de tous les intervalles (`intervals`) et la liste des workers (`all_workers`).


### 4.2. Phase Map/Shuffle/Reduce du tri

#### Distribution hybride low/high fréquence

Dans `run_sort_phase` :

- On définit un seuil `LOW_FREQ_THRESHOLD = 1000`.
- Pour chaque chunk de `(word, freq)` :
  - on appelle `distribute_chunk` qui renvoie :
    - un dict `local_data` pour les mots qui restent sur ce worker ;
    - un dict `outgoing` (liste de paires) par target.

`distribute_chunk` applique :

```python
if freq <= LOW_FREQ_THRESHOLD:
    target_index = deterministic_hash(word) % len(worker_ids)
    target = worker_ids[target_index]
else:
    target = assign_interval_owner(freq, intervals, worker_id)
```

- Les mots fréquents sont donc toujours dirigés vers l’intervalle qui les couvre (tri réparti correct).
- Les mots peu fréquents sont répartis presque uniformément entre workers via hash, évitant qu’un seul nœud ne porte tous les hapax.


#### Shuffle et tri final

Ensuite :

- chaque worker envoie ses `outgoing` à leurs cibles via un message `SHUFFLE_DATA` sur le deuxième service de shuffle ;
- attend la fin du shuffle (`ShuffleState.done_event`) ;
- fusionne :
  - ses données locales ;
  - les données reçues dans `shuffle_state.data`.

Puis il trie :

```python
sorted_items = sorted(
    final_bucket.items(),
    key=lambda kv: (-kv[1], kv[0]),
)
```

et persiste via `persist_sorted_results` :

```json
{
  "worker_id": "...",
  "interval": [min_i, max_i],
  "count": nb_mots,
  "items": [
    {"word": "the", "count": 4613558},
    ...
  ]
}
```

Enfin, il envoie :

```json
{"type": "SORT_DONE", "worker_id": "...", "count": len(sorted_items)}
```

au master, qui déclenche `all_done` lorsque tous les workers ont terminé.


## 5. Chronométrage & Benchmark

### 5.1. Chronométrage Map+Shuffle+Reduce

Dans `master.py` :

- juste avant l’envoi des `START_WORDCOUNT`, on exécute :
  ```python
  job_start_time = time.perf_counter()
  ```
- après :
  - `wordcount_done_event.wait()` (fin MapReduce 1) ;
  - `assign_sort_intervals()` + `notify_sort_phase()` ;
  - `all_done.wait()` (fin tri).
- puis :
  ```python
  duration = time.perf_counter() - job_start_time
  write_master_metrics(duration, workers=len(worker_ids), splits=num_splits, output_dir)
  ```
- La durée stockée dans `master_metrics.json` correspond donc strictement aux deux phases de calcul (Map+Shuffle+Reduce), *hors* déploiement initial et rapatriement.


### 5.2. Résumé d’un run

`results_summary.py` :

- lit tous les `*_wordcount.json` d’un `output_dir` donné, ainsi que `master_metrics.json` ;
- agrège :
  - `total_words`, `unique_words` (globaux) ;
  - `top_words` (top 20 global) ;
  - `per_worker` (mots/unqiues par worker) ;
  - `languages` (agrégation des champs `languages` par worker) ;
- renvoie un dict, et, en mode CLI, affiche :
  - totaux,
  - top 20 global,
  - top langues,
  - détails par worker,
  - temps `Map+Shuffle+Reduce` (depuis `master_metrics.json`).

`run_cluster.py` :

- affiche ce résumé ;
- ré‑écrit `master_metrics.json` pour y inclure `languages`.


### 5.3. Benchmarks multi‑runs

`benchmark_cluster.py` :

- pour chaque (workers, splits) :
  - lance `run_cluster.py` à distance ;
  - récupère le résumé JSON de `results_summary.py` ;
  - ajoute une ligne dans `benchmarks/results.jsonl` contenant toutes les informations nécessaires à l’analyse Amdahl et à la répartition de langues.


## 6. Loi d’Amdahl : analyse et visualisation

### 6.1. Données exploitées

`amdahl_analysis.ipynb` lit `benchmarks/results.jsonl` puis :

- construit un DataFrame `df` avec, pour chaque run :
  - `workers`, `splits`, `duration_seconds` ;
  - `total_words`, `unique_words` ;
  - `languages` ;
- convertit les colonnes numériques en `float`/`int` ;
- normalise la structure des langues en `language_pairs = [(lang, count), ...]`.


### 6.2. Définition du speedup (adaptée)

Pour chaque `splits = S` :

1. Agrégation des runs :
   ```python
   grouped = df.groupby(['splits','workers'], as_index=False).agg(
       elapsed_seconds=('duration_seconds','mean'),
       elapsed_std=('duration_seconds','std'),
       run_count=('timestamp','count'),
   )
   ```

2. Calcul du temps de référence :

   - on cherche la ligne avec `workers == splits` :  
     `T_splits = elapsed_seconds(workers == splits)` ;  
     `T_ref = T_splits / splits` ;
   - si elle n’existe pas, repli sur le run avec le plus petit `workers` disponible :
     `T_ref = elapsed_min / workers_min`.

3. Calcul du speedup observé pour chaque `N` :

   \[
   S_\text{obs}(N) = \frac{N \cdot T_\text{ref}}{T(N)}
   \]

   en code :

   ```python
   data['speedup_observe'] = (data['workers'] * T_ref) / data['elapsed_seconds']
   ```


### 6.3. Ajustement du taux de parallélisation `p`

On utilise la formulation rappelée :

\[
S_{\text{latence}}(s) = \frac{1}{1 - p + \frac{p}{s}}
\]

Pour chaque `splits` :

- on définit :

  ```python
  def amdahl_speedup(p, s):
      return 1.0 / (1.0 - p + p / s)
  ```

- on cherche `p` sur un grid `[0, 0.99]` qui minimise :

  \[
  \sum_N \left( S_{\text{latence}}(p, N) - S_\text{obs}(N) \right)^2
  \]

  via :

  ```python
  s = data['workers'].to_numpy(dtype=float)
  obs = data['speedup_observe'].to_numpy(dtype=float)
  grid = np.linspace(0.0, 0.99, 2000)
  errors = [np.sum((amdahl_speedup(p, s) - obs) ** 2) for p in grid]
  p_parallel = float(grid[int(np.argmin(errors))])
  ```

- on ajoute :

  ```python
  data['speedup_pred_amdahl'] = amdahl_speedup(p_parallel, s)
  data['p_parallel_estimate'] = p_parallel
  f_serial = 1.0 - p_parallel
  ```

On obtient ainsi, pour chaque volume `splits`, un taux de parallélisation `p` et une fraction sérielle `f = 1 - p`.


### 6.4. Visualisations

- **2D par splits** :
  - graphique durée moyenne (avec barres d’erreur) vs workers ;
  - graphique speedup observé vs speedup Amdahl, avec titre :
    `Splits = S (p ≈ ..., f ≈ ...)`.

- **3D** :
  - scatter `(workers, splits, speedup_observe)` ;
  - scatter `(workers, splits, speedup_pred_amdahl)` ;
  - donne une vue globale de l’« hyperplan » Amdahl empirique.

- **Langues** :
  - pour chaque `splits`, on agrège `lang_count` ;
  - pie charts (top 10 langues + “Autres”).

Ces visualisations sont regroupées et commentées dans `amdahl_analysis.ipynb` et `amdahl_report.ipynb`.


## 7. Conclusion

Le système réalisé :

- respecte les contraintes de protocole (TCP, master sans données applicatives, shuffle pair‑à‑pair) ;
- implémente deux MapReduce complets (wordcount + tri réparti par fréquences/ordre alphabétique) sur CommonCrawl ;
- exploite le multi‑cœurs à l’intérieur des workers via des `ProcessPoolExecutor` pour la phase Map et le tri ;
- équilibre la charge lors du tri en combinant une répartition par intervalles de fréquences et un hashing des mots peu fréquents ;
- mesure précisément la durée Map+Shuffle+Reduce, et enregistre les résultats par run ;
- produit des benchmarks structurés (`benchmarks/results.jsonl`) permettant une analyse quantitative conforme à la loi d’Amdahl sur différents volumes de données ;
- intègre une analyse des langues (`langdetect`) dans la phase Map, et expose ces statistiques jusqu’aux pie charts dans les notebooks.

L’estimation du taux de parallélisation `p` et de la fraction sérielle `f` est faite à partir de mesures réelles, en utilisant la définition de speedup adaptée à ton protocole (référence bâtie sur le cas `workers == splits`). Les graphes 2D/3D montrent ainsi comment le système se rapproche ou s’éloigne de la surface idéale prédite par la loi d’Amdahl, pour chaque volume de données, tout en documentant la répartition des langues dans le corpus traité.

