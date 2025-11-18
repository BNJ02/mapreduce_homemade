# Projet MapReduce « fait maison » – Télécom Paris

Ce dépôt contient une implémentation pédagogique d’un système de type MapReduce, destinée à être déployée sur les machines de Télécom Paris, ainsi qu’une expérimentation de la loi d’Amdahl.

Ce document décrit :
- le plan global du projet,
- les étapes à réaliser dans l’ordre chronologique,
- les contraintes d’infrastructure et d’évaluation issues du cahier des charges.

---

## 1. Objectifs fonctionnels (rappel du cahier des charges)

- **Protocole TCP** :
  - Un **master** synchronise les **workers**.
  - Le master envoie aux workers :
    - la liste des nœuds participants,
    - les « splits » (blocs de données) à traiter.
  - Les **données ne transitent jamais par le master**.
  - Le **shuffle** est direct entre workers, via une fonction de hachage (clé modulo nombre de machines).

- **Expérience loi d’Amdahl** :
  - Taille totale des données **fixe**.
  - Tests sur un nombre croissant de nœuds (≥ 3 machines).
  - On chronomètre **uniquement Map + Shuffle + Reduce** (pas le déploiement, ni la collecte des résultats).
  - Tracé de la courbe de **speedup** et estimation du **taux de parallélisation global**.
  - **Extrapolation linéaire** : si un split fait 300 Mo, extrapoler pour X Mo (ex. 3000 Mo = 10× le temps).
  - À partir de 2 nœuds : **distribution round‑robin** des splits (split 1 → nœud 1, split 2 → nœud 2, etc.).

- **Deuxième MapReduce (tri réparti)** :
  - Tri des mots par **fréquence**, puis par **ordre alphabétique**.
  - Répartition des **intervalles de fréquence** entre nœuds (ex. nœud 1 : 1‑10, nœud 2 : 11‑20, etc.).
  - Échange des **min/max de fréquences** pour définir correctement ces intervalles.

- **Déploiement** :
  - Un seul **SCP** vers une machine, en s’appuyant sur un **NFS partagé** pour les autres.
  - Démarrage des serveurs via **SSH** :
    - contrainte : **max 5 connexions SSH/min** depuis une même machine source,
    - pas de limite depuis une machine personnelle.
  - Tests par défaut sous **Linux** (scripts PowerShell/Windows à signaler explicitement si utilisés).

- **Rendu et évaluation** :
  - Rapport (max 5 pages) : description du système, protocole, loi d’Amdahl, données brutes exploitables.
  - Code + scripts de déploiement dans une archive ZIP.
  - Bonus possible : analyse de la répartition des langues dans CommonCrawl (langdetect).

---

## 2. Environnement cible

- **Machines de Télécom Paris** :
  - Accès typique : `ssh blepourt-25@tp-m5-27`.
  - La liste des machines disponibles est accessible (sans scraping) via :  
    `https://tp.telecom-paris.fr/ajax.php`.
  - L’infrastructure repose sur des **machines Linux** avec un **NFS partagé** pour les répertoires.

- **Langage et version** :
  - Développement en **Python 3.14** (en anticipant d’éventuels ajustements de version disponibles sur les machines).
  - Utilisation intensive du **multithreading / multicœurs** côté workers pour accélérer les tâches locales.

- **Gestion des environnements avec `uv`** :
  - Création d’un environnement virtuel :
    - `uv venv .venv`
  - Installation des dépendances :
    - `uv pip install -r requirements.txt`
  - Objectif : avoir des scripts reproductibles et faciles à lancer sur les machines de l’école.

---

## 3. Organisation du dépôt (prévisionnelle)

L’organisation exacte pourra évoluer, mais on vise une structure de ce type :

- `serveur.py` : logique du master (coordination, distribution des splits, suivi des workers).
- `client.py` : logique d’un worker (phases Map/Shuffle/Reduce, communication TCP).
- `scripts/` :
  - `deploy_cluster.sh` : copie des fichiers sur une machine et configuration NFS.
  - `start_workers.sh` : lancement des workers via SSH (respect de la limite 5 SSH/min).
  - `run_experiment.sh` : automatisation des expériences (nombre de nœuds, dataset, mesures).
- `config/` :
  - `nodes.txt` : liste des machines utilisées.
  - `settings.yaml` : paramètres (taille des splits, chemins de données, ports, etc.).
- `data/` :
  - `input/` : données d’entrée (corpus texte).
  - `output/` : résultats par expérience / nombre de nœuds.
  - `raw_metrics/` : mesures brutes (temps par phase, taille des données, etc.).
- `notebooks/` (optionnel) :
  - analyse et visualisation de la loi d’Amdahl.
- `rapport/` :
  - sources du rapport (LaTeX/Markdown) et figures.

---

## 4. Plan de développement (chronologique)

### Étape 0 – Préparation de l’environnement

- Vérifier l’accès SSH aux machines de Télécom :  
  `ssh blepourt-25@tp-m5-27`.
- Récupérer la liste des machines disponibles depuis `https://tp.telecom-paris.fr/ajax.php` et constituer un fichier `config/nodes.txt`.
- Installer `uv` sur la machine de développement (perso ou TP).
- Créer l’environnement Python 3.14 et un premier `requirements.txt` minimal.

### Étape 1 – Conception de l’architecture

- Définir les rôles :
  - **Master** : coordination, distribution des splits, connaissance de la topologie.
  - **Workers** : exécution des phases Map/Shuffle/Reduce, échanges directs de données.
- Spécifier le **protocole TCP** :
  - format des messages (JSON, binaire, etc.),
  - messages de contrôle : `REGISTER`, `ASSIGN_SPLIT`, `SHUFFLE_INFO`, `REDUCE_DONE`, etc.,
  - format des erreurs et mécanismes de reconnexion (simple au début).
- Définir la **fonction de hachage** pour le shuffle (ex. `hash(key) % nb_workers`).
- Concevoir la gestion du **multithreading** côté worker :
  - un thread par split, ou un pool de threads,
  - synchronisation de la phase Reduce.

### Étape 2 – Prototype local (un seul nœud)

- Implémenter une première version **mononœud** pour valider la logique :
  - Master et worker sur la même machine.
  - Communication TCP simple, sans NFS ni SSH.
- Implémenter la **phase Map** sur un petit corpus local (compte de mots).
- Implémenter une **phase Reduce** locale, sans shuffle réseau (simple agrégation).
- Ajouter des **tests unitaires** ou scripts de validation pour vérifier l’exactitude des résultats.

### Étape 3 – MapReduce distribué (multi‑nœuds, shuffle direct)

- Étendre le prototype pour utiliser plusieurs workers sur plusieurs machines :
  - le master lit `config/nodes.txt` pour connaître les adresses des nœuds,
  - chaque worker se connecte au master et annonce sa disponibilité.
- Implémenter le **partitionnement des données en splits** et la distribution :
  - distribution round‑robin à partir de 2 nœuds,
  - garantir que la taille totale des données reste fixe pour les expériences.
- Implémenter le **shuffle direct entre workers** :
  - chaque worker Map envoie ses paires (clé, valeur) vers les workers appropriés, selon la fonction de hachage.
  - format et protocole des messages de shuffle.
- Implémenter la **phase Reduce distribuée** :
  - chaque worker Reduce agrège les clés dont il est responsable,
  - les résultats sont écrits sur le NFS (dossier partagé) ou renvoyés au master pour agrégation finale légère.

### Étape 4 – Intégration du NFS et scripts de déploiement

- S’appuyer sur le **NFS partagé** pour éviter des copies de données multiples :
  - les données d’entrée et de sortie sont stockées sur un répertoire partagé.
- Écrire les scripts dans `scripts/` :
  - `deploy_cluster.sh` :
    - un seul `scp` vers une machine,
    - configuration (ou vérification) de la visibilité NFS des fichiers sur les autres machines.
  - `start_workers.sh` :
    - lancement des workers via SSH en respectant la limite de 5 SSH/min par machine source,
    - possibilité de lancer les SSH depuis une machine perso pour s’affranchir de cette limite si besoin.
- Tester le déploiement complet : master + plusieurs workers + exécution d’un job MapReduce simple.

### Étape 5 – Deuxième MapReduce : tri réparti par fréquence / alphabétique

- Réutiliser le résultat du premier MapReduce (compte de mots).
- Implémenter un second job MapReduce pour :
  - trier les mots par fréquence décroissante,
  - en cas d’égalité de fréquence, trier par ordre alphabétique.
- Mettre en place la **répartition par intervalles de fréquence** :
  - première passe pour déterminer les **min/max** de fréquences globales,
  - calcul d’intervalles (par exemple quantiles ou tranches régulières),
  - assignation des intervalles aux workers Reduce.
- Valider :
  - que chaque mot apparaît exactement dans un intervalle,
  - que l’ordre global (fréquence, puis alphabétique) est correct lorsque les résultats sont concaténés.

### Étape 6 – Expériences de loi d’Amdahl

- Choisir un **dataset fixe** (taille totale constante) dans `data/input/`.
- Automatiser les expériences dans `scripts/run_experiment.sh` :
  - lancement du système pour un nombre de nœuds croissant (ex. 1, 2, 4, 8…),
  - mesure séparée des temps Map, Shuffle et Reduce,
  - enregistrement des résultats dans `data/raw_metrics/amdahl_<date>.csv`.
- Calculer et tracer :
  - **speedup** en fonction du nombre de nœuds,
  - estimation du **taux de parallélisation**,
  - **extrapolation linéaire** pour des tailles de données plus grandes.
- Documenter les hypothèses et les limites (e.g. surcharge réseau, synchronisation, variabilité des machines).

### Étape 7 – Bonus (optionnel) : analyse de CommonCrawl (langdetect)

- Intégrer une bibliothèque de **détection de langue** (ex. `langdetect`) via `uv`.
- Adapter la phase Map pour :
  - détecter la langue de segments de texte,
  - produire des (langue, 1) en sortie Map.
- Réaliser un Reduce pour obtenir la **répartition des langues**.
- Discuter l’impact sur :
  - les performances (modèle plus coûteux),
  - l’équilibrage de charge entre nœuds.

### Étape 8 – Rapport, reproductibilité et rendu

- Rédiger le **rapport (≤ 5 pages)** :
  - architecture globale du système,
  - description du protocole (messages, séquences typiques),
  - résultats de la loi d’Amdahl : courbes, speedup, analyse du plateau,
  - discussion critique et comparaison avec Hadoop (forces/faiblesses).
- Préparer un **jeu de données brutes** :
  - fichier CSV ou équivalent contenant toutes les mesures,
  - documentation claire : emplacement, format, commande pour reproduire les expériences.
- Vérifier la **reproductibilité** :
  - script unique (ou très limité) permettant de lancer une expérience complète à partir d’un cluster de machines,
  - instructions précises dans le README (ou un `HOWTO.md`) pour :
    - installer l’environnement avec `uv`,
    - déployer sur les machines TP,
    - lancer les expériences et récupérer les résultats.
- Préparer l’archive **ZIP** finale :
  - code,
  - scripts de déploiement,
  - ce README et la documentation complémentaire.

---

## 5. Prochaines actions immédiates

- Finaliser la liste des machines dans `config/nodes.txt` à partir de `https://tp.telecom-paris.fr/ajax.php`.
- Valider la disponibilité de Python 3.14 (ou ajuster la version au besoin) sur les machines TP.
- Implémenter le prototype local (Étape 2) à partir de `serveur.py` et `client.py`.
- Mettre en place les premiers scripts de déploiement et d’exécution pour tester rapidement sur 2–3 machines.

