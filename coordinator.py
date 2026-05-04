"""
coordinator.py — Chef d'orchestre du cluster MapReduce

Deux modes :
  local  (défaut) : lance les workers comme sous-processus Python
  docker          : workers déjà lancés par docker-compose, attend leurs résultats

Usage :
    python coordinator.py
    python coordinator.py --reducers 4 --top 30
    python coordinator.py --mode docker --reducers 2
"""

import subprocess
import threading
import sys
import os
import json
import time
import argparse
import glob
from utils import OUTPUT_DIR


# ─────────────────────────────────────────────
# ARGUMENTS CLI
# ─────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(description="Coordinateur MapReduce")
    parser.add_argument("--mode", choices=["local", "docker"], default="local",
                        help="Mode d'exécution (défaut: local)")
    parser.add_argument("--reducers", type=int, default=2,
                        help="Nombre de workers REDUCE (défaut: 2)")
    parser.add_argument("--input-dir", type=str, default="texts",
                        help="Dossier des fichiers .txt (défaut: texts/)")
    parser.add_argument("--top", type=int, default=20,
                        help="Nombre de mots à afficher (défaut: 20)")
    return parser.parse_args()


# ─────────────────────────────────────────────
# UTILITAIRES COMMUNS
# ─────────────────────────────────────────────

def launch_worker(script, args_list, label):
    """Lance un worker comme sous-processus Python."""
    cmd = [sys.executable, script] + [str(a) for a in args_list]
    print(f"  [COORD] Lancement : {' '.join(cmd)}")
    return subprocess.Popen(cmd)


def wait_for_process(proc, label, results_dict):
    """Attend la fin d'un processus et enregistre son code de retour."""
    proc.wait()
    results_dict[label] = proc.returncode
    status = "✓" if proc.returncode == 0 else "⚠️"
    print(f"  [COORD] {status}  {label} terminé (code {proc.returncode})")


def merge_results(num_reducers, output_dir):
    """Fusionne les fichiers JSON de chaque reducer en un résultat global."""
    print("\n" + "─" * 50)
    print("[COORD] Fusion des résultats partiels...")

    merged = {}
    for r_id in range(num_reducers):
        path = os.path.join(output_dir, f"result_reducer_{r_id}.json")
        if not os.path.exists(path):
            print(f"  [COORD] ⚠️  Fichier manquant : {path}")
            continue
        with open(path, "r", encoding="utf-8") as f:
            partial = json.load(f)
        for word, count in partial.items():
            merged[word] = merged.get(word, 0) + count
        print(f"  [COORD] Fusionné {path} ({len(partial)} mots)")

    sorted_result = dict(sorted(merged.items(), key=lambda x: x[1], reverse=True))

    final_path = os.path.join(output_dir, "result_final.json")
    with open(final_path, "w", encoding="utf-8") as f:
        json.dump(sorted_result, f, ensure_ascii=False, indent=2)

    return sorted_result


def display_results(result, top_n, total_time, output_dir):
    """Affiche le Top N des mots et les statistiques."""
    print("\n" + "=" * 60)
    print(f"  RÉSULTATS FINAUX — Top {top_n} mots")
    print("=" * 60)

    top_words = list(result.items())[:top_n]
    max_count = top_words[0][1] if top_words else 1
    for rank, (word, count) in enumerate(top_words, 1):
        bar = "█" * int((count / max_count) * 30)
        print(f"  {rank:3}. {word:<20} {count:6}  {bar}")

    print("\n" + "─" * 50)
    print(f"[COORD] Mots distincts      : {len(result)}")
    print(f"[COORD] Temps total         : {total_time:.3f}s")
    print(f"[COORD] Résultat sauvegardé : {os.path.join(output_dir, 'result_final.json')}")


def clean_output(output_dir):
    """Supprime les anciens résultats avant chaque exécution."""
    os.makedirs(output_dir, exist_ok=True)
    for old in glob.glob(os.path.join(output_dir, "result_*.json")):
        os.remove(old)


# ─────────────────────────────────────────────
# MODE LOCAL
# ─────────────────────────────────────────────

def run_local_mode(args):
    """Lance MAP et REDUCE comme sous-processus sur la machine locale."""
    input_dir = args.input_dir

    if not os.path.isdir(input_dir):
        print(f"[COORD] ERREUR : le dossier '{input_dir}' n'existe pas.")
        print(f"[COORD] Dossier par défaut : 'texts/'  —  utilisez --input-dir pour en choisir un autre.")
        sys.exit(1)

    text_files = sorted(glob.glob(os.path.join(input_dir, "*.txt")))
    if not text_files:
        print(f"[COORD] ERREUR : aucun fichier .txt trouvé dans '{input_dir}'")
        sys.exit(1)

    num_mappers  = len(text_files)
    num_reducers = args.reducers
    output_dir   = OUTPUT_DIR

    print(f"\n[COORD] Configuration :")
    print(f"  - Fichiers : {num_mappers}  ({input_dir}/)")
    for i, f in enumerate(text_files):
        print(f"      MAP-{i} → {f}")
    print(f"  - Reducers : {num_reducers}")
    print(f"  - Top N    : {args.top}")
    print(f"  - Sortie   : {output_dir}/")

    clean_output(output_dir)
    total_start = time.time()

    # ── Phase MAP ───────────────────────────────────────────────
    print("\n" + "─" * 50)
    print("[COORD] Phase MAP — lancement des workers...")

    map_procs = []
    for i, filepath in enumerate(text_files):
        proc = launch_worker("map_worker.py", [i, filepath, num_reducers], f"MAP-{i}")
        map_procs.append((f"MAP-{i}", proc))

    map_results, map_threads = {}, []
    for label, proc in map_procs:
        t = threading.Thread(target=wait_for_process, args=(proc, label, map_results))
        t.start()
        map_threads.append(t)

    # Délai pour laisser les MAPs ouvrir leurs sockets avant les REDUCEs
    print(f"\n[COORD] Attente que les {num_mappers} MAPs soient prêts...")
    time.sleep(2)

    # ── Phase REDUCE ────────────────────────────────────────────
    print("\n" + "─" * 50)
    print("[COORD] Phase REDUCE — lancement des workers...")

    reduce_procs = []
    for r_id in range(num_reducers):
        proc = launch_worker("reduce_worker.py", [r_id, num_mappers, num_reducers], f"REDUCE-{r_id}")
        reduce_procs.append((f"REDUCE-{r_id}", proc))

    reduce_results, reduce_threads = {}, []
    for label, proc in reduce_procs:
        t = threading.Thread(target=wait_for_process, args=(proc, label, reduce_results))
        t.start()
        reduce_threads.append(t)

    for t in map_threads + reduce_threads:
        t.join()

    total_time   = time.time() - total_start
    final_result = merge_results(num_reducers, output_dir)
    display_results(final_result, args.top, total_time, output_dir)

    all_ok = all(v == 0 for v in {**map_results, **reduce_results}.values())
    print(f"\n[COORD] {'✓ Toutes les tâches réussies !' if all_ok else '⚠️  Certaines tâches ont échoué.'}")


# ─────────────────────────────────────────────
# MODE DOCKER
# ─────────────────────────────────────────────

def run_docker_mode(args):
    """
    Mode Docker : les workers sont déjà lancés par docker-compose.
    Le coordinator attend que les fichiers résultats apparaissent dans OUTPUT_DIR.
    """
    num_reducers = args.reducers
    output_dir   = OUTPUT_DIR

    print(f"\n[COORD] Configuration (mode docker) :")
    print(f"  - Reducers : {num_reducers}")
    print(f"  - Sortie   : {output_dir}/")
    print(f"\n[COORD] Workers lancés par docker-compose. Attente des résultats...\n")

    clean_output(output_dir)
    total_start = time.time()

    while True:
        done   = [os.path.exists(os.path.join(output_dir, f"result_reducer_{i}.json"))
                  for i in range(num_reducers)]
        nb_done = sum(done)
        print(f"  [COORD] Reducers terminés : {nb_done}/{num_reducers}", end="\r")
        if all(done):
            print(f"\n[COORD] ✓ Tous les reducers ont terminé !")
            break
        time.sleep(1)

    total_time   = time.time() - total_start
    final_result = merge_results(num_reducers, output_dir)
    display_results(final_result, args.top, total_time, output_dir)
    print(f"\n[COORD] ✓ Cluster terminé en {total_time:.2f}s")


# ─────────────────────────────────────────────
# POINT D'ENTRÉE
# ─────────────────────────────────────────────

def main():
    args = parse_args()
    print("\n" + "=" * 60)
    print(f"  PLATEFORME MAP-REDUCE — Mode : {args.mode.upper()}")
    print("=" * 60)

    if args.mode == "docker":
        run_docker_mode(args)
    else:
        run_local_mode(args)


if __name__ == "__main__":
    main()
