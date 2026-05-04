"""
coordinator.py — Nœud coordinateur

Deux modes de fonctionnement :

  --mode local  (défaut) : lance les workers comme des sous-processus Python
                           (simulation sur une seule machine)

  --mode docker          : les workers sont déjà lancés par docker-compose.
                           Le coordinator attend que les fichiers résultats
                           apparaissent dans OUTPUT_DIR, puis fusionne et affiche.

Usage :
    python coordinator.py                          # local, 3 fichiers, 2 reducers
    python coordinator.py --reducers 4 --top 30
    python coordinator.py --mode docker --reducers 2 --mappers 3
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
# ARGUMENTS
# ─────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Coordinateur Map-Reduce (local ou Docker)"
    )
    parser.add_argument(
        "--mode", type=str, default="local", choices=["local", "docker"],
        help="Mode d'exécution : 'local' (subprocess) ou 'docker' (containers déjà lancés)"
    )
    parser.add_argument(
        "--reducers", type=int, default=2,
        help="Nombre de workers REDUCE (défaut: 2)"
    )
    parser.add_argument(
        "--mappers", type=int, default=None,
        help="Nombre de workers MAP — requis en mode docker"
    )
    parser.add_argument(
        "--input-dir", type=str, default="texts",
        help="Dossier contenant les fichiers .txt (défaut: texts/)"
    )
    parser.add_argument(
        "--top", type=int, default=20,
        help="Nombre de mots à afficher dans le résultat final (défaut: 20)"
    )
    return parser.parse_args()


# ─────────────────────────────────────────────
# UTILITAIRES COMMUNS
# ─────────────────────────────────────────────

def launch_worker(script, args_list, label):
    """Lance un worker comme sous-processus Python (mode local uniquement)."""
    cmd = [sys.executable, script] + [str(a) for a in args_list]
    print(f"  [COORD] Lancement : {' '.join(cmd)}")
    proc = subprocess.Popen(cmd)
    return proc


def wait_for_process(proc, label, results_dict):
    """Surveille un processus et enregistre son code de retour."""
    proc.wait()
    results_dict[label] = proc.returncode
    if proc.returncode != 0:
        print(f"  [COORD] ⚠️  {label} — code d'erreur {proc.returncode}")
    else:
        print(f"  [COORD] ✓  {label} terminé avec succès")


def merge_results(num_reducers, top_n, output_dir=None):
    """
    Lit les fichiers JSON produits par chaque REDUCE et les fusionne.
    Sauvegarde le résultat global dans OUTPUT_DIR/result_final.json.
    """
    if output_dir is None:
        output_dir = OUTPUT_DIR

    print("\n" + "─"*50)
    print("[COORD] Fusion des résultats partiels...")

    merged = {}
    for r_id in range(num_reducers):
        filename = os.path.join(output_dir, f"result_reducer_{r_id}.json")
        if not os.path.exists(filename):
            print(f"  [COORD] ⚠️  Fichier manquant : {filename}")
            continue
        with open(filename, "r", encoding="utf-8") as f:
            partial = json.load(f)
        for word, count in partial.items():
            merged[word] = merged.get(word, 0) + count
        print(f"  [COORD] Fusionné {filename} ({len(partial)} mots)")

    sorted_result = dict(sorted(merged.items(), key=lambda x: x[1], reverse=True))

    os.makedirs(output_dir, exist_ok=True)
    final_path = os.path.join(output_dir, "result_final.json")
    with open(final_path, "w", encoding="utf-8") as f:
        json.dump(sorted_result, f, ensure_ascii=False, indent=2)

    return sorted_result


def display_results(final_result, top_n, total_time, output_dir):
    """Affiche le Top N et les statistiques finales."""
    print("\n" + "="*60)
    print(f"  RÉSULTATS FINAUX — Top {top_n} mots")
    print("="*60)

    top_words = list(final_result.items())[:top_n]
    max_count = top_words[0][1] if top_words else 1

    for rank, (word, count) in enumerate(top_words, 1):
        bar_len = int((count / max_count) * 30)
        bar     = "█" * bar_len
        print(f"  {rank:3}. {word:<20} {count:6}  {bar}")

    final_path = os.path.join(output_dir, "result_final.json")
    print("\n" + "─"*50)
    print(f"[COORD] Mots distincts totaux  : {len(final_result)}")
    print(f"[COORD] Temps total            : {total_time:.3f}s")
    print(f"[COORD] Résultat sauvegardé    : '{final_path}'")


# ─────────────────────────────────────────────
# MODE LOCAL (sous-processus)
# ─────────────────────────────────────────────

def run_local_mode(args):
    """Lance tous les workers comme des sous-processus (mode développement)."""
    input_dir = args.input_dir
    if not os.path.isdir(input_dir):
        print(f"[COORD] ERREUR : le dossier '{input_dir}' n'existe pas.")
        sys.exit(1)

    text_files = sorted(glob.glob(os.path.join(input_dir, "*.txt")))
    if not text_files:
        print(f"[COORD] ERREUR : aucun fichier .txt trouvé dans '{input_dir}'")
        sys.exit(1)

    num_mappers  = len(text_files)
    num_reducers = args.reducers

    print(f"\n[COORD] Configuration (mode local) :")
    print(f"  - Fichiers trouvés : {num_mappers}")
    for i, f in enumerate(text_files):
        print(f"      MAP {i} → {f}")
    print(f"  - Workers REDUCE  : {num_reducers}")
    print(f"  - Top N affiché   : {args.top}")
    print(f"  - Dossier sortie  : {OUTPUT_DIR}/")

    # Nettoyage des anciens résultats
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    for old in glob.glob(os.path.join(OUTPUT_DIR, "result_*.json")):
        os.remove(old)

    total_start = time.time()

    # ── 1. Phase MAP ────────────────────────────────────────────────────
    print("\n" + "─"*50)
    print("[COORD] Phase MAP — lancement des workers...")

    map_procs = []
    for i, filepath in enumerate(text_files):
        proc = launch_worker("map_worker.py", [i, filepath, num_reducers], f"MAP-{i}")
        map_procs.append((f"MAP-{i}", proc))

    map_results = {}
    map_threads = []
    for label, proc in map_procs:
        t = threading.Thread(target=wait_for_process, args=(proc, label, map_results))
        t.start()
        map_threads.append(t)

    print(f"\n[COORD] Attente que les {num_mappers} workers MAP soient prêts...")
    time.sleep(2)

    # ── 2. Phase REDUCE ─────────────────────────────────────────────────
    print("\n" + "─"*50)
    print("[COORD] Phase REDUCE + SHUFFLE — lancement des workers...")

    reduce_procs = []
    for r_id in range(num_reducers):
        proc = launch_worker("reduce_worker.py", [r_id, num_mappers, num_reducers], f"REDUCE-{r_id}")
        reduce_procs.append((f"REDUCE-{r_id}", proc))

    reduce_results = {}
    reduce_threads = []
    for label, proc in reduce_procs:
        t = threading.Thread(target=wait_for_process, args=(proc, label, reduce_results))
        t.start()
        reduce_threads.append(t)

    for t in map_threads:
        t.join()
    for t in reduce_threads:
        t.join()

    total_time   = time.time() - total_start
    final_result = merge_results(num_reducers, args.top)
    display_results(final_result, args.top, total_time, OUTPUT_DIR)

    all_ok = all(v == 0 for v in {**map_results, **reduce_results}.values())
    if all_ok:
        print(f"\n[COORD] ✓ Toutes les tâches terminées avec succès !")
    else:
        print(f"\n[COORD] ⚠️  Certaines tâches ont signalé des erreurs.")


# ─────────────────────────────────────────────
# MODE DOCKER (containers déjà lancés)
# ─────────────────────────────────────────────

def run_docker_mode(args):
    """
    Mode Docker : les workers sont déjà lancés par docker-compose.
    Le coordinator attend que les fichiers résultats apparaissent,
    puis fusionne et affiche.
    """
    num_reducers = args.reducers
    output_dir   = OUTPUT_DIR

    print(f"\n[COORD] Configuration (mode docker) :")
    print(f"  - Workers REDUCE  : {num_reducers}")
    print(f"  - Dossier sortie  : {output_dir}/")
    print(f"  - Top N affiché   : {args.top}")
    print(f"\n[COORD] Workers déjà lancés par docker-compose.")
    print(f"[COORD] Attente des {num_reducers} fichiers résultats...\n")

    # Nettoyage des anciens résultats AVANT d'attendre
    os.makedirs(output_dir, exist_ok=True)
    for old in glob.glob(os.path.join(output_dir, "result_reducer_*.json")):
        os.remove(old)
        print(f"  [COORD] Ancien fichier supprimé : {old}")

    total_start = time.time()

    # Attendre que tous les reducers aient écrit leurs résultats
    while True:
        done = [
            os.path.exists(os.path.join(output_dir, f"result_reducer_{i}.json"))
            for i in range(num_reducers)
        ]
        nb_done = sum(done)
        print(f"  [COORD] Reducers terminés : {nb_done}/{num_reducers}", end="\r")

        if all(done):
            print(f"\n[COORD] ✓ Tous les reducers ont terminé !")
            break
        time.sleep(1)

    total_time   = time.time() - total_start
    final_result = merge_results(num_reducers, args.top, output_dir)
    display_results(final_result, args.top, total_time, output_dir)
    print(f"\n[COORD] ✓ Cluster terminé en {total_time:.2f}s")


# ─────────────────────────────────────────────
# POINT D'ENTRÉE
# ─────────────────────────────────────────────

def main():
    args = parse_args()

    print("\n" + "="*60)
    print("  PLATEFORME MAP-REDUCE DISTRIBUÉE")
    print(f"  Mode : {args.mode.upper()}")
    print("="*60)

    if args.mode == "docker":
        run_docker_mode(args)
    else:
        run_local_mode(args)


if __name__ == "__main__":
    main()
