"""
coordinator.py — Nœud coordinateur (chef d'orchestre)

Rôle : Lancer et gérer tous les workers MAP et REDUCE.

Fonctionnement :
1. Découverte des fichiers texte dans le dossier "texts/"
2. Lancement en parallèle des workers MAP (un par fichier, dans des processus séparés)
3. Attente de la fin des MAP (via des threads de monitoring)
4. Lancement en parallèle des workers REDUCE
5. Attente de la fin des REDUCE
6. Fusion des résultats partiels en un résultat global
7. Affichage du top des mots les plus fréquents + temps de traitement

Usage :
    python coordinator.py [--mappers N] [--reducers M] [--input-dir DIR]

Exemples :
    python coordinator.py
    python coordinator.py --mappers 3 --reducers 2
    python coordinator.py --input-dir mes_textes/ --reducers 4
"""

import subprocess
import threading
import sys
import os
import json
import time
import argparse
import glob


# ─────────────────────────────────────────────
# GESTION DES ARGUMENTS EN LIGNE DE COMMANDE
# ─────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Coordinateur Map-Reduce distribué (simulation avec sockets)"
    )
    parser.add_argument(
        "--reducers", type=int, default=2,
        help="Nombre de workers REDUCE (défaut: 2)"
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
# LANCEMENT D'UN PROCESSUS WORKER
# ─────────────────────────────────────────────

def launch_worker(script, args_list, label):
    """
    Lance un worker dans un sous-processus Python séparé.

    Pourquoi des sous-processus ?
    - Simulation réaliste d'une vraie distribution (chaque worker = machine indépendante)
    - Isolation mémoire (le coordinateur ne partage pas la RAM avec les workers)
    - En production, on remplacerait subprocess par SSH + exécution distante

    Retourne le processus (objet subprocess.Popen).
    """
    cmd = [sys.executable, script] + [str(a) for a in args_list]
    print(f"  [COORD] Lancement : {' '.join(cmd)}")

    # stdout=None, stderr=None → les sorties s'affichent directement dans le terminal
    proc = subprocess.Popen(cmd)
    return proc


def wait_for_process(proc, label, results_dict):
    """
    Attend la fin d'un processus et enregistre son code de retour.
    Utilisé dans des threads pour surveiller plusieurs workers en parallèle.
    """
    proc.wait()
    results_dict[label] = proc.returncode
    if proc.returncode != 0:
        print(f"  [COORD] ⚠️  {label} s'est terminé avec code d'erreur {proc.returncode}")
    else:
        print(f"  [COORD] ✓  {label} terminé avec succès")


# ─────────────────────────────────────────────
# FUSION DES RÉSULTATS
# ─────────────────────────────────────────────

def merge_results(num_reducers, top_n):
    """
    Lit les fichiers JSON produits par chaque REDUCE et les fusionne.

    Chaque REDUCE a produit un fichier result_reducer_X.json contenant
    sa partie du dictionnaire final. On les concatène simplement.
    """
    print("\n" + "─"*50)
    print("[COORD] Fusion des résultats partiels...")

    merged = {}
    for r_id in range(num_reducers):
        filename = f"result_reducer_{r_id}.json"
        if not os.path.exists(filename):
            print(f"  [COORD] ⚠️  Fichier manquant : {filename}")
            continue
        with open(filename, "r", encoding="utf-8") as f:
            partial = json.load(f)
        # Normalement les reducers ont des mots distincts (partitionnement)
        # mais on additionne par sécurité
        for word, count in partial.items():
            merged[word] = merged.get(word, 0) + count
        print(f"  [COORD] Fusionné {filename} ({len(partial)} mots)")

    # Trier par ordre décroissant d'occurrences
    sorted_result = dict(
        sorted(merged.items(), key=lambda x: x[1], reverse=True)
    )

    # Sauvegarde du résultat global
    with open("result_final.json", "w", encoding="utf-8") as f:
        json.dump(sorted_result, f, ensure_ascii=False, indent=2)

    return sorted_result


# ─────────────────────────────────────────────
# PROGRAMME PRINCIPAL
# ─────────────────────────────────────────────

def main():
    args = parse_args()

    print("\n" + "="*60)
    print("  PLATEFORME MAP-REDUCE DISTRIBUÉE (simulation Python)")
    print("="*60)

    # ── 1. Découverte des fichiers texte ─────────────────────────────────
    input_dir = args.input_dir
    if not os.path.isdir(input_dir):
        print(f"[COORD] ERREUR : le dossier '{input_dir}' n'existe pas.")
        print(f"[COORD] Créez le dossier et ajoutez des fichiers .txt")
        sys.exit(1)

    text_files = sorted(glob.glob(os.path.join(input_dir, "*.txt")))
    if not text_files:
        print(f"[COORD] ERREUR : aucun fichier .txt trouvé dans '{input_dir}'")
        sys.exit(1)

    num_mappers = len(text_files)
    num_reducers = args.reducers

    print(f"\n[COORD] Configuration :")
    print(f"  - Fichiers trouvés : {num_mappers}")
    for i, f in enumerate(text_files):
        print(f"      MAP {i} → {f}")
    print(f"  - Workers REDUCE  : {num_reducers}")
    print(f"  - Top N affiché   : {args.top}")

    total_start = time.time()

    # ── 2. Lancement des workers MAP ─────────────────────────────────────
    print("\n" + "─"*50)
    print("[COORD] Phase MAP — lancement des workers...")
    print("─"*50)

    map_procs = []
    for i, filepath in enumerate(text_files):
        proc = launch_worker(
            script="map_worker.py",
            args_list=[i, filepath, num_reducers],
            label=f"MAP-{i}"
        )
        map_procs.append((f"MAP-{i}", proc))

    # Surveiller les MAP dans des threads (pour les logs temps réel)
    map_results = {}
    map_threads = []
    for label, proc in map_procs:
        t = threading.Thread(target=wait_for_process, args=(proc, label, map_results))
        t.start()
        map_threads.append(t)

    # Attendre que TOUS les MAP aient démarré leur serveur socket
    # (On attend un peu pour laisser le temps aux MAP de bind() leur socket)
    # En production, on utiliserait un mécanisme de "ready signal"
    print(f"\n[COORD] En attente que les {num_mappers} workers MAP soient prêts...")
    time.sleep(2)  # Délai simple — à remplacer par un vrai handshake en production

    map_phase_time = time.time()

    # ── 3. Lancement des workers REDUCE ──────────────────────────────────
    print("\n" + "─"*50)
    print("[COORD] Phase REDUCE + SHUFFLE — lancement des workers...")
    print("─"*50)

    reduce_procs = []
    for r_id in range(num_reducers):
        proc = launch_worker(
            script="reduce_worker.py",
            args_list=[r_id, num_mappers, num_reducers],
            label=f"REDUCE-{r_id}"
        )
        reduce_procs.append((f"REDUCE-{r_id}", proc))

    # Surveiller les REDUCE
    reduce_results = {}
    reduce_threads = []
    for label, proc in reduce_procs:
        t = threading.Thread(target=wait_for_process, args=(proc, label, reduce_results))
        t.start()
        reduce_threads.append(t)

    # Attendre la fin de TOUS les MAP (les MAP attendent eux-mêmes d'avoir servi tous les REDUCE)
    for t in map_threads:
        t.join()

    reduce_phase_time = time.time()

    # Attendre la fin de TOUS les REDUCE
    for t in reduce_threads:
        t.join()

    total_time = time.time() - total_start

    # ── 4. Fusion et affichage des résultats ─────────────────────────────
    final_result = merge_results(num_reducers, args.top)

    print("\n" + "="*60)
    print(f"  RÉSULTATS FINAUX — Top {args.top} mots")
    print("="*60)

    top_words = list(final_result.items())[:args.top]
    max_count = top_words[0][1] if top_words else 1

    for rank, (word, count) in enumerate(top_words, 1):
        # Barre visuelle proportionnelle
        bar_len = int((count / max_count) * 30)
        bar = "█" * bar_len
        print(f"  {rank:3}. {word:<20} {count:6}  {bar}")

    print("\n" + "─"*50)
    print(f"[COORD] Statistiques :")
    print(f"  - Mots distincts totaux  : {len(final_result)}")
    print(f"  - Temps total            : {total_time:.3f}s")
    print(f"  - Résultat sauvegardé dans 'result_final.json'")

    # Vérification des erreurs
    all_ok = all(v == 0 for v in {**map_results, **reduce_results}.values())
    if all_ok:
        print(f"\n[COORD] ✓ Toutes les tâches se sont terminées avec succès !")
    else:
        print(f"\n[COORD] ⚠️  Certaines tâches ont signalé des erreurs.")


if __name__ == "__main__":
    main()
