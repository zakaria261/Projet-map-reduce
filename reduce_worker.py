"""
reduce_worker.py — Worker REDUCE

Rôle : Agréger les comptages d'un sous-ensemble de mots provenant de TOUS les MAP.

Fonctionnement :
1. Se connecte à chaque worker MAP via get_map_address() (local, Docker ou LAN)
2. Demande les mots dont il est responsable (phase SHUFFLE)
3. Additionne les comptages de la même clé venant de sources différentes
4. Sauvegarde son résultat dans OUTPUT_DIR/result_reducer_X.json

Usage :
    python reduce_worker.py <reducer_id> <num_mappers> <num_reducers>

Variables d'environnement :
    MAP_HOSTS        : "192.168.1.42:6000,192.168.1.43:6001" (mode multi-machine)
    MAP_HOST_PREFIX  : "map-" (mode Docker Compose)
    OUTPUT_DIR       : dossier de sortie (défaut: ".")
"""

import socket
import sys
import json
import os
import time
from utils import get_map_address, OUTPUT_DIR, send_data, recv_data


def run_reduce_worker(reducer_id, num_mappers, num_reducers):
    """Fonction principale du worker REDUCE."""
    print(f"\n{'='*50}")
    print(f"[REDUCE {reducer_id}] Démarrage")
    print(f"{'='*50}")

    total_word_count = {}
    start = time.time()

    # ── Phase SHUFFLE : collecter les données depuis chaque MAP ──────────
    for map_id in range(num_mappers):
        map_host, map_port = get_map_address(map_id)
        print(f"  [REDUCE {reducer_id}] Connexion au MAP {map_id} ({map_host}:{map_port})...")

        # Retry avec backoff — le MAP peut mettre du temps à démarrer
        connected = False
        for attempt in range(20):  # 20 tentatives × 1s = 20s max
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(15.0)  # timeout par opération
                sock.connect((map_host, map_port))
                connected = True
                break
            except (ConnectionRefusedError, OSError):
                print(f"  [REDUCE {reducer_id}] MAP {map_id} pas prêt, retry {attempt+1}/20...")
                time.sleep(1.0)

        if not connected:
            print(f"  [REDUCE {reducer_id}] ERREUR : impossible de joindre MAP {map_id} après 20 tentatives")
            continue

        # Envoyer la demande : "donne-moi les mots dont je suis responsable"
        send_data(sock, {"reducer_id": reducer_id})

        # Recevoir les mots
        response = recv_data(sock)
        sock.close()

        if response is None:
            print(f"  [REDUCE {reducer_id}] ERREUR : réponse vide du MAP {map_id}")
            continue

        words_from_map = response.get("words", {})
        print(f"  [REDUCE {reducer_id}] ← MAP {map_id} ({map_host}:{map_port}) : {len(words_from_map)} mots reçus")

        # ── Phase REDUCE : additionner les comptages ─────────────────────
        for word, count in words_from_map.items():
            total_word_count[word] = total_word_count.get(word, 0) + count

    elapsed = time.time() - start

    # ── Tri du résultat par occurrences décroissantes ────────────────────
    sorted_result = dict(
        sorted(total_word_count.items(), key=lambda x: x[1], reverse=True)
    )

    # ── Sauvegarde dans OUTPUT_DIR ───────────────────────────────────────
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_file = os.path.join(OUTPUT_DIR, f"result_reducer_{reducer_id}.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(sorted_result, f, ensure_ascii=False, indent=2)

    print(f"\n  [REDUCE {reducer_id}] Terminé en {elapsed:.3f}s")
    print(f"  [REDUCE {reducer_id}] {len(sorted_result)} mots distincts traités")
    print(f"  [REDUCE {reducer_id}] Résultat → '{output_file}'")

    top5 = list(sorted_result.items())[:5]
    print(f"  [REDUCE {reducer_id}] Top 5 : {top5}")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage : python reduce_worker.py <reducer_id> <num_mappers> <num_reducers>")
        sys.exit(1)

    reducer_id   = int(sys.argv[1])
    num_mappers  = int(sys.argv[2])
    num_reducers = int(sys.argv[3])

    run_reduce_worker(reducer_id, num_mappers, num_reducers)
