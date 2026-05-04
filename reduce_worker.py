"""
reduce_worker.py — Worker REDUCE

Rôle : agréger les comptages d'un sous-ensemble de mots venant de TOUS les MAPs.

Étapes :
  1. Se connecte à chaque worker MAP et demande ses mots (phase SHUFFLE)
  2. Additionne les comptages du même mot depuis plusieurs sources
  3. Sauvegarde le résultat dans output/result_reducer_X.json

Usage :
    python reduce_worker.py <reducer_id> <num_mappers> <num_reducers>
    python reduce_worker.py 0 3 2
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

    # ── SHUFFLE : récupérer les mots depuis chaque MAP ───────────
    for map_id in range(num_mappers):
        host, port = get_map_address(map_id)
        print(f"  [REDUCE {reducer_id}] Connexion → MAP {map_id} ({host}:{port})")

        # Retry avec backoff exponentiel (le MAP peut encore démarrer)
        connected = False
        for attempt in range(20):
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(15.0)
                sock.connect((host, port))
                connected = True
                break
            except (ConnectionRefusedError, OSError):
                print(f"  [REDUCE {reducer_id}] MAP {map_id} pas prêt ({attempt+1}/20)...")
                time.sleep(1.0)

        if not connected:
            print(f"  [REDUCE {reducer_id}] ⚠️  Impossible de joindre MAP {map_id}")
            continue

        send_data(sock, {"reducer_id": reducer_id})
        response = recv_data(sock)
        sock.close()

        if response is None:
            print(f"  [REDUCE {reducer_id}] ⚠️  Réponse vide du MAP {map_id}")
            continue

        words = response.get("words", {})
        print(f"  [REDUCE {reducer_id}] ← MAP {map_id} : {len(words)} mots reçus")

        # ── REDUCE : additionner les comptages ───────────────────
        for word, count in words.items():
            total_word_count[word] = total_word_count.get(word, 0) + count

    # ── Tri et sauvegarde ────────────────────────────────────────
    sorted_result = dict(sorted(total_word_count.items(), key=lambda x: x[1], reverse=True))

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_file = os.path.join(OUTPUT_DIR, f"result_reducer_{reducer_id}.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(sorted_result, f, ensure_ascii=False, indent=2)

    elapsed = time.time() - start
    print(f"\n  [REDUCE {reducer_id}] Terminé en {elapsed:.3f}s — {len(sorted_result)} mots")
    print(f"  [REDUCE {reducer_id}] Top 5 : {list(sorted_result.items())[:5]}")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage : python reduce_worker.py <reducer_id> <num_mappers> <num_reducers>")
        sys.exit(1)
    run_reduce_worker(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]))
