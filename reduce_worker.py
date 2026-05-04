"""
reduce_worker.py — Worker REDUCE

Rôle : Agréger les comptages d'un sous-ensemble de mots provenant de TOUS les workers MAP.

Fonctionnement :
1. Se connecte à chaque worker MAP et demande "donne-moi les mots qui me concernent"
2. Additionne les comptages du même mot venant de sources différentes
   Ex: MAP0 dit {"bonjour": 2}, MAP1 dit {"bonjour": 1} → total : {"bonjour": 3}
3. Sauvegarde son résultat partiel dans un fichier JSON

Usage :
    python reduce_worker.py <reducer_id> <num_mappers> <num_reducers>
    python reduce_worker.py 0 3 2
"""

import socket
import sys
import json
import time
from utils import HOST, MAP_BASE_PORT, REDUCE_BASE_PORT, send_data, recv_data


def run_reduce_worker(reducer_id, num_mappers, num_reducers):
    """
    Fonction principale du worker REDUCE.

    Paramètres :
    - reducer_id   : identifiant de CE reducer (0, 1, 2, ...)
    - num_mappers  : nombre total de workers MAP (on va se connecter à chacun)
    - num_reducers : nombre total de reducers (pour la logique de partitionnement)
    """
    print(f"\n{'='*50}")
    print(f"[REDUCE {reducer_id}] Démarrage")
    print(f"{'='*50}")

    total_word_count = {}  # Dictionnaire final de ce reducer
    start = time.time()

    # ── Phase SHUFFLE : collecter les données depuis chaque MAP ──────────
    for map_id in range(num_mappers):
        map_port = MAP_BASE_PORT + map_id
        print(f"  [REDUCE {reducer_id}] Connexion au MAP {map_id} (port {map_port})...")

        # Tentatives de connexion avec retry (le MAP peut mettre un peu de temps à démarrer)
        connected = False
        for attempt in range(10):  # Max 10 tentatives
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((HOST, map_port))
                connected = True
                break
            except ConnectionRefusedError:
                print(f"  [REDUCE {reducer_id}] MAP {map_id} pas encore prêt, retry {attempt+1}/10...")
                time.sleep(0.5)

        if not connected:
            print(f"  [REDUCE {reducer_id}] ERREUR : impossible de joindre MAP {map_id}")
            continue

        # Envoyer ma demande : "donne-moi tes mots dont je suis responsable"
        send_data(sock, {"reducer_id": reducer_id})

        # Recevoir les mots
        response = recv_data(sock)
        sock.close()

        if response is None:
            print(f"  [REDUCE {reducer_id}] ERREUR : réponse vide du MAP {map_id}")
            continue

        words_from_map = response.get("words", {})
        print(f"  [REDUCE {reducer_id}] ← MAP {map_id} : {len(words_from_map)} mots reçus")

        # ── Phase REDUCE : additionner les comptages ─────────────────────
        # Pour chaque mot reçu de ce MAP, on l'ajoute au total
        for word, count in words_from_map.items():
            total_word_count[word] = total_word_count.get(word, 0) + count

    elapsed = time.time() - start

    # ── Tri du résultat par ordre décroissant d'occurrences ──────────────
    sorted_result = dict(
        sorted(total_word_count.items(), key=lambda x: x[1], reverse=True)
    )

    # ── Sauvegarde du résultat partiel ───────────────────────────────────
    output_file = f"result_reducer_{reducer_id}.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(sorted_result, f, ensure_ascii=False, indent=2)

    print(f"\n  [REDUCE {reducer_id}] Terminé en {elapsed:.3f}s")
    print(f"  [REDUCE {reducer_id}] {len(sorted_result)} mots distincts traités")
    print(f"  [REDUCE {reducer_id}] Résultat sauvegardé dans '{output_file}'")

    # Afficher les 5 mots les plus fréquents pour ce reducer
    top5 = list(sorted_result.items())[:5]
    print(f"  [REDUCE {reducer_id}] Top 5 : {top5}")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage : python reduce_worker.py <reducer_id> <num_mappers> <num_reducers>")
        sys.exit(1)

    reducer_id = int(sys.argv[1])
    num_mappers = int(sys.argv[2])
    num_reducers = int(sys.argv[3])

    run_reduce_worker(reducer_id, num_mappers, num_reducers)
