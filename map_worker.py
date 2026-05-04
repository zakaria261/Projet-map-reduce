"""
map_worker.py — Worker MAP

Rôle : compter les mots dans UN fichier texte, puis servir les résultats
       aux workers REDUCE via un socket TCP.

Étapes :
  1. Lit le fichier, nettoie et compte les mots
  2. Ouvre un serveur socket (port 6000 + worker_id)
  3. Répond aux requêtes des reducers (phase SHUFFLE)

Usage :
    python map_worker.py <worker_id> <filepath> <num_reducers>
    python map_worker.py 0 texts/text1.txt 2
"""

import socket
import threading
import sys
import re
import time
from utils import BIND_HOST, MAP_BASE_PORT, send_data, recv_data, get_reducer_for_word


# Mots trop fréquents et non informatifs (français + anglais)
STOPWORDS = {
    "le", "la", "les", "de", "du", "des", "un", "une", "et", "en",
    "au", "aux", "est", "il", "elle", "ils", "elles", "que", "qui",
    "se", "sa", "son", "sur", "par", "pas", "ne", "je", "tu", "nous",
    "vous", "on", "ce", "si", "ou", "mais", "donc", "car",
    "the", "a", "an", "of", "to", "in", "is", "it", "and", "for",
    "on", "that", "with", "as", "at", "be", "by", "this", "was", "are"
}


def count_words(filepath):
    """Lit un fichier et retourne {mot: nb_occurrences} (stopwords exclus)."""
    word_count = {}
    print(f"  [MAP] Lecture : {filepath}")
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                for word in re.findall(r"[a-zA-ZÀ-ÿ]+", line.lower()):
                    if len(word) > 2 and word not in STOPWORDS:
                        word_count[word] = word_count.get(word, 0) + 1
    except FileNotFoundError:
        print(f"  [MAP] ERREUR : fichier '{filepath}' introuvable !")
        return {}

    print(f"  [MAP] {len(word_count)} mots distincts, {sum(word_count.values())} occurrences")
    return word_count


def handle_reduce_request(conn, addr, word_count, worker_id, num_reducers):
    """Répond à la requête d'un reducer : envoie les mots dont il est responsable."""
    try:
        request    = recv_data(conn)
        reducer_id = request["reducer_id"]
        subset     = {w: c for w, c in word_count.items()
                      if get_reducer_for_word(w, num_reducers) == reducer_id}
        print(f"  [MAP {worker_id}] → Reducer {reducer_id} : {len(subset)} mots envoyés")
        send_data(conn, {"words": subset})
    except Exception as e:
        print(f"  [MAP {worker_id}] Erreur : {e}")
    finally:
        conn.close()


def run_map_worker(worker_id, filepath, num_reducers):
    """Fonction principale du worker MAP."""
    print(f"\n{'='*50}")
    print(f"[MAP {worker_id}] Démarrage — {filepath}")
    print(f"{'='*50}")

    start      = time.time()
    word_count = count_words(filepath)
    print(f"  [MAP {worker_id}] Phase MAP : {time.time()-start:.3f}s")

    port   = MAP_BASE_PORT + worker_id
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((BIND_HOST, port))
    server.listen(5)
    print(f"  [MAP {worker_id}] Écoute sur {BIND_HOST}:{port} — attend {num_reducers} reducers")

    threads, handled = [], 0
    while handled < num_reducers:
        conn, addr = server.accept()
        t = threading.Thread(target=handle_reduce_request,
                             args=(conn, addr, word_count, worker_id, num_reducers))
        t.start()
        threads.append(t)
        handled += 1

    for t in threads:
        t.join()

    server.close()
    print(f"  [MAP {worker_id}] Terminé.")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage : python map_worker.py <worker_id> <filepath> <num_reducers>")
        sys.exit(1)
    run_map_worker(int(sys.argv[1]), sys.argv[2], int(sys.argv[3]))
