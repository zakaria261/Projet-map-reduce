"""
map_worker.py — Worker MAP

Rôle : Compter les occurrences de chaque mot dans UN fichier texte,
       puis servir les résultats aux workers REDUCE via socket TCP.

Fonctionnement :
1. Reçoit le chemin du fichier texte et sa configuration
2. Lit le fichier, nettoie les mots (minuscules, sans ponctuation)
3. Compte les occurrences : {"bonjour": 3, "monde": 2, ...}
4. Ouvre un serveur socket sur BIND_HOST (0.0.0.0 pour Docker)
   et attend que les workers REDUCE viennent demander leurs mots

Usage :
    python map_worker.py <worker_id> <filepath> <num_reducers>
    python map_worker.py 0 texts/text1.txt 2

Variables d'environnement :
    BIND_HOST : adresse d'écoute (défaut: "0.0.0.0", compatible Docker)
"""

import socket
import threading
import sys
import re
import time
from utils import BIND_HOST, MAP_BASE_PORT, send_data, recv_data, get_reducer_for_word


# Mots vides français/anglais à ignorer (trop fréquents, pas informatifs)
STOPWORDS = {
    "le", "la", "les", "de", "du", "des", "un", "une", "et", "en",
    "à", "au", "aux", "est", "il", "elle", "ils", "elles", "que", "qui",
    "se", "sa", "son", "sur", "par", "pas", "ne", "je", "tu", "nous",
    "vous", "on", "ce", "si", "ou", "mais", "donc", "or", "ni", "car",
    "the", "a", "an", "of", "to", "in", "is", "it", "and", "for", "on",
    "that", "with", "as", "at", "be", "by", "this", "was", "are", "from"
}


def count_words(filepath):
    """
    Lit un fichier texte et retourne un dictionnaire {mot: nb_occurrences}.

    Nettoyage appliqué :
    - Conversion en minuscules
    - Extraction des séquences de lettres (accents inclus)
    - Filtrage des stopwords et mots de moins de 3 caractères
    """
    word_count = {}

    print(f"  [MAP] Lecture du fichier : {filepath}")
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                words = re.findall(r"[a-zA-ZÀ-ÿ]+", line.lower())
                for word in words:
                    if len(word) > 2 and word not in STOPWORDS:
                        word_count[word] = word_count.get(word, 0) + 1
    except FileNotFoundError:
        print(f"  [MAP] ERREUR : fichier '{filepath}' introuvable !")
        return {}

    total = sum(word_count.values())
    print(f"  [MAP] Comptage terminé : {len(word_count)} mots distincts, {total} occurrences")
    return word_count


def handle_reduce_request(conn, addr, word_count, worker_id, num_reducers):
    """
    Gère la connexion d'UN worker REDUCE.

    Protocole :
    - Le REDUCE envoie  {"reducer_id": X}
    - Le MAP renvoie    {"words": {"bonjour": 3, ...}}
    """
    try:
        request    = recv_data(conn)
        reducer_id = request["reducer_id"]

        subset = {
            word: count
            for word, count in word_count.items()
            if get_reducer_for_word(word, num_reducers) == reducer_id
        }

        print(f"  [MAP {worker_id}] → Reducer {reducer_id} : {len(subset)} mots envoyés")
        send_data(conn, {"words": subset})

    except Exception as e:
        print(f"  [MAP {worker_id}] Erreur communication reducer : {e}")
    finally:
        conn.close()


def run_map_worker(worker_id, filepath, num_reducers):
    """Fonction principale du worker MAP."""
    print(f"\n{'='*50}")
    print(f"[MAP {worker_id}] Démarrage — fichier : {filepath}")
    print(f"{'='*50}")

    # ── Phase MAP : compter les mots ──────────────────────
    start      = time.time()
    word_count = count_words(filepath)
    elapsed    = time.time() - start
    print(f"  [MAP {worker_id}] Phase MAP terminée en {elapsed:.3f}s")

    # ── Phase SHUFFLE : servir les reducers via socket ────
    port   = MAP_BASE_PORT + worker_id
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((BIND_HOST, port))   # 0.0.0.0 → visible depuis Docker/LAN
    server.listen(5)

    print(f"  [MAP {worker_id}] En écoute sur {BIND_HOST}:{port}, attente de {num_reducers} reducers...")

    connections_handled = 0
    threads = []
    while connections_handled < num_reducers:
        conn, addr = server.accept()
        print(f"  [MAP {worker_id}] Connexion reçue de {addr}")
        t = threading.Thread(
            target=handle_reduce_request,
            args=(conn, addr, word_count, worker_id, num_reducers)
        )
        t.start()
        threads.append(t)
        connections_handled += 1

    for t in threads:
        t.join()

    server.close()
    print(f"  [MAP {worker_id}] Toutes les données envoyées. Terminé.")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage : python map_worker.py <worker_id> <filepath> <num_reducers>")
        sys.exit(1)

    worker_id   = int(sys.argv[1])
    filepath    = sys.argv[2]
    num_reducers = int(sys.argv[3])

    run_map_worker(worker_id, filepath, num_reducers)
