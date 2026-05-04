"""
map_worker.py — Worker MAP

Rôle : Compter les occurrences de chaque mot dans UN fichier texte.

Fonctionnement :
1. Reçoit le chemin du fichier texte et sa configuration depuis le coordinateur
2. Lit le fichier, nettoie les mots (minuscules, sans ponctuation)
3. Compte les occurrences : {"bonjour": 3, "monde": 2, ...}
4. Ouvre un serveur socket et attend que les workers REDUCE viennent
   lui demander les mots dont ils sont responsables (phase SHUFFLE)

Usage :
    python map_worker.py <worker_id> <num_reducers>
    python map_worker.py 0 2
"""

import socket
import threading
import sys
import re
import time
from utils import HOST, MAP_BASE_PORT, send_data, recv_data, get_reducer_for_word


def count_words(filepath):
    """
    Lit un fichier texte et retourne un dictionnaire {mot: nb_occurrences}.

    Nettoyage appliqué :
    - Conversion en minuscules ("Bonjour" et "bonjour" = même mot)
    - Suppression des caractères non-alphabétiques (ponctuation, chiffres)
    - Suppression des mots vides (chaînes vides après nettoyage)
    """
    word_count = {}

    print(f"  [MAP] Lecture du fichier : {filepath}")
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                # re.findall extrait tous les "mots" (séquences de lettres)
                # \b[a-zA-ZÀ-ÿ]+\b capture aussi les lettres accentuées
                words = re.findall(r"[a-zA-ZÀ-ÿ]+", line.lower())
                for word in words:
                    word_count[word] = word_count.get(word, 0) + 1
    except FileNotFoundError:
        print(f"  [MAP] ERREUR : fichier '{filepath}' introuvable !")
        return {}

    total = sum(word_count.values())
    print(f"  [MAP] Comptage terminé : {len(word_count)} mots distincts, {total} occurrences totales")
    return word_count


def handle_reduce_request(conn, addr, word_count, worker_id, num_reducers):
    """
    Gère la connexion d'UN worker REDUCE.

    Protocole :
    - Le REDUCE envoie {"reducer_id": X}
    - Le MAP filtre ses mots pour ne garder que ceux appartenant au reducer X
    - Le MAP envoie {"words": {"bonjour": 3, "monde": 2, ...}}
    """
    try:
        request = recv_data(conn)
        reducer_id = request["reducer_id"]

        # Filtrage : on ne renvoie que les mots dont ce reducer est responsable
        subset = {
            word: count
            for word, count in word_count.items()
            if get_reducer_for_word(word, num_reducers) == reducer_id
        }

        print(f"  [MAP {worker_id}] → Reducer {reducer_id} demande ses mots : {len(subset)} mots envoyés")
        send_data(conn, {"words": subset})

    except Exception as e:
        print(f"  [MAP {worker_id}] Erreur lors de la communication avec un reducer : {e}")
    finally:
        conn.close()


def run_map_worker(worker_id, filepath, num_reducers):
    """
    Fonction principale du worker MAP.

    1. Compte les mots dans le fichier
    2. Lance un serveur socket pour attendre les requêtes des reducers
    """
    print(f"\n{'='*50}")
    print(f"[MAP {worker_id}] Démarrage — fichier : {filepath}")
    print(f"{'='*50}")

    # ── Phase MAP : compter les mots ──────────────────
    start = time.time()
    word_count = count_words(filepath)
    elapsed = time.time() - start
    print(f"  [MAP {worker_id}] Phase MAP terminée en {elapsed:.3f}s")

    # ── Phase d'attente (SHUFFLE) : servir les reducers ─
    port = MAP_BASE_PORT + worker_id
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # SO_REUSEADDR : permet de relancer rapidement sans "port déjà utilisé"
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, port))
    server.listen(5)

    print(f"  [MAP {worker_id}] En écoute sur le port {port}, attente de {num_reducers} reducers...")

    # On attend exactement num_reducers connexions (une par reducer)
    connections_handled = 0
    threads = []
    while connections_handled < num_reducers:
        conn, addr = server.accept()
        print(f"  [MAP {worker_id}] Connexion reçue de {addr}")
        # Chaque reducer est servi dans un thread séparé (parallélisme)
        t = threading.Thread(
            target=handle_reduce_request,
            args=(conn, addr, word_count, worker_id, num_reducers)
        )
        t.start()
        threads.append(t)
        connections_handled += 1

    # Attendre que tous les threads aient fini d'envoyer leurs données
    for t in threads:
        t.join()

    server.close()
    print(f"  [MAP {worker_id}] Toutes les données envoyées. Terminé.")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage : python map_worker.py <worker_id> <filepath> <num_reducers>")
        sys.exit(1)

    worker_id = int(sys.argv[1])
    filepath = sys.argv[2]
    num_reducers = int(sys.argv[3])

    run_map_worker(worker_id, filepath, num_reducers)
