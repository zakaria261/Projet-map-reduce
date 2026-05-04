"""
utils.py — Fonctions partagées entre tous les composants

Ce fichier contient :
- Les constantes de configuration (ports, adresses)
- Des fonctions pour envoyer/recevoir des données via les sockets
- La logique de partitionnement : quel REDUCE est responsable de quel mot
"""

import json
import socket

# ─────────────────────────────────────────────
# CONFIGURATION GLOBALE
# ─────────────────────────────────────────────

# Adresse IP sur laquelle tous les composants écoutent.
# "localhost" = tout sur la même machine (simulation distribuée).
# Sur de vraies machines distinctes, remplacer par les vraies IP.
HOST = "localhost"

# Port de base pour les workers MAP.
# Le 1er worker MAP écoutera sur MAP_BASE_PORT,
# le 2e sur MAP_BASE_PORT+1, etc.
MAP_BASE_PORT = 6000

# Port de base pour les workers REDUCE (même principe).
REDUCE_BASE_PORT = 7000


# ─────────────────────────────────────────────
# FONCTIONS DE COMMUNICATION (PROTOCOLE MAISON)
# ─────────────────────────────────────────────
# On envoie des données en JSON sur les sockets.
# Problème : les sockets TCP n'ont pas de notion de "message".
# Un recv() peut recevoir un morceau seulement.
# Solution : on préfixe chaque message par sa taille (4 octets).

def send_data(sock, data):
    """
    Sérialise 'data' en JSON et l'envoie sur le socket 'sock'.
    Format : [4 octets = taille] + [message JSON en UTF-8]
    """
    message = json.dumps(data).encode("utf-8")
    # On prépare un entête de 4 octets contenant la longueur du message
    length = len(message).to_bytes(4, byteorder="big")
    sock.sendall(length + message)


def recv_data(sock):
    """
    Reçoit un message envoyé par send_data().
    Lit d'abord les 4 octets d'entête pour connaître la taille,
    puis lit exactement ce nombre d'octets.
    Retourne l'objet Python désérialisé depuis JSON.
    """
    # Étape 1 : lire l'entête (4 octets)
    raw_len = _recv_exact(sock, 4)
    if raw_len is None:
        return None
    msg_len = int.from_bytes(raw_len, byteorder="big")

    # Étape 2 : lire le message complet
    raw_msg = _recv_exact(sock, msg_len)
    if raw_msg is None:
        return None

    return json.loads(raw_msg.decode("utf-8"))


def _recv_exact(sock, n):
    """
    Fonction interne : lit exactement n octets depuis le socket.
    TCP peut fragmenter les données, donc on boucle jusqu'à avoir tout.
    """
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None  # Connexion fermée
        buf += chunk
    return buf


# ─────────────────────────────────────────────
# PARTITIONNEMENT (QUELLE REDUCE TRAITE QUEL MOT)
# ─────────────────────────────────────────────

def get_reducer_for_word(word, num_reducers):
    """
    Détermine l'index du worker REDUCE responsable d'un mot donné.

    Principe : on utilise le hash du mot modulo le nombre de reducers.
    Cela garantit que le MÊME mot va TOUJOURS au MÊME reducer,
    quelle que soit la machine MAP qui l'a compté.

    Exemple avec 2 reducers :
        hash("bonjour") % 2 = 0  → Reducer 0
        hash("monde")   % 2 = 1  → Reducer 1
        hash("bonjour") % 2 = 0  → Toujours Reducer 0 ✓

    C'est l'équivalent du "partitioner" d'Hadoop.
    """
    # abs() car hash() peut être négatif en Python
    return abs(hash(word)) % num_reducers
