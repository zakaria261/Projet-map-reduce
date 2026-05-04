"""
utils.py — Fonctions partagées entre tous les composants

Variables d'environnement supportées (pour Docker / multi-machine) :
    BIND_HOST        : adresse d'écoute des workers MAP (défaut: "0.0.0.0")
    MAP_BASE_PORT    : port de base des workers MAP (défaut: 6000)
    OUTPUT_DIR       : dossier de sortie des résultats (défaut: ".")
    MAP_HOST_PREFIX  : préfixe hostname Docker Compose (ex: "map-")
    MAP_HOSTS        : liste en mode multi-machine ("192.168.1.42:6000,192.168.1.43:6001")
"""

import json
import hashlib
import os


# ─────────────────────────────────────────────
# CONFIGURATION GLOBALE
# ─────────────────────────────────────────────

# Adresse d'écoute des workers MAP
# "0.0.0.0" = accepte toutes les interfaces (obligatoire pour Docker)
BIND_HOST = os.environ.get("BIND_HOST", "0.0.0.0")

MAP_BASE_PORT    = int(os.environ.get("MAP_BASE_PORT", "6000"))
REDUCE_BASE_PORT = int(os.environ.get("REDUCE_BASE_PORT", "7000"))

# Dossier de sortie pour les résultats JSON
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", ".")


def get_map_address(map_id):
    """
    Retourne (hostname, port) du worker MAP pour l'ID donné.

    Trois modes de résolution (par ordre de priorité) :
    1. MAP_HOSTS="192.168.1.42:6000,192.168.1.43:6001" → multi-machine (LAN)
    2. MAP_HOST_PREFIX="map-"                           → Docker Compose
    3. Défaut                                           → localhost (mode local)

    Exemples :
        get_map_address(0) → ("localhost", 6000)     # local
        get_map_address(1) → ("map-1",    6001)      # docker-compose
        get_map_address(0) → ("192.168.1.42", 6000)  # multi-machine
    """
    # Mode multi-machine : liste explicite d'adresses
    map_hosts_env = os.environ.get("MAP_HOSTS", "").strip()
    if map_hosts_env:
        entries = [h.strip() for h in map_hosts_env.split(",")]
        if map_id < len(entries):
            host, port_str = entries[map_id].rsplit(":", 1)
            return host, int(port_str)

    # Mode Docker Compose : MAP_HOST_PREFIX="map-"
    prefix = os.environ.get("MAP_HOST_PREFIX", "").strip()
    if prefix:
        return f"{prefix}{map_id}", MAP_BASE_PORT + map_id

    # Mode local par défaut
    return "localhost", MAP_BASE_PORT + map_id


# ─────────────────────────────────────────────
# FONCTIONS DE COMMUNICATION
# ─────────────────────────────────────────────
# Format : [4 octets = taille du message] + [message JSON en UTF-8]
# Cela évite la troncature TCP (un recv() peut ne recevoir qu'un fragment).

def send_data(sock, data):
    """Sérialise 'data' en JSON et l'envoie avec un entête de 4 octets."""
    message = json.dumps(data).encode("utf-8")
    length  = len(message).to_bytes(4, byteorder="big")
    sock.sendall(length + message)


def recv_data(sock):
    """Reçoit un message envoyé par send_data() et le désérialise."""
    raw_len = _recv_exact(sock, 4)
    if raw_len is None:
        return None
    msg_len = int.from_bytes(raw_len, byteorder="big")
    raw_msg = _recv_exact(sock, msg_len)
    if raw_msg is None:
        return None
    return json.loads(raw_msg.decode("utf-8"))


def _recv_exact(sock, n):
    """Lit exactement n octets depuis le socket (boucle sur les fragments TCP)."""
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


# ─────────────────────────────────────────────
# PARTITIONNEMENT
# ─────────────────────────────────────────────

def get_reducer_for_word(word, num_reducers):
    """
    Détermine l'index du reducer responsable d'un mot.

    Utilise MD5 (hashlib) au lieu de hash() Python.
    Raison : hash() est aléatoire entre processus (security randomization).
    MD5 est déterministe → même mot = même reducer, toujours.

    Exemple avec 2 reducers :
        md5("bonjour") % 2 → Reducer 0
        md5("monde")   % 2 → Reducer 1
    """
    h = int(hashlib.md5(word.encode("utf-8")).hexdigest(), 16)
    return h % num_reducers
