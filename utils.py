"""
utils.py — Fonctions partagées entre tous les composants

Variables d'environnement (pour Docker / multi-machine) :
    BIND_HOST        : adresse d'écoute des workers MAP (défaut: "0.0.0.0")
    MAP_BASE_PORT    : port de base des workers MAP (défaut: 6000)
    OUTPUT_DIR       : dossier de sortie des résultats (défaut: "output")
    MAP_HOST_PREFIX  : préfixe hostname Docker Compose (ex: "map-")
    MAP_HOSTS        : adresses MAP en mode multi-machine ("192.168.1.42:6000,...")
"""

import json
import hashlib
import os


# ─────────────────────────────────────────────
# CONFIGURATION GLOBALE
# ─────────────────────────────────────────────

BIND_HOST        = os.environ.get("BIND_HOST", "0.0.0.0")
MAP_BASE_PORT    = int(os.environ.get("MAP_BASE_PORT", "6000"))
REDUCE_BASE_PORT = int(os.environ.get("REDUCE_BASE_PORT", "7000"))
OUTPUT_DIR       = os.environ.get("OUTPUT_DIR", "output")  # résultats dans output/


def get_map_address(map_id):
    """
    Retourne (hostname, port) du worker MAP pour l'ID donné.

    Priorité de résolution :
      1. MAP_HOSTS="192.168.1.42:6000,..."  → mode multi-machine (LAN)
      2. MAP_HOST_PREFIX="map-"              → mode Docker Compose
      3. (défaut)                            → localhost (mode local)
    """
    map_hosts_env = os.environ.get("MAP_HOSTS", "").strip()
    if map_hosts_env:
        entries = [h.strip() for h in map_hosts_env.split(",")]
        if map_id < len(entries):
            host, port_str = entries[map_id].rsplit(":", 1)
            return host, int(port_str)

    prefix = os.environ.get("MAP_HOST_PREFIX", "").strip()
    if prefix:
        return f"{prefix}{map_id}", MAP_BASE_PORT + map_id

    return "localhost", MAP_BASE_PORT + map_id


# ─────────────────────────────────────────────
# COMMUNICATION TCP (protocole length-prefix)
# ─────────────────────────────────────────────
# Format d'un message : [4 octets = taille] + [JSON UTF-8]
# Cela évite la troncature des données TCP.

def send_data(sock, data):
    """Sérialise data en JSON et l'envoie avec un entête de 4 octets."""
    message = json.dumps(data).encode("utf-8")
    sock.sendall(len(message).to_bytes(4, byteorder="big") + message)


def recv_data(sock):
    """Reçoit et désérialise un message envoyé par send_data()."""
    raw_len = _recv_exact(sock, 4)
    if raw_len is None:
        return None
    raw_msg = _recv_exact(sock, int.from_bytes(raw_len, byteorder="big"))
    return json.loads(raw_msg.decode("utf-8")) if raw_msg else None


def _recv_exact(sock, n):
    """Lit exactement n octets (boucle sur les fragments TCP)."""
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
    Détermine quel reducer est responsable d'un mot.

    Utilise MD5 (déterministe entre processus) plutôt que hash() Python
    qui change à chaque exécution (randomisation de sécurité).
    """
    h = int(hashlib.md5(word.encode("utf-8")).hexdigest(), 16)
    return h % num_reducers
