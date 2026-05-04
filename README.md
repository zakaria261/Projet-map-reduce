# MapReduce Distribué — Python

Implémentation d'un système MapReduce distribué en Python pur, avec communication TCP entre workers et simulation d'un cluster via Docker.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Docker Network / LAN                      │
│                                                              │
│  ┌─────────┐   ┌─────────┐   ┌─────────┐                   │
│  │  MAP-0  │   │  MAP-1  │   │  MAP-2  │  ← un par fichier │
│  │ :6000   │   │ :6001   │   │ :6002   │                   │
│  └────┬────┘   └────┬────┘   └────┬────┘                   │
│       │    SHUFFLE (TCP pull)      │                         │
│  ┌────▼────────────────────────────▼────┐                   │
│  │      REDUCE-0          REDUCE-1      │                   │
│  └────────────────┬─────────────────────┘                   │
│                   │ result_reducer_X.json                    │
│  ┌────────────────▼─────────────────────┐                   │
│  │            COORDINATOR               │                   │
│  │   fusionne → result_final.json       │                   │
│  └──────────────────────────────────────┘                   │
└─────────────────────────────────────────────────────────────┘
```

## Prérequis

- Python 3.9+
- Docker + Docker Compose (pour le mode cluster)

---

## Mode 1 — Local (une seule machine)

```bash
python coordinator.py
python coordinator.py --reducers 4 --top 30
python coordinator.py --input-dir mes_textes/ --reducers 2
```

---

## Mode 2 — Cluster Docker (simulation locale)

```bash
docker-compose up --build
docker-compose down -v   # nettoyer après
```

Cela lance **3 containers MAP + 2 REDUCE + 1 coordinator** isolés sur ton PC.

---

## Mode 3 — Cluster Multi-machine (même Wi-Fi)

### Sur chaque PC de tes potes (un worker MAP par personne)

Ils clonent le repo et lancent leur container MAP :

```bash
# Pote 1 — lance MAP-0 avec text1.txt
docker build -t mapreduce-img .

docker run --rm -p 6000:6000 \
  -e BIND_HOST=0.0.0.0 \
  mapreduce-img \
  python map_worker.py 0 texts/text1.txt 2

# Pote 2 — MAP-1 avec text2.txt (port 6001)
docker run --rm -p 6001:6001 \
  -e BIND_HOST=0.0.0.0 \
  mapreduce-img \
  python map_worker.py 1 texts/text2.txt 2

# Pote 3 — MAP-2 avec text3.txt (port 6002)
docker run --rm -p 6002:6002 \
  -e BIND_HOST=0.0.0.0 \
  mapreduce-img \
  python map_worker.py 2 texts/text3.txt 2
```

> **Trouver son IP LAN** (Windows) : `ipconfig` → chercher "Adresse IPv4" sous Wi-Fi

### Sur ta machine (coordinator + reducers)

```bash
# 1. Copier et remplir la config avec les IPs LAN de tes potes
cp .env.example .env
# Éditer .env :
# MAP_HOSTS=192.168.1.42:6000,192.168.1.43:6001,192.168.1.44:6002

# 2. Lancer reducers + coordinator
docker-compose -f docker-compose.multi.yml up --build
```

---

## Structure du projet

```
mapreduce/
├── coordinator.py           # Chef d'orchestre (local ou docker)
├── map_worker.py            # Worker MAP (comptage + serveur socket)
├── reduce_worker.py         # Worker REDUCE (shuffle + agrégation)
├── utils.py                 # Protocole TCP, partitionnement, config ENV
├── Dockerfile               # Image unique pour tous les workers
├── docker-compose.yml       # Cluster local (simulation)
├── docker-compose.multi.yml # Cluster multi-machine (LAN)
├── .env.example             # Template de configuration
├── requirements.txt         # Dépendances (stdlib uniquement)
└── texts/                   # Fichiers texte d'entrée
    ├── text1.txt
    ├── text2.txt
    └── text3.txt
```

## Paramètres CLI

| Argument | Défaut | Description |
|---|---|---|
| `--mode` | `local` | `local` ou `docker` |
| `--reducers` | `2` | Nombre de workers REDUCE |
| `--input-dir` | `texts` | Dossier des fichiers .txt |
| `--top` | `20` | Nombre de mots à afficher |

## Variables d'environnement

| Variable | Exemple | Description |
|---|---|---|
| `MAP_HOSTS` | `192.168.1.42:6000,...` | IPs LAN des workers MAP |
| `MAP_HOST_PREFIX` | `map-` | Préfixe hostname Docker Compose |
| `BIND_HOST` | `0.0.0.0` | Adresse d'écoute des MAPs |
| `OUTPUT_DIR` | `/app/output` | Dossier de sortie des résultats |
| `MAP_BASE_PORT` | `6000` | Port de base des workers MAP |
