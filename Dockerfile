FROM python:3.11-slim

WORKDIR /app

# Copier les fichiers source
COPY utils.py coordinator.py map_worker.py reduce_worker.py ./

# Copier les textes d'entrée
COPY texts/ ./texts/

# Créer le dossier de sortie
RUN mkdir -p output

# Pas de dépendances externes (stdlib uniquement)
# requirements.txt vide mais présent pour la convention
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
