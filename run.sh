#!/usr/bin/env bash
# Script de build et de lancement du cluster Map-Reduce Java.

set -e
ROOT="$(cd "$(dirname "$0")"; pwd)"
CP="$ROOT/target/classes"

build() {
    echo "Compilation..."
    find "$ROOT/src" -name "*.java" > /tmp/sources.txt
    javac -d "$CP" --release 17 @/tmp/sources.txt
    echo "Compilation OK."
}

case "${1:-help}" in
  build)
    build
    ;;

  master)
    shift
    java -cp "$CP" mapreduce.master.MasterNode "$@"
    ;;

  mapworker)
    shift
    java -cp "$CP" mapreduce.worker.MapWorker "$@"
    ;;

  reduceworker)
    shift
    java -cp "$CP" mapreduce.worker.ReduceWorker "$@"
    ;;

  cluster)
    shift
    java -cp "$CP" mapreduce.scripts.LaunchCluster "$@"
    ;;

  client)
    shift
    java -cp "$CP" mapreduce.scripts.Client "$@"
    ;;

  *)
    echo "Usage: $0 <commande> [options]"
    echo ""
    echo "Commandes disponibles :"
    echo "  build                         Compiler le projet"
    echo "  cluster [--mappers N] [--reducers N]   Lancer un cluster local"
    echo "  client <fichier> [...]        Soumettre un job"
    echo "  master  [--port P] [--reducers N]"
    echo "  mapworker [--port P]"
    echo "  reduceworker --partition P [--port P]"
    ;;
esac
