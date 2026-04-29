package com.mapreduce.network;

import java.nio.file.*;
import java.util.Map;

/**
 * Lanceur de test : démarre un Master et deux Workers dans le même JVM.
 *
 * Scénario testé :
 *   ┌─────────────────────────────────────────────────────────────┐
 *   │  1. Master démarre sur le port 5000                         │
 *   │  2. Worker A démarre sur le port 5001, s'enregistre        │
 *   │  3. Worker B démarre sur le port 5002, s'enregistre        │
 *   │  4. Master envoie un PING à chaque Worker → attend PONG    │
 *   │  5. Master envoie une MAP_TASK au Worker A                  │
 *   │  6. Master envoie une MAP_TASK au Worker B (même fichier)  │
 *   │  7. Master envoie une REDUCE_TASK au Worker A (partition 0)│
 *   │  8. Master envoie une REDUCE_TASK au Worker B (partition 1)│
 *   │  9. Affiche un bilan final                                  │
 *   └─────────────────────────────────────────────────────────────┘
 *
 * Ports utilisés :
 *   Master  → 5000
 *   Worker A → 5001
 *   Worker B → 5002
 */
public class Launcher {

    // ── Configuration ─────────────────────────────────────────────
    private static final String MASTER_HOST = "127.0.0.1";
    private static final int    MASTER_PORT = 6100;

    private static final String WORKER_A_HOST = "127.0.0.1";
    private static final int    WORKER_A_PORT = 6101;

    private static final String WORKER_B_HOST = "127.0.0.1";
    private static final int    WORKER_B_PORT = 6102;

    private static final int    NB_REDUCERS   = 2;

    // Répertoire de travail pour les fichiers temporaires du test
    private static final String WORK_DIR = "./tmp/network_test";

    // ─────────────────────────────────────────────────────────────

    public static void main(String[] args) throws Exception {
        banner("LANCEMENT DU CLUSTER MAP-REDUCE (test réseau)");

        // ── ÉTAPE 1 : Démarrage du Master ─────────────────────────
        section("Étape 1 — Démarrage du Master (port " + MASTER_PORT + ")");
        MasterNode master = new MasterNode(MASTER_PORT);

        Thread masterThread = new Thread(() -> {
            try { master.start(); }
            catch (Exception e) {
                System.err.println("[LAUNCHER] Master error: " + e.getMessage());
            }
        }, "master-thread");
        masterThread.setDaemon(true);
        masterThread.start();

        // Laisser le temps au Master d'ouvrir son ServerSocket
        Thread.sleep(400);

        // ── ÉTAPE 2 : Démarrage du Worker A ───────────────────────
        section("Étape 2 — Démarrage du Worker A (port " + WORKER_A_PORT + ")");
        WorkerNode workerA = new WorkerNode(
            WORKER_A_HOST, WORKER_A_PORT, MASTER_HOST, MASTER_PORT);

        Thread threadA = new Thread(() -> {
            try { workerA.start(); }
            catch (Exception e) {
                System.err.println("[LAUNCHER] Worker A error: " + e.getMessage());
            }
        }, "worker-a-thread");
        threadA.setDaemon(true);
        threadA.start();

        Thread.sleep(400);

        // ── ÉTAPE 3 : Démarrage du Worker B ───────────────────────
        section("Étape 3 — Démarrage du Worker B (port " + WORKER_B_PORT + ")");
        WorkerNode workerB = new WorkerNode(
            WORKER_B_HOST, WORKER_B_PORT, MASTER_HOST, MASTER_PORT);

        Thread threadB = new Thread(() -> {
            try { workerB.start(); }
            catch (Exception e) {
                System.err.println("[LAUNCHER] Worker B error: " + e.getMessage());
            }
        }, "worker-b-thread");
        threadB.setDaemon(true);
        threadB.start();

        // Laisser le temps aux deux Workers de s'enregistrer
        Thread.sleep(600);

        // ── ÉTAPE 4 : Vérification des enregistrements ────────────
        section("Étape 4 — Vérification des Workers enregistrés");
        System.out.printf("[LAUNCHER] %d worker(s) enregistré(s) auprès du Master :%n",
            master.getWorkers().size());
        for (WorkerHandle w : master.getWorkers()) {
            System.out.println("           → " + w.id);
        }

        if (master.getWorkers().size() < 2) {
            System.err.println("[LAUNCHER] ERREUR : moins de 2 Workers enregistrés. Arrêt.");
            return;
        }

        WorkerHandle wA = master.getWorkers().get(0); // Worker A
        WorkerHandle wB = master.getWorkers().get(1); // Worker B

        // ── ÉTAPE 5 : PING des Workers ────────────────────────────
        section("Étape 5 — Health check (PING / PONG)");
        Map<WorkerHandle, Boolean> pingResults = master.pingAllWorkers();
        pingResults.forEach((worker, alive) ->
            System.out.printf("[LAUNCHER] Worker %-18s → %s%n",
                worker.id, alive ? "ALIVE ✓" : "DEAD ✗"));

        boolean allAlive = pingResults.values().stream().allMatch(b -> b);
        if (!allAlive) {
            System.err.println("[LAUNCHER] Certains Workers ne répondent pas. Arrêt.");
            return;
        }

        // ── ÉTAPE 6 : MAP_TASK sur les deux Workers ───────────────
        section("Étape 6 — Phase MAP (envoi de tâches)");

        // Résolution du fichier d'entrée
        Path sampleFile = resolveSampleFile();
        String filePathStr = sampleFile.toAbsolutePath().toString();

        // Répertoires de sortie pour chaque Mapper
        String outDirA = WORK_DIR + "/mapper_A";
        String outDirB = WORK_DIR + "/mapper_B";

        // Envoi en parallèle (chaque envoi est synchrone mais dans des threads séparés)
        Thread mapA = new Thread(() -> {
            String result = master.sendMapTask(wA, "job-1", filePathStr,
                                               NB_REDUCERS, outDirA);
            System.out.printf("[LAUNCHER] MAP Worker A → %s%n",
                result != null ? "OK (" + result + ")" : "FAILED");
        }, "map-a");

        Thread mapB = new Thread(() -> {
            String result = master.sendMapTask(wB, "job-1", filePathStr,
                                               NB_REDUCERS, outDirB);
            System.out.printf("[LAUNCHER] MAP Worker B → %s%n",
                result != null ? "OK (" + result + ")" : "FAILED");
        }, "map-b");

        mapA.start();
        mapB.start();
        mapA.join();
        mapB.join();

        // ── ÉTAPE 7 : REDUCE_TASK sur les deux Workers ────────────
        section("Étape 7 — Phase REDUCE (envoi de tâches)");

        // Pour chaque partition, on donne au Reducer les fichiers
        // partition_X.txt produits par CHACUN des Mappers.
        String reduceOutDir = WORK_DIR + "/reducer_output";

        for (int p = 0; p < NB_REDUCERS; p++) {
            final int partition = p;
            // Fichier de partition P du Mapper A et du Mapper B
            String fileFromA = outDirA + "/partition_" + p + ".txt";
            String fileFromB = outDirB + "/partition_" + p + ".txt";

            // Choix du Worker Reducer (round-robin)
            WorkerHandle reducer = (p % 2 == 0) ? wA : wB;

            Thread reduceThread = new Thread(() -> {
                String result = master.sendReduceTask(
                    reducer, "job-1", partition, reduceOutDir,
                    fileFromA, fileFromB);
                System.out.printf("[LAUNCHER] REDUCE p=%d → Worker %s → %s%n",
                    partition, reducer.id,
                    result != null ? "OK (" + result + ")" : "FAILED");
            }, "reduce-p" + p);

            reduceThread.start();
            reduceThread.join();
        }

        // ── ÉTAPE 8 : Bilan final ─────────────────────────────────
        section("Étape 8 — Bilan final");
        System.out.println("[LAUNCHER] Workers enregistrés : " + master.getWorkers().size());
        System.out.println("[LAUNCHER] Workers vivants     : " + master.getAliveWorkers().size());
        System.out.println("[LAUNCHER] Fichiers produits dans : " + WORK_DIR);

        // Affichage du résultat du Reducer 0 (partition 0)
        Path resultFile = Paths.get(reduceOutDir, "result_partition_0.txt");
        if (Files.exists(resultFile)) {
            System.out.println("\n[LAUNCHER] Aperçu du résultat (partition 0, 10 premiers mots) :");
            Files.lines(resultFile).limit(10).forEach(l ->
                System.out.println("           " + l));
        }

        banner("TEST RÉSEAU TERMINÉ AVEC SUCCÈS");

        // Arrêt propre
        master.stop();
        workerA.stop();
        workerB.stop();
        Thread.sleep(200); // Laisser le temps aux threads de se terminer
    }

    // ── Utilitaires ───────────────────────────────────────────────

    private static Path resolveSampleFile() {
        for (String candidate : new String[]{
                "data/sample.txt", "../data/sample.txt"}) {
            Path p = Paths.get(candidate);
            if (Files.exists(p)) return p;
        }
        throw new RuntimeException(
            "data/sample.txt introuvable. " +
            "Lancez depuis la racine du projet.");
    }

    private static void banner(String title) {
        String line = "═".repeat(60);
        System.out.println("\n" + line);
        System.out.printf("  %s%n", title);
        System.out.println(line + "\n");
    }

    private static void section(String title) {
        System.out.println("\n── " + title + " " + "─".repeat(
            Math.max(0, 55 - title.length())));
    }
}
