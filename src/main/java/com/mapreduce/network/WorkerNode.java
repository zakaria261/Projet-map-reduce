package com.mapreduce.network;

import com.mapreduce.engine.WordCountEngine;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Nœud Worker du cluster Map-Reduce.
 *
 * Cycle de vie :
 *   1. Ouvre son propre ServerSocket (AVANT de s'enregistrer,
 *      pour être prêt à recevoir des tâches immédiatement).
 *   2. Envoie REGISTER au Master pour signaler sa présence.
 *   3. Boucle infinie d'acceptation de connexions.
 *   4. Chaque connexion est traitée dans un thread séparé
 *      (ExecutorService) pour ne pas bloquer les autres.
 *
 * Commandes supportées :
 *   ┌─────────────┬────────────────────────────────────────────┐
 *   │  PING       │  Répond PONG (health check du Master)      │
 *   │  MAP_TASK   │  Exécute WordCountEngine.map() + partitions │
 *   │  REDUCE_TASK│  Exécute WordCountEngine.reduce()           │
 *   └─────────────┴────────────────────────────────────────────┘
 */
public class WorkerNode {

    private final String host;
    private final int    port;
    private final String masterHost;
    private final int    masterPort;

    // Pool de threads pour traiter les tâches en parallèle.
    // Initialisé dans le constructeur pour accéder à `port` (champ final).
    private final ExecutorService taskPool;

    private volatile boolean running = false;
    private ServerSocket serverSocket;

    public WorkerNode(String host, int port, String masterHost, int masterPort) {
        this.host       = host;
        this.port       = port;
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.taskPool   = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "worker-" + port + "-task");
            t.setDaemon(true);
            return t;
        });
    }

    // ── Démarrage ─────────────────────────────────────────────────

    /**
     * Démarre le Worker :
     *   1. Ouvre le ServerSocket sur son port.
     *   2. S'enregistre auprès du Master.
     *   3. Boucle d'acceptation des connexions.
     */
    public void start() throws IOException {
        // Étape 1 : ouvrir le ServerSocket AVANT de s'enregistrer
        // pour éviter que le Master envoie une tâche avant qu'on écoute.
        serverSocket = new ServerSocket(port);
        serverSocket.setReuseAddress(true);
        running = true;
        log("Listening for tasks on port " + port);

        // Étape 2 : s'enregistrer auprès du Master
        register();

        // Étape 3 : boucle d'acceptation
        log("Ready — waiting for orders from Master...");
        try {
            while (running) {
                try {
                    Socket connection = serverSocket.accept();
                    // Chaque connexion est traitée dans un thread dédié
                    taskPool.submit(() -> handleConnection(connection));
                } catch (SocketException e) {
                    // ServerSocket fermé proprement via stop()
                    if (running) log("Unexpected socket error: " + e.getMessage());
                }
            }
        } finally {
            taskPool.shutdown();
        }
    }

    /** Arrête proprement le Worker. */
    public void stop() {
        running = false;
        try { if (serverSocket != null) serverSocket.close(); }
        catch (IOException ignored) {}
        taskPool.shutdownNow();
        log("Stopped.");
    }

    // ── Enregistrement auprès du Master ───────────────────────────

    /**
     * Se connecte au Master et envoie REGISTER|host|port.
     * Attend un ACK en retour.
     * Retry 3 fois avec 1 seconde d'intervalle en cas d'échec.
     */
    private void register() {
        int maxRetries = 3;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try (Socket s = new Socket(masterHost, masterPort)) {
                s.setSoTimeout(5_000);
                NetworkProtocol.send(s, NetworkProtocol.register(host, port));
                String response = NetworkProtocol.receive(s);

                if (response == null) {
                    log("No response from Master (attempt " + attempt + ")");
                } else {
                    String[] parts = NetworkProtocol.parse(response);
                    if (NetworkProtocol.CMD_ACK.equals(parts[0])) {
                        log("Registered with Master at " +
                            masterHost + ":" + masterPort + " ✓");
                        return; // Succès
                    } else {
                        log("Unexpected response from Master: " + response);
                    }
                }
            } catch (IOException e) {
                log("Registration attempt " + attempt + " failed: " + e.getMessage());
            }

            if (attempt < maxRetries) {
                try { Thread.sleep(1_000); }
                catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
        log("WARNING: Could not register after " + maxRetries + " attempts.");
    }

    // ── Dispatch des connexions entrantes ─────────────────────────

    /**
     * Lit la première ligne de la connexion et route vers
     * le bon gestionnaire selon la commande reçue.
     * Ferme la socket à la fin (try-with-resources).
     */
    private void handleConnection(Socket conn) {
        try (conn) {
            conn.setSoTimeout(120_000); // timeout 2 min par tâche
            String raw = NetworkProtocol.receive(conn);
            if (raw == null || raw.isBlank()) return;

            String[] parts = NetworkProtocol.parse(raw);
            String   cmd   = parts[0];

            log("Received command: " + cmd);

            switch (cmd) {
                case NetworkProtocol.CMD_PING        -> handlePing(conn);
                case NetworkProtocol.CMD_MAP_TASK    -> handleMapTask(conn, parts);
                case NetworkProtocol.CMD_REDUCE_TASK -> handleReduceTask(conn, parts);
                default -> log("Unknown command ignored: " + cmd);
            }

        } catch (SocketTimeoutException e) {
            log("Connection timed out: " + e.getMessage());
        } catch (IOException e) {
            log("Connection error: " + e.getMessage());
        }
    }

    // ── Handlers ──────────────────────────────────────────────────

    /**
     * PING → PONG
     * Permet au Master de vérifier que le Worker est toujours en vie.
     */
    private void handlePing(Socket conn) throws IOException {
        NetworkProtocol.send(conn, NetworkProtocol.pong(host + ":" + port));
        log("PONG sent to Master.");
    }

    /**
     * MAP_TASK|jobId|filePath|nbReducers|outputDir
     *
     * Étapes :
     *   1. Lire le fichier d'entrée
     *   2. Appeler WordCountEngine.map()
     *   3. Appeler WordCountEngine.savePartitions()
     *   4. Répondre TASK_DONE|jobId|outputDir ou TASK_FAILED
     */
    private void handleMapTask(Socket conn, String[] parts) throws IOException {
        // Validation du format
        if (parts.length < 5) {
            log("MAP_TASK: invalid format, expected 5 fields, got " + parts.length);
            NetworkProtocol.send(conn,
                NetworkProtocol.taskFailed("?", "Invalid MAP_TASK format"));
            return;
        }

        String jobId      = parts[1];
        String filePath   = parts[2];
        int    nbReducers = Integer.parseInt(parts[3]);
        Path   outputDir  = Paths.get(parts[4]);
        String taskId     = "map-" + jobId;

        log("MAP_TASK started: job=" + jobId +
            " file=" + Paths.get(filePath).getFileName() +
            " reducers=" + nbReducers);

        try {
            // 1. Lecture du fichier
            String content = Files.readString(Paths.get(filePath));

            // 2. Phase MAP : comptage local des mots
            Map<String, Integer> counts = WordCountEngine.map(content);
            log("MAP_TASK job=" + jobId + ": " + counts.size() + " unique word(s) counted.");

            // 3. Sauvegarde des partitions (spill files)
            List<Path> partitionFiles =
                WordCountEngine.savePartitions(counts, nbReducers, outputDir);

            // Construction du chemin de sortie (répertoire contenant les partitions)
            String outputPath = outputDir.toAbsolutePath().toString();

            // 4. Notification de succès
            NetworkProtocol.send(conn, NetworkProtocol.taskDone(taskId, outputPath));
            log("MAP_TASK done: job=" + jobId + " → " + outputPath);

        } catch (Exception e) {
            log("MAP_TASK FAILED: job=" + jobId + " error=" + e.getMessage());
            NetworkProtocol.send(conn,
                NetworkProtocol.taskFailed(taskId, e.getMessage()));
        }
    }

    /**
     * REDUCE_TASK|jobId|partition|outputDir|file1,file2,...
     *
     * Étapes :
     *   1. Parser les chemins des fichiers de partition
     *   2. Appeler WordCountEngine.reduce()
     *   3. Appeler WordCountEngine.writeResult()
     *   4. Répondre TASK_DONE|jobId|outputFile ou TASK_FAILED
     */
    private void handleReduceTask(Socket conn, String[] parts) throws IOException {
        if (parts.length < 5) {
            log("REDUCE_TASK: invalid format, expected 5 fields, got " + parts.length);
            NetworkProtocol.send(conn,
                NetworkProtocol.taskFailed("?", "Invalid REDUCE_TASK format"));
            return;
        }

        String jobId     = parts[1];
        int    partition = Integer.parseInt(parts[2]);
        Path   outputDir = Paths.get(parts[3]);
        String taskId    = "reduce-" + jobId + "-p" + partition;

        // Les fichiers de partition sont séparés par des virgules
        String[] rawFiles = parts[4].split(",");
        List<Path> partitionFiles = new ArrayList<>();
        for (String f : rawFiles) {
            if (!f.isBlank()) partitionFiles.add(Paths.get(f.trim()));
        }

        log("REDUCE_TASK started: job=" + jobId +
            " partition=" + partition +
            " files=" + partitionFiles.size());

        try {
            // 1. Agrégation des mots depuis tous les fichiers de partition
            Map<String, Integer> result = WordCountEngine.reduce(partitionFiles);
            log("REDUCE_TASK job=" + jobId + " p=" + partition +
                ": " + result.size() + " unique word(s) aggregated.");

            // 2. Écriture du résultat final
            Path outputFile = outputDir.resolve(
                "result_partition_" + partition + ".txt");
            WordCountEngine.writeResult(result, outputFile);

            // 3. Notification de succès
            NetworkProtocol.send(conn,
                NetworkProtocol.taskDone(taskId, outputFile.toAbsolutePath().toString()));
            log("REDUCE_TASK done: job=" + jobId +
                " p=" + partition + " → " + outputFile);

        } catch (Exception e) {
            log("REDUCE_TASK FAILED: job=" + jobId +
                " p=" + partition + " error=" + e.getMessage());
            NetworkProtocol.send(conn,
                NetworkProtocol.taskFailed(taskId, e.getMessage()));
        }
    }

    // ── Log ───────────────────────────────────────────────────────

    private void log(String msg) {
        System.out.printf("%s [WORKER %d] %s%n",
            NetworkProtocol.timestamp(), port, msg);
    }

    // ── Main ──────────────────────────────────────────────────────

    public static void main(String[] args) throws IOException {
        String host       = "127.0.0.1";
        int    port       = 5001;
        String masterHost = "127.0.0.1";
        int    masterPort = 5000;

        for (int i = 0; i < args.length - 1; i++) {
            switch (args[i]) {
                case "--host"        -> host       = args[++i];
                case "--port"        -> port       = Integer.parseInt(args[++i]);
                case "--master-host" -> masterHost = args[++i];
                case "--master-port" -> masterPort = Integer.parseInt(args[++i]);
            }
        }

        new WorkerNode(host, port, masterHost, masterPort).start();
    }
}
