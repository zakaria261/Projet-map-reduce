package com.mapreduce.network;

import java.io.*;
import java.net.*;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Nœud Master du cluster Map-Reduce.
 *
 * Responsabilités :
 *   1. Écouter les inscriptions des Workers (REGISTER).
 *   2. Maintenir la liste des Workers actifs.
 *   3. Envoyer des tâches aux Workers (MAP_TASK, REDUCE_TASK).
 *   4. Vérifier la santé des Workers via PING / PONG.
 *
 * Architecture de threads :
 *   ┌──────────────────────────────────────────────────────────┐
 *   │  Thread principal  →  boucle accept() du ServerSocket    │
 *   │  registrationPool  →  1 thread par connexion entrante    │
 *   │  taskPool          →  1 thread par tâche envoyée         │
 *   └──────────────────────────────────────────────────────────┘
 *
 * Communication :
 *   - Les Workers se CONNECTENT au Master (port 5000) pour REGISTER.
 *   - Le Master se CONNECTE aux Workers pour PING, MAP_TASK, REDUCE_TASK.
 */
public class MasterNode {

    private final int port;

    // Liste thread-safe des Workers enregistrés.
    // CopyOnWriteArrayList : lecture sans verrou, écriture coûteuse mais rare
    private final List<WorkerHandle> workers = new CopyOnWriteArrayList<>();

    // Pool pour traiter les connexions entrantes (registrations) en parallèle
    private final ExecutorService registrationPool =
        Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "master-registration");
            t.setDaemon(true);
            return t;
        });

    // Pool pour envoyer des tâches (une par Worker, en parallèle)
    private final ExecutorService taskPool =
        Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors(),
            r -> {
                Thread t = new Thread(r, "master-task-sender");
                t.setDaemon(true);
                return t;
            });

    private volatile boolean running = false;
    private ServerSocket serverSocket;

    public MasterNode(int port) {
        this.port = port;
    }

    // ── Démarrage / Arrêt ─────────────────────────────────────────

    /**
     * Démarre le Master : ouvre le ServerSocket et boucle d'acceptation.
     * Méthode bloquante — à appeler dans un Thread dédié.
     */
    public void start() throws IOException {
        serverSocket = new ServerSocket();
        serverSocket.setReuseAddress(true);
        serverSocket.bind(new InetSocketAddress(port));
        running = true;

        log("Started on port " + port + " — waiting for Worker registrations...");

        try {
            while (running) {
                try {
                    Socket conn = serverSocket.accept();
                    // Délègue chaque connexion à un thread du pool
                    registrationPool.submit(() -> handleIncomingConnection(conn));
                } catch (SocketException e) {
                    if (running) log("Unexpected socket error: " + e.getMessage());
                }
            }
        } finally {
            registrationPool.shutdown();
            taskPool.shutdown();
        }
    }

    /** Arrête proprement le Master. */
    public void stop() {
        running = false;
        try { if (serverSocket != null) serverSocket.close(); }
        catch (IOException ignored) {}
        registrationPool.shutdownNow();
        taskPool.shutdownNow();
        log("Stopped.");
    }

    // ── Réception des connexions entrantes ────────────────────────

    /**
     * Lit et route la première commande d'une connexion entrante.
     * Appelé dans un thread du registrationPool.
     */
    private void handleIncomingConnection(Socket conn) {
        try (conn) {
            conn.setSoTimeout(10_000);
            String raw = NetworkProtocol.receive(conn);
            if (raw == null || raw.isBlank()) return;

            String[] parts = NetworkProtocol.parse(raw);
            String   cmd   = parts[0];

            if (NetworkProtocol.CMD_REGISTER.equals(cmd)) {
                handleRegister(conn, parts);
            } else {
                log("Unexpected command from " +
                    conn.getRemoteSocketAddress() + ": " + cmd);
            }

        } catch (IOException e) {
            log("Error handling incoming connection: " + e.getMessage());
        }
    }

    /**
     * Traite un message REGISTER|host|port.
     * Ajoute le Worker à la liste et répond ACK.
     */
    private void handleRegister(Socket conn, String[] parts) throws IOException {
        if (parts.length < 3) {
            log("REGISTER: invalid format — expected 3 fields, got " + parts.length);
            return;
        }

        String workerHost = parts[1];
        int    workerPort = Integer.parseInt(parts[2]);

        WorkerHandle handle = new WorkerHandle(workerHost, workerPort);
        workers.add(handle);

        // Confirmation au Worker
        NetworkProtocol.send(conn,
            NetworkProtocol.ack("Worker registered: " + handle.id));

        log("Worker registered: " + handle.id +
            "  (total: " + workers.size() + " worker(s))");
    }

    // ── Health check : PING / PONG ────────────────────────────────

    /**
     * Envoie un PING à un Worker et attend PONG.
     *
     * Ouvre une connexion courte vers le Worker, envoie PING, lit PONG.
     * Timeout de 3 secondes.
     *
     * @param worker le Worker à vérifier
     * @return true si le Worker a répondu PONG, false sinon
     */
    public boolean pingWorker(WorkerHandle worker) {
        log("Pinging " + worker.id + "...");
        try (Socket s = new Socket(worker.host, worker.port)) {
            s.setSoTimeout(3_000);
            NetworkProtocol.send(s, NetworkProtocol.ping());
            String response = NetworkProtocol.receive(s);

            if (response == null) {
                log("PING " + worker.id + " → no response (connection closed)");
                worker.setDead();
                return false;
            }

            String[] parts = NetworkProtocol.parse(response);
            if (NetworkProtocol.CMD_PONG.equals(parts[0])) {
                worker.markSeen();
                log("PONG received from " + worker.id + " ✓");
                return true;
            } else {
                log("PING " + worker.id + " → unexpected response: " + response);
                return false;
            }

        } catch (SocketTimeoutException e) {
            log("PING " + worker.id + " → TIMEOUT (worker may be dead)");
            worker.setDead();
            return false;
        } catch (IOException e) {
            log("PING " + worker.id + " → ERROR: " + e.getMessage());
            worker.setDead();
            return false;
        }
    }

    /**
     * Vérifie tous les Workers enregistrés en parallèle.
     *
     * @return Map { WorkerHandle → alive? }
     */
    public Map<WorkerHandle, Boolean> pingAllWorkers() {
        log("--- Health check: pinging " + workers.size() + " worker(s) ---");

        Map<WorkerHandle, Future<Boolean>> futures = new LinkedHashMap<>();
        for (WorkerHandle w : workers) {
            futures.put(w, taskPool.submit(() -> pingWorker(w)));
        }

        Map<WorkerHandle, Boolean> results = new LinkedHashMap<>();
        futures.forEach((worker, future) -> {
            try {
                results.put(worker, future.get(5, TimeUnit.SECONDS));
            } catch (Exception e) {
                results.put(worker, false);
            }
        });

        return results;
    }

    // ── Envoi de tâches aux Workers ───────────────────────────────

    /**
     * Envoie une MAP_TASK à un Worker et attend TASK_DONE ou TASK_FAILED.
     *
     * @param worker     Worker cible
     * @param jobId      identifiant du job
     * @param filePath   chemin absolu du fichier à traiter
     * @param nbReducers nombre de partitions à produire
     * @param outputDir  répertoire de sortie des spill files
     * @return chemin du répertoire de sortie, ou null en cas d'échec
     */
    public String sendMapTask(WorkerHandle worker, String jobId,
                               String filePath, int nbReducers,
                               String outputDir) {
        log("Sending MAP_TASK to " + worker.id +
            " (job=" + jobId +
            " file=" + Paths.get(filePath).getFileName() + ")");
        try (Socket s = new Socket(worker.host, worker.port)) {
            s.setSoTimeout(300_000); // 5 min pour les gros fichiers
            NetworkProtocol.send(s,
                NetworkProtocol.mapTask(jobId, filePath, nbReducers, outputDir));

            String response = NetworkProtocol.receive(s);
            if (response == null) {
                log("MAP_TASK: no response from " + worker.id);
                return null;
            }

            String[] parts = NetworkProtocol.parse(response);
            if (NetworkProtocol.CMD_TASK_DONE.equals(parts[0])) {
                log("MAP_TASK done on " + worker.id +
                    " → " + (parts.length > 2 ? parts[2] : outputDir));
                return parts.length > 2 ? parts[2] : outputDir;
            } else if (NetworkProtocol.CMD_TASK_FAILED.equals(parts[0])) {
                log("MAP_TASK FAILED on " + worker.id + ": " +
                    (parts.length > 2 ? parts[2] : "unknown error"));
                return null;
            }

        } catch (IOException e) {
            log("MAP_TASK error on " + worker.id + ": " + e.getMessage());
        }
        return null;
    }

    /**
     * Envoie une REDUCE_TASK à un Worker et attend TASK_DONE ou TASK_FAILED.
     *
     * @param worker         Worker cible
     * @param jobId          identifiant du job
     * @param partition      index de partition
     * @param outputDir      répertoire de sortie du résultat
     * @param partitionFiles chemins des spill files à agréger
     * @return chemin du fichier résultat, ou null en cas d'échec
     */
    public String sendReduceTask(WorkerHandle worker, String jobId,
                                  int partition, String outputDir,
                                  String... partitionFiles) {
        log("Sending REDUCE_TASK to " + worker.id +
            " (job=" + jobId + " partition=" + partition + ")");
        try (Socket s = new Socket(worker.host, worker.port)) {
            s.setSoTimeout(300_000);
            NetworkProtocol.send(s,
                NetworkProtocol.reduceTask(jobId, partition, outputDir, partitionFiles));

            String response = NetworkProtocol.receive(s);
            if (response == null) {
                log("REDUCE_TASK: no response from " + worker.id);
                return null;
            }

            String[] parts = NetworkProtocol.parse(response);
            if (NetworkProtocol.CMD_TASK_DONE.equals(parts[0])) {
                log("REDUCE_TASK done on " + worker.id +
                    " → " + (parts.length > 2 ? parts[2] : "?"));
                return parts.length > 2 ? parts[2] : null;
            } else if (NetworkProtocol.CMD_TASK_FAILED.equals(parts[0])) {
                log("REDUCE_TASK FAILED on " + worker.id + ": " +
                    (parts.length > 2 ? parts[2] : "unknown error"));
                return null;
            }

        } catch (IOException e) {
            log("REDUCE_TASK error on " + worker.id + ": " + e.getMessage());
        }
        return null;
    }

    // ── Accesseurs ────────────────────────────────────────────────

    /** Retourne tous les Workers enregistrés (lecture seule). */
    public List<WorkerHandle> getWorkers() {
        return Collections.unmodifiableList(workers);
    }

    /** Retourne uniquement les Workers encore vivants. */
    public List<WorkerHandle> getAliveWorkers() {
        return workers.stream()
                      .filter(WorkerHandle::isAlive)
                      .collect(Collectors.toList());
    }

    // ── Log ───────────────────────────────────────────────────────

    private void log(String msg) {
        System.out.printf("%s [MASTER] %s%n",
            NetworkProtocol.timestamp(), msg);
    }

    // ── Main ──────────────────────────────────────────────────────

    public static void main(String[] args) throws IOException {
        int port = 5000;
        for (int i = 0; i < args.length - 1; i++) {
            if ("--port".equals(args[i])) port = Integer.parseInt(args[++i]);
        }
        new MasterNode(port).start();
    }
}
