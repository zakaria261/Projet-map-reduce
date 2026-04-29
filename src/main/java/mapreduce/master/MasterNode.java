package mapreduce.master;

import mapreduce.common.LogUtil;
import mapreduce.common.Protocol;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Nœud coordinateur Map-Reduce.
 *
 * Responsabilités :
 *   1. Accepter les connexions des Workers (REGISTER)
 *   2. Accepter les soumissions de jobs des clients (SUBMIT)
 *   3. Orchestrer les phases MAP puis REDUCE
 *   4. Fusionner les résultats finaux
 *   5. Surveiller la santé des Workers via Monitor
 */
public class MasterNode {

    private static final Logger logger = Logger.getLogger("Master");

    private final String host;
    private final int    port;
    private final int    nbReducers;
    private final String outputDir;

    private final Map<String, WorkerInfo> workers = new LinkedHashMap<>();
    private final ReentrantLock lock = new ReentrantLock();
    private int workerCounter = 0;
    private int jobCounter    = 0;

    private final Monitor monitor;

    public MasterNode(String host, int port, int nbReducers, String outputDir) {
        this.host       = host;
        this.port       = port;
        this.nbReducers = nbReducers;
        this.outputDir  = outputDir;
        this.monitor    = new Monitor(5.0, 20.0);
        this.monitor.onWorkerFailed = this::onWorkerFailed;
        new File(outputDir).mkdirs();
    }

    // ── Serveur ───────────────────────────────────────────────────

    public void startServer() throws IOException {
        monitor.start();
        try (ServerSocket srv = new ServerSocket()) {
            srv.setReuseAddress(true);
            srv.bind(new InetSocketAddress(host, port));
            logger.info("Master @ " + host + ":" + port + " | " + nbReducers + " reducer(s)");

            while (true) {
                try {
                    Socket conn = srv.accept();
                    new Thread(() -> handleClient(conn), "client-handler").start();
                } catch (IOException e) {
                    logger.warning("Erreur accept: " + e.getMessage());
                }
            }
        } finally {
            monitor.stop();
        }
    }

    private void handleClient(Socket conn) {
        try (conn) {
            String raw = Protocol.receive(conn);
            Map<String, String> msg = Protocol.parseMessage(raw);
            String cmd = msg.getOrDefault("cmd", "");
            if      (Protocol.CMD_REGISTER.equals(cmd)) handleRegister(conn, msg);
            else if ("SUBMIT".equals(cmd))               handleSubmit(conn, msg);
            else if (Protocol.CMD_PING.equals(cmd))
                Protocol.send(conn, Protocol.buildPong("master"));
        } catch (Exception e) {
            logger.warning("Erreur client: " + e.getMessage());
        }
    }

    // ── Enregistrement des Workers ────────────────────────────────

    private void handleRegister(Socket conn, Map<String, String> msg) throws IOException {
        String wid;
        lock.lock();
        try { wid = "W" + (++workerCounter); }
        finally { lock.unlock(); }

        WorkerInfo info = new WorkerInfo(
            wid,
            msg.getOrDefault("type", "MAP").toUpperCase(),
            msg.getOrDefault("host", "127.0.0.1"),
            Integer.parseInt(msg.getOrDefault("port", "6000")),
            Integer.parseInt(msg.getOrDefault("partition", "-1"))
        );
        lock.lock();
        try { workers.put(wid, info); }
        finally { lock.unlock(); }

        monitor.addWorker(info);
        Protocol.send(conn, Protocol.buildAck(Map.of("worker_id", wid)));
        logger.info("Worker " + wid + " (" + info.workerType + ") @ " + info.host + ":" + info.port);
    }

    // ── Soumission de job ─────────────────────────────────────────

    private void handleSubmit(Socket conn, Map<String, String> msg) throws IOException {
        List<String> files = Arrays.stream(msg.getOrDefault("files", "").split(","))
                                   .filter(f -> !f.isBlank())
                                   .collect(Collectors.toList());
        if (files.isEmpty()) {
            Protocol.send(conn, "ERROR files=vide\n");
            return;
        }

        int jobId;
        lock.lock();
        try { jobId = ++jobCounter; }
        finally { lock.unlock(); }

        logger.info("\nJob " + jobId + " — " + files.size() + " fichier(s)");
        JobTracker tracker = new JobTracker(jobId, files, nbReducers);
        Protocol.send(conn, Protocol.buildAck(Map.of("job_id", String.valueOf(jobId))));

        try {
            if (!runMapPhase(tracker)) {
                Protocol.send(conn, "ERROR job_id=" + jobId + " phase=MAP\n");
                return;
            }
            if (!runReducePhase(tracker)) {
                Protocol.send(conn, "ERROR job_id=" + jobId + " phase=REDUCE\n");
                return;
            }
            String out = mergeResults(tracker);
            logger.info("Job " + jobId + " terminé → " + out + "\n" + tracker.summary());
            Protocol.send(conn, "RESULT job_id=" + jobId + " output=" + out + "\n");

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            Protocol.send(conn, "ERROR job_id=" + jobId + " phase=INTERRUPTED\n");
        }
    }

    // ── Phase MAP ─────────────────────────────────────────────────

    private boolean runMapPhase(JobTracker tracker) throws InterruptedException {
        logger.info("Phase MAP — " + tracker.files.size() + " fichier(s)");
        List<WorkerInfo> mapWorkers = getWorkers("MAP");
        if (mapWorkers.isEmpty()) {
            logger.warning("Aucun Worker MAP disponible !");
            return false;
        }

        ExecutorService pool = Executors.newFixedThreadPool(tracker.files.size());
        for (int i = 0; i < tracker.files.size(); i++) {
            String fp = tracker.files.get(i);
            WorkerInfo w = mapWorkers.get(i % mapWorkers.size());
            String tid = tracker.getMapTaskId(fp);
            tracker.markMapRunning(tid, w.workerId);
            pool.submit(() -> dispatchMap(w, tracker, tid, fp));
        }
        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.MINUTES);

        boolean ok = tracker.allMapsDone();
        logger.info(ok ? "MAP terminé." : "MAP : des tâches ont échoué.");
        return ok;
    }

    private void dispatchMap(WorkerInfo worker, JobTracker tracker,
                              String taskId, String filePath) {
        for (int attempt = 1; attempt <= 2; attempt++) {
            try {
                logger.info("  MAP " + new File(filePath).getName() +
                            " → " + worker.workerId + " (#" + attempt + ")");
                try (Socket s = new Socket(worker.host, worker.port)) {
                    s.setSoTimeout(120_000);
                    Protocol.send(s, Protocol.buildMapCommand(
                            tracker.jobId, filePath, nbReducers));
                    while (true) {
                        String raw = Protocol.receive(s);
                        if (raw == null || raw.isBlank()) break;
                        Map<String, String> resp = Protocol.parseMessage(raw);
                        String state = resp.getOrDefault("state", "");
                        if (Protocol.STATE_DONE.equals(state)) {
                            tracker.markMapDone(taskId, resp.getOrDefault("output", ""));
                            return;
                        } else if (Protocol.STATE_FAILED.equals(state)) {
                            logger.warning("  MAP FAILED: " + resp.get("error"));
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                logger.warning("  " + worker.workerId + ": " + e.getMessage());
            }
        }
        tracker.markMapFailed(taskId, "max tentatives atteintes");
    }

    // ── Phase REDUCE ──────────────────────────────────────────────

    private boolean runReducePhase(JobTracker tracker) throws InterruptedException {
        logger.info("Phase REDUCE — " + nbReducers + " partition(s)");
        List<WorkerInfo> rWorkers = getWorkers("REDUCE");
        List<WorkerInfo> mWorkers = getWorkers("MAP");
        if (rWorkers.isEmpty()) {
            logger.warning("Aucun Worker REDUCE disponible !");
            return false;
        }

        List<String> mapperAddrs = mWorkers.stream()
                .map(w -> w.host + ":" + w.port)
                .collect(Collectors.toList());

        ExecutorService pool = Executors.newFixedThreadPool(nbReducers);
        for (int p = 0; p < nbReducers; p++) {
            final int partition = p;
            WorkerInfo w = rWorkers.get(p % rWorkers.size());
            tracker.markReduceRunning(p, w.workerId);
            pool.submit(() -> dispatchReduce(w, tracker, partition, mapperAddrs));
        }
        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.MINUTES);

        boolean ok = tracker.allReducesDone();
        logger.info(ok ? "REDUCE terminé." : "REDUCE : des tâches ont échoué.");
        return ok;
    }

    private void dispatchReduce(WorkerInfo worker, JobTracker tracker,
                                 int partition, List<String> mapperAddrs) {
        for (int attempt = 1; attempt <= 2; attempt++) {
            try {
                logger.info("  REDUCE p=" + partition + " → " + worker.workerId +
                            " (#" + attempt + ")");
                try (Socket s = new Socket(worker.host, worker.port)) {
                    s.setSoTimeout(120_000);
                    Protocol.send(s, Protocol.buildReduceCommand(
                            tracker.jobId, partition, tracker.files.size(), mapperAddrs));
                    while (true) {
                        String raw = Protocol.receive(s);
                        if (raw == null || raw.isBlank()) break;
                        Map<String, String> resp = Protocol.parseMessage(raw);
                        String state = resp.getOrDefault("state", "");
                        if (Protocol.STATE_DONE.equals(state)) {
                            tracker.markReduceDone(partition, resp.getOrDefault("output", ""));
                            return;
                        } else if (Protocol.STATE_FAILED.equals(state)) {
                            logger.warning("  REDUCE p=" + partition + " FAILED: " + resp.get("error"));
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                logger.warning("  " + worker.workerId + ": " + e.getMessage());
            }
        }
        tracker.markReduceFailed(partition, "max tentatives atteintes");
    }

    // ── Fusion des résultats ──────────────────────────────────────

    private String mergeResults(JobTracker tracker) throws IOException {
        Map<String, Integer> counts = new TreeMap<>();
        for (String path : tracker.getReduceOutputs()) {
            if (path == null || path.isBlank() || !new File(path).exists()) {
                logger.warning("Sortie manquante : " + path);
                continue;
            }
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(new FileInputStream(path), "UTF-8"))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split("\t");
                    if (parts.length == 2) {
                        try { counts.merge(parts[0], Integer.parseInt(parts[1]), Integer::sum); }
                        catch (NumberFormatException ignored) {}
                    }
                }
            }
        }

        String out = outputDir + "/wordcount_job_" + tracker.jobId + ".txt";
        try (PrintWriter pw = new PrintWriter(
                new OutputStreamWriter(new FileOutputStream(out), "UTF-8"))) {
            counts.forEach((w, c) -> pw.println(w + "\t" + c));
        }
        logger.info(counts.size() + " mots uniques → " + out);
        return out;
    }

    // ── Gestion des pannes ────────────────────────────────────────

    private void onWorkerFailed(String workerId) {
        logger.warning("Worker " + workerId + " déclaré mort.");
        lock.lock();
        try { if (workers.containsKey(workerId)) workers.get(workerId).alive = false; }
        finally { lock.unlock(); }
    }

    private List<WorkerInfo> getWorkers(String wtype) {
        lock.lock();
        try {
            return workers.values().stream()
                    .filter(w -> w.workerType.equals(wtype) && w.alive)
                    .collect(Collectors.toList());
        } finally { lock.unlock(); }
    }

    // ── Main ──────────────────────────────────────────────────────

    public static void main(String[] args) throws Exception {
        LogUtil.setup();

        String host      = "0.0.0.0";
        int    port      = 5000;
        int    reducers  = 3;
        String outputDir = "./output";

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--host"       -> host      = args[++i];
                case "--port"       -> port      = Integer.parseInt(args[++i]);
                case "--reducers"   -> reducers  = Integer.parseInt(args[++i]);
                case "--output-dir" -> outputDir = args[++i];
            }
        }

        new MasterNode(host, port, reducers, outputDir).startServer();
    }
}
