package mapreduce.master;

import mapreduce.common.Protocol;

import java.net.Socket;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.logging.Logger;

/**
 * Surveillance des Workers via PING périodique.
 * Détecte les pannes et notifie le MasterNode via un callback.
 */
public class Monitor {

    private static final Logger logger = Logger.getLogger("Monitor");

    private final double intervalSec;
    private final double timeoutSec;

    private final Map<String, WorkerInfo> workers = new LinkedHashMap<>();
    private final ReentrantLock lock = new ReentrantLock();

    private volatile boolean running = false;
    private Thread thread;

    /** Callback appelé quand un Worker est déclaré mort. */
    public Consumer<String> onWorkerFailed;

    public Monitor(double intervalSec, double timeoutSec) {
        this.intervalSec = intervalSec;
        this.timeoutSec  = timeoutSec;
    }

    // ── Gestion des Workers ───────────────────────────────────────

    public void addWorker(WorkerInfo info) {
        lock.lock();
        try { workers.put(info.workerId, info); }
        finally { lock.unlock(); }
        logger.info("[Monitor] Worker enregistré : " + info.workerId +
                    " (" + info.workerType + ") @ " + info.host + ":" + info.port);
    }

    public void removeWorker(String workerId) {
        lock.lock();
        try { workers.remove(workerId); }
        finally { lock.unlock(); }
    }

    public List<WorkerInfo> getAliveWorkers(String workerType) {
        lock.lock();
        try {
            List<WorkerInfo> result = new ArrayList<>();
            for (WorkerInfo w : workers.values()) {
                if (!w.alive) continue;
                if (workerType == null || workerType.equals(w.workerType)) result.add(w);
            }
            return result;
        } finally { lock.unlock(); }
    }

    // ── Heartbeat ─────────────────────────────────────────────────

    /** Envoie un PING à un Worker et attend PONG. Retourne true si vivant. */
    private boolean pingWorker(WorkerInfo info) {
        try (Socket s = new Socket(info.host, info.port)) {
            s.setSoTimeout(3_000);
            Protocol.send(s, Protocol.buildPing());
            String resp = Protocol.receive(s);
            Map<String, String> msg = Protocol.parseMessage(resp);
            return Protocol.CMD_PONG.equals(msg.get("cmd"));
        } catch (Exception e) {
            return false;
        }
    }

    /** Vérifie tous les Workers et marque les morts. */
    private void checkHeartbeats() {
        long now = System.currentTimeMillis();
        List<WorkerInfo> snapshot;
        lock.lock();
        try { snapshot = new ArrayList<>(workers.values()); }
        finally { lock.unlock(); }

        for (WorkerInfo info : snapshot) {
            if (!info.alive) continue;

            boolean responded = pingWorker(info);
            if (responded) {
                info.lastHeartbeat = now;
                logger.fine("[Monitor] PONG reçu de " + info.workerId);
            } else {
                double elapsed = (now - info.lastHeartbeat) / 1000.0;
                if (elapsed > timeoutSec) {
                    logger.warning("[Monitor] Worker " + info.workerId +
                                   " injoignable depuis " + String.format("%.1f", elapsed) +
                                   "s → déclaré MORT");
                    info.alive = false;
                    if (onWorkerFailed != null) onWorkerFailed.accept(info.workerId);
                }
            }
        }
    }

    // ── Cycle de vie ──────────────────────────────────────────────

    public void start() {
        running = true;
        thread  = new Thread(() -> {
            logger.info("[Monitor] Démarré (interval=" + intervalSec +
                        "s, timeout=" + timeoutSec + "s)");
            while (running) {
                checkHeartbeats();
                try { Thread.sleep((long)(intervalSec * 1000)); }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            logger.info("[Monitor] Arrêté.");
        }, "Monitor");
        thread.setDaemon(true);
        thread.start();
    }

    public void stop() {
        running = false;
        if (thread != null) thread.interrupt();
    }
}
