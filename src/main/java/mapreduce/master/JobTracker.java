package mapreduce.master;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Gère l'état global d'un job Map-Reduce.
 * Thread-safe via ReentrantLock.
 */
public class JobTracker {

    public final int          jobId;
    public final List<String> files;
    public final int          nbReducers;
    private final long        startTime;
    private final ReentrantLock lock = new ReentrantLock();

    // {taskId → TaskInfo}
    public final Map<String, TaskInfo> mapTasks    = new LinkedHashMap<>();
    public final Map<String, TaskInfo> reduceTasks = new LinkedHashMap<>();

    // Permet de retrouver le taskId depuis le chemin de fichier
    private final Map<String, String> mapFileIndex = new HashMap<>();

    public JobTracker(int jobId, List<String> files, int nbReducers) {
        this.jobId      = jobId;
        this.files      = files;
        this.nbReducers = nbReducers;
        this.startTime  = System.currentTimeMillis();

        // Initialise les tâches MAP (une par fichier)
        for (int i = 0; i < files.size(); i++) {
            String tid = "map_" + jobId + "_" + i;
            mapTasks.put(tid, new TaskInfo(tid, ""));
            mapFileIndex.put(files.get(i), tid);
        }

        // Initialise les tâches REDUCE (une par partition)
        for (int p = 0; p < nbReducers; p++) {
            String tid = "reduce_" + jobId + "_" + p;
            reduceTasks.put(tid, new TaskInfo(tid, ""));
        }
    }

    // ── MAP ───────────────────────────────────────────────────────

    public String getMapTaskId(String filePath) {
        return mapFileIndex.get(filePath);
    }

    public void markMapRunning(String taskId, String workerId) {
        lock.lock();
        try {
            if (mapTasks.containsKey(taskId)) mapTasks.get(taskId).markRunning(workerId);
        } finally { lock.unlock(); }
    }

    public void markMapDone(String taskId, String output) {
        lock.lock();
        try {
            if (mapTasks.containsKey(taskId)) mapTasks.get(taskId).markDone(output);
        } finally { lock.unlock(); }
    }

    public void markMapFailed(String taskId, String error) {
        lock.lock();
        try {
            if (mapTasks.containsKey(taskId)) {
                mapTasks.get(taskId).markFailed(error);
                mapTasks.get(taskId).state = TaskState.PENDING; // permet la relance
            }
        } finally { lock.unlock(); }
    }

    public boolean allMapsDone() {
        lock.lock();
        try {
            return mapTasks.values().stream().allMatch(t -> t.state == TaskState.DONE);
        } finally { lock.unlock(); }
    }

    // ── REDUCE ────────────────────────────────────────────────────

    public String getReduceTaskId(int partition) {
        return "reduce_" + jobId + "_" + partition;
    }

    public void markReduceRunning(int partition, String workerId) {
        String tid = getReduceTaskId(partition);
        lock.lock();
        try {
            if (reduceTasks.containsKey(tid)) reduceTasks.get(tid).markRunning(workerId);
        } finally { lock.unlock(); }
    }

    public void markReduceDone(int partition, String output) {
        String tid = getReduceTaskId(partition);
        lock.lock();
        try {
            if (reduceTasks.containsKey(tid)) reduceTasks.get(tid).markDone(output);
        } finally { lock.unlock(); }
    }

    public void markReduceFailed(int partition, String error) {
        String tid = getReduceTaskId(partition);
        lock.lock();
        try {
            if (reduceTasks.containsKey(tid)) {
                reduceTasks.get(tid).markFailed(error);
                reduceTasks.get(tid).state = TaskState.PENDING; // permet la relance
            }
        } finally { lock.unlock(); }
    }

    public boolean allReducesDone() {
        lock.lock();
        try {
            return reduceTasks.values().stream().allMatch(t -> t.state == TaskState.DONE);
        } finally { lock.unlock(); }
    }

    /** Retourne les chemins de sortie de tous les Reducers (dans l'ordre des partitions). */
    public List<String> getReduceOutputs() {
        List<String> outputs = new ArrayList<>();
        for (int p = 0; p < nbReducers; p++) {
            outputs.add(reduceTasks.get(getReduceTaskId(p)).output);
        }
        return outputs;
    }

    // ── Statistiques ──────────────────────────────────────────────

    public double getProgress() {
        lock.lock();
        try {
            long total = mapTasks.size() + reduceTasks.size();
            long done  = mapTasks.values().stream().filter(t -> t.state == TaskState.DONE).count()
                       + reduceTasks.values().stream().filter(t -> t.state == TaskState.DONE).count();
            return total > 0 ? (done * 100.0 / total) : 0.0;
        } finally { lock.unlock(); }
    }

    public String summary() {
        lock.lock();
        try {
            long mapDone    = mapTasks.values().stream().filter(t -> t.state == TaskState.DONE).count();
            long reduceDone = reduceTasks.values().stream().filter(t -> t.state == TaskState.DONE).count();
            double elapsed  = (System.currentTimeMillis() - startTime) / 1000.0;
            return String.format(
                "Job %d | MAP %d/%d | REDUCE %d/%d | Progression %.1f%% | Durée %.1fs",
                jobId, mapDone, mapTasks.size(), reduceDone, nbReducers, getProgress(), elapsed);
        } finally { lock.unlock(); }
    }
}
