package mapreduce.master;

/**
 * Représente une tâche MAP ou REDUCE individuelle.
 * Thread-safe via volatile sur les champs mutables.
 */
public class TaskInfo {

    public final    String    taskId;
    public volatile String    workerId;
    public volatile TaskState state;
    public volatile long      startTime;
    public volatile long      endTime;
    public volatile String    output;
    public volatile String    error;
    public volatile int       attempts;

    public TaskInfo(String taskId, String workerId) {
        this.taskId   = taskId;
        this.workerId = workerId;
        this.state    = TaskState.PENDING;
        this.output   = "";
        this.error    = "";
    }

    /** Durée de la tâche en secondes. */
    public double duration() {
        if (endTime > 0) return (endTime - startTime) / 1000.0;
        return startTime > 0 ? (System.currentTimeMillis() - startTime) / 1000.0 : 0.0;
    }

    public void markRunning(String wid) {
        this.workerId  = wid;
        this.state     = TaskState.RUNNING;
        this.startTime = System.currentTimeMillis();
        this.attempts++;
    }

    public void markDone(String out) {
        this.state   = TaskState.DONE;
        this.endTime = System.currentTimeMillis();
        this.output  = out != null ? out : "";
    }

    public void markFailed(String err) {
        this.state   = TaskState.FAILED;
        this.endTime = System.currentTimeMillis();
        this.error   = err != null ? err : "";
    }
}
