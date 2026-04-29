package mapreduce.master;

/** Informations sur un Worker enregistré auprès du Master. */
public class WorkerInfo {

    public final    String  workerId;
    public final    String  workerType;   // "MAP" ou "REDUCE"
    public final    String  host;
    public final    int     port;
    public final    int     partition;    // -1 pour les MapWorkers
    public volatile long    lastHeartbeat;
    public volatile boolean alive;

    public WorkerInfo(String workerId, String workerType,
                      String host, int port, int partition) {
        this.workerId      = workerId;
        this.workerType    = workerType;
        this.host          = host;
        this.port          = port;
        this.partition     = partition;
        this.lastHeartbeat = System.currentTimeMillis();
        this.alive         = true;
    }

    @Override
    public String toString() {
        return workerId + "(" + workerType + ")@" + host + ":" + port;
    }
}
