package mapreduce.worker;

import mapreduce.common.LogUtil;
import mapreduce.common.Partitioner;
import mapreduce.common.Protocol;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.Logger;

/**
 * Worker MAP du système Map-Reduce.
 *
 * Responsabilités :
 *   1. S'enregistrer auprès du Master
 *   2. Recevoir la commande MAP (fichier + nb_reducers)
 *   3. Tokeniser et compter localement (FNV-1a)
 *   4. Écrire les spill files partitionnés (un par Reducer)
 *   5. Servir les spill files aux Reducers sur demande (GET)
 *   6. Répondre aux PING du Monitor
 */
public class MapWorker {

    private static final Logger logger = Logger.getLogger("MapWorker");

    private final String host;
    private final int    port;
    private final String masterHost;
    private final int    masterPort;
    private final String tmpDir;

    private String workerId = "MAP_?";

    public MapWorker(String host, int port, String masterHost,
                     int masterPort, String tmpDir) {
        this.host       = host;
        this.port       = port;
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.tmpDir     = tmpDir;
        new File(tmpDir).mkdirs();
    }

    // ── Enregistrement ────────────────────────────────────────────

    public boolean register() {
        try (Socket s = new Socket(masterHost, masterPort)) {
            s.setSoTimeout(10_000);
            Protocol.send(s, Protocol.buildRegister("MAP", host, port, null));
            Map<String, String> resp = Protocol.parseMessage(Protocol.receive(s));
            if (Protocol.CMD_ACK.equals(resp.get("cmd"))) {
                workerId = resp.getOrDefault("worker_id", "MAP_?");
                logger.info("Enregistré comme " + workerId +
                            " auprès du Master " + masterHost + ":" + masterPort);
                return true;
            }
        } catch (Exception e) {
            logger.warning("Échec enregistrement : " + e.getMessage());
        }
        return false;
    }

    // ── Serveur d'écoute ──────────────────────────────────────────

    public void startServer() {
        if (!register()) {
            logger.severe("Impossible de s'enregistrer. Arrêt.");
            return;
        }
        try (ServerSocket srv = new ServerSocket()) {
            srv.setReuseAddress(true);
            srv.bind(new InetSocketAddress(host, port));
            logger.info("MapWorker " + workerId + " en écoute @ " + host + ":" + port);

            while (true) {
                Socket conn = srv.accept();
                new Thread(() -> handleConnection(conn), "map-conn").start();
            }
        } catch (IOException e) {
            logger.severe("Erreur serveur : " + e.getMessage());
        }
    }

    private void handleConnection(Socket conn) {
        try (conn) {
            String raw = Protocol.receive(conn);
            Map<String, String> msg = Protocol.parseMessage(raw);
            String cmd = msg.getOrDefault("cmd", "");
            if      (Protocol.CMD_MAP.equals(cmd))  handleMap(conn, msg);
            else if (Protocol.CMD_GET.equals(cmd))  handleGet(conn, msg);
            else if (Protocol.CMD_PING.equals(cmd)) Protocol.send(conn, Protocol.buildPong(workerId));
        } catch (Exception e) {
            logger.warning("Erreur connexion : " + e.getMessage());
        }
    }

    // ── Tâche MAP ─────────────────────────────────────────────────

    private void handleMap(Socket conn, Map<String, String> msg) throws IOException {
        String jobId     = msg.getOrDefault("job_id", "0");
        String filePath  = msg.getOrDefault("file", "");
        int nbReducers   = Integer.parseInt(msg.getOrDefault("nb_reducers", "3"));
        int jobIdInt     = Integer.parseInt(jobId);

        logger.info("MAP job=" + jobId + " fichier=" + new File(filePath).getName() +
                    " nb_reducers=" + nbReducers);

        Protocol.send(conn, Protocol.buildStatus(jobIdInt, Protocol.STATE_RUNNING,
                Map.of("progress", "0", "worker_id", workerId)));

        try {
            // 1. Lecture du fichier
            File f = new File(filePath);
            if (!f.exists()) throw new FileNotFoundException("Fichier introuvable : " + filePath);

            StringBuilder sb = new StringBuilder();
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(new FileInputStream(f), "UTF-8"))) {
                String line;
                while ((line = br.readLine()) != null) sb.append(line).append(' ');
            }
            String text = sb.toString();

            Protocol.send(conn, Protocol.buildStatus(jobIdInt, Protocol.STATE_RUNNING,
                    Map.of("progress", "30", "worker_id", workerId)));

            // 2. Tokenisation + comptage local
            List<String> tokens = Partitioner.tokenize(text);
            Map<String, Integer> counts = Partitioner.localCount(tokens);
            logger.info("  " + tokens.size() + " tokens → " + counts.size() + " mots uniques");

            Protocol.send(conn, Protocol.buildStatus(jobIdInt, Protocol.STATE_RUNNING,
                    Map.of("progress", "60", "worker_id", workerId)));

            // 3. Partitionnement + écriture des spill files
            Map<Integer, Map<String, Integer>> partitions =
                    Partitioner.partitionCounts(counts, nbReducers);
            String outDir = writeSpillFiles(jobId, partitions);

            Protocol.send(conn, Protocol.buildStatus(jobIdInt, Protocol.STATE_RUNNING,
                    Map.of("progress", "90", "worker_id", workerId)));

            // 4. Notification DONE
            Protocol.send(conn, Protocol.buildStatus(jobIdInt, Protocol.STATE_DONE,
                    Map.of("output", outDir, "worker_id", workerId)));
            logger.info("  MAP job=" + jobId + " terminé → " + outDir);

        } catch (Exception e) {
            logger.warning("  MAP FAILED: " + e.getMessage());
            Protocol.send(conn, Protocol.buildStatus(jobIdInt, Protocol.STATE_FAILED,
                    Map.of("error", e.getMessage().replace(" ", "_"),
                           "worker_id", workerId)));
        }
    }

    // ── Écriture des spill files ──────────────────────────────────

    /**
     * Écrit les spill files partitionnés sur disque.
     *
     * Structure : tmpDir/job_<jobId>/<workerId>/partition_<P>.txt
     * Format de chaque ligne : <mot>\t<count_local>
     */
    private String writeSpillFiles(String jobId,
                                    Map<Integer, Map<String, Integer>> partitions)
            throws IOException {
        String outDir = tmpDir + "/job_" + jobId + "/" + workerId;
        new File(outDir).mkdirs();

        for (Map.Entry<Integer, Map<String, Integer>> e : partitions.entrySet()) {
            String spillPath = outDir + "/partition_" + e.getKey() + ".txt";
            try (PrintWriter pw = new PrintWriter(new OutputStreamWriter(
                    new FileOutputStream(spillPath), "UTF-8"))) {
                e.getValue().entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(entry -> pw.println(entry.getKey() + "\t" + entry.getValue()));
            }
        }
        return outDir;
    }

    // ── Servir les spill files aux Reducers (Shuffle) ────────────

    /**
     * Répond à une requête GET d'un Reducer.
     *
     * Protocole :
     *   Reducer → Mapper : GET job_id=<id> partition=<P>
     *   Mapper → Reducer : <contenu du fichier>\nEOF\n
     */
    private void handleGet(Socket conn, Map<String, String> msg) throws IOException {
        String jobId     = msg.getOrDefault("job_id", "0");
        String partition = msg.getOrDefault("partition", "0");

        String spillPath = tmpDir + "/job_" + jobId + "/" + workerId +
                           "/partition_" + partition + ".txt";
        File f = new File(spillPath);

        OutputStream out = conn.getOutputStream();
        if (f.exists()) {
            try (FileInputStream fis = new FileInputStream(f)) {
                byte[] buf = new byte[Protocol.BUFFER_SIZE];
                int n;
                while ((n = fis.read(buf)) != -1) out.write(buf, 0, n);
            }
            logger.fine("  GET partition=" + partition + " → " + f.length() + " octets");
        } else {
            logger.warning("  GET : fichier introuvable " + spillPath);
        }
        out.write("EOF\n".getBytes("UTF-8"));
        out.flush();
    }

    // ── Main ──────────────────────────────────────────────────────

    public static void main(String[] args) throws Exception {
        LogUtil.setup();

        String host       = "127.0.0.1";
        int    port       = 6001;
        String masterHost = "127.0.0.1";
        int    masterPort = 5000;
        String tmpDir     = "./tmp";

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--host"        -> host       = args[++i];
                case "--port"        -> port       = Integer.parseInt(args[++i]);
                case "--master-host" -> masterHost = args[++i];
                case "--master-port" -> masterPort = Integer.parseInt(args[++i]);
                case "--tmp-dir"     -> tmpDir     = args[++i];
            }
        }
        new MapWorker(host, port, masterHost, masterPort, tmpDir).startServer();
    }
}
