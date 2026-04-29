package mapreduce.worker;

import mapreduce.common.LogUtil;
import mapreduce.common.Protocol;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.Logger;

/**
 * Worker REDUCE du système Map-Reduce.
 *
 * Responsabilités :
 *   1. S'enregistrer auprès du Master (avec son numéro de partition)
 *   2. Recevoir la commande REDUCE (partition + adresses des Mappers)
 *   3. Fetcher les spill files de chaque Mapper pour sa partition (Shuffle)
 *   4. Merger et agréger les comptages
 *   5. Écrire le fichier résultat final trié
 *   6. Répondre aux PING du Monitor
 */
public class ReduceWorker {

    private static final Logger logger = Logger.getLogger("ReduceWorker");

    private final int    partition;
    private final String host;
    private final int    port;
    private final String masterHost;
    private final int    masterPort;
    private final String tmpDir;

    private String workerId = "REDUCE_?";

    public ReduceWorker(int partition, String host, int port,
                         String masterHost, int masterPort, String tmpDir) {
        this.partition  = partition;
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
            Protocol.send(s, Protocol.buildRegister("REDUCE", host, port,
                    Map.of("partition", String.valueOf(partition))));
            Map<String, String> resp = Protocol.parseMessage(Protocol.receive(s));
            if (Protocol.CMD_ACK.equals(resp.get("cmd"))) {
                workerId = resp.getOrDefault("worker_id", "REDUCE_P" + partition);
                logger.info("Enregistré comme " + workerId + " (partition=" + partition + ")");
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
            logger.info("ReduceWorker " + workerId + " (p=" + partition + ") @ " +
                        host + ":" + port);
            while (true) {
                Socket conn = srv.accept();
                new Thread(() -> handleConnection(conn), "reduce-conn").start();
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
            if      (Protocol.CMD_REDUCE.equals(cmd)) handleReduce(conn, msg);
            else if (Protocol.CMD_PING.equals(cmd))   Protocol.send(conn, Protocol.buildPong(workerId));
        } catch (Exception e) {
            logger.warning("Erreur : " + e.getMessage());
        }
    }

    // ── Tâche REDUCE ──────────────────────────────────────────────

    private void handleReduce(Socket conn, Map<String, String> msg) throws IOException {
        String jobId      = msg.getOrDefault("job_id", "0");
        int    part       = Integer.parseInt(msg.getOrDefault("partition", String.valueOf(partition)));
        int    jobIdInt   = Integer.parseInt(jobId);
        String mappersRaw = msg.getOrDefault("mappers", "");

        // Parsing des adresses des Mappers : "host1:port1,host2:port2,..."
        List<String[]> mapperAddrs = new ArrayList<>();
        for (String entry : mappersRaw.split(",")) {
            if (entry.contains(":")) {
                int lastColon = entry.lastIndexOf(':');
                mapperAddrs.add(new String[]{
                    entry.substring(0, lastColon),
                    entry.substring(lastColon + 1)
                });
            }
        }

        logger.info("REDUCE job=" + jobId + " p=" + part + " " +
                    mapperAddrs.size() + " mapper(s)");
        Protocol.send(conn, Protocol.buildStatus(jobIdInt, Protocol.STATE_RUNNING,
                Map.of("progress", "0", "worker_id", workerId)));

        try {
            // 1. Fetch des spill files depuis chaque Mapper
            Map<String, Integer> merged = new HashMap<>();
            for (int i = 0; i < mapperAddrs.size(); i++) {
                String[] addr = mapperAddrs.get(i);
                Map<String, Integer> partial = fetchPartition(
                        addr[0], Integer.parseInt(addr[1]), jobId, part);
                partial.forEach((word, cnt) -> merged.merge(word, cnt, Integer::sum));

                int progress = (int)(40 + ((i + 1.0) / mapperAddrs.size()) * 40);
                Protocol.send(conn, Protocol.buildStatus(jobIdInt, Protocol.STATE_RUNNING,
                        Map.of("progress", String.valueOf(progress), "worker_id", workerId)));
            }
            logger.info("  " + merged.size() + " mots uniques après merge");

            // 2. Écriture du fichier résultat trié
            String outPath = writeResult(jobId, part, merged);

            Protocol.send(conn, Protocol.buildStatus(jobIdInt, Protocol.STATE_DONE,
                    Map.of("output", outPath, "worker_id", workerId)));
            logger.info("  REDUCE job=" + jobId + " p=" + part + " → " + outPath);

        } catch (Exception e) {
            logger.warning("  REDUCE FAILED: " + e.getMessage());
            Protocol.send(conn, Protocol.buildStatus(jobIdInt, Protocol.STATE_FAILED,
                    Map.of("error", e.getMessage().replace(" ", "_"),
                           "worker_id", workerId)));
        }
    }

    // ── Fetch d'un spill file depuis un Mapper ────────────────────

    /**
     * Se connecte au Mapper, demande le spill file pour cette partition.
     *
     * Protocole :
     *   → GET job_id=<id> partition=<P>
     *   ← <contenu>\nEOF
     *
     * @return Map {mot → count_local} agrégé depuis ce Mapper
     */
    private Map<String, Integer> fetchPartition(String host, int port,
                                                 String jobId, int part) {
        Map<String, Integer> counts = new HashMap<>();
        try (Socket s = new Socket(host, port)) {
            s.setSoTimeout(30_000);
            Protocol.send(s, Protocol.buildGet(Integer.parseInt(jobId), part));

            // Réception jusqu'au marqueur EOF
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            byte[] chunk = new byte[Protocol.BUFFER_SIZE];
            s.setSoTimeout(10_000);
            try {
                int n;
                InputStream in = s.getInputStream();
                while ((n = in.read(chunk)) != -1) {
                    buf.write(chunk, 0, n);
                    if (buf.toString("UTF-8").contains("EOF\n")) break;
                }
            } catch (SocketTimeoutException ignored) {}

            // Parsing du contenu
            String content = buf.toString("UTF-8");
            for (String line : content.split("\n")) {
                line = line.strip();
                if (line.equals("EOF") || line.isBlank()) continue;
                String[] parts = line.split("\t");
                if (parts.length == 2) {
                    try { counts.merge(parts[0], Integer.parseInt(parts[1]), Integer::sum); }
                    catch (NumberFormatException ignored) {}
                }
            }
        } catch (Exception e) {
            logger.warning("  Fetch depuis " + host + ":" + port +
                           " p=" + part + " échoué : " + e.getMessage());
        }
        return counts;
    }

    // ── Écriture du résultat ──────────────────────────────────────

    /**
     * Écrit le fichier de résultat trié.
     *
     * Structure : tmpDir/job_<jobId>/reducer_<workerId>/result_partition_<P>.txt
     * Format : <mot>\t<count_global>
     */
    private String writeResult(String jobId, int part,
                                Map<String, Integer> counts) throws IOException {
        String outDir = tmpDir + "/job_" + jobId + "/reducer_" + workerId;
        new File(outDir).mkdirs();
        String outPath = outDir + "/result_partition_" + part + ".txt";

        try (PrintWriter pw = new PrintWriter(
                new OutputStreamWriter(new FileOutputStream(outPath), "UTF-8"))) {
            counts.entrySet().stream()
                  .sorted(Map.Entry.comparingByKey())
                  .forEach(e -> pw.println(e.getKey() + "\t" + e.getValue()));
        }
        return outPath;
    }

    // ── Main ──────────────────────────────────────────────────────

    public static void main(String[] args) throws Exception {
        LogUtil.setup();

        int    partition  = 0;
        String host       = "127.0.0.1";
        int    port       = 7001;
        String masterHost = "127.0.0.1";
        int    masterPort = 5000;
        String tmpDir     = "./tmp";

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--partition"   -> partition  = Integer.parseInt(args[++i]);
                case "--host"        -> host       = args[++i];
                case "--port"        -> port       = Integer.parseInt(args[++i]);
                case "--master-host" -> masterHost = args[++i];
                case "--master-port" -> masterPort = Integer.parseInt(args[++i]);
                case "--tmp-dir"     -> tmpDir     = args[++i];
            }
        }
        new ReduceWorker(partition, host, port, masterHost, masterPort, tmpDir).startServer();
    }
}
