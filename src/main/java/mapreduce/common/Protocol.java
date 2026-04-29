package mapreduce.common;

import java.io.*;
import java.net.Socket;
import java.util.*;

/**
 * Protocole de communication textuel basé sur Sockets TCP.
 * Toutes les commandes sont en UTF-8, une par ligne, terminée par \n.
 *
 * Format général : COMMANDE clé1=valeur1 clé2=valeur2 ...\n
 */
public class Protocol {

    public static final String ENCODING    = "UTF-8";
    public static final String DELIMITER   = "\n";
    public static final int    BUFFER_SIZE = 4096;

    // Master → Worker
    public static final String CMD_MAP    = "MAP";
    public static final String CMD_REDUCE = "REDUCE";
    public static final String CMD_PING   = "PING";
    public static final String CMD_ABORT  = "ABORT";
    public static final String CMD_GET    = "GET";  // Reducer → Mapper : demande les spill files

    // Worker → Master
    public static final String CMD_STATUS   = "STATUS";
    public static final String CMD_PONG     = "PONG";
    public static final String CMD_ACK      = "ACK";
    public static final String CMD_REGISTER = "REGISTER";

    // États d'une tâche
    public static final String STATE_RUNNING = "RUNNING";
    public static final String STATE_DONE    = "DONE";
    public static final String STATE_FAILED  = "FAILED";

    // ── Décodage des messages ──────────────────────────────────────

    /**
     * Parse une ligne de protocole en Map.
     *
     * Exemples :
     *   "MAP job_id=42 file=/data/a.txt nb_reducers=3"
     *   → {"cmd":"MAP", "job_id":"42", "file":"/data/a.txt", "nb_reducers":"3"}
     *
     *   "PING"
     *   → {"cmd":"PING"}
     */
    public static Map<String, String> parseMessage(String line) {
        Map<String, String> result = new LinkedHashMap<>();
        if (line == null || line.isBlank()) return result;
        String[] parts = line.strip().split("\\s+");
        result.put("cmd", parts[0].toUpperCase());
        for (int i = 1; i < parts.length; i++) {
            String token = parts[i];
            int eq = token.indexOf('=');
            if (eq >= 0) {
                result.put(token.substring(0, eq), token.substring(eq + 1));
            }
        }
        return result;
    }

    // ── Constructeurs de messages ──────────────────────────────────

    /** MAP job_id=<id> file=<path> nb_reducers=<N> */
    public static String buildMapCommand(int jobId, String filePath, int nbReducers) {
        return "MAP job_id=" + jobId + " file=" + filePath +
               " nb_reducers=" + nbReducers + DELIMITER;
    }

    /**
     * REDUCE job_id=<id> partition=<P> nb_mappers=<N> mappers=host:port,host:port,...
     */
    public static String buildReduceCommand(int jobId, int partition, int nbMappers,
                                             List<String> mapperAddresses) {
        String mappers = String.join(",", mapperAddresses);
        return "REDUCE job_id=" + jobId + " partition=" + partition +
               " nb_mappers=" + nbMappers + " mappers=" + mappers + DELIMITER;
    }

    /**
     * STATUS job_id=<id> state=<STATE> [clés supplémentaires...]
     * Ex: STATUS job_id=42 state=DONE output=/tmp/job_42/result.txt
     */
    public static String buildStatus(int jobId, String state, Map<String, String> extras) {
        StringBuilder sb = new StringBuilder("STATUS job_id=")
                .append(jobId).append(" state=").append(state);
        if (extras != null) {
            extras.forEach((k, v) -> sb.append(' ').append(k).append('=').append(v));
        }
        return sb.append(DELIMITER).toString();
    }

    public static String buildPing() {
        return "PING" + DELIMITER;
    }

    public static String buildPong(String workerId) {
        return "PONG worker_id=" + workerId + DELIMITER;
    }

    public static String buildAck(Map<String, String> extras) {
        StringBuilder sb = new StringBuilder("ACK");
        if (extras != null) {
            extras.forEach((k, v) -> sb.append(' ').append(k).append('=').append(v));
        }
        return sb.append(DELIMITER).toString();
    }

    /** REGISTER type=MAP host=... port=... [partition=P] */
    public static String buildRegister(String workerType, String host, int port,
                                        Map<String, String> extras) {
        StringBuilder sb = new StringBuilder("REGISTER type=").append(workerType)
                .append(" host=").append(host).append(" port=").append(port);
        if (extras != null) {
            extras.forEach((k, v) -> sb.append(' ').append(k).append('=').append(v));
        }
        return sb.append(DELIMITER).toString();
    }

    /** GET job_id=<id> partition=<P> — demande les spill files au Mapper */
    public static String buildGet(int jobId, int partition) {
        return "GET job_id=" + jobId + " partition=" + partition + DELIMITER;
    }

    public static String buildAbort(int jobId) {
        return "ABORT job_id=" + jobId + DELIMITER;
    }

    // ── Envoi / Réception sur socket ──────────────────────────────

    /** Envoie un message encodé en UTF-8 sur la socket. */
    public static void send(Socket socket, String message) throws IOException {
        if (!message.endsWith(DELIMITER)) message += DELIMITER;
        OutputStream out = socket.getOutputStream();
        out.write(message.getBytes(ENCODING));
        out.flush();
    }

    /**
     * Reçoit une ligne complète (jusqu'au \n).
     * Retourne la ligne décodée sans le délimiteur.
     */
    public static String receive(Socket socket) throws IOException {
        InputStream in = socket.getInputStream();
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        int b;
        while ((b = in.read()) != -1) {
            buf.write(b);
            if (b == '\n') break;
        }
        return buf.toString(ENCODING).strip();
    }

    /**
     * Reçoit un bloc de données jusqu'au marqueur "EOF\n".
     * Utile pour recevoir les spill files complets des Mappers.
     */
    public static byte[] receiveUntilEof(Socket socket) throws IOException {
        InputStream in = socket.getInputStream();
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        byte[] chunk = new byte[BUFFER_SIZE];
        socket.setSoTimeout(10_000);
        try {
            int n;
            while ((n = in.read(chunk)) != -1) {
                buf.write(chunk, 0, n);
                if (buf.toString(ENCODING).contains("EOF\n")) break;
            }
        } catch (java.net.SocketTimeoutException ignored) {}
        return buf.toByteArray();
    }
}
