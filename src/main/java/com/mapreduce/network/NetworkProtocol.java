package com.mapreduce.network;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/**
 * Protocole de communication du cluster Map-Reduce.
 *
 * ┌──────────────────────────────────────────────────────────────┐
 * │  FORMAT : COMMAND|ARG1|ARG2|...\n   (UTF-8, une ligne)      │
 * │  SÉPARATEUR : pipe "|"                                       │
 * │  TERMINAISON : saut de ligne "\n"                            │
 * └──────────────────────────────────────────────────────────────┘
 *
 * Tableau des commandes :
 * ┌─────────────────┬───────────────────┬──────────────────────────────────────────┐
 * │  Commande       │  Direction        │  Format complet                          │
 * ├─────────────────┼───────────────────┼──────────────────────────────────────────┤
 * │  REGISTER       │  Worker → Master  │  REGISTER|host|port                      │
 * │  ACK            │  Master → Worker  │  ACK|message                             │
 * │  MAP_TASK       │  Master → Worker  │  MAP_TASK|jobId|filePath|nbR|outputDir   │
 * │  REDUCE_TASK    │  Master → Worker  │  REDUCE_TASK|jobId|part|outDir|f1,f2,... │
 * │  PING           │  Master → Worker  │  PING                                    │
 * │  PONG           │  Worker → Master  │  PONG|workerId                           │
 * │  TASK_DONE      │  Worker → Master  │  TASK_DONE|taskId|outputPath             │
 * │  TASK_FAILED    │  Worker → Master  │  TASK_FAILED|taskId|errorMessage         │
 * └─────────────────┴───────────────────┴──────────────────────────────────────────┘
 */
public final class NetworkProtocol {

    // ── Séparateur et terminaison ──────────────────────────────────
    public static final String SEP       = "|";
    public static final char   NEWLINE   = '\n';

    // ── Commandes ──────────────────────────────────────────────────
    public static final String CMD_REGISTER    = "REGISTER";
    public static final String CMD_ACK         = "ACK";
    public static final String CMD_MAP_TASK    = "MAP_TASK";
    public static final String CMD_REDUCE_TASK = "REDUCE_TASK";
    public static final String CMD_PING        = "PING";
    public static final String CMD_PONG        = "PONG";
    public static final String CMD_TASK_DONE   = "TASK_DONE";
    public static final String CMD_TASK_FAILED = "TASK_FAILED";

    private static final DateTimeFormatter TIME_FMT =
        DateTimeFormatter.ofPattern("HH:mm:ss");

    private NetworkProtocol() {}

    // ── Constructeurs de messages ──────────────────────────────────

    /**
     * REGISTER|host|port
     * Envoyé par un Worker pour s'annoncer au Master.
     */
    public static String register(String host, int port) {
        return CMD_REGISTER + SEP + host + SEP + port;
    }

    /**
     * ACK|message
     * Confirmation générique envoyée par le Master.
     */
    public static String ack(String message) {
        return CMD_ACK + SEP + message;
    }

    /**
     * MAP_TASK|jobId|filePath|nbReducers|outputDir
     * Ordonne à un Worker d'exécuter une tâche MAP.
     */
    public static String mapTask(String jobId, String filePath,
                                  int nbReducers, String outputDir) {
        return CMD_MAP_TASK + SEP + jobId + SEP + filePath +
               SEP + nbReducers + SEP + outputDir;
    }

    /**
     * REDUCE_TASK|jobId|partition|outputDir|file1,file2,...
     * Ordonne à un Worker d'exécuter une tâche REDUCE.
     * Les fichiers de partition sont séparés par des virgules.
     */
    public static String reduceTask(String jobId, int partition,
                                     String outputDir, String... partitionFiles) {
        return CMD_REDUCE_TASK + SEP + jobId + SEP + partition +
               SEP + outputDir + SEP + String.join(",", partitionFiles);
    }

    /** PING — vérifie qu'un Worker est en vie. */
    public static String ping() {
        return CMD_PING;
    }

    /**
     * PONG|workerId
     * Réponse d'un Worker à un PING.
     */
    public static String pong(String workerId) {
        return CMD_PONG + SEP + workerId;
    }

    /**
     * TASK_DONE|taskId|outputPath
     * Signale au Master la fin d'une tâche (MAP ou REDUCE).
     */
    public static String taskDone(String taskId, String outputPath) {
        return CMD_TASK_DONE + SEP + taskId + SEP + outputPath;
    }

    /**
     * TASK_FAILED|taskId|errorMessage
     * Signale au Master l'échec d'une tâche.
     */
    public static String taskFailed(String taskId, String errorMessage) {
        return CMD_TASK_FAILED + SEP + taskId + SEP +
               errorMessage.replace(SEP, "_"); // évite les ambiguïtés de parsing
    }

    // ── Parsing ────────────────────────────────────────────────────

    /**
     * Découpe un message en tableau de tokens.
     *
     * Exemple :
     *   "MAP_TASK|42|/data/a.txt|3|/tmp" → ["MAP_TASK","42","/data/a.txt","3","/tmp"]
     *
     * @param message message brut reçu
     * @return tableau de tokens (jamais null, au moins 1 élément)
     */
    public static String[] parse(String message) {
        if (message == null || message.isBlank()) {
            return new String[]{""};
        }
        return message.trim().split("\\|", -1);
    }

    // ── Envoi / Réception bas niveau ───────────────────────────────

    /**
     * Envoie un message sur la socket.
     *
     * Le message est encodé en UTF-8 et terminé par un saut de ligne.
     * Un flush est effectué immédiatement pour garantir l'envoi.
     *
     * @param socket  socket de destination
     * @param message message à envoyer (sans "\n" final — ajouté automatiquement)
     */
    public static void send(Socket socket, String message) throws IOException {
        OutputStream out = socket.getOutputStream();
        // On écrit message + \n en une seule passe pour éviter plusieurs write()
        byte[] bytes = (message.trim() + NEWLINE).getBytes(StandardCharsets.UTF_8);
        out.write(bytes);
        out.flush();
    }

    /**
     * Reçoit une ligne complète depuis la socket (jusqu'au '\n').
     *
     * Lecture octet par octet pour ne pas consommer de données
     * au-delà de la ligne courante (pas de buffer interne caché).
     *
     * @param socket socket source
     * @return message reçu, sans le '\n' final (null si connexion fermée)
     */
    public static String receive(Socket socket) throws IOException {
        InputStream in = socket.getInputStream();
        ByteArrayOutputStream buf = new ByteArrayOutputStream(256);
        int b;
        while ((b = in.read()) != -1) {
            if (b == NEWLINE) break;
            buf.write(b);
        }
        if (buf.size() == 0 && b == -1) return null; // connexion fermée
        return buf.toString(StandardCharsets.UTF_8).trim();
    }

    // ── Horodatage pour les logs ───────────────────────────────────

    /** Retourne l'heure courante au format HH:mm:ss (pour les logs). */
    public static String timestamp() {
        return LocalTime.now().format(TIME_FMT);
    }
}
