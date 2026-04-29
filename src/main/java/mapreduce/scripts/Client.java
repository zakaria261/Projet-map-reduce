package mapreduce.scripts;

import mapreduce.common.LogUtil;
import mapreduce.common.Protocol;

import java.io.*;
import java.net.Socket;
import java.util.*;

/**
 * Client de soumission de jobs au Master.
 * Permet d'envoyer une liste de fichiers à traiter et d'attendre le résultat.
 *
 * Usage :
 *   java mapreduce.scripts.Client [--host <h>] [--port <p>] <fichier1> [fichier2 ...]
 */
public class Client {

    public static void main(String[] args) throws Exception {
        LogUtil.setup();

        String masterHost = "127.0.0.1";
        int    masterPort = 5000;
        List<String> files = new ArrayList<>();

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--host" -> masterHost = args[++i];
                case "--port" -> masterPort = Integer.parseInt(args[++i]);
                default       -> files.add(new File(args[i]).getAbsolutePath());
            }
        }

        if (files.isEmpty()) {
            System.out.println("Usage : Client [--host <h>] [--port <p>] <fichier1> ...");
            return;
        }

        // Validation des fichiers
        List<String> validFiles = new ArrayList<>();
        for (String f : files) {
            if (new File(f).exists()) validFiles.add(f);
            else System.out.println("Fichier ignoré (introuvable) : " + f);
        }
        if (validFiles.isEmpty()) {
            System.out.println("Aucun fichier valide à traiter.");
            return;
        }

        submitJob(masterHost, masterPort, validFiles);
    }

    private static void submitJob(String masterHost, int masterPort,
                                   List<String> files) {
        System.out.println("Envoi du job au Master (" + masterHost + ":" + masterPort + ")...");

        try (Socket sock = new Socket(masterHost, masterPort)) {
            sock.setSoTimeout(300_000);

            // Construction de la commande SUBMIT
            String filesStr = String.join(",", files);
            Protocol.send(sock, "SUBMIT files=" + filesStr + "\n");

            // Attente de l'ACK
            String resp = Protocol.receive(sock);
            Map<String, String> msg = Protocol.parseMessage(resp);

            if (Protocol.CMD_ACK.equals(msg.get("cmd"))) {
                String jobId = msg.get("job_id");
                System.out.println("Job " + jobId + " accepté. Traitement en cours...");
            } else {
                System.out.println("Erreur lors de la soumission : " + resp);
                return;
            }

            // Attente du RESULT final
            long start = System.currentTimeMillis();
            while (true) {
                resp = Protocol.receive(sock);
                if (resp == null || resp.isBlank()) break;

                msg = Protocol.parseMessage(resp);
                String cmd = msg.getOrDefault("cmd", "");

                if ("RESULT".equals(cmd)) {
                    double elapsed = (System.currentTimeMillis() - start) / 1000.0;
                    System.out.printf("Job %s terminé en %.2fs !%n", msg.get("job_id"), elapsed);
                    String out = msg.get("output");
                    System.out.println("Résultat disponible : " + out);

                    // Aperçu des 10 premiers résultats
                    if (out != null && new File(out).exists()) {
                        System.out.println("\nAperçu des 10 premiers résultats :");
                        try (BufferedReader br = new BufferedReader(new FileReader(out))) {
                            String line;
                            int count = 0;
                            while ((line = br.readLine()) != null && count++ < 10)
                                System.out.println("   " + line);
                        }
                    }
                    break;

                } else if ("ERROR".equals(cmd)) {
                    System.out.println("Le job a échoué : " + resp);
                    break;

                } else {
                    System.out.println("[Master] " + resp);
                }
            }

        } catch (Exception e) {
            System.out.println("Erreur de connexion au Master : " + e.getMessage());
        }
    }
}
