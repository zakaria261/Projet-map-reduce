package mapreduce.scripts;

import java.util.ArrayList;
import java.util.List;

/**
 * Lance localement un cluster complet (Master + Workers).
 *
 * Usage :
 *   java mapreduce.scripts.LaunchCluster [--mappers <N>] [--reducers <N>]
 *
 * Ports utilisés :
 *   Master    : 5000
 *   MapWorker : 6001, 6002, ...
 *   Reducer   : 7001, 7002, ...
 */
public class LaunchCluster {

    private static final List<Process> processes = new ArrayList<>();

    public static void main(String[] args) throws Exception {
        int nbMappers  = 2;
        int nbReducers = 2;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--mappers"  -> nbMappers  = Integer.parseInt(args[++i]);
                case "--reducers" -> nbReducers = Integer.parseInt(args[++i]);
            }
        }

        // Arrêt propre sur Ctrl+C
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Arrêt du cluster...");
            processes.forEach(Process::destroyForcibly);
        }));

        String javaCmd  = ProcessHandle.current().info().command().orElse("java");
        String classpath = System.getProperty("java.class.path");

        System.out.println("Lancement du cluster (" + nbMappers +
                           " Mappers, " + nbReducers + " Reducers)...");

        // 1. Master
        System.out.println("Démarrage du Master sur le port 5000...");
        processes.add(new ProcessBuilder(
                javaCmd, "-cp", classpath, "mapreduce.master.MasterNode",
                "--reducers", String.valueOf(nbReducers))
                .inheritIO().start());
        Thread.sleep(2000);

        // 2. MapWorkers
        for (int i = 0; i < nbMappers; i++) {
            int port = 6001 + i;
            System.out.println("Démarrage du MapWorker " + i + " sur le port " + port + "...");
            processes.add(new ProcessBuilder(
                    javaCmd, "-cp", classpath, "mapreduce.worker.MapWorker",
                    "--port", String.valueOf(port))
                    .inheritIO().start());
        }

        // 3. ReduceWorkers
        for (int i = 0; i < nbReducers; i++) {
            int port = 7001 + i;
            System.out.println("Démarrage du ReduceWorker " + i +
                               " (partition " + i + ") sur le port " + port + "...");
            processes.add(new ProcessBuilder(
                    javaCmd, "-cp", classpath, "mapreduce.worker.ReduceWorker",
                    "--partition", String.valueOf(i),
                    "--port", String.valueOf(port))
                    .inheritIO().start());
        }

        System.out.println("\nCluster prêt. Appuyez sur Ctrl+C pour tout arrêter.");
        Thread.currentThread().join(); // attend indéfiniment
    }
}
