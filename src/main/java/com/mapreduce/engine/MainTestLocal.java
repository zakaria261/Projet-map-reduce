package com.mapreduce.engine;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

/**
 * Test local du pipeline Map-Reduce SANS réseau.
 *
 * Ce programme simule ce qu'un vrai cluster ferait :
 *
 *   ┌─────────────────────────────────────────────────────────────┐
 *   │  SIMULATION sur data/sample.txt avec 2 Mappers, 2 Reducers │
 *   └─────────────────────────────────────────────────────────────┘
 *
 *   Étape 1 — Lecture
 *     sample.txt est découpé en 2 chunks (moitiés du fichier)
 *     pour simuler 2 Mappers travaillant en parallèle.
 *
 *   Étape 2 — MAP (Mapper 1 et Mapper 2)
 *     Chaque Mapper appelle WordCountEngine.map() sur son chunk.
 *     Produit une Map { mot → count local }.
 *
 *   Étape 3 — PARTITIONNEMENT & SAUVEGARDE (spill files)
 *     Chaque Mapper répartit ses mots en N partitions via getPartition()
 *     et les écrit dans des fichiers temporaires.
 *
 *   Étape 4 — SHUFFLE (simulé localement)
 *     Normalement, les Reducers récupèrent leurs fichiers via le réseau.
 *     Ici, on regroupe simplement les fichiers par numéro de partition.
 *
 *   Étape 5 — REDUCE (Reducer 0 et Reducer 1)
 *     Chaque Reducer lit ses fichiers (un par Mapper) et fusionne
 *     les counts avec WordCountEngine.reduce().
 *
 *   Étape 6 — FUSION FINALE (rôle du Master)
 *     On combine les sorties des deux Reducers pour obtenir le
 *     résultat global trié alphabétiquement.
 */
public class MainTestLocal {

    /** Nombre de Reducers (= nombre de partitions par Mapper). */
    private static final int NB_REDUCERS = 2;

    /** Répertoire de travail pour les fichiers temporaires. */
    private static final Path WORK_DIR = Paths.get("./tmp/local_test");

    public static void main(String[] args) throws IOException {
        // ── Résolution du chemin de sample.txt ────────────────────
        // Fonctionne que le programme soit lancé depuis la racine
        // du projet ou depuis n'importe quel sous-répertoire.
        Path sampleFile = resolveSampleFile();
        System.out.println("═".repeat(60));
        System.out.println("  TEST LOCAL MAP-REDUCE — WordCount");
        System.out.println("  Fichier : " + sampleFile.toAbsolutePath());
        System.out.println("  Reducers: " + NB_REDUCERS);
        System.out.println("═".repeat(60));

        // ══════════════════════════════════════════════════════════
        // ÉTAPE 1 : Lecture et découpage du fichier en chunks
        // ══════════════════════════════════════════════════════════
        System.out.println("\n[1/5] Lecture et découpage du fichier...");
        String fullContent = Files.readString(sampleFile, StandardCharsets.UTF_8);
        List<String> chunks = splitIntoChunks(fullContent, 2);
        System.out.printf("  %d ligne(s) découpées en %d chunk(s)%n",
            fullContent.split("\n").length, chunks.size());

        // ══════════════════════════════════════════════════════════
        // ÉTAPE 2 + 3 : MAP + SAUVEGARDE DES PARTITIONS
        // Chaque Mapper traite son chunk et écrit ses spill files.
        // ══════════════════════════════════════════════════════════
        System.out.println("\n[2/5] Phase MAP + écriture des partitions...");

        // spillFiles[mapperId][partitionId] = chemin du fichier
        List<List<Path>> spillFiles = new ArrayList<>();

        for (int mapperId = 0; mapperId < chunks.size(); mapperId++) {
            System.out.println("\n  ── Mapper " + mapperId + " ──");

            // MAP : comptage local des mots du chunk
            Map<String, Integer> localCounts = WordCountEngine.map(chunks.get(mapperId));
            System.out.printf("  %d mot(s) uniques trouvés localement%n",
                localCounts.size());

            // SAUVEGARDE : écriture des spill files partitionnés
            Path mapperOutputDir = WORK_DIR.resolve("mapper_" + mapperId);
            List<Path> mapperFiles = WordCountEngine.savePartitions(
                localCounts, NB_REDUCERS, mapperOutputDir);

            spillFiles.add(mapperFiles);
        }

        // ══════════════════════════════════════════════════════════
        // ÉTAPE 4 : SHUFFLE — regroupement des fichiers par partition
        // Dans un vrai cluster : les Reducers font GET sur chaque Mapper.
        // Ici : on regroupe simplement les chemins par index de partition.
        // ══════════════════════════════════════════════════════════
        System.out.println("\n[3/5] Shuffle (regroupement des partitions par Reducer)...");

        // reducerInputs[partitionId] = liste des fichiers à lire
        List<List<Path>> reducerInputs = new ArrayList<>();
        for (int p = 0; p < NB_REDUCERS; p++) {
            List<Path> filesForReducer = new ArrayList<>();
            for (List<Path> mapperSpills : spillFiles) {
                // Chaque Mapper a produit un fichier pour cette partition
                filesForReducer.add(mapperSpills.get(p));
            }
            reducerInputs.add(filesForReducer);
            System.out.printf("  Reducer %d recevra %d fichier(s)%n",
                p, filesForReducer.size());
        }

        // ══════════════════════════════════════════════════════════
        // ÉTAPE 5 : REDUCE — fusion et agrégation
        // ══════════════════════════════════════════════════════════
        System.out.println("\n[4/5] Phase REDUCE...");

        List<Map<String, Integer>> reducerResults = new ArrayList<>();

        for (int p = 0; p < NB_REDUCERS; p++) {
            System.out.println("\n  ── Reducer " + p + " ──");

            // REDUCE : lecture des fichiers + fusion des counts
            Map<String, Integer> reduced = WordCountEngine.reduce(reducerInputs.get(p));
            reducerResults.add(reduced);
            System.out.printf("  %d mot(s) uniques après agrégation%n", reduced.size());

            // Sauvegarde du résultat intermédiaire du Reducer
            Path resultFile = WORK_DIR.resolve("result_partition_" + p + ".txt");
            WordCountEngine.writeResult(reduced, resultFile);
            System.out.println("  Résultat écrit → " + resultFile);
        }

        // ══════════════════════════════════════════════════════════
        // ÉTAPE 6 : FUSION FINALE (rôle du Master)
        // Dans un vrai cluster, le Master lit les fichiers de chaque
        // Reducer et les combine en un seul fichier final.
        // ══════════════════════════════════════════════════════════
        System.out.println("\n[5/5] Fusion finale (rôle du Master)...");

        Map<String, Integer> finalResult = new TreeMap<>();
        for (Map<String, Integer> partial : reducerResults) {
            partial.forEach((word, count) ->
                finalResult.merge(word, count, Integer::sum));
        }

        Path finalOutput = WORK_DIR.resolve("wordcount_final.txt");
        WordCountEngine.writeResult(finalResult, finalOutput);

        // ══════════════════════════════════════════════════════════
        // AFFICHAGE DU RÉSULTAT FINAL
        // ══════════════════════════════════════════════════════════
        System.out.println("\n" + "═".repeat(60));
        System.out.println("  RÉSULTAT FINAL (" + finalResult.size() +
                           " mots uniques) — trié alphabétiquement");
        System.out.println("  Fichier : " + finalOutput.toAbsolutePath());
        System.out.println("═".repeat(60));
        System.out.printf("  %-25s %s%n", "MOT", "OCCURRENCES");
        System.out.println("  " + "-".repeat(35));

        finalResult.forEach((word, count) ->
            System.out.printf("  %-25s %d%n", word, count));

        System.out.println("═".repeat(60));
        System.out.println("  Test terminé avec succès.");
        System.out.println("═".repeat(60));
    }

    // ─────────────────────────────────────────────────────────────
    // Méthodes utilitaires du test
    // ─────────────────────────────────────────────────────────────

    /**
     * Découpe un texte en N chunks approximativement égaux,
     * en coupant sur les fins de ligne pour ne pas briser les mots.
     *
     * @param content le texte complet
     * @param n       le nombre de chunks souhaité
     * @return liste de N String (le dernier peut être plus court)
     */
    private static List<String> splitIntoChunks(String content, int n) {
        String[] lines = content.split("\n");
        List<String> chunks = new ArrayList<>();

        // Taille approximative d'un chunk en nombre de lignes
        int linesPerChunk = (int) Math.ceil((double) lines.length / n);

        StringBuilder current = new StringBuilder();
        int lineCount = 0;

        for (String line : lines) {
            current.append(line).append("\n");
            lineCount++;

            // Quand on atteint la taille cible, on ferme le chunk
            if (lineCount >= linesPerChunk && chunks.size() < n - 1) {
                chunks.add(current.toString());
                current = new StringBuilder();
                lineCount = 0;
            }
        }

        // Dernier chunk (peut contenir les lignes restantes)
        if (!current.isEmpty()) {
            chunks.add(current.toString());
        }

        return chunks;
    }

    /**
     * Cherche data/sample.txt depuis le répertoire courant,
     * puis depuis le répertoire parent (pour les lancements depuis target/).
     */
    private static Path resolveSampleFile() {
        Path candidate = Paths.get("data/sample.txt");
        if (Files.exists(candidate)) return candidate;

        // Si le programme est lancé depuis un sous-répertoire
        candidate = Paths.get("../data/sample.txt");
        if (Files.exists(candidate)) return candidate;

        throw new RuntimeException(
            "Impossible de trouver data/sample.txt. " +
            "Lancez le programme depuis la racine du projet.");
    }
}
