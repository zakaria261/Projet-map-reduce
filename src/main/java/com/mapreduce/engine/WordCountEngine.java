package com.mapreduce.engine;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

/**
 * Moteur de calcul Map-Reduce pour le comptage de mots.
 *
 * Ce cœur de traitement est indépendant du réseau : il contient
 * uniquement la logique pure (tokenisation, partitionnement, agrégation).
 * Les Workers n'ont qu'à appeler ces méthodes statiques.
 *
 * Pipeline complet :
 *   content ──► map() ──► savePartitions() ──► [shuffle réseau] ──► reduce()
 */
public class WordCountEngine {

    // ─────────────────────────────────────────────────────────────
    // 1. MAP
    // ─────────────────────────────────────────────────────────────

    /**
     * Phase MAP : compte les occurrences de chaque mot dans un texte.
     *
     * Nettoyage appliqué :
     *   - Conversion en minuscules
     *   - Suppression de toute ponctuation SAUF les apostrophes internes
     *     (ex : "don't" est conservé, "dog." devient "dog")
     *   - Ignore les tokens vides après nettoyage
     *
     * @param content  le texte brut lu depuis un fichier
     * @return Map triée { mot → nombre d'occurrences }
     */
    public static Map<String, Integer> map(String content) {
        Map<String, Integer> counts = new TreeMap<>();

        if (content == null || content.isBlank()) {
            return counts;
        }

        // Étape 1 : passage en minuscules
        String lower = content.toLowerCase();

        // Étape 2 : suppression de la ponctuation sauf l'apostrophe.
        // On remplace tout ce qui n'est pas [a-z0-9'] par un espace.
        // L'apostrophe interne (don't) est conservée ; celles en
        // début/fin de mot seront éliminées au trim ci-dessous.
        String cleaned = lower.replaceAll("[^a-z0-9']", " ");

        // Étape 3 : découpage sur les espaces et comptage
        String[] tokens = cleaned.split("\\s+");
        for (String token : tokens) {
            // Suppression des apostrophes en début et fin du token
            String word = token.replaceAll("^'+|'+$", "");

            // Ignorer les tokens vides ou purement numériques si souhaité
            if (word.isEmpty()) {
                continue;
            }

            // Incrémenter le compteur (merge évite un if/else)
            counts.merge(word, 1, Integer::sum);
        }

        return counts;
    }

    // ─────────────────────────────────────────────────────────────
    // 2. PARTITIONNEMENT
    // ─────────────────────────────────────────────────────────────

    /**
     * Calcule l'index du Reducer responsable d'un mot.
     *
     * Propriété clé : déterministe — le même mot retourne TOUJOURS
     * le même index, quel que soit le Mapper qui le traite.
     *
     * Math.abs est insuffisant seul car Integer.MIN_VALUE.hashCode()
     * reste négatif après Math.abs(). Le masque 0x7FFFFFFF force le
     * bit de signe à 0 sans changer les autres bits.
     *
     * @param word       mot normalisé
     * @param nbReducers nombre total de Reducers (> 0)
     * @return index dans [0, nbReducers - 1]
     */
    public static int getPartition(String word, int nbReducers) {
        if (nbReducers <= 0) {
            throw new IllegalArgumentException(
                "nbReducers doit être > 0, reçu : " + nbReducers);
        }
        // Masquage du bit de signe avant le modulo
        return (word.hashCode() & 0x7FFFFFFF) % nbReducers;
    }

    // ─────────────────────────────────────────────────────────────
    // 3. SAUVEGARDE DES PARTITIONS (spill files)
    // ─────────────────────────────────────────────────────────────

    /**
     * Écrit les résultats du Map dans des fichiers temporaires partitionnés.
     *
     * Un fichier est créé par partition (Reducer) dans {@code outputDir} :
     *   outputDir/
     *     partition_0.txt   ← mots destinés au Reducer 0
     *     partition_1.txt   ← mots destinés au Reducer 1
     *     ...
     *
     * Format de chaque ligne : {@code mot<TAB>occurrences}
     * Les lignes sont triées alphabétiquement à l'intérieur de chaque fichier.
     *
     * @param counts     Map { mot → occurrences } produite par {@link #map}
     * @param nbReducers nombre de partitions à créer
     * @param outputDir  répertoire de destination (créé si absent)
     * @return liste des chemins des fichiers créés (index = numéro de partition)
     * @throws IOException en cas d'erreur d'écriture
     */
    public static List<Path> savePartitions(Map<String, Integer> counts,
                                             int nbReducers,
                                             Path outputDir) throws IOException {
        // Création du répertoire de sortie si nécessaire
        Files.createDirectories(outputDir);

        // Initialisation d'une Map par partition
        @SuppressWarnings("unchecked")
        Map<String, Integer>[] partitions = new TreeMap[nbReducers];
        for (int i = 0; i < nbReducers; i++) {
            partitions[i] = new TreeMap<>();
        }

        // Répartition de chaque mot dans la bonne partition
        for (Map.Entry<String, Integer> entry : counts.entrySet()) {
            int idx = getPartition(entry.getKey(), nbReducers);
            partitions[idx].put(entry.getKey(), entry.getValue());
        }

        // Écriture sur disque : un fichier par partition
        List<Path> filePaths = new ArrayList<>();
        for (int i = 0; i < nbReducers; i++) {
            Path filePath = outputDir.resolve("partition_" + i + ".txt");

            try (BufferedWriter writer = Files.newBufferedWriter(
                    filePath, StandardCharsets.UTF_8)) {
                for (Map.Entry<String, Integer> entry : partitions[i].entrySet()) {
                    // Format : mot<TAB>count
                    writer.write(entry.getKey() + "\t" + entry.getValue());
                    writer.newLine();
                }
            }

            filePaths.add(filePath);
            System.out.printf("  [savePartitions] Partition %d : %d mot(s) → %s%n",
                i, partitions[i].size(), filePath);
        }

        return filePaths;
    }

    // ─────────────────────────────────────────────────────────────
    // 4. REDUCE
    // ─────────────────────────────────────────────────────────────

    /**
     * Phase REDUCE : fusionne et agrège les partitions de plusieurs Mappers.
     *
     * Dans un vrai cluster, chaque Reducer reçoit une liste de fichiers
     * provenant de TOUS les Mappers (un fichier par Mapper pour sa partition).
     * Cette méthode lit tous ces fichiers et somme les occurrences par mot.
     *
     * Exemple :
     *   Mapper 1, partition 0 :  "the → 3",  "fox → 1"
     *   Mapper 2, partition 0 :  "the → 2",  "dog → 1"
     *   Résultat fusionné      :  "dog → 1",  "fox → 1",  "the → 5"
     *
     * @param partitionFiles liste des fichiers de partition à lire
     * @return Map triée { mot → count global }
     * @throws IOException en cas d'erreur de lecture
     */
    public static Map<String, Integer> reduce(List<Path> partitionFiles) throws IOException {
        Map<String, Integer> aggregated = new TreeMap<>();

        for (Path file : partitionFiles) {
            if (!Files.exists(file)) {
                System.err.println("  [reduce] Fichier introuvable, ignoré : " + file);
                continue;
            }

            // Lecture ligne par ligne du fichier de partition
            List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
            for (String line : lines) {
                if (line.isBlank()) {
                    continue;
                }

                // Chaque ligne est au format "mot<TAB>count"
                String[] parts = line.split("\t", 2);
                if (parts.length != 2) {
                    System.err.println("  [reduce] Ligne invalide ignorée : " + line);
                    continue;
                }

                String word = parts[0].trim();
                try {
                    int count = Integer.parseInt(parts[1].trim());
                    // Agrégation : on somme les counts de tous les Mappers
                    aggregated.merge(word, count, Integer::sum);
                } catch (NumberFormatException e) {
                    System.err.println("  [reduce] Nombre invalide : " + parts[1]);
                }
            }
        }

        return aggregated;
    }

    // ─────────────────────────────────────────────────────────────
    // 5. UTILITAIRE : écriture du résultat final
    // ─────────────────────────────────────────────────────────────

    /**
     * Écrit le résultat final d'un Reducer dans un fichier de sortie.
     *
     * @param result    Map { mot → count } produite par {@link #reduce}
     * @param outputFile chemin du fichier de sortie à créer
     * @throws IOException en cas d'erreur d'écriture
     */
    public static void writeResult(Map<String, Integer> result,
                                    Path outputFile) throws IOException {
        Files.createDirectories(outputFile.getParent());

        try (BufferedWriter writer = Files.newBufferedWriter(
                outputFile, StandardCharsets.UTF_8)) {
            for (Map.Entry<String, Integer> entry : result.entrySet()) {
                writer.write(entry.getKey() + "\t" + entry.getValue());
                writer.newLine();
            }
        }
    }
}
