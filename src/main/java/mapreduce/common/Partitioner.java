package mapreduce.common;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Algorithme de partitionnement FNV-1a 32-bit.
 * Garantit une distribution déterministe des mots vers les Reducers,
 * indépendamment de l'environnement d'exécution.
 */
public class Partitioner {

    private static final long FNV_PRIME_32  = 0x01000193L;
    private static final long FNV_OFFSET_32 = 0x811C9DC5L;

    // Caractères autorisés : lettres (y compris accentuées), chiffres, apostrophe
    private static final Pattern DISALLOWED = Pattern.compile("[^a-z0-9àâäéèêëîïôùûüç']");

    // ── FNV-1a 32-bit ─────────────────────────────────────────────

    /**
     * Calcule le hash FNV-1a 32-bit d'un tableau d'octets.
     * Déterministe, rapide et bonne distribution uniforme.
     */
    public static long fnv1aHash(byte[] data) {
        long h = FNV_OFFSET_32;
        for (byte b : data) {
            h ^= (b & 0xFFL);
            h = (h * FNV_PRIME_32) & 0xFFFFFFFFL;
        }
        return h;
    }

    // ── Normalisation des mots ────────────────────────────────────

    /**
     * Normalise un token brut avant comptage et hachage.
     *
     * Étapes :
     *   1. Passage en minuscules
     *   2. Suppression de la ponctuation (conserve lettres, chiffres, apostrophes)
     *   3. Suppression des apostrophes en début/fin de mot
     */
    public static String normalizeWord(String raw) {
        String word = raw.toLowerCase();
        word = DISALLOWED.matcher(word).replaceAll("");
        // Suppression des apostrophes en début/fin
        while (!word.isEmpty() && word.charAt(0) == '\'') word = word.substring(1);
        while (!word.isEmpty() && word.charAt(word.length() - 1) == '\'')
            word = word.substring(0, word.length() - 1);
        return word;
    }

    /**
     * Découpe un texte brut en liste de mots normalisés non vides.
     */
    public static List<String> tokenize(String text) {
        List<String> tokens = new ArrayList<>();
        for (String raw : text.split("\\s+")) {
            String normalized = normalizeWord(raw);
            if (!normalized.isEmpty()) tokens.add(normalized);
        }
        return tokens;
    }

    // ── Partitionnement ───────────────────────────────────────────

    /**
     * Détermine l'index du Reducer responsable d'un mot.
     *
     * La fonction est déterministe : le même mot renvoie toujours
     * vers le même Reducer, quel que soit le Mapper qui l'a traité.
     *
     * @param word       mot normalisé (sortie de normalizeWord)
     * @param nbReducers nombre total de Reducers actifs
     * @return entier dans [0, nbReducers - 1]
     */
    public static int getPartition(String word, int nbReducers) {
        if (nbReducers <= 0)
            throw new IllegalArgumentException("nbReducers doit être > 0, reçu : " + nbReducers);
        long h = fnv1aHash(word.getBytes(StandardCharsets.UTF_8));
        return (int)(h % nbReducers);
    }

    // ── Comptage local ────────────────────────────────────────────

    /**
     * Compte les occurrences de chaque mot dans une liste de tokens.
     */
    public static Map<String, Integer> localCount(List<String> tokens) {
        Map<String, Integer> counts = new HashMap<>();
        for (String word : tokens) {
            counts.merge(word, 1, Integer::sum);
        }
        return counts;
    }

    /**
     * Répartit un dictionnaire de comptages en N sous-Maps,
     * un par partition Reducer.
     *
     * @return Map<partitionIndex, Map<mot, count_local>>
     */
    public static Map<Integer, Map<String, Integer>> partitionCounts(
            Map<String, Integer> counts, int nbReducers) {
        Map<Integer, Map<String, Integer>> partitions = new HashMap<>();
        for (int i = 0; i < nbReducers; i++) partitions.put(i, new HashMap<>());
        counts.forEach((word, count) -> {
            int p = getPartition(word, nbReducers);
            partitions.get(p).put(word, count);
        });
        return partitions;
    }
}
