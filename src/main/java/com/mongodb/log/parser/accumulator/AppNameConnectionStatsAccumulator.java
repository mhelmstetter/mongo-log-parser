package com.mongodb.log.parser.accumulator;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Tracks distinct connection IDs per appName
 * Handles multiple files/mongod instances by tracking per filename
 */
public class AppNameConnectionStatsAccumulator {

    // filename -> appName -> Set of connection IDs
    private final Map<String, Map<String, Set<Long>>> connectionsByFile = new ConcurrentHashMap<>();

    /**
     * Record a connection for an appName in a specific file
     */
    public void recordConnection(String filename, String appName, String ctx) {
        if (filename == null || ctx == null) {
            return;
        }

        Long connId = extractConnId(ctx);
        if (connId == null) {
            return;
        }

        String key = appName != null ? appName : "unknown";

        connectionsByFile
            .computeIfAbsent(filename, f -> new ConcurrentHashMap<>())
            .computeIfAbsent(key, a -> new HashSet<>())
            .add(connId);
    }

    /**
     * Get aggregated stats across all files
     * Returns map of appName -> distinct connection count
     */
    public Map<String, Integer> getAggregatedStats() {
        Map<String, Set<Long>> aggregated = new HashMap<>();

        // Aggregate connection IDs across all files, grouped by appName
        for (Map<String, Set<Long>> fileStats : connectionsByFile.values()) {
            for (Map.Entry<String, Set<Long>> entry : fileStats.entrySet()) {
                String appName = entry.getKey();
                Set<Long> connIds = entry.getValue();

                aggregated
                    .computeIfAbsent(appName, a -> new HashSet<>())
                    .addAll(connIds);
            }
        }

        // Convert to count map
        return aggregated.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> e.getValue().size()
            ));
    }

    /**
     * Get total number of distinct appNames
     */
    public int getAppNameCount() {
        return getAggregatedStats().size();
    }

    /**
     * Get total number of distinct connections across all appNames
     */
    public int getTotalConnectionCount() {
        return getAggregatedStats().values().stream()
            .mapToInt(Integer::intValue)
            .sum();
    }

    /**
     * Extract connection ID from ctx string (e.g., "conn36784483" -> 36784483)
     */
    private Long extractConnId(String ctx) {
        if (ctx != null && ctx.startsWith("conn")) {
            try {
                return Long.parseLong(ctx.substring(4));
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    /**
     * Clear all data
     */
    public void clear() {
        connectionsByFile.clear();
    }
}
