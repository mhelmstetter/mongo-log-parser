package com.mongodb.log.parser.accumulator;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.ArrayList;

/**
 * Accumulator for tracking driver statistics from client metadata log messages
 */
public class DriverStatsAccumulator {
    
    private Map<String, DriverStatsEntry> driverStatsEntries = new HashMap<>();
    // Map from connection id (integer) to authentication info - cleaned up periodically
    private Map<Long, AuthInfo> authInfoByConnId = new ConcurrentHashMap<>();
    // Map from connection id to pending metadata - cleaned up when auth is found
    private Map<Long, MetadataEntry> pendingMetadataByConnId = new ConcurrentHashMap<>();
    // Map from connection id to connection lifetime tracking
    private Map<Long, ConnectionLifetimeTracker> connectionLifetimeByConnId = new ConcurrentHashMap<>();
    private int debugConnectionCount = 0;
    private int totalMetadataCount = 0;
    private int totalAuthCount = 0;
    private int matchedCount = 0;
    private int connectionStartCount = 0;
    private int connectionEndCount = 0;
    private int connectionLifetimesRecorded = 0;
    
    // Inner class to store auth info temporarily
    public static class AuthInfo {
        public final String username;
        public final String database;
        public final String mechanism;
        
        public AuthInfo(String username, String database, String mechanism) {
            this.username = username;
            this.database = database;
            this.mechanism = mechanism;
        }
    }
    
    // Inner class to track connection lifetime
    public static class ConnectionLifetimeTracker {
        public final String driverKey;
        public long firstTimestamp;
        public long lastTimestamp;
        
        public ConnectionLifetimeTracker(String driverKey, long timestamp) {
            this.driverKey = driverKey;
            this.firstTimestamp = timestamp;
            this.lastTimestamp = timestamp;
        }
        
        public void updateTimestamp(long timestamp) {
            if (timestamp < firstTimestamp) {
                firstTimestamp = timestamp;
            }
            if (timestamp > lastTimestamp) {
                lastTimestamp = timestamp;
            }
        }
        
        public long getLifetimeMillis() {
            return lastTimestamp - firstTimestamp;
        }
    }
    
    // Inner class to store metadata entries for post-processing
    public static class MetadataEntry {
        public final String key;
        public final Long connId;
        public final String driverName;
        public final String driverVersion;
        public final Set<String> compressors;
        public final String osType;
        public final String osName;
        public final String platform;
        public final String serverVersion;
        public final String remoteHost;
        
        public MetadataEntry(String key, Long connId, String driverName, String driverVersion, 
                           Set<String> compressors, String osType, String osName, 
                           String platform, String serverVersion, String remoteHost) {
            this.key = key;
            this.connId = connId;
            this.driverName = driverName;
            this.driverVersion = driverVersion;
            this.compressors = compressors;
            this.osType = osType;
            this.osName = osName;
            this.platform = platform;
            this.serverVersion = serverVersion;
            this.remoteHost = remoteHost;
        }
    }
    
    public void accumulate(String driverName, String driverVersion, Set<String> compressors,
                          String osType, String osName, String platform, String serverVersion,
                          String remoteHost) {
        accumulate(driverName, driverVersion, compressors, osType, osName, platform, 
                  serverVersion, remoteHost, null);
    }
    
    public void accumulate(String driverName, String driverVersion, Set<String> compressors,
                          String osType, String osName, String platform, String serverVersion,
                          String remoteHost, String ctx) {
        accumulate(driverName, driverVersion, compressors, osType, osName, platform, 
                  serverVersion, remoteHost, ctx, null);
    }
    
    public void accumulate(String driverName, String driverVersion, Set<String> compressors,
                          String osType, String osName, String platform, String serverVersion,
                          String remoteHost, String ctx, String originalLine) {
        
        // Create a key combining driver name and version for grouping
        String key = createKey(driverName, driverVersion, osType, platform);
        Long connId = extractConnId(ctx);
        
        totalMetadataCount++;
        
        // Debug logging for first few connections
        if (debugConnectionCount < 10) {
            System.err.println(String.format("[DEBUG] Processing metadata: ctx=%s, connId=%s, driver=%s", 
                              ctx, connId, driverName));
            debugConnectionCount++;
        }
        
        // Check if we already have auth info for this connection
        AuthInfo authInfo = connId != null ? authInfoByConnId.remove(connId) : null;
        
        // Create or get driver stats entry immediately
        DriverStatsEntry entry = driverStatsEntries.get(key);
        if (entry == null) {
            entry = new DriverStatsEntry(driverName, driverVersion, compressors, 
                                       osType, osName, platform, serverVersion);
            driverStatsEntries.put(key, entry);
        } else {
            // Add another connection for this driver combination
            entry.addConnection(remoteHost);
        }
        
        // Store sample metadata message and track connection lifetime
        if (originalLine != null) {
            // Disabled to save memory - was storing full log lines
            // entry.addSampleMetadataMessage(originalLine);
            
            // Extract timestamp and track connection lifetime
            long timestamp = extractTimestamp(originalLine);
            if (timestamp > 0 && connId != null) {
                // Update existing tracker with driver info, or create new one
                ConnectionLifetimeTracker tracker = connectionLifetimeByConnId.get(connId);
                if (tracker == null) {
                    tracker = new ConnectionLifetimeTracker(key, timestamp);
                    connectionLifetimeByConnId.put(connId, tracker);
                } else {
                    // Update driver key if it was unknown, and update timestamp
                    if ("unknown".equals(tracker.driverKey)) {
                        tracker = new ConnectionLifetimeTracker(key, tracker.firstTimestamp);
                        tracker.updateTimestamp(timestamp);
                        connectionLifetimeByConnId.put(connId, tracker);
                    } else {
                        tracker.updateTimestamp(timestamp);
                    }
                }
            }
        }
        
        // If we found auth info, add username immediately
        if (authInfo != null) {
            entry.addUsername(authInfo.username);
        } else if (connId != null) {
            // Store metadata temporarily - it will be cleaned up when auth arrives
            MetadataEntry metadataEntry = new MetadataEntry(key, connId, driverName, driverVersion, 
                                                           compressors, osType, osName, platform, 
                                                           serverVersion, remoteHost);
            pendingMetadataByConnId.put(connId, metadataEntry);
        }
    }
    
    public void recordAuthentication(String ctx, String username, String database, String mechanism) {
        recordAuthentication(ctx, username, database, mechanism, null);
    }
    
    public void recordAuthentication(String ctx, String username, String database, String mechanism, String originalLine) {
        if (ctx != null && username != null) {
            Long connId = extractConnId(ctx);
            if (connId != null) {
                totalAuthCount++;
                
                // Debug logging for first few authentications
                if (debugConnectionCount < 10) {
                    System.err.println(String.format("[DEBUG] Processing auth: ctx=%s, connId=%s, username=%s", 
                                      ctx, connId, username));
                }
                
                // Check if we have pending metadata for this connection
                MetadataEntry pendingMetadata = pendingMetadataByConnId.remove(connId);
                
                if (pendingMetadata != null) {
                    matchedCount++;
                    // We found matching metadata - process immediately
                    DriverStatsEntry entry = driverStatsEntries.get(pendingMetadata.key);
                    if (entry != null) {
                        entry.addUsername(username);
                        // Store sample auth message and track connection lifetime
                        if (originalLine != null) {
                            // Disabled to save memory - was storing full log lines
                            // entry.addSampleAuthMessage(originalLine);
                            
                            // Extract timestamp and track connection lifetime
                            long timestamp = extractTimestamp(originalLine);
                            if (timestamp > 0) {
                                // Update the existing tracker for this connection
                                ConnectionLifetimeTracker tracker = connectionLifetimeByConnId.get(connId);
                                if (tracker != null) {
                                    tracker.updateTimestamp(timestamp);
                                }
                            }
                        }
                    }
                } else {
                    // Store auth info temporarily - metadata might come later
                    authInfoByConnId.put(connId, new AuthInfo(username, database, mechanism));
                }
            }
        }
    }
    
    // Extract connection ID from ctx string (e.g., "conn36784483" -> 36784483)
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
    
    // Extract timestamp from log line (MongoDB JSON format)
    private long extractTimestamp(String logLine) {
        if (logLine != null && logLine.contains("\"$date\":\"")) {
            try {
                int dateIndex = logLine.indexOf("\"$date\":\"") + 9;
                int endIndex = logLine.indexOf("\"", dateIndex);
                if (endIndex > dateIndex) {
                    String dateStr = logLine.substring(dateIndex, endIndex);
                    // Parse ISO 8601 timestamp to milliseconds
                    java.time.Instant instant = java.time.Instant.parse(dateStr);
                    return instant.toEpochMilli();
                }
            } catch (Exception e) {
                // Ignore parsing errors
            }
        }
        return 0;
    }
    
    // Post-processing method to handle any remaining unmatched entries
    public void performPostProcessingJoin() {
        // Report statistics before cleanup
        System.err.println(String.format("[STATS] Total metadata messages: %d", totalMetadataCount));
        System.err.println(String.format("[STATS] Total auth messages: %d", totalAuthCount));
        System.err.println(String.format("[STATS] Matched connections: %d", matchedCount));
        System.err.println(String.format("[STATS] Match rate: %.1f%%", 
                           totalAuthCount > 0 ? (matchedCount * 100.0 / totalAuthCount) : 0.0));
        System.err.println(String.format("[STATS] Connection starts tracked: %d", connectionStartCount));
        System.err.println(String.format("[STATS] Connection ends tracked: %d", connectionEndCount));
        System.err.println(String.format("[STATS] Connection lifetimes recorded: %d", connectionLifetimesRecorded));
        System.err.println(String.format("[STATS] Pending metadata at end: %d", pendingMetadataByConnId.size()));
        System.err.println(String.format("[STATS] Pending auth at end: %d", authInfoByConnId.size()));
        System.err.println(String.format("[STATS] Pending connection trackers at end: %d", connectionLifetimeByConnId.size()));
        
        // Process any remaining pending metadata entries (metadata without auth)
        for (MetadataEntry metadataEntry : pendingMetadataByConnId.values()) {
            // These entries already exist in driverStatsEntries, nothing more to do
            // The username will show as "none" which is correct
        }
        
        // Calculate and store connection lifetimes for each driver
        Map<String, java.util.List<Long>> lifetimesByDriverKey = new java.util.HashMap<>();
        for (ConnectionLifetimeTracker tracker : connectionLifetimeByConnId.values()) {
            long lifetime = tracker.getLifetimeMillis();
            lifetimesByDriverKey.computeIfAbsent(tracker.driverKey, k -> new java.util.ArrayList<>()).add(lifetime);
        }
        
        // Update driver stats entries with average lifetimes
        for (java.util.Map.Entry<String, java.util.List<Long>> entry : lifetimesByDriverKey.entrySet()) {
            String driverKey = entry.getKey();
            java.util.List<Long> lifetimes = entry.getValue();
            
            if (!lifetimes.isEmpty()) {
                double averageLifetime = lifetimes.stream().mapToLong(Long::longValue).average().orElse(0.0);
                DriverStatsEntry driverEntry = driverStatsEntries.get(driverKey);
                if (driverEntry != null) {
                    driverEntry.setAverageConnectionLifetimeMillis((long) averageLifetime);
                }
            }
        }
        
        // Clear temporary data to free memory
        pendingMetadataByConnId.clear();
        authInfoByConnId.clear();
        connectionLifetimeByConnId.clear();
    }
    
    // Periodic cleanup to prevent memory leaks for connections that don't get auth messages
    
    // Track connection start (from "Connection accepted" message)
    public void trackConnectionStart(String ctx, long timestamp) {
        Long connId = extractConnId(ctx);
        if (connId != null) {
            connectionStartCount++;
            ConnectionLifetimeTracker tracker = connectionLifetimeByConnId.get(connId);
            if (tracker == null) {
                // Create tracker with placeholder driver key - will be updated when metadata arrives
                tracker = new ConnectionLifetimeTracker("unknown", timestamp);
                connectionLifetimeByConnId.put(connId, tracker);
            } else {
                tracker.updateTimestamp(timestamp);
            }
        }
    }
    
    // Track connection end (from "Connection ended" message)
    public void trackConnectionEnd(String ctx, long timestamp) {
        Long connId = extractConnId(ctx);
        if (connId != null) {
            connectionEndCount++;
            ConnectionLifetimeTracker tracker = connectionLifetimeByConnId.remove(connId);
            if (tracker != null && !"unknown".equals(tracker.driverKey)) {
                tracker.updateTimestamp(timestamp);
                long lifetime = tracker.getLifetimeMillis();
                
                // Only record meaningful lifetimes (> 0)
                if (lifetime > 0) {
                    connectionLifetimesRecorded++;
                    // Add lifetime to the appropriate driver stats entry
                    DriverStatsEntry driverEntry = driverStatsEntries.get(tracker.driverKey);
                    if (driverEntry != null) {
                        driverEntry.addConnectionLifetime(lifetime);
                    }
                }
            }
        }
    }
    
    public void performPeriodicCleanup() {
        // Only perform cleanup if we're actually over the threshold
        // Don't clear data unnecessarily as it might still be waiting to be matched
        
        // For connection lifetime, try to salvage data before clearing
        if (connectionLifetimeByConnId.size() > 25000) {
            // Process current lifetime data before clearing
            Map<String, java.util.List<Long>> lifetimesByDriverKey = new java.util.HashMap<>();
            for (ConnectionLifetimeTracker tracker : connectionLifetimeByConnId.values()) {
                if (!"unknown".equals(tracker.driverKey)) {
                    long lifetime = tracker.getLifetimeMillis();
                    if (lifetime > 0) { // Only include connections with actual lifetime
                        lifetimesByDriverKey.computeIfAbsent(tracker.driverKey, k -> new java.util.ArrayList<>()).add(lifetime);
                    }
                }
            }
            
            // Update driver stats entries with current averages
            for (java.util.Map.Entry<String, java.util.List<Long>> entry : lifetimesByDriverKey.entrySet()) {
                String driverKey = entry.getKey();
                java.util.List<Long> lifetimes = entry.getValue();
                
                if (!lifetimes.isEmpty()) {
                    double averageLifetime = lifetimes.stream().mapToLong(Long::longValue).average().orElse(0.0);
                    DriverStatsEntry driverEntry = driverStatsEntries.get(driverKey);
                    if (driverEntry != null) {
                        // If we already have a lifetime, take weighted average
                        long currentLifetime = driverEntry.getAverageConnectionLifetimeMillis();
                        if (currentLifetime > 0) {
                            // Simple weighted average (give equal weight to current and new data)
                            long newLifetime = (currentLifetime + (long) averageLifetime) / 2;
                            driverEntry.setAverageConnectionLifetimeMillis(newLifetime);
                        } else {
                            driverEntry.setAverageConnectionLifetimeMillis((long) averageLifetime);
                        }
                    }
                }
            }
            
            // Clear connection lifetime data only
            connectionLifetimeByConnId.clear();
        }
        
        // Clear pending metadata more aggressively to prevent OOM
        if (pendingMetadataByConnId.size() > 25000) {
            pendingMetadataByConnId.clear();
        }
        
        // Clear auth info more aggressively to prevent OOM
        if (authInfoByConnId.size() > 25000) {
            authInfoByConnId.clear();
        }
    }
    
    private String createKey(String driverName, String driverVersion, String osType, String platform) {
        return String.format("%s|%s|%s|%s", 
                            driverName != null ? driverName : "unknown",
                            driverVersion != null ? driverVersion : "unknown", 
                            osType != null ? osType : "unknown",
                            platform != null ? platform : "unknown");
    }
    
    public Map<String, DriverStatsEntry> getDriverStatsEntries() {
        return driverStatsEntries;
    }
    
    public boolean hasDriverStats() {
        return !driverStatsEntries.isEmpty();
    }
    
    public long getTotalConnections() {
        return driverStatsEntries.values().stream()
                .mapToLong(DriverStatsEntry::getConnectionCount)
                .sum();
    }
    
    public long getUniqueDriverVersionCombinations() {
        return driverStatsEntries.size();
    }
    
    public long getUniqueDrivers() {
        return driverStatsEntries.values().stream()
                .map(DriverStatsEntry::getDriverName)
                .distinct()
                .count();
    }
    
    public long getUniqueHosts() {
        Set<String> allHosts = new HashSet<>();
        driverStatsEntries.values().forEach(entry -> {
            // Note: We'd need to track individual hosts in the entry to get this accurately
            // For now, we'll sum up the unique hosts per driver combination
        });
        return driverStatsEntries.values().stream()
                .mapToInt(DriverStatsEntry::getUniqueHosts)
                .sum();
    }
    
    public int getPendingAuthCount() {
        return authInfoByConnId.size();
    }
    
    public int getPendingMetadataCount() {
        return pendingMetadataByConnId.size();
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Driver Stats Summary:\n");
        sb.append(String.format("Total Connections: %d\n", getTotalConnections()));
        sb.append(String.format("Unique Driver/Version Combinations: %d\n", getUniqueDriverVersionCombinations()));
        sb.append(String.format("Unique Drivers: %d\n", getUniqueDrivers()));
        return sb.toString();
    }
}