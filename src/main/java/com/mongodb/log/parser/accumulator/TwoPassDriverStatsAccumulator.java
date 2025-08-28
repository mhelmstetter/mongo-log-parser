package com.mongodb.log.parser.accumulator;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Two-pass driver statistics accumulator that eliminates the connection matching issues
 * by processing authentication information in the first pass, then metadata in the second pass
 */
public class TwoPassDriverStatsAccumulator {
    
    /**
     * Connection information holder for unified tracking
     */
    private static class ConnectionInfo {
        String username;
        Long startTime;
        String driverKey;
        long lastAccessTime;
        boolean sampledForLifetime;  // Track if this connection is sampled for lifetime stats
        String sampleAuthMessage;  // Store one sample auth message
        
        ConnectionInfo() {
            this.lastAccessTime = System.currentTimeMillis();
            // Sample 10% of connections for lifetime tracking
            // Using random sampling to get unbiased statistics
            this.sampledForLifetime = Math.random() < 0.1;
        }
        
        void updateAccess() {
            this.lastAccessTime = System.currentTimeMillis();
        }
    }
    
    // Single map for all connection data - much more efficient
    // Pre-size to avoid resizing during processing (2M initial capacity)
    private Map<Long, ConnectionInfo> connectionData = new ConcurrentHashMap<>(2000000, 0.75f, 16);
    
    // Final driver stats
    private Map<String, DriverStatsEntry> driverStatsEntries = new HashMap<>();
    
    // Statistics for monitoring cleanup
    private volatile long processedConnectionEnds = 0;
    private volatile long lastBulkCleanup = 0;
    private volatile long lastSelectiveCleanup = 0;
    private volatile long sampledConnections = 0;
    private volatile long totalConnectionsSeen = 0;
    
    /**
     * Pass 1: Process authentication messages to build connection -> username map
     */
    public void recordAuthentication(String ctx, String username, String database, String mechanism) {
        if (ctx != null && username != null) {
            Long connId = extractConnId(ctx);
            if (connId != null) {
                ConnectionInfo info = connectionData.computeIfAbsent(connId, k -> new ConnectionInfo());
                info.username = username;
                info.updateAccess();
            }
        }
    }
    
    /**
     * Pass 1: Process authentication messages with original log line for sample collection
     */
    public void recordAuthentication(String ctx, String username, String database, String mechanism, String originalLine) {
        recordAuthentication(ctx, username, database, mechanism);
        // Store sample auth message for this username if we haven't already
        if (ctx != null && username != null && originalLine != null) {
            Long connId = extractConnId(ctx);
            if (connId != null) {
                ConnectionInfo info = connectionData.get(connId);
                if (info != null && info.sampleAuthMessage == null) {
                    info.sampleAuthMessage = originalLine;
                }
            }
        }
    }
    
    /**
     * Pass 1: Record connection start time (e.g., when client metadata is received)
     */
    public void recordConnectionStart(String ctx, long timestamp) {
        if (ctx != null) {
            Long connId = extractConnId(ctx);
            if (connId != null) {
                ConnectionInfo info = connectionData.computeIfAbsent(connId, k -> {
                    totalConnectionsSeen++;
                    ConnectionInfo newInfo = new ConnectionInfo();
                    if (newInfo.sampledForLifetime) {
                        sampledConnections++;
                    }
                    return newInfo;
                });
                // Only store start time for sampled connections to save memory
                if (info.sampledForLifetime) {
                    info.startTime = timestamp;
                }
                info.updateAccess();
            }
        }
    }
    
    /**
     * Pass 2: Calculate connection lifetime and add to driver stats
     */
    public void recordConnectionEnd(String ctx, long endTimestamp) {
        if (ctx != null) {
            Long connId = extractConnId(ctx);
            if (connId != null) {
                // Get all connection data in one lookup
                ConnectionInfo info = connectionData.get(connId);
                
                // Only calculate lifetime for sampled connections
                if (info != null && info.sampledForLifetime && info.driverKey != null && info.startTime != null) {
                    long lifetimeMs = endTimestamp - info.startTime;
                    if (lifetimeMs > 0) {
                        DriverStatsEntry entry = driverStatsEntries.get(info.driverKey);
                        if (entry != null) {
                            entry.addConnectionLifetime(lifetimeMs);
                        }
                    }
                }
                
                // Always clean up the connection to free memory
                connectionData.remove(connId);
                processedConnectionEnds++;
                
                // Periodic logging with sampling stats
                if (processedConnectionEnds % 100000 == 0) {
                    long sampledActive = connectionData.values().stream()
                        .filter(ci -> ci.sampledForLifetime)
                        .count();
                    System.out.printf("[MONITOR] Processed %d connection ends, active: %d total, %d sampled for lifetime\n",
                                    processedConnectionEnds, connectionData.size(), sampledActive);
                }
            }
        }
    }
    
    /**
     * Backward compatibility method for LogParserTask (should not be used in two-pass mode)
     */
    public void accumulate(String driverName, String driverVersion, Set<String> compressors,
                          String osType, String osName, String platform, String serverVersion,
                          String remoteHost, String ctx) {
        // Filter out NetworkInterface and MongoDB Internal Client drivers
        if (driverName != null && 
            (driverName.startsWith("NetworkInterface") || driverName.equals("MongoDB Internal Client"))) {
            return; // Skip these driver entries
        }
        
        // Use current time as fallback timestamp - this shouldn't be called in two-pass mode
        accumulate(driverName, driverVersion, compressors, osType, osName, platform, 
                  serverVersion, remoteHost, ctx, System.currentTimeMillis());
    }
    
    /**
     * Pass 2: Process client metadata with immediate username lookup and sample message collection
     */
    public void accumulate(String driverName, String driverVersion, Set<String> compressors,
                          String osType, String osName, String platform, String serverVersion,
                          String remoteHost, String ctx, long timestamp, String originalLine) {
        // Filter out NetworkInterface and MongoDB Internal Client drivers
        if (driverName != null && 
            (driverName.startsWith("NetworkInterface") || driverName.equals("MongoDB Internal Client"))) {
            return; // Skip these driver entries
        }
        
        accumulate(driverName, driverVersion, compressors, osType, osName, platform, serverVersion,
                  remoteHost, ctx, timestamp);
        
        // Store sample metadata message for this driver combination
        if (originalLine != null) {
            Long connId = extractConnId(ctx);
            ConnectionInfo info = connId != null ? connectionData.get(connId) : null;
            String username = info != null ? info.username : null;
            String key = createKey(driverName, driverVersion, osType, platform, compressors, username);
            DriverStatsEntry entry = driverStatsEntries.get(key);
            if (entry != null) {
                entry.addSampleMetadataMessage(originalLine);
                
                // Also add sample auth message if we have one for this connection
                if (info != null && info.sampleAuthMessage != null) {
                    entry.addSampleAuthMessage(info.sampleAuthMessage);
                }
            }
        }
    }
    
    /**
     * Pass 2: Process client metadata with immediate username lookup (without original line)
     */
    public void accumulate(String driverName, String driverVersion, Set<String> compressors,
                          String osType, String osName, String platform, String serverVersion,
                          String remoteHost, String ctx, long timestamp) {
        
        // Filter out NetworkInterface and MongoDB Internal Client drivers
        if (driverName != null && 
            (driverName.startsWith("NetworkInterface") || driverName.equals("MongoDB Internal Client"))) {
            return; // Skip these driver entries
        }
        
        Long connId = extractConnId(ctx);
        
        // Get connection info in single lookup
        ConnectionInfo info = connId != null ? connectionData.get(connId) : null;
        String username = info != null ? info.username : null;
        
        // Create a key combining driver name, version, compressors, and username for grouping
        String key = createKey(driverName, driverVersion, osType, platform, compressors, username);
        
        // Create or get driver stats entry
        DriverStatsEntry entry = driverStatsEntries.get(key);
        if (entry == null) {
            entry = new DriverStatsEntry(driverName, driverVersion, compressors, 
                                       osType, osName, platform, serverVersion);
            driverStatsEntries.put(key, entry);
        } else {
            // Add another connection for this driver combination  
            entry.addConnection(remoteHost);
            // No need to merge compressors since they're part of the key now
        }
        
        // Add username if we found one
        if (username != null) {
            entry.addUsername(username);
        }
        
        // Store driver key and update connection info
        if (connId != null) {
            if (info == null) {
                totalConnectionsSeen++;
                info = new ConnectionInfo();
                if (info.sampledForLifetime) {
                    sampledConnections++;
                }
                connectionData.put(connId, info);
            }
            info.driverKey = key;
            info.updateAccess();
            
            // Only store start time for sampled connections
            if (info.sampledForLifetime && info.startTime == null) {
                info.startTime = timestamp;
            }
        }
    }
    
    /**
     * Call this between pass 1 and pass 2 to prepare for metadata processing
     */
    public void finishPass1() {
        // Count connections with authentication info
        long authCount = connectionData.values().stream()
            .mapToLong(info -> info.username != null ? 1 : 0)
            .sum();
        System.out.printf("Pass 1 complete: %d auth records, %d total connections, %d sampled (%.1f%%) for lifetime tracking\n", 
                         authCount, connectionData.size(), sampledConnections, 
                         (sampledConnections * 100.0) / Math.max(1, totalConnectionsSeen));
    }
    
    /**
     * Selective cleanup based on connection age - preserves recent connections
     */
    private void performSelectiveCleanup(long currentTimestamp) {
        int sizeBefore = connectionData.size();
        
        // Remove connections older than 1 hour that haven't been accessed recently
        long cutoffTime = currentTimestamp - (60 * 60 * 1000); // 1 hour ago
        
        final int[] removed = {0}; // Use array to make it effectively final
        connectionData.entrySet().removeIf(entry -> {
            ConnectionInfo info = entry.getValue();
            // Remove if connection is old AND hasn't been accessed recently
            boolean shouldRemove = info.lastAccessTime < cutoffTime && 
                                 (info.startTime == null || info.startTime < cutoffTime);
            if (shouldRemove) removed[0]++;
            return shouldRemove;
        });
        
        System.out.printf("[SELECTIVE CLEANUP] Removed %d old connections (>1hr), %d remain active (was %d)\n", 
                         removed[0], connectionData.size(), sizeBefore);
        
        // Emergency cleanup if still too large
        if (connectionData.size() > 5000000) {
            System.out.printf("[EMERGENCY CLEANUP] Still too large (%d), clearing connections without driver keys\n", connectionData.size());
            final int[] emergencyRemoved = {0};
            final int[] debugSamples = {0};
            connectionData.entrySet().removeIf(entry -> {
                boolean shouldRemove = entry.getValue().driverKey == null;
                if (shouldRemove) {
                    emergencyRemoved[0]++;
                    // Debug: log first 10 samples of connections without driver keys
                    if (debugSamples[0] < 10) {
                        ConnectionInfo info = entry.getValue();
                        System.out.printf("[DEBUG] No driver key for conn%d: auth=%s, startTime=%s\n", 
                                         entry.getKey(), info.username != null, info.startTime != null);
                        debugSamples[0]++;
                    }
                }
                return shouldRemove;
            });
            System.out.printf("[EMERGENCY CLEANUP] Removed %d connections without driver data, %d remain\n", 
                             emergencyRemoved[0], connectionData.size());
        }
    }
    
    /**
     * Call this after pass 2 to clean up memory
     */
    public void finishPass2() {
        System.out.printf("Pass 2 complete: processed %d driver combinations, %d connections remain unended\n", 
                         driverStatsEntries.size(), connectionData.size());
        
        // Report on unended connections for metrics visibility
        long connectionsWithLifetimeData = connectionData.values().stream()
            .mapToLong(info -> (info.startTime != null && info.driverKey != null) ? 1 : 0)
            .sum();
        System.out.printf("Unended connections: %d total, %d with complete lifetime tracking data\n", 
                         connectionData.size(), connectionsWithLifetimeData);
        
        // Clear the connection map to free memory
        connectionData.clear();
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
    
    private String createKey(String driverName, String driverVersion, String osType, String platform, Set<String> compressors, String username) {
        String compressorStr = "none";
        if (compressors != null && !compressors.isEmpty()) {
            // Sort compressors for consistent key generation
            compressorStr = compressors.stream().sorted().collect(java.util.stream.Collectors.joining(","));
        }
        String usernameStr = username != null ? username : "none";
        return String.format("%s|%s|%s|%s|%s|%s", 
                            driverName != null ? driverName : "unknown",
                            driverVersion != null ? driverVersion : "unknown", 
                            osType != null ? osType : "unknown",
                            platform != null ? platform : "unknown",
                            compressorStr,
                            usernameStr);
    }
    
    // Overload for backward compatibility when username is not known
    private String createKey(String driverName, String driverVersion, String osType, String platform, Set<String> compressors) {
        return createKey(driverName, driverVersion, osType, platform, compressors, null);
    }
    
    // Overload for backward compatibility when compressors aren't needed
    private String createKey(String driverName, String driverVersion, String osType, String platform) {
        return createKey(driverName, driverVersion, osType, platform, null);
    }
    
    // Delegate methods to match original interface
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
        return driverStatsEntries.values().stream()
                .mapToInt(DriverStatsEntry::getUniqueHosts)
                .sum();
    }
    
    // No longer needed with 2-pass approach
    public int getPendingAuthCount() { return 0; }
    public int getPendingMetadataCount() { return 0; }
    public void performPeriodicCleanup() { /* No-op */ }
    public void performPostProcessingJoin() { /* No-op */ }
    
    // Connection lifecycle tracking - not implemented in 2-pass approach
    public void trackConnectionStart(String ctx, long timestamp) { /* No-op */ }
    public void trackConnectionEnd(String ctx, long timestamp) { /* No-op */ }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Two-Pass Driver Stats Summary:\n");
        sb.append(String.format("Total Connections: %d\n", getTotalConnections()));
        sb.append(String.format("Unique Driver/Version Combinations: %d\n", getUniqueDriverVersionCombinations()));
        sb.append(String.format("Unique Drivers: %d\n", getUniqueDrivers()));
        long authCount = connectionData.values().stream()
            .mapToLong(info -> info.username != null ? 1 : 0)
            .sum();
        sb.append(String.format("Authentication Records: %d\n", authCount));
        sb.append(String.format("Active Connections: %d\n", connectionData.size()));
        return sb.toString();
    }
}