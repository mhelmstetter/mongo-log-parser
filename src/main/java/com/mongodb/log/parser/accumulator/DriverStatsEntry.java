package com.mongodb.log.parser.accumulator;

import java.util.Set;
import java.util.HashSet;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

/**
 * Entry for tracking driver statistics including driver name, version, compressors, and connection info
 */
public class DriverStatsEntry {
    
    private final String driverName;
    private final String driverVersion;
    private final Set<String> compressors;
    private final String osType;
    private final String osName;
    private final String platform;
    private final String serverVersion;
    
    private long connectionCount;
    private Set<String> remoteHosts = new HashSet<>();
    private Set<String> usernames = new HashSet<>();
    private Map<String, Long> connectionsByUsername = new HashMap<>();
    
    // Track how many connections used each compressor type
    private Map<String, Long> compressorUsageCounts = new HashMap<>();
    private List<String> sampleMetadataMessages = new ArrayList<>();
    private List<String> sampleAuthMessages = new ArrayList<>();
    private static final int MAX_SAMPLE_MESSAGES = 1;
    private long averageConnectionLifetimeMillis = 0;
    private long maxConnectionLifetimeMillis = 0;
    private java.util.List<Long> connectionLifetimes = new ArrayList<>();
    
    public DriverStatsEntry(String driverName, String driverVersion, Set<String> compressors, 
                           String osType, String osName, String platform, String serverVersion) {
        this.driverName = driverName != null ? driverName : "unknown";
        this.driverVersion = driverVersion != null ? driverVersion : "unknown";
        this.compressors = compressors != null ? new HashSet<>(compressors) : new HashSet<>();
        this.osType = osType;
        this.osName = osName;
        this.platform = platform;
        this.serverVersion = serverVersion;
        this.connectionCount = 1;
        
        // Initialize compressor usage tracking for the first connection
        if (compressors != null && !compressors.isEmpty()) {
            for (String compressor : compressors) {
                compressorUsageCounts.put(compressor, 1L);
            }
        } else {
            compressorUsageCounts.put("none", 1L);
        }
    }
    
    public void addConnection(String remoteHost) {
        connectionCount++;
        if (remoteHost != null) {
            remoteHosts.add(extractHostFromRemoteHost(remoteHost));
        }
    }
    
    public void addUsername(String username) {
        if (username != null) {
            usernames.add(username);
        }
    }
    
    public void addCompressors(Set<String> newCompressors) {
        if (newCompressors != null && !newCompressors.isEmpty()) {
            this.compressors.addAll(newCompressors);
            // Track usage count for each compressor
            for (String compressor : newCompressors) {
                compressorUsageCounts.put(compressor, compressorUsageCounts.getOrDefault(compressor, 0L) + 1);
            }
        } else {
            // Track connections with no compression
            compressorUsageCounts.put("none", compressorUsageCounts.getOrDefault("none", 0L) + 1);
        }
    }
    
    public void addConnectionWithUsername(String remoteHost, String username) {
        connectionCount++;
        if (remoteHost != null) {
            remoteHosts.add(extractHostFromRemoteHost(remoteHost));
        }
        
        String usernameKey = (username != null) ? username : "none";
        connectionsByUsername.put(usernameKey, connectionsByUsername.getOrDefault(usernameKey, 0L) + 1);
        
        if (username != null) {
            usernames.add(username);
        }
    }
    
    // Getters
    public String getDriverName() {
        return driverName;
    }
    
    public String getDriverVersion() {
        return driverVersion;
    }
    
    public Set<String> getCompressors() {
        return Collections.unmodifiableSet(compressors);
    }
    
    public String getCompressorsString() {
        if (compressors == null || compressors.isEmpty()) {
            return "none";
        }
        // Simply return the compressor names since each entry represents one unique set
        return compressors.stream()
            .sorted()
            .collect(java.util.stream.Collectors.joining(", "));
    }
    
    public Map<String, Long> getCompressorUsageCounts() {
        return Collections.unmodifiableMap(compressorUsageCounts);
    }
    
    public String getOsType() {
        return osType != null ? osType : "unknown";
    }
    
    public String getOsName() {
        return osName != null ? osName : "unknown";
    }
    
    public String getPlatform() {
        return platform != null ? platform : "unknown";
    }
    
    public String getServerVersion() {
        return serverVersion != null ? serverVersion : "unknown";
    }
    
    public long getConnectionCount() {
        return connectionCount;
    }
    
    public int getUniqueHosts() {
        return remoteHosts.size();
    }
    
    public Set<String> getUsernames() {
        return Collections.unmodifiableSet(usernames);
    }
    
    public String getUsernamesString() {
        if (usernames.isEmpty()) {
            return "none";
        }
        return String.join(", ", usernames);
    }
    
    public Map<String, Long> getConnectionsByUsername() {
        return Collections.unmodifiableMap(connectionsByUsername);
    }
    
    public void addSampleMetadataMessage(String message) {
        if (sampleMetadataMessages.size() < MAX_SAMPLE_MESSAGES && message != null) {
            sampleMetadataMessages.add(message);
        }
    }
    
    public void addSampleAuthMessage(String message) {
        if (sampleAuthMessages.size() < MAX_SAMPLE_MESSAGES && message != null) {
            sampleAuthMessages.add(message);
        }
    }
    
    // Helper to extract connection ID from a log message
    private String extractConnectionId(String logMessage) {
        if (logMessage != null && logMessage.contains("\"ctx\":\"conn")) {
            try {
                int start = logMessage.indexOf("\"ctx\":\"conn") + 7;
                int end = logMessage.indexOf("\"", start);
                if (end > start) {
                    return logMessage.substring(start, end);
                }
            } catch (Exception e) {
                // Ignore parsing errors
            }
        }
        return null;
    }
    
    // Helper to extract just the host portion from remoteHost (removes port)
    private String extractHostFromRemoteHost(String remoteHost) {
        if (remoteHost == null) {
            return null;
        }
        
        // Handle IPv6 addresses like [::1]:27017
        if (remoteHost.startsWith("[")) {
            int closeBracket = remoteHost.indexOf(']');
            if (closeBracket > 0) {
                return remoteHost.substring(0, closeBracket + 1);
            }
        }
        
        // Handle IPv4 addresses and hostnames like 10.0.1.100:27017 or localhost:27017
        int lastColon = remoteHost.lastIndexOf(':');
        if (lastColon > 0) {
            return remoteHost.substring(0, lastColon);
        }
        
        // No port found, return as-is
        return remoteHost;
    }
    
    public List<String> getSampleMetadataMessages() {
        return Collections.unmodifiableList(sampleMetadataMessages);
    }
    
    public List<String> getSampleAuthMessages() {
        return Collections.unmodifiableList(sampleAuthMessages);
    }
    
    public String getDriverNameAndVersion() {
        return driverName + " " + driverVersion;
    }
    
    public long getAverageConnectionLifetimeMillis() {
        return averageConnectionLifetimeMillis;
    }
    
    public void setAverageConnectionLifetimeMillis(long lifetimeMillis) {
        this.averageConnectionLifetimeMillis = lifetimeMillis;
    }
    
    public long getMaxConnectionLifetimeMillis() {
        return maxConnectionLifetimeMillis;
    }
    
    public String getFormattedMaxConnectionLifetime() {
        if (maxConnectionLifetimeMillis == 0) {
            return "N/A";
        }
        
        if (maxConnectionLifetimeMillis < 1000) {
            return maxConnectionLifetimeMillis + "ms";
        }
        
        long seconds = maxConnectionLifetimeMillis / 1000;
        long remainingMs = maxConnectionLifetimeMillis % 1000;
        
        if (seconds < 60) {
            if (remainingMs == 0) {
                return seconds + "s";
            } else {
                return String.format("%.1fs", maxConnectionLifetimeMillis / 1000.0);
            }
        } else {
            long minutes = seconds / 60;
            long remainingSecs = seconds % 60;
            if (remainingSecs == 0) {
                return minutes + "m";
            } else {
                return String.format("%dm %ds", minutes, remainingSecs);
            }
        }
    }
    
    public void addConnectionLifetime(long lifetimeMillis) {
        connectionLifetimes.add(lifetimeMillis);
        
        // Update max lifetime
        if (lifetimeMillis > maxConnectionLifetimeMillis) {
            maxConnectionLifetimeMillis = lifetimeMillis;
        }
        
        // Recalculate average
        this.averageConnectionLifetimeMillis = (long) connectionLifetimes.stream()
            .mapToLong(Long::longValue)
            .average()
            .orElse(0.0);
    }
    
    public String getFormattedAverageConnectionLifetime() {
        if (averageConnectionLifetimeMillis == 0) {
            return "N/A";
        }
        
        // For very short durations, show milliseconds
        if (averageConnectionLifetimeMillis < 1000) {
            return averageConnectionLifetimeMillis + "ms";
        }
        
        long seconds = averageConnectionLifetimeMillis / 1000;
        long remainingMs = averageConnectionLifetimeMillis % 1000;
        
        // For durations under 10 seconds, show seconds with millisecond precision
        if (seconds < 10) {
            if (remainingMs > 0) {
                return String.format("%.1fs", averageConnectionLifetimeMillis / 1000.0);
            } else {
                return seconds + "s";
            }
        }
        // For durations under 60 seconds, show just seconds
        else if (seconds < 60) {
            return seconds + "s";
        } 
        // For durations under 1 hour, show minutes and seconds
        else if (seconds < 3600) {
            long minutes = seconds / 60;
            long remainingSeconds = seconds % 60;
            if (remainingSeconds > 0) {
                return minutes + "m " + remainingSeconds + "s";
            } else {
                return minutes + "m";
            }
        } 
        // For longer durations, show hours and minutes
        else {
            long hours = seconds / 3600;
            long minutes = (seconds % 3600) / 60;
            if (minutes > 0) {
                return hours + "h " + minutes + "m";
            } else {
                return hours + "h";
            }
        }
    }
    
    @Override
    public String toString() {
        return String.format("%-40s %-15s %-20s %-15s %-15s %8d %8d",
                getDriverNameAndVersion(),
                driverVersion,
                getCompressorsString(),
                getOsType(),
                getServerVersion(),
                connectionCount,
                getUniqueHosts());
    }
}