package com.mongodb.logparse;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Configuration class for MongoDB log filtering.
 * Separates noise (connection spam) from system operations (potentially useful but filtered).
 */
public class FilterConfig {
    
    // Noise categories - these are typically spam/noise that provide little value
    private Set<String> noiseCategories = new HashSet<>();
    
    // System categories - these might be useful for debugging but are filtered by default
    private Set<String> systemCategories = new HashSet<>();
    
    public FilterConfig() {
        initializeDefaults();
    }
    
    private void initializeDefaults() {
        // NOISE PATTERNS - High-frequency, low-value log entries
        noiseCategories.addAll(Arrays.asList(
            // Network connection spam
            "\"c\":\"NETWORK\"",
            
            // Authentication spam (successful auths)
            "\"c\":\"ACCESS\"",
            
            // Connection pool management
            "\"c\":\"CONNPOOL\"",
            
            // Health check spam
            "\"hello\":1",
            "\"isMaster\":1",
            "\"ping\":1",
            
            // Session management spam
            "\"endSessions\":",
            "\"startSession\"",
            
            // SASL continuation spam
            "\"saslContinue\":1",
            
            // Replication heartbeat spam (very frequent)
            "\"replSetHeartbeat\":\"",
            "replSetUpdatePosition",
            
            // Administrative queries that happen frequently
            "\"getDefaultRWConcern\":1",
            "\"listDatabases\":1",
            "\"getCmdLineOpts\":1",
            "\"getParameter\":",
            "\"buildInfo\"",
            "\"logRotate\":\""
        ));
        
        // SYSTEM PATTERNS - Potentially useful but typically filtered
        systemCategories.addAll(Arrays.asList(
            // Control plane operations
            "\"c\":\"CONTROL\"",
            
            // Storage engine operations
            "\"c\":\"STORAGE\"",
            
            // Sharding operations (might be important in sharded clusters)
            "\"c\":\"SHARDING\"",
            
            // Server status queries (periodic but potentially useful)
            "\"serverStatus\":1",
            "\"replSetGetStatus\":1",
            
            // Statistics collection
            "\"dbstats\":1",
            "\"collStats\":\"",
            
            // Index maintenance queries
            "\"listIndexes\":\"",
            
            // Profile setting changes
            "\"profile\":0",
            "\"profile\":1", 
            "\"profile\":2",
            
            // Config database operations (might be important)
            "\"$db\":\"config\"",
            "\"ns\":\"config.",
            
            // Local database operations (usually internal)
            "\"$db\":\"local\"",
            "\"ns\":\"local.oplog.rs\"",
            "\"ns\":\"local.clustermanager\"",
            
            // Admin database operations
            "\"$db\":\"admin\""
        ));
    }
    
    public void loadFromProperties(Properties props) {
        // Load noise categories
        String noiseList = props.getProperty("filter.noise.categories");
        if (noiseList != null && !noiseList.trim().isEmpty()) {
            noiseCategories.clear();
            String[] patterns = noiseList.split(",");
            for (String pattern : patterns) {
                String trimmed = pattern.trim();
                if (!trimmed.isEmpty()) {
                    noiseCategories.add(trimmed);
                }
            }
        }
        
        // Load system categories  
        String systemList = props.getProperty("filter.system.categories");
        if (systemList != null && !systemList.trim().isEmpty()) {
            systemCategories.clear();
            String[] patterns = systemList.split(",");
            for (String pattern : patterns) {
                String trimmed = pattern.trim();
                if (!trimmed.isEmpty()) {
                    systemCategories.add(trimmed);
                }
            }
        }
        
        // Allow adding to defaults instead of replacing
        String additionalNoise = props.getProperty("filter.noise.additional");
        if (additionalNoise != null && !additionalNoise.trim().isEmpty()) {
            String[] patterns = additionalNoise.split(",");
            for (String pattern : patterns) {
                String trimmed = pattern.trim();
                if (!trimmed.isEmpty()) {
                    noiseCategories.add(trimmed);
                }
            }
        }
        
        String additionalSystem = props.getProperty("filter.system.additional");
        if (additionalSystem != null && !additionalSystem.trim().isEmpty()) {
            String[] patterns = additionalSystem.split(",");
            for (String pattern : patterns) {
                String trimmed = pattern.trim();
                if (!trimmed.isEmpty()) {
                    systemCategories.add(trimmed);
                }
            }
        }
        
        // Allow removing patterns
        String removeNoise = props.getProperty("filter.noise.remove");
        if (removeNoise != null && !removeNoise.trim().isEmpty()) {
            String[] patterns = removeNoise.split(",");
            for (String pattern : patterns) {
                noiseCategories.remove(pattern.trim());
            }
        }
        
        String removeSystem = props.getProperty("filter.system.remove");
        if (removeSystem != null && !removeSystem.trim().isEmpty()) {
            String[] patterns = removeSystem.split(",");
            for (String pattern : patterns) {
                systemCategories.remove(pattern.trim());
            }
        }
    }
    
    public Set<String> getNoiseCategories() {
        return new HashSet<>(noiseCategories);
    }
    
    public Set<String> getSystemCategories() {
        return new HashSet<>(systemCategories);
    }
    
    public void addNoisePattern(String pattern) {
        noiseCategories.add(pattern);
    }
    
    public void addSystemPattern(String pattern) {
        systemCategories.add(pattern);
    }
    
    public void removeNoisePattern(String pattern) {
        noiseCategories.remove(pattern);
    }
    
    public void removeSystemPattern(String pattern) {
        systemCategories.remove(pattern);
    }
    
    public boolean isNoisePattern(String line) {
        return noiseCategories.stream().anyMatch(line::contains);
    }
    
    public boolean isSystemPattern(String line) {
        return systemCategories.stream().anyMatch(line::contains);
    }
    
}