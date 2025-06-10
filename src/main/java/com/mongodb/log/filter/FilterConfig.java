package com.mongodb.log.filter;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Simplified configuration class for MongoDB log filtering.
 * Maintains a single list of patterns to ignore.
 */
public class FilterConfig {
    
    private Set<String> ignorePatterns = new HashSet<>();
    
    public FilterConfig() {
        initializeDefaults();
    }
    
    private void initializeDefaults() {
        // Single consolidated list of patterns to ignore
        ignorePatterns.addAll(Arrays.asList(
            // Network operations
            "\"c\":\"NETWORK\"",
            "\"c\":\"ACCESS\"",
            "\"c\":\"CONNPOOL\"",
            
            // Administrative/health checks
            "\"hello\":1",
            "\"isMaster\":1", 
            "\"ping\":1",
            "\"serverStatus\":1",
            "\"buildInfo\"",
            "\"getParameter\":",
            "\"getCmdLineOpts\":1",
            "\"getDefaultRWConcern\":1",
            "\"listDatabases\":1",
            
            // Session management
            "\"endSessions\":",
            "\"startSession\"",
            "\"saslContinue\":1",
            
            // Replication (very noisy)
            "\"replSetHeartbeat\":\"",
            "replSetUpdatePosition",
            "\"replSetGetStatus\":1",
            
            // System databases (usually internal)
            "\"$db\":\"local\"", 
            "\"$db\":\"config\"",
            "\"ns\":\"local.oplog.rs\"",
            "\"ns\":\"local.clustermanager\"",
            "\"ns\":\"config.system.sessions\"",
            "\"ns\":\"config.mongos\"",
            
            // Storage/control operations
            "\"c\":\"STORAGE\"",
            "\"c\":\"CONTROL\"",
            "\"c\":\"SHARDING\"",
            
            // Stats/monitoring
            "\"dbstats\":1",
            "\"collStats\":\"",
            "\"listIndexes\":\"",
            
            // TTL/maintenance 
            "\"ctx\":\"TTLMonitor\"",
            "\"logRotate\":\""
        ));
    }
    
    /**
     * Load patterns from properties file.
     * Supports:
     * - filter.ignore.patterns: comma-separated list (replaces defaults)
     * - filter.ignore.add: comma-separated list (adds to defaults)  
     * - filter.ignore.remove: comma-separated list (removes from current set)
     */
    public void loadFromProperties(Properties props) {
        // Replace entire list if specified
        String ignoreList = props.getProperty("filter.ignore.patterns");
        if (ignoreList != null && !ignoreList.trim().isEmpty()) {
            ignorePatterns.clear();
            addPatterns(ignoreList);
        }
        
        // Add additional patterns
        String additionalPatterns = props.getProperty("filter.ignore.add");
        if (additionalPatterns != null && !additionalPatterns.trim().isEmpty()) {
            addPatterns(additionalPatterns);
        }
        
        // Remove specific patterns
        String removePatterns = props.getProperty("filter.ignore.remove");
        if (removePatterns != null && !removePatterns.trim().isEmpty()) {
            removePatterns(removePatterns);
        }
    }
    
    private void addPatterns(String patternList) {
        String[] patterns = patternList.split(",");
        for (String pattern : patterns) {
            String trimmed = pattern.trim();
            if (!trimmed.isEmpty()) {
                ignorePatterns.add(trimmed);
            }
        }
    }
    
    private void removePatterns(String patternList) {
        String[] patterns = patternList.split(",");
        for (String pattern : patterns) {
            ignorePatterns.remove(pattern.trim());
        }
    }
    
    public Set<String> getIgnorePatterns() {
        return new HashSet<>(ignorePatterns);
    }
    
    public void addPattern(String pattern) {
        ignorePatterns.add(pattern);
    }
    
    public void removePattern(String pattern) {
        ignorePatterns.remove(pattern);
    }
    
    public boolean shouldIgnore(String line) {
        return ignorePatterns.stream().anyMatch(line::contains);
    }
}