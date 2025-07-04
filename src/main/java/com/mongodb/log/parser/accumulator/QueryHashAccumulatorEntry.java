package com.mongodb.log.parser.accumulator;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import com.mongodb.log.parser.SlowQuery;

/**
 * Accumulator entry for tracking execution statistics by query hash
 */
public class QueryHashAccumulatorEntry {
    
    private final QueryHashKey key;
    
    private long count;
    private long total;
    private long min = Long.MAX_VALUE;
    private long max = Long.MIN_VALUE;
    
    private long totalKeysExamined;
    private long totalDocsExamined;
    private long totalReturned = 0;
    private long reslen = 0;
    private long bytesRead = 0;
    private long totalShards = 0;
    
    // Planning time tracking
    private long totalPlanningTimeMicros = 0;
    private long planningTimeCount = 0;
    private long minPlanningTimeMicros = Long.MAX_VALUE;
    private long maxPlanningTimeMicros = Long.MIN_VALUE;
    
    // Replanning tracking
    private long replannedCount = 0;
    private long multiPlannerCount = 0;
    private Map<String, Long> replanReasons = new HashMap<>();
    
    // Plan summary tracking - store the most recent one
    private String planSummary = null;
    
    // Statistics for percentiles
    private DescriptiveStatistics executionStats = new DescriptiveStatistics();
    private DescriptiveStatistics keysExaminedStats = new DescriptiveStatistics();
    private DescriptiveStatistics docsExaminedStats = new DescriptiveStatistics();
    private DescriptiveStatistics planningTimeStats = new DescriptiveStatistics();
    
    // Read preference tracking - store detailed breakdown
    private Map<String, Long> readPreferenceCounts = new HashMap<>();
    
    // Store one example of the sanitized query
    private String sanitizedQuery = null;
    
    // Store a sample log message for accordion display - store the slowest query
    private String sampleLogMessage = null;
    private long maxDurationForSample = 0;
    
    public QueryHashAccumulatorEntry(QueryHashKey key) {
        this.key = key;
    }
    
    public void addExecution(SlowQuery slowQuery) {
        addExecution(slowQuery, null);
    }
    
    public void addExecution(SlowQuery slowQuery, String logMessage) {
        if (slowQuery.durationMillis != null) {
            count++;
            total += slowQuery.durationMillis;
            
            if (slowQuery.durationMillis > max) {
                max = slowQuery.durationMillis;
            }
            if (slowQuery.durationMillis < min) {
                min = slowQuery.durationMillis;
            }
            
            // Limit stats collection to prevent memory issues
            if (executionStats.getN() < 10000) {
                executionStats.addValue(slowQuery.durationMillis);
            }
        }
        
        if (slowQuery.keysExamined != null) {
            totalKeysExamined += slowQuery.keysExamined;
            if (keysExaminedStats.getN() < 10000) {
                keysExaminedStats.addValue(slowQuery.keysExamined);
            }
        }
        
        if (slowQuery.docsExamined != null) {
            totalDocsExamined += slowQuery.docsExamined;
            if (docsExaminedStats.getN() < 10000) {
                docsExaminedStats.addValue(slowQuery.docsExamined);
            }
        }
        
        if (slowQuery.nreturned != null) {
            totalReturned += slowQuery.nreturned;
        }
        
        if (slowQuery.reslen != null) {
            reslen += slowQuery.reslen;
        }
        
        if (slowQuery.bytesRead != null) {
            bytesRead += slowQuery.bytesRead;
        }
        
        if (slowQuery.nShards != null) {
            totalShards += slowQuery.nShards;
        }
        
        // Track planning time
        if (slowQuery.planningTimeMicros != null) {
            planningTimeCount++;
            totalPlanningTimeMicros += slowQuery.planningTimeMicros;
            
            if (slowQuery.planningTimeMicros > maxPlanningTimeMicros) {
                maxPlanningTimeMicros = slowQuery.planningTimeMicros;
            }
            if (slowQuery.planningTimeMicros < minPlanningTimeMicros) {
                minPlanningTimeMicros = slowQuery.planningTimeMicros;
            }
            
            if (planningTimeStats.getN() < 10000) {
                planningTimeStats.addValue(slowQuery.planningTimeMicros);
            }
        }
        
        // Track replanning events
        if (slowQuery.replanned != null && slowQuery.replanned) {
            replannedCount++;
            
            // Track replan reasons
            if (slowQuery.replanReason != null) {
                replanReasons.merge(slowQuery.replanReason, 1L, Long::sum);
            }
        }
        
        // Track multi-planner usage
        if (slowQuery.fromMultiPlanner != null && slowQuery.fromMultiPlanner) {
            multiPlannerCount++;
        }
        
        // Track plan summary (use the most recent one)
        if (slowQuery.planSummary != null) {
            planSummary = slowQuery.planSummary;
        }
        
        // Track read preferences with detailed breakdown
        if (slowQuery.readPreference != null && !slowQuery.readPreference.isEmpty()) {
            readPreferenceCounts.merge(slowQuery.readPreference, 1L, Long::sum);
        } else {
            // Track when no read preference is specified
            readPreferenceCounts.merge("none", 1L, Long::sum);
        }
        
        // Store sanitized query if we don't have one yet
        if (sanitizedQuery == null && slowQuery.sanitizedFilter != null) {
            sanitizedQuery = slowQuery.sanitizedFilter;
        }
        
        // Store sample log message if this is the slowest query we've seen
        if (logMessage != null && slowQuery.durationMillis != null && slowQuery.durationMillis >= maxDurationForSample) {
            sampleLogMessage = logMessage;
            maxDurationForSample = slowQuery.durationMillis;
        }
    }
    
    // Getters
    public QueryHashKey getKey() {
        return key;
    }
    
    public long getCount() {
        return count;
    }
    
    public long getMin() {
        return count > 0 ? min : 0;
    }
    
    public long getMax() {
        return max;
    }
    
    public long getAvg() {
        return count > 0 ? total / count : 0;
    }
    
    public long getAvgReturned() {
        return count > 0 ? totalReturned / count : 0;
    }
    
    public long getAvgDocsExamined() {
        return count > 0 ? totalDocsExamined / count : 0;
    }
    
    public long getAvgKeysExamined() {
        return count > 0 ? totalKeysExamined / count : 0;
    }
    
    public long getAvgShards() {
        return count > 0 ? totalShards / count : 0;
    }
    
    public long getTotalKeysExamined() {
        return totalKeysExamined;
    }
    
    public long getTotalDocsExamined() {
        return totalDocsExamined;
    }
    
    public long getScannedReturnRatio() {
        if (totalReturned > 0) {
            return totalDocsExamined / totalReturned;
        }
        return 0;
    }
    
    public double getPercentile95() {
        return executionStats.getN() > 0 ? executionStats.getPercentile(95) : 0.0;
    }
    
    public double getKeysExaminedPercentile95() {
        return keysExaminedStats.getN() > 0 ? keysExaminedStats.getPercentile(95) : 0.0;
    }
    
    public double getDocsExaminedPercentile95() {
        return docsExaminedStats.getN() > 0 ? docsExaminedStats.getPercentile(95) : 0.0;
    }
    
    /**
     * Get a detailed breakdown of read preferences with counts
     * Format: "mode:primary(150),mode:secondary(50),tags:region(25)"
     */
    public String getReadPreferenceSummary() {
        if (readPreferenceCounts.isEmpty()) {
            return "default: " + count;
        }
        
        return readPreferenceCounts.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .map(entry -> {
                    String key = entry.getKey();
                    Long value = entry.getValue();
                    
                    // Handle special cases
                    if ("none".equals(key)) {
                        return "default: " + value;
                    }
                    
                    // Extract mode from JSON-like strings
                    if (key.contains("\"mode\":")) {
                        // Parse {"mode":"secondaryPreferred"} format
                        String mode = key.replaceAll(".*\"mode\":\\s*\"([^\"]+)\".*", "$1");
                        return mode + ": " + value;
                    }
                    
                    // Handle other formats - remove JSON brackets and quotes
                    String cleanKey = key.replaceAll("[{}\"\\s]", "")
                                        .replaceAll("mode:", "");
                    
                    return cleanKey + ": " + value;
                })
                .collect(Collectors.joining("<br>"));
    }

    /**
     * Get read preference summary with per-line truncation for HTML display
     */
    public String getReadPreferenceSummaryTruncated(int maxLineLength) {
        String fullSummary = getReadPreferenceSummary();
        
        if (!fullSummary.contains("<br>")) {
            // Single line - use normal truncation
            return truncateString(fullSummary, maxLineLength);
        }
        
        // Multi-line - truncate each line individually
        String[] lines = fullSummary.split("<br>");
        StringBuilder result = new StringBuilder();
        
        for (int i = 0; i < lines.length; i++) {
            if (i > 0) {
                result.append("<br>");
            }
            result.append(truncateString(lines[i], maxLineLength));
        }
        
        return result.toString();
    }
    
    /**
     * Get just the most common read preference for simpler display
     */
    public String getPrimaryReadPreference() {
        if (readPreferenceCounts.isEmpty()) {
            return "none";
        }
        
        return readPreferenceCounts.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse("none");
    }
    
    public String getSanitizedQuery() {
        return sanitizedQuery != null ? sanitizedQuery : "none";
    }
    
    public String getSampleLogMessage() {
        return sampleLogMessage;
    }
    
    // Planning time getters (converted to milliseconds)
    public long getMinPlanningTimeMs() {
        return planningTimeCount > 0 ? Math.round(minPlanningTimeMicros / 1000.0) : 0;
    }
    
    public long getMaxPlanningTimeMs() {
        return planningTimeCount > 0 ? Math.round(maxPlanningTimeMicros / 1000.0) : 0;
    }
    
    public long getAvgPlanningTimeMs() {
        return planningTimeCount > 0 ? Math.round((totalPlanningTimeMicros / planningTimeCount) / 1000.0) : 0;
    }
    
    public double getPlanningTimePercentile95Ms() {
        return planningTimeStats.getN() > 0 ? planningTimeStats.getPercentile(95) / 1000.0 : 0.0;
    }
    
    // Replanning getters
    public long getReplannedCount() {
        return replannedCount;
    }
    
    public double getReplannedPercentage() {
        return count > 0 ? (replannedCount * 100.0) / count : 0.0;
    }
    
    public long getMultiPlannerCount() {
        return multiPlannerCount;
    }
    
    public double getMultiPlannerPercentage() {
        return count > 0 ? (multiPlannerCount * 100.0) / count : 0.0;
    }
    
    public Map<String, Long> getReplanReasons() {
        return new HashMap<>(replanReasons);
    }
    
    public String getMostCommonReplanReason() {
        return replanReasons.entrySet().stream()
            .max(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey)
            .orElse("none");
    }
    
    // Plan summary getter
    public String getPlanSummary() {
        return planSummary != null ? planSummary : "UNKNOWN";
    }
    
    /**
     * Get a truncated version of the sanitized query for display
     */
    public String getTruncatedSanitizedQuery(int maxLength) {
        String query = getSanitizedQuery();
        if (query.length() <= maxLength) {
            return query;
        }
        return query.substring(0, maxLength - 3) + "...";
    }
    
    @Override
    public String toString() {
        // Dynamic truncation based on reasonable limits
        String truncatedQueryHash = truncateString(key.getQueryHash(), 12);
        String truncatedNamespace = truncateString(key.getNamespace().toString(), 45);
        String truncatedOperation = truncateString(key.getOperation(), 15);
        String truncatedReadPref = truncateString(getReadPreferenceSummary(), 40);
        String truncatedQuery = truncateString(getSanitizedQuery(), 80);
        
        return String.format("%-12s %-45s %-15s %8d %8d %8d %8d %8.0f %10d %10d %10d %8.0f %8.0f %8.1f %8.1f %8d %8d %-40s %-80s",
                truncatedQueryHash,
                truncatedNamespace,
                truncatedOperation,
                count,
                getMin(),
                max,
                getAvg(),
                getPercentile95(),
                total / 1000,
                getAvgKeysExamined(),
                getAvgDocsExamined(),
                getKeysExaminedPercentile95(),
                getDocsExaminedPercentile95(),
                totalKeysExamined / 1000.0,
                totalDocsExamined / 1000.0,
                getAvgReturned(),
                getScannedReturnRatio(),
                truncatedReadPref,
                truncatedQuery);
    }
    
    public String toCsvString() {
        return String.format("%s,%s,%s,%d,%d,%d,%d,%.0f,%d,%d,%d,%.0f,%.0f,%.1f,%.1f,%d,%d,%s,%s",
                escapeCsv(key.getQueryHash()),
                escapeCsv(key.getNamespace().toString()),
                escapeCsv(key.getOperation()),
                count,
                getMin(),
                max,
                getAvg(),
                getPercentile95(),
                total / 1000,
                getAvgKeysExamined(),
                getAvgDocsExamined(),
                getKeysExaminedPercentile95(),
                getDocsExaminedPercentile95(),
                totalKeysExamined / 1000.0,
                totalDocsExamined / 1000.0,
                getAvgReturned(),
                getScannedReturnRatio(),
                escapeCsv(getReadPreferenceSummary()),
                escapeCsv(getSanitizedQuery()));
    }
    
    private String truncateString(String value, int maxLength) {
        if (value == null) {
            return "null";
        }
        if (value.length() <= maxLength) {
            return value;
        }
        if (maxLength <= 3) {
            return value.substring(0, maxLength);
        }
        return value.substring(0, maxLength - 3) + "...";
    }
    
    private String escapeCsv(String value) {
        if (value == null) {
            return "";
        }
        if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }
}