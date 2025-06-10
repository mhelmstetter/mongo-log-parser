package com.mongodb.log.parser;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import java.util.HashMap;
import java.util.Map;

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
    
    // Statistics for percentiles
    private DescriptiveStatistics executionStats = new DescriptiveStatistics();
    private DescriptiveStatistics keysExaminedStats = new DescriptiveStatistics();
    private DescriptiveStatistics docsExaminedStats = new DescriptiveStatistics();
    
    // Read preference tracking
    private Map<String, Long> readPreferenceCounts = new HashMap<>();
    
    // Store one example of the sanitized query
    private String sanitizedQuery = null;
    
    public QueryHashAccumulatorEntry(QueryHashKey key) {
        this.key = key;
    }
    
    public void addExecution(SlowQuery slowQuery) {
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
        
        // Track read preferences
        if (slowQuery.readPreference != null) {
            readPreferenceCounts.merge(slowQuery.readPreference, 1L, Long::sum);
        }
        
        // Store sanitized query if we don't have one yet
        if (sanitizedQuery == null && slowQuery.sanitizedFilter != null) {
            sanitizedQuery = slowQuery.sanitizedFilter;
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
    
    public String getReadPreferenceSummary() {
        if (readPreferenceCounts.isEmpty()) {
            return "none";
        }
        
        if (readPreferenceCounts.size() == 1) {
            Map.Entry<String, Long> entry = readPreferenceCounts.entrySet().iterator().next();
            return entry.getKey() + "(" + entry.getValue() + ")";
        } else {
            StringBuilder sb = new StringBuilder();
            readPreferenceCounts.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .forEach(entry -> {
                    if (sb.length() > 0) sb.append(", ");
                    sb.append(entry.getKey()).append("(").append(entry.getValue()).append(")");
                });
            return sb.toString();
        }
    }
    
    public String getSanitizedQuery() {
        return sanitizedQuery != null ? sanitizedQuery : "none";
    }
    
    @Override
    public String toString() {
        // Dynamic truncation based on reasonable limits
        String truncatedQueryHash = truncateString(key.getQueryHash(), 12);
        String truncatedNamespace = truncateString(key.getNamespace().toString(), 45);
        String truncatedOperation = truncateString(key.getOperation(), 15);
        String truncatedReadPref = truncateString(getReadPreferenceSummary(), 30);
        String truncatedQuery = truncateString(getSanitizedQuery(), 100);
        
        return String.format("%-12s %-45s %-15s %8d %8d %8d %8d %8.0f %10d %10d %10d %8.0f %8.0f %8.1f %8.1f %8d %8d %-30s %-100s",
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