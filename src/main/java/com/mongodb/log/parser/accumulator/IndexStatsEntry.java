package com.mongodb.log.parser.accumulator;

import com.mongodb.log.parser.Namespace;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

/**
 * Entry for tracking index usage statistics by namespace and plan summary
 */
public class IndexStatsEntry {
    
    private final Namespace namespace;
    private final String planSummary;
    
    private long count;
    private long totalDurationMs;
    private long minDurationMs = Long.MAX_VALUE;
    private long maxDurationMs = Long.MIN_VALUE;
    
    private long totalKeysExamined;
    private long totalDocsExamined;
    private long totalReturned;
    
    // Statistics for percentiles
    private DescriptiveStatistics executionStats = new DescriptiveStatistics();
    
    public IndexStatsEntry(Namespace namespace, String planSummary) {
        this.namespace = namespace;
        this.planSummary = planSummary != null ? planSummary : "UNKNOWN";
    }
    
    public void addOperation(Long durationMs, Long keysExamined, Long docsExamined, Long returned) {
        if (durationMs != null) {
            count++;
            totalDurationMs += durationMs;
            
            if (durationMs > maxDurationMs) {
                maxDurationMs = durationMs;
            }
            if (durationMs < minDurationMs) {
                minDurationMs = durationMs;
            }
            
            // Limit stats collection to prevent memory issues
            if (executionStats.getN() < 10000) {
                executionStats.addValue(durationMs);
            }
        }
        
        if (keysExamined != null) {
            totalKeysExamined += keysExamined;
        }
        
        if (docsExamined != null) {
            totalDocsExamined += docsExamined;
        }
        
        if (returned != null) {
            totalReturned += returned;
        }
    }
    
    // Getters
    public Namespace getNamespace() {
        return namespace;
    }
    
    public String getPlanSummary() {
        return planSummary;
    }
    
    public long getCount() {
        return count;
    }
    
    public long getMinDurationMs() {
        return count > 0 ? minDurationMs : 0;
    }
    
    public long getMaxDurationMs() {
        return maxDurationMs;
    }
    
    public long getAvgDurationMs() {
        return count > 0 ? totalDurationMs / count : 0;
    }
    
    public double getPercentile95() {
        return executionStats.getN() > 0 ? executionStats.getPercentile(95) : 0.0;
    }
    
    public long getTotalDurationSec() {
        return totalDurationMs / 1000;
    }
    
    public long getAvgKeysExamined() {
        return count > 0 ? totalKeysExamined / count : 0;
    }
    
    public long getAvgDocsExamined() {
        return count > 0 ? totalDocsExamined / count : 0;
    }
    
    public long getAvgReturned() {
        return count > 0 ? totalReturned / count : 0;
    }
    
    public long getExaminedToReturnedRatio() {
        return totalReturned > 0 ? (totalKeysExamined + totalDocsExamined) / totalReturned : 0;
    }
    
    public boolean isCollectionScan() {
        return planSummary != null && planSummary.contains("COLLSCAN");
    }
    
    @Override
    public String toString() {
        return String.format("%-50s %-80s %8d %8d %8d %8d %8.0f %10d %10d %10d %10d",
                namespace.toString(),
                planSummary,
                count,
                getMinDurationMs(),
                maxDurationMs,
                getAvgDurationMs(),
                getPercentile95(),
                getTotalDurationSec(),
                getAvgKeysExamined(),
                getAvgDocsExamined(),
                getAvgReturned());
    }
}