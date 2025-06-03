package com.mongodb.logparse;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

/**
 * Accumulator entry for tracking execution statistics by plan cache key
 */
public class PlanCacheAccumulatorEntry {
    
    private final PlanCacheKey key;
    
    private long count;
    private long total;
    private long min = Long.MAX_VALUE;
    private long max = Long.MIN_VALUE;
    
    private long totalKeysExamined;
    private long totalDocsExamined;
    private long totalReturned = 0;
    private long reslen = 0;
    private long bytesRead = 0;
    private long collectionScanCount = 0;
    
    // Statistics for percentiles
    private DescriptiveStatistics executionStats = new DescriptiveStatistics();
    private DescriptiveStatistics keysExaminedStats = new DescriptiveStatistics();
    private DescriptiveStatistics docsExaminedStats = new DescriptiveStatistics();
    
    public PlanCacheAccumulatorEntry(PlanCacheKey key) {
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
        
        // Track collection scans
        if (isCollectionScan()) {
            collectionScanCount++;
        }
    }
    
    public boolean isCollectionScan() {
        return key.getPlanSummary() != null && 
               key.getPlanSummary().contains("COLLSCAN");
    }
    
    // Getters
    public PlanCacheKey getKey() {
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
    
    public double getCollectionScanPercentage() {
        return count > 0 ? (collectionScanCount * 100.0) / count : 0.0;
    }
    
    @Override
    public String toString() {
        String truncatedPlanCacheKey = key.getPlanCacheKey() != null ? 
            (key.getPlanCacheKey().length() > 20 ? 
                key.getPlanCacheKey().substring(0, 17) + "..." : key.getPlanCacheKey()) : "null";
                
        String truncatedQueryHash = key.getQueryHash() != null ?
            (key.getQueryHash().length() > 15 ? 
                key.getQueryHash().substring(0, 12) + "..." : key.getQueryHash()) : "null";
                
        String truncatedPlanSummary = key.getPlanSummary() != null ?
            (key.getPlanSummary().length() > 30 ? 
                key.getPlanSummary().substring(0, 27) + "..." : key.getPlanSummary()) : "UNKNOWN";
        
        return String.format("%-50s %-20s %-15s %-30s %8d %8d %8d %8d %8.0f %10d %10d %10d %8d %8.1f",
                key.getNamespace().toString(),
                truncatedPlanCacheKey,
                truncatedQueryHash,
                truncatedPlanSummary,
                count,
                getMin(),
                max,
                getAvg(),
                getPercentile95(),
                total / 1000,
                getAvgKeysExamined(),
                getAvgDocsExamined(),
                getAvgReturned(),
                getCollectionScanPercentage());
    }
    
    public String toCsvString() {
        return String.format("%s,%s,%s,%s,%d,%d,%d,%d,%.0f,%d,%d,%d,%.0f,%.0f,%.1f,%.1f,%d,%d,%d,%.1f",
                key.getNamespace(),
                escapeCsv(key.getPlanCacheKey()),
                escapeCsv(key.getQueryHash()),
                escapeCsv(key.getPlanSummary()),
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
                collectionScanCount,
                getCollectionScanPercentage());
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