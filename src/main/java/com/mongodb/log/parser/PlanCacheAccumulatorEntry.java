package com.mongodb.log.parser;

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
    
    // Planning time tracking
    private long totalPlanningTimeMicros = 0;
    private long planningTimeCount = 0;
    private long minPlanningTimeMicros = Long.MAX_VALUE;
    private long maxPlanningTimeMicros = Long.MIN_VALUE;
    
    // Replanning tracking
    private long replannedCount = 0;
    private long multiPlannerCount = 0;
    private java.util.Map<String, Long> replanReasons = new java.util.HashMap<>();
    
    // Statistics for percentiles
    private DescriptiveStatistics executionStats = new DescriptiveStatistics();
    private DescriptiveStatistics keysExaminedStats = new DescriptiveStatistics();
    private DescriptiveStatistics docsExaminedStats = new DescriptiveStatistics();
    private DescriptiveStatistics planningTimeStats = new DescriptiveStatistics();
    
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
        
        // Track collection scans based on current query's plan summary
        if (slowQuery.planSummary != null && slowQuery.planSummary.contains("COLLSCAN")) {
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
    
    public java.util.Map<String, Long> getReplanReasons() {
        return new java.util.HashMap<>(replanReasons);
    }
    
    public String getMostCommonReplanReason() {
        return replanReasons.entrySet().stream()
            .max(java.util.Map.Entry.comparingByValue())
            .map(java.util.Map.Entry::getKey)
            .orElse("none");
    }
    
    @Override
    public String toString() {
        // Dynamic truncation based on reasonable limits
        String truncatedNamespace = truncateString(key.getNamespace().toString(), 45);
        String truncatedPlanCacheKey = truncateString(key.getPlanCacheKey(), 12);
        String truncatedQueryHash = truncateString(key.getQueryHash(), 10);
        String truncatedPlanSummary = truncateString(key.getPlanSummary(), 80);
        
        return String.format("%-45s %-12s %-10s %-80s %8d %8d %8d %8d %8.0f %10d %10d %10d %8d %8d %8d %8.1f",
                truncatedNamespace,
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
                getMinPlanningTimeMs(),
                getMaxPlanningTimeMs(),
                getAvgPlanningTimeMs(),
                getReplannedPercentage());
    }
    
    public String toCsvString() {
        return String.format("%s,%s,%s,%s,%d,%d,%d,%d,%.0f,%d,%d,%d,%.0f,%.0f,%.1f,%.1f,%d,%d,%d,%.1f,%d,%d,%d,%.0f,%d,%.1f,%d,%.1f,%s",
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
                getCollectionScanPercentage(),
                getMinPlanningTimeMs(),
                getMaxPlanningTimeMs(),
                getAvgPlanningTimeMs(),
                getPlanningTimePercentile95Ms(),
                replannedCount,
                getReplannedPercentage(),
                multiPlannerCount,
                getMultiPlannerPercentage(),
                escapeCsv(getMostCommonReplanReason()));
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