package com.mongodb.log.parser;

import java.io.File;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

public class LogLineAccumulator {
    
    private Namespace namespace;
    private String operation;
    private File file;
    
    private final static double ONE_MB_DOUBLE = 1024.0 * 1024.0;
    
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
    
    // Storage bytes tracking with min/max
    private long totalStorageBytesRead = 0;
    private long minStorageBytesRead = Long.MAX_VALUE;
    private long maxStorageBytesRead = Long.MIN_VALUE;
    private long totalStorageBytesWritten = 0;
    private long minStorageBytesWritten = Long.MAX_VALUE;
    private long maxStorageBytesWritten = Long.MIN_VALUE;
    
    // Sample log message storage - store the log message for the slowest query
    private String sampleLogMessage = null;
    private long maxDurationForSample = 0;
    
    // Statistics for percentiles (optional, can be disabled for performance)
    private DescriptiveStatistics executionStats = new DescriptiveStatistics();
    private DescriptiveStatistics keysExaminedStats = new DescriptiveStatistics();
    private DescriptiveStatistics docsExaminedStats = new DescriptiveStatistics();
    
    public LogLineAccumulator(File file, String operation, Namespace namespace) {
        this.namespace = namespace;
        this.operation = operation;
        this.file = file;
    }

    public void addExecution(long durationMs) {
        count++;
        total += durationMs;
        
        // Properly track min/max
        if (durationMs > max) {
            max = durationMs;
        }
        if (durationMs < min) {
            min = durationMs;
        }
        
        // Only collect detailed stats if needed (can be expensive)
        if (executionStats.getN() < 10000) { // Limit to prevent memory issues
            executionStats.addValue(durationMs);
        }
    }
    
    public void addExamined(Long keysExamined, Long docsExamined) {
        if (docsExamined != null) {
            totalDocsExamined += docsExamined;
            if (docsExaminedStats.getN() < 10000) {
                docsExaminedStats.addValue(docsExamined);
            }
        }
        if (keysExamined != null) {
            totalKeysExamined += keysExamined;
            if (keysExaminedStats.getN() < 10000) {
                keysExaminedStats.addValue(keysExamined);
            }
        }
    }
    
    public void addReturned(Long nReturned) {
        if (nReturned != null) {
            totalReturned += nReturned;
        }
    }
    
    public void addReslen(Long reslen) {
        if (reslen != null) {
            this.reslen += reslen;
        }
    }

    public void addBytesRead(Long bytesRead) {
        if (bytesRead != null) {
            this.bytesRead += bytesRead;
        }
    }
    
    public void addShards(Long nShards) {
        if (nShards != null) {
            this.totalShards += nShards;
        }
    }
    
    public void addStorageBytesRead(Long bytesRead) {
        if (bytesRead != null) {
            this.totalStorageBytesRead += bytesRead;
            if (bytesRead > maxStorageBytesRead) {
                maxStorageBytesRead = bytesRead;
            }
            if (bytesRead < minStorageBytesRead) {
                minStorageBytesRead = bytesRead;
            }
        }
    }
    
    public void addStorageBytesWritten(Long bytesWritten) {
        if (bytesWritten != null) {
            this.totalStorageBytesWritten += bytesWritten;
            if (bytesWritten > maxStorageBytesWritten) {
                maxStorageBytesWritten = bytesWritten;
            }
            if (bytesWritten < minStorageBytesWritten) {
                minStorageBytesWritten = bytesWritten;
            }
        }
    }

    // Getter methods
    public long getCount() {
        return count;
    }
    
    public long getMax() {
        return max;
    }
    
    public long getMin() {
        return count > 0 ? min : 0;
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
    
    public long getTotalDocsExamined() {
        return totalDocsExamined;
    }
    
    public long getTotalKeysExamined() {
        return totalKeysExamined;
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
    
    public long getAvgShards() {
        return count > 0 ? totalShards / count : 0;
    }
    
    public long getAvgBytesRead() {
        return count > 0 ? totalStorageBytesRead / count : 0;
    }
    
    public long getMaxBytesRead() {
        return minStorageBytesRead != Long.MAX_VALUE ? maxStorageBytesRead : 0;
    }
    
    public long getAvgBytesWritten() {
        return count > 0 ? totalStorageBytesWritten / count : 0;
    }
    
    public long getMaxBytesWritten() {
        return minStorageBytesWritten != Long.MAX_VALUE ? maxStorageBytesWritten : 0;
    }
    
    public String toString() {
        return String.format("%-65s %-20s %10d %10.1f %10d %10d %10d %10d %10d %10d %10d %10d %10d", 
                namespace, operation, count, reslen/ONE_MB_DOUBLE, bytesRead/1048576, 
                getMin(), max, getAvg(), total/1000,
                count > 0 ? totalKeysExamined/count : 0, 
                count > 0 ? totalDocsExamined/count : 0, 
                count > 0 ? totalReturned/count : 0, 
                getScannedReturnRatio());
    }
    
    public String toCsvString() {
        return String.format("%s,%s,%d,%d,%d,%d,%.0f,%d,%d,%d,%.0f,%.0f,%.1f,%.1f,%d,%d", 
                namespace, operation, count, 
                getMin(), max, getAvg(), getPercentile95(), total/1000,
                count > 0 ? totalKeysExamined/count : 0, 
                count > 0 ? totalDocsExamined/count : 0, 
                getKeysExaminedPercentile95(), getDocsExaminedPercentile95(), 
                totalKeysExamined/1000.0, totalDocsExamined/1000.0, 
                count > 0 ? totalReturned/count : 0, 
                getScannedReturnRatio());
    }
    
    public void addSampleLogMessage(String logMessage, long durationMs) {
        // Store sample log message if this is the slowest query we've seen
        if (logMessage != null && durationMs >= maxDurationForSample) {
            sampleLogMessage = logMessage;
            maxDurationForSample = durationMs;
        }
    }
    
    // Backward compatibility method - deprecated but kept for now
    public void addSampleLogMessage(String logMessage) {
        // If no duration provided, only store if we don't have one yet
        if (sampleLogMessage == null && logMessage != null) {
            sampleLogMessage = logMessage;
        }
    }
    
    public String getSampleLogMessage() {
        return sampleLogMessage;
    }
    
    public String getNamespace() {
        return namespace != null ? namespace.toString() : "";
    }
    
    public String getOperation() {
        return operation != null ? operation : "";
    }
}