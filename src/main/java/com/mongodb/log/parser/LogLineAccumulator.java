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
    private long mirroredCount = 0;
    private long totalBatchStatements = 0;
    private long batchCount = 0;
    private long shardedOpCount = 0;
    private long multiShardOpCount = 0;
    private long maxShards = 0;

    // CPU (cpuNanos); entries without cpuNanos are not counted
    private long cpuCount = 0;
    private long totalCpuNanos = 0;
    private long maxCpuNanos = 0;

    // mongos time spent waiting on shard responses (remoteOpWaitMillis)
    private long totalRemoteOpWaitMs = 0;
    private long remoteOpWaitCount = 0;

    // Write matching (nMatched / nModified)
    private long totalMatched = 0;
    private long totalModified = 0;
    private long matchedCount = 0;
    private long maxMatched = 0;
    
    // Storage bytes tracking with min/max
    private long totalStorageBytesRead = 0;
    private long minStorageBytesRead = Long.MAX_VALUE;
    private long maxStorageBytesRead = Long.MIN_VALUE;
    private long totalStorageBytesWritten = 0;
    private long minStorageBytesWritten = Long.MAX_VALUE;
    private long maxStorageBytesWritten = Long.MIN_VALUE;
    
    // Write conflicts tracking
    private long totalWriteConflicts = 0;

    // Dirty bytes, flow-control and write-concern waits on writes
    private final com.mongodb.log.parser.accumulator.WriteWaitStats writeWaits = new com.mongodb.log.parser.accumulator.WriteWaitStats();
    
    // Error count tracking
    private long errorCount = 0;
    
    // Sample log message storage - store the log message for the slowest query
    private String sampleLogMessage = null;
    private long maxDurationForSample = 0;

    // Track if this is a change stream operation
    private boolean isChangeStream = false;

    // Track application name
    private String appName = null;
    
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
            this.shardedOpCount++;
            if (nShards > 1) {
                this.multiShardOpCount++;
            }
            if (nShards > maxShards) {
                maxShards = nShards;
            }
        }
    }

    public void addBatchSize(Long batchSize) {
        if (batchSize != null) {
            totalBatchStatements += batchSize;
            batchCount++;
        }
    }

    /** Average statements per batched write command, or 0 when the operation isn't batched. */
    public long getAvgBatchSize() {
        return batchCount > 0 ? totalBatchStatements / batchCount : 0;
    }

    public void addMirrored(Boolean mirrored) {
        if (Boolean.TRUE.equals(mirrored)) {
            mirroredCount++;
        }
    }

    public long getMirroredCount() {
        return mirroredCount;
    }

    public void addCpu(Long cpuNanos) {
        if (cpuNanos != null) {
            cpuCount++;
            totalCpuNanos += cpuNanos;
            if (cpuNanos > maxCpuNanos) {
                maxCpuNanos = cpuNanos;
            }
        }
    }

    /** Number of entries that logged cpuNanos. */
    public long getCpuCount() {
        return cpuCount;
    }

    public long getTotalCpuNanos() {
        return totalCpuNanos;
    }

    /** Max cpuNanos; only meaningful when getCpuCount() > 0. */
    public long getMaxCpuNanos() {
        return maxCpuNanos;
    }

    public void addRemoteOpWait(Long remoteOpWaitMillis) {
        if (remoteOpWaitMillis != null) {
            this.totalRemoteOpWaitMs += remoteOpWaitMillis;
            this.remoteOpWaitCount++;
        }
    }

    public void addMatched(Long nMatched, Long nModified) {
        if (nMatched != null) {
            this.totalMatched += nMatched;
            this.matchedCount++;
            if (nMatched > maxMatched) {
                maxMatched = nMatched;
            }
            if (nModified != null) {
                this.totalModified += nModified;
            }
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

    public void addWriteConflicts(Long writeConflicts) {
        if (writeConflicts != null) {
            this.totalWriteConflicts += writeConflicts;
        }
    }
    
    public void addError() {
        this.errorCount++;
    }

    // Getter methods
    public long getCount() {
        return count;
    }
    
    public long getMax() {
        return count > 0 && max != Long.MIN_VALUE ? max : 0;
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
    
    public long getTotalWriteConflicts() {
        return totalWriteConflicts;
    }

    /** Write conflicts per operation, as a decimal (an integer average hid anything below one per op). */
    public double getAvgWriteConflicts() {
        return count > 0 ? (double) totalWriteConflicts / count : 0.0;
    }

    public void addWriteWaits(com.mongodb.log.parser.SlowQuery q) {
        writeWaits.add(q);
    }

    public com.mongodb.log.parser.accumulator.WriteWaitStats getWriteWaits() {
        return writeWaits;
    }

    public long getTotalDurationMs() {
        return total;
    }

    public long getShardedOpCount() {
        return shardedOpCount;
    }

    public double getAvgShardsExact() {
        return shardedOpCount > 0 ? (double) totalShards / shardedOpCount : 0.0;
    }

    public double getMultiShardPercentage() {
        return shardedOpCount > 0 ? multiShardOpCount * 100.0 / shardedOpCount : 0.0;
    }

    public long getMaxShards() {
        return maxShards;
    }

    public long getRemoteOpWaitCount() {
        return remoteOpWaitCount;
    }

    public long getTotalRemoteOpWaitMs() {
        return totalRemoteOpWaitMs;
    }

    public long getAvgRemoteOpWaitMs() {
        return remoteOpWaitCount > 0 ? totalRemoteOpWaitMs / remoteOpWaitCount : 0;
    }

    // Share of total execution time spent waiting on shards
    public double getRemoteOpWaitPercentage() {
        return total > 0 ? totalRemoteOpWaitMs * 100.0 / total : 0.0;
    }

    public long getMatchedCount() {
        return matchedCount;
    }

    public long getAvgMatched() {
        return matchedCount > 0 ? totalMatched / matchedCount : 0;
    }

    public long getAvgModified() {
        return matchedCount > 0 ? totalModified / matchedCount : 0;
    }

    public long getMaxMatched() {
        return maxMatched;
    }
    
    // Response length (reslen) getters
    public long getTotalReslen() {
        return reslen;
    }
    
    public long getAvgReslen() {
        return count > 0 ? reslen / count : 0;
    }
    
    // Returned documents (nreturned) getters
    public long getTotalReturned() {
        return totalReturned;
    }
    
    // Note: getAvgReturned() already exists above
    
    // Storage bytes read getters (for the bytesRead from storage.data.bytesRead)
    public long getTotalStorageBytesRead() {
        return totalStorageBytesRead;
    }
    
    public long getAvgStorageBytesRead() {
        return count > 0 ? totalStorageBytesRead / count : 0;
    }
    
    // Regular bytes read getters (for response bytesRead)
    public long getTotalBytesRead() {
        return bytesRead;
    }
    
    public long getAvgBytesReadResponse() {
        return count > 0 ? bytesRead / count : 0;
    }
    
    // Error count getters
    public long getErrorCount() {
        return errorCount;
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

    public boolean isChangeStream() {
        return isChangeStream;
    }

    public void setChangeStream(boolean isChangeStream) {
        this.isChangeStream = isChangeStream;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        // Only set if not already set (use first occurrence)
        if (this.appName == null && appName != null) {
            this.appName = appName;
        }
    }

    public String getNamespace() {
        return namespace != null ? namespace.toString() : "";
    }

    public String getOperation() {
        return operation != null ? operation : "";
    }
}