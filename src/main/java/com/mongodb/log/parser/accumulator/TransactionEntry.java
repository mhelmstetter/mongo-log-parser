package com.mongodb.log.parser.accumulator;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Entry class for tracking transaction statistics
 */
public class TransactionEntry {
    
    private final TransactionKey key;
    private final AtomicLong count;
    
    // Duration tracking (milliseconds)
    private long totalDurationMs = 0;
    private long minDurationMs = Long.MAX_VALUE;
    private long maxDurationMs = Long.MIN_VALUE;
    private long durationCount = 0;
    
    // Commit duration tracking (converted from micros to millis)
    private long totalCommitMs = 0;
    private long maxCommitMs = Long.MIN_VALUE;
    private long commitCount = 0;
    
    // Time active tracking (converted from micros to millis)
    private long totalTimeActiveMs = 0;
    private long maxTimeActiveMs = Long.MIN_VALUE;
    private long timeActiveCount = 0;
    
    // Time inactive tracking (converted from micros to millis)
    private long totalTimeInactiveMs = 0;
    private long maxTimeInactiveMs = Long.MIN_VALUE;
    private long timeInactiveCount = 0;
    
    public TransactionEntry(TransactionKey key) {
        this.key = key;
        this.count = new AtomicLong(0);
    }
    
    public synchronized void addTransaction(Long durationMillis, Long commitDurationMicros, 
                                          Long timeActiveMicros, Long timeInactiveMicros) {
        count.incrementAndGet();
        
        // Track duration in milliseconds
        if (durationMillis != null) {
            durationCount++;
            totalDurationMs += durationMillis;
            
            if (durationMillis > maxDurationMs) {
                maxDurationMs = durationMillis;
            }
            if (durationMillis < minDurationMs) {
                minDurationMs = durationMillis;
            }
        }
        
        // Track commit duration (convert micros to millis)
        if (commitDurationMicros != null) {
            commitCount++;
            long commitMs = Math.round(commitDurationMicros / 1000.0);
            totalCommitMs += commitMs;
            
            if (commitMs > maxCommitMs) {
                maxCommitMs = commitMs;
            }
        }
        
        // Track time active (convert micros to millis)
        if (timeActiveMicros != null) {
            timeActiveCount++;
            long activeMs = Math.round(timeActiveMicros / 1000.0);
            totalTimeActiveMs += activeMs;
            
            if (activeMs > maxTimeActiveMs) {
                maxTimeActiveMs = activeMs;
            }
        }
        
        // Track time inactive (convert micros to millis)
        if (timeInactiveMicros != null) {
            timeInactiveCount++;
            long inactiveMs = Math.round(timeInactiveMicros / 1000.0);
            totalTimeInactiveMs += inactiveMs;
            
            if (inactiveMs > maxTimeInactiveMs) {
                maxTimeInactiveMs = inactiveMs;
            }
        }
    }
    
    // Getters
    public TransactionKey getKey() {
        return key;
    }
    
    public long getCount() {
        return count.get();
    }
    
    public long getMinDurationMs() {
        return durationCount > 0 ? minDurationMs : 0;
    }
    
    public long getMaxDurationMs() {
        return durationCount > 0 ? maxDurationMs : 0;
    }
    
    public long getAvgDurationMs() {
        return durationCount > 0 ? totalDurationMs / durationCount : 0;
    }
    
    public long getMaxCommitMs() {
        return commitCount > 0 ? maxCommitMs : 0;
    }
    
    public long getAvgCommitMs() {
        return commitCount > 0 ? totalCommitMs / commitCount : 0;
    }
    
    public long getMaxTimeActiveMs() {
        return timeActiveCount > 0 ? maxTimeActiveMs : 0;
    }
    
    public long getAvgTimeActiveMs() {
        return timeActiveCount > 0 ? totalTimeActiveMs / timeActiveCount : 0;
    }
    
    public long getMaxTimeInactiveMs() {
        return timeInactiveCount > 0 ? maxTimeInactiveMs : 0;
    }
    
    public long getAvgTimeInactiveMs() {
        return timeInactiveCount > 0 ? totalTimeInactiveMs / timeInactiveCount : 0;
    }
    
    public String toCsvString() {
        return String.format("%s,%s,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d",
            escapeCsv(key.getTxnRetryCounter() != null ? key.getTxnRetryCounter().toString() : ""),
            escapeCsv(key.getTerminationCause()),
            escapeCsv(key.getCommitType()),
            count.get(),
            getMinDurationMs(),
            getMaxDurationMs(),
            getAvgDurationMs(),
            getMaxCommitMs(),
            getAvgCommitMs(),
            getMaxTimeActiveMs(),
            getAvgTimeActiveMs(),
            getMaxTimeInactiveMs(),
            getAvgTimeInactiveMs());
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
