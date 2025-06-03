package com.mongodb.logparse;

import java.io.File;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

public class LogLineAccumulator {
    
    private Namespace namespace;
    private String operation;
    private File file;
    
    private final static double ONE_MB_DOUBLE = 1024.0 * 1024.0;
    private final static long ONE_MB = 1024 * 1024;
    
    private long count;
    private long total;
    private long min = Long.MAX_VALUE;
    private long max = Long.MIN_VALUE;
    
    private long totalKeysExamined;
    private long totalDocsExamined;
    
    private long totalReturned = 0;
    
    private long reslen = 0;
    
    private long bytesRead = 0;
    
    
    DescriptiveStatistics executionStats = new DescriptiveStatistics();
    
    DescriptiveStatistics keysExaminedStats = new DescriptiveStatistics();
    DescriptiveStatistics docsExaminedStats = new DescriptiveStatistics();
    
    
    public LogLineAccumulator(File file, String command, Namespace namespace2) {
        this.namespace = namespace2;
        this.operation = command;
        this.file = file;
    }

    public void addExecution(long amt) {
        count++;
        total += amt;
        if (amt > max) {
            max = amt;
        }
        if (amt < min) {
            min = amt;
        }
        //executionStats.addValue(amt);
    }
    
    public double getKeysExaminedPercentile95() {
        if (totalKeysExamined > 0) {
            return keysExaminedStats.getPercentile(95);
        }
        return 0.0d;
    }
    
    public double getDocsExaminedPercentile95() {
        if (totalDocsExamined > 0) {
            return docsExaminedStats.getPercentile(95);
        }
        return 0.0d;
    }
    
    public double getPercentile95() {
        return executionStats.getPercentile(95);
    }
    
    public long getAvgDocsExamined() {
        return totalDocsExamined/count;
    }
    
    public long getTotalDocsExamined() {
        return totalDocsExamined;
    }
    
    public String toString() {
        return String.format("%-65s %-20s %-10s %-10.1f %-10d %-10d %-10d %-10d %-10d %-10d %-12d %-10d %-10d", 
        		namespace, operation, count, reslen/ONE_MB_DOUBLE, bytesRead/ONE_MB, min, max, total/count, total/1000,
                totalKeysExamined/count, totalDocsExamined/count, 
                //getKeysExaminedPercentile95(), getDocsExaminedPercentile95(), totalKeysExamined/1000.0, totalDocsExamined/1000.0, 
                totalReturned/count, getScannedReturnRatio());
        
    }
    
    public String toCsvString() {
        return String.format("%s,%s,%d,%d,%d,%10d,%10.0f,%10d,%10d,%10d,%10.0f,%10.0f,%12.1f,%12.1f,%10d,%10d", namespace, operation, count, 
                min, max, total/count, getPercentile95(), total/1000,
                totalKeysExamined/count, totalDocsExamined/count, 
                getKeysExaminedPercentile95(), getDocsExaminedPercentile95(), 
                totalKeysExamined/1000.0, totalDocsExamined/1000.0, totalReturned/count, getScannedReturnRatio());
        
    }
    
    

    public long getScannedReturnRatio() {
        if (totalReturned > 0) {
            return totalDocsExamined/totalReturned;
        }
        return 0;
    }

    public void addExamined(Long keysExamined, Long docsExamined) {
        totalDocsExamined += docsExamined;
        totalKeysExamined += keysExamined;
        //keysExaminedStats.addValue(keysExamined);
        //docsExaminedStats.addValue(docsExamined);
    }
    
    public void addReturned(Long nReturned) {
        totalReturned += nReturned;
    }
    
    public void addReslen(Long reslen) {
        this.reslen += reslen;
    }

    public long getCount() {
        return count;
    }
    
    public long getMax() {
        return max;
    }

    public long getAvg() {
        return total/count;
    }

    public long getAvgReturned() {
        return totalReturned/count;
    }

	public void addBytesRead(Long bytesRead) {
		this.bytesRead += bytesRead; 
	}

}
