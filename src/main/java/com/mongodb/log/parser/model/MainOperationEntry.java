package com.mongodb.log.parser.model;

import com.mongodb.log.parser.LogLineAccumulator;

/**
 * POJO representing a single entry in the Main Operations table.
 * This serves as the Model in the MVC pattern for HTML report generation.
 */
public class MainOperationEntry {
    private String namespace;
    private String operation;
    private long count;
    private long minMs;
    private long maxMs;
    private long avgMs;
    private double p95Ms;
    private long totalSec;
    private long avgKeysExamined;
    private long avgDocsExamined;
    private double keysP95;
    private double docsP95;
    private double totalKeysK;
    private double totalDocsK;
    private long avgReturned;
    private long exRetRatio;
    private long avgShards;
    private long avgBytesRead;
    private long maxBytesRead;
    private long avgBytesWritten;
    private long maxBytesWritten;
    private String sampleLogMessage;
    private boolean hasCollScan;
    
    public MainOperationEntry() {
        // Default constructor
    }
    
    /**
     * Constructor that creates a MainOperationEntry from a LogLineAccumulator
     */
    public MainOperationEntry(LogLineAccumulator accumulator) {
        this.namespace = accumulator.getNamespace();
        this.operation = accumulator.getOperation();
        this.count = accumulator.getCount();
        this.minMs = accumulator.getMin();
        this.maxMs = accumulator.getMax();
        this.avgMs = accumulator.getAvg();
        this.p95Ms = accumulator.getPercentile95();
        this.totalSec = accumulator.getCount() > 0 ? accumulator.getAvg() * accumulator.getCount() / 1000 : 0;
        this.avgKeysExamined = accumulator.getAvgKeysExamined();
        this.avgDocsExamined = accumulator.getAvgDocsExamined();
        this.keysP95 = accumulator.getKeysExaminedPercentile95();
        this.docsP95 = accumulator.getDocsExaminedPercentile95();
        this.totalKeysK = accumulator.getTotalKeysExamined() / 1000.0;
        this.totalDocsK = accumulator.getTotalDocsExamined() / 1000.0;
        this.avgReturned = accumulator.getAvgReturned();
        this.exRetRatio = accumulator.getScannedReturnRatio();
        this.avgShards = accumulator.getAvgShards();
        this.avgBytesRead = accumulator.getAvgBytesRead();
        this.maxBytesRead = accumulator.getMaxBytesRead();
        this.avgBytesWritten = accumulator.getAvgBytesWritten();
        this.maxBytesWritten = accumulator.getMaxBytesWritten();
        this.sampleLogMessage = accumulator.getSampleLogMessage();
        this.hasCollScan = (accumulator.getSampleLogMessage() != null && 
                           accumulator.getSampleLogMessage().contains("COLLSCAN"));
    }

    // Getters and setters
    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public long getMinMs() {
        return minMs;
    }

    public void setMinMs(long minMs) {
        this.minMs = minMs;
    }

    public long getMaxMs() {
        return maxMs;
    }

    public void setMaxMs(long maxMs) {
        this.maxMs = maxMs;
    }

    public long getAvgMs() {
        return avgMs;
    }

    public void setAvgMs(long avgMs) {
        this.avgMs = avgMs;
    }

    public double getP95Ms() {
        return p95Ms;
    }

    public void setP95Ms(double p95Ms) {
        this.p95Ms = p95Ms;
    }

    public long getTotalSec() {
        return totalSec;
    }

    public void setTotalSec(long totalSec) {
        this.totalSec = totalSec;
    }

    public long getAvgKeysExamined() {
        return avgKeysExamined;
    }

    public void setAvgKeysExamined(long avgKeysExamined) {
        this.avgKeysExamined = avgKeysExamined;
    }

    public long getAvgDocsExamined() {
        return avgDocsExamined;
    }

    public void setAvgDocsExamined(long avgDocsExamined) {
        this.avgDocsExamined = avgDocsExamined;
    }

    public double getKeysP95() {
        return keysP95;
    }

    public void setKeysP95(double keysP95) {
        this.keysP95 = keysP95;
    }

    public double getDocsP95() {
        return docsP95;
    }

    public void setDocsP95(double docsP95) {
        this.docsP95 = docsP95;
    }

    public double getTotalKeysK() {
        return totalKeysK;
    }

    public void setTotalKeysK(double totalKeysK) {
        this.totalKeysK = totalKeysK;
    }

    public double getTotalDocsK() {
        return totalDocsK;
    }

    public void setTotalDocsK(double totalDocsK) {
        this.totalDocsK = totalDocsK;
    }

    public long getAvgReturned() {
        return avgReturned;
    }

    public void setAvgReturned(long avgReturned) {
        this.avgReturned = avgReturned;
    }

    public long getExRetRatio() {
        return exRetRatio;
    }

    public void setExRetRatio(long exRetRatio) {
        this.exRetRatio = exRetRatio;
    }

    public long getAvgShards() {
        return avgShards;
    }

    public void setAvgShards(long avgShards) {
        this.avgShards = avgShards;
    }

    public long getAvgBytesRead() {
        return avgBytesRead;
    }

    public void setAvgBytesRead(long avgBytesRead) {
        this.avgBytesRead = avgBytesRead;
    }

    public long getMaxBytesRead() {
        return maxBytesRead;
    }

    public void setMaxBytesRead(long maxBytesRead) {
        this.maxBytesRead = maxBytesRead;
    }

    public long getAvgBytesWritten() {
        return avgBytesWritten;
    }

    public void setAvgBytesWritten(long avgBytesWritten) {
        this.avgBytesWritten = avgBytesWritten;
    }

    public long getMaxBytesWritten() {
        return maxBytesWritten;
    }

    public void setMaxBytesWritten(long maxBytesWritten) {
        this.maxBytesWritten = maxBytesWritten;
    }

    public String getSampleLogMessage() {
        return sampleLogMessage;
    }

    public void setSampleLogMessage(String sampleLogMessage) {
        this.sampleLogMessage = sampleLogMessage;
    }

    public boolean isHasCollScan() {
        return hasCollScan;
    }

    public void setHasCollScan(boolean hasCollScan) {
        this.hasCollScan = hasCollScan;
    }

    /**
     * Returns the CSS class for this row (e.g., "collscan" for collection scans)
     */
    public String getCssClass() {
        return hasCollScan ? "collscan" : "";
    }

    /**
     * Returns the formatted count with thousands separator
     */
    public String getFormattedCount() {
        return String.format("%,d", count);
    }

    /**
     * Returns the formatted P95 with one decimal place
     */
    public String getFormattedP95Ms() {
        return String.format("%.1f", p95Ms);
    }

    /**
     * Returns the formatted keys P95 with one decimal place
     */
    public String getFormattedKeysP95() {
        return String.format("%.1f", keysP95);
    }

    /**
     * Returns the formatted docs P95 with one decimal place
     */
    public String getFormattedDocsP95() {
        return String.format("%.1f", docsP95);
    }

    /**
     * Returns the formatted total keys in thousands with one decimal place
     */
    public String getFormattedTotalKeysK() {
        return String.format("%.1f", totalKeysK);
    }

    /**
     * Returns the formatted total docs in thousands with one decimal place
     */
    public String getFormattedTotalDocsK() {
        return String.format("%.1f", totalDocsK);
    }

    /**
     * Returns the formatted average returned with thousands separator
     */
    public String getFormattedAvgReturned() {
        return String.format("%,d", avgReturned);
    }

    /**
     * Returns the formatted average keys examined with thousands separator
     */
    public String getFormattedAvgKeysExamined() {
        return String.format("%,d", avgKeysExamined);
    }

    /**
     * Returns the formatted average docs examined with thousands separator
     */
    public String getFormattedAvgDocsExamined() {
        return String.format("%,d", avgDocsExamined);
    }

    /**
     * Returns the formatted average shards with thousands separator
     */
    public String getFormattedAvgShards() {
        return String.format("%,d", avgShards);
    }

    /**
     * Returns the formatted average bytes read in human-readable format
     */
    public String getFormattedAvgBytesRead() {
        return formatBytes(avgBytesRead);
    }

    /**
     * Returns the formatted maximum bytes read in human-readable format
     */
    public String getFormattedMaxBytesRead() {
        return formatBytes(maxBytesRead);
    }

    /**
     * Returns the formatted average bytes written in human-readable format
     */
    public String getFormattedAvgBytesWritten() {
        return formatBytes(avgBytesWritten);
    }

    /**
     * Returns the formatted maximum bytes written in human-readable format
     */
    public String getFormattedMaxBytesWritten() {
        return formatBytes(maxBytesWritten);
    }

    /**
     * Returns HTML table cell with formatted bytes and sort value attribute
     */
    public String getFormattedAvgBytesReadCell() {
        return "<td class=\"number\" data-sort-value=\"" + avgBytesRead + "\">" + formatBytes(avgBytesRead) + "</td>";
    }

    /**
     * Returns HTML table cell with formatted bytes and sort value attribute
     */
    public String getFormattedMaxBytesReadCell() {
        return "<td class=\"number\" data-sort-value=\"" + maxBytesRead + "\">" + formatBytes(maxBytesRead) + "</td>";
    }

    /**
     * Returns HTML table cell with formatted bytes and sort value attribute
     */
    public String getFormattedAvgBytesWrittenCell() {
        return "<td class=\"number\" data-sort-value=\"" + avgBytesWritten + "\">" + formatBytes(avgBytesWritten) + "</td>";
    }

    /**
     * Returns HTML table cell with formatted bytes and sort value attribute
     */
    public String getFormattedMaxBytesWrittenCell() {
        return "<td class=\"number\" data-sort-value=\"" + maxBytesWritten + "\">" + formatBytes(maxBytesWritten) + "</td>";
    }

    /**
     * Helper method to format bytes in human-readable format
     */
    private String formatBytes(long bytes) {
        if (bytes == 0) return "0";
        if (bytes < 1024) return bytes + "B";
        if (bytes < 1024 * 1024) return String.format("%.1fKB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.1fMB", bytes / (1024.0 * 1024.0));
        return String.format("%.1fGB", bytes / (1024.0 * 1024.0 * 1024.0));
    }

    @Override
    public String toString() {
        return String.format("MainOperationEntry{namespace='%s', operation='%s', count=%d, avgMs=%d}",
                namespace, operation, count, avgMs);
    }
}