package com.mongodb.log.parser.accumulator;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Accumulator for tracking MongoDB error codes found in log entries
 */
public class ErrorCodeAccumulator {
    
    private Map<String, ErrorCodeEntry> errorCodeEntries = new HashMap<>();
    
    private String[] headers = new String[] { 
        "CodeName", "ErrorCode", "Count", "SampleErrorMessage"
    };

    /**
     * Accumulates an error code entry from a log line
     */
    public synchronized void accumulate(String codeName, Integer errorCode, String errorMessage) {
        if (codeName == null || codeName.isEmpty()) {
            return;
        }
        
        ErrorCodeEntry entry = errorCodeEntries.get(codeName);
        if (entry == null) {
            entry = new ErrorCodeEntry(codeName, errorCode, errorMessage);
            errorCodeEntries.put(codeName, entry);
        } else {
            entry.incrementCount();
            // Update error code if we didn't have one before
            if (entry.getErrorCode() == null && errorCode != null) {
                entry.setErrorCode(errorCode);
            }
            // Update sample message if we didn't have one before
            if (entry.getSampleErrorMessage() == null && errorMessage != null) {
                entry.setSampleErrorMessage(errorMessage);
            }
        }
    }
    
    /**
     * Prints a formatted report of all error codes found
     */
    public void report() {
        if (errorCodeEntries.isEmpty()) {
            System.out.println("No error codes found in logs");
            return;
        }
        
        System.out.println("\n=== Error Code Analysis ===");
        
        // Calculate column widths
        final int maxCodeNameWidth = Math.min(
            Math.max("CodeName".length(), 
                errorCodeEntries.keySet().stream()
                    .mapToInt(String::length)
                    .max()
                    .orElse(20)),
            40); // Cap at reasonable width
        
        final int maxErrorMessageWidth = Math.max("SampleErrorMessage".length(), 80);
        
        // Print header
        String headerFormat = String.format("%%-%ds %%10s %%10s %%-%ds", 
            maxCodeNameWidth, maxErrorMessageWidth);
        System.out.println(String.format(headerFormat,
            "CodeName", "ErrorCode", "Count", "SampleErrorMessage"));
        
        // Print separator
        int separatorLength = maxCodeNameWidth + 10 + 10 + maxErrorMessageWidth + 3;
        System.out.println("=".repeat(separatorLength));
        
        // Print entries sorted by count (descending)
        String dataFormat = String.format("%%-%ds %%10s %%10d %%-%ds", 
            maxCodeNameWidth, maxErrorMessageWidth);
        
        errorCodeEntries.values().stream()
            .sorted(Comparator.comparingLong(ErrorCodeEntry::getCount).reversed())
            .forEach(entry -> {
                String truncatedCodeName = truncateString(entry.getCodeName(), maxCodeNameWidth);
                String errorCodeStr = entry.getErrorCode() != null ? 
                    entry.getErrorCode().toString() : "unknown";
                String truncatedMessage = truncateString(entry.getSampleErrorMessage(), maxErrorMessageWidth);
                
                System.out.println(String.format(dataFormat,
                    truncatedCodeName,
                    errorCodeStr,
                    entry.getCount(),
                    truncatedMessage));
            });
        
        // Print summary
        long totalErrors = errorCodeEntries.values().stream()
            .mapToLong(ErrorCodeEntry::getCount)
            .sum();
        long uniqueErrorCodes = errorCodeEntries.size();
        
        System.out.println("\n=== Error Code Summary ===");
        System.out.println(String.format("Total error occurrences: %,d", totalErrors));
        System.out.println(String.format("Unique error codes: %,d", uniqueErrorCodes));
        
        // Show top 5 most frequent errors
        System.out.println("\n=== Top 5 Most Frequent Errors ===");
        errorCodeEntries.values().stream()
            .sorted(Comparator.comparingLong(ErrorCodeEntry::getCount).reversed())
            .limit(5)
            .forEach(entry -> {
                double percentage = (entry.getCount() * 100.0) / totalErrors;
                System.out.println(String.format("  %s: %,d (%.1f%%)", 
                    entry.getCodeName(), entry.getCount(), percentage));
            });
    }
    
    /**
     * Exports error code data to CSV format
     */
    public void reportCsv(String fileName) throws FileNotFoundException {
        PrintWriter writer = new PrintWriter(fileName);
        writer.println(String.join(",", headers));
        
        errorCodeEntries.values().stream()
            .sorted(Comparator.comparingLong(ErrorCodeEntry::getCount).reversed())
            .forEach(entry -> writer.println(entry.toCsvString()));
        
        writer.close();
    }
    
    /**
     * Returns the map of error code entries for external access
     */
    public Map<String, ErrorCodeEntry> getErrorCodeEntries() {
        return new HashMap<>(errorCodeEntries);
    }
    
    /**
     * Returns true if any error codes have been accumulated
     */
    public boolean hasErrors() {
        return !errorCodeEntries.isEmpty();
    }
    
    /**
     * Returns the total number of error occurrences
     */
    public long getTotalErrorCount() {
        return errorCodeEntries.values().stream()
            .mapToLong(ErrorCodeEntry::getCount)
            .sum();
    }
    
    /**
     * Truncates a string to fit within the specified width
     */
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
    
    /**
     * Inner class representing an error code entry
     */
    public static class ErrorCodeEntry {
        private final String codeName;
        private Integer errorCode;
        private String sampleErrorMessage;
        private final AtomicLong count;
        
        public ErrorCodeEntry(String codeName, Integer errorCode, String sampleErrorMessage) {
            this.codeName = codeName;
            this.errorCode = errorCode;
            this.sampleErrorMessage = sampleErrorMessage;
            this.count = new AtomicLong(1);
        }
        
        public void incrementCount() {
            count.incrementAndGet();
        }
        
        // Getters
        public String getCodeName() {
            return codeName;
        }
        
        public Integer getErrorCode() {
            return errorCode;
        }
        
        public void setErrorCode(Integer errorCode) {
            this.errorCode = errorCode;
        }
        
        public String getSampleErrorMessage() {
            return sampleErrorMessage;
        }
        
        public void setSampleErrorMessage(String sampleErrorMessage) {
            if (this.sampleErrorMessage == null) {
                this.sampleErrorMessage = sampleErrorMessage;
            }
        }
        
        public long getCount() {
            return count.get();
        }
        
        public String toCsvString() {
            return String.format("%s,%s,%d,%s",
                escapeCsv(codeName),
                errorCode != null ? errorCode.toString() : "",
                count.get(),
                escapeCsv(sampleErrorMessage));
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
}