package com.mongodb.log.parser;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

/**
 * Accumulator for tracking MongoDB queries by queryHash with sanitized query examples
 */
public class QueryHashAccumulator {
    
    private Map<QueryHashKey, QueryHashAccumulatorEntry> queryHashEntries = new HashMap<>();
    
    private String[] headers = new String[] { 
    	    "QueryHash", "Namespace", "Operation", "Count", "MinMs", "MaxMs", 
    	    "AvgMs", "P95Ms", "TotalSec", "AvgKeysEx", "AvgDocsEx", "KeysP95", 
    	    "DocsP95", "TotalKeysK", "TotalDocsK", "AvgReturn", "ExRetRatio",
    	    "ReadPreference", "SanitizedQuery"
    	};

    public void accumulate(SlowQuery slowQuery) {
        if (slowQuery.queryHash == null) {
            return;
        }
        
        QueryHashKey key = new QueryHashKey(
            slowQuery.queryHash,
            slowQuery.ns,
            slowQuery.opType != null ? slowQuery.opType.getType() : "unknown"
        );
        
        QueryHashAccumulatorEntry entry = queryHashEntries.get(key);
        if (entry == null) {
            entry = new QueryHashAccumulatorEntry(key);
            queryHashEntries.put(key, entry);
        }
        
        entry.addExecution(slowQuery);
    }
    
    public void report() {
        System.out.println("\n=== Query Hash Analysis ===");
        
        // Calculate actual column widths needed
        ColumnWidths widths = calculateColumnWidths();
        
        // Create dynamic format strings
        String headerFormat = String.format("%%-%ds %%-%ds %%-%ds %%8s %%8s %%8s %%8s %%8s %%10s %%10s %%10s %%8s %%8s %%8s %%8s %%8s %%8s %%-%ds %%-%ds", 
                widths.queryHashWidth, widths.namespaceWidth, widths.operationWidth,
                widths.readPreferenceWidth, widths.sanitizedQueryWidth);
        
        // Print header
        System.out.println(String.format(headerFormat,
                "QueryHash", "Namespace", "Operation", "Count", "MinMs", "MaxMs", "AvgMs", "P95Ms", 
                "TotalSec", "AvgKeysEx", "AvgDocsEx", "KeysP95", "DocsP95", "TotalKeysK", "TotalDocsK", 
                "AvgReturn", "ExRetRatio", "ReadPreference", "SanitizedQuery"));
        
        // Calculate separator length dynamically
        int separatorLength = widths.queryHashWidth + widths.namespaceWidth + widths.operationWidth + 
                             (8 * 15) + widths.readPreferenceWidth + widths.sanitizedQueryWidth + 18; // spaces
        System.out.println("=".repeat(separatorLength));
        
        queryHashEntries.values().stream()
            .sorted(Comparator.comparingLong(QueryHashAccumulatorEntry::getCount).reversed())
            .forEach(entry -> {
                String truncatedQueryHash = truncateToWidth(entry.getKey().getQueryHash(), widths.queryHashWidth);
                String truncatedNamespace = truncateToWidth(entry.getKey().getNamespace().toString(), widths.namespaceWidth);
                String truncatedOperation = truncateToWidth(entry.getKey().getOperation(), widths.operationWidth);
                String truncatedReadPref = truncateToWidth(entry.getReadPreferenceSummary(), widths.readPreferenceWidth);
                String truncatedQuery = truncateToWidth(entry.getSanitizedQuery(), widths.sanitizedQueryWidth);
                
                String dataFormat = String.format("%%-%ds %%-%ds %%-%ds %%8d %%8d %%8d %%8d %%8.0f %%10d %%10d %%10d %%8.0f %%8.0f %%8.1f %%8.1f %%8d %%8d %%-%ds %%-%ds", 
                        widths.queryHashWidth, widths.namespaceWidth, widths.operationWidth,
                        widths.readPreferenceWidth, widths.sanitizedQueryWidth);
                
                System.out.println(String.format(dataFormat,
                        truncatedQueryHash,
                        truncatedNamespace,
                        truncatedOperation,
                        entry.getCount(),
                        entry.getMin(),
                        entry.getMax(),
                        entry.getAvg(),
                        entry.getPercentile95(),
                        entry.getCount() * entry.getAvg() / 1000,
                        entry.getAvgKeysExamined(),
                        entry.getAvgDocsExamined(),
                        entry.getKeysExaminedPercentile95(),
                        entry.getDocsExaminedPercentile95(),
                        entry.getTotalKeysExamined() / 1000.0,
                        entry.getTotalDocsExamined() / 1000.0,
                        entry.getAvgReturned(),
                        entry.getScannedReturnRatio(),
                        truncatedReadPref,
                        truncatedQuery));
            });
        
        // Summary statistics
        long totalQueries = queryHashEntries.values().stream()
            .mapToLong(QueryHashAccumulatorEntry::getCount)
            .sum();
            
        long uniqueQueryHashes = queryHashEntries.size();
        long uniqueNamespaces = queryHashEntries.keySet().stream()
            .map(key -> key.getNamespace())
            .distinct()
            .count();
            
        System.out.println("\n=== Query Hash Summary ===");
        System.out.println(String.format("Total queries analyzed: %,d", totalQueries));
        System.out.println(String.format("Unique query hashes: %,d", uniqueQueryHashes));
        System.out.println(String.format("Unique namespaces: %,d", uniqueNamespaces));
    }
    
    private ColumnWidths calculateColumnWidths() {
        int maxQueryHashWidth = "QueryHash".length();
        int maxNamespaceWidth = "Namespace".length();
        int maxOperationWidth = "Operation".length();
        int maxReadPreferenceWidth = "ReadPreference".length();
        int maxSanitizedQueryWidth = "SanitizedQuery".length();
        
        for (QueryHashAccumulatorEntry entry : queryHashEntries.values()) {
            QueryHashKey key = entry.getKey();
            
            if (key.getQueryHash() != null) {
                maxQueryHashWidth = Math.max(maxQueryHashWidth, 
                    Math.min(key.getQueryHash().length(), 12));
            }
            
            if (key.getNamespace() != null) {
                maxNamespaceWidth = Math.max(maxNamespaceWidth, 
                    Math.min(key.getNamespace().toString().length(), 45));
            }
            
            if (key.getOperation() != null) {
                maxOperationWidth = Math.max(maxOperationWidth, 
                    Math.min(key.getOperation().length(), 15));
            }
            
            String readPrefSummary = entry.getReadPreferenceSummary();
            if (readPrefSummary != null) {
                maxReadPreferenceWidth = Math.max(maxReadPreferenceWidth, 
                    Math.min(readPrefSummary.length(), 30));
            }
            
            String sanitizedQuery = entry.getSanitizedQuery();
            if (sanitizedQuery != null) {
                maxSanitizedQueryWidth = Math.max(maxSanitizedQueryWidth, 
                    Math.min(sanitizedQuery.length(), 100));
            }
        }
        
        return new ColumnWidths(maxQueryHashWidth, maxNamespaceWidth, maxOperationWidth, 
                               maxReadPreferenceWidth, maxSanitizedQueryWidth);
    }
    
    public void reportCsv(String fileName) throws FileNotFoundException {
        PrintWriter writer = new PrintWriter(fileName);
        writer.println(String.join(",", headers));
        
        queryHashEntries.values().stream()
            .sorted(Comparator.comparingLong(QueryHashAccumulatorEntry::getCount).reversed())
            .forEach(entry -> writer.println(entry.toCsvString()));
        
        writer.close();
    }
    
    public Map<QueryHashKey, QueryHashAccumulatorEntry> getQueryHashEntries() {
        return queryHashEntries;
    }
    
    /**
     * Truncate string to fit width, adding ellipsis if needed
     */
    private String truncateToWidth(String str, int width) {
        if (str == null) {
            return "null";
        }
        if (str.length() <= width) {
            return str;
        }
        if (width <= 3) {
            return str.substring(0, width);
        }
        return str.substring(0, width - 3) + "...";
    }
    
    /**
     * Helper class to store calculated column widths
     */
    private static class ColumnWidths {
        final int queryHashWidth;
        final int namespaceWidth;
        final int operationWidth;
        final int readPreferenceWidth;
        final int sanitizedQueryWidth;
        
        ColumnWidths(int queryHashWidth, int namespaceWidth, int operationWidth, 
                    int readPreferenceWidth, int sanitizedQueryWidth) {
            this.queryHashWidth = queryHashWidth;
            this.namespaceWidth = namespaceWidth;
            this.operationWidth = operationWidth;
            this.readPreferenceWidth = readPreferenceWidth;
            this.sanitizedQueryWidth = sanitizedQueryWidth;
        }
    }
}