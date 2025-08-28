package com.mongodb.log.parser.accumulator;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import com.mongodb.log.parser.SlowQuery;

/**
 * Accumulator for tracking MongoDB query execution plans by planCacheKey
 * This helps analyze query plan selection and performance characteristics
 */
public class PlanCacheAccumulator {
    
    private Map<PlanCacheKey, PlanCacheAccumulatorEntry> planCacheEntries = new HashMap<>();
    
    private String[] headers = new String[] { 
    	    "Namespace", "PlanCacheKey", "QueryHash", "PlanSummary", "Count", 
    	    "MinMs", "MaxMs", "AvgMs", "P95Ms", "TotalSec", "AvgKeysEx", 
    	    "AvgDocsEx", "KeysP95", "DocsP95", "TotalKeysK", "TotalDocsK", 
    	    "AvgReturn", "ExRetRatio", "CollScanCount", "CollScanPct", 
    	    "MinPlanMs", "MaxPlanMs", "AvgPlanMs", "PlanP95Ms", "ReplannedCount", 
    	    "ReplannedPct", "MultiPlannerCount", "MultiPlannerPct", "TopReplanReason"
    	};

    public void accumulate(SlowQuery slowQuery) {
        accumulate(slowQuery, null);
    }
    
    public void accumulate(SlowQuery slowQuery, String logMessage) {
        if (slowQuery.planCacheKey == null || slowQuery.planSummary == null) {
            return;
        }
        
        PlanCacheKey key = new PlanCacheKey(
            slowQuery.ns, 
            slowQuery.opType,
            slowQuery.queryHash,
            slowQuery.planSummary
        );
        
        PlanCacheAccumulatorEntry entry = planCacheEntries.get(key);
        if (entry == null) {
            entry = new PlanCacheAccumulatorEntry(key);
            planCacheEntries.put(key, entry);
        }
        
        entry.addExecution(slowQuery, logMessage);
    }
    
    public void report() {
        System.out.println("\n=== Plan Cache Key Analysis ===");
        
        // Calculate actual column widths needed
        ColumnWidths widths = calculateColumnWidths();
        
        // Create dynamic format strings
        String headerFormat = String.format("%%-%ds %%-%ds %%-%ds %%8s %%8s %%8s %%8s %%8s %%10s %%10s %%10s %%8s %%8s %%8s %%8s %%8s", 
                widths.namespaceWidth, widths.queryHashWidth, widths.planSummaryWidth);
        
        // Print header
        System.out.println(String.format(headerFormat,
                "Namespace", "QueryHash", "PlanSummary", "Count", 
                "MinMs", "MaxMs", "AvgMs", "P95Ms", "TotalSec", "AvgKeysEx", 
                "AvgDocsEx", "AvgRet", "MinPlanMs", "MaxPlanMs", "AvgPlanMs", "ReplanPct"));
        
        // Calculate separator length dynamically
        int separatorLength = widths.namespaceWidth + widths.queryHashWidth + 
                             widths.planSummaryWidth + (8 * 13) + 14; // 8 chars * 13 columns + spaces
        System.out.println("=".repeat(separatorLength));
        
        planCacheEntries.values().stream()
            .sorted(Comparator.comparingLong(PlanCacheAccumulatorEntry::getCount).reversed())
            .forEach(entry -> {
                String truncatedNamespace = truncateToWidth(entry.getKey().getNamespace().toString(), widths.namespaceWidth);
                String truncatedQueryHash = truncateToWidth(entry.getKey().getQueryHash(), widths.queryHashWidth);
                String truncatedPlanSummary = truncateToWidth(entry.getKey().getPlanSummary(), widths.planSummaryWidth);
                
                String dataFormat = String.format("%%-%ds %%-%ds %%-%ds %%8d %%8d %%8d %%8d %%8.0f %%10d %%10d %%10d %%8d %%8d %%8d %%8d %%8.1f", 
                        widths.namespaceWidth, widths.queryHashWidth, widths.planSummaryWidth);
                
                System.out.println(String.format(dataFormat,
                        truncatedNamespace,
                        truncatedQueryHash,
                        truncatedPlanSummary,
                        entry.getCount(),
                        entry.getMin(),
                        entry.getMax(),
                        entry.getAvg(),
                        entry.getPercentile95(),
                        entry.getCount() * entry.getAvg() / 1000,
                        entry.getAvgKeysExamined(),
                        entry.getAvgDocsExamined(),
                        entry.getAvgReturned(),
                        entry.getMinPlanningTimeMs(),
                        entry.getMaxPlanningTimeMs(),
                        entry.getAvgPlanningTimeMs(),
                        entry.getReplannedPercentage()));
            });
        
        // Summary statistics including replanning
        long totalQueries = planCacheEntries.values().stream()
            .mapToLong(PlanCacheAccumulatorEntry::getCount)
            .sum();
            
        long collScanQueries = planCacheEntries.values().stream()
            .filter(entry -> entry.isCollectionScan())
            .mapToLong(PlanCacheAccumulatorEntry::getCount)
            .sum();
        
        long totalReplanned = planCacheEntries.values().stream()
            .mapToLong(PlanCacheAccumulatorEntry::getReplannedCount)
            .sum();
        
        long totalMultiPlanner = planCacheEntries.values().stream()
            .mapToLong(PlanCacheAccumulatorEntry::getMultiPlannerCount)
            .sum();
            
        long uniquePlanCacheKeys = planCacheEntries.size();
        long uniqueQueryHashes = planCacheEntries.keySet().stream()
            .map(key -> key.getQueryHash())
            .distinct()
            .count();
            
        System.out.println("\n=== Plan Cache Summary ===");
        System.out.println(String.format("Total queries analyzed: %,d", totalQueries));
        System.out.println(String.format("Unique plan cache keys: %,d", uniquePlanCacheKeys));
        System.out.println(String.format("Unique query hashes: %,d", uniqueQueryHashes));
        System.out.println(String.format("Collection scan queries: %,d (%.1f%%)", 
            collScanQueries, (collScanQueries * 100.0) / Math.max(totalQueries, 1)));
        System.out.println(String.format("Replanned queries: %,d (%.1f%%)", 
            totalReplanned, (totalReplanned * 100.0) / Math.max(totalQueries, 1)));
        System.out.println(String.format("Multi-planner queries: %,d (%.1f%%)", 
            totalMultiPlanner, (totalMultiPlanner * 100.0) / Math.max(totalQueries, 1)));
        
        // Report most common replan reasons
        Map<String, Long> allReplanReasons = new HashMap<>();
        planCacheEntries.values().forEach(entry -> {
            entry.getReplanReasons().forEach((reason, count) -> 
                allReplanReasons.merge(reason, count, Long::sum));
        });
        
        if (!allReplanReasons.isEmpty()) {
            System.out.println("\n=== Top Replan Reasons ===");
            allReplanReasons.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .limit(5)
                .forEach(entry -> 
                    System.out.println(String.format("  %s: %,d (%.1f%%)", 
                        entry.getKey(), entry.getValue(), 
                        (entry.getValue() * 100.0) / Math.max(totalReplanned, 1))));
        }
    }
    
    private ColumnWidths calculateColumnWidths() {
        int maxNamespaceWidth = "Namespace".length();
        int maxQueryHashWidth = "QueryHash".length();
        int maxPlanSummaryWidth = "PlanSummary".length();
        
        for (PlanCacheAccumulatorEntry entry : planCacheEntries.values()) {
            PlanCacheKey key = entry.getKey();
            
            if (key.getNamespace() != null) {
                maxNamespaceWidth = Math.max(maxNamespaceWidth, 
                    Math.min(key.getNamespace().toString().length(), 45));
            }
            
            
            if (key.getQueryHash() != null) {
                maxQueryHashWidth = Math.max(maxQueryHashWidth, 
                    Math.min(key.getQueryHash().length(), 12));
            }
            
            if (key.getPlanSummary() != null) {
                maxPlanSummaryWidth = Math.max(maxPlanSummaryWidth, 
                    Math.min(key.getPlanSummary().length(), 80));
            }
        }
        
        return new ColumnWidths(maxNamespaceWidth, maxQueryHashWidth, maxPlanSummaryWidth);
    }
    
    public void reportCsv(String fileName) throws FileNotFoundException {
        PrintWriter writer = new PrintWriter(fileName);
        writer.println(String.join(",", headers));
        
        planCacheEntries.values().stream()
            .sorted(Comparator.comparingLong(PlanCacheAccumulatorEntry::getCount).reversed())
            .forEach(entry -> writer.println(entry.toCsvString()));
        
        writer.close();
    }
    
    public Map<PlanCacheKey, PlanCacheAccumulatorEntry> getPlanCacheEntries() {
        return planCacheEntries;
    }
    
    public void reportByQueryHash() {
        System.out.println("\n=== Query Hash Analysis (Multiple Plans) ===");
        
        // Group by query hash and show different plan cache keys
        Map<String, java.util.List<PlanCacheAccumulatorEntry>> byQueryHash = new HashMap<>();
        
        planCacheEntries.values().forEach(entry -> {
            String queryHash = entry.getKey().getQueryHash();
            if (queryHash != null) {
                byQueryHash.computeIfAbsent(queryHash, k -> new java.util.ArrayList<>()).add(entry);
            }
        });
        
        // Show all query hashes, sorted by total execution count
        var allQueryHashes = byQueryHash.entrySet().stream()
            .sorted((e1, e2) -> {
                long count1 = e1.getValue().stream().mapToLong(PlanCacheAccumulatorEntry::getCount).sum();
                long count2 = e2.getValue().stream().mapToLong(PlanCacheAccumulatorEntry::getCount).sum();
                return Long.compare(count2, count1);
            })
            .toList();
            
        if (allQueryHashes.isEmpty()) {
            System.out.println("No query hashes found.");
            return;
        }
        
        // Calculate dynamic column widths by scanning all data first
        ColumnWidths widths = calculateOptimalColumnWidths(allQueryHashes);
        
        // Print the report with consistent formatting
        allQueryHashes.forEach(entry -> {
            String queryHash = entry.getKey();
            java.util.List<PlanCacheAccumulatorEntry> plans = entry.getValue();
            
            // Print query hash header
            System.out.println(String.format("\nQuery Hash: %s (%d different plans)", 
                queryHash, plans.size()));
            
            // Print column headers
            String headerFormat = String.format("  %%-%ds %%-%ds %%8s %%8s %%8s %%8s %%8s %%10s %%8s %%8s %%8s", 
                widths.planSummaryWidth, widths.namespaceWidth);
                
            System.out.println(String.format(headerFormat,
                "PlanSummary", "Namespace", "Count", "MinMs", "MaxMs", "AvgMs", "P95Ms", "TotalSec", "AvgPlanMs", "PlanP95", "ReplanPct"));
            
            String separatorFormat = String.format("  %%-%ds %%-%ds %%8s %%8s %%8s %%8s %%8s %%10s %%8s %%8s %%8s", 
                widths.planSummaryWidth, widths.namespaceWidth);
                
            System.out.println(String.format(separatorFormat,
                "=".repeat(widths.planSummaryWidth),
                "=".repeat(widths.namespaceWidth),
                "========", "========", "========", "========", "========", "==========", "========", "========", "========"));
            
            // Print data rows with consistent formatting
            String dataFormat = String.format("  %%-%ds %%-%ds %%8d %%8d %%8d %%8d %%8.0f %%10d %%8d %%8.0f %%8.1f", 
                widths.planSummaryWidth, widths.namespaceWidth);
                
            plans.stream()
                .sorted(Comparator.comparingLong(PlanCacheAccumulatorEntry::getCount).reversed())
                .forEach(plan -> {
                    String planSummary = truncateToWidth(plan.getKey().getPlanSummary(), widths.planSummaryWidth);
                    String namespace = truncateToWidth(plan.getKey().getNamespace().toString(), widths.namespaceWidth);
                    
                    System.out.println(String.format(dataFormat,
                        planSummary,
                        namespace,
                        plan.getCount(),
                        plan.getMin(),
                        plan.getMax(),
                        plan.getAvg(),
                        plan.getPercentile95(),
                        plan.getCount() * plan.getAvg() / 1000,
                        plan.getAvgPlanningTimeMs(),
                        plan.getPlanningTimePercentile95Ms(),
                        plan.getReplannedPercentage()));
                });
        });
    }
    
    /**
     * Calculate optimal column widths by scanning all data
     */
    private ColumnWidths calculateOptimalColumnWidths(java.util.List<Map.Entry<String, java.util.List<PlanCacheAccumulatorEntry>>> queries) {
        int maxPlanSummaryWidth = "PlanSummary".length();
        int maxNamespaceWidth = "Namespace".length();
        
        for (var queryEntry : queries) {
            for (var plan : queryEntry.getValue()) {
                // Track actual lengths, but set reasonable limits
                
                if (plan.getKey().getPlanSummary() != null) {
                    maxPlanSummaryWidth = Math.max(maxPlanSummaryWidth, 
                        Math.min(plan.getKey().getPlanSummary().length(), 80));
                }
                
                if (plan.getKey().getNamespace() != null) {
                    maxNamespaceWidth = Math.max(maxNamespaceWidth, 
                        Math.min(plan.getKey().getNamespace().toString().length(), 45));
                }
            }
        }
        
        return new ColumnWidths(maxPlanSummaryWidth, maxNamespaceWidth);
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
        final int namespaceWidth;
        final int queryHashWidth;
        final int planSummaryWidth;
        
        // Constructor for main report (3 parameters)
        ColumnWidths(int namespaceWidth, int queryHashWidth, int planSummaryWidth) {
            this.namespaceWidth = namespaceWidth;
            this.queryHashWidth = queryHashWidth;
            this.planSummaryWidth = planSummaryWidth;
        }
        
        // Constructor for query hash report (2 parameters) - reusing existing fields
        ColumnWidths(int planSummaryWidth, int namespaceWidth) {
            this.planSummaryWidth = planSummaryWidth;
            this.namespaceWidth = namespaceWidth;
            this.queryHashWidth = 10; // Default for query hash display
        }
    }
    
    public int getSize() {
        return planCacheEntries.size();
    }
}