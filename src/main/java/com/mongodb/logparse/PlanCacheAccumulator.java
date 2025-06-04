package com.mongodb.logparse;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

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
        "AvgReturn", "ExRetRatio", "CollScanCount", "CollScanPct"
    };

    public void accumulate(SlowQuery slowQuery) {
        if (slowQuery.planCacheKey == null) {
            return; // Skip entries without planCacheKey
        }
        
        PlanCacheKey key = new PlanCacheKey(
            slowQuery.ns, 
            slowQuery.planCacheKey, 
            slowQuery.queryHash,
            slowQuery.planSummary
        );
        
        PlanCacheAccumulatorEntry entry = planCacheEntries.get(key);
        if (entry == null) {
            entry = new PlanCacheAccumulatorEntry(key);
            planCacheEntries.put(key, entry);
        }
        
        entry.addExecution(slowQuery);
    }
    
    public void report() {
        System.out.println("\n=== Plan Cache Key Analysis ===");
        System.out.println(String.format("%-50s %-20s %-15s %-30s %8s %8s %8s %8s %8s %10s %10s %10s %8s %8s", 
                "Namespace", "PlanCacheKey", "QueryHash", "PlanSummary", "Count", 
                "MinMs", "MaxMs", "AvgMs", "P95Ms", "TotalSec", "AvgKeysEx", 
                "AvgDocsEx", "AvgRet", "CSPct%"));
        System.out.println("=".repeat(200));
        
        planCacheEntries.values().stream()
            .sorted(Comparator.comparingLong(PlanCacheAccumulatorEntry::getCount).reversed())
            .forEach(entry -> System.out.println(entry.toString()));
            
        // Summary statistics
        long totalQueries = planCacheEntries.values().stream()
            .mapToLong(PlanCacheAccumulatorEntry::getCount)
            .sum();
            
        long collScanQueries = planCacheEntries.values().stream()
            .filter(entry -> entry.isCollectionScan())
            .mapToLong(PlanCacheAccumulatorEntry::getCount)
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
            String headerFormat = String.format("  %%-%ds %%-%ds %%-%ds %%8s %%8s %%8s %%8s %%8s %%10s", 
                widths.planCacheKeyWidth, widths.planSummaryWidth, widths.namespaceWidth);
                
            System.out.println(String.format(headerFormat,
                "PlanCacheKey", "PlanSummary", "Namespace", "Count", "MinMs", "MaxMs", "AvgMs", "P95Ms", "TotalSec"));
            
            String separatorFormat = String.format("  %%-%ds %%-%ds %%-%ds %%8s %%8s %%8s %%8s %%8s %%10s", 
                widths.planCacheKeyWidth, widths.planSummaryWidth, widths.namespaceWidth);
                
            System.out.println(String.format(separatorFormat,
                "=".repeat(widths.planCacheKeyWidth), 
                "=".repeat(widths.planSummaryWidth),
                "=".repeat(widths.namespaceWidth),
                "========", "========", "========", "========", "========", "=========="));
            
            // Print data rows with consistent formatting
            String dataFormat = String.format("  %%-%ds %%-%ds %%-%ds %%8d %%8d %%8d %%8d %%8.0f %%10d", 
                widths.planCacheKeyWidth, widths.planSummaryWidth, widths.namespaceWidth);
                
            plans.stream()
                .sorted(Comparator.comparingLong(PlanCacheAccumulatorEntry::getCount).reversed())
                .forEach(plan -> {
                    String planCacheKey = truncateToWidth(plan.getKey().getPlanCacheKey(), widths.planCacheKeyWidth);
                    String planSummary = truncateToWidth(plan.getKey().getPlanSummary(), widths.planSummaryWidth);
                    String namespace = truncateToWidth(plan.getKey().getNamespace().toString(), widths.namespaceWidth);
                    
                    System.out.println(String.format(dataFormat,
                        planCacheKey,
                        planSummary,
                        namespace,
                        plan.getCount(),
                        plan.getMin(),
                        plan.getMax(),
                        plan.getAvg(),
                        plan.getPercentile95(),
                        plan.getCount() * plan.getAvg() / 1000));
                });
        });
    }
    
    /**
     * Calculate optimal column widths by scanning all data
     */
    private ColumnWidths calculateOptimalColumnWidths(java.util.List<Map.Entry<String, java.util.List<PlanCacheAccumulatorEntry>>> queries) {
        int maxPlanCacheKeyWidth = "PlanCacheKey".length();
        int maxPlanSummaryWidth = "PlanSummary".length();
        int maxNamespaceWidth = "Namespace".length();
        
        for (var queryEntry : queries) {
            for (var plan : queryEntry.getValue()) {
                // Track actual lengths, but set reasonable limits
                if (plan.getKey().getPlanCacheKey() != null) {
                    maxPlanCacheKeyWidth = Math.max(maxPlanCacheKeyWidth, 
                        Math.min(plan.getKey().getPlanCacheKey().length(), 20));
                }
                
                if (plan.getKey().getPlanSummary() != null) {
                    maxPlanSummaryWidth = Math.max(maxPlanSummaryWidth, 
                        Math.min(plan.getKey().getPlanSummary().length(), 40));
                }
                
                if (plan.getKey().getNamespace() != null) {
                    maxNamespaceWidth = Math.max(maxNamespaceWidth, 
                        Math.min(plan.getKey().getNamespace().toString().length(), 50));
                }
            }
        }
        
        return new ColumnWidths(maxPlanCacheKeyWidth, maxPlanSummaryWidth, maxNamespaceWidth);
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
        final int planCacheKeyWidth;
        final int planSummaryWidth;
        final int namespaceWidth;
        
        ColumnWidths(int planCacheKeyWidth, int planSummaryWidth, int namespaceWidth) {
            this.planCacheKeyWidth = planCacheKeyWidth;
            this.planSummaryWidth = planSummaryWidth;
            this.namespaceWidth = namespaceWidth;
        }
    }
}