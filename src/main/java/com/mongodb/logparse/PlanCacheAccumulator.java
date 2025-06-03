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
        
        // Only show query hashes with multiple plan cache keys
        byQueryHash.entrySet().stream()
            .filter(entry -> entry.getValue().size() > 1)
            .sorted((e1, e2) -> {
                long count1 = e1.getValue().stream().mapToLong(PlanCacheAccumulatorEntry::getCount).sum();
                long count2 = e2.getValue().stream().mapToLong(PlanCacheAccumulatorEntry::getCount).sum();
                return Long.compare(count2, count1);
            })
            .forEach(entry -> {
                String queryHash = entry.getKey();
                java.util.List<PlanCacheAccumulatorEntry> plans = entry.getValue();
                
                System.out.println(String.format("\nQuery Hash: %s (%d different plans)", 
                    queryHash, plans.size()));
                
                plans.stream()
                    .sorted(Comparator.comparingLong(PlanCacheAccumulatorEntry::getCount).reversed())
                    .forEach(plan -> {
                        System.out.println(String.format("  %-20s %-30s %8d %8d %s", 
                            plan.getKey().getPlanCacheKey().substring(0, Math.min(20, plan.getKey().getPlanCacheKey().length())),
                            plan.getKey().getPlanSummary(),
                            plan.getCount(),
                            plan.getAvg(),
                            plan.getKey().getNamespace()));
                    });
            });
    }
}