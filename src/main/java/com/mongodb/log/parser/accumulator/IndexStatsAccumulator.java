package com.mongodb.log.parser.accumulator;

import com.mongodb.log.parser.Namespace;
import com.mongodb.log.parser.SlowQuery;
import java.util.HashMap;
import java.util.Map;

/**
 * Accumulator for tracking index usage statistics by namespace and plan summary
 */
public class IndexStatsAccumulator {
    
    private Map<String, IndexStatsEntry> indexStatsEntries = new HashMap<>();
    
    public void accumulate(SlowQuery slowQuery) {
        if (slowQuery == null || slowQuery.ns == null || slowQuery.planSummary == null) {
            return;
        }
        
        // Create a key combining namespace and plan summary
        String key = createKey(slowQuery.ns, slowQuery.planSummary);
        
        IndexStatsEntry entry = indexStatsEntries.get(key);
        if (entry == null) {
            entry = new IndexStatsEntry(slowQuery.ns, slowQuery.planSummary);
            indexStatsEntries.put(key, entry);
        }
        
        entry.addOperation(
            slowQuery.durationMillis,
            slowQuery.keysExamined,
            slowQuery.docsExamined,
            slowQuery.nreturned
        );
    }
    
    private String createKey(Namespace namespace, String planSummary) {
        return namespace.toString() + "|" + (planSummary != null ? planSummary : "UNKNOWN");
    }
    
    public Map<String, IndexStatsEntry> getIndexStatsEntries() {
        return indexStatsEntries;
    }
    
    public boolean hasIndexStats() {
        return !indexStatsEntries.isEmpty();
    }
    
    public long getTotalOperations() {
        return indexStatsEntries.values().stream()
                .mapToLong(IndexStatsEntry::getCount)
                .sum();
    }
    
    public long getUniqueIndexUsagePatterns() {
        return indexStatsEntries.size();
    }
    
    public long getCollectionScanOperations() {
        return indexStatsEntries.values().stream()
                .filter(IndexStatsEntry::isCollectionScan)
                .mapToLong(IndexStatsEntry::getCount)
                .sum();
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Index Stats Summary:\n");
        sb.append(String.format("Total Operations: %d\n", getTotalOperations()));
        sb.append(String.format("Unique Index Usage Patterns: %d\n", getUniqueIndexUsagePatterns()));
        sb.append(String.format("Collection Scan Operations: %d\n", getCollectionScanOperations()));
        return sb.toString();
    }
}