package com.mongodb.log.parser.service;

import com.mongodb.log.parser.LogLineAccumulator;
import com.mongodb.log.parser.accumulator.Accumulator;
import com.mongodb.log.parser.model.MainOperationEntry;

import java.util.List;
import java.util.stream.Collectors;
import java.util.Comparator;

/**
 * Service class that converts Accumulator data to MainOperationEntry models.
 * This serves as the Controller in the MVC pattern for HTML report generation.
 */
public class MainOperationService {
    
    /**
     * Converts all accumulator entries to MainOperationEntry models,
     * sorted by count in descending order.
     */
    public static List<MainOperationEntry> getMainOperationEntries(Accumulator accumulator) {
        return accumulator.getAccumulators().values().stream()
                .map(MainOperationEntry::new)
                .sorted(Comparator.comparingLong(MainOperationEntry::getCount).reversed())
                .collect(Collectors.toList());
    }
    
    /**
     * Filters main operation entries by operation type.
     */
    public static List<MainOperationEntry> filterByOperation(List<MainOperationEntry> entries, String operation) {
        return entries.stream()
                .filter(entry -> operation.equals(entry.getOperation()))
                .collect(Collectors.toList());
    }
    
    /**
     * Filters main operation entries by namespace.
     */
    public static List<MainOperationEntry> filterByNamespace(List<MainOperationEntry> entries, String namespace) {
        return entries.stream()
                .filter(entry -> namespace.equals(entry.getNamespace()))
                .collect(Collectors.toList());
    }
    
    /**
     * Gets entries that have collection scans.
     */
    public static List<MainOperationEntry> getCollectionScans(List<MainOperationEntry> entries) {
        return entries.stream()
                .filter(MainOperationEntry::isHasCollScan)
                .collect(Collectors.toList());
    }
    
    /**
     * Gets entries for write operations (update_w, insert, remove).
     */
    public static List<MainOperationEntry> getWriteOperations(List<MainOperationEntry> entries) {
        return entries.stream()
                .filter(entry -> entry.getOperation().endsWith("_w") || 
                               "insert".equals(entry.getOperation()) || 
                               "remove".equals(entry.getOperation()))
                .collect(Collectors.toList());
    }
    
    /**
     * Gets entries for read operations (find, aggregate, count, etc.).
     */
    public static List<MainOperationEntry> getReadOperations(List<MainOperationEntry> entries) {
        return entries.stream()
                .filter(entry -> !entry.getOperation().endsWith("_w") && 
                               !"insert".equals(entry.getOperation()) && 
                               !"remove".equals(entry.getOperation()))
                .collect(Collectors.toList());
    }
    
    /**
     * Gets summary statistics for debugging purposes.
     */
    public static String getSummaryStats(List<MainOperationEntry> entries) {
        long totalEntries = entries.size();
        long writeEntries = getWriteOperations(entries).size();
        long readEntries = getReadOperations(entries).size();
        long collScanEntries = getCollectionScans(entries).size();
        
        return String.format("Total: %d, Reads: %d, Writes: %d, CollScans: %d", 
                totalEntries, readEntries, writeEntries, collScanEntries);
    }
}