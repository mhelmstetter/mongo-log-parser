package com.mongodb.log.parser.accumulator;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Accumulator for tracking MongoDB transaction statistics
 */
public class TransactionAccumulator {
    
    private Map<TransactionKey, TransactionEntry> transactionEntries = new HashMap<>();
    
    private String[] headers = new String[] { 
        "TxnRetryCounter", "TerminationCause", "CommitType", "Count", 
        "MinDurationMs", "MaxDurationMs", "AvgDurationMs",
        "MaxCommitMs", "AvgCommitMs", "MaxTimeActiveMs", "AvgTimeActiveMs",
        "MaxTimeInactiveMs", "AvgTimeInactiveMs"
    };

    /**
     * Accumulates a transaction entry from a log line
     */
    public synchronized void accumulate(Integer txnRetryCounter, String terminationCause, 
                                      String commitType, Long durationMillis, 
                                      Long commitDurationMicros, Long timeActiveMicros, 
                                      Long timeInactiveMicros) {
        
        TransactionKey key = new TransactionKey(txnRetryCounter, terminationCause, commitType);
        
        TransactionEntry entry = transactionEntries.get(key);
        if (entry == null) {
            entry = new TransactionEntry(key);
            transactionEntries.put(key, entry);
        }
        
        entry.addTransaction(durationMillis, commitDurationMicros, timeActiveMicros, timeInactiveMicros);
    }
    
    /**
     * Prints a formatted report of all transaction statistics
     */
    public void report() {
        if (transactionEntries.isEmpty()) {
            System.out.println("No transaction entries found in logs");
            return;
        }
        
        System.out.println("\n=== Transaction Analysis ===");
        
        // Calculate column widths
        final int maxRetryCounterWidth = Math.max("TxnRetryCounter".length(), 15);
        final int maxTerminationCauseWidth = Math.max("TerminationCause".length(), 20);
        final int maxCommitTypeWidth = Math.max("CommitType".length(), 15);
        
        // Print header
        String headerFormat = String.format("%%-%ds %%-%ds %%-%ds %%8s %%12s %%12s %%12s %%12s %%12s %%14s %%14s %%16s %%16s", 
            maxRetryCounterWidth, maxTerminationCauseWidth, maxCommitTypeWidth);
        
        System.out.println(String.format(headerFormat,
            "TxnRetryCounter", "TerminationCause", "CommitType", "Count",
            "MinDurMs", "MaxDurMs", "AvgDurMs", "MaxCommitMs", "AvgCommitMs",
            "MaxTimeActiveMs", "AvgTimeActiveMs", "MaxTimeInactiveMs", "AvgTimeInactiveMs"));
        
        // Print separator
        int separatorLength = maxRetryCounterWidth + maxTerminationCauseWidth + maxCommitTypeWidth + 
                             8 + (12 * 6) + (14 * 2) + (16 * 2) + 12; // +12 for spaces
        System.out.println("=".repeat(separatorLength));
        
        // Print entries sorted by count (descending)
        String dataFormat = String.format("%%-%ds %%-%ds %%-%ds %%8d %%12d %%12d %%12d %%12d %%12d %%14d %%14d %%16d %%16d", 
            maxRetryCounterWidth, maxTerminationCauseWidth, maxCommitTypeWidth);
        
        transactionEntries.values().stream()
            .sorted(Comparator.comparingLong(TransactionEntry::getCount).reversed())
            .forEach(entry -> {
                TransactionKey key = entry.getKey();
                
                System.out.println(String.format(dataFormat,
                    key.getTxnRetryCounter() != null ? key.getTxnRetryCounter().toString() : "null",
                    key.getTerminationCause() != null ? key.getTerminationCause() : "null",
                    key.getCommitType() != null ? key.getCommitType() : "null",
                    entry.getCount(),
                    entry.getMinDurationMs(),
                    entry.getMaxDurationMs(),
                    entry.getAvgDurationMs(),
                    entry.getMaxCommitMs(),
                    entry.getAvgCommitMs(),
                    entry.getMaxTimeActiveMs(),
                    entry.getAvgTimeActiveMs(),
                    entry.getMaxTimeInactiveMs(),
                    entry.getAvgTimeInactiveMs()));
            });
        
        // Print summary
        long totalTransactions = transactionEntries.values().stream()
            .mapToLong(TransactionEntry::getCount)
            .sum();
        long uniqueCombinations = transactionEntries.size();
        
        System.out.println("\n=== Transaction Summary ===");
        System.out.println(String.format("Total transactions: %,d", totalTransactions));
        System.out.println(String.format("Unique retry/cause/type combinations: %,d", uniqueCombinations));
        
        // Show breakdown by termination cause
        Map<String, AtomicLong> terminationCauseCounts = new HashMap<>();
        transactionEntries.values().forEach(entry -> {
            String cause = entry.getKey().getTerminationCause();
            if (cause == null) cause = "unknown";
            terminationCauseCounts.computeIfAbsent(cause, k -> new AtomicLong(0))
                .addAndGet(entry.getCount());
        });
        
        System.out.println("\n=== Termination Cause Breakdown ===");
        terminationCauseCounts.entrySet().stream()
            .sorted(Map.Entry.<String, AtomicLong>comparingByValue(
                (a, b) -> Long.compare(b.get(), a.get())))
            .forEach(entry -> {
                double percentage = (entry.getValue().get() * 100.0) / totalTransactions;
                System.out.println(String.format("  %s: %,d (%.1f%%)", 
                    entry.getKey(), entry.getValue().get(), percentage));
            });
    }
    
    /**
     * Exports transaction data to CSV format
     */
    public void reportCsv(String fileName) throws FileNotFoundException {
        PrintWriter writer = new PrintWriter(fileName);
        writer.println(String.join(",", headers));
        
        transactionEntries.values().stream()
            .sorted(Comparator.comparingLong(TransactionEntry::getCount).reversed())
            .forEach(entry -> writer.println(entry.toCsvString()));
        
        writer.close();
    }
    
    /**
     * Returns the map of transaction entries for external access
     */
    public Map<TransactionKey, TransactionEntry> getTransactionEntries() {
        return new HashMap<>(transactionEntries);
    }
    
    /**
     * Returns true if any transactions have been accumulated
     */
    public boolean hasTransactions() {
        return !transactionEntries.isEmpty();
    }
    
    /**
     * Returns the total number of transactions
     */
    public long getTotalTransactionCount() {
        return transactionEntries.values().stream()
            .mapToLong(TransactionEntry::getCount)
            .sum();
    }
}