package com.mongodb.log.parser;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.mongodb.log.parser.accumulator.QueryHashAccumulator;
import com.mongodb.log.parser.accumulator.QueryHashAccumulatorEntry;

/**
 * Flags query shapes worth fixing: collection scans and queries that examine far
 * more than they return. Built from the query hash accumulator, which excludes
 * mirrored reads, so every finding is application traffic.
 */
public class QueryFindings {

    /** Examined-to-returned ratio at or above which a query shape is flagged. */
    public static final long HIGH_RATIO = 100;
    /** Ignore shapes that examine fewer than this many documents/keys per execution on average. */
    public static final long MIN_AVG_EXAMINED = 1000;
    public static final int MAX_FINDINGS = 50;
    /** Collection scans examining fewer docs than this per execution are counted, not flagged. */
    public static final long MIN_COLLSCAN_DOCS = 100;

    public static class Finding {
        public final String type;
        public final QueryHashAccumulatorEntry entry;
        public final String reason;

        Finding(String type, QueryHashAccumulatorEntry entry, String reason) {
            this.type = type;
            this.entry = entry;
            this.reason = reason;
        }
    }

    private final List<Finding> collectionScans = new ArrayList<>();
    private final List<Finding> highScanRatio = new ArrayList<>();
    private int smallCollectionScans = 0;
    private int oplogScans = 0;

    public QueryFindings(QueryHashAccumulator queryHashAccumulator) {
        for (QueryHashAccumulatorEntry entry : queryHashAccumulator.getQueryHashEntries().values()) {
            String plan = entry.getPlanSummary();
            boolean collscan = plan != null && plan.contains("COLLSCAN");
            long count = entry.getCount();
            long avgDocs = entry.getTotalDocsExamined() / count;
            long avgKeys = entry.getTotalKeysExamined() / count;
            long returned = entry.getTotalReturned();

            if (collscan && "local.oplog.rs".equals(entry.getKey().getNamespace().toString())) {
                // Oplog tailing (replication, change streams) always scans the oplog
                oplogScans++;
                continue;
            }
            if (collscan && avgDocs < MIN_COLLSCAN_DOCS) {
                smallCollectionScans++;
                continue;
            }
            if (collscan) {
                collectionScans.add(new Finding("COLLSCAN", entry,
                        String.format("collection scan, %,d docs examined per execution", avgDocs)));
                continue;
            }

            long examined = Math.max(entry.getTotalDocsExamined(), entry.getTotalKeysExamined());
            if (Math.max(avgDocs, avgKeys) < MIN_AVG_EXAMINED) {
                continue;
            }
            String what = entry.getTotalKeysExamined() >= entry.getTotalDocsExamined() ? "keys" : "docs";
            if (returned == 0) {
                highScanRatio.add(new Finding("HIGH_SCAN_RATIO", entry,
                        String.format("%,d %s examined per execution, nothing returned", Math.max(avgDocs, avgKeys), what)));
            } else if (examined / returned >= HIGH_RATIO) {
                highScanRatio.add(new Finding("HIGH_SCAN_RATIO", entry,
                        String.format("%,d %s examined per document returned", examined / returned, what)));
            }
        }
        Comparator<Finding> byTime = Comparator.comparingLong((Finding f) -> f.entry.getTotalDurationMs()).reversed();
        collectionScans.sort(byTime);
        highScanRatio.sort(byTime);
    }

    public List<Finding> getCollectionScans() {
        return collectionScans.subList(0, Math.min(MAX_FINDINGS, collectionScans.size()));
    }

    public int getCollectionScanCount() {
        return collectionScans.size();
    }

    public List<Finding> getHighScanRatio() {
        return highScanRatio.subList(0, Math.min(MAX_FINDINGS, highScanRatio.size()));
    }

    public int getHighScanRatioCount() {
        return highScanRatio.size();
    }

    public int getSmallCollectionScans() {
        return smallCollectionScans;
    }

    public int getOplogScans() {
        return oplogScans;
    }

    public boolean hasFindings() {
        return !collectionScans.isEmpty() || !highScanRatio.isEmpty();
    }
}
