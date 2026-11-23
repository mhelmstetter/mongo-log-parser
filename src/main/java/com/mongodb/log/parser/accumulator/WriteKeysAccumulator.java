package com.mongodb.log.parser.accumulator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.mongodb.log.parser.SlowQuery;

/**
 * Index maintenance per write, from slow write log lines: keysInserted / keysDeleted against the documents written
 * (ninserted + nModified + ndeleted + nUpserted), per namespace and operation. Keys per document written approximates
 * how many index entries each write touches (an update rewrites only the indexes whose keys changed).
 *
 * Only operations at or above slowms are logged, so this ranks the slow writes; it is not the node's total.
 */
public class WriteKeysAccumulator {

    public static class Entry {
        private final String namespace;
        private final String op;
        private long count;
        private long docsWritten;
        private long keysInserted;
        private long keysDeleted;
        private long opsWithKeyCounts;
        private long totalDurationMs;

        Entry(String namespace, String op) {
            this.namespace = namespace;
            this.op = op;
        }

        public String getNamespace() { return namespace; }
        public String getOp() { return op; }
        public long getCount() { return count; }
        public long getDocsWritten() { return docsWritten; }
        public long getKeysInserted() { return keysInserted; }
        public long getKeysDeleted() { return keysDeleted; }
        public long getOpsWithKeyCounts() { return opsWithKeyCounts; }
        public long getTotalDurationMs() { return totalDurationMs; }

        /** Index keys inserted + deleted per document written; null when no documents were written. */
        public Double getKeysPerDocWritten() {
            return docsWritten > 0 ? (keysInserted + keysDeleted) / (double) docsWritten : null;
        }
    }

    private final Map<String, Entry> entries = new HashMap<>();

    public synchronized void accumulate(SlowQuery q) {
        if (Boolean.TRUE.equals(q.mirrored) || q.ns == null || q.opType == null) {
            return;
        }
        boolean hasWriteCounts = q.nInserted != null || q.nModified != null || q.nDeleted != null || q.nUpserted != null;
        boolean hasKeys = q.keysInserted != null || q.keysDeleted != null;
        if (!hasWriteCounts && !hasKeys) {
            return;
        }
        String ns = q.ns.toString();
        String op = q.opType.getType();
        Entry e = entries.computeIfAbsent(ns + "\u0000" + op, k -> new Entry(ns, op));
        e.count++;
        e.docsWritten += nz(q.nInserted) + nz(q.nModified) + nz(q.nDeleted) + nz(q.nUpserted);
        e.keysInserted += nz(q.keysInserted);
        e.keysDeleted += nz(q.keysDeleted);
        if (hasKeys) {
            e.opsWithKeyCounts++;
        }
        e.totalDurationMs += nz(q.durationMillis);
    }

    private static long nz(Long v) {
        return v == null ? 0 : v;
    }

    public synchronized boolean hasData() {
        return !entries.isEmpty();
    }

    /** Entries ranked by index keys touched (inserted + deleted). */
    public synchronized List<Entry> getEntries() {
        List<Entry> list = new ArrayList<>(entries.values());
        list.sort(Comparator.comparingLong((Entry e) -> e.keysInserted + e.keysDeleted).reversed()
                .thenComparing(Entry::getNamespace).thenComparing(Entry::getOp));
        return list;
    }
}
