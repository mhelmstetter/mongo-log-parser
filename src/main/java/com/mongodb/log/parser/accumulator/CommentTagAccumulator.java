package com.mongodb.log.parser.accumulator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.mongodb.log.parser.CommentTag;
import com.mongodb.log.parser.SlowQuery;

/**
 * Slow operations grouped by the application tag in their comment: by origin_uri, by code_fqn and by
 * the pair. Also holds the log-wide CPU coverage counters (every line carrying attr.cpuNanos, whether
 * or not it became an analyzed operation) and any host core count found in the log.
 */
public class CommentTagAccumulator {

    public static final String PAIR_SEPARATOR = " | ";

    public static class Group {
        private final String name;
        private long count;
        private long durationCount;
        private long totalDurationMs;
        private long maxDurationMs;
        private long cpuCount;
        private long totalCpuNanos;
        private long maxCpuNanos;
        private long cpuWithDurationCpuNanos;
        private long cpuWithDurationMs;

        Group(String name) {
            this.name = name;
        }

        void add(SlowQuery sq) {
            count++;
            if (sq.durationMillis != null) {
                durationCount++;
                totalDurationMs += sq.durationMillis;
                maxDurationMs = Math.max(maxDurationMs, sq.durationMillis);
            }
            if (sq.cpuNanos != null) {
                cpuCount++;
                totalCpuNanos += sq.cpuNanos;
                maxCpuNanos = Math.max(maxCpuNanos, sq.cpuNanos);
                if (sq.durationMillis != null) {
                    cpuWithDurationCpuNanos += sq.cpuNanos;
                    cpuWithDurationMs += sq.durationMillis;
                }
            }
        }

        public String getName() { return name; }
        public long getCount() { return count; }
        public long getDurationCount() { return durationCount; }
        public long getTotalDurationMs() { return totalDurationMs; }
        /** Null when no entry logged durationMillis. */
        public Double getAvgDurationMs() { return durationCount > 0 ? (double) totalDurationMs / durationCount : null; }
        public Long getMaxDurationMs() { return durationCount > 0 ? maxDurationMs : null; }
        public long getCpuCount() { return cpuCount; }
        public long getTotalCpuNanos() { return totalCpuNanos; }
        /** Null when no entry logged cpuNanos. */
        public Double getAvgCpuNanos() { return cpuCount > 0 ? (double) totalCpuNanos / cpuCount : null; }
        public Long getMaxCpuNanos() { return cpuCount > 0 ? maxCpuNanos : null; }
        /** CPU as % of duration over entries with both values; null when their duration sums to 0. */
        public Double getCpuDurationPercent() {
            return QueryHashAccumulatorEntry.cpuDurationPercent(cpuWithDurationCpuNanos, cpuWithDurationMs);
        }
    }

    private final Map<String, Group> byOrigin = new HashMap<>();
    private final Map<String, Group> byCodeFqn = new HashMap<>();
    private final Map<String, Group> byOriginAndCodeFqn = new HashMap<>();

    // How operations were tagged: status/source -> [count, cpuNanos]
    private final Map<String, long[]> tagSources = new TreeMap<>();

    // Analyzed operations' CPU
    private long analyzedCpuCount;
    private long analyzedCpuNanos;

    // Operations dropped by the namespace filter (always includes the config database)
    private long namespaceFilteredCpuCount;
    private long namespaceFilteredCpuNanos;

    // Every input line carrying "cpuNanos" (within the time window), parsed on the reader thread
    private long loggedCpuLines;
    private long loggedCpuNanos;
    private long unparsedCpuLines;

    // Host core counts found in the log: value -> sample message
    private final Map<Long, String> numCores = new TreeMap<>();

    public synchronized void accumulate(SlowQuery sq) {
        CommentTag tag = sq.commentTag;
        if (tag == null) {
            throw new IllegalStateException("comment tag not extracted for operation on " + sq.ns);
        }
        add(byOrigin, tag.originUri, sq);
        add(byCodeFqn, tag.codeFqn, sq);
        add(byOriginAndCodeFqn, tag.originUri + PAIR_SEPARATOR + tag.codeFqn, sq);
        String sourceKey = tag.status == CommentTag.Status.UNTAGGED ? "untagged"
                : tag.status.name().toLowerCase() + " via " + tag.source;
        long[] s = tagSources.computeIfAbsent(sourceKey, k -> new long[2]);
        s[0]++;
        if (sq.cpuNanos != null) {
            s[1] += sq.cpuNanos;
            analyzedCpuCount++;
            analyzedCpuNanos += sq.cpuNanos;
        }
    }

    private static void add(Map<String, Group> map, String key, SlowQuery sq) {
        map.computeIfAbsent(key, Group::new).add(sq);
    }

    public synchronized void recordNamespaceFiltered(Long cpuNanos) {
        if (cpuNanos != null) {
            namespaceFilteredCpuCount++;
            namespaceFilteredCpuNanos += cpuNanos;
        }
    }

    public synchronized void recordLoggedCpu(long cpuNanos) {
        loggedCpuLines++;
        loggedCpuNanos += cpuNanos;
    }

    public synchronized void recordUnparsedCpuLine() {
        unparsedCpuLines++;
    }

    public synchronized void recordNumCores(long cores, String sample) {
        numCores.putIfAbsent(cores, sample);
    }

    private static List<Group> sorted(Map<String, Group> map) {
        List<Group> list = new ArrayList<>(map.values());
        list.sort(Comparator.comparingLong(Group::getTotalCpuNanos).reversed()
                .thenComparing(Comparator.comparingLong(Group::getTotalDurationMs).reversed())
                .thenComparing(Group::getName));
        return list;
    }

    public List<Group> getByOrigin() { return sorted(byOrigin); }
    public List<Group> getByCodeFqn() { return sorted(byCodeFqn); }
    public List<Group> getByOriginAndCodeFqn() { return sorted(byOriginAndCodeFqn); }
    public Map<String, long[]> getTagSources() { return tagSources; }
    public boolean hasData() { return !byOrigin.isEmpty() || loggedCpuLines > 0; }
    public long getAnalyzedCpuCount() { return analyzedCpuCount; }
    public long getAnalyzedCpuNanos() { return analyzedCpuNanos; }
    public long getNamespaceFilteredCpuCount() { return namespaceFilteredCpuCount; }
    public long getNamespaceFilteredCpuNanos() { return namespaceFilteredCpuNanos; }
    public long getLoggedCpuLines() { return loggedCpuLines; }
    public long getLoggedCpuNanos() { return loggedCpuNanos; }
    public long getUnparsedCpuLines() { return unparsedCpuLines; }
    public Map<Long, String> getNumCores() { return numCores; }

    /**
     * Log-wide CPU coverage over the window earliest..latest timestamp.
     */
    public static class Coverage {
        public final long loggedCpuNanos;
        public final long loggedCpuLines;
        public final long unparsedCpuLines;
        public final long analyzedCpuNanos;
        public final long analyzedCpuCount;
        public final long namespaceFilteredCpuNanos;
        public final long namespaceFilteredCpuCount;
        /** Logged CPU in lines that did not become analyzed operations for other reasons. */
        public final long otherUnanalyzedCpuNanos;
        public final String windowStart;
        public final String windowEnd;
        /** Null when the log has no timestamps. */
        public final Double windowSeconds;
        public final Double avgLoggedCores;
        /** Single core count found in the log, null when none or several differing values. */
        public final Long numCores;
        public final Map<Long, String> numCoresFound;
        public final Double percentOfCapacity;

        Coverage(CommentTagAccumulator a, String earliest, String latest) {
            loggedCpuNanos = a.loggedCpuNanos;
            loggedCpuLines = a.loggedCpuLines;
            unparsedCpuLines = a.unparsedCpuLines;
            analyzedCpuNanos = a.analyzedCpuNanos;
            analyzedCpuCount = a.analyzedCpuCount;
            namespaceFilteredCpuNanos = a.namespaceFilteredCpuNanos;
            namespaceFilteredCpuCount = a.namespaceFilteredCpuCount;
            otherUnanalyzedCpuNanos = a.loggedCpuNanos - a.analyzedCpuNanos - a.namespaceFilteredCpuNanos;
            windowStart = earliest;
            windowEnd = latest;
            if (earliest != null && latest != null) {
                java.time.Instant s = java.time.OffsetDateTime.parse(earliest).toInstant();
                java.time.Instant e = java.time.OffsetDateTime.parse(latest).toInstant();
                windowSeconds = java.time.Duration.between(s, e).toMillis() / 1000.0;
            } else {
                windowSeconds = null;
            }
            avgLoggedCores = windowSeconds != null && windowSeconds > 0 ? loggedCpuNanos / 1e9 / windowSeconds : null;
            numCoresFound = new TreeMap<>(a.numCores);
            numCores = a.numCores.size() == 1 ? a.numCores.keySet().iterator().next() : null;
            percentOfCapacity = numCores != null && avgLoggedCores != null ? avgLoggedCores * 100.0 / numCores : null;
        }
    }

    public synchronized Coverage getCoverage(String earliestTimestamp, String latestTimestamp) {
        return new Coverage(this, earliestTimestamp, latestTimestamp);
    }
}
