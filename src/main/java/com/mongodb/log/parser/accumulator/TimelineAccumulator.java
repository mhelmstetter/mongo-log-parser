package com.mongodb.log.parser.accumulator;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.mongodb.log.parser.SlowQuery;

/**
 * Buckets slow operations per UTC minute, to show bursts where many unrelated
 * operations are slow at the same time (e.g. a stalled shard or node).
 */
public class TimelineAccumulator {

    public static class Bucket {
        private final String start;
        private long count;
        private long totalDurationMs;
        private long maxDurationMs;
        private long totalRemoteOpWaitMs;
        // Non-mirrored slow time by node and by namespace, to see who drove a burst
        private final Map<String, long[]> byNode = new HashMap<>();
        private final Map<String, Long> namespaceDurationMs = new HashMap<>();

        Bucket(String start) {
            this.start = start;
        }

        void add(SlowQuery slowQuery) {
            count++;
            long duration = slowQuery.durationMillis;
            totalDurationMs += duration;
            if (duration > maxDurationMs) {
                maxDurationMs = duration;
            }
            if (slowQuery.remoteOpWaitMillis != null) {
                totalRemoteOpWaitMs += slowQuery.remoteOpWaitMillis;
            }
            if (Boolean.TRUE.equals(slowQuery.mirrored)) {
                return;
            }
            if (slowQuery.node != null) {
                long[] n = byNode.computeIfAbsent(slowQuery.node, k -> new long[2]);
                n[0]++;
                n[1] += duration;
            }
            namespaceDurationMs.merge(slowQuery.ns != null ? slowQuery.ns.toString() : "(none)", duration, Long::sum);
        }

        void merge(Bucket other) {
            count += other.count;
            totalDurationMs += other.totalDurationMs;
            maxDurationMs = Math.max(maxDurationMs, other.maxDurationMs);
            totalRemoteOpWaitMs += other.totalRemoteOpWaitMs;
            other.byNode.forEach((node, n) -> {
                long[] mine = byNode.computeIfAbsent(node, k -> new long[2]);
                mine[0] += n[0];
                mine[1] += n[1];
            });
            other.namespaceDurationMs.forEach((ns, ms) -> namespaceDurationMs.merge(ns, ms, Long::sum));
        }

        public String getStart() {
            return start;
        }

        public long getCount() {
            return count;
        }

        public long getTotalDurationMs() {
            return totalDurationMs;
        }

        public long getMaxDurationMs() {
            return maxDurationMs;
        }

        public double getRemoteOpWaitPercentage() {
            return totalDurationMs > 0 ? totalRemoteOpWaitMs * 100.0 / totalDurationMs : 0.0;
        }

        public int getDistinctNamespaces() {
            return namespaceDurationMs.size();
        }

        /** Non-mirrored [count, durationMs] per node, highest duration first. */
        public List<Map.Entry<String, long[]>> getTopNodes(int limit) {
            List<Map.Entry<String, long[]>> list = new ArrayList<>(byNode.entrySet());
            list.sort((a, b) -> Long.compare(b.getValue()[1], a.getValue()[1]));
            return list.subList(0, Math.min(limit, list.size()));
        }

        /** Non-mirrored duration per namespace, highest first. */
        public List<Map.Entry<String, Long>> getTopNamespaces(int limit) {
            List<Map.Entry<String, Long>> list = new ArrayList<>(namespaceDurationMs.entrySet());
            list.sort(Map.Entry.<String, Long>comparingByValue().reversed());
            return list.subList(0, Math.min(limit, list.size()));
        }
    }

    private final Map<String, Bucket> minutes = new TreeMap<>();
    private long missingTimestamp = 0;
    private long unparseableTimestamp = 0;

    public synchronized void accumulate(SlowQuery slowQuery) {
        if (slowQuery.durationMillis == null) {
            return;
        }
        if (slowQuery.timestamp == null) {
            missingTimestamp++;
            return;
        }
        OffsetDateTime time;
        try {
            time = OffsetDateTime.parse(slowQuery.timestamp);
        } catch (DateTimeParseException e) {
            unparseableTimestamp++;
            return;
        }
        String minute = time.withOffsetSameInstant(ZoneOffset.UTC).truncatedTo(ChronoUnit.MINUTES).toString();
        minutes.computeIfAbsent(minute, Bucket::new).add(slowQuery);
    }

    public synchronized boolean hasData() {
        return !minutes.isEmpty();
    }

    public synchronized long getMissingTimestamp() {
        return missingTimestamp;
    }

    public synchronized long getUnparseableTimestamp() {
        return unparseableTimestamp;
    }

    public synchronized int getMinuteCount() {
        return minutes.size();
    }

    /** Hourly buckets in time order. */
    public synchronized List<Bucket> getHourly() {
        Map<String, Bucket> hours = new TreeMap<>();
        for (Bucket minute : minutes.values()) {
            String hour = minute.getStart().substring(0, 13) + ":00Z";
            hours.computeIfAbsent(hour, Bucket::new).merge(minute);
        }
        return new ArrayList<>(hours.values());
    }

    /** The minutes with the most total slow-operation time. */
    public synchronized List<Bucket> getTopMinutes(int limit) {
        List<Bucket> sorted = new ArrayList<>(minutes.values());
        sorted.sort(Comparator.comparingLong(Bucket::getTotalDurationMs).reversed());
        return sorted.subList(0, Math.min(limit, sorted.size()));
    }
}
