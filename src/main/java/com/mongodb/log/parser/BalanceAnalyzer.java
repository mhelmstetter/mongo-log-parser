package com.mongodb.log.parser;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.mongodb.log.parser.accumulator.NodeStatsAccumulator;
import com.mongodb.log.parser.accumulator.NodeStatsAccumulator.NodeStats;

/**
 * Compares shards with each other, and nodes with others in the same role
 * (primaries, secondaries, mongos), on non-mirrored slow-operation time.
 * Flags blatant unevenness and names the namespaces behind it, noting when
 * an outlier is driven by namespaces that appear in only one shard's slow-query
 * logs. That alone does not tell whether a namespace is unsharded: a sharded
 * collection whose hot chunks sit on one shard looks the same, so the flag says
 * to check config.collections and the chunk distribution.
 */
public class BalanceAnalyzer {

    /** A group member at or above this multiple of the group median is flagged high. */
    public static final double HIGH_RATIO = 2.0;
    /** A group member at or below this fraction of the group median is flagged low. */
    public static final double LOW_RATIO = 0.33;
    public static final int MIN_GROUP_SIZE = 3;
    public static final int TOP_NAMESPACES = 5;

    public static class NamespaceShare {
        public final String namespace;
        public final long durationMs;
        /** Number of shards whose slow-query logs mention this namespace (not where its data lives). */
        public final int shardsInSlowLogs;

        NamespaceShare(String namespace, long durationMs, int shardsInSlowLogs) {
            this.namespace = namespace;
            this.durationMs = durationMs;
            this.shardsInSlowLogs = shardsInSlowLogs;
        }
    }

    public static class Flag {
        public final String scope;      // "shard" or "node"
        public final String group;      // e.g. "shards", "PRIMARY mongod", "SECONDARY mongod in shard 03"
        public final String subject;    // shard number or node name
        public final String direction;  // "high" or "low"
        public final long durationMs;
        public final long groupMedianMs;
        public final double ratio;
        public final List<NamespaceShare> topNamespaces;
        public final String likelyCause;

        Flag(String scope, String group, String subject, String direction, long durationMs, long groupMedianMs,
                List<NamespaceShare> topNamespaces, String likelyCause) {
            this.scope = scope;
            this.group = group;
            this.subject = subject;
            this.direction = direction;
            this.durationMs = durationMs;
            this.groupMedianMs = groupMedianMs;
            this.ratio = groupMedianMs > 0 ? (double) durationMs / groupMedianMs : 0.0;
            this.topNamespaces = topNamespaces;
            this.likelyCause = likelyCause;
        }
    }

    /** Per-shard totals across that shard's mongod members. */
    public static class ShardTotals {
        public final String shard;
        public long durationMs;
        public long count;
        public final Map<String, Long> namespaceDurationMs = new HashMap<>();

        ShardTotals(String shard) {
            this.shard = shard;
        }
    }

    private final Map<String, ShardTotals> shards = new TreeMap<>();
    private final List<Flag> flags = new ArrayList<>();
    private final Map<String, Set<String>> namespaceShards = new HashMap<>();

    public BalanceAnalyzer(NodeStatsAccumulator nodeStats) {
        Collection<NodeStats> nodes = nodeStats.getNodes();

        for (NodeStats node : nodes) {
            String shard = node.getIdentity().getShard();
            if (!"mongod".equals(node.getIdentity().getProcessType()) || shard == null || "config".equals(shard)) {
                continue;
            }
            ShardTotals totals = shards.computeIfAbsent(shard, ShardTotals::new);
            totals.durationMs += node.getNonMirroredDurationMs();
            totals.count += node.getCount() - node.getMirroredCount();
            node.getNamespaceDurationMs().forEach((ns, ms) -> {
                totals.namespaceDurationMs.merge(ns, ms, Long::sum);
                namespaceShards.computeIfAbsent(ns, k -> new HashSet<>()).add(shard);
            });
        }

        compareShards();
        compareNodes(nodes);
    }

    private void compareShards() {
        if (shards.size() < MIN_GROUP_SIZE) {
            return;
        }
        long median = median(shards.values().stream().map(s -> s.durationMs).toList());
        for (ShardTotals shard : shards.values()) {
            String direction = direction(shard.durationMs, median);
            if (direction == null) {
                continue;
            }
            List<NamespaceShare> top = topNamespaces(shard.namespaceDurationMs);
            flags.add(new Flag("shard", "shards", shard.shard, direction, shard.durationMs, median, top,
                    likelyCause(direction, shard.durationMs, top)));
        }
    }

    private void compareNodes(Collection<NodeStats> nodes) {
        // Same-role nodes across the cluster, and secondaries within each shard
        Map<String, List<NodeStats>> groups = new TreeMap<>();
        for (NodeStats node : nodes) {
            NodeIdentity id = node.getIdentity();
            if ("config".equals(id.getShard())) {
                continue;
            }
            String role = node.getRole();
            groups.computeIfAbsent(role + " " + id.getProcessType(), k -> new ArrayList<>()).add(node);
            if ("SECONDARY".equals(role) && id.getShard() != null) {
                groups.computeIfAbsent("SECONDARY mongod in shard " + id.getShard(), k -> new ArrayList<>()).add(node);
            }
        }
        for (Map.Entry<String, List<NodeStats>> group : groups.entrySet()) {
            List<NodeStats> members = group.getValue();
            boolean withinShard = group.getKey().contains(" in shard ");
            // Two secondaries of one shard are compared with each other; cluster-wide groups need MIN_GROUP_SIZE
            if (members.size() < (withinShard ? 2 : MIN_GROUP_SIZE) || group.getKey().startsWith("UNKNOWN")) {
                continue;
            }
            long median = median(members.stream().map(NodeStats::getNonMirroredDurationMs).toList());
            for (NodeStats node : members) {
                long duration = node.getNonMirroredDurationMs();
                String direction = withinShard ? withinShardDirection(duration, members) : direction(duration, median);
                if (direction == null) {
                    continue;
                }
                List<NamespaceShare> top = topNamespaces(node.getNamespaceDurationMs());
                flags.add(new Flag("node", group.getKey(), node.getIdentity().getNode(), direction, duration, median,
                        top, likelyCause(direction, duration, top)));
            }
        }
    }

    /** Within a shard's secondaries, flag one that does at least 3x the work of another. */
    private static String withinShardDirection(long duration, List<NodeStats> members) {
        long min = members.stream().mapToLong(NodeStats::getNonMirroredDurationMs).min().getAsLong();
        long max = members.stream().mapToLong(NodeStats::getNonMirroredDurationMs).max().getAsLong();
        if (duration == max && max >= 3 * Math.max(min, 1) && max > 0) {
            return "high";
        }
        return null;
    }

    private static String direction(long value, long median) {
        if (median <= 0) {
            return null;
        }
        if (value >= HIGH_RATIO * median) {
            return "high";
        }
        if (value <= LOW_RATIO * median) {
            return "low";
        }
        return null;
    }

    private List<NamespaceShare> topNamespaces(Map<String, Long> namespaceDurationMs) {
        return namespaceDurationMs.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .limit(TOP_NAMESPACES)
                .map(e -> new NamespaceShare(e.getKey(), e.getValue(),
                        namespaceShards.getOrDefault(e.getKey(), Set.of()).size()))
                .toList();
    }

    private static String likelyCause(String direction, long durationMs, List<NamespaceShare> top) {
        if (!"high".equals(direction) || durationMs == 0) {
            return null;
        }
        long singleShardMs = top.stream().filter(ns -> ns.shardsInSlowLogs == 1).mapToLong(ns -> ns.durationMs).sum();
        if (singleShardMs * 2 >= durationMs) {
            return "Most of this time is on namespaces that appear in only this shard's slow-query logs. They may be "
                    + "unsharded (living on their database's primary shard, often expected) or sharded with their hot "
                    + "chunks here: check config.collections and the chunk distribution before concluding either.";
        }
        return "Driven by namespaces that exist on several shards: check for hot shard key ranges, "
                + "uneven chunk distribution, or read preference / tag routing.";
    }

    private static long median(List<Long> values) {
        List<Long> sorted = new ArrayList<>(values);
        sorted.sort(null);
        int n = sorted.size();
        return n % 2 == 1 ? sorted.get(n / 2) : (sorted.get(n / 2 - 1) + sorted.get(n / 2)) / 2;
    }

    public Collection<ShardTotals> getShards() {
        return shards.values();
    }

    public List<Flag> getFlags() {
        return flags;
    }
}
