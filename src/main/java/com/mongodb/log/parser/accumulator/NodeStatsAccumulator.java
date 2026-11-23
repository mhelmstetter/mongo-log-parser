package com.mongodb.log.parser.accumulator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.mongodb.log.parser.NodeIdentity;
import com.mongodb.log.parser.OpType;
import com.mongodb.log.parser.SlowQuery;

/**
 * Per-process (node) slow operation statistics, used to compare shards and
 * same-role nodes against each other.
 */
public class NodeStatsAccumulator {

    public static class NodeStats {
        private final NodeIdentity identity;
        private long count;
        private long totalDurationMs;
        private long mirroredCount;
        private long nonMirroredDurationMs;
        private long writeCount;
        private long collscanCount;
        private final Map<String, Long> readPreferenceCounts = new TreeMap<>();
        private final Map<String, Long> appNameDurationMs = new HashMap<>();
        private final Map<String, Long> namespaceDurationMs = new HashMap<>();
        private final Map<String, Long> mirroredNamespaceDurationMs = new HashMap<>();
        private final List<String[]> stateTransitions = new ArrayList<>();
        private String role = "UNKNOWN";
        private String roleBasis = "not resolved";

        NodeStats(NodeIdentity identity) {
            this.identity = identity;
        }

        void add(SlowQuery slowQuery) {
            count++;
            long duration = slowQuery.durationMillis;
            totalDurationMs += duration;
            boolean mirrored = Boolean.TRUE.equals(slowQuery.mirrored);
            if (mirrored) {
                mirroredCount++;
            } else {
                nonMirroredDurationMs += duration;
                if (isWrite(slowQuery.opType)) {
                    writeCount++;
                }
                if (slowQuery.planSummary != null && slowQuery.planSummary.contains("COLLSCAN")) {
                    collscanCount++;
                }
            }
            String readPreference = slowQuery.readPreference != null ? slowQuery.readPreference : "(none)";
            readPreferenceCounts.merge(readPreference, 1L, Long::sum);
            // Breakdowns use the same basis as nonMirroredDurationMs, so they add up to it
            if (!mirrored) {
                String appName = slowQuery.appName != null ? slowQuery.appName : "(none)";
                appNameDurationMs.merge(appName, duration, Long::sum);
                if (slowQuery.ns != null) {
                    namespaceDurationMs.merge(slowQuery.ns.toString(), duration, Long::sum);
                }
            } else if (slowQuery.ns != null) {
                mirroredNamespaceDurationMs.merge(slowQuery.ns.toString(), duration, Long::sum);
            }
        }

        private static boolean isWrite(OpType opType) {
            return opType == OpType.INSERT || opType == OpType.UPDATE || opType == OpType.UPDATE_W
                    || opType == OpType.REMOVE || opType == OpType.FIND_AND_MODIFY;
        }

        public NodeIdentity getIdentity() {
            return identity;
        }

        public long getCount() {
            return count;
        }

        public long getTotalDurationMs() {
            return totalDurationMs;
        }

        public long getMirroredCount() {
            return mirroredCount;
        }

        public long getNonMirroredDurationMs() {
            return nonMirroredDurationMs;
        }

        public long getWriteCount() {
            return writeCount;
        }

        public long getCollscanCount() {
            return collscanCount;
        }

        public Map<String, Long> getReadPreferenceCounts() {
            return readPreferenceCounts;
        }

        public Map<String, Long> getAppNameDurationMs() {
            return appNameDurationMs;
        }

        public Map<String, Long> getNamespaceDurationMs() {
            return namespaceDurationMs;
        }

        /** Slow time of mirrored reads received, by namespace. */
        public Map<String, Long> getMirroredNamespaceDurationMs() {
            return mirroredNamespaceDurationMs;
        }

        public List<String[]> getStateTransitions() {
            return stateTransitions;
        }

        /** PRIMARY, SECONDARY, ROUTER (mongos) or UNKNOWN, as inferred from the logs. */
        public String getRole() {
            return role;
        }

        public String getRoleBasis() {
            return roleBasis;
        }

        /** True when the node changed replica set state during the log window. */
        public boolean hasRoleChanges() {
            return stateTransitions.size() > 0;
        }
    }

    private final Map<String, NodeStats> nodes = new TreeMap<>();

    public synchronized void accumulate(NodeIdentity identity, SlowQuery slowQuery) {
        if (slowQuery.durationMillis == null) {
            return;
        }
        nodes.computeIfAbsent(identity.getNode(), k -> new NodeStats(identity)).add(slowQuery);
    }

    public synchronized void recordStateTransition(NodeIdentity identity, String timestamp, String newState) {
        nodes.computeIfAbsent(identity.getNode(), k -> new NodeStats(identity))
                .stateTransitions.add(new String[] { timestamp, newState });
    }

    public synchronized boolean hasData() {
        return !nodes.isEmpty();
    }

    public synchronized Collection<NodeStats> getNodes() {
        resolveRoles();
        return new ArrayList<>(nodes.values());
    }

    /**
     * Infer each node's role. A secondary can log a few slow writes (e.g. ones that failed
     * with NotWritablePrimary), so within a shard the member with the most writes is the
     * primary, rather than any member with writes.
     */
    private void resolveRoles() {
        Map<String, Long> maxWritesPerShard = new HashMap<>();
        for (NodeStats n : nodes.values()) {
            String shard = n.identity.getShard();
            if (shard != null && "mongod".equals(n.identity.getProcessType())) {
                maxWritesPerShard.merge(shard, n.writeCount, Math::max);
            }
        }
        for (NodeStats n : nodes.values()) {
            String shard = n.identity.getShard();
            long shardMaxWrites = shard != null ? maxWritesPerShard.getOrDefault(shard, 0L) : 0L;
            if ("mongos".equals(n.identity.getProcessType())) {
                set(n, "ROUTER", "mongos log");
            } else if (!n.stateTransitions.isEmpty()) {
                set(n, n.stateTransitions.get(n.stateTransitions.size() - 1)[1], "last replica set state transition");
            } else if (shardMaxWrites > 0 && n.writeCount == shardMaxWrites) {
                set(n, "PRIMARY", "most writes in its shard");
            } else if (n.mirroredCount > 0) {
                set(n, "SECONDARY", "received mirrored reads");
            } else if (shardMaxWrites > 0 && n.writeCount * 100 < shardMaxWrites) {
                set(n, "SECONDARY", "under 1% of its shard primary's writes");
            } else if (shard == null && n.writeCount > 0) {
                set(n, "PRIMARY", "executed writes");
            } else {
                set(n, "UNKNOWN", "no writes, mirrored reads or state transitions in log");
            }
        }
    }

    private static void set(NodeStats n, String role, String basis) {
        n.role = role;
        n.roleBasis = basis;
    }
}
