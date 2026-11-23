package com.mongodb.log.parser.accumulator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.json.JSONObject;

/**
 * Background work that can overlap with (and explain) a slow period, read from every log line before line filtering:
 *
 * - Index builds, grouped by buildUUID from the "Index build: ..." INDEX messages: node, namespace, index names,
 *   first and last message seen, records scanned and collection-scan duration, and the outcome (completed / aborted /
 *   failed). A build whose start or end falls outside the log is reported as such, never inferred.
 * - Sharding metadata events ("About to log metadata event", id 22080): the config changelog entry a shard writes for
 *   moveChunk.start / from / to / commit / error, splits, merges and range deletions. Counted per event type,
 *   namespace and from -> to shard, with cloned documents and bytes from moveChunk.commit and the "aborted" notes.
 * - Migration errors ("Error during migration", id 21998), grouped by message.
 */
public class MaintenanceAccumulator {

    public static final int CHANGELOG_EVENT_ID = 22080;
    public static final int MIGRATION_ERROR_ID = 21998;

    public static class IndexBuild {
        private final String buildUUID;
        private String node;
        private String namespace;
        private final TreeSet<String> indexes = new TreeSet<>();
        private String firstSeen;
        private String firstMessage;
        private String lastSeen;
        private String lastMessage;
        private Long totalRecords;
        private Long scanDurationMillis;
        private String scanDoneAt;
        private String outcome;
        private String error;
        private boolean startSeen;
        private long messages;

        IndexBuild(String buildUUID) {
            this.buildUUID = buildUUID;
        }

        public String getBuildUUID() { return buildUUID; }
        public String getNode() { return node; }
        public String getNamespace() { return namespace; }
        public TreeSet<String> getIndexes() { return indexes; }
        public String getFirstSeen() { return firstSeen; }
        public String getFirstMessage() { return firstMessage; }
        public String getLastSeen() { return lastSeen; }
        public String getLastMessage() { return lastMessage; }
        public Long getTotalRecords() { return totalRecords; }
        public Long getScanDurationMillis() { return scanDurationMillis; }
        public String getScanDoneAt() { return scanDoneAt; }
        /** completed, aborted, failed, or "not in log" when no final message was seen. */
        public String getOutcome() { return outcome != null ? outcome : "not in log"; }
        /** codeName (or errmsg) of the first error attribute seen on the build's messages; null if none. */
        public String getError() { return error; }
        public boolean isStartSeen() { return startSeen; }
        public long getMessages() { return messages; }
    }

    public static class ChangelogGroup {
        private final String what;
        private final String namespace;
        private final String from;
        private final String to;
        private long count;
        private long aborted;
        private long clonedDocs;
        private long clonedBytes;
        private String first;
        private String last;

        ChangelogGroup(String what, String namespace, String from, String to) {
            this.what = what;
            this.namespace = namespace;
            this.from = from;
            this.to = to;
        }

        public String getWhat() { return what; }
        public String getNamespace() { return namespace; }
        public String getFrom() { return from; }
        public String getTo() { return to; }
        public long getCount() { return count; }
        public long getAborted() { return aborted; }
        public long getClonedDocs() { return clonedDocs; }
        public long getClonedBytes() { return clonedBytes; }
        public String getFirst() { return first; }
        public String getLast() { return last; }
    }

    /** "Index build: drained side writes" lines carry namespace + index, not buildUUID. */
    public static class SideWriteDrains {
        private final String namespace;
        private final String index;
        private long count;
        private long applied;
        private long durationMillis;
        private String first;
        private String last;

        SideWriteDrains(String namespace, String index) {
            this.namespace = namespace;
            this.index = index;
        }

        public String getNamespace() { return namespace; }
        public String getIndex() { return index; }
        public long getCount() { return count; }
        public long getApplied() { return applied; }
        public long getDurationMillis() { return durationMillis; }
        public String getFirst() { return first; }
        public String getLast() { return last; }
    }

    private final Map<String, SideWriteDrains> sideDrains = new LinkedHashMap<>();
    /** "Index build: done building" with buildUUID null: built inline (e.g. _id_ at collection creation), not a background build. ns -> index -> count */
    private final Map<String, TreeMap<String, Long>> inlineIndexes = new TreeMap<>();
    private final Map<String, IndexBuild> builds = new LinkedHashMap<>();
    private final Map<String, ChangelogGroup> changelog = new LinkedHashMap<>();
    /** event type -> UTC hour -> count */
    private final Map<String, TreeMap<String, Long>> changelogByHour = new TreeMap<>();
    private final Map<String, long[]> migrationErrors = new TreeMap<>();
    private long unparsedLines;

    /** Cheap pre-check, so only candidate lines are parsed as JSON. */
    public static boolean isCandidate(String line) {
        return (line.contains("\"c\":\"INDEX\"") && line.contains("\"msg\":\"Index build: "))
                || line.contains("\"id\":" + CHANGELOG_EVENT_ID + ",")
                || line.contains("\"id\":" + MIGRATION_ERROR_ID + ",");
    }

    public synchronized void recordLine(String line, String node) {
        if (!isCandidate(line)) {
            return;
        }
        JSONObject jo;
        try {
            jo = new JSONObject(line);
        } catch (org.json.JSONException e) {
            unparsedLines++;
            return;
        }
        String t = jo.getJSONObject("t").getString("$date");
        JSONObject attr = jo.optJSONObject("attr");
        if (attr == null) {
            unparsedLines++;
            return;
        }
        int id = jo.optInt("id");
        String msg = jo.optString("msg");
        if (id == CHANGELOG_EVENT_ID) {
            recordChangelog(attr.optJSONObject("event"), t);
        } else if (id == MIGRATION_ERROR_ID) {
            String err = attr.optString("error", "(no error attribute)");
            String key = err.length() > 200 ? err.substring(0, 200) : err;
            long[] v = migrationErrors.computeIfAbsent(key, k -> new long[1]);
            v[0]++;
        } else if (msg.startsWith("Index build: ")) {
            recordIndexBuild(attr, msg, t, node);
        }
    }

    private void recordIndexBuild(JSONObject attr, String msg, String t, String node) {
        if (msg.startsWith("Index build: drained side writes") && !attr.has("buildUUID")) {
            if (!attr.has("namespace") || !attr.has("index")) {
                unparsedLines++;
                return;
            }
            String ns = attr.getString("namespace"), ix = attr.getString("index");
            SideWriteDrains d = sideDrains.computeIfAbsent(ns + "\u0000" + ix, k -> new SideWriteDrains(ns, ix));
            d.count++;
            d.applied += attr.optLong("numApplied", 0);
            d.durationMillis += attr.optLong("durationMillis", 0);
            if (d.first == null || t.compareTo(d.first) < 0) d.first = t;
            if (d.last == null || t.compareTo(d.last) > 0) d.last = t;
            return;
        }
        if (attr.has("buildUUID") && attr.isNull("buildUUID") && msg.startsWith("Index build: done building")
                && attr.has("namespace") && attr.has("index")) {
            inlineIndexes.computeIfAbsent(attr.getString("namespace"), k -> new TreeMap<>()).merge(attr.getString("index"), 1L, Long::sum);
            return;
        }
        JSONObject b = attr.optJSONObject("buildUUID");
        JSONObject u = b != null ? b.optJSONObject("uuid") : null;
        if (u == null || !u.has("$uuid")) {
            unparsedLines++;
            return;
        }
        String id = u.getString("$uuid");
        IndexBuild ib = builds.computeIfAbsent(id, IndexBuild::new);
        ib.messages++;
        if (ib.node == null) ib.node = node;
        if (ib.firstSeen == null || t.compareTo(ib.firstSeen) < 0) {
            ib.firstSeen = t;
            ib.firstMessage = msg;
        }
        if (ib.lastSeen == null || t.compareTo(ib.lastSeen) >= 0) {
            ib.lastSeen = t;
            ib.lastMessage = msg;
        }
        if (attr.has("namespace")) ib.namespace = attr.getString("namespace");
        if (attr.has("index")) ib.indexes.add(attr.getString("index"));
        if (attr.has("indexes")) {
            Object v = attr.get("indexes");
            if (v instanceof org.json.JSONArray) {
                org.json.JSONArray a = (org.json.JSONArray) v;
                for (int i = 0; i < a.length(); i++) {
                    Object x = a.get(i);
                    ib.indexes.add(x instanceof JSONObject && ((JSONObject) x).has("name") ? ((JSONObject) x).getString("name") : String.valueOf(x));
                }
            } else {
                ib.indexes.add(String.valueOf(v));
            }
        }
        String lower = msg.toLowerCase();
        if (lower.contains("starting") || lower.contains("registering")) ib.startSeen = true;
        if (msg.startsWith("Index build: collection scan done")) {
            if (attr.has("totalRecords")) ib.totalRecords = attr.getLong("totalRecords");
            if (attr.has("durationMillis")) ib.scanDurationMillis = attr.getLong("durationMillis");
            ib.scanDoneAt = t;
        }
        if (ib.error == null && attr.has("error")) {
            Object e = attr.get("error");
            ib.error = e instanceof JSONObject ? ((JSONObject) e).optString("codeName", ((JSONObject) e).optString("errmsg", e.toString())) : String.valueOf(e);
        }
        if (msg.startsWith("Index build: completed")) ib.outcome = "completed";
        else if (lower.contains("abort")) ib.outcome = "aborted";
        else if (lower.contains("fail")) ib.outcome = "failed";
    }

    private void recordChangelog(JSONObject ev, String t) {
        if (ev == null || !ev.has("what")) {
            unparsedLines++;
            return;
        }
        String what = ev.getString("what");
        String ns = ev.optString("ns", "(no ns)");
        JSONObject d = ev.optJSONObject("details");
        String from = d != null ? d.optString("from", "") : "";
        String to = d != null ? d.optString("to", "") : "";
        ChangelogGroup g = changelog.computeIfAbsent(what + "\u0000" + ns + "\u0000" + from + "\u0000" + to,
                k -> new ChangelogGroup(what, ns, from, to));
        g.count++;
        if (g.first == null || t.compareTo(g.first) < 0) g.first = t;
        if (g.last == null || t.compareTo(g.last) > 0) g.last = t;
        if (d != null) {
            if ("aborted".equals(d.optString("note"))) g.aborted++;
            JSONObject c = d.optJSONObject("counts");
            if (c != null) {
                g.clonedDocs += c.optLong("cloned", 0);
                g.clonedBytes += c.optLong("clonedBytes", 0);
            }
        }
        changelogByHour.computeIfAbsent(what, k -> new TreeMap<>()).merge(t.substring(0, 13), 1L, Long::sum);
    }

    public synchronized boolean hasData() {
        return !builds.isEmpty() || !sideDrains.isEmpty() || !inlineIndexes.isEmpty() || !changelog.isEmpty() || !migrationErrors.isEmpty();
    }

    public synchronized Map<String, TreeMap<String, Long>> getInlineIndexes() {
        return inlineIndexes;
    }

    public synchronized List<SideWriteDrains> getSideWriteDrains() {
        return new ArrayList<>(sideDrains.values());
    }

    public synchronized List<IndexBuild> getIndexBuilds() {
        List<IndexBuild> l = new ArrayList<>(builds.values());
        l.sort(Comparator.comparing(IndexBuild::getFirstSeen));
        return l;
    }

    public synchronized List<ChangelogGroup> getChangelogGroups() {
        List<ChangelogGroup> l = new ArrayList<>(changelog.values());
        l.sort(Comparator.comparingLong(ChangelogGroup::getCount).reversed().thenComparing(ChangelogGroup::getWhat));
        return l;
    }

    public synchronized Map<String, TreeMap<String, Long>> getChangelogByHour() {
        return changelogByHour;
    }

    public synchronized Map<String, long[]> getMigrationErrors() {
        return migrationErrors;
    }

    public synchronized long getUnparsedLines() {
        return unparsedLines;
    }
}
