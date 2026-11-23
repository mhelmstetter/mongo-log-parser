package com.mongodb.log.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.mongodb.log.parser.accumulator.MaintenanceAccumulator;
import com.mongodb.log.parser.accumulator.WriteKeysAccumulator;

class MaintenanceAccumulatorTest {

    private static final String UUID = "11111111-2222-3333-4444-555555555555";

    private static String indexLine(String t, int id, String msg, String extraAttr) {
        return "{\"t\":{\"$date\":\"" + t + "\"},\"s\":\"I\",  \"c\":\"INDEX\",    \"id\":" + id
                + ",   \"ctx\":\"IndexBuildsCoordinatorMongod-3\",\"msg\":\"" + msg + "\",\"attr\":{\"buildUUID\":{\"uuid\":{\"$uuid\":\""
                + UUID + "\"}}" + extraAttr + "}}";
    }

    @Test
    void indexBuildWithoutStartInLog() {
        MaintenanceAccumulator m = new MaintenanceAccumulator();
        m.recordLine(indexLine("2026-01-01T08:15:45.956+00:00", 20391, "Index build: collection scan done",
                ",\"namespace\":\"shop.orders\",\"totalRecords\":1000000,\"durationMillis\":600000"), "shard-00-01");
        m.recordLine(indexLine("2026-01-01T08:17:35.071+00:00", 20345, "Index build: done building",
                ",\"namespace\":\"shop.orders\",\"index\":\"customer_1_status_1\""), "shard-00-01");
        m.recordLine(indexLine("2026-01-01T08:17:35.189+00:00", 20447, "Index build: completed", ""), "shard-00-01");

        List<MaintenanceAccumulator.IndexBuild> b = m.getIndexBuilds();
        assertEquals(1, b.size());
        MaintenanceAccumulator.IndexBuild ib = b.get(0);
        assertEquals("shop.orders", ib.getNamespace());
        assertEquals("completed", ib.getOutcome());
        assertFalse(ib.isStartSeen());
        assertEquals(1000000L, ib.getTotalRecords());
        assertEquals(600000L, ib.getScanDurationMillis());
        assertEquals("Index build: collection scan done", ib.getFirstMessage());
        assertEquals("Index build: completed", ib.getLastMessage());
        assertTrue(ib.getIndexes().contains("customer_1_status_1"));
        assertEquals(3, ib.getMessages());
    }

    @Test
    void inlineIndexWithNullBuildUuid() {
        MaintenanceAccumulator m = new MaintenanceAccumulator();
        m.recordLine("{\"t\":{\"$date\":\"2026-01-01T21:24:09.000+00:00\"},\"s\":\"I\",  \"c\":\"INDEX\",    \"id\":20345,   \"ctx\":\"x\","
                + "\"msg\":\"Index build: done building\",\"attr\":{\"buildUUID\":null,\"namespace\":\"shop.audit\",\"index\":\"_id_\"}}", "n");
        assertEquals(0, m.getUnparsedLines());
        assertTrue(m.getIndexBuilds().isEmpty());
        assertEquals(1L, m.getInlineIndexes().get("shop.audit").get("_id_"));
    }

    @Test
    void sideWriteDrainsWithoutBuildUuid() {
        MaintenanceAccumulator m = new MaintenanceAccumulator();
        String l = "{\"t\":{\"$date\":\"2026-01-01T08:16:30.101+00:00\"},\"s\":\"I\",  \"c\":\"INDEX\",    \"id\":20689,   \"ctx\":\"x\","
                + "\"msg\":\"Index build: drained side writes\",\"attr\":{\"index\":\"ix1\",\"namespace\":\"shop.orders\",\"numApplied\":1000,\"durationMillis\":50}}";
        m.recordLine(l, "n");
        m.recordLine(l, "n");
        assertEquals(0, m.getUnparsedLines());
        assertTrue(m.getIndexBuilds().isEmpty());
        MaintenanceAccumulator.SideWriteDrains d = m.getSideWriteDrains().get(0);
        assertEquals(2, d.getCount());
        assertEquals(2000, d.getApplied());
        assertEquals(100, d.getDurationMillis());
    }

    @Test
    void outcomeNotInLogWhenNoFinalMessage() {
        MaintenanceAccumulator m = new MaintenanceAccumulator();
        m.recordLine(indexLine("2026-01-01T06:00:00.000+00:00", 20384, "Index build: starting",
                ",\"namespace\":\"db.c\""), "n1");
        MaintenanceAccumulator.IndexBuild ib = m.getIndexBuilds().get(0);
        assertTrue(ib.isStartSeen());
        assertEquals("not in log", ib.getOutcome());
        assertNull(ib.getTotalRecords());
    }

    @Test
    void changelogMigrationEvents() {
        MaintenanceAccumulator m = new MaintenanceAccumulator();
        String commit = "{\"t\":{\"$date\":\"2026-01-01T21:21:22.915+00:00\"},\"s\":\"I\",  \"c\":\"SHARDING\", \"id\":22080,   \"ctx\":\"x\","
                + "\"msg\":\"About to log metadata event\",\"attr\":{\"namespace\":\"changelog\",\"event\":{\"what\":\"moveChunk.commit\","
                + "\"ns\":\"shop.events\",\"details\":{\"from\":\"s1\",\"to\":\"s0\",\"counts\":{\"cloned\":5000,\"clonedBytes\":4000000}}}}}";
        String aborted = "{\"t\":{\"$date\":\"2026-01-01T20:57:27.645+00:00\"},\"s\":\"I\",  \"c\":\"SHARDING\", \"id\":22080,   \"ctx\":\"x\","
                + "\"msg\":\"About to log metadata event\",\"attr\":{\"namespace\":\"changelog\",\"event\":{\"what\":\"moveChunk.from\","
                + "\"ns\":\"shop.events\",\"details\":{\"from\":\"s1\",\"to\":\"s0\",\"note\":\"aborted\"}}}}";
        String err = "{\"t\":{\"$date\":\"2026-01-01T20:55:44.774+00:00\"},\"s\":\"I\",  \"c\":\"MIGRATE\",  \"id\":21998,   \"ctx\":\"migrateThread\","
                + "\"msg\":\"Error during migration\",\"attr\":{\"error\":\"migrate failed: ExceededTimeLimit: range not cleared\"}}";
        m.recordLine(commit, "n");
        m.recordLine(aborted, "n");
        m.recordLine(aborted, "n");
        m.recordLine(err, "n");

        List<MaintenanceAccumulator.ChangelogGroup> g = m.getChangelogGroups();
        assertEquals(2, g.size());
        MaintenanceAccumulator.ChangelogGroup from = g.get(0);
        assertEquals("moveChunk.from", from.getWhat());
        assertEquals(2, from.getCount());
        assertEquals(2, from.getAborted());
        MaintenanceAccumulator.ChangelogGroup c = g.get(1);
        assertEquals(5000L, c.getClonedDocs());
        assertEquals(4000000L, c.getClonedBytes());
        assertEquals(1L, m.getChangelogByHour().get("moveChunk.commit").get("2026-01-01T21"));
        assertEquals(1L, m.getMigrationErrors().get("migrate failed: ExceededTimeLimit: range not cleared")[0]);
    }

    @Test
    void nonCandidateLinesIgnoredAndBadCandidatesCounted() {
        MaintenanceAccumulator m = new MaintenanceAccumulator();
        m.recordLine("{\"t\":{\"$date\":\"2026-01-01T00:00:00Z\"},\"c\":\"COMMAND\",\"id\":51803,\"msg\":\"Slow query\",\"attr\":{}}", "n");
        assertFalse(m.hasData());
        m.recordLine("{\"t\":{\"$date\":\"2026-01-01T00:00:00Z\"},\"c\":\"INDEX\",\"id\":1,\"msg\":\"Index build: starting\",\"attr\":{}}", "n");
        assertEquals(1, m.getUnparsedLines());
    }

    @Test
    void writeKeysPerDoc() {
        WriteKeysAccumulator w = new WriteKeysAccumulator();
        SlowQuery q = new SlowQuery();
        q.ns = new Namespace("shop.payments");
        q.opType = OpType.INSERT;
        q.nInserted = 1L;
        q.keysInserted = 6L;
        q.durationMillis = 507L;
        w.accumulate(q);
        SlowQuery u = new SlowQuery();
        u.ns = new Namespace("shop.payments");
        u.opType = OpType.INSERT;
        u.nInserted = 1L;
        u.keysInserted = 6L;
        u.durationMillis = 100L;
        w.accumulate(u);
        SlowQuery mirrored = new SlowQuery();
        mirrored.ns = q.ns;
        mirrored.opType = OpType.INSERT;
        mirrored.nInserted = 1L;
        mirrored.mirrored = true;
        w.accumulate(mirrored);
        SlowQuery read = new SlowQuery();
        read.ns = q.ns;
        read.opType = OpType.QUERY;
        w.accumulate(read);

        List<WriteKeysAccumulator.Entry> e = w.getEntries();
        assertEquals(1, e.size());
        assertEquals(2, e.get(0).getCount());
        assertEquals(12, e.get(0).getKeysInserted());
        assertEquals(6.0, e.get(0).getKeysPerDocWritten());
        assertEquals(607, e.get(0).getTotalDurationMs());
    }
}
