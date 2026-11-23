package com.mongodb.log.parser;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.log.parser.accumulator.Accumulator;
import com.mongodb.log.parser.accumulator.TwoPassDriverStatsAccumulator;
import com.mongodb.log.parser.accumulator.DriverStatsEntry;
import com.mongodb.log.parser.accumulator.ErrorCodeAccumulator;
import com.mongodb.log.parser.accumulator.IndexStatsAccumulator;
import com.mongodb.log.parser.accumulator.PlanCacheAccumulator;
import com.mongodb.log.parser.accumulator.QueryHashAccumulator;
import com.mongodb.log.parser.accumulator.QueryHashAccumulatorEntry;
import com.mongodb.log.parser.accumulator.NodeStatsAccumulator;
import com.mongodb.log.parser.accumulator.CommentTagAccumulator;
import com.mongodb.log.parser.accumulator.MaintenanceAccumulator;
import com.mongodb.log.parser.accumulator.WriteKeysAccumulator;
import com.mongodb.log.parser.accumulator.TimelineAccumulator;
import com.mongodb.log.parser.accumulator.TransactionAccumulator;

/**
 * Generates structured JSON reports from MongoDB log analysis data
 */
public class JsonReportGenerator {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static void generateReport(String fileName, Accumulator accumulator, Accumulator ttlAccumulator,
            PlanCacheAccumulator planCacheAccumulator,
            QueryHashAccumulator queryHashAccumulator,
            ErrorCodeAccumulator errorCodeAccumulator,
            TransactionAccumulator transactionAccumulator,
            IndexStatsAccumulator indexStatsAccumulator,
            TwoPassDriverStatsAccumulator driverStatsAccumulator,
            TimelineAccumulator timelineAccumulator,
            NodeStatsAccumulator nodeStatsAccumulator,
            CommentTagAccumulator commentTagAccumulator,
            MaintenanceAccumulator maintenanceAccumulator,
            WriteKeysAccumulator writeKeysAccumulator,
            Map<String, java.util.concurrent.atomic.AtomicLong> operationTypeStats, boolean redactQueries,
            String earliestTimestamp, String latestTimestamp, String windowFrom, String windowTo) throws IOException {

        ObjectNode report = mapper.createObjectNode();
        
        // Report metadata
        ObjectNode metadata = mapper.createObjectNode();
        metadata.put("generatedAt", java.time.Instant.now().toString());
        metadata.put("earliestTimestamp", earliestTimestamp);
        metadata.put("latestTimestamp", latestTimestamp);
        metadata.put("redactedQueries", redactQueries);
        if (windowFrom != null || windowTo != null) {
            ObjectNode window = mapper.createObjectNode();
            window.put("from", windowFrom);
            window.put("to", windowTo);
            metadata.set("timeWindow", window);
        }
        report.set("metadata", metadata);

        // Main operations analysis
        report.set("mainOperations", generateMainOperationsJson(accumulator, redactQueries));

        // Log-wide CPU coverage and CPU by application comment tag
        if (commentTagAccumulator.hasData()) {
            report.set("cpuCoverage", generateCpuCoverageJson(commentTagAccumulator.getCoverage(earliestTimestamp, latestTimestamp)));
            report.set("cpuByCommentTag", generateCommentTagJson(commentTagAccumulator));
        }

        // Index builds, chunk migrations and sharding metadata events
        if (maintenanceAccumulator != null && (maintenanceAccumulator.hasData() || maintenanceAccumulator.getUnparsedLines() > 0)) {
            report.set("maintenance", generateMaintenanceJson(maintenanceAccumulator));
        }

        // Index keys touched per document written (slow writes)
        if (writeKeysAccumulator != null && writeKeysAccumulator.hasData()) {
            report.set("writeKeys", generateWriteKeysJson(writeKeysAccumulator));
        }

        // TTL operations analysis
        if (ttlAccumulator != null && !ttlAccumulator.getAccumulators().isEmpty()) {
            report.set("ttlOperations", generateTtlOperationsJson(ttlAccumulator));
        }

        // Operation type statistics
        report.set("operationTypeStats", generateOperationStatsJson(operationTypeStats));

        // Error codes analysis
        if (errorCodeAccumulator != null && errorCodeAccumulator.hasErrors()) {
            report.set("errorCodes", generateErrorCodesJson(errorCodeAccumulator, redactQueries));
        }

        // Query hash analysis
        if (queryHashAccumulator != null) {
            report.set("queryHashAnalysis", generateQueryHashJson(queryHashAccumulator, redactQueries));
        }

        // Transaction analysis
        if (transactionAccumulator != null && transactionAccumulator.hasTransactions()) {
            report.set("transactions", generateTransactionJson(transactionAccumulator));
        }

        // Index usage statistics
        if (indexStatsAccumulator != null && indexStatsAccumulator.hasIndexStats()) {
            report.set("indexStats", generateIndexStatsJson(indexStatsAccumulator, redactQueries));
        }

        // Flagged query shapes (collection scans, high examined/returned), excluding mirrored reads
        if (queryHashAccumulator != null) {
            QueryFindings findings = new QueryFindings(queryHashAccumulator);
            if (findings.hasFindings()) {
                report.set("findings", generateFindingsJson(findings, redactQueries));
            }
        }

        // Per-node statistics and shard / same-role balance
        if (nodeStatsAccumulator != null && nodeStatsAccumulator.hasData()) {
            report.set("nodes", generateNodesJson(nodeStatsAccumulator));
            report.set("balance", generateBalanceJson(new BalanceAnalyzer(nodeStatsAccumulator)));
        }

        // Slow operation timeline (per UTC minute/hour)
        if (timelineAccumulator != null && timelineAccumulator.hasData()) {
            report.set("timeline", generateTimelineJson(timelineAccumulator));
        }

        // Driver statistics
        if (driverStatsAccumulator != null && driverStatsAccumulator.hasDriverStats()) {
            report.set("driverStats", generateDriverStatsJson(driverStatsAccumulator));
        }

        // Write to file
        try (FileWriter writer = new FileWriter(fileName)) {
            mapper.writerWithDefaultPrettyPrinter().writeValue(writer, report);
        }
    }

    /** Dirty bytes and flow-control / write-concern waits; only written when an operation logged them. */
    private static void putWriteWaits(ObjectNode o, com.mongodb.log.parser.accumulator.WriteWaitStats w, long totalDurationMs) {
        if (w.getDirtyOps() > 0) {
            o.put("txnBytesDirtyOps", w.getDirtyOps());
            o.put("avgTxnBytesDirty", w.getAvgDirtyBytes());
            o.put("maxTxnBytesDirty", w.getMaxDirtyBytes());
            o.put("totalTxnBytesDirty", w.getTotalDirtyBytes());
        }
        if (w.getFlowControlOps() > 0) {
            o.put("flowControlOps", w.getFlowControlOps());
            o.put("totalFlowControlWaitMs", w.getTotalFlowControlMicros() / 1000);
            Double p = w.flowControlPercentOf(totalDurationMs);
            if (p != null) o.put("flowControlWaitPercentage", Math.round(p * 10.0) / 10.0);
        }
        if (w.getWriteConcernOps() > 0) {
            o.put("writeConcernOps", w.getWriteConcernOps());
            o.put("totalWriteConcernWaitMs", w.getTotalWriteConcernMillis());
            Double p = w.writeConcernPercentOf(totalDurationMs);
            if (p != null) o.put("writeConcernWaitPercentage", Math.round(p * 10.0) / 10.0);
        }
    }

    private static JsonNode generateMaintenanceJson(MaintenanceAccumulator m) {
        ObjectNode out = mapper.createObjectNode();
        ArrayNode builds = mapper.createArrayNode();
        for (MaintenanceAccumulator.IndexBuild b : m.getIndexBuilds()) {
            ObjectNode o = mapper.createObjectNode();
            o.put("buildUUID", b.getBuildUUID());
            o.put("node", b.getNode());
            o.put("namespace", b.getNamespace());
            ArrayNode ix = mapper.createArrayNode();
            b.getIndexes().forEach(ix::add);
            o.set("indexes", ix);
            o.put("startInLog", b.isStartSeen());
            o.put("firstSeen", b.getFirstSeen());
            o.put("firstMessage", b.getFirstMessage());
            o.put("lastSeen", b.getLastSeen());
            o.put("lastMessage", b.getLastMessage());
            o.put("outcome", b.getOutcome());
            if (b.getError() != null) o.put("error", b.getError());
            if (b.getTotalRecords() != null) o.put("totalRecords", b.getTotalRecords());
            if (b.getScanDurationMillis() != null) o.put("scanDurationMillis", b.getScanDurationMillis());
            if (b.getScanDoneAt() != null) o.put("scanDoneAt", b.getScanDoneAt());
            o.put("messages", b.getMessages());
            builds.add(o);
        }
        out.set("indexBuilds", builds);
        ArrayNode drains = mapper.createArrayNode();
        for (MaintenanceAccumulator.SideWriteDrains d : m.getSideWriteDrains()) {
            ObjectNode o = mapper.createObjectNode();
            o.put("namespace", d.getNamespace());
            o.put("index", d.getIndex());
            o.put("count", d.getCount());
            o.put("applied", d.getApplied());
            o.put("durationMillis", d.getDurationMillis());
            o.put("first", d.getFirst());
            o.put("last", d.getLast());
            drains.add(o);
        }
        out.set("indexBuildSideWriteDrains", drains);
        ObjectNode inline = mapper.createObjectNode();
        m.getInlineIndexes().forEach((ns, ix) -> {
            ObjectNode o = mapper.createObjectNode();
            ix.forEach(o::put);
            inline.set(ns, o);
        });
        out.set("inlineIndexCreations", inline);
        ArrayNode cl = mapper.createArrayNode();
        for (MaintenanceAccumulator.ChangelogGroup g : m.getChangelogGroups()) {
            ObjectNode o = mapper.createObjectNode();
            o.put("what", g.getWhat());
            o.put("namespace", g.getNamespace());
            o.put("from", g.getFrom());
            o.put("to", g.getTo());
            o.put("count", g.getCount());
            o.put("aborted", g.getAborted());
            o.put("clonedDocs", g.getClonedDocs());
            o.put("clonedBytes", g.getClonedBytes());
            o.put("first", g.getFirst());
            o.put("last", g.getLast());
            cl.add(o);
        }
        out.set("changelogEvents", cl);
        ObjectNode byHour = mapper.createObjectNode();
        m.getChangelogByHour().forEach((what, hours) -> {
            ObjectNode h = mapper.createObjectNode();
            hours.forEach(h::put);
            byHour.set(what, h);
        });
        out.set("changelogEventsByHour", byHour);
        ArrayNode errs = mapper.createArrayNode();
        m.getMigrationErrors().forEach((msg, n) -> {
            ObjectNode o = mapper.createObjectNode();
            o.put("error", msg);
            o.put("count", n[0]);
            errs.add(o);
        });
        out.set("migrationErrors", errs);
        out.put("unparsedLines", m.getUnparsedLines());
        return out;
    }

    private static JsonNode generateWriteKeysJson(WriteKeysAccumulator w) {
        ArrayNode arr = mapper.createArrayNode();
        for (WriteKeysAccumulator.Entry e : w.getEntries()) {
            ObjectNode o = mapper.createObjectNode();
            o.put("namespace", e.getNamespace());
            o.put("op", e.getOp());
            o.put("count", e.getCount());
            o.put("docsWritten", e.getDocsWritten());
            o.put("keysInserted", e.getKeysInserted());
            o.put("keysDeleted", e.getKeysDeleted());
            o.put("opsWithKeyCounts", e.getOpsWithKeyCounts());
            if (e.getKeysPerDocWritten() != null) o.put("keysPerDocWritten", e.getKeysPerDocWritten());
            o.put("totalDurationMs", e.getTotalDurationMs());
            arr.add(o);
        }
        return arr;
    }

    private static JsonNode generateCpuCoverageJson(CommentTagAccumulator.Coverage c) {
        ObjectNode node = mapper.createObjectNode();
        node.put("loggedCpuSec", round1(c.loggedCpuNanos / 1e9));
        node.put("linesWithCpuNanos", c.loggedCpuLines);
        node.put("linesWithUnparsedCpuNanos", c.unparsedCpuLines);
        node.put("analyzedOperationsCpuSec", round1(c.analyzedCpuNanos / 1e9));
        node.put("analyzedOperationsWithCpu", c.analyzedCpuCount);
        node.put("namespaceFilteredCpuSec", round1(c.namespaceFilteredCpuNanos / 1e9));
        node.put("namespaceFilteredOperationsWithCpu", c.namespaceFilteredCpuCount);
        node.put("otherUnanalyzedCpuSec", round1(c.otherUnanalyzedCpuNanos / 1e9));
        node.put("windowStart", c.windowStart);
        node.put("windowEnd", c.windowEnd);
        if (c.windowSeconds != null) {
            node.put("windowSec", round1(c.windowSeconds));
        } else {
            node.putNull("windowSec");
        }
        if (c.avgLoggedCores != null) {
            node.put("avgLoggedCores", Math.round(c.avgLoggedCores * 100.0) / 100.0);
        } else {
            node.putNull("avgLoggedCores");
        }
        if (c.numCoresFound.isEmpty()) {
            node.putNull("numCores");
            node.put("numCoresNote", "core count not in the log");
        } else if (c.numCores == null) {
            node.putNull("numCores");
            ArrayNode values = mapper.createArrayNode();
            c.numCoresFound.keySet().forEach(values::add);
            node.set("numCoresValuesFound", values);
            node.put("numCoresNote", "differing core counts in the log; capacity % not computed");
        } else {
            node.put("numCores", c.numCores);
        }
        if (c.percentOfCapacity != null) {
            node.put("percentOfCapacity", round1(c.percentOfCapacity));
        } else {
            node.putNull("percentOfCapacity");
        }
        node.put("note", "Logged CPU covers only operations slow enough to be logged; it is a lower bound on mongod CPU.");
        return node;
    }

    private static JsonNode generateCommentTagJson(CommentTagAccumulator acc) {
        ObjectNode node = mapper.createObjectNode();
        ObjectNode sources = mapper.createObjectNode();
        acc.getTagSources().forEach((k, v) -> {
            ObjectNode s = mapper.createObjectNode();
            s.put("count", v[0]);
            s.put("cpuSec", round1(v[1] / 1e9));
            sources.set(k, s);
        });
        node.set("tagSources", sources);
        node.set("byOriginUri", commentTagGroupsJson(acc.getByOrigin(), acc.getAnalyzedCpuNanos()));
        node.set("byCodeFqn", commentTagGroupsJson(acc.getByCodeFqn(), acc.getAnalyzedCpuNanos()));
        node.set("byOriginUriAndCodeFqn", commentTagGroupsJson(acc.getByOriginAndCodeFqn(), acc.getAnalyzedCpuNanos()));
        return node;
    }

    private static ArrayNode commentTagGroupsJson(java.util.List<CommentTagAccumulator.Group> groups, long totalCpuNanos) {
        ArrayNode list = mapper.createArrayNode();
        for (CommentTagAccumulator.Group g : groups) {
            ObjectNode n = mapper.createObjectNode();
            n.put("name", g.getName());
            n.put("count", g.getCount());
            n.put("totalDurationMs", g.getTotalDurationMs());
            putNullable(n, "avgDurationMs", g.getAvgDurationMs());
            if (g.getMaxDurationMs() != null) n.put("maxDurationMs", g.getMaxDurationMs()); else n.putNull("maxDurationMs");
            n.put("cpuCount", g.getCpuCount());
            if (g.getCpuCount() > 0) {
                n.put("totalCpuMs", round1(g.getTotalCpuNanos() / 1e6));
                n.put("avgCpuMs", round1(g.getAvgCpuNanos() / 1e6));
                n.put("maxCpuMs", round1(g.getMaxCpuNanos() / 1e6));
                n.put("cpuPercentOfAnalyzed", totalCpuNanos > 0 ? round1(g.getTotalCpuNanos() * 100.0 / totalCpuNanos) : null);
            } else {
                n.putNull("totalCpuMs");
                n.putNull("avgCpuMs");
                n.putNull("maxCpuMs");
                n.putNull("cpuPercentOfAnalyzed");
            }
            putNullable(n, "cpuPercentOfDuration", g.getCpuDurationPercent());
            list.add(n);
        }
        return list;
    }

    private static void putNullable(ObjectNode n, String field, Double value) {
        if (value == null) {
            n.putNull(field);
        } else {
            n.put(field, round1(value));
        }
    }

    private static JsonNode generateMainOperationsJson(Accumulator accumulator, boolean redactQueries) {
        ObjectNode mainOps = mapper.createObjectNode();
        
        // Summary statistics
        ObjectNode summary = mapper.createObjectNode();
        long totalOps = accumulator.getAccumulators().values().stream().mapToLong(LogLineAccumulator::getCount).sum();
        long totalDocsExamined = accumulator.getAccumulators().values().stream()
                .mapToLong(acc -> acc.getAvgDocsExamined() * acc.getCount()).sum();
        long totalDocsReturned = accumulator.getAccumulators().values().stream()
                .mapToLong(acc -> acc.getAvgReturned() * acc.getCount()).sum();
        
        summary.put("totalOperations", totalOps);
        summary.put("totalDocumentsExamined", totalDocsExamined);
        summary.put("totalDocumentsReturned", totalDocsReturned);
        summary.put("uniqueOperationPatterns", accumulator.getAccumulators().size());
        long mirroredOps = accumulator.getAccumulators().values().stream()
                .mapToLong(LogLineAccumulator::getMirroredCount).sum();
        summary.put("mirroredOperations", mirroredOps);
        long remoteWaitOps = accumulator.getAccumulators().values().stream()
                .mapToLong(LogLineAccumulator::getRemoteOpWaitCount).sum();
        if (remoteWaitOps > 0) {
            long totalRemoteWaitMs = accumulator.getAccumulators().values().stream()
                    .mapToLong(LogLineAccumulator::getTotalRemoteOpWaitMs).sum();
            long totalDurationMs = accumulator.getAccumulators().values().stream()
                    .mapToLong(acc -> acc.getCount() * acc.getAvg()).sum();
            summary.put("remoteOpWaitPercentage", round1(totalDurationMs > 0 ? totalRemoteWaitMs * 100.0 / totalDurationMs : 0.0));
        }
        long cpuOps = accumulator.getAccumulators().values().stream().mapToLong(LogLineAccumulator::getCpuCount).sum();
        long cpuNanos = accumulator.getAccumulators().values().stream().mapToLong(LogLineAccumulator::getTotalCpuNanos).sum();
        summary.put("cpuCount", cpuOps);
        if (cpuOps > 0) {
            summary.put("totalCpuMs", round1(cpuNanos / 1e6));
        } else {
            summary.putNull("totalCpuMs");
        }
        mainOps.set("summary", summary);

        // Individual operations
        ArrayNode operations = mapper.createArrayNode();
        accumulator.getAccumulators().entrySet().stream()
                .sorted(Map.Entry.comparingByValue(
                        (a, b) -> Long.compare(b.getCount(), a.getCount())))
                .forEach(entry -> {
                    LogLineAccumulator acc = entry.getValue();
                    ObjectNode op = mapper.createObjectNode();
                    
                    op.put("namespace", entry.getKey().getNamespace());
                    op.put("operation", acc.getOperation());
                    if (acc.getAppName() != null) {
                        op.put("appName", acc.getAppName());
                    }
                    op.put("count", acc.getCount());
                    if (acc.getMirroredCount() > 0) {
                        op.put("mirroredCount", acc.getMirroredCount());
                    }
                    if (acc.getAvgBatchSize() > 0) {
                        // Slow entries are whole batches: per-statement cost is avgDurationMs / avgBatchSize
                        op.put("avgBatchSize", acc.getAvgBatchSize());
                    }
                    op.put("minDurationMs", acc.getMin());
                    op.put("maxDurationMs", acc.getMax());
                    op.put("avgDurationMs", acc.getAvg());
                    op.put("p95DurationMs", Math.round(acc.getPercentile95()));
                    op.put("totalDurationSec", acc.getCount() * acc.getAvg() / 1000);
                    op.put("avgKeysExamined", acc.getAvgKeysExamined());
                    op.put("avgDocsExamined", acc.getAvgDocsExamined());
                    op.put("avgDocsReturned", acc.getAvgReturned());
                    op.put("examineToReturnRatio", acc.getScannedReturnRatio());
                    op.put("avgShards", round1(acc.getAvgShardsExact()));
                    if (acc.getShardedOpCount() > 0) {
                        op.put("multiShardPercentage", round1(acc.getMultiShardPercentage()));
                        op.put("maxShards", acc.getMaxShards());
                    }
                    if (acc.getRemoteOpWaitCount() > 0) {
                        op.put("avgRemoteOpWaitMs", acc.getAvgRemoteOpWaitMs());
                        op.put("remoteOpWaitPercentage", round1(acc.getRemoteOpWaitPercentage()));
                    }
                    if (acc.getMatchedCount() > 0) {
                        op.put("avgMatched", acc.getAvgMatched());
                        op.put("avgModified", acc.getAvgModified());
                        op.put("maxMatched", acc.getMaxMatched());
                    }
                    op.put("avgBytesRead", acc.getAvgBytesRead());
                    op.put("maxBytesRead", acc.getMaxBytesRead());
                    op.put("avgBytesWritten", acc.getAvgBytesWritten());
                    op.put("maxBytesWritten", acc.getMaxBytesWritten());
                    op.put("avgWriteConflicts", Math.round(acc.getAvgWriteConflicts() * 100.0) / 100.0);
                    op.put("totalWriteConflicts", acc.getTotalWriteConflicts());
                    putWriteWaits(op, acc.getWriteWaits(), acc.getTotalDurationMs());
                    // CPU over the entries that logged cpuNanos (cpuCount); null when none did
                    op.put("cpuCount", acc.getCpuCount());
                    if (acc.getCpuCount() > 0) {
                        op.put("totalCpuMs", round1(acc.getTotalCpuNanos() / 1e6));
                        op.put("avgCpuMs", round1(acc.getTotalCpuNanos() / 1e6 / acc.getCpuCount()));
                        op.put("maxCpuMs", round1(acc.getMaxCpuNanos() / 1e6));
                    } else {
                        op.putNull("totalCpuMs");
                        op.putNull("avgCpuMs");
                        op.putNull("maxCpuMs");
                    }
                    
                    // Add sample log message if available
                    if (acc.getSampleLogMessage() != null) {
                        String processedMessage = LogRedactionUtil.processLogMessage(acc.getSampleLogMessage(), redactQueries);
                        op.put("sampleLogMessage", processedMessage);
                        op.put("isTruncated", LogRedactionUtil.isLogMessageTruncated(acc.getSampleLogMessage()));
                    }
                    
                    operations.add(op);
                });
        
        mainOps.set("operations", operations);
        return mainOps;
    }

    private static JsonNode generateFindingsJson(QueryFindings findings, boolean redactQueries) {
        ObjectNode node = mapper.createObjectNode();
        ObjectNode summary = mapper.createObjectNode();
        summary.put("collectionScanShapes", findings.getCollectionScanCount());
        summary.put("minCollscanDocsExamined", QueryFindings.MIN_COLLSCAN_DOCS);
        summary.put("smallCollectionScanShapesNotFlagged", findings.getSmallCollectionScans());
        summary.put("oplogScanShapesNotFlagged", findings.getOplogScans());
        summary.put("highScanRatioShapes", findings.getHighScanRatioCount());
        summary.put("highScanRatioThreshold", QueryFindings.HIGH_RATIO);
        summary.put("minAvgExamined", QueryFindings.MIN_AVG_EXAMINED);
        summary.put("excludes", "mirrored reads");
        node.set("summary", summary);
        node.set("collectionScans", findingsArray(findings.getCollectionScans(), redactQueries));
        node.set("highScanRatio", findingsArray(findings.getHighScanRatio(), redactQueries));
        return node;
    }

    private static ArrayNode findingsArray(java.util.List<QueryFindings.Finding> list, boolean redactQueries) {
        ArrayNode array = mapper.createArrayNode();
        for (QueryFindings.Finding f : list) {
            QueryHashAccumulatorEntry e = f.entry;
            ObjectNode o = mapper.createObjectNode();
            o.put("reason", f.reason);
            o.put("queryHash", e.getKey().getQueryHash());
            o.put("namespace", e.getKey().getNamespace().toString());
            o.put("operation", e.getKey().getOperation());
            o.put("count", e.getCount());
            o.put("totalDurationSec", round1(e.getTotalDurationMs() / 1000.0));
            o.put("avgDurationMs", e.getAvg());
            o.put("p95DurationMs", Math.round(e.getPercentile95()));
            o.put("avgDocsExamined", e.getTotalDocsExamined() / e.getCount());
            o.put("avgKeysExamined", e.getTotalKeysExamined() / e.getCount());
            o.put("avgReturned", round1((double) e.getTotalReturned() / e.getCount()));
            o.put("planSummary", e.getPlanSummary());
            o.put("readPreference", e.getReadPreferenceSummary().replace("<br>", "; "));
            if (e.getAppName() != null) {
                o.put("appName", e.getAppName());
            }
            o.put("sanitizedQuery", e.getSanitizedQuery());
            ObjectNode nodes = mapper.createObjectNode();
            e.getNodeCounts().entrySet().stream()
                    .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                    .limit(10)
                    .forEach(n -> nodes.put(n.getKey(), n.getValue()));
            o.set("nodes", nodes);
            if (e.getSampleLogMessage() != null) {
                o.put("sampleLogMessage", LogRedactionUtil.processLogMessage(e.getSampleLogMessage(), redactQueries));
            }
            array.add(o);
        }
        return array;
    }

    private static JsonNode generateNodesJson(NodeStatsAccumulator nodeStats) {
        ArrayNode array = mapper.createArrayNode();
        for (NodeStatsAccumulator.NodeStats n : nodeStats.getNodes()) {
            ObjectNode o = mapper.createObjectNode();
            o.put("node", n.getIdentity().getNode());
            o.put("processType", n.getIdentity().getProcessType());
            o.put("shard", n.getIdentity().getShard());
            o.put("member", n.getIdentity().getMember());
            o.put("role", n.getRole());
            o.put("roleBasis", n.getRoleBasis());
            if (n.hasRoleChanges()) {
                ArrayNode transitions = mapper.createArrayNode();
                for (String[] t : n.getStateTransitions()) {
                    transitions.add(mapper.createObjectNode().put("time", t[0]).put("newState", t[1]));
                }
                o.set("stateTransitions", transitions);
            }
            o.put("slowOps", n.getCount());
            o.put("totalDurationSec", round1(n.getTotalDurationMs() / 1000.0));
            o.put("nonMirroredDurationSec", round1(n.getNonMirroredDurationMs() / 1000.0));
            o.put("mirroredOps", n.getMirroredCount());
            o.put("writeOps", n.getWriteCount());
            o.put("collscanOps", n.getCollscanCount());
            ObjectNode readPrefs = mapper.createObjectNode();
            n.getReadPreferenceCounts().forEach(readPrefs::put);
            o.set("readPreferenceCounts", readPrefs);
            o.set("topAppNamesSec", topSeconds(n.getAppNameDurationMs()));
            o.set("topNamespacesSec", topSeconds(n.getNamespaceDurationMs()));
            if (n.getMirroredCount() > 0) {
                o.set("topMirroredNamespacesSec", topSeconds(n.getMirroredNamespaceDurationMs()));
            }
            array.add(o);
        }
        return array;
    }

    private static ObjectNode topSeconds(Map<String, Long> durationMs) {
        ObjectNode o = mapper.createObjectNode();
        durationMs.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .limit(5)
                .forEach(e -> o.put(e.getKey(), round1(e.getValue() / 1000.0)));
        return o;
    }

    private static JsonNode generateBalanceJson(BalanceAnalyzer balance) {
        ObjectNode node = mapper.createObjectNode();
        ObjectNode thresholds = mapper.createObjectNode();
        thresholds.put("highRatio", BalanceAnalyzer.HIGH_RATIO);
        thresholds.put("lowRatio", BalanceAnalyzer.LOW_RATIO);
        thresholds.put("basis", "non-mirrored slow-operation time vs. group median");
        node.set("thresholds", thresholds);

        ArrayNode shards = mapper.createArrayNode();
        for (BalanceAnalyzer.ShardTotals s : balance.getShards()) {
            ObjectNode o = mapper.createObjectNode();
            o.put("shard", s.shard);
            o.put("slowOps", s.count);
            o.put("durationSec", round1(s.durationMs / 1000.0));
            o.set("topNamespacesSec", topSeconds(s.namespaceDurationMs));
            shards.add(o);
        }
        node.set("shards", shards);

        ArrayNode flags = mapper.createArrayNode();
        for (BalanceAnalyzer.Flag f : balance.getFlags()) {
            ObjectNode o = mapper.createObjectNode();
            o.put("scope", f.scope);
            o.put("group", f.group);
            o.put("subject", f.subject);
            o.put("direction", f.direction);
            o.put("durationSec", round1(f.durationMs / 1000.0));
            o.put("groupMedianSec", round1(f.groupMedianMs / 1000.0));
            o.put("ratioToMedian", round1(f.ratio));
            ArrayNode top = mapper.createArrayNode();
            for (BalanceAnalyzer.NamespaceShare ns : f.topNamespaces) {
                top.add(mapper.createObjectNode().put("namespace", ns.namespace)
                        .put("durationSec", round1(ns.durationMs / 1000.0)).put("shardsInSlowLogs", ns.shardsInSlowLogs));
            }
            o.set("topNamespaces", top);
            if (f.likelyCause != null) {
                o.put("likelyCause", f.likelyCause);
            }
            flags.add(o);
        }
        node.set("flags", flags);
        return node;
    }

    private static double round1(double value) {
        return Math.round(value * 10.0) / 10.0;
    }

    private static JsonNode generateTimelineJson(TimelineAccumulator timeline) {
        ObjectNode timelineNode = mapper.createObjectNode();

        ObjectNode summary = mapper.createObjectNode();
        summary.put("bucket", "1 minute, UTC");
        summary.put("minutesWithSlowOps", timeline.getMinuteCount());
        summary.put("opsMissingTimestamp", timeline.getMissingTimestamp());
        summary.put("opsUnparseableTimestamp", timeline.getUnparseableTimestamp());
        timelineNode.set("summary", summary);

        ArrayNode hourly = mapper.createArrayNode();
        for (TimelineAccumulator.Bucket bucket : timeline.getHourly()) {
            hourly.add(timelineBucketJson("hour", bucket));
        }
        timelineNode.set("hourly", hourly);

        ArrayNode topMinutes = mapper.createArrayNode();
        for (TimelineAccumulator.Bucket bucket : timeline.getTopMinutes(25)) {
            ObjectNode minute = timelineBucketJson("minute", bucket);
            // Who drove the minute (non-mirrored slow time)
            ArrayNode nodes = mapper.createArrayNode();
            for (Map.Entry<String, long[]> n : bucket.getTopNodes(10)) {
                nodes.add(mapper.createObjectNode().put("node", n.getKey()).put("count", n.getValue()[0])
                        .put("durationSec", round1(n.getValue()[1] / 1000.0)));
            }
            minute.set("topNodes", nodes);
            ObjectNode namespaces = mapper.createObjectNode();
            for (Map.Entry<String, Long> ns : bucket.getTopNamespaces(5)) {
                namespaces.put(ns.getKey(), round1(ns.getValue() / 1000.0));
            }
            minute.set("topNamespacesSec", namespaces);
            topMinutes.add(minute);
        }
        timelineNode.set("topMinutes", topMinutes);

        return timelineNode;
    }

    private static ObjectNode timelineBucketJson(String startField, TimelineAccumulator.Bucket bucket) {
        ObjectNode node = mapper.createObjectNode();
        node.put(startField, bucket.getStart());
        node.put("count", bucket.getCount());
        node.put("totalDurationSec", round1(bucket.getTotalDurationMs() / 1000.0));
        node.put("maxDurationMs", bucket.getMaxDurationMs());
        node.put("remoteOpWaitPercentage", round1(bucket.getRemoteOpWaitPercentage()));
        node.put("distinctNamespaces", bucket.getDistinctNamespaces());
        return node;
    }

    private static JsonNode generateTtlOperationsJson(Accumulator ttlAccumulator) {
        ObjectNode ttlOps = mapper.createObjectNode();
        
        // TTL summary
        ObjectNode summary = mapper.createObjectNode();
        long totalTtlOps = ttlAccumulator.getAccumulators().values().stream().mapToLong(LogLineAccumulator::getCount).sum();
        long totalDeletedDocs = ttlAccumulator.getAccumulators().values().stream()
                .mapToLong(acc -> acc.getAvgReturned() * acc.getCount()).sum();
        
        summary.put("totalTtlOperations", totalTtlOps);
        summary.put("totalDocumentsDeleted", totalDeletedDocs);
        ttlOps.set("summary", summary);

        // Individual TTL operations
        ArrayNode operations = mapper.createArrayNode();
        ttlAccumulator.getAccumulators().entrySet().stream()
                .sorted(Map.Entry.comparingByValue(
                        (a, b) -> Long.compare(b.getCount(), a.getCount())))
                .forEach(entry -> {
                    LogLineAccumulator acc = entry.getValue();
                    ObjectNode op = mapper.createObjectNode();
                    
                    op.put("namespace", entry.getKey().getNamespace());
                    op.put("count", acc.getCount());
                    op.put("avgDurationMs", acc.getAvg());
                    op.put("maxDurationMs", acc.getMax());
                    op.put("avgDocsDeleted", acc.getAvgReturned());
                    
                    operations.add(op);
                });
        
        ttlOps.set("operations", operations);
        return ttlOps;
    }

    private static JsonNode generateOperationStatsJson(Map<String, java.util.concurrent.atomic.AtomicLong> operationTypeStats) {
        ObjectNode stats = mapper.createObjectNode();
        
        long totalOps = operationTypeStats.values().stream().mapToLong(val -> val.get()).sum();
        stats.put("totalOperations", totalOps);
        
        ObjectNode breakdown = mapper.createObjectNode();
        operationTypeStats.entrySet().stream()
                .sorted(Map.Entry.<String, java.util.concurrent.atomic.AtomicLong>comparingByValue(
                        (a, b) -> Long.compare(b.get(), a.get())))
                .forEach(entry -> {
                    ObjectNode opType = mapper.createObjectNode();
                    long count = entry.getValue().get();
                    opType.put("count", count);
                    opType.put("percentage", totalOps > 0 ? (count * 100.0 / totalOps) : 0.0);
                    breakdown.set(entry.getKey(), opType);
                });
        
        stats.set("breakdown", breakdown);
        return stats;
    }

    private static JsonNode generateErrorCodesJson(ErrorCodeAccumulator errorCodeAccumulator, boolean redactQueries) {
        ObjectNode errors = mapper.createObjectNode();
        
        // Summary
        ObjectNode summary = mapper.createObjectNode();
        long totalErrors = errorCodeAccumulator.getTotalErrorCount();
        summary.put("totalErrors", totalErrors);
        summary.put("uniqueErrorCodes", errorCodeAccumulator.getErrorCodeEntries().size());
        errors.set("summary", summary);

        // Individual error codes
        ArrayNode errorList = mapper.createArrayNode();
        errorCodeAccumulator.getErrorCodeEntries().values().stream()
                .sorted((a, b) -> Long.compare(b.getCount(), a.getCount()))
                .forEach(entry -> {
                    ObjectNode error = mapper.createObjectNode();
                    error.put("errorCode", entry.getErrorCode() != null ? entry.getErrorCode().toString() : "unknown");
                    error.put("codeName", entry.getCodeName());
                    error.put("count", entry.getCount());
                    double percentage = totalErrors > 0 ? (entry.getCount() * 100.0 / totalErrors) : 0.0;
                    error.put("percentage", Math.round(percentage * 10.0) / 10.0); // Round to 1 decimal place
                    error.put("sampleErrorMessage", entry.getSampleErrorMessage());
                    errorList.add(error);
                });
        
        errors.set("errorCodes", errorList);
        return errors;
    }

    private static JsonNode generateQueryHashJson(QueryHashAccumulator queryHashAccumulator, boolean redactQueries) {
        ObjectNode queryHash = mapper.createObjectNode();
        
        // Summary  
        ObjectNode summary = mapper.createObjectNode();
        long totalQueries = queryHashAccumulator.getQueryHashEntries().values().stream().mapToLong(entry -> entry.getCount()).sum();
        long totalScanAndOrder = queryHashAccumulator.getQueryHashEntries().values().stream().mapToLong(entry -> entry.getScanAndOrderCount()).sum();
        double scanAndOrderPercentage = totalQueries > 0 ? (totalScanAndOrder * 100.0) / totalQueries : 0.0;
        summary.put("totalQueries", totalQueries);
        summary.put("uniqueQueryHashes", queryHashAccumulator.getQueryHashEntries().size());
        summary.put("scanAndOrderQueries", totalScanAndOrder);
        summary.put("scanAndOrderPercentage", Math.round(scanAndOrderPercentage * 10.0) / 10.0);
        queryHash.set("summary", summary);

        // Individual query hashes
        ArrayNode queries = mapper.createArrayNode();
        queryHashAccumulator.getQueryHashEntries().values().stream()
                .sorted((a, b) -> Long.compare(b.getCount(), a.getCount()))
                .limit(100) // Limit to top 100 for JSON size
                .forEach(entry -> {
                    ObjectNode query = mapper.createObjectNode();
                    query.put("queryHash", entry.getKey().getQueryHash());
                    query.put("namespace", entry.getKey().getNamespace().toString());
                    query.put("operation", entry.getKey().getOperation());
                    query.put("count", entry.getCount());
                    query.put("minDurationMs", entry.getMin());
                    query.put("maxDurationMs", entry.getMax());
                    query.put("avgDurationMs", entry.getAvg());
                    query.put("p95DurationMs", Math.round(entry.getPercentile95()));
                    query.put("totalDurationSec", entry.getCount() * entry.getAvg() / 1000);
                    query.put("avgKeysExamined", entry.getAvgKeysExamined());
                    query.put("avgDocsExamined", entry.getAvgDocsExamined());
                    query.put("avgDocsReturned", entry.getAvgReturned());
                    query.put("examinedReturnedRatio", entry.getScannedReturnRatio());
                    query.put("avgShards", entry.getAvgShards());
                    query.put("avgBytesRead", entry.getAvgBytesRead());
                    query.put("maxBytesRead", entry.getMaxBytesRead());
                    query.put("avgBytesWritten", entry.getAvgBytesWritten());
                    query.put("maxBytesWritten", entry.getMaxBytesWritten());
                    putWriteWaits(query, entry.getWriteWaits(), entry.getTotalDurationMs());
                    query.put("readPreference", entry.getReadPreferenceSummary());
                    query.put("readPreferenceTags", entry.getReadPreferenceTagsSummary());
                    query.put("planSummary", entry.getPlanSummary());
                    query.put("avgPlanningTimeMs", entry.getAvgPlanningTimeMs());
                    query.put("replannedPercentage", entry.getReplannedPercentage());
                    query.put("scanAndOrderPercentage", entry.getScanAndOrderPercentage());
                    query.put("sanitizedQuery", entry.getSanitizedQuery());
                    
                    if (entry.getSampleLogMessage() != null) {
                        String processedMessage = LogRedactionUtil.processLogMessage(entry.getSampleLogMessage(), redactQueries);
                        query.put("sampleLogMessage", processedMessage);
                    }
                    
                    queries.add(query);
                });
        
        queryHash.set("queries", queries);
        return queryHash;
    }

    private static JsonNode generateTransactionJson(TransactionAccumulator transactionAccumulator) {
        ObjectNode transactions = mapper.createObjectNode();
        
        // Summary - simplified to avoid missing method issues
        ObjectNode summary = mapper.createObjectNode();
        summary.put("hasTransactions", true);
        transactions.set("summary", summary);

        // Transaction patterns could be added here if available in the accumulator
        return transactions;
    }

    private static JsonNode generateIndexStatsJson(IndexStatsAccumulator indexStatsAccumulator, boolean redactQueries) {
        ObjectNode indexStats = mapper.createObjectNode();
        
        // Summary with actual statistics
        ObjectNode summary = mapper.createObjectNode();
        long totalOps = indexStatsAccumulator.getTotalOperations();
        long collScans = indexStatsAccumulator.getCollectionScanOperations();
        double collScanPercentage = totalOps > 0 ? (collScans * 100.0 / totalOps) : 0.0;
        
        summary.put("totalOperations", totalOps);
        summary.put("uniqueIndexUsagePatterns", indexStatsAccumulator.getUniqueIndexUsagePatterns());
        summary.put("collectionScans", collScans);
        summary.put("collectionScanPercentage", Math.round(collScanPercentage * 10.0) / 10.0);
        indexStats.set("summary", summary);

        // Individual index usage patterns
        ArrayNode indexUsageList = mapper.createArrayNode();
        indexStatsAccumulator.getIndexStatsEntries().values().stream()
                .sorted((a, b) -> Long.compare(b.getCount(), a.getCount()))
                .forEach(entry -> {
                    ObjectNode indexUsage = mapper.createObjectNode();
                    indexUsage.put("namespace", entry.getNamespace().toString());
                    indexUsage.put("planSummary", entry.getPlanSummary());
                    indexUsage.put("count", entry.getCount());
                    indexUsage.put("minDurationMs", entry.getMinDurationMs());
                    indexUsage.put("maxDurationMs", entry.getMaxDurationMs());
                    indexUsage.put("avgDurationMs", entry.getAvgDurationMs());
                    indexUsage.put("p95DurationMs", Math.round(entry.getPercentile95()));
                    indexUsage.put("totalDurationSec", entry.getTotalDurationSec());
                    indexUsage.put("avgKeysExamined", entry.getAvgKeysExamined());
                    indexUsage.put("avgDocsExamined", entry.getAvgDocsExamined());
                    indexUsage.put("avgReturned", entry.getAvgReturned());
                    indexUsage.put("examinedReturnedRatio", entry.getExaminedToReturnedRatio());
                    indexUsage.put("isCollectionScan", entry.isCollectionScan());
                    indexUsageList.add(indexUsage);
                });
        
        indexStats.set("indexUsage", indexUsageList);
        return indexStats;
    }

    private static JsonNode generateDriverStatsJson(TwoPassDriverStatsAccumulator driverStatsAccumulator) {
        ObjectNode driverStats = mapper.createObjectNode();
        
        // Summary with actual statistics
        ObjectNode summary = mapper.createObjectNode();
        summary.put("totalConnections", driverStatsAccumulator.getTotalConnections());
        summary.put("uniqueDriverVersionCombinations", driverStatsAccumulator.getUniqueDriverVersionCombinations());
        summary.put("uniqueDrivers", driverStatsAccumulator.getUniqueDrivers());
        driverStats.set("summary", summary);

        // Individual driver statistics
        ArrayNode driverList = mapper.createArrayNode();
        driverStatsAccumulator.getDriverStatsEntries().values().stream()
                .sorted((a, b) -> Long.compare(b.getConnectionCount(), a.getConnectionCount()))
                .forEach(entry -> {
                    ObjectNode driver = mapper.createObjectNode();
                    driver.put("driverName", entry.getDriverName());
                    driver.put("driverVersion", entry.getDriverVersion());
                    driver.put("compressors", entry.getCompressorsString());
                    driver.put("osType", entry.getOsType());
                    driver.put("osName", entry.getOsName());
                    driver.put("platform", entry.getPlatform());
                    driver.put("serverVersion", entry.getServerVersion());
                    driver.put("connectionCount", entry.getConnectionCount());
                    driver.put("uniqueHosts", entry.getUniqueHosts());
                    driverList.add(driver);
                });
        
        driverStats.set("drivers", driverList);
        return driverStats;
    }
}