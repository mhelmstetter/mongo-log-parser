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
            Map<String, java.util.concurrent.atomic.AtomicLong> operationTypeStats, boolean redactQueries,
            String earliestTimestamp, String latestTimestamp) throws IOException {

        ObjectNode report = mapper.createObjectNode();
        
        // Report metadata
        ObjectNode metadata = mapper.createObjectNode();
        metadata.put("generatedAt", java.time.Instant.now().toString());
        metadata.put("earliestTimestamp", earliestTimestamp);
        metadata.put("latestTimestamp", latestTimestamp);
        metadata.put("redactedQueries", redactQueries);
        report.set("metadata", metadata);

        // Main operations analysis
        report.set("mainOperations", generateMainOperationsJson(accumulator, redactQueries));

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

        // Driver statistics
        if (driverStatsAccumulator != null && driverStatsAccumulator.hasDriverStats()) {
            report.set("driverStats", generateDriverStatsJson(driverStatsAccumulator));
        }

        // Write to file
        try (FileWriter writer = new FileWriter(fileName)) {
            mapper.writerWithDefaultPrettyPrinter().writeValue(writer, report);
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
                    op.put("count", acc.getCount());
                    op.put("minDurationMs", acc.getMin());
                    op.put("maxDurationMs", acc.getMax());
                    op.put("avgDurationMs", acc.getAvg());
                    op.put("p95DurationMs", Math.round(acc.getPercentile95()));
                    op.put("totalDurationSec", acc.getCount() * acc.getAvg() / 1000);
                    op.put("avgKeysExamined", acc.getAvgKeysExamined());
                    op.put("avgDocsExamined", acc.getAvgDocsExamined());
                    op.put("avgDocsReturned", acc.getAvgReturned());
                    op.put("examineToReturnRatio", acc.getScannedReturnRatio());
                    op.put("avgShards", acc.getAvgShards());
                    op.put("avgBytesRead", acc.getAvgBytesRead());
                    op.put("maxBytesRead", acc.getMaxBytesRead());
                    op.put("avgBytesWritten", acc.getAvgBytesWritten());
                    op.put("maxBytesWritten", acc.getMaxBytesWritten());
                    op.put("avgWriteConflicts", acc.getAvgWriteConflicts());
                    
                    // Add sample log message if available
                    if (acc.getSampleLogMessage() != null) {
                        String processedMessage = LogRedactionUtil.processLogMessage(acc.getSampleLogMessage(), redactQueries);
                        op.put("sampleLogMessage", processedMessage);
                        op.put("isTruncated", LogRedactionUtil.isLogMessageTruncated(acc.getSampleLogMessage()));
                        op.put("querySource", LogRedactionUtil.detectQuerySource(acc.getSampleLogMessage()));
                    }
                    
                    operations.add(op);
                });
        
        mainOps.set("operations", operations);
        return mainOps;
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
        summary.put("totalQueries", totalQueries);
        summary.put("uniqueQueryHashes", queryHashAccumulator.getQueryHashEntries().size());
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
                    query.put("readPreference", entry.getReadPreferenceSummary());
                    query.put("readPreferenceTags", entry.getReadPreferenceTagsSummary());
                    query.put("planSummary", entry.getPlanSummary());
                    query.put("avgPlanningTimeMs", entry.getAvgPlanningTimeMs());
                    query.put("replannedPercentage", entry.getReplannedPercentage());
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