package com.mongodb.log.parser;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.mongodb.log.parser.accumulator.Accumulator;
import com.mongodb.log.parser.accumulator.AppNameConnectionStatsAccumulator;
import com.mongodb.log.parser.accumulator.TwoPassDriverStatsAccumulator;
import com.mongodb.log.parser.accumulator.DriverStatsEntry;
import com.mongodb.log.parser.LogRedactionUtil;
import com.mongodb.log.parser.accumulator.ErrorCodeAccumulator;
import com.mongodb.log.parser.accumulator.IndexStatsAccumulator;
import com.mongodb.log.parser.accumulator.IndexStatsEntry;
import com.mongodb.log.parser.accumulator.PlanCacheAccumulator;
import com.mongodb.log.parser.accumulator.PlanCacheAccumulatorEntry;
import com.mongodb.log.parser.accumulator.PlanCacheKey;
import com.mongodb.log.parser.accumulator.QueryHashAccumulator;
import com.mongodb.log.parser.accumulator.QueryHashAccumulatorEntry;
import com.mongodb.log.parser.accumulator.QueryHashKey;
import com.mongodb.log.parser.accumulator.SlowPlanningAccumulator;
import com.mongodb.log.parser.accumulator.TransactionAccumulator;
import com.mongodb.log.parser.model.MainOperationEntry;
import com.mongodb.log.parser.service.MainOperationService;

/**
 * Generates interactive HTML reports with sortable and filterable tables
 */
public class HtmlReportGenerator {

	private static final NumberFormat NUMBER_FORMAT = NumberFormat.getNumberInstance(Locale.US);

	// New overloaded method with shard tracking support
	public static void generateReport(String fileName, Accumulator accumulator, Accumulator ttlAccumulator,
	        PlanCacheAccumulator planCacheAccumulator,
	        QueryHashAccumulator queryHashAccumulator,
	        SlowPlanningAccumulator slowPlanningAccumulator,
	        ErrorCodeAccumulator errorCodeAccumulator,
	        TransactionAccumulator transactionAccumulator,
	        IndexStatsAccumulator indexStatsAccumulator,
	        TwoPassDriverStatsAccumulator driverStatsAccumulator,
	        AppNameConnectionStatsAccumulator appNameConnectionStatsAccumulator,
	        Map<String, java.util.concurrent.atomic.AtomicLong> operationTypeStats, boolean redactQueries,
	        String earliestTimestamp, String latestTimestamp,
	        boolean enableShardTracking,
	        Map<ShardInfo, Accumulator> shardAccumulators,
	        Map<ShardInfo, Accumulator> shardTtlAccumulators,
	        Map<ShardInfo, PlanCacheAccumulator> shardPlanCacheAccumulators,
	        Map<ShardInfo, QueryHashAccumulator> shardQueryHashAccumulators,
	        Map<ShardInfo, ErrorCodeAccumulator> shardErrorCodeAccumulators,
	        Map<ShardInfo, TransactionAccumulator> shardTransactionAccumulators,
	        Map<ShardInfo, IndexStatsAccumulator> shardIndexStatsAccumulators) throws IOException {
	        
	    try (PrintWriter writer = new PrintWriter(new FileWriter(fileName))) {
	        writeHtmlHeader(writer);
	        writeNavigationHeader(writer, accumulator, ttlAccumulator, planCacheAccumulator,
	                             queryHashAccumulator, slowPlanningAccumulator, errorCodeAccumulator, transactionAccumulator,
	                             indexStatsAccumulator, driverStatsAccumulator, appNameConnectionStatsAccumulator, operationTypeStats, earliestTimestamp, latestTimestamp);
	        
	        // Write tables with shard column if shard tracking is enabled
	        if (enableShardTracking && !shardAccumulators.isEmpty()) {
	            writeMainOperationsTableWithShards(writer, shardAccumulators, redactQueries);
	            writeTtlOperationsTableWithShards(writer, shardTtlAccumulators);
	            writeOperationStatsTable(writer, operationTypeStats);
	            writeErrorCodesTableWithShards(writer, shardErrorCodeAccumulators, redactQueries);

	            if (!shardQueryHashAccumulators.isEmpty()) {
	                writeQueryHashTableWithShards(writer, shardQueryHashAccumulators, redactQueries);
	            }
	            
	            if (!shardTransactionAccumulators.isEmpty()) {
	                writeTransactionTableWithShards(writer, shardTransactionAccumulators);
	            }

	            if (!shardIndexStatsAccumulators.isEmpty()) {
	                writeIndexStatsTableWithShards(writer, shardIndexStatsAccumulators, redactQueries);
	            }
	        } else {
	            // Write regular tables (aggregated across all files)
	            writeMainOperationsTable(writer, accumulator, redactQueries);
	            writeTtlOperationsTable(writer, ttlAccumulator);
	            writeOperationStatsTable(writer, operationTypeStats);
	            writeErrorCodesTable(writer, errorCodeAccumulator, redactQueries);

	            if (queryHashAccumulator != null) {
	                writeQueryHashTable(writer, queryHashAccumulator, redactQueries);
	            }

	            if (slowPlanningAccumulator != null) {
	                writeSlowPlanningTable(writer, slowPlanningAccumulator, redactQueries);
	            }

	            if (transactionAccumulator != null && transactionAccumulator.hasTransactions()) {
	                writeTransactionTable(writer, transactionAccumulator);
	            }

	            if (indexStatsAccumulator != null && indexStatsAccumulator.hasIndexStats()) {
	                writeIndexStatsTable(writer, indexStatsAccumulator, redactQueries);
	            }
	        }

	        // AppName connection stats table
	        if (appNameConnectionStatsAccumulator != null && appNameConnectionStatsAccumulator.getAppNameCount() > 0) {
	            writeAppNameStatsTable(writer, appNameConnectionStatsAccumulator);
	        }

	        // Driver stats table is not sharded
	        if (driverStatsAccumulator != null && driverStatsAccumulator.hasDriverStats()) {
	            writeDriverStatsTable(writer, driverStatsAccumulator);
	        }

	        writeHtmlFooter(writer);

	        // Check for any I/O errors that occurred during writing
	        if (writer.checkError()) {
	            throw new IOException("Error writing HTML report to " + fileName +
	                                 " - possible disk full, I/O error, or out of memory");
	        }
	    }
	}

	// Original method for backwards compatibility
	public static void generateReport(String fileName, Accumulator accumulator, Accumulator ttlAccumulator,
	        PlanCacheAccumulator planCacheAccumulator,
	        QueryHashAccumulator queryHashAccumulator,
	        SlowPlanningAccumulator slowPlanningAccumulator,
	        ErrorCodeAccumulator errorCodeAccumulator,
	        TransactionAccumulator transactionAccumulator,
	        IndexStatsAccumulator indexStatsAccumulator,
	        TwoPassDriverStatsAccumulator driverStatsAccumulator,
	        AppNameConnectionStatsAccumulator appNameConnectionStatsAccumulator,
	        Map<String, java.util.concurrent.atomic.AtomicLong> operationTypeStats, boolean redactQueries,
	        String earliestTimestamp, String latestTimestamp) throws IOException {

	    try (PrintWriter writer = new PrintWriter(new FileWriter(fileName))) {
	        writeHtmlHeader(writer);
	        writeNavigationHeader(writer, accumulator, ttlAccumulator, planCacheAccumulator,
	                             queryHashAccumulator, slowPlanningAccumulator, errorCodeAccumulator, transactionAccumulator,
	                             indexStatsAccumulator, driverStatsAccumulator, appNameConnectionStatsAccumulator, operationTypeStats, earliestTimestamp, latestTimestamp);
	        writeMainOperationsTable(writer, accumulator, redactQueries);
	        writeTtlOperationsTable(writer, ttlAccumulator);
	        writeOperationStatsTable(writer, operationTypeStats);
	        writeErrorCodesTable(writer, errorCodeAccumulator, redactQueries);

	        if (queryHashAccumulator != null) {
	            writeQueryHashTable(writer, queryHashAccumulator, redactQueries);
	        }

	        if (slowPlanningAccumulator != null) {
	            writeSlowPlanningTable(writer, slowPlanningAccumulator, redactQueries);
	        }

	        if (transactionAccumulator != null && transactionAccumulator.hasTransactions()) {
	            writeTransactionTable(writer, transactionAccumulator);
	        }

	        if (indexStatsAccumulator != null && indexStatsAccumulator.hasIndexStats()) {
	            writeIndexStatsTable(writer, indexStatsAccumulator, redactQueries);
	        }

	        if (appNameConnectionStatsAccumulator != null && appNameConnectionStatsAccumulator.getAppNameCount() > 0) {
	            writeAppNameStatsTable(writer, appNameConnectionStatsAccumulator);
	        }

	        if (driverStatsAccumulator != null && driverStatsAccumulator.hasDriverStats()) {
	            writeDriverStatsTable(writer, driverStatsAccumulator);
	        }

	        writeHtmlFooter(writer);

	        // Check for any I/O errors that occurred during writing
	        if (writer.checkError()) {
	            throw new IOException("Error writing HTML report to " + fileName +
	                                 " - possible disk full, I/O error, or out of memory");
	        }
	    }
	}

	private static void writeHtmlHeader(PrintWriter writer) {
		writer.println("<!DOCTYPE html>");
		writer.println("<html lang=\"en\">");
		writer.println("<head>");
		writer.println("    <meta charset=\"UTF-8\">");
		writer.println("    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">");
		writer.println("    <title>MongoDB Log Analysis Report</title>");
		writer.println("    <style>");
		writer.println("        html { height: 100%; }");
		writer.println(
				"        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; background-color: #f8f9fa; height: 100%; }");
		writer.println("        .container { max-width: 95%; margin: 0 auto; padding: 20px; }");
		writer.println("        h1 { color: #001E2B; text-align: center; margin-bottom: 10px; }");
		writer.println(
				"        h2 { color: #001E2B; margin-top: 40px; margin-bottom: 20px; border-bottom: 2px solid #00684A; padding-bottom: 5px; scroll-margin-top: 70px; }");
		writer.println("        .table-container { margin-bottom: 40px; }");
		writer.println("        .controls { margin-bottom: 15px; }");
		writer.println(
				"        .filter-input { padding: 8px; border: 1px solid #b8c4c2; border-radius: 4px; margin-right: 10px; width: 200px; }");
		writer.println(
				"        .clear-btn { padding: 8px 12px; background-color: #5d6c74; color: white; border: none; border-radius: 4px; cursor: pointer; }");
		writer.println("        .clear-btn:hover { background-color: #21313C; }");
		writer.println(
				"        .expand-btn { padding: 8px 12px; background-color: #00684A; color: white; border: none; border-radius: 4px; cursor: pointer; margin-left: 10px; }");
		writer.println("        .expand-btn:hover { background-color: #006CFA; }");
		writer.println(
				"        .collapse-btn { padding: 8px 12px; background-color: #00684A; color: white; border: none; border-radius: 4px; cursor: pointer; margin-left: 5px; }");
		writer.println("        .collapse-btn:hover { background-color: #006CFA; }");
		writer.println("        table { width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 14px; table-layout: auto; }");
		writer.println("        th, td { border: 1px solid #b8c4c2; padding: 8px; text-align: left; }");
		writer.println(
				"        thead { background-color: #00684A; color: white; }");
		writer.println(
				"        thead th { position: sticky; top: 0; background-color: #00684A; z-index: 10; cursor: pointer; user-select: none; font-weight: bold; white-space: normal; word-wrap: break-word; min-width: 80px; }");
		writer.println("        th:hover { background-color: #001E2B; }");
		writer.println("        th.sortable::after { content: ' ↕'; font-size: 12px; opacity: 0.5; }");
		writer.println("        th.sort-asc::after { content: ' ↑'; opacity: 1; }");
		writer.println("        th.sort-desc::after { content: ' ↓'; opacity: 1; }");
		writer.println("        tbody { display: table-row-group; }");
		writer.println("        tbody tr:first-child td { padding-top: 12px; }");
		writer.println("        tr:nth-child(even) { background-color: #f8f9fa; }");
		writer.println("        tr:hover { background-color: #E9FF99; }");
		writer.println("        .number { text-align: right; }");
		writer.println("        .highlight { background-color: #E9FF99 !important; }");
		writer.println("        .namespace-cell { max-width: 250px; word-wrap: break-word; white-space: normal; }");
		writer.println("        .shard-cell { text-align: center; font-weight: bold; min-width: 60px; }");
		writer.println("        .operation-cell { min-width: 80px; }");
		writer.println("        .count-cell { text-align: right; min-width: 70px; }");
		writer.println("        .time-cell { text-align: right; min-width: 60px; }");
		writer.println("        .bytes-cell { text-align: right; min-width: 80px; }");
		writer.println(
				"        .summary { background-color: #f0f7f4; padding: 15px; border-radius: 5px; margin-bottom: 20px; border-left: 4px solid #00684A; }");
		writer.println("        .summary h3 { margin-top: 0; color: #00684A; }");
		writer.println(
				"        .summary-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 10px; }");
		writer.println(
				"        .summary-item { background-color: white; padding: 10px; border-radius: 4px; border-left: 4px solid #00684A; }");
		writer.println("        .summary-label { font-weight: bold; color: #21313C; }");
		writer.println("        .summary-value { font-size: 18px; color: #00684A; }");
		writer.println("        .collscan { background-color: #ffebee !important; }");
		writer.println("        .mirrored { color: #999999 !important; }");
		writer.println(
				"        .truncated { max-width: 300px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; cursor: help; }");
		writer.println(
				"        .multiline { white-space: normal !important; }");
		writer.println(
				"        .wrapped { max-width: 400px; word-wrap: break-word; white-space: normal; cursor: help; }");
		
		// Navigation styles
		writer.println("        .nav-header { background-color: #001E2B; color: white; padding: 15px; margin-bottom: 30px; border-radius: 8px; }");
		writer.println("        .nav-content { max-width: 95%; margin: 0 auto; padding: 0 20px; }");
		writer.println("        .nav-title { margin: 0 0 10px 0; font-size: 1.2em; font-weight: bold; }");
		writer.println("        .nav-links { display: flex; flex-wrap: wrap; gap: 10px; }");
		writer.println("        .nav-link { color: #ffffff; text-decoration: none; padding: 8px 12px; border-radius: 4px; background-color: #00684A; transition: background-color 0.2s; font-size: 0.9em; }");
		writer.println("        .nav-link:hover { background-color: #006CFA; color: white; }");
		writer.println("        .nav-link.active { background-color: #00ED64; color: #001E2B; }");
		writer.println("        .report-info { color: #b8c4c2; font-size: 0.85em; margin-top: 5px; }");
		
		// Accordion styles
		writer.println("        .accordion-row { cursor: pointer; }");
		writer.println("        .accordion-row:hover { background-color: #E9FF99 !important; }");
		writer.println("        .accordion-content { display: none; }");
		writer.println("        .accordion-content.open { display: table-row; }");
		writer.println("        .accordion-content td { max-width: 0; overflow: hidden; }");
		writer.println("        .log-sample { background-color: #f8f9fa; padding: 10px; font-family: 'Courier New', monospace; font-size: 12px; word-wrap: break-word; border-left: 4px solid #00684A; max-height: 300px; overflow-y: auto; }");
		writer.println("        .log-sample.truncated-query { background-color: #ffebee; border-left: 4px solid #d32f2f; }");
		writer.println("        .truncated-warning { color: #d32f2f; font-weight: bold; margin-bottom: 8px; }");
		writer.println("        .accordion-toggle { font-size: 12px; opacity: 0.7; margin-right: 8px; display: inline-block; }");
		writer.println("        .accordion-toggle::before { content: '▶'; }");
		writer.println("        .accordion-row.open .accordion-toggle::before { content: '▼'; }");
		writer.println("        .compact-header { font-size: 11px; padding: 4px 8px; max-width: 80px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }");
		writer.println("        .pretty-print-btn { background-color: #5d6c74; color: white; border: none; border-radius: 3px; padding: 4px 8px; font-size: 11px; cursor: pointer; margin-left: 10px; }");
		writer.println("        .pretty-print-btn:hover { background-color: #21313C; }");
		writer.println("        .json-content { white-space: pre-wrap; }");

		// Column management styles
		writer.println("        .manage-cols-btn { padding: 8px 12px; background-color: #00684A; color: white; border: none; border-radius: 4px; cursor: pointer; margin-left: 10px; }");
		writer.println("        .manage-cols-btn:hover { background-color: #006CFA; }");
		writer.println("        .col-hide-btn { margin-left: 8px; cursor: pointer; color: #ff6b6b; font-weight: bold; opacity: 0; transition: opacity 0.2s; }");
		writer.println("        th:hover .col-hide-btn { opacity: 1; }");
		writer.println("        .col-hide-btn:hover { color: #ff0000; }");
		writer.println("        .hidden-col { display: none; }");

		// Modal styles
		writer.println("        .modal { display: none; position: fixed; z-index: 1000; left: 0; top: 0; width: 100%; height: 100%; overflow: auto; background-color: rgba(0, 0, 0, 0.4); }");
		writer.println("        .modal-content { background-color: #fefefe; margin: 10% auto; padding: 20px; border: 1px solid #888; border-radius: 8px; width: 400px; max-width: 80%; }");
		writer.println("        .modal-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; border-bottom: 2px solid #00684A; padding-bottom: 10px; }");
		writer.println("        .modal-header h3 { margin: 0; color: #001E2B; }");
		writer.println("        .close { color: #aaa; font-size: 28px; font-weight: bold; cursor: pointer; }");
		writer.println("        .close:hover, .close:focus { color: #000; }");
		writer.println("        .modal-body { max-height: 400px; overflow-y: auto; }");
		writer.println("        .column-item { padding: 8px; margin: 4px 0; background-color: #f0f7f4; border-radius: 4px; cursor: pointer; transition: background-color 0.2s; }");
		writer.println("        .column-item:hover { background-color: #E9FF99; }");
		writer.println("        .column-item.hidden { background-color: #ffebee; }");

		writer.println("    </style>");
		writer.println("</head>");
		writer.println("<body>");
		writer.println("    <div class=\"container\">");
	}

	private static void writeNavigationHeader(PrintWriter writer, Accumulator accumulator,
	        Accumulator ttlAccumulator, PlanCacheAccumulator planCacheAccumulator,
	        QueryHashAccumulator queryHashAccumulator, SlowPlanningAccumulator slowPlanningAccumulator,
	        ErrorCodeAccumulator errorCodeAccumulator,
	        TransactionAccumulator transactionAccumulator, IndexStatsAccumulator indexStatsAccumulator,
	        TwoPassDriverStatsAccumulator driverStatsAccumulator,
	        AppNameConnectionStatsAccumulator appNameConnectionStatsAccumulator,
	        Map<String, java.util.concurrent.atomic.AtomicLong> operationTypeStats,
	        String earliestTimestamp, String latestTimestamp) {
	    
	    writer.println("    <div class=\"nav-header\">");
	    writer.println("        <div class=\"nav-content\">");
	    writer.println("            <div class=\"nav-title\">MongoDB Log Analysis Report</div>");
	    writer.println("            <div class=\"nav-links\">");
	    
	    // Main Operations (always present)
	    if (!accumulator.getAccumulators().isEmpty()) {
	        writer.println("                    <a href=\"#main-operations\" class=\"nav-link\">Main Operations</a>");
	    }
	    
	    // TTL Operations
	    if (ttlAccumulator != null && !ttlAccumulator.getAccumulators().isEmpty()) {
	        writer.println("                    <a href=\"#ttl-operations\" class=\"nav-link\">TTL Operations</a>");
	    }
	    
	    // Operation Statistics
	    if (operationTypeStats != null && !operationTypeStats.isEmpty()) {
	        writer.println("                    <a href=\"#operation-stats\" class=\"nav-link\">Operation Stats</a>");
	    }
	    
	    // Error Codes
	    if (errorCodeAccumulator != null && errorCodeAccumulator.hasErrors()) {
	        writer.println("                    <a href=\"#error-codes\" class=\"nav-link\">Error Codes</a>");
	    }
	    
	    // Query Hash Analysis
	    if (queryHashAccumulator != null && !queryHashAccumulator.getQueryHashEntries().isEmpty()) {
	        writer.println("                    <a href=\"#query-hash\" class=\"nav-link\">Query Hash Analysis</a>");
	    }

	    // Slow Planning Analysis
	    if (slowPlanningAccumulator != null && slowPlanningAccumulator.getSize() > 0) {
	        writer.println("                    <a href=\"#slow-planning\" class=\"nav-link\">Slow Planning</a>");
	    }

	    // Transaction Analysis
	    if (transactionAccumulator != null && transactionAccumulator.hasTransactions()) {
	        writer.println("                    <a href=\"#transactions\" class=\"nav-link\">Transaction Analysis</a>");
	    }
	    
	    // Index Usage Statistics
	    if (indexStatsAccumulator != null && indexStatsAccumulator.hasIndexStats()) {
	        writer.println("                    <a href=\"#index-stats\" class=\"nav-link\">Index Usage Statistics</a>");
	    }
	    
	    // AppName Connection Statistics
	    if (appNameConnectionStatsAccumulator != null && appNameConnectionStatsAccumulator.getAppNameCount() > 0) {
	        writer.println("                    <a href=\"#appname-stats\" class=\"nav-link\">AppName Statistics</a>");
	    }

	    // Driver Statistics (last)
	    if (driverStatsAccumulator != null && driverStatsAccumulator.hasDriverStats()) {
	        writer.println("                    <a href=\"#driver-stats\" class=\"nav-link\">Driver Statistics</a>");
	    }

	    writer.println("                </div>");
	    
	    // Add timestamp range information with right-justified generation date
	    String logDataInfo = "";
	    if (earliestTimestamp != null && latestTimestamp != null) {
	        try {
	            // Format the timestamp range for display
	            String timeRange = formatTimestampRange(earliestTimestamp, latestTimestamp);
	            logDataInfo = "Log data: " + timeRange;
	        } catch (Exception e) {
	            // Fallback to raw timestamps if formatting fails
	            logDataInfo = "Log data: " + earliestTimestamp.substring(0, Math.min(19, earliestTimestamp.length()))
	                       + " to " + latestTimestamp.substring(0, Math.min(19, latestTimestamp.length()));
	        }
	    }
	    
	    writer.println("                <div class=\"report-info\" style=\"display: flex; justify-content: space-between;\"><span>" + logDataInfo + "</span><span>Generated on " + new java.util.Date() + "</span></div>");
	    writer.println("        </div>");
	    writer.println("    </div>");
	}
	
	private static void writeMainOperationsTableWithShards(PrintWriter writer, 
	        Map<ShardInfo, Accumulator> shardAccumulators, boolean redactQueries) {
	    
	    
	    writer.println("        <h2 id=\"main-operations\">Main Operations Analysis (By Shard)</h2>");
	    
	    // Combine all operations from all shards
	    java.util.List<ShardedMainOperationEntry> allOperations = new ArrayList<>();
	    
	    for (Map.Entry<ShardInfo, Accumulator> entry : shardAccumulators.entrySet()) {
	        ShardInfo shardInfo = entry.getKey();
	        Accumulator accumulator = entry.getValue();
	        
	        
	        // Get operations for this shard
	        java.util.List<MainOperationEntry> shardOps = MainOperationService.getMainOperationEntries(accumulator);
	        
	        
	        // Wrap each operation with shard info
	        for (MainOperationEntry op : shardOps) {
	            allOperations.add(new ShardedMainOperationEntry(shardInfo, op));
	        }
	    }
	    
	    // Calculate appropriate unit scales for byte columns based on averages
	    double avgTotalReslen = allOperations.isEmpty() ? 0 : 
	        allOperations.stream().mapToLong(e -> e.getOperation().getTotalReslen()).average().orElse(0);
	    double avgAvgReslen = allOperations.isEmpty() ? 0 : 
	        allOperations.stream().mapToLong(e -> e.getOperation().getAvgReslen()).average().orElse(0);
	    double avgTotalBytesRead = allOperations.isEmpty() ? 0 : 
	        allOperations.stream().mapToLong(e -> e.getOperation().getTotalStorageBytesRead()).average().orElse(0);
	    double avgAvgBytesRead = allOperations.isEmpty() ? 0 : 
	        allOperations.stream().mapToLong(e -> e.getOperation().getAvgStorageBytesRead()).average().orElse(0);
	    
	    MainOperationEntry.ByteUnit totalReslenUnit = MainOperationEntry.determineByteUnit(avgTotalReslen);
	    MainOperationEntry.ByteUnit avgReslenUnit = MainOperationEntry.determineByteUnit(avgAvgReslen);
	    MainOperationEntry.ByteUnit totalBytesReadUnit = MainOperationEntry.determineByteUnit(avgTotalBytesRead);
	    MainOperationEntry.ByteUnit avgBytesReadUnit = MainOperationEntry.determineByteUnit(avgAvgBytesRead);
	    
	    // Sort by shard, then by count
	    allOperations.sort((a, b) -> {
	        int shardCompare = a.getShardInfo().toString().compareTo(b.getShardInfo().toString());
	        if (shardCompare != 0) return shardCompare;
	        return Long.compare(b.getOperation().getCount(), a.getOperation().getCount());
	    });
	    
	    // Calculate summary statistics
	    long totalOperations = allOperations.stream().mapToLong(e -> e.getOperation().getCount()).sum();
	    long totalTimeMs = allOperations.stream().mapToLong(e -> e.getOperation().getCount() * e.getOperation().getAvgMs()).sum();
	    int uniqueShards = (int) allOperations.stream().map(ShardedMainOperationEntry::getShardInfo).distinct().count();
	    
	    writer.println("        <div class=\"summary\">");
	    writer.println("            <h3>Summary</h3>");
	    writer.println("            <div class=\"summary-grid\">");
	    writer.println("                <div class=\"summary-item\">");
	    writer.println("                    <div class=\"summary-label\">Total Shards</div>");
	    writer.println("                    <div class=\"summary-value\">" + uniqueShards + "</div>");
	    writer.println("                </div>");
	    writer.println("                <div class=\"summary-item\">");
	    writer.println("                    <div class=\"summary-label\">Total Operations</div>");
	    writer.println("                    <div class=\"summary-value\">" + NUMBER_FORMAT.format(totalOperations) + "</div>");
	    writer.println("                </div>");
	    writer.println("                <div class=\"summary-item\">");
	    writer.println("                    <div class=\"summary-label\">Total Time (seconds)</div>");
	    writer.println("                    <div class=\"summary-value\">" + NUMBER_FORMAT.format(totalTimeMs / 1000) + "</div>");
	    writer.println("                </div>");
	    writer.println("                <div class=\"summary-item\">");
	    writer.println("                    <div class=\"summary-label\">Unique Operations</div>");
	    writer.println("                    <div class=\"summary-value\">" + allOperations.size() + "</div>");
	    writer.println("                </div>");
	    writer.println("            </div>");
	    writer.println("        </div>");
	    
	    writer.println("        <div class=\"table-container\">");
	    writer.println("            <div class=\"controls\">");
	    writer.println("                <input type=\"text\" id=\"mainOpsFilter\" class=\"filter-input\" placeholder=\"Filter by shard, namespace or operation...\">");
	    writer.println("                <button class=\"clear-btn\" onclick=\"clearFilter('mainOpsFilter', 'mainOpsTable')\">Clear Filter</button>");
	    writer.println("                <button class=\"expand-btn\" onclick=\"expandAllAccordions('mainOpsTable')\">Expand All</button>");
	    writer.println("                <button class=\"collapse-btn\" onclick=\"collapseAllAccordions('mainOpsTable')\">Collapse All</button>");
	    writer.println("                <button class=\"manage-cols-btn\" onclick=\"openColumnModal('mainOpsTable')\">Manage Columns</button>");
	    writer.println("            </div>");
	    writer.println("            <table id=\"mainOpsTable\">");
	    writer.println("                <thead>");
	    writer.println("                    <tr>");
	    writer.println("                        " + generateTableHeader("mainOpsTable", 0, "Shard", "string"));
	    writer.println("                        " + generateTableHeader("mainOpsTable", 1, "Namespace", "string"));
	    writer.println("                        " + generateTableHeader("mainOpsTable", 2, "Operation", "string"));
	    writer.println("                        " + generateTableHeader("mainOpsTable", 3, "Count", "number"));
	    writer.println("                        " + generateTableHeader("mainOpsTable", 4, "Min (ms)", "number"));
	    writer.println("                        " + generateTableHeader("mainOpsTable", 5, "Max (ms)", "number"));
	    writer.println("                        " + generateTableHeader("mainOpsTable", 6, "Avg (ms)", "number"));
	    writer.println("                        " + generateTableHeader("mainOpsTable", 7, "P95 (ms)", "number"));
	    writer.println("                        " + generateTableHeader("mainOpsTable", 8, "Total (sec)", "number"));
	    writer.println("                        " + generateTableHeader("mainOpsTable", 9, "Avg Keys Ex", "number"));
	    writer.println("                        " + generateTableHeader("mainOpsTable", 10, "Avg Docs Ex", "number"));
	    writer.println("                        " + generateTableHeader("mainOpsTable", 11, "Errors", "number"));
	    writer.println("                        " + generateTableHeader("mainOpsTable", 12, "Total Reslen (" + totalReslenUnit.getSuffix() + ")", "number"));
	    writer.println("                        " + generateTableHeader("mainOpsTable", 13, "Avg Reslen (" + avgReslenUnit.getSuffix() + ")", "number"));
	    writer.println("                        " + generateTableHeader("mainOpsTable", 14, "Total Returned", "number"));
	    writer.println("                        " + generateTableHeader("mainOpsTable", 15, "Avg Returned", "number"));
	    writer.println("                        " + generateTableHeader("mainOpsTable", 16, "Total Bytes Read (" + totalBytesReadUnit.getSuffix() + ")", "number"));
	    writer.println("                        " + generateTableHeader("mainOpsTable", 17, "Avg Bytes Read (" + avgBytesReadUnit.getSuffix() + ")", "number"));
	    writer.println("                    </tr>");
	    writer.println("                </thead>");
	    writer.println("                <tbody>");
	    
	    for (ShardedMainOperationEntry entry : allOperations) {
	        MainOperationEntry op = entry.getOperation();
	        String shardDisplay = entry.getShardInfo().getShard() + "-" + entry.getShardInfo().getNode();
	        String cssClass = "accordion-row " + op.getCssClass();
	        String rowId = "mo-shard-" + Math.abs((entry.getShardInfo().toString() + op.getNamespace() + op.getOperation()).hashCode());
	        
	        // Main row with accordion functionality
	        writer.println("                    <tr class=\"" + cssClass + "\" onclick=\"toggleAccordion('" + rowId + "')\">");
	        writer.println("                        <td class=\"shard-cell\"><span class=\"accordion-toggle\"></span>" + shardDisplay + "</td>");
	        writer.println("                        <td class=\"namespace-cell\">" + escapeHtml(op.getNamespace()) + "</td>");
	        writer.println("                        <td class=\"operation-cell\">" + escapeHtml(op.getOperationDisplay()) + "</td>");
	        writer.println("                        <td class=\"count-cell\">" + op.getFormattedCount() + "</td>");
	        writer.println("                        <td class=\"time-cell\">" + NUMBER_FORMAT.format(op.getMinMs()) + "</td>");
	        writer.println("                        <td class=\"time-cell\">" + NUMBER_FORMAT.format(op.getMaxMs()) + "</td>");
	        writer.println("                        <td class=\"time-cell\">" + NUMBER_FORMAT.format(op.getAvgMs()) + "</td>");
	        writer.println("                        <td class=\"time-cell\">" + op.getFormattedP95Ms() + "</td>");
	        writer.println("                        <td class=\"time-cell\">" + NUMBER_FORMAT.format(op.getTotalSec()) + "</td>");
	        writer.println("                        <td class=\"count-cell\">" + op.getFormattedAvgKeysExamined() + "</td>");
	        writer.println("                        <td class=\"count-cell\">" + op.getFormattedAvgDocsExamined() + "</td>");
	        writer.println("                        <td class=\"count-cell\">" + NUMBER_FORMAT.format(op.getErrorCount()) + "</td>");
	        writer.println("                        <td class=\"bytes-cell\">" + MainOperationEntry.formatBytesWithUnit(op.getTotalReslen(), totalReslenUnit) + "</td>");
	        writer.println("                        <td class=\"bytes-cell\">" + MainOperationEntry.formatBytesWithUnit(op.getAvgReslen(), avgReslenUnit) + "</td>");
	        writer.println("                        <td class=\"count-cell\">" + op.getFormattedTotalReturned() + "</td>");
	        writer.println("                        <td class=\"count-cell\">" + op.getFormattedAvgReturned() + "</td>");
	        writer.println("                        <td class=\"bytes-cell\">" + MainOperationEntry.formatBytesWithUnit(op.getTotalStorageBytesRead(), totalBytesReadUnit) + "</td>");
	        writer.println("                        <td class=\"bytes-cell\">" + MainOperationEntry.formatBytesWithUnit(op.getAvgStorageBytesRead(), avgBytesReadUnit) + "</td>");
	        writer.println("                    </tr>");
	        
	        // Accordion content row
	        if (op.getSampleLogMessage() != null && !op.getSampleLogMessage().isEmpty()) {
	            boolean isTruncated = LogRedactionUtil.isLogMessageTruncated(op.getSampleLogMessage());
	            String logCssClass = isTruncated ? "log-sample truncated-query" : "log-sample";
	            
	            writer.println("                    <tr id=\"" + rowId + "\" class=\"accordion-content\">");
	            writer.println("                        <td colspan=\"18\">");
	            writer.println("                            <div class=\"" + logCssClass + "\">");
	            if (isTruncated) {
	                writer.println("                                <div class=\"truncated-warning\">⚠ Query was truncated in MongoDB logs</div>");
	            }
	            
	            String contentId = "log-content-" + rowId;
	            String buttonId = "btn-" + rowId;
	            String formattedDuration = LogRedactionUtil.extractFormattedDuration(op.getSampleLogMessage());
	            String formattedBytesRead = LogRedactionUtil.extractFormattedBytesRead(op.getSampleLogMessage());
	            
	            writer.println("                                <strong>Query Sample:</strong>");
	            if (formattedDuration != null && !formattedDuration.isEmpty()) {
	                writer.println(" <em>(Duration: " + formattedDuration + ")</em>");
	            }
	            if (formattedBytesRead != null && !formattedBytesRead.isEmpty()) {
	                writer.println(" <em>(Bytes Read: " + formattedBytesRead + ")</em>");
	            }
	            writer.println("<button id=\"" + buttonId + "\" class=\"pretty-print-btn\" onclick=\"togglePrettyPrint('" + buttonId + "', '" + contentId + "')\">Pretty Print</button>");
	            writer.println("                                <div id=\"" + contentId + "\" class=\"json-content\">");
	            
	            String logMessage = LogRedactionUtil.redactLogMessage(op.getSampleLogMessage(), redactQueries);
	            writer.println("                                    " + escapeHtml(logMessage));
	            
	            writer.println("                                </div>");
	            writer.println("                            </div>");
	            writer.println("                        </td>");
	            writer.println("                    </tr>");
	        }
	    }
	    
	    writer.println("                </tbody>");
	    writer.println("            </table>");
	    writer.println("        </div>");
	}
	
	// Helper class to pair shard info with operation entry
	private static class ShardedMainOperationEntry {
	    private final ShardInfo shardInfo;
	    private final MainOperationEntry operation;
	    
	    public ShardedMainOperationEntry(ShardInfo shardInfo, MainOperationEntry operation) {
	        this.shardInfo = shardInfo;
	        this.operation = operation;
	    }
	    
	    public ShardInfo getShardInfo() { return shardInfo; }
	    public MainOperationEntry getOperation() { return operation; }
	}
	
	// Helper method to write operations table content without the wrapper and controls
	private static void writeOperationsTableContent(PrintWriter writer, Accumulator accumulator, boolean redactQueries, String tableId) {
	    if (accumulator.getAccumulators().isEmpty()) {
	        writer.println("            <p>No operations found.</p>");
	        return;
	    }
	    
	    // Use MVC pattern: Convert to model objects via service
	    java.util.List<MainOperationEntry> operations = MainOperationService.getMainOperationEntries(accumulator);
	    
	    writer.println("            <table id=\"" + tableId + "\">");
	    writer.println("                <thead>");
	    writer.println("                    <tr>");
	    writer.println("                        <th>Namespace</th>");
	    writer.println("                        <th>Operation</th>");
	    writer.println("                        <th>Count</th>");
	    writer.println("                        <th>Min (ms)</th>");
	    writer.println("                        <th>Max (ms)</th>");
	    writer.println("                        <th>Avg (ms)</th>");
	    writer.println("                        <th>P95 (ms)</th>");
	    writer.println("                        <th>Total (sec)</th>");
	    writer.println("                        <th>Avg Keys Ex</th>");
	    writer.println("                        <th>Avg Docs Ex</th>");
	    writer.println("                    </tr>");
	    writer.println("                </thead>");
	    writer.println("                <tbody>");
	    
	    operations.forEach(op -> {
	        String cssClass = op.getCssClass();
	        writer.println("                    <tr class=\"" + cssClass + "\">");
	        writer.println("                        <td>" + op.getNamespace() + "</td>");
	        writer.println("                        <td>" + op.getOperationDisplay() + "</td>");
	        writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(op.getCount()) + "</td>");
	        writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(op.getMinMs()) + "</td>");
	        writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(op.getMaxMs()) + "</td>");
	        writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(op.getAvgMs()) + "</td>");
	        writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(op.getP95Ms()) + "</td>");
	        writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(op.getTotalSec()) + "</td>");
	        writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(op.getAvgKeysExamined()) + "</td>");
	        writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(op.getAvgDocsExamined()) + "</td>");
	        writer.println("                    </tr>");
	    });
	    
	    writer.println("                </tbody>");
	    writer.println("            </table>");
	}
	
	// Helper method to write TTL table content
	private static void writeTtlTableContent(PrintWriter writer, Accumulator ttlAccumulator, String tableId) {
	    if (ttlAccumulator.getAccumulators().isEmpty()) {
	        writer.println("            <p>No TTL operations found.</p>");
	        return;
	    }
	    
	    writer.println("            <table id=\"" + tableId + "\">");
	    writer.println("                <thead>");
	    writer.println("                    <tr>");
	    writer.println("                        <th>Namespace</th>");
	    writer.println("                        <th>Count</th>");
	    writer.println("                        <th>Total Deleted</th>");
	    writer.println("                        <th>Avg Deleted</th>");
	    writer.println("                        <th>Min (ms)</th>");
	    writer.println("                        <th>Max (ms)</th>");
	    writer.println("                        <th>Avg (ms)</th>");
	    writer.println("                    </tr>");
	    writer.println("                </thead>");
	    writer.println("                <tbody>");
	    
	    ttlAccumulator.getAccumulators().values().stream()
	        .sorted((a, b) -> Long.compare(b.getCount(), a.getCount()))
	        .forEach(acc -> {
	            String namespace = acc.toString().split(" ")[0];
	            long totalDeleted = acc.getAvgReturned() * acc.getCount();
	            
	            writer.println("                    <tr>");
	            writer.println("                        <td>" + namespace + "</td>");
	            writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(acc.getCount()) + "</td>");
	            writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(totalDeleted) + "</td>");
	            writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(acc.getAvgReturned()) + "</td>");
	            writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(acc.getMin()) + "</td>");
	            writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(acc.getMax()) + "</td>");
	            writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(acc.getAvg()) + "</td>");
	            writer.println("                    </tr>");
	        });
	    
	    writer.println("                </tbody>");
	    writer.println("            </table>");
	}
	
	private static void writeTtlOperationsTableWithShards(PrintWriter writer, 
	        Map<ShardInfo, Accumulator> shardTtlAccumulators) {
	    
	    writer.println("        <h2 id=\"ttl-operations\">TTL Operations (By Shard)</h2>");
	    
	    writer.println("        <div class=\"table-container\">");
	    writer.println("            <table id=\"ttlTable\">");
	    writer.println("                <thead>");
	    writer.println("                    <tr>");
	    writer.println("                        <th class=\"sortable\" onclick=\"sortTable('ttlTable', 0, 'string')\">Shard</th>");
	    writer.println("                        <th class=\"sortable\" onclick=\"sortTable('ttlTable', 1, 'string')\">Namespace</th>");
	    writer.println("                        <th class=\"sortable\" onclick=\"sortTable('ttlTable', 2, 'number')\">Count</th>");
	    writer.println("                        <th class=\"sortable\" onclick=\"sortTable('ttlTable', 3, 'number')\">Total Deleted</th>");
	    writer.println("                        <th class=\"sortable\" onclick=\"sortTable('ttlTable', 4, 'number')\">Avg Deleted</th>");
	    writer.println("                        <th class=\"sortable\" onclick=\"sortTable('ttlTable', 5, 'number')\">Min (ms)</th>");
	    writer.println("                        <th class=\"sortable\" onclick=\"sortTable('ttlTable', 6, 'number')\">Max (ms)</th>");
	    writer.println("                        <th class=\"sortable\" onclick=\"sortTable('ttlTable', 7, 'number')\">Avg (ms)</th>");
	    writer.println("                    </tr>");
	    writer.println("                </thead>");
	    writer.println("                <tbody>");
	    
	    for (Map.Entry<ShardInfo, Accumulator> shardEntry : shardTtlAccumulators.entrySet()) {
	        ShardInfo shardInfo = shardEntry.getKey();
	        Accumulator ttlAccumulator = shardEntry.getValue();
	        String shardDisplay = shardInfo.getShard() + "-" + shardInfo.getNode();
	        
	        if (ttlAccumulator.getAccumulators().isEmpty()) {
	            continue;
	        }
	        
	        ttlAccumulator.getAccumulators().values().stream()
	            .sorted((a, b) -> Long.compare(b.getCount(), a.getCount()))
	            .forEach(acc -> {
	                String namespace = acc.toString().split(" ")[0];
	                long totalDeleted = acc.getAvgReturned() * acc.getCount();
	                
	                writer.println("                    <tr>");
	                writer.println("                        <td>" + shardDisplay + "</td>");
	                writer.println("                        <td>" + namespace + "</td>");
	                writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(acc.getCount()) + "</td>");
	                writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(totalDeleted) + "</td>");
	                writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(acc.getAvgReturned()) + "</td>");
	                writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(acc.getMin()) + "</td>");
	                writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(acc.getMax()) + "</td>");
	                writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(acc.getAvg()) + "</td>");
	                writer.println("                    </tr>");
	            });
	    }
	    
	    writer.println("                </tbody>");
	    writer.println("            </table>");
	    writer.println("        </div>");
	}
	
	// Stub methods for other shard tables (to be implemented)
	private static void writeErrorCodesTableWithShards(PrintWriter writer, 
	        Map<ShardInfo, ErrorCodeAccumulator> shardErrorCodeAccumulators, boolean redactQueries) {
	    
	    
	    // Check if we have any error data across all shards
	    boolean hasAnyErrors = shardErrorCodeAccumulators.values().stream()
	        .anyMatch(acc -> acc != null && acc.hasErrors());
	    
	    if (!hasAnyErrors) {
	        return;
	    }
	    
	    writer.println("        <h2 id=\"error-codes\">Error Codes Analysis (By Shard)</h2>");
	    
	    // Helper class to combine shard info with error entries
	    class ShardedErrorEntry {
	        private final ShardInfo shardInfo;
	        private final ErrorCodeAccumulator.ErrorCodeEntry errorEntry;
	        
	        public ShardedErrorEntry(ShardInfo shardInfo, ErrorCodeAccumulator.ErrorCodeEntry errorEntry) {
	            this.shardInfo = shardInfo;
	            this.errorEntry = errorEntry;
	        }
	        
	        public ShardInfo getShardInfo() { return shardInfo; }
	        public ErrorCodeAccumulator.ErrorCodeEntry getErrorEntry() { return errorEntry; }
	    }
	    
	    // Combine all error entries from all shards
	    java.util.List<ShardedErrorEntry> allErrors = new ArrayList<>();
	    long totalErrorsAcrossShards = 0;
	    
	    for (Map.Entry<ShardInfo, ErrorCodeAccumulator> shardEntry : shardErrorCodeAccumulators.entrySet()) {
	        ShardInfo shardInfo = shardEntry.getKey();
	        ErrorCodeAccumulator errorAccum = shardEntry.getValue();
	        
	        if (errorAccum != null && errorAccum.hasErrors()) {
	            
	            var entries = errorAccum.getErrorCodeEntries().values();
	            for (var errorEntry : entries) {
	                allErrors.add(new ShardedErrorEntry(shardInfo, errorEntry));
	                totalErrorsAcrossShards += errorEntry.getCount();
	            }
	        }
	    }
	    
	    
	    // Sort by shard, then by error count
	    allErrors.sort((a, b) -> {
	        int shardCompare = a.getShardInfo().toString().compareTo(b.getShardInfo().toString());
	        if (shardCompare != 0) return shardCompare;
	        return Long.compare(b.getErrorEntry().getCount(), a.getErrorEntry().getCount());
	    });
	    
	    // Calculate summary statistics
	    int uniqueShards = (int) allErrors.stream().map(ShardedErrorEntry::getShardInfo).distinct().count();
	    int uniqueErrorCodes = (int) allErrors.stream().map(e -> e.getErrorEntry().getCodeName()).distinct().count();
	    
	    writer.println("        <div class=\"summary\">");
	    writer.println("            <h3>Error Summary</h3>");
	    writer.println("            <div class=\"summary-grid\">");
	    writer.println("                <div class=\"summary-item\">");
	    writer.println("                    <div class=\"summary-label\">Total Shards with Errors</div>");
	    writer.println("                    <div class=\"summary-value\">" + uniqueShards + "</div>");
	    writer.println("                </div>");
	    writer.println("                <div class=\"summary-item\">");
	    writer.println("                    <div class=\"summary-label\">Total Error Occurrences</div>");
	    writer.println("                    <div class=\"summary-value\">" + NUMBER_FORMAT.format(totalErrorsAcrossShards) + "</div>");
	    writer.println("                </div>");
	    writer.println("                <div class=\"summary-item\">");
	    writer.println("                    <div class=\"summary-label\">Unique Error Codes</div>");
	    writer.println("                    <div class=\"summary-value\">" + uniqueErrorCodes + "</div>");
	    writer.println("                </div>");
	    writer.println("                <div class=\"summary-item\">");
	    writer.println("                    <div class=\"summary-label\">Total Error Entries</div>");
	    writer.println("                    <div class=\"summary-value\">" + allErrors.size() + "</div>");
	    writer.println("                </div>");
	    writer.println("            </div>");
	    writer.println("        </div>");
	    
	    writer.println("        <div class=\"table-container\">");
	    writer.println("            <div class=\"controls\">");
	    writer.println("                <input type=\"text\" id=\"errorCodesFilter\" class=\"filter-input\" placeholder=\"Filter by shard, error code, or message...\">");
	    writer.println("                <button class=\"clear-btn\" onclick=\"clearFilter('errorCodesFilter', 'errorCodesTable')\">Clear Filter</button>");
	    writer.println("                <button class=\"expand-btn\" onclick=\"expandAllAccordions('errorCodesTable')\">Expand All</button>");
	    writer.println("                <button class=\"collapse-btn\" onclick=\"collapseAllAccordions('errorCodesTable')\">Collapse All</button>");
	    writer.println("            </div>");
	    writer.println("            <table id=\"errorCodesTable\">");
	    writer.println("                <thead>");
	    writer.println("                    <tr>");
	    writer.println("                        <th class=\"sortable\" onclick=\"sortTable('errorCodesTable', 0, 'string')\">Shard</th>");
	    writer.println("                        <th class=\"sortable\" onclick=\"sortTable('errorCodesTable', 1, 'string')\">Code Name</th>");
	    writer.println("                        <th class=\"sortable\" onclick=\"sortTable('errorCodesTable', 2, 'number')\">Error Code</th>");
	    writer.println("                        <th class=\"sortable\" onclick=\"sortTable('errorCodesTable', 3, 'number')\">Count</th>");
	    writer.println("                        <th class=\"sortable\" onclick=\"sortTable('errorCodesTable', 4, 'number')\">% of Shard's Errors</th>");
	    writer.println("                        <th class=\"sortable\" onclick=\"sortTable('errorCodesTable', 5, 'string')\">Sample Error Message</th>");
	    writer.println("                    </tr>");
	    writer.println("                </thead>");
	    writer.println("                <tbody>");
	    
	    for (ShardedErrorEntry shardedEntry : allErrors) {
	        ShardInfo shard = shardedEntry.getShardInfo();
	        ErrorCodeAccumulator.ErrorCodeEntry errorEntry = shardedEntry.getErrorEntry();
	        String shardDisplay = shard.getShard() + "-" + shard.getNode();
	        String rowId = "err-shard-" + Math.abs((shard.toString() + errorEntry.getCodeName()).hashCode());
	        
	        // Calculate percentage within this shard
	        long shardTotalErrors = shardErrorCodeAccumulators.get(shard).getTotalErrorCount();
	        double percentage = (errorEntry.getCount() * 100.0) / Math.max(shardTotalErrors, 1);
	        
	        // Main row with accordion functionality  
	        writer.println("                    <tr class=\"accordion-row\" onclick=\"toggleAccordion('" + rowId + "')\">");
	        writer.println("                        <td><span class=\"accordion-toggle\"></span>" + shardDisplay + "</td>");
	        writer.println("                        <td>" + escapeHtml(errorEntry.getCodeName()) + "</td>");
	        writer.println("                        <td class=\"number\">" + 
	                      (errorEntry.getErrorCode() != null ? errorEntry.getErrorCode().toString() : "unknown") + "</td>");
	        writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(errorEntry.getCount()) + "</td>");
	        writer.println("                        <td class=\"number\">" + String.format("%.1f%%", percentage) + "</td>");
	        
	        String sampleErrorMessage = errorEntry.getSampleErrorMessage();
	        if (sampleErrorMessage != null) {
	            String processedErrorMessage = LogRedactionUtil.processLogMessage(sampleErrorMessage, redactQueries);
	            writer.println("                        <td class=\"truncated\" title=\"" + escapeHtml(processedErrorMessage) + "\">" + 
	                          escapeHtml(truncate(processedErrorMessage, 100)) + "</td>");
	        } else {
	            writer.println("                        <td>N/A</td>");
	        }
	        writer.println("                    </tr>");
	        
	        // Accordion content row with full error message
	        if (sampleErrorMessage != null && !sampleErrorMessage.isEmpty()) {
	            writer.println("                    <tr id=\"" + rowId + "\" class=\"accordion-content\">");
	            writer.println("                        <td colspan=\"6\">");
	            writer.println("                            <div class=\"log-sample\">");
	            writer.println("                                <strong>Full Error Message:</strong><br>");
	            
	            String processedErrorMessage = LogRedactionUtil.processLogMessage(sampleErrorMessage, redactQueries);
	            writer.println("                                " + escapeHtml(processedErrorMessage));
	            
	            writer.println("                            </div>");
	            writer.println("                        </td>");
	            writer.println("                    </tr>");
	        }
	    }
	    
	    writer.println("                </tbody>");
	    writer.println("            </table>");
	    writer.println("        </div>");
	}
	
	private static void writeQueryHashTableWithShards(PrintWriter writer, 
	        Map<ShardInfo, QueryHashAccumulator> shardQueryHashAccumulators, boolean redactQueries) {
	    // For now, just write a placeholder
	    writer.println("        <h2>Query Patterns (By Shard)</h2>");
	    writer.println("        <p>Query patterns analysis by shard would appear here.</p>");
	}
	
	private static void writeTransactionTableWithShards(PrintWriter writer, 
	        Map<ShardInfo, TransactionAccumulator> shardTransactionAccumulators) {
	    // For now, just write a placeholder
	    writer.println("        <h2>Transactions (By Shard)</h2>");
	    writer.println("        <p>Transaction analysis by shard would appear here.</p>");
	}
	
	private static void writeIndexStatsTableWithShards(PrintWriter writer, 
	        Map<ShardInfo, IndexStatsAccumulator> shardIndexStatsAccumulators, boolean redactQueries) {
	    // For now, just write a placeholder
	    writer.println("        <h2>Index Statistics (By Shard)</h2>");
	    writer.println("        <p>Index statistics by shard would appear here.</p>");
	}
	
	// Stub methods for other table content generation (to be implemented)
	private static void writeErrorCodesTableContent(PrintWriter writer, ErrorCodeAccumulator errorAccum, boolean redactQueries, String tableId) {
	    // Implementation would go here - for now just a placeholder
	    writer.println("            <p>Error codes analysis would appear here.</p>");
	}
	
	private static void writeQueryHashTableContent(PrintWriter writer, QueryHashAccumulator queryHashAccum, boolean redactQueries, String tableId) {
	    // Implementation would go here - for now just a placeholder
	    writer.println("            <p>Query hash analysis would appear here.</p>");
	}
	
	private static void writeTransactionTableContent(PrintWriter writer, TransactionAccumulator transAccum, String tableId) {
	    // Implementation would go here - for now just a placeholder
	    writer.println("            <p>Transaction analysis would appear here.</p>");
	}
	
	private static void writeIndexStatsTableContent(PrintWriter writer, IndexStatsAccumulator indexAccum, boolean redactQueries, String tableId) {
	    // Implementation would go here - for now just a placeholder
	    writer.println("            <p>Index statistics would appear here.</p>");
	}

	private static void writeMainOperationsTable(PrintWriter writer, Accumulator accumulator, boolean redactQueries) {
		if (accumulator.getAccumulators().isEmpty()) {
			return;
		}

		writer.println("        <h2 id=\"main-operations\">Main Operations Analysis</h2>");

		// Use MVC pattern: Convert to model objects via service
		java.util.List<MainOperationEntry> operations = MainOperationService.getMainOperationEntries(accumulator);
		
		// Calculate summary statistics using model objects
		long totalOperations = operations.stream().mapToLong(MainOperationEntry::getCount).sum();
		long totalTimeMs = operations.stream().mapToLong(op -> op.getCount() * op.getAvgMs()).sum();
		long writeOperations = MainOperationService.getWriteOperations(operations).size();

		writer.println("        <div class=\"summary\">");
		writer.println("            <h3>Summary</h3>");
		writer.println("            <div class=\"summary-grid\">");
		writer.println("                <div class=\"summary-item\">");
		writer.println("                    <div class=\"summary-label\">Total Operations</div>");
		writer.println(
				"                    <div class=\"summary-value\">" + NUMBER_FORMAT.format(totalOperations) + "</div>");
		writer.println("                </div>");
		writer.println("                <div class=\"summary-item\">");
		writer.println("                    <div class=\"summary-label\">Total Time (seconds)</div>");
		writer.println("                    <div class=\"summary-value\">" + NUMBER_FORMAT.format(totalTimeMs / 1000)
				+ "</div>");
		writer.println("                </div>");
		writer.println("                <div class=\"summary-item\">");
		writer.println("                    <div class=\"summary-label\">Unique Operations</div>");
		writer.println(
				"                    <div class=\"summary-value\">" + operations.size() + "</div>");
		writer.println("                </div>");
		writer.println("                <div class=\"summary-item\">");
		writer.println("                    <div class=\"summary-label\">Write Operations</div>");
		writer.println(
				"                    <div class=\"summary-value\">" + writeOperations + "</div>");
		writer.println("                </div>");
		writer.println("            </div>");
		writer.println("        </div>");

		writer.println("        <div class=\"table-container\">");
		writer.println("            <div class=\"controls\">");
		writer.println(
				"                <input type=\"text\" id=\"mainOpsFilter\" class=\"filter-input\" placeholder=\"Filter by namespace or operation...\">");
		writer.println(
				"                <button class=\"clear-btn\" onclick=\"clearFilter('mainOpsFilter', 'mainOpsTable')\">Clear Filter</button>");
		writer.println(
				"                <button class=\"expand-btn\" onclick=\"expandAllAccordions('mainOpsTable')\">Expand All</button>");
		writer.println(
				"                <button class=\"collapse-btn\" onclick=\"collapseAllAccordions('mainOpsTable')\">Collapse All</button>");
		writer.println(
				"                <button class=\"manage-cols-btn\" onclick=\"openColumnModal('mainOpsTable')\">Manage Columns</button>");
		writer.println("            </div>");
		writer.println("            <table id=\"mainOpsTable\">");
		writer.println("                <thead>");
		writer.println("                    <tr>");
		writer.println("                        " + generateTableHeader("mainOpsTable", 0, "Namespace", "string"));
		writer.println("                        " + generateTableHeader("mainOpsTable", 1, "Operation", "string"));
		writer.println("                        " + generateTableHeader("mainOpsTable", 2, "App Name", "string"));
		writer.println("                        " + generateTableHeader("mainOpsTable", 3, "Count", "number"));
		writer.println("                        " + generateTableHeader("mainOpsTable", 4, "Min (ms)", "number"));
		writer.println("                        " + generateTableHeader("mainOpsTable", 5, "Max (ms)", "number"));
		writer.println("                        " + generateTableHeader("mainOpsTable", 6, "Avg (ms)", "number"));
		writer.println("                        " + generateTableHeader("mainOpsTable", 7, "P95 (ms)", "number"));
		writer.println("                        " + generateTableHeader("mainOpsTable", 8, "Total (sec)", "number"));
		writer.println("                        " + generateTableHeader("mainOpsTable", 9, "Avg Keys Ex", "number"));
		writer.println("                        " + generateTableHeader("mainOpsTable", 10, "Avg Docs Ex", "number"));
		writer.println("                        " + generateTableHeader("mainOpsTable", 11, "Avg Return", "number"));
		writer.println("                        " + generateTableHeader("mainOpsTable", 12, "Total Return", "number"));
		writer.println("                        " + generateTableHeader("mainOpsTable", 13, "Ex/Ret Ratio", "number"));
		writer.println("                        " + generateTableHeader("mainOpsTable", 14, "Avg Shards", "number"));
		writer.println("                        " + generateTableHeader("mainOpsTable", 15, "Avg Read", "number"));
		writer.println("                        " + generateTableHeader("mainOpsTable", 16, "Max Read", "number"));
		writer.println("                        " + generateTableHeader("mainOpsTable", 17, "Avg Write", "number"));
		writer.println("                        " + generateTableHeader("mainOpsTable", 18, "Max Write", "number"));
		writer.println("                        " + generateTableHeader("mainOpsTable", 19, "Avg Write Conflicts", "number"));
		writer.println("                    </tr>");
		writer.println("                </thead>");
		writer.println("                <tbody>");

		// Use model objects for rendering (View layer)
		operations.forEach(op -> {
			String namespace = op.getNamespace();
			String operation = op.getOperationDisplay();
			String rowId = "mo-" + Math.abs((namespace + op.getOperation()).hashCode());
			String cssClass = op.getCssClass();

			writer.println("                    <tr class=\"accordion-row " + cssClass + "\" onclick=\"toggleAccordion('" + rowId + "')\">");
			writer.println("                        <td class=\"namespace-cell\"><span class=\"accordion-toggle\"></span>" + escapeHtml(namespace) + "</td>");
			writer.println("                        <td class=\"operation-cell\">" + escapeHtml(operation) + "</td>");
			writer.println("                        <td class=\"truncated\">" + (op.getAppName() != null ? escapeHtml(op.getAppName()) : "") + "</td>");
			writer.println("                        <td class=\"count-cell\">" + op.getFormattedCount() + "</td>");
			writer.println("                        <td class=\"time-cell\">" + NUMBER_FORMAT.format(op.getMinMs()) + "</td>");
			writer.println("                        <td class=\"time-cell\">" + NUMBER_FORMAT.format(op.getMaxMs()) + "</td>");
			writer.println("                        <td class=\"time-cell\">" + NUMBER_FORMAT.format(op.getAvgMs()) + "</td>");
			writer.println("                        <td class=\"time-cell\">" + op.getFormattedP95Ms() + "</td>");
			writer.println("                        <td class=\"time-cell\">" + NUMBER_FORMAT.format(op.getTotalSec()) + "</td>");
			writer.println("                        <td class=\"count-cell\">" + op.getFormattedAvgKeysExamined() + "</td>");
			writer.println("                        <td class=\"count-cell\">" + op.getFormattedAvgDocsExamined() + "</td>");
			writer.println("                        <td class=\"count-cell\">" + op.getFormattedAvgReturned() + "</td>");
			writer.println("                        <td class=\"count-cell\">" + op.getFormattedTotalReturned() + "</td>");
			writer.println("                        <td class=\"count-cell\">" + NUMBER_FORMAT.format(op.getExRetRatio()) + "</td>");
			writer.println("                        <td class=\"count-cell\">" + op.getFormattedAvgShards() + "</td>");
			writer.print("                        ");
			writer.println(op.getFormattedAvgBytesReadCell());
			writer.print("                        ");
			writer.println(op.getFormattedMaxBytesReadCell());
			writer.print("                        ");
			writer.println(op.getFormattedAvgBytesWrittenCell());
			writer.print("                        ");
			writer.println(op.getFormattedMaxBytesWrittenCell());
			writer.println("                        <td class=\"number\">" + op.getFormattedAvgWriteConflicts() + "</td>");
			writer.println("                    </tr>");
			
			// Add accordion content row for sample log message
			if (op.getSampleLogMessage() != null) {
				String processedLogMessage = LogRedactionUtil.processLogMessage(op.getSampleLogMessage(), redactQueries);
				String enhancedLogMessage = LogRedactionUtil.enhanceLogMessageForHtml(processedLogMessage);
				enhancedLogMessage = escapeHtml(enhancedLogMessage);
				String querySource = LogRedactionUtil.detectQuerySource(op.getSampleLogMessage());
				boolean isTruncated = LogRedactionUtil.isLogMessageTruncated(op.getSampleLogMessage());
				String logCssClass = isTruncated ? "log-sample truncated-query" : "log-sample";
				writer.println("                    <tr id=\"" + rowId + "\" class=\"accordion-content\">");
				writer.println("                        <td colspan=\"20\">");
				writer.println("                            <div class=\"" + logCssClass + "\">");
				if (isTruncated) {
					writer.println("                                <div class=\"truncated-warning\">⚠ Query was truncated in MongoDB logs</div>");
				}
				String contentId = "log-content-" + rowId;
				String buttonId = "btn-" + rowId;
				String formattedDuration = LogRedactionUtil.extractFormattedDuration(op.getSampleLogMessage());
				String formattedPlanningTime = LogRedactionUtil.extractFormattedPlanningTime(op.getSampleLogMessage());
				String formattedStorageTime = LogRedactionUtil.extractFormattedStorageTime(op.getSampleLogMessage());
				String formattedBytesRead = LogRedactionUtil.extractFormattedBytesRead(op.getSampleLogMessage());

				StringBuilder headerText = new StringBuilder();
				if (!formattedDuration.isEmpty()) {
					headerText.append(" (total: ").append(formattedDuration);
					if (!formattedPlanningTime.isEmpty()) {
						headerText.append(", planning: ").append(formattedPlanningTime);
					}
					if (!formattedStorageTime.isEmpty()) {
						headerText.append(", storage: ").append(formattedStorageTime);
					}
				}
				if (!formattedBytesRead.isEmpty()) {
					if (headerText.length() > 0) {
						headerText.append(", ").append(formattedBytesRead);
					} else {
						headerText.append(" (").append(formattedBytesRead);
					}
				}
				if (headerText.length() > 0) {
					headerText.append(")");
				}
				
				writer.println("                                <strong>Slowest Query Log Message" + querySource + headerText.toString() + ":</strong>");
				writer.println("                                <button class=\"pretty-print-btn\" id=\"" + buttonId + "\" onclick=\"togglePrettyPrint('" + buttonId + "', '" + contentId + "')\">Pretty</button><br>");
				writer.println("                                <div class=\"json-content\" id=\"" + contentId + "\">" + enhancedLogMessage + "</div>");
				writer.println("                            </div>");
				writer.println("                        </td>");
				writer.println("                    </tr>");
			}
		});

		writer.println("                </tbody>");
		writer.println("            </table>");
		writer.println("        </div>");
	}

	private static void writeTtlOperationsTable(PrintWriter writer, Accumulator ttlAccumulator) {
		if (ttlAccumulator == null || ttlAccumulator.getAccumulators().isEmpty()) {
			return;
		}

		writer.println("        <h2 id=\"ttl-operations\">TTL Operations Analysis</h2>");

		// Calculate TTL summary
		long totalTtlOps = ttlAccumulator.getAccumulators().values().stream().mapToLong(LogLineAccumulator::getCount)
				.sum();
		long totalDeletedDocs = ttlAccumulator.getAccumulators().values().stream()
				.mapToLong(acc -> acc.getAvgReturned() * acc.getCount()).sum();

		writer.println("        <div class=\"summary\">");
		writer.println("            <h3>TTL Summary</h3>");
		writer.println("            <div class=\"summary-grid\">");
		writer.println("                <div class=\"summary-item\">");
		writer.println("                    <div class=\"summary-label\">Total TTL Operations</div>");
		writer.println(
				"                    <div class=\"summary-value\">" + NUMBER_FORMAT.format(totalTtlOps) + "</div>");
		writer.println("                </div>");
		writer.println("                <div class=\"summary-item\">");
		writer.println("                    <div class=\"summary-label\">Total Documents Deleted</div>");
		writer.println("                    <div class=\"summary-value\">" + NUMBER_FORMAT.format(totalDeletedDocs)
				+ "</div>");
		writer.println("                </div>");
		writer.println("            </div>");
		writer.println("        </div>");

		writer.println("        <div class=\"table-container\">");
		writer.println("            <div class=\"controls\">");
		writer.println(
				"                <input type=\"text\" id=\"ttlFilter\" class=\"filter-input\" placeholder=\"Filter by namespace...\">");
		writer.println(
				"                <button class=\"clear-btn\" onclick=\"clearFilter('ttlFilter', 'ttlTable')\">Clear Filter</button>");
		writer.println(
				"                <button class=\"manage-cols-btn\" onclick=\"openColumnModal('ttlTable')\">Manage Columns</button>");
		writer.println("            </div>");
		writer.println("            <table id=\"ttlTable\">");
		writer.println("                <thead>");
		writer.println("                    <tr>");
		writer.println("                        " + generateTableHeader("ttlTable", 0, "Namespace", "string"));
		writer.println("                        " + generateTableHeader("ttlTable", 1, "Count", "number"));
		writer.println("                        " + generateTableHeader("ttlTable", 2, "Total Deleted", "number"));
		writer.println("                        " + generateTableHeader("ttlTable", 3, "Avg Deleted", "number"));
		writer.println("                        " + generateTableHeader("ttlTable", 4, "Min (ms)", "number"));
		writer.println("                        " + generateTableHeader("ttlTable", 5, "Max (ms)", "number"));
		writer.println("                    </tr>");
		writer.println("                </thead>");
		writer.println("                <tbody>");

		ttlAccumulator.getAccumulators().values().stream()
				.sorted(Comparator.comparingLong(LogLineAccumulator::getCount).reversed()).forEach(acc -> {
					String namespace = acc.toString().split(" ")[0];
					long deletedByThisNamespace = acc.getAvgReturned() * acc.getCount();

					writer.println("                    <tr>");
					writer.println("                        <td class=\"truncated\" title=\"" + escapeHtml(namespace)
							+ "\">" + escapeHtml(truncate(namespace, 50)) + "</td>");
					writer.println("                        <td class=\"number\">"
							+ NUMBER_FORMAT.format(acc.getCount()) + "</td>");
					writer.println("                        <td class=\"number\">"
							+ NUMBER_FORMAT.format(deletedByThisNamespace) + "</td>");
					writer.println("                        <td class=\"number\">"
							+ NUMBER_FORMAT.format(acc.getAvgReturned()) + "</td>");
					writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(acc.getMin())
							+ "</td>");
					writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(acc.getMax())
							+ "</td>");
					writer.println("                    </tr>");
				});

		writer.println("                </tbody>");
		writer.println("            </table>");
		writer.println("        </div>");
	}

	private static void writeQueryHashTable(PrintWriter writer, QueryHashAccumulator queryHashAccumulator, boolean redactQueries) {
		writer.println("        <h2 id=\"query-hash\">Query Hash Analysis</h2>");

		var entries = queryHashAccumulator.getQueryHashEntries().values();

		if (entries.isEmpty()) {
			writer.println("        <p>No query hash entries found.</p>");
			return;
		}

		// Calculate query hash summary
		long totalQueries = entries.stream().mapToLong(QueryHashAccumulatorEntry::getCount).sum();
		long uniqueQueryHashes = entries.size();
		long uniqueNamespaces = queryHashAccumulator.getQueryHashEntries().keySet().stream()
				.map(key -> key.getNamespace()).distinct().count();

		writer.println("        <div class=\"summary\">");
		writer.println("            <h3>Query Hash Summary</h3>");
		writer.println("            <div class=\"summary-grid\">");
		writer.println("                <div class=\"summary-item\">");
		writer.println("                    <div class=\"summary-label\">Total Queries</div>");
		writer.println(
				"                    <div class=\"summary-value\">" + NUMBER_FORMAT.format(totalQueries) + "</div>");
		writer.println("                </div>");
		writer.println("                <div class=\"summary-item\">");
		writer.println("                    <div class=\"summary-label\">Unique Query Hashes</div>");
		writer.println("                    <div class=\"summary-value\">" + NUMBER_FORMAT.format(uniqueQueryHashes)
				+ "</div>");
		writer.println("                </div>");
		writer.println("                <div class=\"summary-item\">");
		writer.println("                    <div class=\"summary-label\">Unique Namespaces</div>");
		writer.println("                    <div class=\"summary-value\">" + NUMBER_FORMAT.format(uniqueNamespaces)
				+ "</div>");
		writer.println("                </div>");
		writer.println("            </div>");
		writer.println("        </div>");

		writer.println("        <div class=\"table-container\">");
		writer.println("            <div class=\"controls\">");
		writer.println(
				"                <input type=\"text\" id=\"queryHashFilter\" class=\"filter-input\" placeholder=\"Filter by query hash, namespace, etc...\">");
		writer.println(
				"                <button class=\"clear-btn\" onclick=\"clearFilter('queryHashFilter', 'queryHashTable')\">Clear Filter</button>");
		writer.println(
				"                <button class=\"expand-btn\" onclick=\"expandAllAccordions('queryHashTable')\">Expand All</button>");
		writer.println(
				"                <button class=\"collapse-btn\" onclick=\"collapseAllAccordions('queryHashTable')\">Collapse All</button>");
		writer.println(
				"                <button class=\"manage-cols-btn\" onclick=\"openColumnModal('queryHashTable')\">Manage Columns</button>");
		writer.println("            </div>");
		writer.println("            <table id=\"queryHashTable\">");
		writer.println("                <thead>");
		writer.println("                    <tr>");
		writer.println("                        " + generateTableHeader("queryHashTable", 0, "Query Hash", "string"));
		writer.println("                        " + generateTableHeader("queryHashTable", 1, "Namespace", "string"));
		writer.println("                        " + generateTableHeader("queryHashTable", 2, "Operation", "string"));
		writer.println("                        " + generateTableHeader("queryHashTable", 3, "App Name", "string"));
		writer.println("                        " + generateTableHeader("queryHashTable", 4, "Count", "number"));
		writer.println("                        " + generateTableHeader("queryHashTable", 5, "Min (ms)", "number"));
		writer.println("                        " + generateTableHeader("queryHashTable", 6, "Max (ms)", "number"));
		writer.println("                        " + generateTableHeader("queryHashTable", 7, "Avg (ms)", "number"));
		writer.println("                        " + generateTableHeader("queryHashTable", 8, "P95 (ms)", "number"));
		writer.println("                        " + generateTableHeader("queryHashTable", 9, "Total (sec)", "number"));
		writer.println("                        " + generateTableHeader("queryHashTable", 10, "Avg Keys Ex", "number"));
		writer.println("                        " + generateTableHeader("queryHashTable", 11, "Avg Docs Ex", "number"));
		writer.println("                        " + generateTableHeader("queryHashTable", 12, "Avg Return", "number"));
		writer.println("                        " + generateTableHeader("queryHashTable", 13, "Ex/Ret Ratio", "number"));
		writer.println("                        " + generateTableHeader("queryHashTable", 14, "Avg Shards", "number"));
		writer.println("                        " + generateTableHeader("queryHashTable", 15, "Avg Read", "number"));
		writer.println("                        " + generateTableHeader("queryHashTable", 16, "Max Read", "number"));
		writer.println("                        " + generateTableHeader("queryHashTable", 17, "Avg Write", "number"));
		writer.println("                        " + generateTableHeader("queryHashTable", 18, "Max Write", "number"));
		writer.println("                        " + generateTableHeader("queryHashTable", 19, "Read Preference", "string"));
		writer.println("                        " + generateTableHeader("queryHashTable", 20, "Read Preference Tags", "string"));
		writer.println("                        " + generateTableHeader("queryHashTable", 21, "Plan Summary", "string"));
		writer.println("                        " + generateTableHeader("queryHashTable", 22, "Avg Plan (ms)", "number"));
		writer.println("                        " + generateTableHeader("queryHashTable", 23, "Replan %", "number"));
		writer.println("                    </tr>");
		writer.println("                </thead>");
		writer.println("                <tbody>");

		entries.stream().sorted(Comparator.comparingLong(QueryHashAccumulatorEntry::getCount).reversed())
				.forEach(entry -> {
					QueryHashKey key = entry.getKey();
					String rowId = "qh-" + Math.abs(key.hashCode());

					writer.println("                    <tr class=\"accordion-row\" onclick=\"toggleAccordion('" + rowId + "')\">");
					writer.println(
							"                        <td class=\"truncated\" title=\"" + escapeHtml(key.getQueryHash())
									+ "\"><span class=\"accordion-toggle\"></span>" + escapeHtml(truncate(key.getQueryHash(), 12))
									+ "</td>");
					writer.println("                        <td class=\"truncated\" title=\""
							+ escapeHtml(key.getNamespace().toString()) + "\">"
							+ escapeHtml(truncate(key.getNamespace().toString(), 40)) + "</td>");
					writer.println("                        <td>" + escapeHtml(key.getOperation()) + "</td>");
					writer.println("                        <td class=\"truncated\">" + (entry.getAppName() != null ? escapeHtml(entry.getAppName()) : "") + "</td>");
					writer.println("                        <td class=\"number\">"
							+ NUMBER_FORMAT.format(entry.getCount()) + "</td>");
					writer.println("                        <td class=\"number\">"
							+ NUMBER_FORMAT.format(entry.getMin()) + "</td>");
					writer.println("                        <td class=\"number\">"
							+ NUMBER_FORMAT.format(entry.getMax()) + "</td>");
					writer.println("                        <td class=\"number\">"
							+ NUMBER_FORMAT.format(entry.getAvg()) + "</td>");
					writer.println("                        <td class=\"number\">"
							+ NUMBER_FORMAT.format(Math.round(entry.getPercentile95())) + "</td>");
					writer.println("                        <td class=\"number\">"
							+ NUMBER_FORMAT.format((entry.getCount() * entry.getAvg()) / 1000) + "</td>");
					writer.println("                        <td class=\"number\">"
							+ NUMBER_FORMAT.format(entry.getAvgKeysExamined()) + "</td>");
					writer.println("                        <td class=\"number\">"
							+ NUMBER_FORMAT.format(entry.getAvgDocsExamined()) + "</td>");
					writer.println("                        <td class=\"number\">"
							+ NUMBER_FORMAT.format(entry.getAvgReturned()) + "</td>");
					writer.println("                        <td class=\"number\">"
							+ NUMBER_FORMAT.format(entry.getScannedReturnRatio()) + "</td>");
					writer.println("                        <td class=\"number\">"
							+ NUMBER_FORMAT.format(entry.getAvgShards()) + "</td>");
					writer.print("                        ");
					writer.println(entry.getFormattedAvgBytesReadCell());
					writer.print("                        ");
					writer.println(entry.getFormattedMaxBytesReadCell());
					writer.print("                        ");
					writer.println(entry.getFormattedAvgBytesWrittenCell());
					writer.print("                        ");
					writer.println(entry.getFormattedMaxBytesWrittenCell());
					writer.println("                        <td class=\"truncated multiline\" title=\""
					        + escapeHtml(entry.getReadPreferenceSummary().replace("<br>", ", ")) + "\">"
					        + entry.getReadPreferenceSummaryTruncated(30) + "</td>");
					String readPrefTagsSummary = entry.getReadPreferenceTagsSummary();
					if (readPrefTagsSummary != null && !readPrefTagsSummary.isEmpty()) {
						writer.println("                        <td class=\"truncated multiline\" title=\""
						        + escapeHtml(readPrefTagsSummary.replace("<br>", ", ")) + "\">"
						        + entry.getReadPreferenceTagsSummaryTruncated(30) + "</td>");
					} else {
						writer.println("                        <td></td>");
					}
					String planSummary = entry.getPlanSummary();
					String compressedPlanSummary = compressPlanSummary(planSummary);
					writer.println("                        <td class=\"wrapped\" title=\""
							+ escapeHtml(planSummary) + "\">" + escapeHtml(compressedPlanSummary) + "</td>");
					writer.println("                        <td class=\"number\">"
							+ NUMBER_FORMAT.format(entry.getAvgPlanningTimeMs()) + "</td>");
					writer.println("                        <td class=\"number\">"
							+ String.format("%.1f", entry.getReplannedPercentage()) + "</td>");
					writer.println("                    </tr>");
					
					// Add accordion content row for sample log message
					if (entry.getSampleLogMessage() != null) {
						String processedLogMessage = LogRedactionUtil.processLogMessage(entry.getSampleLogMessage(), redactQueries);
						String enhancedLogMessage = LogRedactionUtil.enhanceLogMessageForHtml(processedLogMessage);
						enhancedLogMessage = escapeHtml(enhancedLogMessage);
						String querySource = LogRedactionUtil.detectQuerySource(entry.getSampleLogMessage());
						boolean isTruncated = LogRedactionUtil.isLogMessageTruncated(entry.getSampleLogMessage());
						String cssClass = isTruncated ? "log-sample truncated-query" : "log-sample";
						writer.println("                    <tr id=\"" + rowId + "\" class=\"accordion-content\">");
						writer.println("                        <td colspan=\"23\">");
						writer.println("                            <div class=\"" + cssClass + "\">");
						if (isTruncated) {
							writer.println("                                <div class=\"truncated-warning\">⚠ Query was truncated in MongoDB logs</div>");
						}
						writer.println("                                <strong>Query:</strong><br>");
						writer.println("                                " + escapeHtml(entry.getSanitizedQuery()));
						writer.println("                                <br><br>");
						String contentId = "log-content-" + rowId;
						String buttonId = "btn-" + rowId;
						String formattedDuration = LogRedactionUtil.extractFormattedDuration(entry.getSampleLogMessage());
						String formattedPlanningTime = LogRedactionUtil.extractFormattedPlanningTime(entry.getSampleLogMessage());
						String formattedStorageTime = LogRedactionUtil.extractFormattedStorageTime(entry.getSampleLogMessage());
						String formattedBytesRead = LogRedactionUtil.extractFormattedBytesRead(entry.getSampleLogMessage());

						StringBuilder headerText = new StringBuilder();
						if (!formattedDuration.isEmpty()) {
							headerText.append(" (total: ").append(formattedDuration);
							if (!formattedPlanningTime.isEmpty()) {
								headerText.append(", planning: ").append(formattedPlanningTime);
							}
							if (!formattedStorageTime.isEmpty()) {
								headerText.append(", storage: ").append(formattedStorageTime);
							}
						}
						if (!formattedBytesRead.isEmpty()) {
							if (headerText.length() > 0) {
								headerText.append(", ").append(formattedBytesRead);
							} else {
								headerText.append(" (").append(formattedBytesRead);
							}
						}
						if (headerText.length() > 0) {
							headerText.append(")");
						}
						
						writer.println("                                <strong>Slowest Query Log Message" + querySource + headerText.toString() + ":</strong>");
						writer.println("                                <button class=\"pretty-print-btn\" id=\"" + buttonId + "\" onclick=\"togglePrettyPrint('" + buttonId + "', '" + contentId + "')\">Pretty</button><br>");
						writer.println("                                <div class=\"json-content\" id=\"" + contentId + "\">" + enhancedLogMessage + "</div>");
						writer.println("                            </div>");
						writer.println("                        </td>");
						writer.println("                    </tr>");
					}
				});

		writer.println("                </tbody>");
		writer.println("            </table>");
		writer.println("        </div>");
	}

	private static void writeSlowPlanningTable(PrintWriter writer, SlowPlanningAccumulator slowPlanningAccumulator, boolean redactQueries) {
		if (slowPlanningAccumulator == null || slowPlanningAccumulator.getSize() == 0) {
			return;
		}

		writer.println("        <h2 id=\"slow-planning\">Slow Query Planning Analysis</h2>");

		var topEntries = slowPlanningAccumulator.getTopEntries();

		writer.println("        <div class=\"summary\">");
		writer.println("            <h3>Slow Planning Summary</h3>");
		writer.println("            <div class=\"summary-grid\">");
		writer.println("                <div class=\"summary-item\">");
		writer.println("                    <div class=\"summary-label\">Total with Planning Time</div>");
		writer.println("                    <div class=\"summary-value\">" + NUMBER_FORMAT.format(slowPlanningAccumulator.getSize()) + "</div>");
		writer.println("                </div>");
		writer.println("                <div class=\"summary-item\">");
		writer.println("                    <div class=\"summary-label\">Showing Top</div>");
		writer.println("                    <div class=\"summary-value\">" + NUMBER_FORMAT.format(topEntries.size()) + "</div>");
		writer.println("                </div>");
		writer.println("            </div>");
		writer.println("        </div>");

		writer.println("        <div class=\"table-container\">");
		writer.println("            <div class=\"controls\">");
		writer.println("                <input type=\"text\" id=\"slowPlanningFilter\" class=\"filter-input\" placeholder=\"Filter by namespace, operation, etc...\">");
		writer.println("                <button class=\"clear-btn\" onclick=\"clearFilter('slowPlanningFilter', 'slowPlanningTable')\">Clear Filter</button>");
		writer.println("                <button class=\"expand-btn\" onclick=\"expandAllAccordions('slowPlanningTable')\">Expand All</button>");
		writer.println("                <button class=\"collapse-btn\" onclick=\"collapseAllAccordions('slowPlanningTable')\">Collapse All</button>");
		writer.println("                <button class=\"manage-cols-btn\" onclick=\"openColumnModal('slowPlanningTable')\">Manage Columns</button>");
		writer.println("            </div>");
		writer.println("            <table id=\"slowPlanningTable\">");
		writer.println("                <thead>");
		writer.println("                    <tr>");
		writer.println("                        " + generateTableHeader("slowPlanningTable", 0, "Timestamp", "string"));
		writer.println("                        " + generateTableHeader("slowPlanningTable", 1, "Planning Time", "number"));
		writer.println("                        " + generateTableHeader("slowPlanningTable", 2, "Namespace", "string"));
		writer.println("                        " + generateTableHeader("slowPlanningTable", 3, "Operation", "string"));
		writer.println("                        " + generateTableHeader("slowPlanningTable", 4, "Query Hash", "string"));
		writer.println("                        " + generateTableHeader("slowPlanningTable", 5, "App Name", "string"));
		writer.println("                        " + generateTableHeader("slowPlanningTable", 6, "Plan Summary", "string"));
		writer.println("                    </tr>");
		writer.println("                </thead>");
		writer.println("                <tbody>");

		topEntries.forEach(entry -> {
			String sanitizedQuery = redactQueries ? LogRedactionUtil.redactLogMessage(entry.getSanitizedQuery(), true) : entry.getSanitizedQuery();
			String rowId = "sp-" + Math.abs(entry.getLogMessage() != null ? entry.getLogMessage().hashCode() : entry.hashCode());

			writer.println("                    <tr class=\"accordion-row\" onclick=\"toggleAccordion('" + rowId + "')\">");
			writer.println("                        <td><span class=\"accordion-toggle\"></span>" + escapeHtml(entry.getTimestamp()) + "</td>");
			writer.println("                        <td class=\"number\" data-sort-value=\"" + entry.getPlanningTimeMicros() + "\">" + escapeHtml(entry.getFormattedPlanningTime()) + "</td>");
			writer.println("                        <td class=\"truncated\" title=\"" + escapeHtml(entry.getNamespace()) + "\">"
					+ escapeHtml(truncate(entry.getNamespace(), 40)) + "</td>");
			writer.println("                        <td>" + escapeHtml(entry.getOperation()) + "</td>");
			writer.println("                        <td class=\"truncated\" title=\"" + escapeHtml(entry.getQueryHash() != null ? entry.getQueryHash() : "") + "\">"
					+ escapeHtml(truncate(entry.getQueryHash() != null ? entry.getQueryHash() : "-", 12)) + "</td>");
			writer.println("                        <td class=\"truncated\" title=\"" + escapeHtml(entry.getAppName() != null ? entry.getAppName() : "") + "\">"
					+ escapeHtml(truncate(entry.getAppName() != null ? entry.getAppName() : "-", 20)) + "</td>");
			writer.println("                        <td class=\"truncated\" title=\"" + escapeHtml(entry.getPlanSummary()) + "\">"
					+ escapeHtml(truncate(entry.getPlanSummary(), 30)) + "</td>");
			writer.println("                    </tr>");

			// Add accordion content row with query and log message
			if (entry.getLogMessage() != null && !entry.getLogMessage().isEmpty()) {
				String processedLogMessage = LogRedactionUtil.processLogMessage(entry.getLogMessage(), redactQueries);
				String enhancedLogMessage = LogRedactionUtil.enhanceLogMessageForHtml(processedLogMessage);
				enhancedLogMessage = escapeHtml(enhancedLogMessage);
				boolean isTruncated = LogRedactionUtil.isLogMessageTruncated(entry.getLogMessage());
				String cssClass = isTruncated ? "log-sample truncated-query" : "log-sample";
				writer.println("                    <tr id=\"" + rowId + "\" class=\"accordion-content\">");
				writer.println("                        <td colspan=\"7\">");
				writer.println("                            <div class=\"" + cssClass + "\">");
				if (isTruncated) {
					writer.println("                                <div class=\"truncated-warning\">⚠ Query was truncated in MongoDB logs</div>");
				}
				writer.println("                                <strong>Query:</strong><br>");
				writer.println("                                " + escapeHtml(sanitizedQuery));
				writer.println("                                <br><br>");
				writer.println("                                <strong>Log Entry:</strong><br>");
				writer.println("                                " + enhancedLogMessage);
				writer.println("                            </div>");
				writer.println("                        </td>");
				writer.println("                    </tr>");
			}
		});

		writer.println("                </tbody>");
		writer.println("            </table>");
		writer.println("        </div>");
	}

	private static void writeOperationStatsTable(PrintWriter writer,
			Map<String, java.util.concurrent.atomic.AtomicLong> operationTypeStats) {
		if (operationTypeStats == null || operationTypeStats.isEmpty()) {
			return;
		}

		writer.println("        <h2 id=\"operation-stats\">Operation Type Statistics</h2>");

		long totalOps = operationTypeStats.values().stream().mapToLong(java.util.concurrent.atomic.AtomicLong::get)
				.sum();

		writer.println("        <div class=\"table-container\">");
		writer.println("            <div class=\"controls\">");
		writer.println(
				"                <input type=\"text\" id=\"opStatsFilter\" class=\"filter-input\" placeholder=\"Filter by operation type...\">");
		writer.println(
				"                <button class=\"clear-btn\" onclick=\"clearFilter('opStatsFilter', 'opStatsTable')\">Clear Filter</button>");
		writer.println("            </div>");
		writer.println("            <table id=\"opStatsTable\">");
		writer.println("                <thead>");
		writer.println("                    <tr>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('opStatsTable', 0, 'string')\">Operation Type</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('opStatsTable', 1, 'number')\">Count</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('opStatsTable', 2, 'number')\">Percentage</th>");
		writer.println("                    </tr>");
		writer.println("                </thead>");
		writer.println("                <tbody>");

		operationTypeStats.entrySet().stream()
				.sorted(Map.Entry.<String, java.util.concurrent.atomic.AtomicLong>comparingByValue(
						(a, b) -> Long.compare(b.get(), a.get())))
				.forEach(entry -> {
					long count = entry.getValue().get();
					double percentage = (count * 100.0) / Math.max(totalOps, 1);

					writer.println("                    <tr>");
					writer.println("                        <td>" + escapeHtml(entry.getKey()) + "</td>");
					writer.println(
							"                        <td class=\"number\">" + NUMBER_FORMAT.format(count) + "</td>");
					writer.println("                        <td class=\"number\">" + String.format("%.1f%%", percentage)
							+ "</td>");
					writer.println("                    </tr>");
				});

		writer.println("                </tbody>");
		writer.println("            </table>");
		writer.println("        </div>");
	}

	private static void writePlanCacheTable(PrintWriter writer, PlanCacheAccumulator planCacheAccumulator, boolean redactQueries) {
		writer.println("        <h2 id=\"plan-cache\">Plan Cache Analysis</h2>");

		// Get all entries, but separate those with and without valid plan summaries
		var allEntries = planCacheAccumulator.getPlanCacheEntries().values().stream()
				.collect(java.util.stream.Collectors.toList());
		
		var entriesWithPlanSummary = allEntries.stream().filter(
				entry -> entry.getKey().getPlanSummary() != null && !"UNKNOWN".equals(entry.getKey().getPlanSummary()))
				.collect(java.util.stream.Collectors.toList());
		
		var entriesWithoutPlanSummary = allEntries.stream().filter(
				entry -> entry.getKey().getPlanSummary() == null || "UNKNOWN".equals(entry.getKey().getPlanSummary()))
				.collect(java.util.stream.Collectors.toList());

		if (allEntries.isEmpty()) {
			writer.println("        <p>No plan cache entries found.</p>");
			return;
		}
		
		// Use all entries for the table, but note which ones lack plan summaries
		var filteredEntries = allEntries;

		// Calculate plan cache summary (using filtered entries)
		long totalQueries = filteredEntries.stream().mapToLong(PlanCacheAccumulatorEntry::getCount).sum();
		long collScanQueries = filteredEntries.stream().filter(entry -> entry.isCollectionScan())
				.mapToLong(PlanCacheAccumulatorEntry::getCount).sum();
		long totalReplanned = filteredEntries.stream().mapToLong(PlanCacheAccumulatorEntry::getReplannedCount).sum();
		long uniquePlanKeys = filteredEntries.size();

		writer.println("        <div class=\"summary\">");
		writer.println("            <h3>Plan Cache Summary</h3>");
		writer.println("            <div class=\"summary-grid\">");
		writer.println("                <div class=\"summary-item\">");
		writer.println("                    <div class=\"summary-label\">Total Queries</div>");
		writer.println(
				"                    <div class=\"summary-value\">" + NUMBER_FORMAT.format(totalQueries) + "</div>");
		writer.println("                </div>");
		writer.println("                <div class=\"summary-item\">");
		writer.println("                    <div class=\"summary-label\">Unique Plan Cache Keys</div>");
		writer.println(
				"                    <div class=\"summary-value\">" + NUMBER_FORMAT.format(uniquePlanKeys) + "</div>");
		writer.println("                </div>");
		writer.println("                <div class=\"summary-item\">");
		writer.println("                    <div class=\"summary-label\">Collection Scans</div>");
		writer.println("                    <div class=\"summary-value\">" + NUMBER_FORMAT.format(collScanQueries)
				+ " (" + String.format("%.1f%%", (collScanQueries * 100.0) / Math.max(totalQueries, 1)) + ")</div>");
		writer.println("                </div>");
		writer.println("                <div class=\"summary-item\">");
		writer.println("                    <div class=\"summary-label\">Replanned Queries</div>");
		writer.println("                    <div class=\"summary-value\">" + NUMBER_FORMAT.format(totalReplanned) + " ("
				+ String.format("%.1f%%", (totalReplanned * 100.0) / Math.max(totalQueries, 1)) + ")</div>");
		writer.println("                </div>");
		writer.println("            </div>");
		writer.println("        </div>");
		
		// Add informational note if there are entries without plan summaries
		if (!entriesWithoutPlanSummary.isEmpty()) {
			long entriesWithoutPlanSummaryCount = entriesWithoutPlanSummary.stream().mapToLong(PlanCacheAccumulatorEntry::getCount).sum();
			writer.println("        <div class=\"summary\">");
			writer.println("            <h3>Note</h3>");
			writer.println("            <p>" + NUMBER_FORMAT.format(entriesWithoutPlanSummaryCount) + " queries (" + 
			             NUMBER_FORMAT.format(entriesWithoutPlanSummary.size()) + " unique plan cache keys) do not have plan summary information. " +
			             "This may indicate older MongoDB versions or specific operation types that don't provide plan summaries.</p>");
			writer.println("        </div>");
		}

		writer.println("        <div class=\"table-container\">");
		writer.println("            <div class=\"controls\">");
		writer.println(
				"                <input type=\"text\" id=\"planCacheFilter\" class=\"filter-input\" placeholder=\"Filter by namespace, plan summary, etc...\">");
		writer.println(
				"                <button class=\"clear-btn\" onclick=\"clearFilter('planCacheFilter', 'planCacheTable')\">Clear Filter</button>");
		writer.println(
				"                <button class=\"expand-btn\" onclick=\"expandAllAccordions('planCacheTable')\">Expand All</button>");
		writer.println(
				"                <button class=\"collapse-btn\" onclick=\"collapseAllAccordions('planCacheTable')\">Collapse All</button>");
		writer.println("            </div>");
		writer.println("            <table id=\"planCacheTable\">");
		writer.println("                <thead>");
		writer.println("                    <tr>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('planCacheTable', 0, 'string')\">Namespace</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('planCacheTable', 1, 'string')\">Query Hash</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('planCacheTable', 2, 'string')\">Plan Summary</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('planCacheTable', 3, 'number')\">Count</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('planCacheTable', 4, 'number')\">Min (ms)</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('planCacheTable', 5, 'number')\">Max (ms)</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('planCacheTable', 6, 'number')\">Avg (ms)</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('planCacheTable', 7, 'number')\">P95 (ms)</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('planCacheTable', 8, 'number')\">Avg Keys Ex</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('planCacheTable', 9, 'number')\">Avg Docs Ex</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('planCacheTable', 10, 'number')\">Avg Return</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('planCacheTable', 11, 'number')\">Avg Plan (ms)</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('planCacheTable', 12, 'number')\">Replan %</th>");
		writer.println("                    </tr>");
		writer.println("                </thead>");
		writer.println("                <tbody>");

		filteredEntries.stream().sorted(Comparator.comparingLong(PlanCacheAccumulatorEntry::getCount).reversed())
				.forEach(entry -> {
					PlanCacheKey key = entry.getKey();
					String rowId = "pc-" + Math.abs(key.hashCode());

					String rowClass = "accordion-row";
					if (key.getPlanSummary() != null && key.getPlanSummary().contains("COLLSCAN")) {
						rowClass += " collscan";
					}

					writer.println("                    <tr class=\"" + rowClass + "\" onclick=\"toggleAccordion('" + rowId + "')\">");
					writer.println("                        <td class=\"truncated\" title=\""
							+ escapeHtml(key.getNamespace().toString()) + "\"><span class=\"accordion-toggle\"></span>" + escapeHtml(truncate(key.getNamespace().toString(), 40)) 
							+ "</td>");
					writer.println(
							"                        <td class=\"truncated\" title=\"" + escapeHtml(key.getQueryHash())
									+ "\">" + escapeHtml(truncate(key.getQueryHash(), 10)) + "</td>");
					String compressedPlanSummary = compressPlanSummary(key.getPlanSummary());
					writer.println("                        <td class=\"wrapped\" title=\""
							+ escapeHtml(key.getPlanSummary()) + "\">" + escapeHtml(compressedPlanSummary)
							+ "</td>");
					// Removed Plan Type column
					writer.println("                        <td class=\"number\">"
							+ NUMBER_FORMAT.format(entry.getCount()) + "</td>");
					writer.println("                        <td class=\"number\">"
							+ NUMBER_FORMAT.format(entry.getMin()) + "</td>");
					writer.println("                        <td class=\"number\">"
							+ NUMBER_FORMAT.format(entry.getMax()) + "</td>");
					writer.println("                        <td class=\"number\">"
							+ NUMBER_FORMAT.format(entry.getAvg()) + "</td>");
					writer.println("                        <td class=\"number\">"
							+ NUMBER_FORMAT.format(Math.round(entry.getPercentile95())) + "</td>");
					writer.println("                        <td class=\"number\">"
							+ NUMBER_FORMAT.format(entry.getAvgKeysExamined()) + "</td>");
					writer.println("                        <td class=\"number\">"
							+ NUMBER_FORMAT.format(entry.getAvgDocsExamined()) + "</td>");
					writer.println("                        <td class=\"number\">"
							+ NUMBER_FORMAT.format(entry.getAvgReturned()) + "</td>");
					writer.println("                        <td class=\"number\">"
							+ NUMBER_FORMAT.format(entry.getAvgPlanningTimeMs()) + "</td>");
					writer.println("                        <td class=\"number\">"
							+ String.format("%.1f%%", entry.getReplannedPercentage()) + "</td>");
					writer.println("                    </tr>");
					
					// Add accordion content row for sample log message
					if (entry.getSampleLogMessage() != null) {
						String processedLogMessage = LogRedactionUtil.processLogMessage(entry.getSampleLogMessage(), redactQueries);
						String enhancedLogMessage = LogRedactionUtil.enhanceLogMessageForHtml(processedLogMessage);
						enhancedLogMessage = escapeHtml(enhancedLogMessage);
						String querySource = LogRedactionUtil.detectQuerySource(entry.getSampleLogMessage());
						boolean isTruncated = LogRedactionUtil.isLogMessageTruncated(entry.getSampleLogMessage());
						String cssClass = isTruncated ? "log-sample truncated-query" : "log-sample";
						writer.println("                    <tr id=\"" + rowId + "\" class=\"accordion-content\">");
						writer.println("                        <td colspan=\"18\">");
						writer.println("                            <div class=\"" + cssClass + "\">");
						if (isTruncated) {
							writer.println("                                <div class=\"truncated-warning\">⚠ Query was truncated in MongoDB logs</div>");
						}
						String contentId = "log-content-" + rowId;
						String buttonId = "btn-" + rowId;
						String formattedDuration = LogRedactionUtil.extractFormattedDuration(entry.getSampleLogMessage());
						String formattedPlanningTime = LogRedactionUtil.extractFormattedPlanningTime(entry.getSampleLogMessage());
						String formattedStorageTime = LogRedactionUtil.extractFormattedStorageTime(entry.getSampleLogMessage());
						String formattedBytesRead = LogRedactionUtil.extractFormattedBytesRead(entry.getSampleLogMessage());

						StringBuilder headerText = new StringBuilder();
						if (!formattedDuration.isEmpty()) {
							headerText.append(" (total: ").append(formattedDuration);
							if (!formattedPlanningTime.isEmpty()) {
								headerText.append(", planning: ").append(formattedPlanningTime);
							}
							if (!formattedStorageTime.isEmpty()) {
								headerText.append(", storage: ").append(formattedStorageTime);
							}
						}
						if (!formattedBytesRead.isEmpty()) {
							if (headerText.length() > 0) {
								headerText.append(", ").append(formattedBytesRead);
							} else {
								headerText.append(" (").append(formattedBytesRead);
							}
						}
						if (headerText.length() > 0) {
							headerText.append(")");
						}
						
						writer.println("                                <strong>Slowest Query Log Message" + querySource + headerText.toString() + ":</strong>");
						writer.println("                                <button class=\"pretty-print-btn\" id=\"" + buttonId + "\" onclick=\"togglePrettyPrint('" + buttonId + "', '" + contentId + "')\">Pretty</button><br>");
						writer.println("                                <div class=\"json-content\" id=\"" + contentId + "\">" + enhancedLogMessage + "</div>");
						writer.println("                            </div>");
						writer.println("                        </td>");
						writer.println("                    </tr>");
					}
				});

		writer.println("                </tbody>");
		writer.println("            </table>");
		writer.println("        </div>");
	}
	
	private static void writeErrorCodesTable(PrintWriter writer, ErrorCodeAccumulator errorCodeAccumulator, boolean redactQueries) {
	    if (errorCodeAccumulator == null || !errorCodeAccumulator.hasErrors()) {
	        return;
	    }

	    writer.println("        <h2 id=\"error-codes\">Error Codes Analysis</h2>");

	    var entries = errorCodeAccumulator.getErrorCodeEntries().values();
	    long totalErrors = errorCodeAccumulator.getTotalErrorCount();

	    writer.println("        <div class=\"summary\">");
	    writer.println("            <h3>Error Summary</h3>");
	    writer.println("            <div class=\"summary-grid\">");
	    writer.println("                <div class=\"summary-item\">");
	    writer.println("                    <div class=\"summary-label\">Total Error Occurrences</div>");
	    writer.println(
	            "                    <div class=\"summary-value\">" + NUMBER_FORMAT.format(totalErrors) + "</div>");
	    writer.println("                </div>");
	    writer.println("                <div class=\"summary-item\">");
	    writer.println("                    <div class=\"summary-label\">Unique Error Codes</div>");
	    writer.println("                    <div class=\"summary-value\">" + NUMBER_FORMAT.format(entries.size())
	            + "</div>");
	    writer.println("                </div>");
	    writer.println("            </div>");
	    writer.println("        </div>");

	    writer.println("        <div class=\"table-container\">");
	    writer.println("            <div class=\"controls\">");
	    writer.println(
	            "                <input type=\"text\" id=\"errorCodesFilter\" class=\"filter-input\" placeholder=\"Filter by error code name...\">");
	    writer.println(
	            "                <button class=\"clear-btn\" onclick=\"clearFilter('errorCodesFilter', 'errorCodesTable')\">Clear Filter</button>");
	    writer.println("            </div>");
	    writer.println("            <table id=\"errorCodesTable\">");
	    writer.println("                <thead>");
	    writer.println("                    <tr>");
	    writer.println(
	            "                        <th class=\"sortable\" onclick=\"sortTable('errorCodesTable', 0, 'string')\">Code Name</th>");
	    writer.println(
	            "                        <th class=\"sortable\" onclick=\"sortTable('errorCodesTable', 1, 'number')\">Error Code</th>");
	    writer.println(
	            "                        <th class=\"sortable\" onclick=\"sortTable('errorCodesTable', 2, 'number')\">Count</th>");
	    writer.println(
	            "                        <th class=\"sortable\" onclick=\"sortTable('errorCodesTable', 3, 'number')\">Percentage</th>");
	    writer.println(
	            "                        <th class=\"sortable\" onclick=\"sortTable('errorCodesTable', 4, 'string')\">Sample Error Message</th>");
	    writer.println("                    </tr>");
	    writer.println("                </thead>");
	    writer.println("                <tbody>");

	    entries.stream().sorted(Comparator.comparingLong(ErrorCodeAccumulator.ErrorCodeEntry::getCount).reversed())
	            .forEach(entry -> {
	                double percentage = (entry.getCount() * 100.0) / Math.max(totalErrors, 1);

	                writer.println("                    <tr>");
	                writer.println("                        <td>" + escapeHtml(entry.getCodeName()) + "</td>");
	                writer.println("                        <td class=\"number\">"
	                        + (entry.getErrorCode() != null ? entry.getErrorCode().toString() : "unknown") + "</td>");
	                writer.println("                        <td class=\"number\">"
	                        + NUMBER_FORMAT.format(entry.getCount()) + "</td>");
	                writer.println("                        <td class=\"number\">" + String.format("%.1f%%", percentage)
	                        + "</td>");
	                String sampleErrorMessage = entry.getSampleErrorMessage();
	                if (sampleErrorMessage != null) {
	                    String processedErrorMessage = LogRedactionUtil.processLogMessage(sampleErrorMessage, redactQueries);
	                    writer.println("                        <td class=\"truncated\" title=\""
	                            + escapeHtml(processedErrorMessage) + "\">"
	                            + escapeHtml(truncate(processedErrorMessage, 100)) + "</td>");
	                } else {
	                    writer.println("                        <td>N/A</td>");
	                }
	                writer.println("                    </tr>");
	            });

	    writer.println("                </tbody>");
	    writer.println("            </table>");
	    writer.println("        </div>");
	}

	private static void writeIndexStatsTable(PrintWriter writer, IndexStatsAccumulator indexStatsAccumulator, boolean redactQueries) {
	    if (indexStatsAccumulator == null || !indexStatsAccumulator.hasIndexStats()) {
	        return;
	    }

	    writer.println("        <h2 id=\"index-stats\">Index Usage Statistics</h2>");

	    var entries = indexStatsAccumulator.getIndexStatsEntries().values();
	    long totalOperations = indexStatsAccumulator.getTotalOperations();
	    long collectionScanOps = indexStatsAccumulator.getCollectionScanOperations();

	    writer.println("        <div class=\"summary\">");
	    writer.println("            <h3>Index Usage Summary</h3>");
	    writer.println("            <div class=\"summary-grid\">");
	    writer.println("                <div class=\"summary-item\">");
	    writer.println("                    <div class=\"summary-label\">Total Operations</div>");
	    writer.println("                    <div class=\"summary-value\">" + NUMBER_FORMAT.format(totalOperations) + "</div>");
	    writer.println("                </div>");
	    writer.println("                <div class=\"summary-item\">");
	    writer.println("                    <div class=\"summary-label\">Unique Index Usage Patterns</div>");
	    writer.println("                    <div class=\"summary-value\">" + NUMBER_FORMAT.format(indexStatsAccumulator.getUniqueIndexUsagePatterns()) + "</div>");
	    writer.println("                </div>");
	    writer.println("                <div class=\"summary-item\">");
	    writer.println("                    <div class=\"summary-label\">Collection Scans</div>");
	    writer.println("                    <div class=\"summary-value\">" + NUMBER_FORMAT.format(collectionScanOps)
	            + " (" + String.format("%.1f%%", (collectionScanOps * 100.0) / Math.max(totalOperations, 1)) + ")</div>");
	    writer.println("                </div>");
	    writer.println("            </div>");
	    writer.println("        </div>");

	    writer.println("        <div class=\"table-container\">");
	    writer.println("            <div class=\"controls\">");
	    writer.println("                <input type=\"text\" id=\"indexStatsFilter\" class=\"filter-input\" placeholder=\"Filter by namespace, plan summary...\">");
	    writer.println("                <button class=\"clear-btn\" onclick=\"clearFilter('indexStatsFilter', 'indexStatsTable')\">Clear Filter</button>");
	    writer.println("            </div>");
	    writer.println("            <table id=\"indexStatsTable\">");
	    writer.println("                <thead>");
	    writer.println("                    <tr>");
	    writer.println("                        <th class=\"sortable\" onclick=\"sortTable('indexStatsTable', 0, 'string')\">Namespace</th>");
	    writer.println("                        <th class=\"sortable\" onclick=\"sortTable('indexStatsTable', 1, 'string')\">Plan Summary</th>");
	    writer.println("                        <th class=\"sortable\" onclick=\"sortTable('indexStatsTable', 2, 'number')\">Count</th>");
	    writer.println("                        <th class=\"sortable\" onclick=\"sortTable('indexStatsTable', 3, 'number')\">Min (ms)</th>");
	    writer.println("                        <th class=\"sortable\" onclick=\"sortTable('indexStatsTable', 4, 'number')\">Max (ms)</th>");
	    writer.println("                        <th class=\"sortable\" onclick=\"sortTable('indexStatsTable', 5, 'number')\">Avg (ms)</th>");
	    writer.println("                        <th class=\"sortable\" onclick=\"sortTable('indexStatsTable', 6, 'number')\">P95 (ms)</th>");
	    writer.println("                        <th class=\"sortable\" onclick=\"sortTable('indexStatsTable', 7, 'number')\">Total (sec)</th>");
	    writer.println("                        <th class=\"sortable\" onclick=\"sortTable('indexStatsTable', 8, 'number')\">Avg Keys Ex</th>");
	    writer.println("                        <th class=\"sortable\" onclick=\"sortTable('indexStatsTable', 9, 'number')\">Avg Docs Ex</th>");
	    writer.println("                        <th class=\"sortable\" onclick=\"sortTable('indexStatsTable', 10, 'number')\">Avg Return</th>");
	    writer.println("                        <th class=\"sortable\" onclick=\"sortTable('indexStatsTable', 11, 'number')\">Ex/Ret Ratio</th>");
	    writer.println("                    </tr>");
	    writer.println("                </thead>");
	    writer.println("                <tbody>");

	    entries.stream().sorted(Comparator.comparingLong(IndexStatsEntry::getCount).reversed())
	            .forEach(entry -> {
	                String rowClass = entry.isCollectionScan() ? "collscan" : "";
	                
	                writer.println("                    <tr class=\"" + rowClass + "\">");
	                writer.println("                        <td class=\"truncated\" title=\""
	                        + escapeHtml(entry.getNamespace().toString()) + "\">"
	                        + escapeHtml(truncate(entry.getNamespace().toString(), 50)) + "</td>");
	                
	                String planSummary = entry.getPlanSummary();
	                String compressedPlanSummary = compressPlanSummary(planSummary);
	                writer.println("                        <td class=\"wrapped\" title=\""
	                        + escapeHtml(planSummary) + "\">" + escapeHtml(compressedPlanSummary) + "</td>");
	                
	                writer.println("                        <td class=\"number\">"
	                        + NUMBER_FORMAT.format(entry.getCount()) + "</td>");
	                writer.println("                        <td class=\"number\">"
	                        + NUMBER_FORMAT.format(entry.getMinDurationMs()) + "</td>");
	                writer.println("                        <td class=\"number\">"
	                        + NUMBER_FORMAT.format(entry.getMaxDurationMs()) + "</td>");
	                writer.println("                        <td class=\"number\">"
	                        + NUMBER_FORMAT.format(entry.getAvgDurationMs()) + "</td>");
	                writer.println("                        <td class=\"number\">"
	                        + NUMBER_FORMAT.format(Math.round(entry.getPercentile95())) + "</td>");
	                writer.println("                        <td class=\"number\">"
	                        + NUMBER_FORMAT.format(entry.getTotalDurationSec()) + "</td>");
	                writer.println("                        <td class=\"number\">"
	                        + NUMBER_FORMAT.format(entry.getAvgKeysExamined()) + "</td>");
	                writer.println("                        <td class=\"number\">"
	                        + NUMBER_FORMAT.format(entry.getAvgDocsExamined()) + "</td>");
	                writer.println("                        <td class=\"number\">"
	                        + NUMBER_FORMAT.format(entry.getAvgReturned()) + "</td>");
	                writer.println("                        <td class=\"number\">"
	                        + NUMBER_FORMAT.format(entry.getExaminedToReturnedRatio()) + "</td>");
	                writer.println("                    </tr>");
	            });

	    writer.println("                </tbody>");
	    writer.println("            </table>");
	    writer.println("        </div>");
	}

	private static void writeTransactionTable(PrintWriter writer, TransactionAccumulator transactionAccumulator) {
	    writer.println("        <h2 id=\"transactions\">Transaction Analysis</h2>");

	    var entries = transactionAccumulator.getTransactionEntries().values();
	    long totalTransactions = transactionAccumulator.getTotalTransactionCount();

	    writer.println("        <div class=\"summary\">");
	    writer.println("            <h3>Transaction Summary</h3>");
	    writer.println("            <div class=\"summary-grid\">");
	    writer.println("                <div class=\"summary-item\">");
	    writer.println("                    <div class=\"summary-label\">Total Transactions</div>");
	    writer.println(
	            "                    <div class=\"summary-value\">" + NUMBER_FORMAT.format(totalTransactions) + "</div>");
	    writer.println("                </div>");
	    writer.println("                <div class=\"summary-item\">");
	    writer.println("                    <div class=\"summary-label\">Unique Combinations</div>");
	    writer.println("                    <div class=\"summary-value\">" + NUMBER_FORMAT.format(entries.size())
	            + "</div>");
	    writer.println("                </div>");
	    writer.println("            </div>");
	    writer.println("        </div>");

	    writer.println("        <div class=\"table-container\">");
	    writer.println("            <div class=\"controls\">");
	    writer.println(
	            "                <input type=\"text\" id=\"transactionFilter\" class=\"filter-input\" placeholder=\"Filter by termination cause, commit type...\">");
	    writer.println(
	            "                <button class=\"clear-btn\" onclick=\"clearFilter('transactionFilter', 'transactionTable')\">Clear Filter</button>");
	    writer.println("            </div>");
	    writer.println("            <table id=\"transactionTable\">");
	    writer.println("                <thead>");
	    writer.println("                    <tr>");
	    writer.println(
	            "                        <th class=\"sortable\" onclick=\"sortTable('transactionTable', 0, 'string')\">Retry Counter</th>");
	    writer.println(
	            "                        <th class=\"sortable\" onclick=\"sortTable('transactionTable', 1, 'string')\">Termination Cause</th>");
	    writer.println(
	            "                        <th class=\"sortable\" onclick=\"sortTable('transactionTable', 2, 'string')\">Commit Type</th>");
	    writer.println(
	            "                        <th class=\"sortable\" onclick=\"sortTable('transactionTable', 3, 'number')\">Count</th>");
	    writer.println(
	            "                        <th class=\"sortable\" onclick=\"sortTable('transactionTable', 4, 'number')\">Min Duration (ms)</th>");
	    writer.println(
	            "                        <th class=\"sortable\" onclick=\"sortTable('transactionTable', 5, 'number')\">Max Duration (ms)</th>");
	    writer.println(
	            "                        <th class=\"sortable\" onclick=\"sortTable('transactionTable', 6, 'number')\">Avg Duration (ms)</th>");
	    writer.println(
	            "                        <th class=\"sortable\" onclick=\"sortTable('transactionTable', 7, 'number')\">Max Commit (ms)</th>");
	    writer.println(
	            "                        <th class=\"sortable\" onclick=\"sortTable('transactionTable', 8, 'number')\">Avg Commit (ms)</th>");
	    writer.println(
	            "                        <th class=\"sortable\" onclick=\"sortTable('transactionTable', 9, 'number')\">Max Time Active (ms)</th>");
	    writer.println(
	            "                        <th class=\"sortable\" onclick=\"sortTable('transactionTable', 10, 'number')\">Avg Time Active (ms)</th>");
	    writer.println(
	            "                        <th class=\"sortable\" onclick=\"sortTable('transactionTable', 11, 'number')\">Max Time Inactive (ms)</th>");
	    writer.println(
	            "                        <th class=\"sortable\" onclick=\"sortTable('transactionTable', 12, 'number')\">Avg Time Inactive (ms)</th>");
	    writer.println("                    </tr>");
	    writer.println("                </thead>");
	    writer.println("                <tbody>");

	    entries.stream().sorted(Comparator.comparingLong(com.mongodb.log.parser.accumulator.TransactionEntry::getCount).reversed())
	            .forEach(entry -> {
	                var key = entry.getKey();

	                writer.println("                    <tr>");
	                writer.println("                        <td>" + 
	                    (key.getTxnRetryCounter() != null ? key.getTxnRetryCounter().toString() : "null") + "</td>");
	                writer.println("                        <td>" + 
	                    escapeHtml(key.getTerminationCause() != null ? key.getTerminationCause() : "null") + "</td>");
	                writer.println("                        <td>" + 
	                    escapeHtml(key.getCommitType() != null ? key.getCommitType() : "null") + "</td>");
	                writer.println("                        <td class=\"number\">"
	                        + NUMBER_FORMAT.format(entry.getCount()) + "</td>");
	                writer.println("                        <td class=\"number\">"
	                        + NUMBER_FORMAT.format(entry.getMinDurationMs()) + "</td>");
	                writer.println("                        <td class=\"number\">"
	                        + NUMBER_FORMAT.format(entry.getMaxDurationMs()) + "</td>");
	                writer.println("                        <td class=\"number\">"
	                        + NUMBER_FORMAT.format(entry.getAvgDurationMs()) + "</td>");
	                writer.println("                        <td class=\"number\">"
	                        + NUMBER_FORMAT.format(entry.getMaxCommitMs()) + "</td>");
	                writer.println("                        <td class=\"number\">"
	                        + NUMBER_FORMAT.format(entry.getAvgCommitMs()) + "</td>");
	                writer.println("                        <td class=\"number\">"
	                        + NUMBER_FORMAT.format(entry.getMaxTimeActiveMs()) + "</td>");
	                writer.println("                        <td class=\"number\">"
	                        + NUMBER_FORMAT.format(entry.getAvgTimeActiveMs()) + "</td>");
	                writer.println("                        <td class=\"number\">"
	                        + NUMBER_FORMAT.format(entry.getMaxTimeInactiveMs()) + "</td>");
	                writer.println("                        <td class=\"number\">"
	                        + NUMBER_FORMAT.format(entry.getAvgTimeInactiveMs()) + "</td>");
	                writer.println("                    </tr>");
	            });

	    writer.println("                </tbody>");
	    writer.println("            </table>");
	    writer.println("        </div>");
	}

	private static void writeAppNameStatsTable(PrintWriter writer, AppNameConnectionStatsAccumulator appNameConnectionStatsAccumulator) {
	    if (appNameConnectionStatsAccumulator == null || appNameConnectionStatsAccumulator.getAppNameCount() == 0) {
	        return;
	    }

	    writer.println("        <h2 id=\"appname-stats\">AppName Connection Statistics</h2>");

	    Map<String, Integer> stats = appNameConnectionStatsAccumulator.getAggregatedStats();
	    long totalConnections = appNameConnectionStatsAccumulator.getTotalConnectionCount();
	    long uniqueAppNames = appNameConnectionStatsAccumulator.getAppNameCount();

	    // Summary
	    writer.println("        <div class=\"summary\">");
	    writer.println("            <h3>AppName Summary</h3>");
	    writer.println("            <div class=\"summary-grid\">");
	    writer.println("                <div class=\"summary-item\">");
	    writer.println("                    <div class=\"summary-label\">Total Distinct Connections</div>");
	    writer.println("                    <div class=\"summary-value\">" + NUMBER_FORMAT.format(totalConnections) + "</div>");
	    writer.println("                </div>");
	    writer.println("                <div class=\"summary-item\">");
	    writer.println("                    <div class=\"summary-label\">Unique AppNames</div>");
	    writer.println("                    <div class=\"summary-value\">" + NUMBER_FORMAT.format(uniqueAppNames) + "</div>");
	    writer.println("                </div>");
	    writer.println("            </div>");
	    writer.println("        </div>");

	    // Table
	    writer.println("        <div class=\"table-container\">");
	    writer.println("            <div class=\"controls\">");
	    writer.println("                <input type=\"text\" id=\"appNameStatsFilter\" class=\"filter-input\" placeholder=\"Filter by appName...\">");
	    writer.println("                <button class=\"clear-btn\" onclick=\"clearAppNameStatsFilter()\" style=\"margin-left: 10px;\">Clear Filter</button>");
	    writer.println("            </div>");
	    writer.println("            <table id=\"appNameStatsTable\">");
	    writer.println("                <thead>");
	    writer.println("                    <tr>");
	    writer.println("                        <th class=\"sortable\" onclick=\"sortTable('appNameStatsTable', 0, 'string')\">AppName</th>");
	    writer.println("                        <th class=\"sortable\" onclick=\"sortTable('appNameStatsTable', 1, 'number')\">Distinct Connections</th>");
	    writer.println("                    </tr>");
	    writer.println("                </thead>");
	    writer.println("                <tbody>");

	    stats.entrySet().stream()
	            .sorted((a, b) -> Integer.compare(b.getValue(), a.getValue()))
	            .forEach(entry -> {
	                String appName = entry.getKey();
	                int connCount = entry.getValue();

	                writer.println("                    <tr>");
	                writer.println("                        <td>" + escapeHtml(appName) + "</td>");
	                writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(connCount) + "</td>");
	                writer.println("                    </tr>");
	            });

	    writer.println("                </tbody>");
	    writer.println("            </table>");
	    writer.println("        </div>");

	    // Add filter script
	    writer.println("        <script>");
	    writer.println("            function clearAppNameStatsFilter() {");
	    writer.println("                document.getElementById('appNameStatsFilter').value = '';");
	    writer.println("                filterTable('appNameStatsTable', 'appNameStatsFilter');");
	    writer.println("            }");
	    writer.println("            document.getElementById('appNameStatsFilter').addEventListener('input', function() {");
	    writer.println("                filterTable('appNameStatsTable', 'appNameStatsFilter');");
	    writer.println("            });");
	    writer.println("        </script>");
	}

	private static void writeDriverStatsTable(PrintWriter writer, TwoPassDriverStatsAccumulator driverStatsAccumulator) {
	    if (driverStatsAccumulator == null || !driverStatsAccumulator.hasDriverStats()) {
	        return;
	    }

	    writer.println("        <h2 id=\"driver-stats\">Driver Statistics</h2>");
	    var entries = driverStatsAccumulator.getDriverStatsEntries().values();
	    long totalConnections = driverStatsAccumulator.getTotalConnections();
	    long uniqueDrivers = driverStatsAccumulator.getUniqueDrivers();

	    // Summary
	    writer.println("        <div class=\"summary\">");
	    writer.println("            <h3>Driver Summary</h3>");
	    writer.println("            <div class=\"summary-grid\">");
	    writer.println("                <div class=\"summary-item\">");
	    writer.println("                    <div class=\"summary-label\">Total Connections</div>");
	    writer.println("                    <div class=\"summary-value\">" + NUMBER_FORMAT.format(totalConnections) + "</div>");
	    writer.println("                </div>");
	    writer.println("                <div class=\"summary-item\">");
	    writer.println("                    <div class=\"summary-label\">Unique Driver/Version Combinations</div>");
	    writer.println("                    <div class=\"summary-value\">" + NUMBER_FORMAT.format(driverStatsAccumulator.getUniqueDriverVersionCombinations()) + "</div>");
	    writer.println("                </div>");
	    writer.println("                <div class=\"summary-item\">");
	    writer.println("                    <div class=\"summary-label\">Unique Drivers</div>");
	    writer.println("                    <div class=\"summary-value\">" + NUMBER_FORMAT.format(uniqueDrivers) + "</div>");
	    writer.println("                </div>");
	    writer.println("            </div>");
	    writer.println("        </div>");

	    // Table
	    writer.println("        <div class=\"table-container\">");
	    writer.println("            <div class=\"controls\">");
	    writer.println("                <input type=\"text\" id=\"driverStatsFilter\" class=\"filter-input\" placeholder=\"Filter by driver name, version, OS...\">");
	    writer.println("                <select id=\"driverStatsColumnFilter\" class=\"column-filter\" style=\"margin-left: 10px; padding: 5px 10px; border: 1px solid #ddd; border-radius: 4px;\">");
	    writer.println("                    <option value=\"all\">All Columns</option>");
	    writer.println("                    <option value=\"0\">Driver Name</option>");
	    writer.println("                    <option value=\"1\">Version</option>");
	    writer.println("                    <option value=\"2\">Compressors</option>");
	    writer.println("                    <option value=\"3\">OS Type</option>");
	    writer.println("                    <option value=\"4\">Platform</option>");
	    writer.println("                    <option value=\"5\">Username</option>");
	    writer.println("                    <option value=\"6\"># Connections</option>");
	    writer.println("                    <option value=\"7\">Avg Conn Lifetime</option>");
	    writer.println("                    <option value=\"8\">Max Conn Lifetime</option>");
	    writer.println("                </select>");
	    writer.println("                <button class=\"clear-btn\" onclick=\"clearDriverStatsFilter()\" style=\"margin-left: 10px;\">Clear Filter</button>");
	    writer.println("            </div>");
	    writer.println("            <table id=\"driverStatsTable\">");
	    writer.println("                <thead>");
	    writer.println("                    <tr>");
	    writer.println("                        <th class=\"sortable\" onclick=\"sortTable('driverStatsTable', 0, 'string')\">Driver Name</th>");
	    writer.println("                        <th class=\"sortable\" onclick=\"sortTable('driverStatsTable', 1, 'string')\">Version</th>");
	    writer.println("                        <th class=\"sortable\" onclick=\"sortTable('driverStatsTable', 2, 'string')\">Compressors</th>");
	    writer.println("                        <th class=\"sortable\" onclick=\"sortTable('driverStatsTable', 3, 'string')\">OS Type</th>");
	    writer.println("                        <th class=\"sortable\" onclick=\"sortTable('driverStatsTable', 4, 'string')\">Platform</th>");
	    writer.println("                        <th class=\"sortable\" onclick=\"sortTable('driverStatsTable', 5, 'string')\">Username</th>");
	    writer.println("                        <th class=\"sortable\" onclick=\"sortTable('driverStatsTable', 6, 'number')\"># Connections</th>");
	    writer.println("                        <th class=\"sortable\" onclick=\"sortTable('driverStatsTable', 7, 'string')\">Avg Conn Lifetime</th>");
	    writer.println("                        <th class=\"sortable\" onclick=\"sortTable('driverStatsTable', 8, 'string')\">Max Conn Lifetime</th>");
	    writer.println("                    </tr>");
	    writer.println("                </thead>");
	    writer.println("                <tbody>");

	    java.util.concurrent.atomic.AtomicInteger entryCounter = new java.util.concurrent.atomic.AtomicInteger(0);
	    entries.stream()
	            .sorted((a, b) -> Long.compare(b.getConnectionCount(), a.getConnectionCount()))
	            .forEach(entry -> {
	                String entryId = "driver-" + entryCounter.getAndIncrement();
	                
	                // Each entry now represents a unique driver+version+OS+platform+compressor+username combination
	                String compressorsDisplay = entry.getCompressorsString();
	                String usernameDisplay = entry.getUsernamesString();
	                
	                writer.println("                    <tr class=\"accordion-row\" onclick=\"toggleAccordion('" + entryId + "')\">");
	                writer.println("                        <td class=\"truncated\" title=\"" + escapeHtml(entry.getDriverName()) + "\"><span class=\"accordion-toggle\"></span>" + escapeHtml(entry.getDriverName()) + "</td>");
	                writer.println("                        <td>" + escapeHtml(entry.getDriverVersion()) + "</td>");
	                writer.println("                        <td>" + escapeHtml(compressorsDisplay) + "</td>");
	                writer.println("                        <td>" + escapeHtml(entry.getOsType()) + "</td>");
	                writer.println("                        <td title=\"" + escapeHtml(entry.getPlatform()) + "\">" + escapeHtml(entry.getPlatform()) + "</td>");
	                writer.println("                        <td>" + (usernameDisplay.equals("none") ? "<span style=\"color: #999;\">none</span>" : escapeHtml(usernameDisplay)) + "</td>");
	                writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(entry.getConnectionCount()) + "</td>");
	                writer.println("                        <td>" + escapeHtml(entry.getFormattedAverageConnectionLifetime()) + "</td>");
	                writer.println("                        <td>" + escapeHtml(entry.getFormattedMaxConnectionLifetime()) + "</td>");
	                writer.println("                    </tr>");
	                
	                // Add accordion content
	                addDriverAccordionContent(writer, entryId, entry);
	            });

	    writer.println("                </tbody>");
	    writer.println("            </table>");
	    writer.println("        </div>");
	}

	private static void addDriverAccordionContent(PrintWriter writer, String entryId, DriverStatsEntry entry) {
	    writer.println("                    <tr id=\"" + entryId + "\" class=\"accordion-content\">");
	    writer.println("                        <td colspan=\"9\">"    );
	    
	    // Display sample metadata message
	    java.util.List<String> metadataMessages = entry.getSampleMetadataMessages();
	    if (!metadataMessages.isEmpty()) {
	        String message = metadataMessages.get(0);
	        String enhancedMessage = LogRedactionUtil.enhanceLogMessageForHtml(message);
	        enhancedMessage = escapeHtml(enhancedMessage);
	        writer.println("                            <div class=\"log-sample\">");
	        writer.println("                                <h4>Sample Metadata Message</h4>");
	        writer.println("                                <div style=\"margin-bottom: 10px; padding: 8px; background: #f8f9fa; border-left: 3px solid #00684A;\">");
	        writer.println("                                    " + enhancedMessage);
	        writer.println("                                </div>");
	        writer.println("                            </div>");
	    }
	    
	    // Display sample auth message
	    java.util.List<String> authMessages = entry.getSampleAuthMessages();
	    if (!authMessages.isEmpty()) {
	        String message = authMessages.get(0);
	        String enhancedMessage = LogRedactionUtil.enhanceLogMessageForHtml(message);
	        enhancedMessage = escapeHtml(enhancedMessage);
	        writer.println("                            <div class=\"log-sample\" style=\"margin-top: 15px;\">");
	        writer.println("                                <h4>Sample Authentication Message</h4>");
	        writer.println("                                <div style=\"margin-bottom: 10px; padding: 8px; background: #f8f9fa; border-left: 3px solid #0066CC;\">");
	        writer.println("                                    " + enhancedMessage);
	        writer.println("                                </div>");
	        writer.println("                            </div>");
	    }
	    
	    // If no sample messages available
	    if (metadataMessages.isEmpty() && authMessages.isEmpty()) {
	        writer.println("                            <div class=\"log-sample\" style=\"color: #666; font-style: italic;\">");
	        writer.println("                                No sample log messages available for this driver.");
	        writer.println("                            </div>");
	    }
	    
	    writer.println("                        </td>");
	    writer.println("                    </tr>");
	}

	private static void writeHtmlFooter(PrintWriter writer) {
	    writer.println("    </div>");
	    writer.println("    <script>");
	    writer.println("        let sortStates = {};");
	    writer.println("");
	    writer.println("        function sortTable(tableId, column, type) {");
	    writer.println("            const table = document.getElementById(tableId);");
	    writer.println("            const tbody = table.querySelector('tbody');");
	    writer.println("            const headers = table.querySelectorAll('th');");
	    writer.println("            ");
	    writer.println("            // Collect row pairs (main row + optional accordion content)");
	    writer.println("            const rowPairs = [];");
	    writer.println("            const allRows = Array.from(tbody.querySelectorAll('tr'));");
	    writer.println("            ");
	    writer.println("            for (let i = 0; i < allRows.length; i++) {");
	    writer.println("                const row = allRows[i];");
	    writer.println("                if (!row.classList.contains('accordion-content')) {");
	    writer.println("                    const pair = { mainRow: row, accordionRow: null };");
	    writer.println("                    // Check if next row is accordion content");
	    writer.println("                    if (i + 1 < allRows.length && allRows[i + 1].classList.contains('accordion-content')) {");
	    writer.println("                        pair.accordionRow = allRows[i + 1];");
	    writer.println("                        i++; // Skip the accordion row in next iteration");
	    writer.println("                    }");
	    writer.println("                    rowPairs.push(pair);");
	    writer.println("                }");
	    writer.println("            }");
	    writer.println("            ");
	    writer.println("            // Initialize sort state for this table/column");
	    writer.println("            const stateKey = tableId + '_' + column;");
	    writer.println("            if (!sortStates[stateKey]) {");
	    writer.println("                sortStates[stateKey] = 'none';");
	    writer.println("            }");
	    writer.println("            ");
	    writer.println("            // Clear all header classes");
	    writer.println("            headers.forEach(h => {");
	    writer.println("                h.classList.remove('sort-asc', 'sort-desc');");
	    writer.println("            });");
	    writer.println("            ");
	    writer.println("            // Determine sort direction");
	    writer.println("            let ascending = true;");
	    writer.println("            if (sortStates[stateKey] === 'asc') {");
	    writer.println("                ascending = false;");
	    writer.println("                sortStates[stateKey] = 'desc';");
	    writer.println("                headers[column].classList.add('sort-desc');");
	    writer.println("            } else {");
	    writer.println("                sortStates[stateKey] = 'asc';");
	    writer.println("                headers[column].classList.add('sort-asc');");
	    writer.println("            }");
	    writer.println("            ");
	    writer.println("            // Sort row pairs based on main row content");
	    writer.println("            rowPairs.sort((a, b) => {");
	    writer.println("                const aCell = a.mainRow.cells[column];");
	    writer.println("                const bCell = b.mainRow.cells[column];");
	    writer.println("                ");
	    writer.println("                // Check for data-sort-value attribute first (for bytes sorting)");
	    writer.println("                let aVal, bVal;");
	    writer.println("                if (aCell.hasAttribute('data-sort-value') && bCell.hasAttribute('data-sort-value')) {");
	    writer.println("                    aVal = parseFloat(aCell.getAttribute('data-sort-value'));");
	    writer.println("                    bVal = parseFloat(bCell.getAttribute('data-sort-value'));");
	    writer.println("                    const aValue = isNaN(aVal) ? 0 : aVal;");
	    writer.println("                    const bValue = isNaN(bVal) ? 0 : bVal;");
	    writer.println("                    return ascending ? aValue - bValue : bValue - aValue;");
	    writer.println("                }");
	    writer.println("                ");
	    writer.println("                // Fall back to text content");
	    writer.println("                aVal = aCell.textContent.trim();");
	    writer.println("                bVal = bCell.textContent.trim();");
	    writer.println("                ");
	    writer.println("                if (type === 'number') {");
	    writer.println("                    // Handle various number formats");
	    writer.println("                    // Remove commas, percentage signs, and other formatting");
	    writer.println("                    aVal = aVal.replace(/[,%$]/g, '');");
	    writer.println("                    bVal = bVal.replace(/[,%$]/g, '');");
	    writer.println("                    ");
	    writer.println("                    // Handle special cases like 'sec', 'ms', 'MB', 'KB', 'GB', etc.");
	    writer.println("                    aVal = aVal.replace(/\\s*(sec|ms|MB|KB|GB)$/i, '');");
	    writer.println("                    bVal = bVal.replace(/\\s*(sec|ms|MB|KB|GB)$/i, '');");
	    writer.println("                    ");
	    writer.println("                    // Convert to numbers, treating empty/invalid as 0");
	    writer.println("                    const aNum = parseFloat(aVal);");
	    writer.println("                    const bNum = parseFloat(bVal);");
	    writer.println("                    ");
	    writer.println("                    const aValue = isNaN(aNum) ? 0 : aNum;");
	    writer.println("                    const bValue = isNaN(bNum) ? 0 : bNum;");
	    writer.println("                    ");
	    writer.println("                    return ascending ? aValue - bValue : bValue - aValue;");
	    writer.println("                } else {");
	    writer.println("                    // String comparison");
	    writer.println("                    return ascending ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal);");
	    writer.println("                }");
	    writer.println("            });");
	    writer.println("            ");
	    writer.println("            // Re-append sorted row pairs");
	    writer.println("            rowPairs.forEach(pair => {");
	    writer.println("                tbody.appendChild(pair.mainRow);");
	    writer.println("                if (pair.accordionRow) {");
	    writer.println("                    tbody.appendChild(pair.accordionRow);");
	    writer.println("                }");
	    writer.println("            });");
	    writer.println("        }");
	    writer.println("");
	    writer.println("        function filterTable(inputId, tableId) {");
	    writer.println("            const filter = document.getElementById(inputId).value.toLowerCase();");
	    writer.println("            const table = document.getElementById(tableId);");
	    writer.println("            const rows = table.querySelectorAll('tbody tr');");
	    writer.println("            ");
	    writer.println("            // Check if this is the driver stats table and we have column filter");
	    writer.println("            let columnFilter = null;");
	    writer.println("            if (tableId === 'driverStatsTable') {");
	    writer.println("                const columnSelect = document.getElementById('driverStatsColumnFilter');");
	    writer.println("                if (columnSelect) {");
	    writer.println("                    columnFilter = columnSelect.value;");
	    writer.println("                }");
	    writer.println("            }");
	    writer.println("            ");
	    writer.println("            rows.forEach(row => {");
	    writer.println("                // Skip accordion content rows");
	    writer.println("                if (row.classList.contains('accordion-content')) {");
	    writer.println("                    return;");
	    writer.println("                }");
	    writer.println("                ");
	    writer.println("                let text = '';");
	    writer.println("                if (columnFilter && columnFilter !== 'all') {");
	    writer.println("                    // Filter by specific column");
	    writer.println("                    const cell = row.cells[parseInt(columnFilter)];");
	    writer.println("                    text = cell ? cell.textContent.toLowerCase() : '';");
	    writer.println("                } else {");
	    writer.println("                    // Filter by all columns (default behavior)");
	    writer.println("                    text = row.textContent.toLowerCase();");
	    writer.println("                }");
	    writer.println("                ");
	    writer.println("                const matches = text.includes(filter);");
	    writer.println("                ");
	    writer.println("                // Show/hide main row");
	    writer.println("                if (matches) {");
	    writer.println("                    row.style.display = '';");
	    writer.println("                    row.classList.remove('highlight');");
	    writer.println("                } else {");
	    writer.println("                    row.style.display = 'none';");
	    writer.println("                }");
	    writer.println("                ");
	    writer.println("                // Also hide/show corresponding accordion content if it exists");
	    writer.println("                const accordionId = row.getAttribute('onclick');");
	    writer.println("                if (accordionId) {");
	    writer.println("                    const match = accordionId.match(/toggleAccordion\\('([^']+)'\\)/);");
	    writer.println("                    if (match) {");
	    writer.println("                        const accordionRow = document.getElementById(match[1]);");
	    writer.println("                        if (accordionRow) {");
	    writer.println("                            if (matches) {");
	    writer.println("                                // Remove inline style to let CSS classes control visibility");
	    writer.println("                                accordionRow.style.removeProperty('display');");
	    writer.println("                            } else {");
	    writer.println("                                accordionRow.style.display = 'none';");
	    writer.println("                            }");
	    writer.println("                        }");
	    writer.println("                    }");
	    writer.println("                }");
	    writer.println("            });");
	    writer.println("        }");
	    writer.println("");
	    writer.println("        function clearFilter(inputId, tableId) {");
	    writer.println("            document.getElementById(inputId).value = '';");
	    writer.println("            filterTable(inputId, tableId);");
	    writer.println("        }");
	    writer.println("");
	    writer.println("        function clearDriverStatsFilter() {");
	    writer.println("            document.getElementById('driverStatsFilter').value = '';");
	    writer.println("            document.getElementById('driverStatsColumnFilter').value = 'all';");
	    writer.println("            filterTable('driverStatsFilter', 'driverStatsTable');");
	    writer.println("        }");
	    writer.println("");
	    writer.println("        // Add event listeners for live filtering");
	    writer.println("        document.addEventListener('DOMContentLoaded', function() {");
	    writer.println("            const filterInputs = document.querySelectorAll('.filter-input');");
	    writer.println("            filterInputs.forEach(input => {");
	    writer.println("                const tableId = input.id.replace('Filter', 'Table');");
	    writer.println("                if (tableId === 'mainOpsTable') {");
	    writer.println("                    input.addEventListener('input', () => filterTable('mainOpsFilter', 'mainOpsTable'));");
	    writer.println("                } else if (tableId === 'ttlTable') {");
	    writer.println("                    input.addEventListener('input', () => filterTable('ttlFilter', 'ttlTable'));");
	    writer.println("                } else if (tableId === 'opStatsTable') {");
	    writer.println("                    input.addEventListener('input', () => filterTable('opStatsFilter', 'opStatsTable'));");
	    writer.println("                } else if (tableId === 'planCacheTable') {");
	    writer.println("                    input.addEventListener('input', () => filterTable('planCacheFilter', 'planCacheTable'));");
	    writer.println("                } else if (tableId === 'queryHashTable') {");
	    writer.println("                    input.addEventListener('input', () => filterTable('queryHashFilter', 'queryHashTable'));");
	    writer.println("                } else if (tableId === 'errorCodesTable') {");
	    writer.println("                    input.addEventListener('input', () => filterTable('errorCodesFilter', 'errorCodesTable'));");
	    writer.println("                } else if (tableId === 'transactionTable') {");
	    writer.println("                    input.addEventListener('input', () => filterTable('transactionFilter', 'transactionTable'));");
	    writer.println("                } else if (tableId === 'driverStatsTable') {");
	    writer.println("                    input.addEventListener('input', () => filterTable('driverStatsFilter', 'driverStatsTable'));");
	    writer.println("                    // Also add listener for column filter dropdown");
	    writer.println("                    const columnFilter = document.getElementById('driverStatsColumnFilter');");
	    writer.println("                    if (columnFilter) {");
	    writer.println("                        columnFilter.addEventListener('change', () => filterTable('driverStatsFilter', 'driverStatsTable'));");
	    writer.println("                    }");
	    writer.println("                }");
	    writer.println("            });");
	    writer.println("            ");
	    writer.println("            // Add smooth scrolling for navigation links");
	    writer.println("            document.querySelectorAll('.nav-link').forEach(link => {");
	    writer.println("                link.addEventListener('click', function(e) {");
	    writer.println("                    e.preventDefault();");
	    writer.println("                    const targetId = this.getAttribute('href').substring(1);");
	    writer.println("                    const targetElement = document.getElementById(targetId);");
	    writer.println("                    if (targetElement) {");
	    writer.println("                        targetElement.scrollIntoView({");
	    writer.println("                            behavior: 'smooth',");
	    writer.println("                            block: 'start'");
	    writer.println("                        });");
	    writer.println("                        ");
	    writer.println("                        // Update active nav link");
	    writer.println("                        document.querySelectorAll('.nav-link').forEach(l => l.classList.remove('active'));");
	    writer.println("                        this.classList.add('active');");
	    writer.println("                    }");
	    writer.println("                });");
	    writer.println("            });");
	    writer.println("            ");
	    writer.println("            // Highlight active navigation link based on scroll position");
	    writer.println("            window.addEventListener('scroll', function() {");
	    writer.println("                const sections = document.querySelectorAll('h2[id]');");
	    writer.println("                const navLinks = document.querySelectorAll('.nav-link');");
	    writer.println("                ");
	    writer.println("                let current = '';");
	    writer.println("                sections.forEach(section => {");
	    writer.println("                    const sectionTop = section.offsetTop - 100;");
	    writer.println("                    if (pageYOffset >= sectionTop) {");
	    writer.println("                        current = section.getAttribute('id');");
	    writer.println("                    }");
	    writer.println("                });");
	    writer.println("                ");
	    writer.println("                navLinks.forEach(link => {");
	    writer.println("                    link.classList.remove('active');");
	    writer.println("                    if (link.getAttribute('href') === '#' + current) {");
	    writer.println("                        link.classList.add('active');");
	    writer.println("                    }");
	    writer.println("                });");
	    writer.println("            });");
	    writer.println("        });");
	    writer.println("        ");
	    writer.println("        // Accordion toggle functionality");
	    writer.println("        function toggleAccordion(rowId) {");
	    writer.println("            const accordionContent = document.getElementById(rowId);");
	    writer.println("            const accordionRow = accordionContent ? accordionContent.previousElementSibling : null;");
	    writer.println("            ");
	    writer.println("            if (accordionContent && accordionRow) {");
	    writer.println("                if (accordionContent.classList.contains('open')) {");
	    writer.println("                    accordionContent.classList.remove('open');");
	    writer.println("                    accordionRow.classList.remove('open');");
	    writer.println("                } else {");
	    writer.println("                    accordionContent.classList.add('open');");
	    writer.println("                    accordionRow.classList.add('open');");
	    writer.println("                }");
	    writer.println("            }");
	    writer.println("        }");
	    writer.println("");
	    writer.println("        // Expand all accordions in a table");
	    writer.println("        function expandAllAccordions(tableId) {");
	    writer.println("            const table = document.getElementById(tableId);");
	    writer.println("            const accordionRows = table.querySelectorAll('.accordion-row');");
	    writer.println("            const accordionContents = table.querySelectorAll('.accordion-content');");
	    writer.println("            ");
	    writer.println("            accordionRows.forEach(row => row.classList.add('open'));");
	    writer.println("            accordionContents.forEach(content => content.classList.add('open'));");
	    writer.println("        }");
	    writer.println("");
	    writer.println("        // Collapse all accordions in a table");
	    writer.println("        function collapseAllAccordions(tableId) {");
	    writer.println("            const table = document.getElementById(tableId);");
	    writer.println("            const accordionRows = table.querySelectorAll('.accordion-row');");
	    writer.println("            const accordionContents = table.querySelectorAll('.accordion-content');");
	    writer.println("            ");
	    writer.println("            accordionRows.forEach(row => row.classList.remove('open'));");
	    writer.println("            accordionContents.forEach(content => content.classList.remove('open'));");
	    writer.println("        }");
	    writer.println("");
	    writer.println("        // Pretty-print JSON toggle functionality with bolding preservation");
	    writer.println("        function togglePrettyPrint(buttonId, contentId) {");
	    writer.println("            const button = document.getElementById(buttonId);");
	    writer.println("            const content = document.getElementById(contentId);");
	    writer.println("            const isPretty = button.textContent === 'Compact';");
	    writer.println("            ");
	    writer.println("            if (isPretty) {");
	    writer.println("                // Switch to compact mode");
	    writer.println("                const prettyHtml = content.innerHTML;");
	    writer.println("                try {");
	    writer.println("                    // Remove pretty-print formatting while preserving <strong> tags");
	    writer.println("                    const compactHtml = prettyHtml.replace(/\\n\\s*/g, '').replace(/\\s*{\\s*/g, '{').replace(/\\s*}\\s*/g, '}').replace(/\\s*,\\s*/g, ',').replace(/\":\\s+/g, '\":');");
	    writer.println("                    content.innerHTML = compactHtml;");
	    writer.println("                    button.textContent = 'Pretty';");
	    writer.println("                } catch (e) {");
	    writer.println("                    console.error('Error formatting JSON:', e);");
	    writer.println("                    button.textContent = 'Pretty';");
	    writer.println("                }");
	    writer.println("            } else {");
	    writer.println("                // Switch to pretty mode");
	    writer.println("                const compactHtml = content.innerHTML;");
	    writer.println("                try {");
	    writer.println("                    // Add pretty-print formatting while preserving <strong> tags");
	    writer.println("                    let prettyHtml = compactHtml;");
	    writer.println("                    prettyHtml = prettyHtml.replace(/\\{/g, '{\\n  ');");
	    writer.println("                    prettyHtml = prettyHtml.replace(/\\}/g, '\\n}');");
	    writer.println("                    prettyHtml = prettyHtml.replace(/,/g, ',\\n  ');");
	    writer.println("                    prettyHtml = prettyHtml.replace(/\":([^\\s])/g, '\": $1');");
	    writer.println("                    ");
	    writer.println("                    // Fix nested objects indentation");
	    writer.println("                    const lines = prettyHtml.split('\\n');");
	    writer.println("                    let indentLevel = 0;");
	    writer.println("                    const indentedLines = lines.map(line => {");
	    writer.println("                        const trimmed = line.trim();");
	    writer.println("                        if (trimmed.includes('}')) indentLevel--;");
	    writer.println("                        const indented = '  '.repeat(Math.max(0, indentLevel)) + trimmed;");
	    writer.println("                        if (trimmed.includes('{')) indentLevel++;");
	    writer.println("                        return indented;");
	    writer.println("                    });");
	    writer.println("                    ");
	    writer.println("                    content.innerHTML = indentedLines.join('\\n');");
	    writer.println("                    button.textContent = 'Compact';");
	    writer.println("                } catch (e) {");
	    writer.println("                    console.error('Error formatting JSON:', e);");
	    writer.println("                    button.textContent = 'Compact';");
	    writer.println("                }");
	    writer.println("            }");
	    writer.println("        }");

	    // Column management functions
	    writer.println("");
	    writer.println("        // Column visibility management");
	    writer.println("        let hiddenColumns = {};");
	    writer.println("");
	    writer.println("        function hideColumn(tableId, colIndex) {");
	    writer.println("            if (!hiddenColumns[tableId]) hiddenColumns[tableId] = [];");
	    writer.println("            if (!hiddenColumns[tableId].includes(colIndex)) hiddenColumns[tableId].push(colIndex);");
	    writer.println("            const table = document.getElementById(tableId);");
	    writer.println("            const headers = table.querySelectorAll('thead th');");
	    writer.println("            const rows = table.querySelectorAll('tbody tr');");
	    writer.println("            headers[colIndex].classList.add('hidden-col');");
	    writer.println("            rows.forEach(row => {");
	    writer.println("                const cells = row.querySelectorAll('td');");
	    writer.println("                if (cells[colIndex]) cells[colIndex].classList.add('hidden-col');");
	    writer.println("            });");
	    writer.println("        }");
	    writer.println("");
	    writer.println("        function showColumn(tableId, colIndex) {");
	    writer.println("            if (hiddenColumns[tableId]) {");
	    writer.println("                hiddenColumns[tableId] = hiddenColumns[tableId].filter(i => i !== colIndex);");
	    writer.println("            }");
	    writer.println("            const table = document.getElementById(tableId);");
	    writer.println("            const headers = table.querySelectorAll('thead th');");
	    writer.println("            const rows = table.querySelectorAll('tbody tr');");
	    writer.println("            headers[colIndex].classList.remove('hidden-col');");
	    writer.println("            rows.forEach(row => {");
	    writer.println("                const cells = row.querySelectorAll('td');");
	    writer.println("                if (cells[colIndex]) cells[colIndex].classList.remove('hidden-col');");
	    writer.println("            });");
	    writer.println("        }");
	    writer.println("");
	    writer.println("        let currentTableId = null;");
	    writer.println("");
	    writer.println("        function openColumnModal(tableId) {");
	    writer.println("            currentTableId = tableId;");
	    writer.println("            const modal = document.getElementById('columnModal');");
	    writer.println("            const columnList = document.getElementById('columnList');");
	    writer.println("            const table = document.getElementById(tableId);");
	    writer.println("            const headers = table.querySelectorAll('thead th');");
	    writer.println("            columnList.innerHTML = '';");
	    writer.println("            headers.forEach((header, index) => {");
	    writer.println("                const colName = header.getAttribute('data-col-name');");
	    writer.println("                if (!colName) return;");
	    writer.println("                const isHidden = hiddenColumns[tableId] && hiddenColumns[tableId].includes(index);");
	    writer.println("                const item = document.createElement('div');");
	    writer.println("                item.className = 'column-item' + (isHidden ? ' hidden' : '');");
	    writer.println("                item.textContent = (isHidden ? '☐ ' : '☑ ') + colName;");
	    writer.println("                item.onclick = () => toggleColumn(tableId, index);");
	    writer.println("                columnList.appendChild(item);");
	    writer.println("            });");
	    writer.println("            modal.style.display = 'block';");
	    writer.println("        }");
	    writer.println("");
	    writer.println("        function closeColumnModal() {");
	    writer.println("            document.getElementById('columnModal').style.display = 'none';");
	    writer.println("        }");
	    writer.println("");
	    writer.println("        function toggleColumn(tableId, colIndex) {");
	    writer.println("            const isHidden = hiddenColumns[tableId] && hiddenColumns[tableId].includes(colIndex);");
	    writer.println("            if (isHidden) showColumn(tableId, colIndex);");
	    writer.println("            else hideColumn(tableId, colIndex);");
	    writer.println("            openColumnModal(tableId);");
	    writer.println("        }");
	    writer.println("");
	    writer.println("        window.onclick = function(event) {");
	    writer.println("            const modal = document.getElementById('columnModal');");
	    writer.println("            if (event.target == modal) closeColumnModal();");
	    writer.println("        }");

	    writer.println("    </script>");
	    writer.println("");
	    writer.println("    <!-- Column Management Modal -->");
	    writer.println("    <div id=\"columnModal\" class=\"modal\">");
	    writer.println("        <div class=\"modal-content\">");
	    writer.println("            <div class=\"modal-header\">");
	    writer.println("                <h3>Manage Columns</h3>");
	    writer.println("                <span class=\"close\" onclick=\"closeColumnModal()\">&times;</span>");
	    writer.println("            </div>");
	    writer.println("            <div class=\"modal-body\" id=\"columnList\"></div>");
	    writer.println("        </div>");
	    writer.println("    </div>");
	    writer.println("</body>");
	    writer.println("</html>");
	}

	/**
	 * Helper method to generate a table header with column management features
	 */
	private static String generateTableHeader(String tableId, int colIndex, String columnName, String sortType) {
		return String.format(
			"<th class=\"sortable\" onclick=\"sortTable('%s', %d, '%s')\" data-col-name=\"%s\">%s<span class=\"col-hide-btn\" onclick=\"event.stopPropagation(); hideColumn('%s', %d)\">✕</span></th>",
			tableId, colIndex, sortType, columnName, columnName, tableId, colIndex
		);
	}

	private static String escapeHtml(String text) {
		if (text == null)
			return "";
		
		// First, escape all HTML characters
		String escaped = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;").replace("'", "&#x27;");
		
		// Then, restore the <strong> tags we added for bolding
		escaped = escaped.replace("&lt;strong&gt;", "<strong>").replace("&lt;/strong&gt;", "</strong>");
		
		return escaped;
	}

	private static String truncate(String text, int maxLength) {
		if (text == null)
			return "";
		if (text.length() <= maxLength)
			return text;
		return text.substring(0, maxLength - 3) + "...";
	}
	
	private static String compressPlanSummary(String planSummary) {
		if (planSummary == null || planSummary.length() < 50) {
			return planSummary;
		}
		
		// Split by comma, but handle braces properly
		String[] parts = splitPlanSummary(planSummary);
		if (parts.length < 3) {
			return planSummary;
		}
		
		// Count occurrences of each unique plan step
		Map<String, Integer> planCounts = new java.util.LinkedHashMap<>();
		for (String part : parts) {
			String trimmedPart = part.trim();
			planCounts.put(trimmedPart, planCounts.getOrDefault(trimmedPart, 0) + 1);
		}
		
		// Build compressed representation
		StringBuilder compressed = new StringBuilder();
		boolean wasCompressed = false;
		
		for (Map.Entry<String, Integer> entry : planCounts.entrySet()) {
			String planStep = entry.getKey();
			int count = entry.getValue();
			
			if (compressed.length() > 0) {
				compressed.append(",<br>");
			}
			
			compressed.append(planStep);
			if (count > 1) {
				compressed.append(" (x").append(count).append(")");
				wasCompressed = true;
			}
		}
		
		// Add compression indicator
		if (wasCompressed) {
			compressed.append(" ⚡");
		}
		
		return compressed.toString();
	}
	
	private static String[] splitPlanSummary(String planSummary) {
		java.util.List<String> parts = new java.util.ArrayList<>();
		StringBuilder current = new StringBuilder();
		int braceCount = 0;
		
		for (int i = 0; i < planSummary.length(); i++) {
			char c = planSummary.charAt(i);
			
			if (c == '{') {
				braceCount++;
				current.append(c);
			} else if (c == '}') {
				braceCount--;
				current.append(c);
			} else if (c == ',' && braceCount == 0) {
				// We found a comma outside of braces - this is a separator
				String part = current.toString().trim();
				if (!part.isEmpty()) {
					parts.add(part);
				}
				current = new StringBuilder();
				
				// Skip spaces after comma
				while (i + 1 < planSummary.length() && planSummary.charAt(i + 1) == ' ') {
					i++;
				}
			} else {
				current.append(c);
			}
		}
		
		// Add the last part
		String part = current.toString().trim();
		if (!part.isEmpty()) {
			parts.add(part);
		}
		
		return parts.toArray(new String[0]);
	}
	
	private static String formatTimestampRange(String earliestTimestamp, String latestTimestamp) {
	    try {
	        // MongoDB timestamps are in ISO format like "2024-01-01T12:00:00.000+00:00"
	        // Extract date and time parts for display
	        String earliestDisplay = earliestTimestamp.substring(0, Math.min(19, earliestTimestamp.length())).replace("T", " ");
	        String latestDisplay = latestTimestamp.substring(0, Math.min(19, latestTimestamp.length())).replace("T", " ");
	        
	        // Calculate duration
	        String durationText = "";
	        try {
	            // Parse timestamps to calculate duration
	            java.time.Instant startTime = java.time.Instant.parse(earliestTimestamp);
	            java.time.Instant endTime = java.time.Instant.parse(latestTimestamp);
	            long durationMillis = java.time.Duration.between(startTime, endTime).toMillis();
	            
	            // Format duration using the same method we use for query durations
	            String formattedDuration = formatDuration(durationMillis);
	            durationText = " (" + formattedDuration + ")";
	        } catch (Exception e) {
	            // If we can't parse timestamps, skip duration
	        }
	        
	        // If same date, show date once
	        if (earliestDisplay.substring(0, 10).equals(latestDisplay.substring(0, 10))) {
	            return earliestDisplay.substring(0, 10) + " " + 
	                   earliestDisplay.substring(11) + " to " + latestDisplay.substring(11) + durationText;
	        } else {
	            return earliestDisplay + " to " + latestDisplay + durationText;
	        }
	    } catch (Exception e) {
	        // Fallback to simple format
	        return earliestTimestamp + " to " + latestTimestamp;
	    }
	}
	
	/**
	 * Format duration from milliseconds to human-readable format
	 * (Same as in LogRedactionUtil but included here to avoid dependency)
	 */
	private static String formatDuration(long durationMs) {
	    if (durationMs < 1000) {
	        return durationMs + "ms";
	    } else if (durationMs < 60000) {
	        return String.format("%.1fs", durationMs / 1000.0);
	    } else if (durationMs < 3600000) {
	        long minutes = durationMs / 60000;
	        long seconds = (durationMs % 60000) / 1000;
	        return String.format("%dm %ds", minutes, seconds);
	    } else if (durationMs < 86400000) {
	        long hours = durationMs / 3600000;
	        long minutes = (durationMs % 3600000) / 60000;
	        long seconds = (durationMs % 60000) / 1000;
	        return String.format("%dh %dm %ds", hours, minutes, seconds);
	    } else {
	        long days = durationMs / 86400000;
	        long hours = (durationMs % 86400000) / 3600000;
	        long minutes = (durationMs % 3600000) / 60000;
	        return String.format("%dd %dh %dm", days, hours, minutes);
	    }
	}
}