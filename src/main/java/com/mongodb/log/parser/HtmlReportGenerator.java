package com.mongodb.log.parser;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.NumberFormat;
import java.util.Comparator;
import java.util.Locale;
import java.util.Map;

import com.mongodb.log.parser.accumulator.Accumulator;
import com.mongodb.log.parser.accumulator.ErrorCodeAccumulator;
import com.mongodb.log.parser.accumulator.IndexStatsAccumulator;
import com.mongodb.log.parser.accumulator.IndexStatsEntry;
import com.mongodb.log.parser.accumulator.PlanCacheAccumulator;
import com.mongodb.log.parser.accumulator.PlanCacheAccumulatorEntry;
import com.mongodb.log.parser.accumulator.PlanCacheKey;
import com.mongodb.log.parser.accumulator.QueryHashAccumulator;
import com.mongodb.log.parser.accumulator.QueryHashAccumulatorEntry;
import com.mongodb.log.parser.accumulator.QueryHashKey;
import com.mongodb.log.parser.accumulator.TransactionAccumulator;

/**
 * Generates interactive HTML reports with sortable and filterable tables
 */
public class HtmlReportGenerator {

	private static final NumberFormat NUMBER_FORMAT = NumberFormat.getNumberInstance(Locale.US);

	public static void generateReport(String fileName, Accumulator accumulator, Accumulator ttlAccumulator,
	        PlanCacheAccumulator planCacheAccumulator,
	        QueryHashAccumulator queryHashAccumulator,
	        ErrorCodeAccumulator errorCodeAccumulator,
	        TransactionAccumulator transactionAccumulator,
	        IndexStatsAccumulator indexStatsAccumulator,
	        Map<String, java.util.concurrent.atomic.AtomicLong> operationTypeStats, boolean redactQueries,
	        String earliestTimestamp, String latestTimestamp) throws IOException {

	    try (PrintWriter writer = new PrintWriter(new FileWriter(fileName))) {
	        writeHtmlHeader(writer);
	        writeNavigationHeader(writer, accumulator, ttlAccumulator, planCacheAccumulator, 
	                             queryHashAccumulator, errorCodeAccumulator, transactionAccumulator, 
	                             indexStatsAccumulator, operationTypeStats, earliestTimestamp, latestTimestamp);
	        writeMainOperationsTable(writer, accumulator, redactQueries);
	        writeTtlOperationsTable(writer, ttlAccumulator);
	        writeOperationStatsTable(writer, operationTypeStats);
	        writeErrorCodesTable(writer, errorCodeAccumulator, redactQueries);

	        if (queryHashAccumulator != null) {
	            writeQueryHashTable(writer, queryHashAccumulator, redactQueries);
	        }
	        
	        if (transactionAccumulator != null && transactionAccumulator.hasTransactions()) {
	            writeTransactionTable(writer, transactionAccumulator);
	        }

	        if (indexStatsAccumulator != null && indexStatsAccumulator.hasIndexStats()) {
	            writeIndexStatsTable(writer, indexStatsAccumulator, redactQueries);
	        }

	        writeHtmlFooter(writer);
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
		writer.println(
				"        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; background-color: #f8f9fa; }");
		writer.println("        .container { max-width: 95%; margin: 0 auto; background-color: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }");
		writer.println("        h1 { color: #001E2B; text-align: center; margin-bottom: 10px; }");
		writer.println(
				"        h2 { color: #001E2B; margin-top: 40px; margin-bottom: 20px; border-bottom: 2px solid #00684A; padding-bottom: 5px; scroll-margin-top: 70px; }");
		writer.println("        .table-container { margin-bottom: 40px; overflow-x: auto; }");
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
		writer.println("        table { width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 14px; }");
		writer.println("        th, td { border: 1px solid #b8c4c2; padding: 8px; text-align: left; }");
		writer.println(
				"        th { background-color: #00684A; color: white; font-weight: bold; cursor: pointer; user-select: none; position: relative; }");
		writer.println("        th:hover { background-color: #001E2B; }");
		writer.println("        th.sortable::after { content: ' ↕'; font-size: 12px; opacity: 0.5; }");
		writer.println("        th.sort-asc::after { content: ' ↑'; opacity: 1; }");
		writer.println("        th.sort-desc::after { content: ' ↓'; opacity: 1; }");
		writer.println("        tr:nth-child(even) { background-color: #f8f9fa; }");
		writer.println("        tr:hover { background-color: #E9FF99; }");
		writer.println("        .number { text-align: right; }");
		writer.println("        .highlight { background-color: #E9FF99 !important; }");
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
		writer.println(
				"        .truncated { max-width: 300px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; cursor: help; }");
		writer.println(
				"        .wrapped { max-width: 400px; word-wrap: break-word; white-space: normal; cursor: help; }");
		
		// Navigation styles
		writer.println("        .nav-header { background-color: #001E2B; color: white; padding: 15px 0; margin: -20px -20px 20px -20px; position: sticky; top: 0; z-index: 1000; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }");
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
		writer.println("        .accordion-toggle { font-size: 12px; opacity: 0.7; margin-right: 8px; display: inline-block; }");
		writer.println("        .accordion-toggle::before { content: '▶'; }");
		writer.println("        .accordion-row.open .accordion-toggle::before { content: '▼'; }");
		
		writer.println("    </style>");
		writer.println("</head>");
		writer.println("<body>");
		writer.println("    <div class=\"container\">");
	}

	private static void writeNavigationHeader(PrintWriter writer, Accumulator accumulator, 
	        Accumulator ttlAccumulator, PlanCacheAccumulator planCacheAccumulator,
	        QueryHashAccumulator queryHashAccumulator, ErrorCodeAccumulator errorCodeAccumulator,
	        TransactionAccumulator transactionAccumulator, IndexStatsAccumulator indexStatsAccumulator,
	        Map<String, java.util.concurrent.atomic.AtomicLong> operationTypeStats,
	        String earliestTimestamp, String latestTimestamp) {
	    
	    writer.println("        <div class=\"nav-header\">");
	    writer.println("            <div class=\"nav-content\">");
	    writer.println("                <div class=\"nav-title\">MongoDB Log Analysis Report</div>");
	    writer.println("                <div class=\"nav-links\">");
	    
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
	    
	    // Transaction Analysis
	    if (transactionAccumulator != null && transactionAccumulator.hasTransactions()) {
	        writer.println("                    <a href=\"#transactions\" class=\"nav-link\">Transaction Analysis</a>");
	    }
	    
	    // Index Usage Statistics (last)
	    if (indexStatsAccumulator != null && indexStatsAccumulator.hasIndexStats()) {
	        writer.println("                    <a href=\"#index-stats\" class=\"nav-link\">Index Usage Statistics</a>");
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
	    writer.println("            </div>");
	    writer.println("        </div>");
	}

	private static void writeMainOperationsTable(PrintWriter writer, Accumulator accumulator, boolean redactQueries) {
		if (accumulator.getAccumulators().isEmpty()) {
			return;
		}

		writer.println("        <h2 id=\"main-operations\">Main Operations Analysis</h2>");

		// Calculate summary statistics
		long totalOperations = accumulator.getAccumulators().values().stream().mapToLong(LogLineAccumulator::getCount)
				.sum();
		long totalTimeMs = accumulator.getAccumulators().values().stream()
				.mapToLong(acc -> acc.getCount() * acc.getAvg()).sum();

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
		writer.println("                    <div class=\"summary-label\">Unique Namespaces</div>");
		writer.println(
				"                    <div class=\"summary-value\">" + accumulator.getAccumulators().size() + "</div>");
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
		writer.println("            </div>");
		writer.println("            <table id=\"mainOpsTable\">");
		writer.println("                <thead>");
		writer.println("                    <tr>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('mainOpsTable', 0, 'string')\">Namespace</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('mainOpsTable', 1, 'string')\">Operation</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('mainOpsTable', 2, 'number')\">Count</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('mainOpsTable', 3, 'number')\">Min (ms)</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('mainOpsTable', 4, 'number')\">Max (ms)</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('mainOpsTable', 5, 'number')\">Avg (ms)</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('mainOpsTable', 6, 'number')\">P95 (ms)</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('mainOpsTable', 7, 'number')\">Total (sec)</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('mainOpsTable', 8, 'number')\">Avg Keys Ex</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('mainOpsTable', 9, 'number')\">Avg Docs Ex</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('mainOpsTable', 10, 'number')\">Avg Return</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('mainOpsTable', 11, 'number')\">Ex/Ret Ratio</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('mainOpsTable', 12, 'number')\">Avg Shards</th>");
		writer.println("                    </tr>");
		writer.println("                </thead>");
		writer.println("                <tbody>");

		accumulator.getAccumulators().values().stream()
				.sorted(Comparator.comparingLong(LogLineAccumulator::getCount).reversed()).forEach(acc -> {
					// The LogLineAccumulator.toString() method uses this format:
					// String.format("%-65s %-20s %10d %10.1f %10d %10d %10d %10d %10d %10d %10d
					// %10d %10d",
					// namespace, operation, count, ...)

					// Get namespace and operation directly from the accumulator instead of parsing toString()
					String namespace = acc.getNamespace();
					String operation = acc.getOperation();

					String rowId = "mo-" + Math.abs((namespace + operation).hashCode());
					writer.println("                    <tr class=\"accordion-row\" onclick=\"toggleAccordion('" + rowId + "')\">");
					writer.println("                        <td class=\"truncated\" title=\"" + escapeHtml(namespace)
							+ "\"><span class=\"accordion-toggle\"></span>" + escapeHtml(truncate(namespace, 50)) + "</td>");
					writer.println("                        <td>" + escapeHtml(operation) + "</td>");
					writer.println("                        <td class=\"number\">"
							+ NUMBER_FORMAT.format(acc.getCount()) + "</td>");
					writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(acc.getMin())
							+ "</td>");
					writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(acc.getMax())
							+ "</td>");
					writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(acc.getAvg())
							+ "</td>");
					writer.println("                        <td class=\"number\">"
							+ String.format("%.0f", acc.getPercentile95()) + "</td>");
					writer.println("                        <td class=\"number\">"
							+ NUMBER_FORMAT.format((acc.getCount() * acc.getAvg()) / 1000) + "</td>");
					writer.println("                        <td class=\"number\">"
							+ NUMBER_FORMAT.format(acc.getCount() > 0 ? acc.getTotalDocsExamined() / acc.getCount() : 0)
							+ "</td>");
					writer.println("                        <td class=\"number\">"
							+ NUMBER_FORMAT.format(acc.getAvgDocsExamined()) + "</td>");
					writer.println("                        <td class=\"number\">"
							+ NUMBER_FORMAT.format(acc.getAvgReturned()) + "</td>");
					writer.println("                        <td class=\"number\">"
							+ NUMBER_FORMAT.format(acc.getScannedReturnRatio()) + "</td>");
					writer.println("                        <td class=\"number\">"
							+ NUMBER_FORMAT.format(acc.getAvgShards()) + "</td>");
					writer.println("                    </tr>");
					
					// Add accordion content row for sample log message
					if (acc.getSampleLogMessage() != null) {
						String processedLogMessage = LogRedactionUtil.processLogMessage(acc.getSampleLogMessage(), redactQueries);
						writer.println("                    <tr id=\"" + rowId + "\" class=\"accordion-content\">");
						writer.println("                        <td colspan=\"13\">");
						writer.println("                            <div class=\"log-sample\">");
						writer.println("                                <strong>Slowest Query Log Message:</strong><br>");
						writer.println("                                " + escapeHtml(processedLogMessage));
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
		writer.println("            </div>");
		writer.println("            <table id=\"ttlTable\">");
		writer.println("                <thead>");
		writer.println("                    <tr>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('ttlTable', 0, 'string')\">Namespace</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('ttlTable', 1, 'number')\">Count</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('ttlTable', 2, 'number')\">Total Deleted</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('ttlTable', 3, 'number')\">Avg Deleted</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('ttlTable', 4, 'number')\">Min (ms)</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('ttlTable', 5, 'number')\">Max (ms)</th>");
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
		writer.println("            </div>");
		writer.println("            <table id=\"queryHashTable\">");
		writer.println("                <thead>");
		writer.println("                    <tr>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('queryHashTable', 0, 'string')\">Query Hash</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('queryHashTable', 1, 'string')\">Namespace</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('queryHashTable', 2, 'string')\">Operation</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('queryHashTable', 3, 'number')\">Count</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('queryHashTable', 4, 'number')\">Min (ms)</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('queryHashTable', 5, 'number')\">Max (ms)</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('queryHashTable', 6, 'number')\">Avg (ms)</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('queryHashTable', 7, 'number')\">P95 (ms)</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('queryHashTable', 8, 'number')\">Total (sec)</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('queryHashTable', 9, 'number')\">Avg Keys Ex</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('queryHashTable', 10, 'number')\">Avg Docs Ex</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('queryHashTable', 11, 'number')\">Avg Return</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('queryHashTable', 12, 'number')\">Ex/Ret Ratio</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('queryHashTable', 13, 'number')\">Avg Shards</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('queryHashTable', 14, 'string')\">Read Preference</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('queryHashTable', 15, 'string')\">Plan Summary</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('queryHashTable', 16, 'number')\">Avg Plan (ms)</th>");
		writer.println(
				"                        <th class=\"sortable\" onclick=\"sortTable('queryHashTable', 17, 'number')\">Replan %</th>");
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
					writer.println("                        <td class=\"number\">"
							+ NUMBER_FORMAT.format(entry.getCount()) + "</td>");
					writer.println("                        <td class=\"number\">"
							+ NUMBER_FORMAT.format(entry.getMin()) + "</td>");
					writer.println("                        <td class=\"number\">"
							+ NUMBER_FORMAT.format(entry.getMax()) + "</td>");
					writer.println("                        <td class=\"number\">"
							+ NUMBER_FORMAT.format(entry.getAvg()) + "</td>");
					writer.println("                        <td class=\"number\">"
							+ String.format("%.0f", entry.getPercentile95()) + "</td>");
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
					writer.println("                        <td class=\"truncated\" title=\""
					        + escapeHtml(entry.getReadPreferenceSummary().replace("<br>", ", ")) + "\">"
					        + entry.getReadPreferenceSummaryTruncated(30) + "</td>");
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
						writer.println("                    <tr id=\"" + rowId + "\" class=\"accordion-content\">");
						writer.println("                        <td colspan=\"18\">");
						writer.println("                            <div class=\"log-sample\">");
						writer.println("                                <strong>Query:</strong><br>");
						writer.println("                                " + escapeHtml(entry.getSanitizedQuery()));
						writer.println("                                <br><br>");
						writer.println("                                <strong>Slowest Query Log Message:</strong><br>");
						writer.println("                                " + escapeHtml(processedLogMessage));
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
							+ String.format("%.0f", entry.getPercentile95()) + "</td>");
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
						writer.println("                    <tr id=\"" + rowId + "\" class=\"accordion-content\">");
						writer.println("                        <td colspan=\"14\">");
						writer.println("                            <div class=\"log-sample\">");
						writer.println("                                <strong>Slowest Query Log Message:</strong><br>");
						writer.println("                                " + escapeHtml(processedLogMessage));
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
	                        + String.format("%.0f", entry.getPercentile95()) + "</td>");
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
	    writer.println("                let aVal = a.mainRow.cells[column].textContent.trim();");
	    writer.println("                let bVal = b.mainRow.cells[column].textContent.trim();");
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
	    writer.println("            rows.forEach(row => {");
	    writer.println("                const text = row.textContent.toLowerCase();");
	    writer.println("                if (text.includes(filter)) {");
	    writer.println("                    row.style.display = '';");
	    writer.println("                    row.classList.remove('highlight');");
	    writer.println("                    if (filter && filter.length > 0) {");
	    writer.println("                        row.classList.add('highlight');");
	    writer.println("                    }");
	    writer.println("                } else {");
	    writer.println("                    row.style.display = 'none';");
	    writer.println("                }");
	    writer.println("            });");
	    writer.println("        }");
	    writer.println("");
	    writer.println("        function clearFilter(inputId, tableId) {");
	    writer.println("            document.getElementById(inputId).value = '';");
	    writer.println("            filterTable(inputId, tableId);");
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
	    writer.println("    </script>");
	    writer.println("</body>");
	    writer.println("</html>");
	}

	private static String escapeHtml(String text) {
		if (text == null)
			return "";
		return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;").replace("'",
				"&#x27;");
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
		
		// Split by comma and look for repeated patterns
		String[] parts = planSummary.split(", ");
		if (parts.length < 3) {
			return planSummary;
		}
		
		StringBuilder compressed = new StringBuilder();
		String lastPart = null;
		int repeatCount = 1;
		boolean wasCompressed = false;
		
		for (int i = 0; i < parts.length; i++) {
			String part = parts[i].trim();
			
			if (part.equals(lastPart)) {
				repeatCount++;
				wasCompressed = true;
			} else {
				// Output the previous part with its count
				if (lastPart != null) {
					if (compressed.length() > 0) {
						compressed.append(", ");
					}
					compressed.append(lastPart);
					if (repeatCount > 1) {
						compressed.append(" (x").append(repeatCount).append(")");
					}
				}
				lastPart = part;
				repeatCount = 1;
			}
		}
		
		// Don't forget the last part
		if (lastPart != null) {
			if (compressed.length() > 0) {
				compressed.append(", ");
			}
			compressed.append(lastPart);
			if (repeatCount > 1) {
				compressed.append(" (x").append(repeatCount).append(")");
			}
		}
		
		// Add indicator if compressed
		if (wasCompressed) {
			compressed.append(" ⚡");
		}
		
		return compressed.toString();
	}
	
	private static String formatTimestampRange(String earliestTimestamp, String latestTimestamp) {
	    try {
	        // MongoDB timestamps are in ISO format like "2024-01-01T12:00:00.000+00:00"
	        // Extract date and time parts for display
	        String earliestDisplay = earliestTimestamp.substring(0, Math.min(19, earliestTimestamp.length())).replace("T", " ");
	        String latestDisplay = latestTimestamp.substring(0, Math.min(19, latestTimestamp.length())).replace("T", " ");
	        
	        // If same date, show date once
	        if (earliestDisplay.substring(0, 10).equals(latestDisplay.substring(0, 10))) {
	            return earliestDisplay.substring(0, 10) + " " + 
	                   earliestDisplay.substring(11) + " to " + latestDisplay.substring(11);
	        } else {
	            return earliestDisplay + " to " + latestDisplay;
	        }
	    } catch (Exception e) {
	        // Fallback to simple format
	        return earliestTimestamp + " to " + latestTimestamp;
	    }
	}
}