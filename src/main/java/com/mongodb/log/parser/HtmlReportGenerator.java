package com.mongodb.log.parser;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.NumberFormat;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Generates interactive HTML reports with sortable and filterable tables
 */
public class HtmlReportGenerator {
    
    private static final NumberFormat NUMBER_FORMAT = NumberFormat.getNumberInstance(Locale.US);
    
    public static void generateReport(String fileName, Accumulator accumulator, 
                                    Accumulator ttlAccumulator, 
                                    PlanCacheAccumulator planCacheAccumulator,
                                    Map<String, java.util.concurrent.atomic.AtomicLong> operationTypeStats) throws IOException {
        
        try (PrintWriter writer = new PrintWriter(new FileWriter(fileName))) {
            writeHtmlHeader(writer);
            writeMainOperationsTable(writer, accumulator);
            writeTtlOperationsTable(writer, ttlAccumulator);
            writeOperationStatsTable(writer, operationTypeStats);
            
            if (planCacheAccumulator != null && !planCacheAccumulator.getPlanCacheEntries().isEmpty()) {
                writePlanCacheTable(writer, planCacheAccumulator);
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
        writer.println("        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 20px; background-color: #f5f5f5; }");
        writer.println("        .container { max-width: 1400px; margin: 0 auto; background-color: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }");
        writer.println("        h1 { color: #2c3e50; text-align: center; margin-bottom: 30px; }");
        writer.println("        h2 { color: #34495e; margin-top: 40px; margin-bottom: 20px; border-bottom: 2px solid #3498db; padding-bottom: 5px; }");
        writer.println("        .table-container { margin-bottom: 40px; overflow-x: auto; }");
        writer.println("        .controls { margin-bottom: 15px; }");
        writer.println("        .filter-input { padding: 8px; border: 1px solid #ddd; border-radius: 4px; margin-right: 10px; width: 200px; }");
        writer.println("        .clear-btn { padding: 8px 12px; background-color: #95a5a6; color: white; border: none; border-radius: 4px; cursor: pointer; }");
        writer.println("        .clear-btn:hover { background-color: #7f8c8d; }");
        writer.println("        table { width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 14px; }");
        writer.println("        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }");
        writer.println("        th { background-color: #3498db; color: white; font-weight: bold; cursor: pointer; user-select: none; position: relative; }");
        writer.println("        th:hover { background-color: #2980b9; }");
        writer.println("        th.sortable::after { content: ' ↕'; font-size: 12px; opacity: 0.5; }");
        writer.println("        th.sort-asc::after { content: ' ↑'; opacity: 1; }");
        writer.println("        th.sort-desc::after { content: ' ↓'; opacity: 1; }");
        writer.println("        tr:nth-child(even) { background-color: #f9f9f9; }");
        writer.println("        tr:hover { background-color: #e8f4fd; }");
        writer.println("        .number { text-align: right; }");
        writer.println("        .highlight { background-color: #fff3cd !important; }");
        writer.println("        .summary { background-color: #e8f6f3; padding: 15px; border-radius: 5px; margin-bottom: 20px; }");
        writer.println("        .summary h3 { margin-top: 0; color: #16a085; }");
        writer.println("        .summary-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 10px; }");
        writer.println("        .summary-item { background-color: white; padding: 10px; border-radius: 4px; border-left: 4px solid #3498db; }");
        writer.println("        .summary-label { font-weight: bold; color: #2c3e50; }");
        writer.println("        .summary-value { font-size: 18px; color: #27ae60; }");
        writer.println("        .collscan { background-color: #ffebee !important; }");
        writer.println("        .truncated { max-width: 300px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; cursor: help; }");
        writer.println("    </style>");
        writer.println("</head>");
        writer.println("<body>");
        writer.println("    <div class=\"container\">");
        writer.println("        <h1>MongoDB Log Analysis Report</h1>");
        writer.println("        <p style=\"text-align: center; color: #7f8c8d;\">Generated on " + new java.util.Date() + "</p>");
    }
    
    private static void writeMainOperationsTable(PrintWriter writer, Accumulator accumulator) {
        if (accumulator.getAccumulators().isEmpty()) {
            return;
        }
        
        writer.println("        <h2>Main Operations Analysis</h2>");
        
        // Calculate summary statistics
        long totalOperations = accumulator.getAccumulators().values().stream()
            .mapToLong(LogLineAccumulator::getCount)
            .sum();
        long totalTimeMs = accumulator.getAccumulators().values().stream()
            .mapToLong(acc -> acc.getCount() * acc.getAvg())
            .sum();
        
        writer.println("        <div class=\"summary\">");
        writer.println("            <h3>Summary</h3>");
        writer.println("            <div class=\"summary-grid\">");
        writer.println("                <div class=\"summary-item\">");
        writer.println("                    <div class=\"summary-label\">Total Operations</div>");
        writer.println("                    <div class=\"summary-value\">" + NUMBER_FORMAT.format(totalOperations) + "</div>");
        writer.println("                </div>");
        writer.println("                <div class=\"summary-item\">");
        writer.println("                    <div class=\"summary-label\">Total Time (seconds)</div>");
        writer.println("                    <div class=\"summary-value\">" + NUMBER_FORMAT.format(totalTimeMs / 1000) + "</div>");
        writer.println("                </div>");
        writer.println("                <div class=\"summary-item\">");
        writer.println("                    <div class=\"summary-label\">Unique Namespaces</div>");
        writer.println("                    <div class=\"summary-value\">" + accumulator.getAccumulators().size() + "</div>");
        writer.println("                </div>");
        writer.println("            </div>");
        writer.println("        </div>");
        
        writer.println("        <div class=\"table-container\">");
        writer.println("            <div class=\"controls\">");
        writer.println("                <input type=\"text\" id=\"mainOpsFilter\" class=\"filter-input\" placeholder=\"Filter by namespace or operation...\">");
        writer.println("                <button class=\"clear-btn\" onclick=\"clearFilter('mainOpsFilter', 'mainOpsTable')\">Clear Filter</button>");
        writer.println("            </div>");
        writer.println("            <table id=\"mainOpsTable\">");
        writer.println("                <thead>");
        writer.println("                    <tr>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('mainOpsTable', 0, 'string')\">Namespace</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('mainOpsTable', 1, 'string')\">Operation</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('mainOpsTable', 2, 'number')\">Count</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('mainOpsTable', 3, 'number')\">Min (ms)</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('mainOpsTable', 4, 'number')\">Max (ms)</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('mainOpsTable', 5, 'number')\">Avg (ms)</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('mainOpsTable', 6, 'number')\">P95 (ms)</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('mainOpsTable', 7, 'number')\">Total (sec)</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('mainOpsTable', 8, 'number')\">Avg Keys Ex</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('mainOpsTable', 9, 'number')\">Avg Docs Ex</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('mainOpsTable', 10, 'number')\">Avg Return</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('mainOpsTable', 11, 'number')\">Ex/Ret Ratio</th>");
        writer.println("                    </tr>");
        writer.println("                </thead>");
        writer.println("                <tbody>");
        
        accumulator.getAccumulators().values().stream()
            .sorted(Comparator.comparingLong(LogLineAccumulator::getCount).reversed())
            .forEach(acc -> {
                // The LogLineAccumulator.toString() method uses this format:
                // String.format("%-65s %-20s %10d %10.1f %10d %10d %10d %10d %10d %10d %10d %10d %10d", 
                //               namespace, operation, count, ...)
                
                String accString = acc.toString();
                
                // Extract namespace (first 65 characters) and operation (next 20 characters)
                String namespace = "";
                String operation = "";
                
                if (accString.length() > 65) {
                    namespace = accString.substring(0, 65).trim();
                    if (accString.length() > 85) {
                        operation = accString.substring(65, 85).trim();
                    } else {
                        operation = accString.substring(65).trim();
                    }
                } else {
                    // Handle case where string is shorter than expected
                    namespace = accString.trim();
                }
                
                writer.println("                    <tr>");
                writer.println("                        <td class=\"truncated\" title=\"" + escapeHtml(namespace) + "\">" 
                    + escapeHtml(truncate(namespace, 50)) + "</td>");
                writer.println("                        <td>" + escapeHtml(operation) + "</td>");
                writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(acc.getCount()) + "</td>");
                writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(acc.getMin()) + "</td>");
                writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(acc.getMax()) + "</td>");
                writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(acc.getAvg()) + "</td>");
                writer.println("                        <td class=\"number\">" + String.format("%.0f", acc.getPercentile95()) + "</td>");
                writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format((acc.getCount() * acc.getAvg()) / 1000) + "</td>");
                writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(acc.getCount() > 0 ? acc.getTotalDocsExamined() / acc.getCount() : 0) + "</td>");
                writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(acc.getAvgDocsExamined()) + "</td>");
                writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(acc.getAvgReturned()) + "</td>");
                writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(acc.getScannedReturnRatio()) + "</td>");
                writer.println("                    </tr>");
            });
        
        writer.println("                </tbody>");
        writer.println("            </table>");
        writer.println("        </div>");
    }
    
    private static void writeTtlOperationsTable(PrintWriter writer, Accumulator ttlAccumulator) {
        if (ttlAccumulator == null || ttlAccumulator.getAccumulators().isEmpty()) {
            return;
        }
        
        writer.println("        <h2>TTL Operations Analysis</h2>");
        
        // Calculate TTL summary
        long totalTtlOps = ttlAccumulator.getAccumulators().values().stream()
            .mapToLong(LogLineAccumulator::getCount)
            .sum();
        long totalDeletedDocs = ttlAccumulator.getAccumulators().values().stream()
            .mapToLong(acc -> acc.getAvgReturned() * acc.getCount())
            .sum();
        
        writer.println("        <div class=\"summary\">");
        writer.println("            <h3>TTL Summary</h3>");
        writer.println("            <div class=\"summary-grid\">");
        writer.println("                <div class=\"summary-item\">");
        writer.println("                    <div class=\"summary-label\">Total TTL Operations</div>");
        writer.println("                    <div class=\"summary-value\">" + NUMBER_FORMAT.format(totalTtlOps) + "</div>");
        writer.println("                </div>");
        writer.println("                <div class=\"summary-item\">");
        writer.println("                    <div class=\"summary-label\">Total Documents Deleted</div>");
        writer.println("                    <div class=\"summary-value\">" + NUMBER_FORMAT.format(totalDeletedDocs) + "</div>");
        writer.println("                </div>");
        writer.println("            </div>");
        writer.println("        </div>");
        
        writer.println("        <div class=\"table-container\">");
        writer.println("            <div class=\"controls\">");
        writer.println("                <input type=\"text\" id=\"ttlFilter\" class=\"filter-input\" placeholder=\"Filter by namespace...\">");
        writer.println("                <button class=\"clear-btn\" onclick=\"clearFilter('ttlFilter', 'ttlTable')\">Clear Filter</button>");
        writer.println("            </div>");
        writer.println("            <table id=\"ttlTable\">");
        writer.println("                <thead>");
        writer.println("                    <tr>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('ttlTable', 0, 'string')\">Namespace</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('ttlTable', 1, 'number')\">Count</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('ttlTable', 2, 'number')\">Total Deleted</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('ttlTable', 3, 'number')\">Avg Deleted</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('ttlTable', 4, 'number')\">Min (ms)</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('ttlTable', 5, 'number')\">Max (ms)</th>");
        writer.println("                    </tr>");
        writer.println("                </thead>");
        writer.println("                <tbody>");
        
        ttlAccumulator.getAccumulators().values().stream()
            .sorted(Comparator.comparingLong(LogLineAccumulator::getCount).reversed())
            .forEach(acc -> {
                String namespace = acc.toString().split(" ")[0];
                long deletedByThisNamespace = acc.getAvgReturned() * acc.getCount();
                
                writer.println("                    <tr>");
                writer.println("                        <td class=\"truncated\" title=\"" + escapeHtml(namespace) + "\">" 
                    + escapeHtml(truncate(namespace, 50)) + "</td>");
                writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(acc.getCount()) + "</td>");
                writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(deletedByThisNamespace) + "</td>");
                writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(acc.getAvgReturned()) + "</td>");
                writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(acc.getMin()) + "</td>");
                writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(acc.getMax()) + "</td>");
                writer.println("                    </tr>");
            });
        
        writer.println("                </tbody>");
        writer.println("            </table>");
        writer.println("        </div>");
    }
    
    private static void writeOperationStatsTable(PrintWriter writer, Map<String, java.util.concurrent.atomic.AtomicLong> operationTypeStats) {
        if (operationTypeStats == null || operationTypeStats.isEmpty()) {
            return;
        }
        
        writer.println("        <h2>Operation Type Statistics</h2>");
        
        long totalOps = operationTypeStats.values().stream()
            .mapToLong(java.util.concurrent.atomic.AtomicLong::get)
            .sum();
        
        writer.println("        <div class=\"table-container\">");
        writer.println("            <div class=\"controls\">");
        writer.println("                <input type=\"text\" id=\"opStatsFilter\" class=\"filter-input\" placeholder=\"Filter by operation type...\">");
        writer.println("                <button class=\"clear-btn\" onclick=\"clearFilter('opStatsFilter', 'opStatsTable')\">Clear Filter</button>");
        writer.println("            </div>");
        writer.println("            <table id=\"opStatsTable\">");
        writer.println("                <thead>");
        writer.println("                    <tr>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('opStatsTable', 0, 'string')\">Operation Type</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('opStatsTable', 1, 'number')\">Count</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('opStatsTable', 2, 'number')\">Percentage</th>");
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
                writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(count) + "</td>");
                writer.println("                        <td class=\"number\">" + String.format("%.1f%%", percentage) + "</td>");
                writer.println("                    </tr>");
            });
        
        writer.println("                </tbody>");
        writer.println("            </table>");
        writer.println("        </div>");
    }
    
    private static void writePlanCacheTable(PrintWriter writer, PlanCacheAccumulator planCacheAccumulator) {
        writer.println("        <h2>Plan Cache Analysis</h2>");
        
        // Filter out entries with UNKNOWN plan summaries
        var filteredEntries = planCacheAccumulator.getPlanCacheEntries().values().stream()
            .filter(entry -> entry.getKey().getPlanSummary() != null && 
                            !"UNKNOWN".equals(entry.getKey().getPlanSummary()))
            .collect(java.util.stream.Collectors.toList());
        
        if (filteredEntries.isEmpty()) {
            writer.println("        <p>No plan cache entries with valid plan summaries found.</p>");
            return;
        }
        
        // Calculate plan cache summary (using filtered entries)
        long totalQueries = filteredEntries.stream()
            .mapToLong(PlanCacheAccumulatorEntry::getCount)
            .sum();
        long collScanQueries = filteredEntries.stream()
            .filter(entry -> entry.isCollectionScan())
            .mapToLong(PlanCacheAccumulatorEntry::getCount)
            .sum();
        long totalReplanned = filteredEntries.stream()
            .mapToLong(PlanCacheAccumulatorEntry::getReplannedCount)
            .sum();
        long uniquePlanKeys = filteredEntries.size();
        
        writer.println("        <div class=\"summary\">");
        writer.println("            <h3>Plan Cache Summary</h3>");
        writer.println("            <div class=\"summary-grid\">");
        writer.println("                <div class=\"summary-item\">");
        writer.println("                    <div class=\"summary-label\">Total Queries</div>");
        writer.println("                    <div class=\"summary-value\">" + NUMBER_FORMAT.format(totalQueries) + "</div>");
        writer.println("                </div>");
        writer.println("                <div class=\"summary-item\">");
        writer.println("                    <div class=\"summary-label\">Unique Plan Cache Keys</div>");
        writer.println("                    <div class=\"summary-value\">" + NUMBER_FORMAT.format(uniquePlanKeys) + "</div>");
        writer.println("                </div>");
        writer.println("                <div class=\"summary-item\">");
        writer.println("                    <div class=\"summary-label\">Collection Scans</div>");
        writer.println("                    <div class=\"summary-value\">" + NUMBER_FORMAT.format(collScanQueries) + " (" + 
                         String.format("%.1f%%", (collScanQueries * 100.0) / Math.max(totalQueries, 1)) + ")</div>");
        writer.println("                </div>");
        writer.println("                <div class=\"summary-item\">");
        writer.println("                    <div class=\"summary-label\">Replanned Queries</div>");
        writer.println("                    <div class=\"summary-value\">" + NUMBER_FORMAT.format(totalReplanned) + " (" + 
                         String.format("%.1f%%", (totalReplanned * 100.0) / Math.max(totalQueries, 1)) + ")</div>");
        writer.println("                </div>");
        writer.println("            </div>");
        writer.println("        </div>");
        
        writer.println("        <div class=\"table-container\">");
        writer.println("            <div class=\"controls\">");
        writer.println("                <input type=\"text\" id=\"planCacheFilter\" class=\"filter-input\" placeholder=\"Filter by namespace, plan summary, etc...\">");
        writer.println("                <button class=\"clear-btn\" onclick=\"clearFilter('planCacheFilter', 'planCacheTable')\">Clear Filter</button>");
        writer.println("            </div>");
        writer.println("            <table id=\"planCacheTable\">");
        writer.println("                <thead>");
        writer.println("                    <tr>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('planCacheTable', 0, 'string')\">Namespace</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('planCacheTable', 1, 'string')\">Plan Cache Key</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('planCacheTable', 2, 'string')\">Query Hash</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('planCacheTable', 3, 'string')\">Plan Summary</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('planCacheTable', 4, 'number')\">Count</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('planCacheTable', 5, 'number')\">Min (ms)</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('planCacheTable', 6, 'number')\">Max (ms)</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('planCacheTable', 7, 'number')\">Avg (ms)</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('planCacheTable', 8, 'number')\">P95 (ms)</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('planCacheTable', 9, 'number')\">Avg Keys Ex</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('planCacheTable', 10, 'number')\">Avg Docs Ex</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('planCacheTable', 11, 'number')\">Avg Return</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('planCacheTable', 12, 'number')\">Avg Plan (ms)</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('planCacheTable', 13, 'number')\">Replan %</th>");
        writer.println("                    </tr>");
        writer.println("                </thead>");
        writer.println("                <tbody>");
        
        filteredEntries.stream()
            .sorted(Comparator.comparingLong(PlanCacheAccumulatorEntry::getCount).reversed())
            .forEach(entry -> {
                PlanCacheKey key = entry.getKey();
                
                // Determine row class (only for COLLSCAN, remove IXSCAN styling)
                String rowClass = "";
                if (key.getPlanSummary() != null && key.getPlanSummary().contains("COLLSCAN")) {
                    rowClass = " class=\"collscan\"";
                }
                
                writer.println("                    <tr" + rowClass + ">");
                writer.println("                        <td class=\"truncated\" title=\"" + escapeHtml(key.getNamespace().toString()) + "\">" 
                    + escapeHtml(truncate(key.getNamespace().toString(), 40)) + "</td>");
                writer.println("                        <td class=\"truncated\" title=\"" + escapeHtml(key.getPlanCacheKey()) + "\">" 
                    + escapeHtml(truncate(key.getPlanCacheKey(), 12)) + "</td>");
                writer.println("                        <td class=\"truncated\" title=\"" + escapeHtml(key.getQueryHash()) + "\">" 
                    + escapeHtml(truncate(key.getQueryHash(), 10)) + "</td>");
                writer.println("                        <td class=\"truncated\" title=\"" + escapeHtml(key.getPlanSummary()) + "\">" 
                    + escapeHtml(truncate(key.getPlanSummary(), 35)) + "</td>");
                // Removed Plan Type column
                writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(entry.getCount()) + "</td>");
                writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(entry.getMin()) + "</td>");
                writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(entry.getMax()) + "</td>");
                writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(entry.getAvg()) + "</td>");
                writer.println("                        <td class=\"number\">" + String.format("%.0f", entry.getPercentile95()) + "</td>");
                writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(entry.getAvgKeysExamined()) + "</td>");
                writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(entry.getAvgDocsExamined()) + "</td>");
                writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(entry.getAvgReturned()) + "</td>");
                writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(entry.getAvgPlanningTimeMs()) + "</td>");
                writer.println("                        <td class=\"number\">" + String.format("%.1f%%", entry.getReplannedPercentage()) + "</td>");
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
        writer.println("            const rows = Array.from(tbody.querySelectorAll('tr'));");
        writer.println("            const headers = table.querySelectorAll('th');");
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
        writer.println("            // Sort rows");
        writer.println("            rows.sort((a, b) => {");
        writer.println("                let aVal = a.cells[column].textContent.trim();");
        writer.println("                let bVal = b.cells[column].textContent.trim();");
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
        writer.println("            // Re-append sorted rows");
        writer.println("            rows.forEach(row => tbody.appendChild(row));");
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
        writer.println("                }");
        writer.println("            });");
        writer.println("        });");
        writer.println("    </script>");
        writer.println("</body>");
        writer.println("</html>");
    }
    
    private static String escapeHtml(String text) {
        if (text == null) return "";
        return text.replace("&", "&amp;")
                  .replace("<", "&lt;")
                  .replace(">", "&gt;")
                  .replace("\"", "&quot;")
                  .replace("'", "&#x27;");
    }
    
    private static String truncate(String text, int maxLength) {
        if (text == null) return "";
        if (text.length() <= maxLength) return text;
        return text.substring(0, maxLength - 3) + "...";
    }
}