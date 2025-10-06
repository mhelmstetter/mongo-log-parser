package com.mongodb.log.parser.accumulator;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.mongodb.log.parser.SlowQuery;

/**
 * Accumulator for tracking queries with slow planning times
 */
public class SlowPlanningAccumulator {

    private List<SlowPlanningEntry> entries = new ArrayList<>();
    private int topN = 50; // default to top 50

    public SlowPlanningAccumulator() {
        this(50);
    }

    public SlowPlanningAccumulator(int topN) {
        this.topN = topN;
    }

    public void accumulate(SlowQuery slowQuery, String logMessage) {
        if (slowQuery.planningTimeMicros == null) {
            return;
        }

        SlowPlanningEntry entry = new SlowPlanningEntry(
            slowQuery.planningTimeMicros,
            slowQuery.ns != null ? slowQuery.ns.toString() : "unknown",
            slowQuery.opType != null ? slowQuery.opType.getType() : "unknown",
            slowQuery.planSummary,
            slowQuery.sanitizedFilter,
            slowQuery.queryHash,
            slowQuery.appName,
            logMessage
        );

        entries.add(entry);
    }

    /**
     * Get the top N slowest planning queries
     */
    public List<SlowPlanningEntry> getTopEntries() {
        return entries.stream()
            .sorted(Comparator.comparingLong(SlowPlanningEntry::getPlanningTimeMicros).reversed())
            .limit(topN)
            .collect(java.util.stream.Collectors.toList());
    }

    public List<SlowPlanningEntry> getAllEntries() {
        return new ArrayList<>(entries);
    }

    public int getSize() {
        return entries.size();
    }

    public void report() {
        System.out.println("\n=== Slow Planning Queries (Top " + topN + ") ===");

        String headerFormat = "%-12s %-45s %-15s %-30s %s";
        System.out.println(String.format(headerFormat,
                "PlanningMs", "Namespace", "Operation", "PlanSummary", "Query"));
        System.out.println("=".repeat(150));

        getTopEntries().forEach(entry -> {
            String truncatedNs = truncateString(entry.getNamespace(), 45);
            String truncatedOp = truncateString(entry.getOperation(), 15);
            String truncatedPlan = truncateString(entry.getPlanSummary(), 30);
            String truncatedQuery = truncateString(entry.getSanitizedQuery(), 60);

            System.out.println(String.format("%-12d %-45s %-15s %-30s %s",
                    entry.getPlanningTimeMs(),
                    truncatedNs,
                    truncatedOp,
                    truncatedPlan,
                    truncatedQuery));
        });

        System.out.println(String.format("\nTotal queries with planning time: %,d", entries.size()));
    }

    public void reportCsv(String fileName) throws FileNotFoundException {
        PrintWriter writer = new PrintWriter(fileName);
        writer.println("PlanningTimeMicros,PlanningTimeMs,Namespace,Operation,PlanSummary,SanitizedQuery");

        getTopEntries().forEach(entry -> {
            writer.println(String.format("%d,%d,%s,%s,%s,%s",
                    entry.getPlanningTimeMicros(),
                    entry.getPlanningTimeMs(),
                    escapeCsv(entry.getNamespace()),
                    escapeCsv(entry.getOperation()),
                    escapeCsv(entry.getPlanSummary()),
                    escapeCsv(entry.getSanitizedQuery())));
        });

        writer.close();
    }

    private String truncateString(String value, int maxLength) {
        if (value == null) {
            return "null";
        }
        if (value.length() <= maxLength) {
            return value;
        }
        if (maxLength <= 3) {
            return value.substring(0, maxLength);
        }
        return value.substring(0, maxLength - 3) + "...";
    }

    private String escapeCsv(String value) {
        if (value == null) {
            return "";
        }
        if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }

    /**
     * Entry representing a single query with slow planning time
     */
    public static class SlowPlanningEntry {
        private final long planningTimeMicros;
        private final String namespace;
        private final String operation;
        private final String planSummary;
        private final String sanitizedQuery;
        private final String queryHash;
        private final String appName;
        private final String logMessage;
        private final String timestamp;

        public SlowPlanningEntry(long planningTimeMicros, String namespace, String operation,
                                 String planSummary, String sanitizedQuery, String queryHash,
                                 String appName, String logMessage) {
            this.planningTimeMicros = planningTimeMicros;
            this.namespace = namespace != null ? namespace : "unknown";
            this.operation = operation != null ? operation : "unknown";
            this.planSummary = planSummary != null ? planSummary : "UNKNOWN";
            this.sanitizedQuery = sanitizedQuery != null ? sanitizedQuery : "none";
            this.queryHash = queryHash;
            this.appName = appName;
            this.logMessage = logMessage;
            this.timestamp = extractTimestamp(logMessage);
        }

        /**
         * Extract timestamp from log message in format like:
         * {"t":{"$date":"2025-09-26T22:16:32.190+00:00"},...}
         */
        private String extractTimestamp(String logMessage) {
            if (logMessage == null || logMessage.isEmpty()) {
                return "";
            }

            // Look for the $date field in JSON
            int dateStart = logMessage.indexOf("\"$date\":\"");
            if (dateStart == -1) {
                return "";
            }

            dateStart += 9; // Move past "$date":"
            int dateEnd = logMessage.indexOf("\"", dateStart);
            if (dateEnd == -1) {
                return "";
            }

            return logMessage.substring(dateStart, dateEnd);
        }

        public long getPlanningTimeMicros() {
            return planningTimeMicros;
        }

        public long getPlanningTimeMs() {
            return Math.round(planningTimeMicros / 1000.0);
        }

        /**
         * Get formatted planning time with dynamic units (µs, ms, or s)
         */
        public String getFormattedPlanningTime() {
            if (planningTimeMicros < 1000) {
                // Less than 1ms - show in microseconds
                return planningTimeMicros + "µs";
            } else if (planningTimeMicros < 1000000) {
                // Less than 1s - show in milliseconds
                double ms = planningTimeMicros / 1000.0;
                if (ms < 10) {
                    return String.format("%.2fms", ms);
                } else if (ms < 100) {
                    return String.format("%.1fms", ms);
                } else {
                    return String.format("%.0fms", ms);
                }
            } else {
                // 1s or more - show in seconds
                double s = planningTimeMicros / 1000000.0;
                if (s < 10) {
                    return String.format("%.2fs", s);
                } else {
                    return String.format("%.1fs", s);
                }
            }
        }

        public String getNamespace() {
            return namespace;
        }

        public String getOperation() {
            return operation;
        }

        public String getPlanSummary() {
            return planSummary;
        }

        public String getSanitizedQuery() {
            return sanitizedQuery;
        }

        public String getLogMessage() {
            return logMessage;
        }

        public String getTimestamp() {
            return timestamp;
        }

        public String getQueryHash() {
            return queryHash;
        }

        public String getAppName() {
            return appName;
        }
    }
}
