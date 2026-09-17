package com.mongodb.log.parser;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

public class LogQueryWriter {

    private static final String[] HEADERS = {
        "Timestamp", "Namespace", "AppName", "Operation", "QueryHash", "PlanCacheKey",
        "DurationMs", "KeysExamined", "DocsExamined", "Returned", "PlanSummary",
        "ReadPreference", "ReadPreferenceTags", "Replanned", "FromMultiPlanner", "HasSortStage",
        "PlanningTimeMicros", "BytesRead", "SanitizedFilter"
    };

    private final PrintWriter writer;
    private final List<FilterPredicate> predicates;

    public LogQueryWriter(String outputFile, List<String> filterSpecs) throws FileNotFoundException {
        this.writer = new PrintWriter(outputFile);
        this.predicates = parseFilters(filterSpecs);
        writer.println(String.join(",", HEADERS));
    }

    private List<FilterPredicate> parseFilters(List<String> filterSpecs) {
        List<FilterPredicate> result = new ArrayList<>();
        if (filterSpecs == null) {
            return result;
        }
        for (String spec : filterSpecs) {
            result.add(parseFilterSpec(spec));
        }
        return result;
    }

    private static FilterPredicate parseFilterSpec(String spec) {
        // Check longer operators first to avoid prefix ambiguity (>= before >)
        for (String op : new String[]{">=", "<=", ">", "<", "="}) {
            int idx = spec.indexOf(op);
            if (idx > 0) {
                String key = spec.substring(0, idx).trim();
                String value = spec.substring(idx + op.length()).trim();
                return new FilterPredicate(key, op, value);
            }
        }
        throw new IllegalArgumentException("--logQueryFilter must be key=value or key>value etc: " + spec);
    }

    public synchronized void writeIfMatching(SlowQuery sq) {
        for (FilterPredicate p : predicates) {
            if (!p.matches(sq)) {
                return;
            }
        }
        writer.println(buildCsvRow(sq));
    }

    public void close() {
        writer.close();
    }

    private String buildCsvRow(SlowQuery sq) {
        return escapeCsv(sq.timestamp) + "," +
            escapeCsv(sq.ns != null ? sq.ns.toString() : null) + "," +
            escapeCsv(sq.appName) + "," +
            escapeCsv(sq.opType != null ? sq.opType.getType() : null) + "," +
            escapeCsv(sq.queryHash) + "," +
            escapeCsv(sq.planCacheKey) + "," +
            (sq.durationMillis != null ? sq.durationMillis : "") + "," +
            (sq.keysExamined != null ? sq.keysExamined : "") + "," +
            (sq.docsExamined != null ? sq.docsExamined : "") + "," +
            (sq.nreturned != null ? sq.nreturned : "") + "," +
            escapeCsv(sq.planSummary) + "," +
            escapeCsv(sq.readPreference) + "," +
            escapeCsv(sq.readPreferenceTags) + "," +
            (sq.replanned != null ? sq.replanned : "") + "," +
            (sq.fromMultiPlanner != null ? sq.fromMultiPlanner : "") + "," +
            (sq.hasSortStage != null ? sq.hasSortStage : "") + "," +
            (sq.planningTimeMicros != null ? sq.planningTimeMicros : "") + "," +
            (sq.bytesRead != null ? sq.bytesRead : "") + "," +
            escapeCsv(sq.sanitizedFilter);
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

    private static class FilterPredicate {
        private final String key;
        private final String operator;
        private final String rawValue;
        private final long numericValueMs;

        FilterPredicate(String key, String operator, String rawValue) {
            this.key = key;
            this.operator = operator;
            this.rawValue = rawValue;
            this.numericValueMs = key.equals("duration") ? parseDurationMs(rawValue) : 0;
        }

        boolean matches(SlowQuery sq) {
            switch (key) {
                case "appName":
                    return rawValue.equals(sq.appName);
                case "ns":
                    return sq.ns != null && rawValue.equals(sq.ns.toString());
                case "queryHash":
                    return rawValue.equals(sq.queryHash);
                case "opType":
                    return sq.opType != null && rawValue.equals(sq.opType.getType());
                case "planSummary":
                    return sq.planSummary != null && sq.planSummary.contains(rawValue);
                case "readPreference":
                    return rawValue.equals(sq.readPreference);
                case "hasSortStage":
                    return sq.hasSortStage != null && sq.hasSortStage == Boolean.parseBoolean(rawValue);
                case "duration":
                    return sq.durationMillis != null && compareNumeric(sq.durationMillis, numericValueMs);
                default:
                    throw new IllegalArgumentException("Unknown --logQueryFilter key: " + key +
                        ". Valid keys: appName, ns, queryHash, opType, planSummary, readPreference, hasSortStage, duration");
            }
        }

        private boolean compareNumeric(long actual, long threshold) {
            switch (operator) {
                case "=":  return actual == threshold;
                case ">":  return actual >  threshold;
                case "<":  return actual <  threshold;
                case ">=": return actual >= threshold;
                case "<=": return actual <= threshold;
                default:   return false;
            }
        }

        private static long parseDurationMs(String value) {
            String v = value.trim().toLowerCase();
            if (v.endsWith("ms")) {
                return Long.parseLong(v.substring(0, v.length() - 2).trim());
            } else if (v.endsWith("min")) {
                return Long.parseLong(v.substring(0, v.length() - 3).trim()) * 60_000L;
            } else if (v.endsWith("m")) {
                return Long.parseLong(v.substring(0, v.length() - 1).trim()) * 60_000L;
            } else if (v.endsWith("h")) {
                return Long.parseLong(v.substring(0, v.length() - 1).trim()) * 3_600_000L;
            } else if (v.endsWith("s")) {
                return Long.parseLong(v.substring(0, v.length() - 1).trim()) * 1_000L;
            } else {
                return Long.parseLong(v);
            }
        }
    }
}
