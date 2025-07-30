package com.mongodb.log.parser;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

/**
 * Consolidated utility class for redacting and trimming MongoDB log messages
 */
public class LogRedactionUtil {
    
    private static final Pattern DIGITS_PATTERN = Pattern.compile("\\d");
    private static final Pattern REGEX_SPECIAL_CHARS = Pattern.compile("[^\\^\\$\\.\\*\\+\\?\\(\\)\\[\\]\\{\\}\\|\\\\]");
    
    // Fields that should never be redacted - preserve for analysis
    private static final Set<String> PRESERVE_FIELDS = Set.of(
        // Namespace and operation info
        "ns", "namespace", "collection", "database", 
        // Performance metrics
        "durationMillis", "planSummary", "queryHash", "planCacheKey",
        "keysExamined", "docsExamined", "nreturned", "nModified", "nDeleted", "nInserted",
        "executionTimeMillis", "totalTime", "cpuNanos", "reslen",
        "timeReadingMicros", "bytesRead", "limit", "skip",
        "workingMillis", "maxTimeMS", "remoteOpWaitMillis",
        // Index and plan info
        "indexName", "direction", "stage", "inputStage", "rejectedPlans", "winningPlan",
        // Shard info
        "nShards", "shardName", "shardVersion",
        // Error codes and messages
        "code", "codeName", "ok", "errmsg", "errCode", "errMsg", "errName",
        // Transaction info
        "txnNumber", "autocommit", "startTransaction",
        // System info
        "component", "severity", "id", "msg", "attr", "t", "c", "s", "ctx",
        // Date/time fields
        "$date", "$timestamp", "$oid",
        // Client and connection info  
        "collation", "locale", "clientOperationKey", "$uuid", "$client", "$readPreference", "mode",
        "mongos", "host", "client", "driver", "os", "platform",
        // Read concern info
        "readConcern", "provenance", "level",
        // Write concern info
        "writeConcern", "w", "j", "wtimeout", "fsync",
        // Storage metrics
        "storage", "data",
        // Sort and query structure  
        "$sortKey", "$meta",
        // Metadata timestamps and shard version fields
        "i", "e", "v"
    );
    
    // Fields that should be completely removed for log trimming (only truly verbose fields)
    private static final Set<String> TRIM_FIELDS = Set.of(
        "advanced", "bypassDocumentValidation", "databaseVersion", "flowControl", 
        "fromMultiPlanner", "let", "maxTimeMSOpOnly", "mayBypassWriteBlocking", 
        "multiKeyPaths", "needTime", "planningTimeMicros", "runtimeConstants",
        "totalOplogSlotDurationMicros", "waitForWriteConcernDurationMillis", "works",
        "shardVersion", "clientOperationKey", "lsid", "$clusterTime", "$configTime", "$topologyTime"
    );
    
    // Fields that should preserve text content but may be truncated for trimming
    private static final Set<String> PRESERVE_TEXT_FIELDS = Set.of("ns", "planSummary", "namespace");
    
    // Fields that should preserve array structure
    private static final Set<String> PRESERVE_ARRAY_FIELDS = Set.of("pipeline", "$and", "$or");
    
    // Explicit paths that should NEVER be redacted (MongoDB system data)
    private static final Set<String> PRESERVE_PATHS = Set.of(
        // Top level log structure
        "t", "s", "c", "id", "ctx", "msg",
        
        // Top level attr fields  
        "attr.type", "attr.ns", "attr.ok", "attr.code", "attr.codeName", 
        "attr.errCode", "attr.errName", "attr.remote", "attr.protocol",
        
        // Performance metrics
        "attr.durationMillis", "attr.cpuNanos", "attr.keysExamined", "attr.docsExamined", 
        "attr.nreturned", "attr.nModified", "attr.nDeleted", "attr.nInserted", 
        "attr.reslen", "attr.queryHash", "attr.planCacheKey", "attr.planSummary",
        "attr.nShards", "attr.nBatches", "attr.cursorExhausted", "attr.numYields",
        "attr.replanReason", "attr.replanned", "attr.placementVersionRefreshDurationMillis",
        "attr.queryFramework", "attr.cursorid", "attr.appName",
        
        // Storage metrics
        "attr.storage", "attr.storage.data", "attr.storage.data.timeReadingMicros", 
        "attr.storage.data.bytesRead",
        
        // Locks and system operations
        "attr.locks", "attr.locks.FeatureCompatibilityVersion", "attr.locks.FeatureCompatibilityVersion.acquireCount",
        "attr.locks.FeatureCompatibilityVersion.acquireCount.r", "attr.locks.FeatureCompatibilityVersion.acquireCount.w",
        "attr.locks.Mutex", "attr.locks.Mutex.acquireCount", "attr.locks.Mutex.acquireCount.r",
        "attr.locks.Global", "attr.locks.Global.acquireCount", "attr.locks.Global.acquireCount.r",
        "attr.locks.Global.acquireCount.w",
        
        // Read concern (all contexts)
        "attr.readConcern", "attr.readConcern.level", "attr.readConcern.provenance",
        "attr.originatingCommand.readConcern", "attr.originatingCommand.readConcern.level", 
        "attr.originatingCommand.readConcern.provenance",
        "attr.command.readConcern", "attr.command.readConcern.level", 
        "attr.command.readConcern.provenance",
        
        // Command parameters and collection names (needed for MongoDB analysis)
        "attr.command.limit", "attr.command.skip", "attr.command.maxTimeMS", 
        "attr.command.cursorid", "attr.command.queryHash", "attr.command.planCacheKey", 
        "attr.command.cursorExhausted", "attr.command.getMore", "attr.command.$db",
        "attr.command.mayBypassWriteBlocking", "attr.command.fromMongos", "attr.command.needsMerge",
        "attr.command.queryFramework", "attr.command.find", "attr.command.aggregate", 
        "attr.command.update", "attr.command.delete", "attr.command.insert", "attr.command.count",
        "attr.command.collection", "attr.workingMillis", "attr.remoteOpWaitMillis",
        
        // Write concern fields
        "attr.command.writeConcern", "attr.command.writeConcern.w", "attr.command.writeConcern.j", 
        "attr.command.writeConcern.wtimeout", "attr.command.writeConcern.fsync",
        "attr.originatingCommand.writeConcern", "attr.originatingCommand.writeConcern.w", 
        "attr.originatingCommand.writeConcern.j", "attr.originatingCommand.writeConcern.wtimeout", 
        "attr.originatingCommand.writeConcern.fsync",
        
        // $audit system fields
        "attr.command.$audit", "attr.command.$audit.$impersonatedUser", 
        "attr.command.$audit.$impersonatedUser.user", "attr.command.$audit.$impersonatedUser.db",
        "attr.command.$audit.$impersonatedRoles", "attr.command.$audit.$impersonatedRoles.role",
        "attr.command.$audit.$impersonatedRoles.db",
        "attr.originatingCommand.$audit", "attr.originatingCommand.$audit.$impersonatedUser",
        "attr.originatingCommand.$audit.$impersonatedUser.user", "attr.originatingCommand.$audit.$impersonatedUser.db",
        "attr.originatingCommand.$audit.$impersonatedRoles", "attr.originatingCommand.$audit.$impersonatedRoles.role",
        "attr.originatingCommand.$audit.$impersonatedRoles.db",
        "attr.originatingCommand.$db", "attr.originatingCommand.mayBypassWriteBlocking",
        "attr.originatingCommand.fromMongos", "attr.originatingCommand.needsMerge",
        "attr.originatingCommand.find", "attr.originatingCommand.aggregate", 
        "attr.originatingCommand.update", "attr.originatingCommand.delete", 
        "attr.originatingCommand.insert", "attr.originatingCommand.count",
        "attr.originatingCommand.collection",
        
        // Collation system fields
        "attr.command.collation", "attr.command.collation.locale",
        "attr.originatingCommand.collation", "attr.originatingCommand.collation.locale",
        
        // Client info - preserve all nested paths
        "attr.command.$client", "attr.command.$client.mongos", 
        "attr.command.$client.mongos.host", "attr.command.$client.mongos.client", 
        "attr.command.$client.mongos.version",
        "attr.command.$client.driver", "attr.command.$client.driver.name", 
        "attr.command.$client.driver.version",
        "attr.command.$client.os", "attr.command.$client.os.name", 
        "attr.command.$client.os.type", "attr.command.$client.os.version", 
        "attr.command.$client.os.architecture",
        "attr.command.$client.platform",
        "attr.command.$client.application", "attr.command.$client.application.name",
        
        // Originating command client info
        "attr.originatingCommand.$client", "attr.originatingCommand.$client.mongos", 
        "attr.originatingCommand.$client.mongos.host", "attr.originatingCommand.$client.mongos.client", 
        "attr.originatingCommand.$client.mongos.version",
        "attr.originatingCommand.$client.driver", "attr.originatingCommand.$client.driver.name", 
        "attr.originatingCommand.$client.driver.version",
        "attr.originatingCommand.$client.os", "attr.originatingCommand.$client.os.name", 
        "attr.originatingCommand.$client.os.type", "attr.originatingCommand.$client.os.version", 
        "attr.originatingCommand.$client.os.architecture",
        "attr.originatingCommand.$client.platform",
        "attr.originatingCommand.$client.application", "attr.originatingCommand.$client.application.name",
        
        // Read preference - preserve tags field and mode
        "attr.command.$readPreference", "attr.command.$readPreference.mode", "attr.command.$readPreference.tags",
        
        // Shard version structure
        "attr.command.shardVersion", "attr.command.shardVersion.t", 
        "attr.command.shardVersion.e", "attr.command.shardVersion.v",
        
        // Collation (already defined above)
        
        // Client operation key structure (but not value)
        "attr.command.clientOperationKey",
        
        // Originating command read preference - preserve tags field and mode
        "attr.originatingCommand.$readPreference", "attr.originatingCommand.$readPreference.mode", "attr.originatingCommand.$readPreference.tags"
    );

    /**
     * Redacts sensitive values in a log message while preserving field names and structure
     */
    public static String redactLogMessage(String logMessage, boolean enableRedaction) {
        if (!enableRedaction) {
            return logMessage;
        }
        
        try {
            JSONObject jo = new JSONObject(logMessage);
            JSONObject redacted = redactObjectWithExplicitPaths(jo, "");
            return redacted.toString();
        } catch (Exception e) {
            // If redaction fails, return original message
            return logMessage;
        }
    }

    /**
     * Path-based redaction using explicit whitelist - much more secure
     */
    private static JSONObject redactObjectWithExplicitPaths(JSONObject obj, String currentPath) throws JSONException {
        JSONObject redacted = new JSONObject();
        
        Iterator<String> keys = obj.keys();
        while (keys.hasNext()) {
            String key = keys.next();
            Object value = obj.get(key);
            String fieldPath = currentPath.isEmpty() ? key : currentPath + "." + key;
            
            if (PRESERVE_PATHS.contains(fieldPath)) {
                // This exact path should be preserved - but may need hostname redaction
                if (value instanceof String && fieldPath.endsWith(".host")) {
                    // Debug: System.out.println("Redacting host field: " + fieldPath + " = " + value);
                    redacted.put(key, redactPreservedHostname((String) value));
                } else if (value instanceof JSONObject) {
                    // For preserved object paths, still need to process nested fields
                    redacted.put(key, redactObjectWithExplicitPaths((JSONObject) value, fieldPath));
                } else if (value instanceof JSONArray) {
                    // For preserved array paths, still need to process nested fields
                    redacted.put(key, redactArrayWithExplicitPaths((JSONArray) value, fieldPath));
                } else {
                    // Preserve other values as-is
                    redacted.put(key, value);
                }
            } else if (isMongoDBSpecialObject(key)) {
                // MongoDB objects should be preserved even in query contexts
                redacted.put(key, value);
            } else {
                // Default: redact the value but continue processing structure
                redacted.put(key, redactValueWithExplicitPaths(value, fieldPath));
            }
        }
        
        return redacted;
    }

    /**
     * Redact values using explicit path approach
     */
    private static Object redactValueWithExplicitPaths(Object value, String currentPath) throws JSONException {
        if (value == null || value == JSONObject.NULL) {
            return value;
        }
        
        if (value instanceof JSONObject) {
            JSONObject obj = (JSONObject) value;
            
            // Handle $regularExpression - always redact pattern in user queries
            if (obj.has("$regularExpression")) {
                return redactRegularExpressionInQuery(obj);
            }
            
            return redactObjectWithExplicitPaths(obj, currentPath);
        }
        
        if (value instanceof JSONArray) {
            return redactArrayWithExplicitPaths((JSONArray) value, currentPath);
        }
        
        if (value instanceof String) {
            return redactAtlasHostname((String) value);  // Smart redaction for Atlas hostnames
        }
        
        if (value instanceof Number) {
            return redactNumber(value);
        }
        
        if (value instanceof Boolean) {
            return value;
        }
        
        return "xxx";
    }

    /**
     * Redact arrays using explicit path approach
     */
    private static JSONArray redactArrayWithExplicitPaths(JSONArray arr, String currentPath) throws JSONException {
        JSONArray redacted = new JSONArray();
        
        for (int i = 0; i < arr.length(); i++) {
            Object value = arr.get(i);
            String arrayPath = currentPath + "[" + i + "]";
            redacted.put(redactValueWithExplicitPaths(value, arrayPath));
        }
        
        return redacted;
    }

    /**
     * Check if field is a MongoDB special object or operator
     */
    private static boolean isMongoDBSpecialObject(String key) {
        return key.equals("$date") || key.equals("$timestamp") || 
               key.equals("$oid") || key.equals("$uuid") ||
               key.equals("$skip") || key.equals("$limit") ||
               key.equals("distanceField") || key.equals("maxDistance") ||
               key.equals("near") || key.equals("spherical") ||
               key.equals("distanceMultiplier");
    }

    /**
     * Smart regex redaction that preserves regex operators but redacts user data
     */
    private static JSONObject redactRegularExpressionInQuery(JSONObject regexObj) throws JSONException {
        JSONObject redacted = new JSONObject();
        redacted.put("$regularExpression", new JSONObject());
        
        JSONObject regex = regexObj.getJSONObject("$regularExpression");
        JSONObject redactedRegex = redacted.getJSONObject("$regularExpression");
        
        if (regex.has("pattern")) {
            String originalPattern = regex.getString("pattern");
            // Preserve regex operators but redact user content
            redactedRegex.put("pattern", redactRegexPatternSmart(originalPattern));
        }
        
        if (regex.has("options")) {
            redactedRegex.put("options", regex.get("options"));
        }
        
        return redacted;
    }
    
    /**
     * Smart Atlas hostname redaction that preserves structure while redacting cluster IDs
     */
    private static String redactAtlasHostname(String value) {
        if (value == null || value.isEmpty()) {
            return value;
        }
        
        // Pattern for MongoDB Atlas hostnames: atlas-[CLUSTER_ID]-shard-XX-XX.[REGION_ID].mongodb.net
        if (value.contains("atlas-") && value.contains(".mongodb.net")) {
            // Replace cluster ID (between atlas- and -shard) and region ID (before .mongodb.net)
            // Handle optional port number at the end
            String redacted = value.replaceAll("atlas-([a-zA-Z0-9]+)(-shard-[0-9]+-[0-9]+\\.)([a-zA-Z0-9-]+)(\\.mongodb\\.net)(:[0-9]+)?", 
                                             "atlas-xxx$2xxx$4$5");
            return redacted;
        }
        
        // For other strings, just return "xxx"
        return "xxx";
    }
    
    /**
     * Redact hostnames in preserved paths - only redact Atlas cluster IDs
     */
    private static String redactPreservedHostname(String value) {
        if (value == null || value.isEmpty()) {
            return value;
        }
        
        // Pattern for MongoDB Atlas hostnames: atlas-[CLUSTER_ID]-shard-XX-XX.[REGION_ID].mongodb.net
        if (value.contains("atlas-") && value.contains(".mongodb.net")) {
            // Replace cluster ID (between atlas- and -shard) and region ID (before .mongodb.net)
            // Handle optional port number at the end
            String redacted = value.replaceAll("atlas-([a-zA-Z0-9]+)(-shard-[0-9]+-[0-9]+\\.)([a-zA-Z0-9-]+)(\\.mongodb\\.net)(:[0-9]+)?", 
                                             "atlas-xxx$2xxx$4$5");
            return redacted;
        }
        
        // For non-Atlas hostnames in preserved paths, keep them as-is
        return value;
    }
    
    /**
     * Smart regex pattern redaction that preserves regex operators
     */
    private static String redactRegexPatternSmart(String pattern) {
        if (pattern == null || pattern.isEmpty()) {
            return pattern;
        }
        
        // For complex patterns with multiple user data segments, just use "xxx"
        // This avoids the "xxx xxx xxx" issue while still preserving regex structure
        if (pattern.contains(" ") || pattern.length() > 20) {
            return "xxx";
        }
        
        // For simple patterns, preserve regex metacharacters but redact alphanumeric content
        // Regex metacharacters: ^ $ . * + ? ( ) [ ] { } | \
        Pattern userDataPattern = Pattern.compile("[a-zA-Z0-9_\\-/]+");
        return userDataPattern.matcher(pattern).replaceAll("xxx");
    }
    
    /**
     * Trims a log message by removing verbose fields while preserving key analysis data
     */
    public static String trimLogMessage(String logMessage) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode rootNode = mapper.readTree(logMessage);
            JsonNode trimmed = trimJsonNode(rootNode);
            return trimmed.toString();
        } catch (Exception e) {
            // If trimming fails, return original message
            return logMessage;
        }
    }
    
    /**
     * Combined redact and trim operation
     */
    public static String processLogMessage(String logMessage, boolean enableRedaction) {
        String processed = trimLogMessage(logMessage);
        if (enableRedaction) {
            processed = redactLogMessage(processed, true);
        }
        return processed;
    }
    
    /**
     * Check if a log message indicates truncation
     */
    public static boolean isLogMessageTruncated(String logMessage) {
        if (logMessage == null || logMessage.isEmpty()) {
            return false;
        }
        
        try {
            JSONObject jo = new JSONObject(logMessage);
            return hasNestedTruncationField(jo);
        } catch (Exception e) {
            // If parsing fails, check for simple string pattern
            return logMessage.contains("\"truncated\"") && logMessage.contains("\"errMsg\"");
        }
    }
    
    /**
     * Enhance log message for HTML display by bolding specific fields
     */
    public static String enhanceLogMessageForHtml(String logMessage) {
        if (logMessage == null || logMessage.isEmpty()) {
            return logMessage;
        }
        
        // Bold important performance and diagnostic fields in the JSON output
        String enhanced = logMessage;
        enhanced = enhanced.replaceAll("\"nreturned\":(\\s*)(\\d+)", "<strong>\"nreturned\":</strong>$1<strong>$2</strong>");
        enhanced = enhanced.replaceAll("\"durationMillis\":(\\s*)(\\d+)", "<strong>\"durationMillis\":</strong>$1<strong>$2</strong>");
        enhanced = enhanced.replaceAll("\"keysExamined\":(\\s*)(\\d+)", "<strong>\"keysExamined\":</strong>$1<strong>$2</strong>");
        enhanced = enhanced.replaceAll("\"docsExamined\":(\\s*)(\\d+)", "<strong>\"docsExamined\":</strong>$1<strong>$2</strong>");
        enhanced = enhanced.replaceAll("\"hasSortStage\":(\\s*)(true|false)", "<strong>\"hasSortStage\":</strong>$1<strong>$2</strong>");
        
        // Also bold other important performance indicators
        enhanced = enhanced.replaceAll("\"nModified\":(\\s*)(\\d+)", "<strong>\"nModified\":</strong>$1<strong>$2</strong>");
        enhanced = enhanced.replaceAll("\"nDeleted\":(\\s*)(\\d+)", "<strong>\"nDeleted\":</strong>$1<strong>$2</strong>");
        enhanced = enhanced.replaceAll("\"nInserted\":(\\s*)(\\d+)", "<strong>\"nInserted\":</strong>$1<strong>$2</strong>");
        enhanced = enhanced.replaceAll("\"executionTimeMillis\":(\\s*)(\\d+)", "<strong>\"executionTimeMillis\":</strong>$1<strong>$2</strong>");
        enhanced = enhanced.replaceAll("\"totalTime\":(\\s*)(\\d+)", "<strong>\"totalTime\":</strong>$1<strong>$2</strong>");
        enhanced = enhanced.replaceAll("\"fromPlanCache\":(\\s*)(true|false)", "<strong>\"fromPlanCache\":</strong>$1<strong>$2</strong>");
        enhanced = enhanced.replaceAll("\"usedDisk\":(\\s*)(true|false)", "<strong>\"usedDisk\":</strong>$1<strong>$2</strong>");
        enhanced = enhanced.replaceAll("\"cursorExhausted\":(\\s*)(true|false)", "<strong>\"cursorExhausted\":</strong>$1<strong>$2</strong>");
        enhanced = enhanced.replaceAll("\"numYields\":(\\s*)(\\d+)", "<strong>\"numYields\":</strong>$1<strong>$2</strong>");
        
        // Bold storage-related fields
        enhanced = enhanced.replaceAll("\"bytesRead\":(\\s*)(\\d+)", "<strong>\"bytesRead\":</strong>$1<strong>$2</strong>");
        enhanced = enhanced.replaceAll("\"timeReadingMicros\":(\\s*)(\\d+)", "<strong>\"timeReadingMicros\":</strong>$1<strong>$2</strong>");
        
        return enhanced;
    }
    
    /**
     * Detect if log message is from mongod or mongos
     */
    public static String detectQuerySource(String logMessage) {
        if (logMessage == null || logMessage.isEmpty()) {
            return "";
        }
        
        try {
            JSONObject jo = new JSONObject(logMessage);
            // Check if there's a mongos field in the client information
            if (hasNestedMongosField(jo)) {
                return " (from mongos)";
            }
            // Check for other indicators of mongos vs mongod
            if (logMessage.contains("\"mongos\"") || logMessage.contains("\"fromMongos\"")) {
                return " (from mongos)";
            }
            // Default to mongod if no mongos indicators found
            return " (from mongod)";
        } catch (Exception e) {
            // If parsing fails, try simple string matching
            if (logMessage.contains("mongos") || logMessage.contains("fromMongos")) {
                return " (from mongos)";
            }
            return " (from mongod)";
        }
    }
    
    /**
     * Recursively check for mongos field in JSON object
     */
    private static boolean hasNestedMongosField(JSONObject obj) throws JSONException {
        Iterator<String> keys = obj.keys();
        while (keys.hasNext()) {
            String key = keys.next();
            Object value = obj.get(key);
            
            if (key.equals("mongos") || key.equals("fromMongos")) {
                return true;
            }
            
            if (value instanceof JSONObject) {
                if (hasNestedMongosField((JSONObject) value)) {
                    return true;
                }
            } else if (value instanceof JSONArray) {
                JSONArray arr = (JSONArray) value;
                for (int i = 0; i < arr.length(); i++) {
                    Object arrValue = arr.get(i);
                    if (arrValue instanceof JSONObject) {
                        if (hasNestedMongosField((JSONObject) arrValue)) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }
    
    /**
     * Recursively check for truncation field in JSON object
     */
    private static boolean hasNestedTruncationField(JSONObject obj) throws JSONException {
        Iterator<String> keys = obj.keys();
        while (keys.hasNext()) {
            String key = keys.next();
            Object value = obj.get(key);
            
            if (key.equals("truncated") && value instanceof JSONObject) {
                JSONObject truncatedObj = (JSONObject) value;
                if (truncatedObj.has("errMsg")) {
                    return true;
                }
            }
            
            if (value instanceof JSONObject) {
                if (hasNestedTruncationField((JSONObject) value)) {
                    return true;
                }
            } else if (value instanceof JSONArray) {
                JSONArray arr = (JSONArray) value;
                for (int i = 0; i < arr.length(); i++) {
                    Object arrValue = arr.get(i);
                    if (arrValue instanceof JSONObject) {
                        if (hasNestedTruncationField((JSONObject) arrValue)) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }
    
    /**
     * Sanitizes a MongoDB filter query by obfuscating values while preserving structure
     */
    public static String sanitizeFilter(JSONObject filter, boolean enableRedaction) {
        if (filter == null) {
            return null;
        }
        
        if (!enableRedaction) {
            return filter.toString();
        }
        
        try {
            JSONObject sanitized = redactObjectWithExplicitPaths(filter, "");
            return sanitized.toString();
        } catch (Exception e) {
            return "{\"sanitization_error\": \"xxx\"}";
        }
    }
    
    // Private helper methods for redaction
    private static JSONObject redactObject(JSONObject obj) throws JSONException {
        return redactObject(obj, false);
    }
    
    private static JSONObject redactObject(JSONObject obj, boolean isInQueryContext) throws JSONException {
        return redactObject(obj, isInQueryContext, false);
    }
    
    private static JSONObject redactObject(JSONObject obj, boolean isInQueryContext, boolean isInPreservedContext) throws JSONException {
        return redactObjectWithPath(obj, isInQueryContext, isInPreservedContext, "");
    }
    
    private static JSONObject redactObjectWithPath(JSONObject obj, boolean isInQueryContext, boolean isInPreservedContext, String path) throws JSONException {
        JSONObject redacted = new JSONObject();
        
        Iterator<String> keys = obj.keys();
        while (keys.hasNext()) {
            String key = keys.next();
            Object value = obj.get(key);
            String currentPath = path.isEmpty() ? key : path + "." + key;
            
            if (isInPreservedContext || isSystemFieldPath(currentPath)) {
                // Preserve system fields completely, including nested content
                if (value instanceof JSONObject) {
                    redacted.put(key, redactObjectWithPath((JSONObject) value, isInQueryContext, true, currentPath));
                } else if (value instanceof JSONArray) {
                    redacted.put(key, redactArrayWithPath((JSONArray) value, isInQueryContext, true, currentPath));
                } else {
                    redacted.put(key, value);
                }
            } else if (isQueryField(key)) {
                // For query fields, redact values but preserve structure
                redacted.put(key, redactValueWithPath(value, true, currentPath));
            } else if (isInQueryContext && !isAlwaysPreservedInQuery(key)) {
                // We're inside a query context, redact all values except always preserved fields
                redacted.put(key, redactValueWithPath(value, true, currentPath));
            } else {
                // Redact the value but keep the field
                redacted.put(key, redactValueWithPath(value, false, currentPath));
            }
        }
        
        return redacted;
    }
    
    // Check if a field contains query data that should be redacted
    private static boolean isQueryField(String fieldName) {
        return fieldName.equals("filter") || 
               fieldName.equals("query") || 
               fieldName.equals("command") || 
               fieldName.equals("find") || 
               fieldName.equals("aggregate") || 
               fieldName.equals("update") || 
               fieldName.equals("delete") || 
               fieldName.equals("insert") || 
               fieldName.equals("pipeline") ||
               fieldName.startsWith("$");
    }
    
    // Check if a path represents a MongoDB system field that should always be preserved
    private static boolean isSystemFieldPath(String path) {
        // Top-level MongoDB system fields
        if (PRESERVE_FIELDS.contains(path)) {
            return true;
        }
        
        // System field paths within $client, readConcern, etc.
        if (path.startsWith("$client.") ||
            path.startsWith("readConcern.") ||
            path.startsWith("$clusterTime.") ||
            path.startsWith("$timestamp.") ||
            path.startsWith("shardVersion.") ||
            path.startsWith("storage.") ||
            path.equals("attr")) {
            return true;
        }
        
        // MongoDB date/time objects
        if (path.endsWith(".$date") ||
            path.endsWith(".$timestamp") ||
            path.endsWith(".$oid") ||
            path.endsWith(".$uuid")) {
            return true;
        }
        
        // Specific system field paths that should be preserved
        if (path.equals("ctx") || // MongoDB context (thread/connection)
            path.equals("attr.type") || // Command type
            // Client system fields
            (path.startsWith("attr.command.$client.") && 
             (path.endsWith(".version") || path.endsWith(".name") || path.endsWith(".type") || 
              path.endsWith(".architecture") || path.endsWith(".application"))) ||
            // Direct $client fields (when $client is at attr level)
            (path.startsWith("attr.$client.") && 
             (path.endsWith(".version") || path.endsWith(".name") || path.endsWith(".type") || 
              path.endsWith(".architecture") || path.endsWith(".application")))) {
            return true;
        }
        
        return false;
    }
    
    // Fields that should always be preserved even within query contexts
    private static boolean isAlwaysPreservedInQuery(String key) {
        return key.equals("$date") || 
               key.equals("$timestamp") || 
               key.equals("$oid") ||
               key.equals("$uuid");
    }
    
    private static Object redactValue(Object value) throws JSONException {
        return redactValue(value, false);
    }
    
    private static Object redactValue(Object value, boolean isInQueryContext) throws JSONException {
        return redactValueWithPath(value, isInQueryContext, "");
    }
    
    private static Object redactValueWithPath(Object value, boolean isInQueryContext, String path) throws JSONException {
        if (value == null || value == JSONObject.NULL) {
            return value;
        }
        
        if (value instanceof JSONObject) {
            JSONObject obj = (JSONObject) value;
            
            // Special handling for $regularExpression
            if (obj.has("$regularExpression")) {
                return redactRegularExpression(obj, isInQueryContext);
            }
            
            return redactObjectWithPath(obj, isInQueryContext, false, path);
        }
        
        if (value instanceof JSONArray) {
            return redactArrayWithPath((JSONArray) value, isInQueryContext, false, path);
        }
        
        if (value instanceof String) {
            return redactString((String) value);
        }
        
        if (value instanceof Number) {
            return redactNumber(value);
        }
        
        if (value instanceof Boolean) {
            return value; // Keep boolean values as-is
        }
        
        return redactString(value.toString());
    }
    
    private static JSONArray redactArray(JSONArray arr) throws JSONException {
        return redactArray(arr, false);
    }
    
    private static JSONArray redactArray(JSONArray arr, boolean isInQueryContext) throws JSONException {
        return redactArray(arr, isInQueryContext, false);
    }
    
    private static JSONArray redactArray(JSONArray arr, boolean isInQueryContext, boolean isInPreservedContext) throws JSONException {
        return redactArrayWithPath(arr, isInQueryContext, isInPreservedContext, "");
    }
    
    private static JSONArray redactArrayWithPath(JSONArray arr, boolean isInQueryContext, boolean isInPreservedContext, String path) throws JSONException {
        JSONArray redacted = new JSONArray();
        
        for (int i = 0; i < arr.length(); i++) {
            Object value = arr.get(i);
            String currentPath = path + "[" + i + "]";
            
            if (isInPreservedContext) {
                if (value instanceof JSONObject) {
                    redacted.put(redactObjectWithPath((JSONObject) value, isInQueryContext, true, currentPath));
                } else if (value instanceof JSONArray) {
                    redacted.put(redactArrayWithPath((JSONArray) value, isInQueryContext, true, currentPath));
                } else {
                    redacted.put(value);
                }
            } else {
                redacted.put(redactValueWithPath(value, isInQueryContext, currentPath));
            }
        }
        
        return redacted;
    }
    
    private static JSONObject redactRegularExpression(JSONObject regexObj) throws JSONException {
        return redactRegularExpression(regexObj, false);
    }
    
    private static JSONObject redactRegularExpression(JSONObject regexObj, boolean isInQueryContext) throws JSONException {
        JSONObject redacted = new JSONObject();
        redacted.put("$regularExpression", new JSONObject());
        
        JSONObject regex = regexObj.getJSONObject("$regularExpression");
        JSONObject redactedRegex = redacted.getJSONObject("$regularExpression");
        
        if (regex.has("pattern")) {
            String pattern = regex.getString("pattern");
            if (isInQueryContext) {
                // In query contexts, redact the entire pattern content as user data
                redactedRegex.put("pattern", "xxx");
            } else {
                // In system contexts, preserve regex structure but redact non-regex chars
                redactedRegex.put("pattern", redactRegexPattern(pattern));
            }
        }
        
        if (regex.has("options")) {
            redactedRegex.put("options", regex.get("options"));
        }
        
        return redacted;
    }
    
    private static String redactRegexPattern(String pattern) {
        if (pattern == null || pattern.isEmpty()) {
            return pattern;
        }
        return REGEX_SPECIAL_CHARS.matcher(pattern).replaceAll("x");
    }
    
    private static String redactString(String value) {
        if (value == null || value.isEmpty()) {
            return value;
        }
        
        if (value.length() <= 3) {
            return "xxx".substring(0, value.length());
        }
        
        return "xxx";
    }
    
    private static Object redactNumber(Object value) {
        // Preserve sort direction values (-1, 1)
        if (value instanceof Integer) {
            int intVal = (Integer) value;
            if (intVal == 1 || intVal == -1) {
                return value; // Preserve sort order
            }
        }
        
        if (value instanceof Integer || value instanceof Long) {
            String numStr = value.toString();
            String redacted = DIGITS_PATTERN.matcher(numStr).replaceAll("9");
            try {
                if (value instanceof Integer) {
                    return Integer.parseInt(redacted);
                } else {
                    return Long.parseLong(redacted);
                }
            } catch (NumberFormatException e) {
                return value instanceof Integer ? 999 : 999L;
            }
        }
        
        if (value instanceof Double || value instanceof Float) {
            String numStr = value.toString();
            String redacted = DIGITS_PATTERN.matcher(numStr).replaceAll("9");
            try {
                if (value instanceof Double) {
                    return Double.parseDouble(redacted);
                } else {
                    return Float.parseFloat(redacted);
                }
            } catch (NumberFormatException e) {
                return value instanceof Double ? 999.0 : 999.0f;
            }
        }
        
        String numStr = value.toString();
        return DIGITS_PATTERN.matcher(numStr).replaceAll("9");
    }
    
    // Private helper methods for trimming
    private static JsonNode trimJsonNode(JsonNode node) {
        if (node.isObject()) {
            Iterator<String> fieldNames = node.fieldNames();
            List<String> keysToRemove = new ArrayList<>();
            
            while (fieldNames.hasNext()) {
                String fieldName = fieldNames.next();
                JsonNode childNode = node.get(fieldName);
                
                if (TRIM_FIELDS.contains(fieldName)) {
                    keysToRemove.add(fieldName);
                } else {
                    processTrimField(node, fieldName, childNode);
                }
            }
            
            // Remove trimmed keys
            for (String key : keysToRemove) {
                ((ObjectNode) node).remove(key);
            }
        } else if (node.isArray()) {
            for (JsonNode arrayElement : node) {
                trimJsonNode(arrayElement);
            }
        }
        return node;
    }
    
    private static void processTrimField(JsonNode parent, String fieldName, JsonNode childNode) {
        if (childNode.isTextual()) {
            String textValue = childNode.asText();
            if (!PRESERVE_TEXT_FIELDS.contains(fieldName) && textValue.length() > 35) {
                ((ObjectNode) parent).set(fieldName, new TextNode(textValue.substring(0, 35) + "..."));
            }
        } else if (childNode.isArray() && childNode.size() > 3) {
            if (!PRESERVE_ARRAY_FIELDS.contains(fieldName)) {
                ArrayNode arr = (ArrayNode) childNode;
                JsonNode first = arr.get(0);
                int size = arr.size();
                arr.removeAll();
                arr.add(first);
                arr.add(new TextNode("<truncated " + (size - 1) + " elements>"));
            }
            // Recursively trim array elements
            for (JsonNode arrayElement : childNode) {
                trimJsonNode(arrayElement);
            }
        } else if (childNode.isObject()) {
            if (childNode.isEmpty()) {
                ((ObjectNode) parent).remove(fieldName);
            } else {
                trimJsonNode(childNode);
            }
        }
    }
    
    // Legacy method for backward compatibility with existing QuerySanitizer usage
    private static JSONObject sanitizeObject(JSONObject obj) throws JSONException {
        JSONObject sanitized = new JSONObject();
        
        Iterator<String> keys = obj.keys();
        while (keys.hasNext()) {
            String key = keys.next();
            Object value = obj.get(key);
            sanitized.put(key, sanitizeValue(value));
        }
        
        return sanitized;
    }
    
    private static Object sanitizeValue(Object value) throws JSONException {
        if (value == null || value == JSONObject.NULL) {
            return value;
        }
        
        if (value instanceof JSONObject) {
            JSONObject obj = (JSONObject) value;
            
            if (obj.has("$regularExpression")) {
                return redactRegularExpression(obj);
            }
            
            return sanitizeObject(obj);
        }
        
        if (value instanceof JSONArray) {
            JSONArray sanitized = new JSONArray();
            JSONArray arr = (JSONArray) value;
            
            for (int i = 0; i < arr.length(); i++) {
                Object arrValue = arr.get(i);
                sanitized.put(sanitizeValue(arrValue));
            }
            
            return sanitized;
        }
        
        if (value instanceof String) {
            return redactString((String) value);
        }
        
        if (value instanceof Number) {
            return redactNumber(value);
        }
        
        if (value instanceof Boolean) {
            return value;
        }
        
        return redactString(value.toString());
    }
}