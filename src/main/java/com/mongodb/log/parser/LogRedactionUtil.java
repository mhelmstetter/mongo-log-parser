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
        // Index and plan info
        "indexName", "direction", "stage", "inputStage", "rejectedPlans", "winningPlan",
        // Shard info
        "nShards", "shardName", "shardVersion",
        // Error codes and messages
        "code", "codeName", "ok", "errmsg", "errCode", "errMsg", "errName",
        // Transaction info
        "txnNumber", "autocommit", "startTransaction",
        // System info
        "component", "context", "severity", "id", "msg", "attr", "t", "c", "s", "type",
        // Date/time fields
        "$date"
    );
    
    // Fields that should be completely removed for log trimming (similar to LogFilter)
    private static final Set<String> TRIM_FIELDS = Set.of(
        "writeConcern", "$audit", "$client", "$clusterTime", "$configTime", "$db", 
        "$topologyTime", "advanced", "bypassDocumentValidation", "clientOperationKey",
        "clusterTime", "collation", "cpuNanos", "cursor", "cursorid", "cursorExhausted", 
        "databaseVersion", "flowControl", "fromMongos", "fromMultiPlanner", "let", "locks", 
        "lsid", "maxTimeMS", "maxTimeMSOpOnly", "mayBypassWriteBlocking", "multiKeyPaths", 
        "needsMerge", "needTime", "numYields", "planningTimeMicros", "protocol", 
        "queryFramework", "readConcern", "remote", "runtimeConstants", "shardVersion",
        "totalOplogSlotDurationMicros", "waitForWriteConcernDurationMillis", "works"
    );
    
    // Fields that should preserve text content but may be truncated for trimming
    private static final Set<String> PRESERVE_TEXT_FIELDS = Set.of("ns", "planSummary", "namespace");
    
    // Fields that should preserve array structure
    private static final Set<String> PRESERVE_ARRAY_FIELDS = Set.of("pipeline", "$and", "$or");
    
    /**
     * Redacts sensitive values in a log message while preserving field names and structure
     */
    public static String redactLogMessage(String logMessage, boolean enableRedaction) {
        if (!enableRedaction) {
            return logMessage;
        }
        
        try {
            JSONObject jo = new JSONObject(logMessage);
            JSONObject redacted = redactObject(jo);
            return redacted.toString();
        } catch (Exception e) {
            // If redaction fails, return original message
            return logMessage;
        }
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
            JSONObject sanitized = sanitizeObject(filter);
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
        JSONObject redacted = new JSONObject();
        
        Iterator<String> keys = obj.keys();
        while (keys.hasNext()) {
            String key = keys.next();
            Object value = obj.get(key);
            
            if (!isInQueryContext && PRESERVE_FIELDS.contains(key) && !key.equals("attr")) {
                // Preserve these fields completely (only at top level), except attr which needs processing
                redacted.put(key, value);
            } else if (isQueryField(key)) {
                // For query fields, redact values but preserve structure
                redacted.put(key, redactValue(value, true));
            } else if (isInQueryContext) {
                // We're inside a query context, redact all values
                redacted.put(key, redactValue(value, true));
            } else {
                // Redact the value but keep the field
                redacted.put(key, redactValue(value, false));
            }
        }
        
        return redacted;
    }
    
    // Check if a field contains query data that should be redacted
    private static boolean isQueryField(String fieldName) {
        return fieldName.equals("filter") || 
               fieldName.equals("command") || 
               fieldName.equals("find") || 
               fieldName.equals("aggregate") || 
               fieldName.equals("update") || 
               fieldName.equals("delete") || 
               fieldName.equals("insert") || 
               fieldName.equals("pipeline") ||
               fieldName.startsWith("$");
    }
    
    private static Object redactValue(Object value) throws JSONException {
        return redactValue(value, false);
    }
    
    private static Object redactValue(Object value, boolean isInQueryContext) throws JSONException {
        if (value == null || value == JSONObject.NULL) {
            return value;
        }
        
        if (value instanceof JSONObject) {
            JSONObject obj = (JSONObject) value;
            
            // Special handling for $regularExpression
            if (obj.has("$regularExpression")) {
                return redactRegularExpression(obj);
            }
            
            return redactObject(obj, isInQueryContext);
        }
        
        if (value instanceof JSONArray) {
            return redactArray((JSONArray) value, isInQueryContext);
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
        JSONArray redacted = new JSONArray();
        
        for (int i = 0; i < arr.length(); i++) {
            Object value = arr.get(i);
            redacted.put(redactValue(value, isInQueryContext));
        }
        
        return redacted;
    }
    
    private static JSONObject redactRegularExpression(JSONObject regexObj) throws JSONException {
        JSONObject redacted = new JSONObject();
        redacted.put("$regularExpression", new JSONObject());
        
        JSONObject regex = regexObj.getJSONObject("$regularExpression");
        JSONObject redactedRegex = redacted.getJSONObject("$regularExpression");
        
        if (regex.has("pattern")) {
            String pattern = regex.getString("pattern");
            redactedRegex.put("pattern", redactRegexPattern(pattern));
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