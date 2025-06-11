package com.mongodb.log.parser;

import java.util.Iterator;
import java.util.regex.Pattern;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Utility class for sanitizing MongoDB queries to remove sensitive data while preserving structure
 */
public class QuerySanitizer {
    
    private static final Pattern DIGITS_PATTERN = Pattern.compile("\\d");
    private static final Pattern REGEX_SPECIAL_CHARS = Pattern.compile("[^\\^\\$\\.\\*\\+\\?\\(\\)\\[\\]\\{\\}\\|\\\\]");
    
    /**
     * Sanitizes a MongoDB filter query by obfuscating values while preserving field names and structure
     */
    public static String sanitizeFilter(JSONObject filter) {
        if (filter == null) {
            return null;
        }
        
        try {
            JSONObject sanitized = sanitizeObject(filter);
            return sanitized.toString();
        } catch (Exception e) {
            // If sanitization fails, return a generic placeholder
            return "{\"sanitization_error\": \"xxx\"}";
        }
    }
    
    /**
     * Recursively sanitizes a JSON object
     */
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
    
    /**
     * Recursively sanitizes a JSON array
     */
    private static JSONArray sanitizeArray(JSONArray arr) throws JSONException {
        JSONArray sanitized = new JSONArray();
        
        for (int i = 0; i < arr.length(); i++) {
            Object value = arr.get(i);
            sanitized.put(sanitizeValue(value));
        }
        
        return sanitized;
    }
    
    /**
     * Sanitizes a value based on its type and content
     */
    private static Object sanitizeValue(Object value) throws JSONException {
        if (value == null || value == JSONObject.NULL) {
            return value;
        }
        
        if (value instanceof JSONObject) {
            JSONObject obj = (JSONObject) value;
            
            // Special handling for $regularExpression
            if (obj.has("$regularExpression")) {
                return sanitizeRegularExpression(obj);
            }
            
            return sanitizeObject(obj);
        }
        
        if (value instanceof JSONArray) {
            return sanitizeArray((JSONArray) value);
        }
        
        if (value instanceof String) {
            return sanitizeString((String) value);
        }
        
        if (value instanceof Number) {
            return sanitizeNumber(value);
        }
        
        if (value instanceof Boolean) {
            return value; // Keep boolean values as-is
        }
        
        // For any other type, convert to string and sanitize
        return sanitizeString(value.toString());
    }
    
    /**
     * Sanitizes a regular expression object, preserving anchors and special regex chars
     */
    private static JSONObject sanitizeRegularExpression(JSONObject regexObj) throws JSONException {
        JSONObject sanitized = new JSONObject();
        sanitized.put("$regularExpression", new JSONObject());
        
        JSONObject regex = regexObj.getJSONObject("$regularExpression");
        JSONObject sanitizedRegex = sanitized.getJSONObject("$regularExpression");
        
        if (regex.has("pattern")) {
            String pattern = regex.getString("pattern");
            sanitizedRegex.put("pattern", sanitizeRegexPattern(pattern));
        }
        
        if (regex.has("options")) {
            // Keep options as-is
            sanitizedRegex.put("options", regex.get("options"));
        }
        
        return sanitized;
    }
    
    /**
     * Sanitizes a regex pattern, preserving anchors and special chars but obfuscating content
     */
    private static String sanitizeRegexPattern(String pattern) {
        if (pattern == null || pattern.isEmpty()) {
            return pattern;
        }
        
        // Replace non-regex-special characters with 'x', but preserve regex metacharacters
        return REGEX_SPECIAL_CHARS.matcher(pattern).replaceAll("x");
    }
    
    /**
     * Sanitizes a string value
     */
    private static String sanitizeString(String value) {
        if (value == null || value.isEmpty()) {
            return value;
        }
        
        // For strings that look like they might contain sensitive data,
        // replace with a short dummy string
        if (value.length() <= 3) {
            return "xxx".substring(0, value.length());
        }
        
        return "xxx";
    }
    
    /**
     * Sanitizes a numeric value by replacing digits with 9
     */
    private static Object sanitizeNumber(Object value) {
        if (value instanceof Integer || value instanceof Long) {
            String numStr = value.toString();
            String sanitized = DIGITS_PATTERN.matcher(numStr).replaceAll("9");
            try {
                if (value instanceof Integer) {
                    return Integer.parseInt(sanitized);
                } else {
                    return Long.parseLong(sanitized);
                }
            } catch (NumberFormatException e) {
                // If parsing fails, return a default sanitized number
                return value instanceof Integer ? 999 : 999L;
            }
        }
        
        if (value instanceof Double || value instanceof Float) {
            String numStr = value.toString();
            String sanitized = DIGITS_PATTERN.matcher(numStr).replaceAll("9");
            try {
                if (value instanceof Double) {
                    return Double.parseDouble(sanitized);
                } else {
                    return Float.parseFloat(sanitized);
                }
            } catch (NumberFormatException e) {
                // If parsing fails, return a default sanitized number
                return value instanceof Double ? 999.0 : 999.0f;
            }
        }
        
        // For other numeric types, convert to string and sanitize
        String numStr = value.toString();
        return DIGITS_PATTERN.matcher(numStr).replaceAll("9");
    }
}