package com.mongodb.log.parser;

import org.json.JSONObject;

/**
 * Utility class for sanitizing MongoDB queries to remove sensitive data while preserving structure
 * @deprecated Use LogRedactionUtil.sanitizeFilter instead
 */
@Deprecated
public class QuerySanitizer {
    
    /**
     * Sanitizes a MongoDB filter query by obfuscating values while preserving field names and structure
     * @deprecated Use LogRedactionUtil.sanitizeFilter instead
     */
    @Deprecated
    public static String sanitizeFilter(JSONObject filter) {
        return LogRedactionUtil.sanitizeFilter(filter, true);
    }
}