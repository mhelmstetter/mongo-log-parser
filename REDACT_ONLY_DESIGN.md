# --redactOnly Feature Design

## Overview
Add a `--redactOnly` flag that reads MongoDB log files, applies redaction/sanitization and filtering, then outputs clean log files - without performing full parsing and report generation.

## Use Cases
1. **Privacy**: Redact sensitive data from logs before sharing with MongoDB support or team members
2. **Compliance**: Sanitize logs before storing in compliance-unfriendly locations
3. **Log Size Reduction**: Filter out unwanted log lines (e.g., connection events) before analysis
4. **Pre-processing**: Prepare logs for external tools or further analysis

## Requirements
- Minimal code duplication - reuse existing components
- Support all input formats (plain text, gzip, zip)
- Support output compression (auto-detect from output filename)
- Apply existing filters (namespace, config-based patterns)
- Apply existing redaction (query sanitization)
- Fast streaming processing (don't load entire file in memory)
- Preserve log format (JSON structure)

## Architecture

### Reusable Components

#### From LogParser:
- ✅ `createReader(File, String)` - Handles gzip/zip/plain file reading with buffering
- ✅ `readLineSafe(BufferedReader, int)` - Safe line reading with size limits
- ✅ `FilterConfig` + `shouldIgnore(String)` - Pattern-based line filtering
- ✅ `namespaceFilters` - Namespace-based filtering

#### From LogRedactionUtil:
- ✅ `processLogMessage(String, boolean)` - Trim + redact log message
- ✅ `redactLogMessage(String, boolean)` - Query/filter redaction

#### From LogFilter:
- ✅ `filterLogMessage(String)` - JSON field filtering (removes verbose fields)

### New Components Needed

#### 1. Output Writer Factory
**Location**: New method in `LogParser` or utility class

```java
private BufferedWriter createWriter(File file) throws IOException {
    String mimeType = MimeTypes.guessContentTypeFromName(file.getName());
    int bufferSize = 16 * 1024 * 1024; // 16MB buffer

    if (mimeType != null && mimeType.equals(MimeTypes.GZIP)) {
        return new BufferedWriter(
            new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(file))),
            bufferSize);
    } else {
        return new BufferedWriter(new FileWriter(file), bufferSize);
    }
}
```

#### 2. Redaction Pipeline Method
**Location**: New method in `LogParser`

```java
private void redactAndFilterLogs() throws IOException {
    // Validate: redactOnly requires output file
    if (redactOutputFile == null) {
        System.err.println("❌ --redactOnly requires --redactOutput to be specified");
        return;
    }

    System.out.println("🔒 Redaction-only mode");
    System.out.println("   Input files: " + fileNames.length);
    System.out.println("   Output file: " + redactOutputFile);
    System.out.println("   Redaction: " + (redactQueries ? "enabled" : "disabled"));
    System.out.println("   Filters: " + (configFile != null ? configFile : "none"));
    if (!namespaceFilters.isEmpty()) {
        System.out.println("   Namespace filters: " + namespaceFilters);
    }

    long startTime = System.currentTimeMillis();
    long totalLines = 0;
    long writtenLines = 0;
    long filteredLines = 0;

    try (BufferedWriter writer = createWriter(new File(redactOutputFile))) {
        for (String fileName : fileNames) {
            File file = new File(fileName);
            System.out.println("📖 Processing: " + fileName);

            String guess = MimeTypes.guessContentTypeFromName(file.getName());
            BufferedReader reader = createReader(file, guess);

            String line;
            while ((line = readLineSafe(reader, 1 * 1024 * 1024)) != null) {
                totalLines++;

                // Apply line-level filtering
                if (shouldSkipLine(line)) {
                    filteredLines++;
                    continue;
                }

                // Apply namespace filtering if configured
                if (!namespaceFilters.isEmpty() && !matchesNamespaceFilter(line)) {
                    filteredLines++;
                    continue;
                }

                // Apply redaction/processing
                String processed = processLine(line);

                writer.write(processed);
                writer.write('\n');
                writtenLines++;

                // Progress reporting
                if (totalLines % 100000 == 0) {
                    System.out.printf("   Processed %,d lines, wrote %,d, filtered %,d\n",
                                     totalLines, writtenLines, filteredLines);
                }
            }

            reader.close();
            System.out.println("✅ Completed: " + fileName);
        }

        writer.flush();
    }

    long elapsed = System.currentTimeMillis() - startTime;
    System.out.println("\n🎉 Redaction complete!");
    System.out.printf("   Total lines: %,d\n", totalLines);
    System.out.printf("   Written: %,d (%.1f%%)\n", writtenLines,
                     100.0 * writtenLines / Math.max(totalLines, 1));
    System.out.printf("   Filtered: %,d (%.1f%%)\n", filteredLines,
                     100.0 * filteredLines / Math.max(totalLines, 1));
    System.out.printf("   Time: %.1f seconds (%.0f lines/sec)\n",
                     elapsed / 1000.0, totalLines / (elapsed / 1000.0));
}

private String processLine(String line) {
    // 1. Trim/filter JSON fields (remove verbose attrs)
    String processed = LogRedactionUtil.trimLogMessage(line);

    // 2. Apply redaction if enabled
    if (redactQueries) {
        processed = LogRedactionUtil.redactLogMessage(processed, true);
    }

    return processed;
}

private boolean shouldSkipLine(String line) {
    // Use existing FilterConfig logic
    return filterConfig != null && filterConfig.shouldIgnore(line);
}

private boolean matchesNamespaceFilter(String line) {
    // Extract namespace from log line and check against filters
    // Reuse existing namespace filter logic
    try {
        // Quick check: if line doesn't have "ns" field, skip parsing
        if (!line.contains("\"ns\":")) {
            return true; // No namespace to filter, include it
        }

        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(line);
        JsonNode nsNode = root.path("attr").path("ns");

        if (nsNode.isMissingNode() || nsNode.isNull()) {
            return true; // No namespace, include
        }

        String namespace = nsNode.asText();

        // Check against filters (reuse existing logic)
        for (String filter : namespaceFilters) {
            if (filter.endsWith(".*")) {
                String dbPrefix = filter.substring(0, filter.length() - 2);
                if (namespace.startsWith(dbPrefix + ".")) {
                    return true;
                }
            } else if (namespace.equals(filter)) {
                return true;
            }
        }

        return false; // Doesn't match any filter, exclude
    } catch (Exception e) {
        return true; // Parse error, include line
    }
}
```

### Command-Line Integration

#### New Options
```java
@Option(names = {"--redactOnly"},
        description = "Redaction-only mode: read logs, apply redaction and filters, output cleaned logs (skips parsing and reporting)")
private boolean redactOnly = false;

@Option(names = {"--redactOutput"},
        description = "Output file for redacted logs (supports .gz for compression)")
private String redactOutputFile;
```

#### Modified call() Method
```java
@Override
public Integer call() throws Exception {
    long start = System.currentTimeMillis();

    // Validate arguments
    if (fileNames == null || fileNames.length == 0) {
        System.err.println("❌ No input files specified");
        return 1;
    }

    // NEW: Handle redactOnly mode
    if (redactOnly) {
        redactAndFilterLogs();
        return 0;
    }

    // Existing: Full parsing and reporting
    // ... rest of existing code ...
}
```

## Usage Examples

### Basic Redaction
```bash
java -jar MongoLogParser.jar \
  -f mongodb.log \
  --redactOnly \
  --redactOutput mongodb-redacted.log \
  --redact
```

### Redact with Filtering
```bash
java -jar MongoLogParser.jar \
  -f mongodb.log \
  --redactOnly \
  --redactOutput mongodb-clean.log.gz \
  --redact \
  --config filter-config.properties \
  --ns mydb.users --ns mydb.orders
```

### Multiple Files to Single Output
```bash
java -jar MongoLogParser.jar \
  -f shard-01.log.gz shard-02.log.gz shard-03.log.gz \
  --redactOnly \
  --redactOutput all-shards-redacted.log.gz \
  --redact
```

### Redact Without Query Sanitization (Just Filter)
```bash
java -jar MongoLogParser.jar \
  -f mongodb.log \
  --redactOnly \
  --redactOutput mongodb-filtered.log \
  --config filter-config.properties
```

## Benefits of This Design

### ✅ No Code Duplication
- Reuses `createReader()` for input
- Reuses `readLineSafe()` for safe reading
- Reuses `LogRedactionUtil` for redaction
- Reuses `FilterConfig` for filtering
- Reuses namespace filter logic
- Adds symmetric `createWriter()` for output

### ✅ Clean Separation
- Redaction-only mode is a separate code path
- Doesn't complicate existing parsing logic
- Easy to test independently

### ✅ Consistent Behavior
- Uses same redaction logic as reporting
- Uses same filtering logic as parsing
- Same input format support (gzip, zip, plain)

### ✅ Performance
- Streaming processing (low memory)
- Large buffer sizes (16MB)
- No accumulator overhead
- Fast line-by-line processing

### ✅ User Experience
- Clear progress reporting
- Statistics at end
- Auto-compression from filename
- Works with existing flags (--config, --ns, --redact)

## Implementation Checklist

1. [ ] Add `--redactOnly` and `--redactOutput` command-line options
2. [ ] Implement `createWriter()` method with compression support
3. [ ] Implement `redactAndFilterLogs()` method
4. [ ] Implement `processLine()` helper
5. [ ] Implement `matchesNamespaceFilter()` helper (or extract existing logic)
6. [ ] Modify `call()` to route to redactOnly mode
7. [ ] Add validation (redactOnly requires redactOutput)
8. [ ] Add unit tests for redaction pipeline
9. [ ] Add integration test with sample log file
10. [ ] Update README with usage examples
11. [ ] Test with gzip input and output
12. [ ] Test with namespace filters
13. [ ] Test with config file filters
14. [ ] Build and deploy

## Error Handling

### Input Errors
- Missing input files → Clear error message, exit code 1
- Unreadable files → Error message, skip file, continue
- Invalid compression format → Fall back to plain text

### Output Errors
- Missing output path when --redactOnly → Error message, exit code 1
- Cannot create output file → Error message with suggestion, exit code 1
- Disk full during writing → Detect with flush/close, clean up partial file, error message
- Permission denied → Clear error message, exit code 1

### Processing Errors
- Line too long → Skip with warning (reuse existing logic)
- Invalid JSON → Include original line with warning
- Parse errors → Include original line, continue

## Future Enhancements (Out of Scope)

- **Parallel processing**: Process multiple files in parallel
- **Streaming output**: Support stdout for piping to other tools
- **Sampling**: Include only a percentage of lines (e.g., --sample 0.1)
- **Date range filtering**: Filter by timestamp range
- **Operation type filtering**: Filter specific operations (find, aggregate, etc.)
- **Line format transformation**: Convert to different log formats

## Testing Strategy

### Unit Tests
```java
@Test
public void testRedactionPipeline() {
    // Test processLine() with various inputs
}

@Test
public void testNamespaceFiltering() {
    // Test matchesNamespaceFilter() logic
}

@Test
public void testCreateWriter() {
    // Test gzip vs plain output
}
```

### Integration Tests
```java
@Test
public void testRedactOnlyMode() {
    // Create sample log file
    // Run with --redactOnly
    // Verify output file exists
    // Verify redacted content
    // Verify line counts
}

@Test
public void testRedactWithFilters() {
    // Test with --config filter
    // Verify filtered lines removed
}

@Test
public void testGzipOutput() {
    // Test .gz output
    // Verify compressed
    // Verify readable
}
```

## Estimated Effort

- **Design**: 1 hour ✅ (Done)
- **Implementation**: 2-3 hours
  - Add options: 15 min
  - createWriter(): 30 min
  - redactAndFilterLogs(): 1 hour
  - Helper methods: 45 min
  - Error handling: 30 min
- **Testing**: 1-2 hours
  - Unit tests: 45 min
  - Integration tests: 45 min
  - Manual testing: 30 min
- **Documentation**: 30 min

**Total**: ~4-6 hours
