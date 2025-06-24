package com.mongodb.log.filter;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.mongodb.util.MimeTypes;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Unified Log Filter with configurable patterns and TTL operation tracking
 */
@Command(name = "logFilter", mixinStandardHelpOptions = true, version = "0.2", 
         description = "MongoDB log filter with configurable ignore patterns")
public class LogFilter implements Callable<Integer> {

    private static Logger logger = LoggerFactory.getLogger(LogFilter.class);

    @Option(names = "-f", description = "File names (if not provided, reads from stdin)")
    private List<String> files;

    @Option(names = "--config", description = "Properties file for filter configuration")
    private String configFile;

    @Option(names = "--report-ttl", description = "Report TTL operations by namespace")
    private boolean reportTtl = false;
    
    @Option(names = "--stdout", description = "Output filtered logs to stdout instead of files")
    private boolean outputToStdout = false;

    @Option(names = "--aggressive", description = "Use aggressive filtering mode (removes more fields)")
    private boolean aggressiveMode = false;

    private String currentLine;
    private FilterConfig filterConfig;

    // Default filtering keys
    private static List<String> defaultIgnoreKeys = List.of(
        "writeConcern", "$audit", "$client", "$clusterTime", "$configTime", "$db", 
        "$topologyTime", "advanced", "bypassDocumentValidation", "clientOperationKey",
        "clusterTime", "collation", "cpuNanos", "cursor", "cursorid", "cursorExhausted", 
        "databaseVersion", "flowControl", "fromMongos", "fromMultiPlanner", "let", "locks", 
        "lsid", "maxTimeMS", "maxTimeMSOpOnly", "mayBypassWriteBlocking", "multiKeyPaths", 
        "needsMerge", "needTime", "numYields", "protocol", 
        "queryFramework", "readConcern", "remote", "runtimeConstants", "shardVersion",
        "totalOplogSlotDurationMicros", "waitForWriteConcernDurationMillis", "works"
    );

    // Additional keys to ignore in aggressive mode (additive to default)
    private static List<String> additionalAggressiveKeys = List.of(
        "planningTimeMicros"
    );

    private static List<String> preserveTextFields = List.of("ns", "planSummary");
    private static List<String> preserveArrayFields = List.of("pipeline", "$and", "$or");

    @Override
    public Integer call() throws Exception {
        filterConfig = new FilterConfig();
        loadConfiguration();
        read();
        return 0;
    }

    private void loadConfiguration() {
        if (configFile != null) {
            try {
                Properties props = new Properties();
                props.load(new FileInputStream(configFile));
                filterConfig.loadFromProperties(props);
                logger.info("Loaded filter configuration from: {}", configFile);
            } catch (IOException e) {
                logger.warn("Could not load config file: {}. Using defaults.", configFile);
            }
        }
    }

    public void read() throws IOException, ParseException, InterruptedException, ExecutionException {
        if (files == null || files.isEmpty()) {
            // Read from stdin
            readFromStdin();
        } else {
            // Read from files
            for (String fileName : files) {
                File f = new File(fileName);
                read(f);
            }
        }
    }

    public void read(File file) throws IOException, ParseException, InterruptedException, ExecutionException {
        String originalFileName = file.getName();
        String originalFileExtension = "";
        int dotIndex = originalFileName.lastIndexOf('.');
        if (dotIndex > 0 && dotIndex < originalFileName.length() - 1) {
            originalFileExtension = originalFileName.substring(dotIndex);
            originalFileName = originalFileName.substring(0, dotIndex);
        }

        String guess = MimeTypes.guessContentTypeFromName(originalFileName);
        logger.debug("MIME type guess: {}", guess);

        BufferedReader in = createReader(file, guess);
        PrintStream os;
        
        if (outputToStdout) {
            os = System.out;
        } else {
            String filteredFileName = originalFileName + "_filtered" + originalFileExtension;
            File filteredFile = new File(file.getParent(), filteredFileName);
            os = new PrintStream(new BufferedOutputStream(new FileOutputStream(filteredFile)));
        }

        int filteredCount = 0;
        int ttlCount = 0;
        int lineNum = 0;
        long start = System.currentTimeMillis();

        while ((currentLine = in.readLine()) != null) {
            lineNum++;

            if (shouldIgnoreLine(currentLine) || !containsMongoJsonPattern(currentLine)) {
                filteredCount++;
                continue;
            }

            JsonNode filteredNode = filterJsonContent(currentLine);
            if (filteredNode != null && !filteredNode.isEmpty()) {
                os.println(filteredNode.toString());
            }

            if (lineNum % 25000 == 0) {
                System.out.print(".");
                if (lineNum % 250000 == 0) {
                    System.out.println();
                }
            }
        }

        in.close();
        if (!outputToStdout) {
            os.close();
        }
        
        long end = System.currentTimeMillis();
        long dur = (end - start);
        logger.info("Processed file: {} in {}ms", file.getName(), dur);
        logger.info("Lines: {}, Filtered: {}, TTL ops: {}", lineNum, filteredCount, ttlCount);
    }

    private String processLine(String line) {
        // Extract JSON part if line has filename prefix from grep
        String jsonPart = extractJsonFromLine(line);
        JsonNode filteredNode = filterJsonContent(jsonPart);
        if (filteredNode != null && !filteredNode.isEmpty()) {
            return filteredNode.toString();
        }
        return null;
    }

    private String extractJsonFromLine(String line) {
        int colonIndex = line.indexOf(':');
        if (colonIndex > 0 && colonIndex < line.length() - 1) {
            String afterColon = line.substring(colonIndex + 1);
            if (afterColon.startsWith("{\"t\":{\"$date\"")) {
                return afterColon;
            }
        }
        return line;
    }

    public void readFromStdin() throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        PrintStream os = outputToStdout ? System.out : System.err; // Use stderr for stats when stdout is for data
        
        int filteredCount = 0;
        int totalCount = 0;
        long start = System.currentTimeMillis();
        
        String line;
        while ((line = in.readLine()) != null) {
            totalCount++;
            currentLine = line;
            
            if (!shouldIgnoreLine(line) && containsMongoJsonPattern(line)) {
                String processedLine = processLine(line);
                if (processedLine != null) {
                    filteredCount++;
                    System.out.println(processedLine);
                }
            }
        }
        
        long end = System.currentTimeMillis();
        long dur = (end - start);
        
        // Send stats to stderr so they don't interfere with piped output
        if (outputToStdout) {
            System.err.printf("Processed stdin in %dms%n", dur);
            System.err.printf("Lines: %d, Filtered: %d%n", totalCount, filteredCount);
        } else {
            logger.info("Processed stdin in {}ms", dur);
            logger.info("Lines: {}, Filtered: {}", totalCount, filteredCount);
        }
    }

    private BufferedReader createReader(File file, String mimeType) throws IOException {
        if (mimeType != null && mimeType.equals(MimeTypes.GZIP)) {
            FileInputStream fis = new FileInputStream(file);
            GZIPInputStream gzis = new GZIPInputStream(fis);
            return new BufferedReader(new InputStreamReader(gzis));
        } else if (mimeType != null && mimeType.equals(MimeTypes.ZIP)) {
            FileInputStream fis = new FileInputStream(file);
            ZipInputStream zis = new ZipInputStream(fis);
            return new BufferedReader(new InputStreamReader(zis));
        } else {
            return new BufferedReader(new FileReader(file));
        }
    }

    private boolean shouldIgnoreLine(String line) {
        return filterConfig.shouldIgnore(line);
    }

    private boolean containsMongoJsonPattern(String line) {
        // Handle grep output with filename prefix (filename.gz:{"t":{"$date"...)
        int colonIndex = line.indexOf(':');
        if (colonIndex > 0 && colonIndex < line.length() - 1) {
            String jsonPart = line.substring(colonIndex + 1);
            return jsonPart.startsWith("{\"t\":{\"$date\"");
        }
        // Also handle direct JSON input
        return line.startsWith("{\"t\":{\"$date\"");
    }

    private List<String> getIgnoreKeys() {
        if (aggressiveMode) {
            // Combine default keys with additional aggressive keys
            List<String> combined = new ArrayList<>(defaultIgnoreKeys);
            combined.addAll(additionalAggressiveKeys);
            return combined;
        }
        return defaultIgnoreKeys;
    }

    private JsonNode filterJsonContent(String jsonLine) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode rootNode = mapper.readTree(jsonLine);
            return filterJson(rootNode);
        } catch (Exception e) {
            logger.debug("Failed to parse JSON: {}", e.getMessage());
            return null;
        }
    }

    private JsonNode filterJson(JsonNode node) {
        if (node.isObject()) {
            Iterator<String> fieldNames = node.fieldNames();
            List<String> keysToRemove = new ArrayList<>();

            while (fieldNames.hasNext()) {
                String fieldName = fieldNames.next();
                JsonNode childNode = node.get(fieldName);

                if (getIgnoreKeys().contains(fieldName)) {
                    keysToRemove.add(fieldName);
                } else {
                    processJsonField(node, fieldName, childNode);
                }
            }

            // Remove ignored keys
            for (String key : keysToRemove) {
                ((ObjectNode) node).remove(key);
            }
        } else if (node.isArray()) {
            for (JsonNode arrayElement : node) {
                filterJson(arrayElement);
            }
        }
        return node;
    }

    private void processJsonField(JsonNode parent, String fieldName, JsonNode childNode) {
        if (childNode.isTextual()) {
            String textValue = childNode.asText();
            if (!preserveTextFields.contains(fieldName) && textValue.length() > 35) {
                ((ObjectNode) parent).set(fieldName, new TextNode(textValue.substring(0, 35) + "..."));
            }
        } else if (childNode.isArray() && childNode.size() > 3) {
            if (!preserveArrayFields.contains(fieldName)) {
                ArrayNode arr = (ArrayNode) childNode;
                JsonNode first = arr.get(0);
                int size = arr.size();
                arr.removeAll();
                arr.add(first);
                arr.add(new TextNode("<truncated " + (size - 1) + " elements>"));
            }
            // Recursively filter array elements
            for (JsonNode arrayElement : childNode) {
                filterJson(arrayElement);
            }
        } else if (childNode.isObject()) {
            if (childNode.isEmpty()) {
                ((ObjectNode) parent).remove(fieldName);
            } else {
                filterJson(childNode);
            }
        }
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new LogFilter()).execute(args);
        System.exit(exitCode);
    }
}