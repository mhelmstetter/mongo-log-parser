package com.mongodb.logparse;

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

import com.fasterxml.jackson.core.JsonFactory;
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

    @Option(names = "-f", description = "File names", required = true)
    private List<String> files;

    @Option(names = "--config", description = "Properties file for filter configuration")
    private String configFile;

    @Option(names = "--report-ttl", description = "Report TTL operations by namespace")
    private boolean reportTtl = false;

    private String currentLine;
    private JsonFactory jsonFactory;
    private FilterConfig filterConfig;
    private Accumulator ttlAccumulator; // Track TTL operations separately

    // Simplified ignore keys for JSON filtering
    private static List<String> ignoreJsonKeys = List.of(
        "writeConcern", "$audit", "$client", "$clusterTime", "$configTime", "$db", 
        "$topologyTime", "advanced", "bypassDocumentValidation", "clientOperationKey",
        "clusterTime", "collation", "cpuNanos", "cursor", "cursorid", "cursorExhausted", 
        "databaseVersion", "flowControl", "fromMongos", "fromMultiPlanner", "let", "locks", 
        "lsid", "maxTimeMS", "maxTimeMSOpOnly", "mayBypassWriteBlocking", "multiKeyPaths", 
        "needsMerge", "needTime", "numYields", "planningTimeMicros", "protocol", 
        "queryFramework", "readConcern", "remote", "runtimeConstants", "shardVersion",
        "totalOplogSlotDurationMicros", "waitForWriteConcernDurationMillis", "works"
    );

    private static List<String> preserveTextFields = List.of("ns", "planSummary");
    private static List<String> preserveArrayFields = List.of("pipeline", "$and", "$or");

    @Override
    public Integer call() throws Exception {
        jsonFactory = JsonFactory.builder().build();
        filterConfig = new FilterConfig();
        
        if (reportTtl) {
            ttlAccumulator = new Accumulator();
        }
        
        loadConfiguration();
        read();
        
        if (reportTtl && ttlAccumulator != null) {
            reportTtlOperations();
        }
        
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
        for (String fileName : files) {
            File f = new File(fileName);
            read(f);
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

        String filteredFileName = originalFileName + "_filtered" + originalFileExtension;
        File filteredFile = new File(file.getParent(), filteredFileName);

        String guess = MimeTypes.guessContentTypeFromName(originalFileName);
        logger.debug("MIME type guess: {}", guess);

        BufferedReader in = createReader(file, guess);
        PrintStream os = new PrintStream(new BufferedOutputStream(new FileOutputStream(filteredFile)));

        int filteredCount = 0;
        int ttlCount = 0;
        int lineNum = 0;
        long start = System.currentTimeMillis();

        while ((currentLine = in.readLine()) != null) {
            lineNum++;

            // Check for TTL operations first (before filtering)
            if (reportTtl && currentLine.contains("TTL") && currentLine.contains("deleted")) {
                processTtlOperation(currentLine);
                ttlCount++;
            }

            if (shouldIgnoreLine(currentLine) || !currentLine.startsWith("{\"t\":{\"$date\"")) {
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
        os.close();
        
        long end = System.currentTimeMillis();
        long dur = (end - start);
        logger.info("Processed file: {} in {}ms", file.getName(), dur);
        logger.info("Lines: {}, Filtered: {}, TTL ops: {}", lineNum, filteredCount, ttlCount);
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

                if (ignoreJsonKeys.contains(fieldName)) {
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

    private void processTtlOperation(String line) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode rootNode = mapper.readTree(line);
            
            if (rootNode.has("attr")) {
                JsonNode attr = rootNode.get("attr");
                if (attr.has("namespace") && attr.has("numDeleted")) {
                    String namespace = attr.get("namespace").asText();
                    int numDeleted = attr.get("numDeleted").asInt();
                    int durationMs = attr.has("durationMillis") ? attr.get("durationMillis").asInt() : 0;
                    
                    // Add to TTL accumulator
                    Namespace ns = new Namespace(namespace);
                    ttlAccumulator.accumulate(null, "ttl_delete", ns, (long) durationMs, 
                                            null, null, (long) numDeleted, null, null);
                }
            }
        } catch (Exception e) {
            logger.debug("Failed to parse TTL operation: {}", e.getMessage());
        }
    }

    private void reportTtlOperations() {
        logger.info("=== TTL Operations by Namespace ===");
        if (ttlAccumulator.getAccumulators().isEmpty()) {
            logger.info("No TTL operations found");
            return;
        }
        
        // Print header
        System.out.println(String.format("%-40s %-10s %-10s %-10s %-10s", 
                "Namespace", "Count", "Deleted", "Min(ms)", "Max(ms)"));
        System.out.println("=".repeat(80));
        
        // Print TTL stats
        ttlAccumulator.getAccumulators().values().stream()
                .sorted((a, b) -> Long.compare(b.getCount(), a.getCount()))
                .forEach(acc -> {
                    System.out.println(String.format("%-40s %-10d %-10d %-10d %-10d",
                            acc.toString().split(" ")[0], // namespace
                            acc.getCount(),
                            acc.getAvgReturned() * acc.getCount(), // total deleted
                            acc.getMax(), // This might need adjustment based on actual min tracking
                            acc.getMax()));
                });
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new LogFilter()).execute(args);
        System.exit(exitCode);
    }
}