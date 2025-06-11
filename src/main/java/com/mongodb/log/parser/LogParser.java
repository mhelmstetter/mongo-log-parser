package com.mongodb.log.parser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipInputStream;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.log.filter.FilterConfig;
import com.mongodb.log.parser.accumulator.Accumulator;
import com.mongodb.log.parser.accumulator.ErrorCodeAccumulator;
import com.mongodb.log.parser.accumulator.PlanCacheAccumulator;
import com.mongodb.log.parser.accumulator.QueryHashAccumulator;
import com.mongodb.log.parser.accumulator.TransactionAccumulator;
import com.mongodb.util.MimeTypes;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Enhanced MongoDB Log Parser with unified filtering, TTL operation reporting, plan cache analysis, and namespace filtering
 */
@Command(name = "logParser", mixinStandardHelpOptions = true, version = "1.3", 
         description = "Parse MongoDB log files with configurable filtering, comprehensive reporting, plan cache analysis, and namespace filtering")
public class LogParser implements Callable<Integer> {

    static final Logger logger = LoggerFactory.getLogger(LogParser.class);

    @Option(names = { "-f", "--files" }, description = "MongoDB log file(s)", required = true, arity = "1..*")
    private String[] fileNames;

    @Option(names = { "-c", "--csv" }, description = "CSV output file")
    private String csvOutputFile;

    @Option(names = { "--config" }, description = "Filter configuration file")
    private String configFile;

    @Option(names = { "--debug" }, description = "Enable debug logging")
    private boolean debug = false;

    @Option(names = {"--ignored-analysis"}, description = "Output file for ignored lines analysis")
    private String ignoredAnalysisFile;

    @Option(names = {"--plan-cache-csv"}, description = "CSV output file for plan cache analysis")
    private String planCacheCsvFile;

    @Option(names = {"--query-hash-csv"}, description = "CSV output file for query hash analysis")
    private String queryHashCsvFile;
    
    @Option(names = {"--error-codes-csv"}, description = "CSV output file for error code analysis")
    private String errorCodesCsvFile;
    
    @Option(names = {"--transaction-csv"}, description = "CSV output file for transaction analysis")
    private String transactionCsvFile;

    @Option(names = {"--ns", "--namespace"}, description = "Filter to specific namespace(s). Can be specified multiple times. Supports patterns like 'mydb.*' or exact matches like 'mydb.mycoll'")
    private Set<String> namespaceFilters = new HashSet<>();
    
    @Option(names = {"--html"}, description = "HTML output file for interactive report")
    private String htmlOutputFile;

    // Statistics tracking
    private AtomicLong totalParseErrors = new AtomicLong(0);
    private AtomicLong totalNoAttr = new AtomicLong(0);
    private AtomicLong totalNoCommand = new AtomicLong(0);
    private AtomicLong totalNoNs = new AtomicLong(0);
    private AtomicLong totalFoundOps = new AtomicLong(0);
    private AtomicLong totalFilteredByNamespace = new AtomicLong(0);
    private Map<String, AtomicLong> operationTypeStats = new HashMap<>();

    private String currentLine = null;
    private final Accumulator accumulator;
    private final Accumulator ttlAccumulator;
    private final PlanCacheAccumulator planCacheAccumulator;
    private final ErrorCodeAccumulator errorCodeAccumulator;
    private final TransactionAccumulator transactionAccumulator;
    
    private FilterConfig filterConfig;
    private int unmatchedCount = 0;
    private int ignoredCount = 0;
    private int processedCount = 0;

    // Query shape tracking (if enabled)
    private Map<Namespace, Map<Set<String>, AtomicInteger>> shapesByNamespace = new HashMap<>();
    private Accumulator shapeAccumulator = new Accumulator();
    
    private QueryHashAccumulator queryHashAccumulator;

    // Ignored lines analysis
    private Map<String, AtomicLong> ignoredCategories = new HashMap<>();
    private PrintWriter ignoredWriter = null;

    public LogParser() {
        accumulator = new Accumulator();
        ttlAccumulator = new Accumulator();
        queryHashAccumulator = new QueryHashAccumulator();
        planCacheAccumulator = new PlanCacheAccumulator();
        errorCodeAccumulator = new ErrorCodeAccumulator();
        transactionAccumulator = new TransactionAccumulator();
        filterConfig = new FilterConfig();
    }

    @Override
    public Integer call() throws Exception {
        logger.info("Starting MongoDB log parsing with {} files and {} processors", 
                fileNames.length, Runtime.getRuntime().availableProcessors());
        
        // Debug: Print all files that will be processed
        for (int i = 0; i < fileNames.length; i++) {
            logger.info("File {}: {}", i + 1, fileNames[i]);
        }

        // Log namespace filtering configuration
        if (!namespaceFilters.isEmpty()) {
            logger.info("Namespace filtering enabled. Filters: {}", namespaceFilters);
        } else {
            logger.info("No namespace filtering - processing all namespaces");
        }

        loadConfiguration();
        read();

        logger.info("Parsing complete. Total processed: {}, Ignored: {}, Filtered by namespace: {}", 
                   processedCount, ignoredCount, totalFilteredByNamespace.get());
        reportOperationStats();
        reportIgnoredAnalysis();

        if (csvOutputFile != null) {
            logger.info("Writing CSV report to: {}", csvOutputFile);
            reportCsv();
        } else {
            report();
        }

        reportTtlOperations();
        
        // Generate plan cache reports if enabled
        if (planCacheAccumulator != null) {
            planCacheAccumulator.report();
            planCacheAccumulator.reportByQueryHash();
            
            if (planCacheCsvFile != null) {
                logger.info("Writing plan cache CSV report to: {}", planCacheCsvFile);
                planCacheAccumulator.reportCsv(planCacheCsvFile);
            }
        }
        
        if (queryHashAccumulator != null) {
            queryHashAccumulator.report();
            
            if (queryHashCsvFile != null) {
                logger.info("Writing query hash CSV report to: {}", queryHashCsvFile);
                queryHashAccumulator.reportCsv(queryHashCsvFile);
            }
        }
        
        if (errorCodeAccumulator.hasErrors()) {
            errorCodeAccumulator.report();
            
            if (errorCodesCsvFile != null) {
                logger.info("Writing error codes CSV report to: {}", errorCodesCsvFile);
                errorCodeAccumulator.reportCsv(errorCodesCsvFile);
            }
        }
        
        if (transactionAccumulator.hasTransactions()) {
            transactionAccumulator.report();
            
            if (transactionCsvFile != null) {
                logger.info("Writing transaction CSV report to: {}", transactionCsvFile);
                transactionAccumulator.reportCsv(transactionCsvFile);
            }
        }
        
        if (htmlOutputFile != null) {
            try {
                logger.info("Writing HTML report to: {}", htmlOutputFile);
                HtmlReportGenerator.generateReport(
                    htmlOutputFile, 
                    accumulator, 
                    ttlAccumulator, 
                    planCacheAccumulator,
                    queryHashAccumulator,
                    errorCodeAccumulator,
                    operationTypeStats
                );
                logger.info("HTML report generated successfully");
            } catch (IOException e) {
                logger.error("Failed to generate HTML report: {}", e.getMessage(), e);
            }
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

    /**
     * Check if a namespace matches any of the configured filters
     */
    private boolean matchesNamespaceFilter(Namespace namespace) {
        if (namespaceFilters.isEmpty()) {
            return true; // No filters means accept all
        }
        
        if (namespace == null) {
            return false;
        }
        
        String fullNamespace = namespace.toString();
        String dbName = namespace.getDatabaseName();
        
        for (String filter : namespaceFilters) {
            // Exact match
            if (filter.equals(fullNamespace)) {
                return true;
            }
            
            // Database wildcard pattern (e.g., "mydb.*")
            if (filter.endsWith(".*")) {
                String filterDb = filter.substring(0, filter.length() - 2);
                if (filterDb.equals(dbName)) {
                    return true;
                }
            }
            
            // Database-only filter (e.g., "mydb")
            if (!filter.contains(".") && filter.equals(dbName)) {
                return true;
            }
            
            // Simple wildcard matching for collection names
            if (filter.contains("*")) {
                String regex = filter.replace(".", "\\.")
                                   .replace("*", ".*");
                if (fullNamespace.matches(regex)) {
                    return true;
                }
            }
        }
        
        return false;
    }

    public void read() throws IOException, ExecutionException, InterruptedException {
        logger.info("Processing {} files: {}", fileNames.length, String.join(", ", fileNames));
        
        for (String fileName : fileNames) {
            File f = new File(fileName);
            
            if (!f.exists()) {
                logger.error("File does not exist: {}", fileName);
                continue;
            }
            
            if (!f.canRead()) {
                logger.error("Cannot read file: {}", fileName);
                continue;
            }
            
            try {
                logger.info("Starting to process file: {} (size: {} bytes)", fileName, f.length());
                read(f);
            } catch (Exception e) {
                logger.error("Failed to process file: {}. Error: {}", fileName, e.getMessage(), e);
                // Continue with next file
            }
        }
    }

    public void read(File file) throws IOException, ExecutionException, InterruptedException {
        String guess = MimeTypes.guessContentTypeFromName(file.getName());

        BufferedReader in = createReader(file, guess);
        
        // Initialize ignored lines analysis if requested
        if (ignoredAnalysisFile != null) {
            try {
                ignoredWriter = new PrintWriter(new FileWriter(ignoredAnalysisFile));
            } catch (IOException e) {
                logger.error("Failed to create ignored analysis file: {}", ignoredAnalysisFile, e);
                ignoredAnalysisFile = null;
            }
        }

        int numThreads = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CompletionService<ProcessingStats> completionService = new ExecutorCompletionService<>(executor);

        resetCounters();
        int lineNum = 0;
        int submittedTasks = 0;
        long start = System.currentTimeMillis();
        List<String> lines = new ArrayList<>();

        while ((currentLine = in.readLine()) != null) {
            lineNum++;

            // Process TTL operations before filtering
            if (isTtlOperation(currentLine)) {
                processTtlOperation(currentLine);
            }

            if (shouldIgnoreLine(currentLine)) {
                ignoredCount++;
                processIgnoredLine(currentLine);
                continue;
            }

            lines.add(currentLine);
            processedCount++;

            if (lines.size() >= 25000) {
                completionService.submit(
                    new LogParserTask(new ArrayList<>(lines), accumulator, planCacheAccumulator, queryHashAccumulator,
                    		errorCodeAccumulator, transactionAccumulator, operationTypeStats, debug, namespaceFilters, totalFilteredByNamespace));
                submittedTasks++;
                lines.clear();
            }
        }

        // Submit remaining lines
        if (!lines.isEmpty()) {
            completionService.submit(
                new LogParserTask(new ArrayList<>(lines), accumulator, planCacheAccumulator, queryHashAccumulator,
                		errorCodeAccumulator, transactionAccumulator, operationTypeStats, debug, namespaceFilters, totalFilteredByNamespace));
            submittedTasks++;
        }

        // Wait for all tasks to complete
        for (int i = 0; i < submittedTasks; i++) {
            try {
                ProcessingStats stats = completionService.take().get();
                updateStats(stats);
            } catch (ExecutionException e) {
                logger.error("Task execution failed", e);
            }
        }

        cleanup(executor, in);
        logProcessingResults(file, System.currentTimeMillis() - start, lineNum);
    }

    private BufferedReader createReader(File file, String mimeType) throws IOException {
        if (mimeType != null && mimeType.equals(MimeTypes.GZIP)) {
            return new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))));
        } else if (mimeType != null && mimeType.equals(MimeTypes.ZIP)) {
            return new BufferedReader(new InputStreamReader(new ZipInputStream(new FileInputStream(file))));
        } else {
            return new BufferedReader(new FileReader(file));
        }
    }

    private boolean shouldIgnoreLine(String line) {
        // Don't ignore if it's not JSON-like
        if (!line.trim().startsWith("{")) {
            return true;
        }

        // Never ignore lines that contain target operations
        if (containsTargetOperation(line)) {
            return false;
        }

        return filterConfig.shouldIgnore(line);
    }

    private boolean containsTargetOperation(String line) {
        return line.contains("\"find\":") || line.contains("\"aggregate\":") || 
               line.contains("\"update\":") || line.contains("\"insert\":") || 
               line.contains("\"delete\":") || line.contains("\"findAndModify\":") ||
               line.contains("\"getMore\":") || line.contains("\"count\":") || 
               line.contains("\"distinct\":");
    }

    private boolean isTtlOperation(String line) {
        return line.contains("TTL") && (line.contains("deleted") || line.contains("Deleted expired documents"));
    }

    private void processTtlOperation(String line) {
        try {
            JSONObject jo = new JSONObject(line);
            if (jo.has("attr")) {
                JSONObject attr = jo.getJSONObject("attr");
                if (attr.has("namespace")) {
                    String namespace = attr.getString("namespace");
                    Namespace ns = new Namespace(namespace);
                    
                    // Apply namespace filtering to TTL operations
                    if (!matchesNamespaceFilter(ns)) {
                        return;
                    }
                    
                    Long numDeleted = getMetric(attr, "numDeleted");
                    Long durationMs = getMetric(attr, "durationMillis");
                    
                    synchronized (ttlAccumulator) {
                        ttlAccumulator.accumulate(null, "ttl_delete", ns, durationMs, 
                                                null, null, numDeleted, null, null);
                    }
                }
            }
        } catch (Exception e) {
            if (debug) {
                logger.debug("Failed to parse TTL operation: {}", e.getMessage());
            }
        }
    }

    private void processIgnoredLine(String line) {
        // Category analysis for ignored lines
        String category = categorizeIgnoredLine(line);
        synchronized (ignoredCategories) {
            ignoredCategories.computeIfAbsent(category, k -> new AtomicLong(0)).incrementAndGet();
        }
        
        // Sample ignored lines if analysis is enabled
        if (ignoredWriter != null && ignoredCount % 100 == 0) { // Sample every 100th line
            ignoredWriter.println(line);
        }
    }

    private String categorizeIgnoredLine(String line) {
        if (line.contains("\"c\":\"NETWORK\"")) return "NETWORK";
        if (line.contains("\"c\":\"ACCESS\"")) return "ACCESS";
        if (line.contains("\"c\":\"STORAGE\"")) return "STORAGE";
        if (line.contains("\"c\":\"CONTROL\"")) return "CONTROL";
        if (line.contains("\"hello\":1") || line.contains("\"isMaster\":1")) return "HEALTH_CHECK";
        if (line.contains("\"replSetHeartbeat\"")) return "REPLICATION";
        if (line.contains("\"$db\":\"admin\"")) return "ADMIN_DB";
        if (line.contains("\"$db\":\"local\"")) return "LOCAL_DB";
        if (line.contains("\"$db\":\"config\"")) return "CONFIG_DB";
        if (line.contains("\"profile\":")) return "PROFILING";
        if (line.contains("TTL")) return "TTL_MONITOR";
        if (!line.trim().startsWith("{")) return "NON_JSON";
        return "OTHER";
    }

    private void resetCounters() {
        unmatchedCount = 0;
        ignoredCount = 0;
        processedCount = 0;
    }

    private void updateStats(ProcessingStats stats) {
        totalParseErrors.addAndGet(stats.parseErrors);
        totalNoAttr.addAndGet(stats.noAttr);
        totalNoCommand.addAndGet(stats.noCommand);
        totalNoNs.addAndGet(stats.noNs);
        totalFoundOps.addAndGet(stats.foundOps);
    }

    private void cleanup(ExecutorService executor, BufferedReader in) throws IOException {
        if (ignoredWriter != null) {
            ignoredWriter.close();
        }
        
        executor.shutdown();
        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                logger.warn("Executor did not terminate gracefully");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            logger.warn("Executor interrupted");
            Thread.currentThread().interrupt();
        }
        
        in.close();
    }

    private void logProcessingResults(File file, long duration, int totalLines) {
    	logger.info("File processing complete - Duration: {}ms | Lines: {} read, {} ignored, {} processed, {} filtered by namespace | Errors: {} parse, {} no attr, {} no command, {} no namespace | Operations parsed: {}", 
    		    duration, totalLines, ignoredCount, processedCount, totalFilteredByNamespace.get(),
    		    totalParseErrors.get(), totalNoAttr.get(), totalNoCommand.get(), totalNoNs.get(), totalFoundOps.get());

        if (totalFoundOps.get() == 0) {
            logger.warn("WARNING: No operations were successfully parsed!");
            logger.warn("This might indicate wrong log format, excessive filtering, or namespace filter mismatch");
            if (!namespaceFilters.isEmpty()) {
                logger.warn("Namespace filters applied: {}", namespaceFilters);
                logger.warn("Consider checking if your namespace filters match the actual namespaces in the logs");
            }
        }
    }

    private void reportOperationStats() {
        logger.info("=== Operation Type Breakdown ===");
        operationTypeStats.entrySet().stream()
                .sorted((e1, e2) -> Long.compare(e2.getValue().get(), e1.getValue().get()))
                .forEach(entry -> logger.info("  {}: {}", entry.getKey(), entry.getValue()));
        
        if (!namespaceFilters.isEmpty()) {
            logger.info("=== Namespace Filtering Results ===");
            logger.info("Namespace filters: {}", namespaceFilters);
            logger.info("Operations filtered by namespace: {}", totalFilteredByNamespace.get());
        }
    }

    private void reportIgnoredAnalysis() {
        if (!ignoredCategories.isEmpty()) {
            logger.info("=== Ignored Lines Analysis ===");
            ignoredCategories.entrySet().stream()
                .sorted((e1, e2) -> Long.compare(e2.getValue().get(), e1.getValue().get()))
                .forEach(entry -> {
                    String category = entry.getKey();
                    long count = entry.getValue().get();
                    double percentage = (count * 100.0) / Math.max(ignoredCount, 1);
                    logger.info("  {}: {} ({}%)", category, count, String.format("%.1f", percentage));
                });
            
            if (ignoredAnalysisFile != null) {
                logger.info("Ignored lines sample written to: {}", ignoredAnalysisFile);
            }
        }
    }

    private void reportTtlOperations() {
        if (ttlAccumulator.getAccumulators().isEmpty()) {
            logger.info("No TTL operations found");
            return;
        }
        
        logger.info("=== TTL Operations Report ===");
        
        // Calculate the maximum namespace width
        int calculatedWidth = ttlAccumulator.getAccumulators().values().stream()
            .mapToInt(acc -> {
                String namespace = acc.toString().split(" ")[0];
                return namespace.length();
            })
            .max()
            .orElse("Namespace".length());
            
        // Set minimum width to header length, maximum to reasonable limit
        final int maxNamespaceWidth = Math.min(Math.max(calculatedWidth, "Namespace".length()), 55);
        
        // Create dynamic format string
        String headerFormat = String.format("%%-%ds %%10s %%12s %%10s %%10s %%10s", maxNamespaceWidth);
        String dataFormat = String.format("%%-%ds %%10d %%12d %%10d %%10d %%10d", maxNamespaceWidth);
        int separatorLength = maxNamespaceWidth + 10 + 12 + 10 + 10 + 10 + 5; // +5 for spaces
        
        // Print header
        System.out.println(String.format(headerFormat, 
                "Namespace", "Count", "TotalDeleted", "AvgDeleted", "MinMs", "MaxMs"));
        System.out.println("=".repeat(separatorLength));
        
        // Print TTL stats with consistent formatting
        ttlAccumulator.getAccumulators().values().stream()
                .sorted((a, b) -> Long.compare(b.getCount(), a.getCount()))
                .forEach(acc -> {
                    String namespace = acc.toString().split(" ")[0]; // Extract namespace part only
                    
                    // Truncate namespace if it's too long
                    if (namespace.length() > maxNamespaceWidth) {
                        namespace = namespace.substring(0, maxNamespaceWidth - 3) + "...";
                    }
                    
                    long totalDeleted = acc.getAvgReturned() * acc.getCount();
                    
                    System.out.println(String.format(dataFormat,
                            namespace,
                            acc.getCount(),
                            totalDeleted,
                            acc.getAvgReturned(),
                            acc.getMin(),
                            acc.getMax()));
                });
    }

    static Long getMetric(JSONObject attr, String key) {
        if (attr.has(key)) {
            return Long.valueOf(attr.getInt(key));
        }
        return null;
    }

    // Getter methods
    public Accumulator getAccumulator() {
        return accumulator;
    }

    public void report() {
        accumulator.report();
    }

    public void reportCsv() throws FileNotFoundException {
        accumulator.reportCsv(csvOutputFile);
    }

    public int getUnmatchedCount() {
        return unmatchedCount;
    }

    public String[] getFileNames() {
        return fileNames;
    }

    public void setFileNames(String[] fileNames) {
        this.fileNames = fileNames;
    }

    public void setFilterConfig(FilterConfig filterConfig) {
        this.filterConfig = filterConfig;
    }

    public Set<String> getNamespaceFilters() {
        return new HashSet<>(namespaceFilters);
    }

    public void setNamespaceFilters(Set<String> namespaceFilters) {
        this.namespaceFilters = new HashSet<>(namespaceFilters);
    }

    public void addNamespaceFilter(String namespaceFilter) {
        this.namespaceFilters.add(namespaceFilter);
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new LogParser()).execute(args);
        System.exit(exitCode);
    }
}