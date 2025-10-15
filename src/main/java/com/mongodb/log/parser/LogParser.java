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
import com.mongodb.log.parser.accumulator.AppNameConnectionStatsAccumulator;
import com.mongodb.log.parser.accumulator.TwoPassDriverStatsAccumulator;
import com.mongodb.log.parser.accumulator.ErrorCodeAccumulator;
import com.mongodb.log.parser.accumulator.IndexStatsAccumulator;
import com.mongodb.log.parser.accumulator.PlanCacheAccumulator;
import com.mongodb.log.parser.accumulator.QueryHashAccumulator;
import com.mongodb.log.parser.accumulator.SlowPlanningAccumulator;
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
    
    @Option(names = { "--verbose" }, description = "Enable verbose output with timing and memory information")
    private boolean verbose = false;

    @Option(names = {"--ignoredAnalysis"}, description = "Output file for ignored lines analysis")
    private String ignoredAnalysisFile;

    @Option(names = {"--planCacheCsv"}, description = "CSV output file for plan cache analysis")
    private String planCacheCsvFile;

    @Option(names = {"--queryHashCsv"}, description = "CSV output file for query hash analysis")
    private String queryHashCsvFile;
    
    @Option(names = {"--errorCodesCsv"}, description = "CSV output file for error code analysis")
    private String errorCodesCsvFile;
    
    @Option(names = {"--transactionCsv"}, description = "CSV output file for transaction analysis")
    private String transactionCsvFile;

    @Option(names = {"--ns", "--namespace"}, description = "Filter to specific namespace(s). Can be specified multiple times. Supports patterns like 'mydb.*' or exact matches like 'mydb.mycoll'")
    private Set<String> namespaceFilters = new HashSet<>();
    
    @Option(names = {"--html"}, description = "HTML output file for interactive report (default: report.html)")
    private String htmlOutputFile = "report.html";
    
    @Option(names = {"--text"}, description = "Enable text output to console")
    private boolean textOutput = false;
    
    @Option(names = {"--redact"}, description = "Enable query redaction/sanitization (default: false)")
    private boolean redactQueries = false;
    
    @Option(names = {"--json"}, description = "JSON output file for structured report data")
    private String jsonOutputFile;
    
    @Option(names = {"--json-only"}, description = "Generate only JSON output (skip HTML)")
    private boolean jsonOnly = false;
    
    @Option(names = {"--shards"}, description = "Enable shard/node tracking based on filename pattern (e.g., shard-XX-YY)")
    private boolean enableShardTracking = false;
    
    @Option(names = {"--drivers"}, description = "Enable driver statistics analysis (disabled by default)")
    private boolean enableDriverStats = false;

    @Option(names = {"--appNameStats"}, description = "Enable appName connection statistics (tracks distinct connections per appName)")
    private boolean enableAppNameStats = false;

    @Option(names = {"--limit"}, description = "Limit parsing to the first N lines of each log file")
    private Long lineLimit = null;

    // Statistics tracking
    private AtomicLong totalParseErrors = new AtomicLong(0);
    private AtomicLong totalNoAttr = new AtomicLong(0);
    private AtomicLong totalNoCommand = new AtomicLong(0);
    private AtomicLong totalNoNs = new AtomicLong(0);
    private AtomicLong totalFoundOps = new AtomicLong(0);
    private AtomicLong totalFilteredByNamespace = new AtomicLong(0);
    private Map<String, AtomicLong> operationTypeStats = new HashMap<>();
    
    // Timestamp tracking
    private volatile String earliestTimestamp = null;
    private volatile String latestTimestamp = null;

    private String currentLine = null;
    private final Accumulator accumulator;
    private final Accumulator ttlAccumulator;
    private final PlanCacheAccumulator planCacheAccumulator;
    private final ErrorCodeAccumulator errorCodeAccumulator;
    private final TransactionAccumulator transactionAccumulator;
    private final IndexStatsAccumulator indexStatsAccumulator;
    private final TwoPassDriverStatsAccumulator driverStatsAccumulator;
    private final AppNameConnectionStatsAccumulator appNameConnectionStatsAccumulator;
    
    private FilterConfig filterConfig;
    private int unmatchedCount = 0;
    private int ignoredCount = 0;
    private int processedCount = 0;

    // Query shape tracking (if enabled)
    private Map<Namespace, Map<Set<String>, AtomicInteger>> shapesByNamespace = new HashMap<>();
    private Accumulator shapeAccumulator = new Accumulator();
    
    private QueryHashAccumulator queryHashAccumulator;
    private SlowPlanningAccumulator slowPlanningAccumulator;

    // Shard tracking
    private ShardInfo currentShardInfo = null;
    private Map<ShardInfo, Accumulator> shardAccumulators = new HashMap<>();
    private Map<ShardInfo, Accumulator> shardTtlAccumulators = new HashMap<>();
    private Map<ShardInfo, PlanCacheAccumulator> shardPlanCacheAccumulators = new HashMap<>();
    private Map<ShardInfo, QueryHashAccumulator> shardQueryHashAccumulators = new HashMap<>();
    private Map<ShardInfo, ErrorCodeAccumulator> shardErrorCodeAccumulators = new HashMap<>();
    private Map<ShardInfo, TransactionAccumulator> shardTransactionAccumulators = new HashMap<>();
    private Map<ShardInfo, IndexStatsAccumulator> shardIndexStatsAccumulators = new HashMap<>();

    // Ignored lines analysis
    private Map<String, AtomicLong> ignoredCategories = new HashMap<>();
    private PrintWriter ignoredWriter = null;

    public LogParser() {
        accumulator = new Accumulator();
        ttlAccumulator = new Accumulator();
        queryHashAccumulator = new QueryHashAccumulator();
        slowPlanningAccumulator = new SlowPlanningAccumulator();
        planCacheAccumulator = new PlanCacheAccumulator();
        errorCodeAccumulator = new ErrorCodeAccumulator();
        transactionAccumulator = new TransactionAccumulator();
        indexStatsAccumulator = new IndexStatsAccumulator();
        driverStatsAccumulator = new TwoPassDriverStatsAccumulator();
        appNameConnectionStatsAccumulator = new AppNameConnectionStatsAccumulator();
        filterConfig = new FilterConfig();
    }

    @Override
    public Integer call() throws Exception {
        // Improved console output
        System.out.println("🚀 MongoDB Log Analyzer");
        System.out.println("📁 Processing " + fileNames.length + " file(s)...");
        
        // Show redaction status early in processing
        if (redactQueries) {
            System.out.println("🔒 Query redaction/sanitization: ENABLED");
        } else {
            System.out.println("⚠️  Query redaction/sanitization: DISABLED (use --redact to enable)");
        }
        
        if (!namespaceFilters.isEmpty()) {
            System.out.println("🔍 Namespace filters: " + String.join(", ", namespaceFilters));
        }

        long overallStart = System.currentTimeMillis();
        
        loadConfiguration();
        if (verbose) {
            System.err.println("[VERBOSE] Configuration loaded");
            Runtime runtime = Runtime.getRuntime();
            System.err.printf("[VERBOSE] JVM Memory: %d MB max, %d MB total, %d processors%n",
                            runtime.maxMemory() / 1024 / 1024,
                            runtime.totalMemory() / 1024 / 1024,
                            runtime.availableProcessors());
        }
        
        int successfulFiles = read();
        
        if (verbose) {
            long overallTime = System.currentTimeMillis() - overallStart;
            System.err.printf("[VERBOSE] Overall processing completed in %d ms%n", overallTime);
        }
        
        if (successfulFiles == 0) {
            System.err.println("❌ No files were successfully processed. Exiting without generating reports.");
            return 1;
        }

        System.out.println("✅ Analysis complete!");
        
        if (textOutput) {
            reportOperationStats();
            reportIgnoredAnalysis();
        }

        if (csvOutputFile != null) {
            System.out.println("📊 Generating CSV report: " + csvOutputFile);
            reportCsv();
        } else if (textOutput) {
            report();
        }

        if (textOutput) {
            reportTtlOperations();
        }
        
        // Generate plan cache reports if enabled
        if (planCacheAccumulator != null && textOutput) {
            planCacheAccumulator.report();
            planCacheAccumulator.reportByQueryHash();
        }
        
        if (queryHashAccumulator != null && textOutput) {
            queryHashAccumulator.report();
        }
        
        if (errorCodeAccumulator.hasErrors() && textOutput) {
            errorCodeAccumulator.report();
        }
        
        if (transactionAccumulator.hasTransactions() && textOutput) {
            transactionAccumulator.report();
        }

        if (enableAppNameStats) {
            System.out.println("\n=== AppName Connection Statistics ===");
            System.out.println("Feature enabled: " + enableAppNameStats);
            System.out.println("Total distinct connections: " + appNameConnectionStatsAccumulator.getTotalConnectionCount());
            System.out.println("Unique appNames: " + appNameConnectionStatsAccumulator.getAppNameCount());
            if (appNameConnectionStatsAccumulator.getAppNameCount() > 0 && textOutput) {
                System.out.println("\nTop appNames by connection count:");
                appNameConnectionStatsAccumulator.getAggregatedStats().entrySet().stream()
                    .sorted((a, b) -> Integer.compare(b.getValue(), a.getValue()))
                    .limit(10)
                    .forEach(entry -> System.out.println("  " + entry.getKey() + ": " + entry.getValue()));
            }
        }

        if (enableDriverStats) {
            System.out.println("\n=== Driver Statistics ===");
            System.out.println("Feature enabled: " + enableDriverStats);
            if (driverStatsAccumulator != null) {
                System.out.println("Driver stats entries: " + (driverStatsAccumulator.hasDriverStats() ? driverStatsAccumulator.getDriverStatsEntries().size() : 0));
                System.out.println("Total connections: " + driverStatsAccumulator.getTotalConnections());
            } else {
                System.out.println("Driver stats accumulator is NULL");
            }
        }

        // Generate CSV reports if requested
        if (planCacheCsvFile != null) {
            System.out.println("📊 Generating plan cache CSV: " + planCacheCsvFile);
            planCacheAccumulator.reportCsv(planCacheCsvFile);
        }
        
        if (queryHashCsvFile != null) {
            System.out.println("📊 Generating query hash CSV: " + queryHashCsvFile);
            queryHashAccumulator.reportCsv(queryHashCsvFile);
        }
        
        if (errorCodesCsvFile != null) {
            System.out.println("📊 Generating error codes CSV: " + errorCodesCsvFile);
            errorCodeAccumulator.reportCsv(errorCodesCsvFile);
        }
        
        if (transactionCsvFile != null) {
            System.out.println("📊 Generating transaction CSV: " + transactionCsvFile);
            transactionAccumulator.reportCsv(transactionCsvFile);
        }
        
        // Generate HTML report (unless JSON-only mode)
        if (!jsonOnly) {
            try {
                System.out.println("📝 Generating HTML report: " + htmlOutputFile);
                long htmlStart = System.currentTimeMillis();
                if (verbose) {
                    System.err.println("[VERBOSE] Starting HTML report generation");
                }
                HtmlReportGenerator.generateReport(
                    htmlOutputFile,
                    accumulator,
                    ttlAccumulator,
                    planCacheAccumulator,
                    queryHashAccumulator,
                    slowPlanningAccumulator,
                    errorCodeAccumulator,
                    transactionAccumulator,
                    indexStatsAccumulator,
                    enableDriverStats ? driverStatsAccumulator : null,
                    enableAppNameStats ? appNameConnectionStatsAccumulator : null,
                    operationTypeStats,
                    redactQueries,
                    earliestTimestamp,
                    latestTimestamp,
                    enableShardTracking,
                    shardAccumulators,
                    shardTtlAccumulators,
                    shardPlanCacheAccumulators,
                    shardQueryHashAccumulators,
                    shardErrorCodeAccumulators,
                    shardTransactionAccumulators,
                    shardIndexStatsAccumulators
                );
                long htmlEnd = System.currentTimeMillis();
                if (verbose) {
                    System.err.printf("[VERBOSE] HTML report generation completed in %d ms%n", htmlEnd - htmlStart);
                }
                System.out.println("🎉 HTML report completed: " + htmlOutputFile);
            } catch (IOException e) {
                System.err.println("❌ Failed to generate HTML report: " + e.getMessage());
            }
        }
        
        // Generate JSON report if requested
        if (jsonOutputFile != null) {
            try {
                System.out.println("📊 Generating JSON report: " + jsonOutputFile);
                JsonReportGenerator.generateReport(
                    jsonOutputFile,
                    accumulator,
                    ttlAccumulator,
                    planCacheAccumulator,
                    queryHashAccumulator,
                    errorCodeAccumulator,
                    transactionAccumulator,
                    indexStatsAccumulator,
                    driverStatsAccumulator,
                    operationTypeStats,
                    redactQueries,
                    earliestTimestamp,
                    latestTimestamp
                );
                System.out.println("🎉 JSON report completed: " + jsonOutputFile);
            } catch (IOException e) {
                System.err.println("❌ Failed to generate JSON report: " + e.getMessage());
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
        if (namespace == null) {
            return false;
        }
        
        String fullNamespace = namespace.toString();
        String dbName = namespace.getDatabaseName();
        
        // Always exclude the config database
        if ("config".equals(dbName)) {
            return false;
        }
        
        // If no filters configured, accept all (except config which was already excluded)
        if (namespaceFilters.isEmpty()) {
            return true;
        }
        
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

    public int read() throws IOException, ExecutionException, InterruptedException {
        int fileCount = 0;
        int successfulFiles = 0;
        
        for (String fileName : fileNames) {
            File f = new File(fileName);
            fileCount++;
            
            if (!f.exists()) {
                System.err.println("❌ File not found: " + fileName);
                continue;
            }
            
            if (!f.canRead()) {
                System.err.println("❌ Cannot read file: " + fileName);
                continue;
            }
            
            try {
                String fileSize = formatFileSize(f.length());
                String shortName = f.getName();
                if (shortName.length() > 80) {
                    shortName = "..." + shortName.substring(shortName.length() - 77);
                }
                System.out.printf("📄 [%d/%d] %s (%s)\n", fileCount, fileNames.length, shortName, fileSize);
                read(f);
                successfulFiles++;
            } catch (Exception e) {
                System.err.println("❌ Failed to process " + fileName + ": " + e.getMessage());
                // Continue with next file
            }
        }
        
        return successfulFiles;
    }
    
    private String formatFileSize(long bytes) {
        if (bytes < 1024) return bytes + " B";
        int unit = (int) (Math.log(bytes) / Math.log(1024));
        String pre = "KMGTPE".charAt(unit - 1) + "";
        return String.format("%.1f %sB", bytes / Math.pow(1024, unit), pre);
    }

    public void read(File file) throws IOException, ExecutionException, InterruptedException {
        // Extract shard info from filename if shard tracking is enabled
        if (enableShardTracking) {
            currentShardInfo = ShardInfo.extractFromFilename(file.getName());
            if (currentShardInfo != null) {
                System.out.println("📍 Processing shard: " + currentShardInfo);
                // Initialize accumulators for this shard if not already present
                initializeShardAccumulators(currentShardInfo);
                if (verbose) {
                    System.err.printf("[VERBOSE] Initialized shard accumulators for %s. Total shard accumulators: %d%n", 
                                    currentShardInfo, shardAccumulators.size());
                }
            } else {
                System.out.println("⚠️  Could not extract shard info from filename: " + file.getName());
            }
        }
        
        if (enableDriverStats) {
            if (driverStatsAccumulator instanceof TwoPassDriverStatsAccumulator) {
                readTwoPass(file);
            } else {
                readSinglePass(file);
            }
        } else {
            // Skip two-pass processing when driver stats are disabled
            readSinglePass(file);
        }
    }
    
    private void initializeShardAccumulators(ShardInfo shardInfo) {
        shardAccumulators.computeIfAbsent(shardInfo, k -> new Accumulator());
        shardTtlAccumulators.computeIfAbsent(shardInfo, k -> new Accumulator());
        shardPlanCacheAccumulators.computeIfAbsent(shardInfo, k -> new PlanCacheAccumulator());
        shardQueryHashAccumulators.computeIfAbsent(shardInfo, k -> new QueryHashAccumulator());
        shardErrorCodeAccumulators.computeIfAbsent(shardInfo, k -> new ErrorCodeAccumulator());
        shardTransactionAccumulators.computeIfAbsent(shardInfo, k -> new TransactionAccumulator());
        shardIndexStatsAccumulators.computeIfAbsent(shardInfo, k -> new IndexStatsAccumulator());
    }
    
    private void readTwoPass(File file) throws IOException, ExecutionException, InterruptedException {
        System.out.println("🔄 Starting two-pass driver statistics analysis...");
        
        // Pass 1: Build authentication mapping
        System.out.println("📖 Pass 1: Building authentication mapping...");
        long pass1Start = System.currentTimeMillis();
        readPass1(file);
        driverStatsAccumulator.finishPass1();
        long pass1Time = System.currentTimeMillis() - pass1Start;
        System.out.printf("✅ Pass 1 completed in %.1f seconds\n", pass1Time / 1000.0);
        
        // Pass 2: Process metadata with authentication context
        System.out.println("📖 Pass 2: Processing metadata with authentication context...");
        long pass2Start = System.currentTimeMillis();
        readPass2(file);
        driverStatsAccumulator.finishPass2();
        long pass2Time = System.currentTimeMillis() - pass2Start;
        System.out.printf("✅ Pass 2 completed in %.1f seconds\n", pass2Time / 1000.0);
        
        System.out.printf("🎉 Two-pass analysis completed in %.1f seconds total\n", (pass1Time + pass2Time) / 1000.0);
        
        // Now process the slow queries and other operations
        System.out.println("📖 Processing slow queries and operations...");
        long slowQueryStart = System.currentTimeMillis();
        readForSlowQueries(file);
        long slowQueryTime = System.currentTimeMillis() - slowQueryStart;
        System.out.printf("✅ Slow query processing completed in %.1f seconds\n", slowQueryTime / 1000.0);
    }

    private void readSinglePass(File file) throws IOException, ExecutionException, InterruptedException {
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
        long lastVerboseTime = start;
        List<String> lines = new ArrayList<>();
        
        Runtime runtime = Runtime.getRuntime();
        if (verbose) {
            long totalMemory = runtime.totalMemory() / 1024 / 1024;
            long freeMemory = runtime.freeMemory() / 1024 / 1024;
            long usedMemory = totalMemory - freeMemory;
            System.err.printf("[VERBOSE] Starting processing. Memory: %d MB used, %d MB free, %d MB total%n", 
                            usedMemory, freeMemory, totalMemory);
        }

        while ((currentLine = readLineSafe(in, 1 * 1024 * 1024)) != null) {
            lineNum++;

            // Check line limit
            if (lineLimit != null && lineNum > lineLimit) {
                System.out.printf("📊 Reached line limit of %d lines, stopping parsing\n", lineLimit);
                break;
            }

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
                // Use shard-specific accumulators if shard tracking is enabled
                Accumulator targetAccumulator = (enableShardTracking && currentShardInfo != null) 
                    ? shardAccumulators.get(currentShardInfo) : accumulator;
                PlanCacheAccumulator targetPlanCache = (enableShardTracking && currentShardInfo != null)
                    ? shardPlanCacheAccumulators.get(currentShardInfo) : planCacheAccumulator;
                QueryHashAccumulator targetQueryHash = (enableShardTracking && currentShardInfo != null)
                    ? shardQueryHashAccumulators.get(currentShardInfo) : queryHashAccumulator;
                ErrorCodeAccumulator targetErrorCode = (enableShardTracking && currentShardInfo != null)
                    ? shardErrorCodeAccumulators.get(currentShardInfo) : errorCodeAccumulator;
                TransactionAccumulator targetTransaction = (enableShardTracking && currentShardInfo != null)
                    ? shardTransactionAccumulators.get(currentShardInfo) : transactionAccumulator;
                IndexStatsAccumulator targetIndexStats = (enableShardTracking && currentShardInfo != null)
                    ? shardIndexStatsAccumulators.get(currentShardInfo) : indexStatsAccumulator;
                    
                completionService.submit(
                    new LogParserTask(new ArrayList<>(lines), targetAccumulator, targetPlanCache, targetQueryHash,
                    		slowPlanningAccumulator, targetErrorCode, targetTransaction, targetIndexStats,
                    		enableDriverStats ? driverStatsAccumulator : null,
                    		enableAppNameStats ? appNameConnectionStatsAccumulator : null,
                    		file.getName(),
                    		operationTypeStats, debug, namespaceFilters, totalFilteredByNamespace, redactQueries, this));
                submittedTasks++;
                lines.clear();
                
                // Periodic cleanup to prevent memory leaks
                if (enableDriverStats && driverStatsAccumulator != null && submittedTasks % 10 == 0) {
                    int authCount = driverStatsAccumulator.getPendingAuthCount();
                    int metadataCount = driverStatsAccumulator.getPendingMetadataCount();
                    driverStatsAccumulator.performPeriodicCleanup();
                    if (verbose && (authCount > 1000 || metadataCount > 1000)) {
                        System.err.printf("[VERBOSE] Cleanup performed. Auth: %d -> %d, Metadata: %d -> %d%n",
                                        authCount, driverStatsAccumulator.getPendingAuthCount(),
                                        metadataCount, driverStatsAccumulator.getPendingMetadataCount());
                    }
                }
                
                // Verbose logging every 5 seconds
                if (verbose && (System.currentTimeMillis() - lastVerboseTime) > 5000) {
                    long currentTime = System.currentTimeMillis();
                    long totalMemory = runtime.totalMemory() / 1024 / 1024;
                    long freeMemory = runtime.freeMemory() / 1024 / 1024;
                    long usedMemory = totalMemory - freeMemory;
                    double linesPerSec = lineNum / ((currentTime - start) / 1000.0);
                    
                    System.err.printf("[VERBOSE] Processed %d lines (%d tasks), %.1f lines/sec. Memory: %d MB used, %d MB free%n", 
                                    lineNum, submittedTasks, linesPerSec, usedMemory, freeMemory);
                    
                    if (enableDriverStats && driverStatsAccumulator != null) {
                        // Add method to get stats
                        System.err.printf("[VERBOSE] Driver stats: %d entries, pending auth: %d, pending metadata: %d%n",
                                        driverStatsAccumulator.getDriverStatsEntries().size(),
                                        driverStatsAccumulator.getPendingAuthCount(),
                                        driverStatsAccumulator.getPendingMetadataCount());
                    }
                    
                    // Log accumulator sizes to identify memory usage
                    long estimatedMemory = 0;
                    int mainSize = accumulator.getAccumulatorSize();
                    int queryHashSize = queryHashAccumulator != null ? queryHashAccumulator.getSize() : 0;
                    int planCacheSize = planCacheAccumulator != null ? planCacheAccumulator.getSize() : 0;
                    int indexSize = indexStatsAccumulator != null ? indexStatsAccumulator.getSize() : 0;
                    
                    // Estimate memory usage (rough calculation)
                    estimatedMemory = (mainSize + queryHashSize + planCacheSize) * 2000; // ~2KB per sample log message
                    
                    System.err.printf("[VERBOSE] Accumulator sizes - Main: %d, QueryHash: %d, PlanCache: %d, Index: %d (Est Memory: %d KB)%n",
                                    mainSize, queryHashSize, planCacheSize, indexSize, estimatedMemory / 1024);
                    
                    lastVerboseTime = currentTime;
                }
            }
        }

        // Submit remaining lines
        if (!lines.isEmpty()) {
            // Use shard-specific accumulators if shard tracking is enabled
            Accumulator targetAccumulator = (enableShardTracking && currentShardInfo != null) 
                ? shardAccumulators.get(currentShardInfo) : accumulator;
            PlanCacheAccumulator targetPlanCache = (enableShardTracking && currentShardInfo != null)
                ? shardPlanCacheAccumulators.get(currentShardInfo) : planCacheAccumulator;
            QueryHashAccumulator targetQueryHash = (enableShardTracking && currentShardInfo != null)
                ? shardQueryHashAccumulators.get(currentShardInfo) : queryHashAccumulator;
            ErrorCodeAccumulator targetErrorCode = (enableShardTracking && currentShardInfo != null)
                ? shardErrorCodeAccumulators.get(currentShardInfo) : errorCodeAccumulator;
            TransactionAccumulator targetTransaction = (enableShardTracking && currentShardInfo != null)
                ? shardTransactionAccumulators.get(currentShardInfo) : transactionAccumulator;
            IndexStatsAccumulator targetIndexStats = (enableShardTracking && currentShardInfo != null)
                ? shardIndexStatsAccumulators.get(currentShardInfo) : indexStatsAccumulator;
                
            completionService.submit(
                new LogParserTask(new ArrayList<>(lines), targetAccumulator, targetPlanCache, targetQueryHash,
                		slowPlanningAccumulator, targetErrorCode, targetTransaction, targetIndexStats,
                		enableDriverStats ? driverStatsAccumulator : null,
                		enableAppNameStats ? appNameConnectionStatsAccumulator : null,
                		file.getName(),
                		operationTypeStats, debug, namespaceFilters, totalFilteredByNamespace, redactQueries, this));
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

        // Perform post-processing join for driver statistics
        if (enableDriverStats && driverStatsAccumulator != null) {
            if (verbose) {
                System.err.printf("[VERBOSE] Starting post-processing join. Pending auth: %d, pending metadata: %d%n",
                                driverStatsAccumulator.getPendingAuthCount(),
                                driverStatsAccumulator.getPendingMetadataCount());
            }
            long joinStart = System.currentTimeMillis();
            logger.info("Performing post-processing join for driver statistics");
            driverStatsAccumulator.performPostProcessingJoin();
            if (verbose) {
                long joinTime = System.currentTimeMillis() - joinStart;
                System.err.printf("[VERBOSE] Post-processing join completed in %d ms%n", joinTime);
            }
        }

        // Debug shard accumulator contents
        if (enableShardTracking && verbose) {
            System.err.println("[VERBOSE] Shard accumulator summary:");
            for (Map.Entry<ShardInfo, Accumulator> entry : shardAccumulators.entrySet()) {
                int size = entry.getValue().getAccumulators().size();
                System.err.printf("[VERBOSE]   %s: %d operations%n", entry.getKey(), size);
            }
        }

        cleanup(executor, in);
        logProcessingResults(file, System.currentTimeMillis() - start, lineNum);
    }

    private BufferedReader createReader(File file, String mimeType) throws IOException {
        // Use 16MB buffer size to handle large lines efficiently
        int bufferSize = 16 * 1024 * 1024;

        if (mimeType != null && mimeType.equals(MimeTypes.GZIP)) {
            return new BufferedReader(
                new InputStreamReader(new GZIPInputStream(new FileInputStream(file))),
                bufferSize);
        } else if (mimeType != null && mimeType.equals(MimeTypes.ZIP)) {
            return new BufferedReader(
                new InputStreamReader(new ZipInputStream(new FileInputStream(file))),
                bufferSize);
        } else {
            return new BufferedReader(new FileReader(file), bufferSize);
        }
    }

    /**
     * Read a line with maximum length protection to prevent OOM.
     * If line exceeds maxLength, the rest of the line is skipped and null is returned.
     * Returns null when end of stream is reached or line is too long.
     */
    private String readLineSafe(BufferedReader reader, int maxLength) throws IOException {
        char[] buffer = new char[Math.min(maxLength, 8192)];
        StringBuilder sb = new StringBuilder(8192);
        int totalRead = 0;

        while (totalRead < maxLength) {
            int toRead = Math.min(buffer.length, maxLength - totalRead);
            reader.mark(toRead + 1);
            int n = reader.read(buffer, 0, toRead);

            if (n == -1) {
                // EOF
                if (totalRead == 0) {
                    return null;
                }
                return sb.toString();
            }

            // Look for newline in what we just read
            int newlinePos = -1;
            for (int i = 0; i < n; i++) {
                if (buffer[i] == '\n' || buffer[i] == '\r') {
                    newlinePos = i;
                    break;
                }
            }

            if (newlinePos >= 0) {
                // Found newline - add chars up to newline
                sb.append(buffer, 0, newlinePos);

                // Position reader after the newline
                reader.reset();
                reader.skip(newlinePos + 1);
                // Handle \r\n
                if (buffer[newlinePos] == '\r') {
                    reader.mark(1);
                    int next = reader.read();
                    if (next != '\n' && next != -1) {
                        reader.reset();
                    }
                }
                return sb.toString();
            }

            // No newline found, add all chars
            sb.append(buffer, 0, n);
            totalRead += n;
        }

        // Reached maxLength without finding newline - log prefix and skip rest of line
        String prefix = sb.toString();
        String displayPrefix = prefix.length() > 64 ? prefix.substring(0, 64) : prefix;
        System.err.printf("WARNING: Line exceeds %d chars (%.1f MB), skipping. First 64 chars: %s...%n",
            maxLength, maxLength / (1024.0 * 1024.0), displayPrefix);

        // Skip until we find newline or EOF
        int ch;
        long skippedCount = 0;
        while ((ch = reader.read()) != -1) {
            skippedCount++;
            if (ch == '\n' || ch == '\r') {
                if (ch == '\r') {
                    reader.mark(1);
                    int next = reader.read();
                    if (next != '\n' && next != -1) {
                        reader.reset();
                    }
                }
                break;
            }
        }

        if (skippedCount > 0) {
            System.err.printf("         Skipped %d additional chars to reach end of line%n", skippedCount);
        }

        // Return null to skip this line entirely
        return null;
    }

    private boolean shouldIgnoreLine(String line) {
        // Ignore if it's not JSON-like
        if (!line.trim().startsWith("{")) {
            return true;
        }

        // Never ignore lines that contain target operations
        if (containsTargetOperation(line)) {
            return false;
        }

        // Never ignore client metadata messages (for driver statistics)
        if (line.contains("\"msg\":\"client metadata\"")) {
            return false;
        }
        
        // Never ignore successful authentication messages (for driver statistics username tracking)
        if (line.contains("\"c\":\"ACCESS\"") && line.contains("\"msg\":\"Successfully authenticated\"")) {
            return false;
        }
        
        // Never ignore connection lifecycle messages (for connection lifetime tracking)
        if (line.contains("\"c\":\"NETWORK\"") && 
            (line.contains("\"msg\":\"Connection accepted\"") || line.contains("\"msg\":\"Connection ended\""))) {
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
                    
                    // Use shard-specific accumulator if shard tracking is enabled
                    Accumulator targetTtlAccumulator = (enableShardTracking && currentShardInfo != null)
                        ? shardTtlAccumulators.get(currentShardInfo) : ttlAccumulator;
                    
                    synchronized (targetTtlAccumulator) {
                        targetTtlAccumulator.accumulate(null, "ttl_delete", ns, durationMs, 
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
        if (debug || textOutput) {
            logger.info("File processing complete - Duration: {}ms | Lines: {} read, {} ignored, {} processed, {} filtered by namespace | Errors: {} parse, {} no attr, {} no command, {} no namespace | Operations parsed: {}", 
                duration, totalLines, ignoredCount, processedCount, totalFilteredByNamespace.get(),
                totalParseErrors.get(), totalNoAttr.get(), totalNoCommand.get(), totalNoNs.get(), totalFoundOps.get());
        }

        if (totalFoundOps.get() == 0) {
            System.err.println("⚠️  WARNING: No operations were successfully parsed!");
            System.err.println("   This might indicate wrong log format, excessive filtering, or namespace filter mismatch");
            if (!namespaceFilters.isEmpty()) {
                System.err.println("   Namespace filters applied: " + namespaceFilters);
                System.err.println("   Consider checking if your namespace filters match the actual namespaces in the logs");
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
    
    public synchronized void updateTimestamps(String timestamp) {
        if (timestamp != null) {
            if (earliestTimestamp == null || timestamp.compareTo(earliestTimestamp) < 0) {
                earliestTimestamp = timestamp;
            }
            if (latestTimestamp == null || timestamp.compareTo(latestTimestamp) > 0) {
                latestTimestamp = timestamp;
            }
        }
    }

    private void readPass1(File file) throws IOException, ExecutionException, InterruptedException {
        // Pass 1: Parallel processing to build authentication mapping and connection start times
        String guess = MimeTypes.guessContentTypeFromName(file.getName());
        BufferedReader in = createReader(file, guess);
        
        int numWorkers = Runtime.getRuntime().availableProcessors() * 2; // Use 2x cores for I/O bound work
        System.out.printf("Pass 1: Using %d parallel workers\n", numWorkers);
        ExecutorService executor = Executors.newFixedThreadPool(numWorkers);
        CompletionService<Void> completionService = new ExecutorCompletionService<>(executor);
        
        String currentLine;
        int lineNum = 0;
        List<String> lines = new ArrayList<>();
        int submittedTasks = 0;
        long startTime = System.currentTimeMillis();
        
        while ((currentLine = readLineSafe(in, 1 * 1024 * 1024)) != null) {
            lineNum++;

            // Check line limit
            if (lineLimit != null && lineNum > lineLimit) {
                System.out.printf("📊 Pass 1: Reached line limit of %d lines, stopping parsing\n", lineLimit);
                break;
            }

            if (shouldIgnoreLine(currentLine)) {
                continue;
            }

            lines.add(currentLine);
            
            // Submit chunks for parallel processing (let tasks do the filtering)
            if (lines.size() >= 25000) {
                completionService.submit(new Pass1Task(new ArrayList<>(lines), driverStatsAccumulator));
                submittedTasks++;
                lines.clear();
                
                if (submittedTasks % 100 == 0) {
                    long elapsed = System.currentTimeMillis() - startTime;
                    long totalLines = submittedTasks * 25000L;
                    double linesPerSec = totalLines / (elapsed / 1000.0);
                    System.out.printf("Pass 1: Submitted %d tasks, ~%d lines, %.1fs elapsed, %.0f lines/sec\n", 
                                    submittedTasks, totalLines, elapsed / 1000.0, linesPerSec);
                }
            }
        }
        
        // Submit remaining lines
        if (!lines.isEmpty()) {
            completionService.submit(new Pass1Task(new ArrayList<>(lines), driverStatsAccumulator));
            submittedTasks++;
        }
        
        // Wait for all tasks to complete with progress tracking
        System.out.printf("Pass 1: All %d tasks submitted, waiting for completion...\n", submittedTasks);
        long completionStart = System.currentTimeMillis();
        int completedTasks = 0;
        
        for (int i = 0; i < submittedTasks; i++) {
            try {
                completionService.take().get();
                completedTasks++;
                
                if (completedTasks % 100 == 0 || completedTasks == submittedTasks) {
                    long elapsed = System.currentTimeMillis() - completionStart;
                    double completionRate = completedTasks / (elapsed / 1000.0);
                    System.out.printf("Pass 1: Completed %d/%d tasks, %.1fs elapsed, %.1f tasks/sec\n", 
                                    completedTasks, submittedTasks, elapsed / 1000.0, completionRate);
                }
            } catch (ExecutionException e) {
                logger.error("Pass 1 task execution failed", e);
                completedTasks++;
            }
        }
        
        executor.shutdown();
        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        in.close();
        System.out.printf("Pass 1 complete: Read %d total lines, submitted %d tasks\n", lineNum, submittedTasks);
    }
    
    private long extractTimestamp(JSONObject jo) {
        try {
            JSONObject t = jo.optJSONObject("t");
            if (t != null) {
                String dateStr = t.optString("$date");
                if (dateStr != null && !dateStr.isEmpty()) {
                    // Parse ISO 8601 timestamp
                    return java.time.Instant.parse(dateStr).toEpochMilli();
                }
            }
        } catch (Exception e) {
            // Return invalid timestamp if parsing fails
        }
        return -1;
    }
    
    // Fast extraction methods that avoid full JSON parsing
    private String extractCtxFast(String line) {
        // Pattern: "ctx":"conn12345"
        int ctxIndex = line.indexOf("\"ctx\":\"");
        if (ctxIndex < 0) return null;
        
        int start = ctxIndex + 7; // Length of "ctx":"
        int end = line.indexOf('"', start);
        if (end < 0) return null;
        
        String ctx = line.substring(start, end);
        return ctx.startsWith("conn") ? ctx : null;
    }
    
    private long extractTimestampFast(String line) {
        // Pattern: {"t":{"$date":"2025-08-20T10:04:01.084+00:00"}
        int dateIndex = line.indexOf("\"$date\":\"");
        if (dateIndex < 0) {
            return -1; // Invalid timestamp
        }
        
        int start = dateIndex + 9; // Length of "\"$date\":\""
        int end = line.indexOf('"', start);
        if (end < 0) {
            return -1; // Invalid timestamp
        }
        
        try {
            String dateStr = line.substring(start, end);
            return java.time.Instant.parse(dateStr).toEpochMilli();
        } catch (Exception e) {
            return -1; // Invalid timestamp
        }
    }
    
    private Long extractConnId(String ctx) {
        if (ctx != null && ctx.startsWith("conn")) {
            try {
                return Long.parseLong(ctx.substring(4));
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }
    
    private void readForSlowQueries(File file) throws IOException, ExecutionException, InterruptedException {
        String guess = MimeTypes.guessContentTypeFromName(file.getName());
        BufferedReader in = createReader(file, guess);
        
        int numThreads = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CompletionService<ProcessingStats> completionService = new ExecutorCompletionService<>(executor);

        int lineNum = 0;
        int submittedTasks = 0;
        List<String> lines = new ArrayList<>();
        
        while ((currentLine = readLineSafe(in, 1 * 1024 * 1024)) != null) {
            lineNum++;

            // Check line limit
            if (lineLimit != null && lineNum > lineLimit) {
                System.out.printf("📊 Slow query pass: Reached line limit of %d lines, stopping parsing\n", lineLimit);
                break;
            }

            // Process TTL operations before filtering
            if (isTtlOperation(currentLine)) {
                processTtlOperation(currentLine);
            }

            if (shouldIgnoreLine(currentLine)) {
                ignoredCount++;
                continue;
            }

            lines.add(currentLine);
            processedCount++;

            if (lines.size() >= 25000) {
                completionService.submit(
                    new LogParserTask(new ArrayList<>(lines), accumulator, planCacheAccumulator, queryHashAccumulator,
                    		slowPlanningAccumulator, errorCodeAccumulator, transactionAccumulator, indexStatsAccumulator,
                    		enableDriverStats ? driverStatsAccumulator : null, enableAppNameStats ? appNameConnectionStatsAccumulator : null, file.getName(),
                    		operationTypeStats, debug, namespaceFilters, totalFilteredByNamespace, redactQueries, this));
                submittedTasks++;
                lines.clear();
            }
        }

        // Submit remaining lines
        if (!lines.isEmpty()) {
            completionService.submit(
                new LogParserTask(new ArrayList<>(lines), accumulator, planCacheAccumulator, queryHashAccumulator,
                		slowPlanningAccumulator, errorCodeAccumulator, transactionAccumulator, indexStatsAccumulator,
                		null, enableAppNameStats ? appNameConnectionStatsAccumulator : null, file.getName(),
                		operationTypeStats, debug, namespaceFilters, totalFilteredByNamespace, redactQueries, this));
            submittedTasks++;
        }

        // Wait for all tasks to complete
        for (int i = 0; i < submittedTasks; i++) {
            try {
                completionService.take().get();
            } catch (InterruptedException | ExecutionException e) {
                logger.error("Task failed", e);
            }
        }

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.MINUTES);
        
        in.close();
    }
    
    private void readPass2(File file) throws IOException {
        // Pass 2: Sequential processing of client metadata with immediate auth lookup
        String guess = MimeTypes.guessContentTypeFromName(file.getName());
        BufferedReader in = createReader(file, guess);
        
        String currentLine;
        int lineNum = 0;
        int metadataCount = 0;
        int connectionEndCount = 0;
        long startTime = System.currentTimeMillis();
        
        while ((currentLine = readLineSafe(in, 1 * 1024 * 1024)) != null) {
            lineNum++;

            // Check line limit
            if (lineLimit != null && lineNum > lineLimit) {
                System.out.printf("📊 Pass 2: Reached line limit of %d lines, stopping parsing\n", lineLimit);
                break;
            }

            // Fast filtering: only process lines relevant to Pass 2
            // Optimize: single check for NETWORK, then check specific message types
            boolean isMetadataMessage = false;
            boolean isConnectionEndMessage = false;
            
            int networkIdx = currentLine.indexOf("\"c\":\"NETWORK\"");
            if (networkIdx >= 0) {
                // Only check for specific messages if it's a NETWORK message
                isMetadataMessage = currentLine.indexOf("\"msg\":\"client metadata\"") >= 0;
                if (!isMetadataMessage) {
                    isConnectionEndMessage = currentLine.indexOf("\"msg\":\"Connection ended\"") >= 0;
                }
            }
            
            if (!isMetadataMessage && !isConnectionEndMessage) {
                continue; // Skip irrelevant lines immediately
            }
            
            // Process client metadata messages in Pass 2
            if (isMetadataMessage) {
                // Quick ctx check first to avoid unnecessary JSON parsing
                String ctxFast = extractCtxFast(currentLine);
                if (ctxFast == null) {
                    continue; // Skip if no valid ctx
                }
                
                try {
                    JSONObject jo = new JSONObject(currentLine);
                    String ctx = jo.optString("ctx");
                    long timestamp = extractTimestamp(jo);
                    
                    // Skip if timestamp extraction failed
                    if (timestamp <= 0) {
                        continue;
                    }
                    
                    JSONObject attr = jo.optJSONObject("attr");
                    
                    if (attr != null) {
                        JSONObject doc = attr.optJSONObject("doc");
                        if (doc != null) {
                            String remoteHost = attr.optString("remote");
                            JSONObject driver = doc.optJSONObject("driver");
                            JSONObject os = doc.optJSONObject("os");
                            
                            String driverName = null, driverVersion = null;
                            if (driver != null) {
                                driverName = driver.optString("name");
                                driverVersion = driver.optString("version");
                            }
                            
                            String osType = null, osName = null, platform = null;
                            if (os != null) {
                                osType = os.optString("type");
                                osName = os.optString("name");
                                platform = os.optString("architecture");
                            }
                            
                            // Extract compressors
                            Set<String> compressors = new HashSet<>();
                            try {
                                org.json.JSONArray negotiatedCompressors = attr.optJSONArray("negotiatedCompressors");
                                if (negotiatedCompressors != null) {
                                    for (int i = 0; i < negotiatedCompressors.length(); i++) {
                                        String compressor = negotiatedCompressors.optString(i);
                                        if (compressor != null && !compressor.isEmpty()) {
                                            compressors.add(compressor);
                                        }
                                    }
                                }
                                // Compressor extraction working correctly
                            } catch (Exception e) {
                                // Skip compressor extraction on error
                            }
                            
                            // Debug: log specific connection we're tracking
                            if ("conn41943681".equals(ctx)) {
                                System.out.printf("[DEBUG] Calling accumulate for %s: driver=%s|%s, os=%s|%s\n", 
                                                 ctx, driverName, driverVersion, osType, osName);
                            }
                            
                            driverStatsAccumulator.accumulate(driverName, driverVersion, compressors, 
                                                            osType, osName, platform, null, remoteHost, ctx, timestamp, currentLine);
                            metadataCount++;
                        }
                    }
                } catch (Exception e) {
                    // Skip malformed JSON
                }
            }
            
            // Process connection end events to calculate lifetimes  
            if (isConnectionEndMessage) {
                // Optimized: Extract only ctx and timestamp without full JSON parsing
                String ctx = extractCtxFast(currentLine);
                if (ctx != null) {
                    // Only parse JSON for timestamp extraction
                    try {
                        long timestamp = extractTimestampFast(currentLine);
                        if (timestamp > 0) {
                            driverStatsAccumulator.recordConnectionEnd(ctx, timestamp);
                            connectionEndCount++;
                        }
                    } catch (Exception e) {
                        // Skip if timestamp extraction fails
                    }
                }
            }
            
            // Process other log types for the main accumulators
            processLineForMainAccumulators(currentLine, lineNum);
            
            if (lineNum % 1000000 == 0) {
                long elapsed = System.currentTimeMillis() - startTime;
                double linesPerSec = lineNum / (elapsed / 1000.0);
                System.out.printf("Pass 2: Processed %d lines, %d metadata, %d conn ends, %.1fs elapsed, %.0f lines/sec\n", 
                                lineNum, metadataCount, connectionEndCount, elapsed / 1000.0, linesPerSec);
            }
        }
        
        long totalElapsed = System.currentTimeMillis() - startTime;
        double avgLinesPerSec = lineNum / (totalElapsed / 1000.0);
        System.out.printf("Pass 2 complete: Processed %d TOTAL LINES, %d metadata, %d connection ends, %.1fs total, %.0f avg lines/sec\n", 
                        lineNum, metadataCount, connectionEndCount, totalElapsed / 1000.0, avgLinesPerSec);
        in.close();
    }
    
    private void processLineForMainAccumulators(String line, int lineNum) {
        // For now, skip other accumulators in two-pass mode to keep it simple
        // The focus is on fixing the driver stats memory leak
        // Other accumulators can be added back later if needed
    }
    
    private void processAuthenticationPass1(JSONObject jo, JSONObject attr) {
        try {
            if (jo.has("msg") && "Authentication succeeded".equals(jo.getString("msg"))) {
                String ctx = jo.has("ctx") ? jo.getString("ctx") : null;
                
                if (attr.has("principalName")) {
                    String username = attr.getString("principalName");
                    String database = attr.has("authenticationDatabase") ? attr.getString("authenticationDatabase") : null;
                    String mechanism = attr.has("mechanism") ? attr.getString("mechanism") : null;
                    
                    if (ctx != null && username != null && driverStatsAccumulator != null) {
                        synchronized (driverStatsAccumulator) {
                            driverStatsAccumulator.recordAuthentication(ctx, username, database, mechanism);
                        }
                    }
                }
            }
        } catch (Exception e) {
            // Ignore errors in Pass 1
        }
    }


    // Message types for Pass 1 processing
    private enum MessageType {
        AUTH, METADATA
    }
    
    // Pass 1 Task for parallel processing of authentication and connection start events
    private static class Pass1Task implements Callable<Void> {
        private final List<String> linesChunk;
        private final TwoPassDriverStatsAccumulator driverStatsAccumulator;
        
        public Pass1Task(List<String> linesChunk, TwoPassDriverStatsAccumulator driverStatsAccumulator) {
            this.linesChunk = linesChunk;
            this.driverStatsAccumulator = driverStatsAccumulator;
        }
        
        @Override
        public Void call() throws Exception {
            for (String line : linesChunk) {
                // Fast string matching first - avoid JSON parsing unless needed
                boolean isAuthMessage = line.contains("\"c\":\"ACCESS\"") && line.contains("\"msg\":\"Successfully authenticated\"");
                boolean isMetadataMessage = line.contains("\"c\":\"NETWORK\"") && line.contains("\"msg\":\"client metadata\"");
                
                if (!isAuthMessage && !isMetadataMessage) {
                    continue; // Skip irrelevant lines without JSON parsing
                }
                
                try {
                    // Only parse JSON for relevant lines
                    JSONObject jo = new JSONObject(line);
                    String ctx = jo.optString("ctx");
                    
                    if (isAuthMessage) {
                        // Process authentication messages
                        JSONObject attr = jo.optJSONObject("attr");
                        if (attr != null) {
                            String username = attr.optString("user");
                            String database = attr.optString("db");
                            String mechanism = attr.optString("mechanism");
                            
                            if (ctx != null && username != null) {
                                // ConcurrentHashMap is thread-safe, no need for additional synchronization
                                driverStatsAccumulator.recordAuthentication(ctx, username, database, mechanism, line);
                            }
                        }
                    }
                    
                    if (isMetadataMessage) {
                        // Track client metadata as connection start times
                        long timestamp = extractTimestampStatic(jo);
                        if (ctx != null && timestamp > 0) {
                            // ConcurrentHashMap is thread-safe, no need for additional synchronization
                            driverStatsAccumulator.recordConnectionStart(ctx, timestamp);
                        }
                    }
                    
                } catch (Exception e) {
                    // Skip malformed JSON
                }
            }
            return null;
        }
        
        // Static version of extractTimestamp for use in static context
        private static long extractTimestampStatic(JSONObject jo) {
            try {
                JSONObject t = jo.optJSONObject("t");
                if (t != null) {
                    String dateStr = t.optString("$date");
                    if (dateStr != null && !dateStr.isEmpty()) {
                        return java.time.Instant.parse(dateStr).toEpochMilli();
                    }
                }
            } catch (Exception e) {
                // Return invalid timestamp if parsing fails
            }
            return -1;
        }
    }
    
    public static void main(String[] args) {
        int exitCode = new CommandLine(new LogParser()).execute(args);
        System.exit(exitCode);
    }
}