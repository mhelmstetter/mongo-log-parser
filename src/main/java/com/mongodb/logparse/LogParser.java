package com.mongodb.logparse;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipInputStream;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.util.MimeTypes;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.concurrent.atomic.AtomicLong;



/**
 * MongoDB Log Parser - Unified class combining LogParser interface, 
 * AbstractLogParser, LogParserApp, and LogParserJson functionality
 */
@Command(name = "logParser", mixinStandardHelpOptions = true, version = "1.0", 
         description = "Parse MongoDB log files and generate performance reports")
public class LogParser implements Callable<Integer> {

    static final Logger logger = LoggerFactory.getLogger(LogParser.class);
    
  //Add these fields to LogParser class
    private AtomicLong totalParseErrors = new AtomicLong(0);
    private AtomicLong totalNoAttr = new AtomicLong(0);
    private AtomicLong totalNoCommand = new AtomicLong(0);
    private AtomicLong totalNoNs = new AtomicLong(0);
    private AtomicLong totalFoundOps = new AtomicLong(0);

    @Option(names = {"-f", "--files"}, description = "MongoDB log file(s)", required = true)
    private String[] fileNames;

    @Option(names = {"-q", "--queries"}, description = "Parse queries")
    private boolean parseQueries = false;

    @Option(names = {"--replay"}, description = "Replay operations")
    private boolean replay = false;

    @Option(names = {"--uri"}, description = "MongoDB connection string URI")
    private String uri;

    @Option(names = {"-c", "--csv"}, description = "CSV output file")
    private String csvOutputFile;

    // Constants for operation types
    public static final String FIND = "find";
    public static final String FIND_AND_MODIFY = "findAndModify";
    public static final String UPDATE = "update";
    public static final String INSERT = "insert";
    public static final String DELETE = "delete";
    public static final String DELETE_W = "delete_w";
    public static final String COUNT = "count";
    public static final String UPDATE_W = "update_w";
    public static final String GETMORE = "getMore";

    private String currentLine = null;
    private Accumulator accumulator;
    private int unmatchedCount = 0;
    private int ignoredCount = 0;
    private int processedCount = 0;

    private Map<Namespace, Map<Set<String>, AtomicInteger>> shapesByNamespace = new HashMap<>();
    private Accumulator shapeAccumulator = new Accumulator();

    private static List<String> ignore = Arrays.asList("\"c\":\"NETWORK\"", "\"c\":\"ACCESS\"", 
            "\"c\":\"CONNPOOL\"", "\"c\":\"STORAGE\"", "\"c\":\"SHARDING\"", "\"c\":\"CONTROL\"",
            "\"profile\":", "\"killCursors\":", "\"hello\":1", "\"isMaster\":1", "\"ping\":1", 
            "\"saslContinue\":1", "\"replSetHeartbeat\":\"", "\"serverStatus\":1", "\"replSetGetStatus\":1", 
            "\"buildInfo\"", "\"getParameter\":", "\"getCmdLineOpts\":1", "\"logRotate\":\"", 
            "\"getDefaultRWConcern\":1", "\"listDatabases\":1", "\"endSessions\":", "\"ctx\":\"TTLMonitor\"",
            "\"$db\":\"admin\"", "\"$db\":\"local\"", "\"$db\":\"config\"", "\"ns\":\"local.clustermanager\"", 
            "\"dbstats\":1", "\"listIndexes\":\"", "\"collStats\":\"");

    public LogParser() {
        accumulator = new Accumulator();
    }

    @Override
    public Integer call() throws Exception {
        logger.info("Starting MongoDB log parsing with {} processors", Runtime.getRuntime().availableProcessors());
        
        read();
        
        logger.info("Parsing complete. Total processed: {}, Ignored: {}", 
                   processedCount, ignoredCount);
        
        if (csvOutputFile != null) {
            logger.info("Writing CSV report to: {}", csvOutputFile);
            reportCsv();
        } else {
            report();
        }
        
        return 0;
    }

    public void read() throws IOException, ExecutionException, InterruptedException {
        for (String fileName : fileNames) {
            File f = new File(fileName);
            read(f);
        }
    }
    
    public void read(File file) throws IOException, ExecutionException, InterruptedException {
        String guess = MimeTypes.guessContentTypeFromName(file.getName());
        logger.info("MIME type guess: {}", guess); // Changed from debug to info

        BufferedReader in = null;

        if (guess != null && guess.equals(MimeTypes.GZIP)) {
            FileInputStream fis = new FileInputStream(file);
            GZIPInputStream gzis = new GZIPInputStream(fis);
            in = new BufferedReader(new InputStreamReader(gzis));
        } else if (guess != null && guess.equals(MimeTypes.ZIP)) {
            FileInputStream fis = new FileInputStream(file);
            ZipInputStream zis = new ZipInputStream(fis);
            in = new BufferedReader(new InputStreamReader(zis));
        } else {
            in = new BufferedReader(new FileReader(file));
        }

        int numThreads = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CompletionService<ProcessingStats> completionService = new ExecutorCompletionService<>(executor);

        unmatchedCount = 0;
        ignoredCount = 0;
        processedCount = 0;
        int lineNum = 0;
        int submittedTasks = 0;
        long start = System.currentTimeMillis();
        List<String> lines = new ArrayList<>();

        logger.info("Processing file: {}", file.getName());

        while ((currentLine = in.readLine()) != null) {
            lineNum++;
            
            if (ignoreLine(currentLine)) {
                ignoredCount++;
                continue;
            }

            lines.add(currentLine);
            processedCount++;

            if (lines.size() >= 25000) {
                // Submit the chunk for processing
                completionService.submit(new LogParserTask(new ArrayList<>(lines), file, accumulator));
                submittedTasks++;
                lines.clear();
            }

            if (lineNum % 50000 == 0) {
                logger.info("Read {} lines (ignored: {}, queued for processing: {})", lineNum, ignoredCount, processedCount);
            }
        }

        // Submit the last chunk if there are remaining lines
        if (!lines.isEmpty()) {
            completionService.submit(new LogParserTask(new ArrayList<>(lines), file, accumulator));
            submittedTasks++;
        }

        // IMPORTANT: Wait for all tasks to complete and collect their results
        logger.info("Waiting for {} tasks to complete...", submittedTasks);
        for (int i = 0; i < submittedTasks; i++) {
            try {
                ProcessingStats stats = completionService.take().get();
                totalParseErrors.addAndGet(stats.parseErrors);
                totalNoAttr.addAndGet(stats.noAttr);
                totalNoCommand.addAndGet(stats.noCommand);
                totalNoNs.addAndGet(stats.noNs);
                totalFoundOps.addAndGet(stats.foundOps);
                
                if (i % 10 == 0 || i == submittedTasks - 1) {
                    logger.info("Completed {}/{} tasks", i + 1, submittedTasks);
                }
            } catch (ExecutionException e) {
                logger.error("Task execution failed", e);
            }
        }

        // Shutdown the executor
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

        if (parseQueries) {
            reportParsedQueries();
        }

        in.close();
        long end = System.currentTimeMillis();
        long dur = (end - start);
        
        // Enhanced logging with detailed statistics
        logger.info("File processing complete - Duration: {}ms", dur);
        logger.info("Lines read: {}, Ignored: {}, Processed: {}", lineNum, ignoredCount, processedCount);
        logger.info("Parse errors: {}, No attr: {}, No command: {}, No namespace: {}", 
                   totalParseErrors.get(), totalNoAttr.get(), totalNoCommand.get(), totalNoNs.get());
        logger.info("Successfully parsed operations: {}", totalFoundOps.get());
        
        if (totalFoundOps.get() == 0) {
            logger.warn("WARNING: No operations were successfully parsed!");
            logger.warn("This might indicate:");
            logger.warn("  - Wrong log format");
            logger.warn("  - All operations are being filtered out");
            logger.warn("  - JSON parsing issues");
        }
    }

    static Long getMetric(JSONObject attr, String key) {
        if (attr.has(key)) {
            return Long.valueOf(attr.getInt(key));
        }
        return null;
    }

    private void reportParsedQueries() {
        for (Namespace namespace : shapesByNamespace.keySet()) {
            Map<Set<String>, AtomicInteger> shapeCounter = shapesByNamespace.get(namespace);

            for (Map.Entry<Set<String>, AtomicInteger> entry : shapeCounter.entrySet()) {
                Set<String> key = entry.getKey();
                AtomicInteger value = entry.getValue();
                System.out.println(namespace + "|" + key + "|" + value);
            }
        }
        System.out.println("------------------\n");
        shapeAccumulator.report();
        System.out.println("------------------\n");
    }

    private static boolean ignoreLine(String line) {
        for (String keyword : ignore) {
            if (line.contains(keyword)) {
                return true;
            }
        }
        return false;
    }

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

    public boolean isParseQueries() {
        return parseQueries;
    }

    public void setParseQueries(boolean parseQueries) {
        this.parseQueries = parseQueries;
    }

    public boolean isReplay() {
        return replay;
    }

    public void setReplay(boolean replay) {
        this.replay = replay;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String[] getFileNames() {
        return fileNames;
    }

    public void setFileNames(String[] fileNames) {
        this.fileNames = fileNames;
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new LogParser()).execute(args);
        System.exit(exitCode);
    }
}