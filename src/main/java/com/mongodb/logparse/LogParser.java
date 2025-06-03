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

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.util.MimeTypes;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * MongoDB Log Parser - Unified class combining LogParser interface, 
 * AbstractLogParser, LogParserApp, and LogParserJson functionality
 */
@Command(name = "logParser", mixinStandardHelpOptions = true, version = "1.0", 
         description = "Parse MongoDB log files and generate performance reports")
public class LogParser implements Callable<Integer> {

    private static final Logger logger = LoggerFactory.getLogger(LogParser.class);

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
        logger.debug("MIME type guess: {}", guess);

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
        CompletionService<Void> completionService = new ExecutorCompletionService<>(executor);

        unmatchedCount = 0;
        ignoredCount = 0;
        processedCount = 0;
        int lineNum = 0;
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
                // Submit the chunk for processing as soon as it's filled
                completionService.submit(new LogParserTask(new ArrayList<>(lines), file, accumulator));
                lines.clear();
            }

            if (lineNum % 50000 == 0) {
                logger.info("Processed {} lines (ignored: {}, processed: {})", lineNum, ignoredCount, processedCount);
            }
        }

        // Submit the last chunk if there are remaining lines
        if (!lines.isEmpty()) {
            completionService.submit(new LogParserTask(new ArrayList<>(lines), file, accumulator));
        }

        // Shutdown the executor
        executor.shutdown();
        try {
            executor.awaitTermination(999, TimeUnit.DAYS);
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
        logger.info("File processing complete - Duration: {}ms, Total lines: {}, Processed: {}, Ignored: {}", 
                   dur, lineNum, processedCount, ignoredCount);
        
        // Log breakdown of why operations weren't parsed
        int totalTasks = (processedCount / 25000) + (processedCount % 25000 > 0 ? 1 : 0);
        logger.info("Submitted {} processing tasks for {} lines", totalTasks, processedCount);
    }

    private static Long getMetric(JSONObject attr, String key) {
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

    // Task to parse a chunk of log lines
    static class LogParserTask implements Callable<Void> {
        private List<String> linesChunk;
        private final Accumulator accumulator;
		private int localParseErrors;
		private int localNoAttr;
		private int localFoundOps;
		private int localNoCommand;

        public LogParserTask(List<String> linesChunk, File file, Accumulator accumulator) {
            this.linesChunk = linesChunk;
            this.accumulator = accumulator;
        }

        @Override
        public Void call() throws Exception {
            
            int localNoNs = 0;
			for (String currentLine : linesChunk) {
                
                JSONObject jo = null;
                try {
                    jo = new JSONObject(currentLine);
                } catch (JSONException jse) {
                    if (currentLine.length() > 0) {
                        localParseErrors++;
                        // Only log first few parse errors to avoid spam
                        if (localParseErrors <= 2) {
                            logger.warn("Error parsing line: {}", currentLine.substring(0, Math.min(100, currentLine.length())));
                        }
                    }
                    continue;
                }
                
                JSONObject attr = null;
                if (jo.has("attr")) {
                    attr = jo.getJSONObject("attr");
                } else {
                    localNoAttr++;
                    // Log a sample of entries without 'attr' to understand the structure
                    if (localNoAttr <= 2) {
                        logger.debug("No 'attr' field in log entry: {}", currentLine.substring(0, Math.min(150, currentLine.length())));
                    }
                    continue;
                }
                
                Namespace ns = null;
                if (attr.has("command")) {
                    SlowQuery slowQuery = new SlowQuery();
                    if (attr.has("ns")) {
                        ns = new Namespace(attr.getString("ns"));
                        slowQuery.ns = ns;
                    } else {
                        localNoNs++;
                        continue;
                    }
                    
                    if (attr.has("queryHash")) {
                        slowQuery.queryHash = attr.getString("queryHash");
                    }
                    
                    if (attr.has("reslen")) {
                        slowQuery.reslen = getMetric(attr, "reslen");
                    }
                    
                    if (attr.has("remote")) {
                        slowQuery.queryHash = attr.getString("queryHash");
                    }
                    
                    slowQuery.opType = null;
                    JSONObject command = attr.getJSONObject("command");
                    if (command.has("find")) {
                        String find = command.getString("find");
                        slowQuery.opType = OpType.QUERY;
                    } else if (command.has("aggregate")) {
                        slowQuery.opType = OpType.AGGREGATE;
                    } else if (command.has("findAndModify")) {
                        slowQuery.opType = OpType.FIND_AND_MODIFY;
                        ns.setCollectionName(command.getString("findAndModify"));
                    } else if (command.has("update")) {
                        slowQuery.opType = OpType.UPDATE;
                        ns.setCollectionName(command.getString("update"));
                    } else if (command.has("insert")) {
                        slowQuery.opType = OpType.INSERT;
                        ns.setCollectionName(command.getString("insert"));
                    } else if (command.has("delete")) {
                        slowQuery.opType = OpType.REMOVE;
                        ns.setCollectionName(command.getString("delete"));
                    } else if (command.has("getMore")) {
                        slowQuery.opType = OpType.GETMORE;
                        ns.setCollectionName(command.getString("collection"));
                    } else if (command.has("u")) {
                        slowQuery.opType = OpType.UPDATE_W;
                    } else if (command.has("distinct")) {
                        slowQuery.opType = OpType.DISTINCT;
                    } else if (command.has("count")) {
                        slowQuery.opType = OpType.COUNT;
                    } else {
                        logger.debug("Unknown command: {}", command);
                    }
                    
                    if (attr.has("storage")) {
                        JSONObject storage = attr.getJSONObject("storage");
                        if (storage != null) {
                            if (storage.has("bytesRead")) {
                                slowQuery.bytesRead = getMetric(storage, "bytesRead");
                            } else if (storage.has("data")) {
                                JSONObject data = storage.getJSONObject("data");
                                slowQuery.bytesRead = getMetric(data, "bytesRead");
                            }
                        }
                    }
                    
                    if (slowQuery.opType != null) {
                        if (attr.has("nreturned")) {
                            slowQuery.docsExamined = getMetric(attr, "docsExamined");
                            slowQuery.keysExamined = getMetric(attr, "keysExamined");
                            slowQuery.nreturned = getMetric(attr, "nreturned");
                        }
                        slowQuery.durationMillis = getMetric(attr, "durationMillis");
                        accumulator.accumulate(slowQuery);
                        localFoundOps++;
                    }
                } else {
                    localNoCommand++;
                    // Log sample of entries without 'command' to understand what we're missing
                    if (localNoCommand <= 2) {
                        logger.debug("No 'command' field in attr: {}", attr.toString().substring(0, Math.min(150, attr.toString().length())));
                    }
                }
            }
            
            // Log summary for this chunk if there were significant issues
            if (localParseErrors > 0 || localNoAttr > 1000 || localNoCommand > 1000) {
                logger.debug("Chunk processed - Parse errors: {}, No attr: {}, No command: {}, No ns: {}, Found ops: {}", 
                           localParseErrors, localNoAttr, localNoCommand, localNoNs, localFoundOps);
            }
            
            this.linesChunk = null;
            return null;
        }
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new LogParser()).execute(args);
        System.exit(exitCode);
    }
}