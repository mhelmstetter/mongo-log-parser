package com.mongodb.logparse;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
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
import org.json.simple.parser.ParseException;

import com.mongodb.util.MimeTypes;

public class LogParserJson extends AbstractLogParser implements LogParser {

    private Map<Namespace, Map<Set<String>, AtomicInteger>> shapesByNamespace = new HashMap<>();
    private Accumulator shapeAccumulator = new Accumulator();
    private static List<String> ignore = Arrays.asList("\"c\":\"NETWORK\"", "\"c\":\"ACCESS\"", 
    		"\"c\":\"CONNPOOL\"", "\"c\":\"STORAGE\"", "\"profile\":", "\"killCursors\":",
//    		"\"hello\":1", "\"isMaster\":1", "\"ping\":1", "\"saslContinue\":1", 
//    		"\"replSetHeartbeat\":\"", "\"serverStatus\":1", "\"replSetGetStatus\":1", "\"buildInfo\":1",
//    		"\"getParameter\":", "\"getCmdLineOpts\":1",
//    		"\"logRotate\":\"", "\"getDefaultRWConcern\":1", "\"listDatabases\":1", "\"endSessions\":",
    		"\"$db\":\"admin\"", "\"$db\":\"local\"", "\"$db\":\"config\"", "\"ns\":\"local.clustermanager\"", 
    		"\"dbstats\":1", "\"listIndexes\":\"", "\"collStats\":\"");

    @Override
    public void read(File file) throws IOException, ParseException, InterruptedException, ExecutionException {

        String guess = MimeTypes.guessContentTypeFromName(file.getName());
        logger.debug(guess);

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
        int lineNum = 0;
        long start = System.currentTimeMillis();
        List<String> lines = new ArrayList<>();

        while ((currentLine = in.readLine()) != null) {
            lineNum++;
            
            if (ignoreLine(currentLine)) {
                continue;
            }

            lines.add(currentLine);

            if (lines.size() >= 25000) {
                // Submit the chunk for processing as soon as it's filled
                completionService.submit(new LogParserTask(new ArrayList<>(lines), file, accumulator));
                lines.clear();
            }

            if (lineNum % 25000 == 0) {
                System.out.print(".");
                if (lineNum % 250000 == 0) {
                    System.out.println();
                }
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
        logger.debug(String.format("Elapsed millis: %s, lineCount: %s, unmatchedCount: %s", dur, lineNum, unmatchedCount));
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

    // Task to parse a chunk of log lines
    static class LogParserTask implements Callable<Void> {
        private List<String> linesChunk;
        private final Accumulator accumulator;

        public LogParserTask(List<String> linesChunk, File file, Accumulator accumulator) {
            this.linesChunk = linesChunk;
            this.accumulator = accumulator;
            logger.debug("new task, size: {}", linesChunk.size());
        }

        @Override
        public Void call() throws Exception {
        	
            for (String currentLine : linesChunk) {
                
                JSONObject jo = null;
                try {
                    jo = new JSONObject(currentLine);
                } catch (JSONException jse) {
                    if (currentLine.length() > 0) {
                        logger.warn("Error parsing line: {}", currentLine);
                    }
                    continue;
                }
                JSONObject attr = null;
                if (jo.has("attr")) {
                    attr = jo.getJSONObject("attr");
                } else {
                    continue;
                }
                
                Namespace ns = null;
                if (attr.has("command")) {
                    SlowQuery slowQuery = new SlowQuery();
                    if (attr.has("ns")) {
                        ns = new Namespace(attr.getString("ns"));
                        slowQuery.ns = ns;
                    } else {
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
                    
//                    if (ns.getCollectionName().equals("$cmd")) {
//                    	
//                    }
                    
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
                    	logger.debug("unk command: {}", command);
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
                    } else {
                    	System.out.println();
                    }
                }
            } 
            this.linesChunk = null;
            return null;
        }
    }
}
