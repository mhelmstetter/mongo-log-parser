package com.mongodb.logparse;

import java.io.File;
import java.util.List;
import java.util.concurrent.Callable;

import org.json.JSONException;
import org.json.JSONObject;

// Replace the LogParserTask class with this improved version:
class LogParserTask implements Callable<ProcessingStats> {
    private List<String> linesChunk;
    private final Accumulator accumulator;
    private final File file;

    public LogParserTask(List<String> linesChunk, File file, Accumulator accumulator) {
        this.linesChunk = linesChunk;
        this.file = file;
        this.accumulator = accumulator;
    }

    @Override
    public ProcessingStats call() throws Exception {
        long localParseErrors = 0;
        long localNoAttr = 0;
        long localNoCommand = 0;
        long localNoNs = 0;
        long localFoundOps = 0;

        for (String currentLine : linesChunk) {
            JSONObject jo = null;
            try {
                jo = new JSONObject(currentLine);
            } catch (JSONException jse) {
                if (currentLine.length() > 0) {
                    localParseErrors++;
                    // Log first few parse errors for debugging
                    if (localParseErrors <= 3) {
                        LogParser.logger.warn("Parse error in thread {}: {}", 
                                   Thread.currentThread().getName(), 
                                   currentLine.substring(0, Math.min(200, currentLine.length())));
                    }
                }
                continue;
            }
            
            JSONObject attr = null;
            if (jo.has("attr")) {
                attr = jo.getJSONObject("attr");
            } else {
                localNoAttr++;
                // Log sample of non-attr entries
                if (localNoAttr <= 3) {
                    LogParser.logger.info("No 'attr' field (thread {}): {}", 
                               Thread.currentThread().getName(),
                               currentLine.substring(0, Math.min(300, currentLine.length())));
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
                    if (localNoNs <= 3) {
                        LogParser.logger.info("No 'ns' field in command (thread {}): {}", 
                                   Thread.currentThread().getName(),
                                   attr.toString().substring(0, Math.min(200, attr.toString().length())));
                    }
                    continue;
                }
                
                if (attr.has("queryHash")) {
                    slowQuery.queryHash = attr.getString("queryHash");
                }
                
                if (attr.has("reslen")) {
                    slowQuery.reslen = LogParser.getMetric(attr, "reslen");
                }
                
                if (attr.has("remote")) {
                    slowQuery.remote = attr.getString("remote");
                }
                
                slowQuery.opType = null;
                JSONObject command = attr.getJSONObject("command");
                
                // Enhanced operation type detection with logging
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
                    if (command.has("collection")) {
                        ns.setCollectionName(command.getString("collection"));
                    }
                } else if (command.has("u")) {
                    slowQuery.opType = OpType.UPDATE_W;
                } else if (command.has("distinct")) {
                    slowQuery.opType = OpType.DISTINCT;
                } else if (command.has("count")) {
                    slowQuery.opType = OpType.COUNT;
                } else {
                    // Log unknown commands for the first few occurrences
                    if (localFoundOps == 0) {
                        LogParser.logger.info("Unknown command type (thread {}): {}", 
                                   Thread.currentThread().getName(),
                                   command.toString().substring(0, Math.min(300, command.toString().length())));
                    }
                }
                
                if (attr.has("storage")) {
                    JSONObject storage = attr.getJSONObject("storage");
                    if (storage != null) {
                        if (storage.has("bytesRead")) {
                            slowQuery.bytesRead = LogParser.getMetric(storage, "bytesRead");
                        } else if (storage.has("data")) {
                            JSONObject data = storage.getJSONObject("data");
                            if (data.has("bytesRead")) {
                                slowQuery.bytesRead = LogParser.getMetric(data, "bytesRead");
                            }
                        }
                    }
                }
                
                if (slowQuery.opType != null) {
                    if (attr.has("nreturned")) {
                        slowQuery.docsExamined = LogParser.getMetric(attr, "docsExamined");
                        slowQuery.keysExamined = LogParser.getMetric(attr, "keysExamined");
                        slowQuery.nreturned = LogParser.getMetric(attr, "nreturned");
                    }
                    slowQuery.durationMillis = LogParser.getMetric(attr, "durationMillis");
                    
                    synchronized (accumulator) {
                        accumulator.accumulate(slowQuery);
                    }
                    localFoundOps++;
                }
            } else {
                localNoCommand++;
                if (localNoCommand <= 3) {
                    LogParser.logger.info("No 'command' field in attr (thread {}): {}", 
                               Thread.currentThread().getName(),
                               attr.toString().substring(0, Math.min(200, attr.toString().length())));
                }
            }
        }
        
        // Log summary for this chunk
        LogParser.logger.info("Thread {} processed {} lines: {} ops found, {} parse errors, {} no attr, {} no command, {} no ns", 
                   Thread.currentThread().getName(), linesChunk.size(), localFoundOps, 
                   localParseErrors, localNoAttr, localNoCommand, localNoNs);
        
        this.linesChunk = null;
        return new ProcessingStats(localParseErrors, localNoAttr, localNoCommand, localNoNs, localFoundOps);
    }
}