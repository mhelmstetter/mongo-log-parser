package com.mongodb.logparse;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

import org.json.JSONException;
import org.json.JSONObject;

class LogParserTask implements Callable<ProcessingStats> {
    private List<String> linesChunk;
    private final Accumulator accumulator;
    private final PlanCacheAccumulator planCacheAccumulator;
    private final File file;
    private final Map<String, AtomicLong> operationTypeStats;
    private final boolean enablePlanCacheAnalysis;
    private final boolean debug;

    public LogParserTask(List<String> linesChunk, File file, Accumulator accumulator, 
                        PlanCacheAccumulator planCacheAccumulator,
                        Map<String, AtomicLong> operationTypeStats, 
                        boolean enablePlanCacheAnalysis, boolean debug) {
        this.linesChunk = linesChunk;
        this.file = file;
        this.accumulator = accumulator;
        this.planCacheAccumulator = planCacheAccumulator;
        this.operationTypeStats = operationTypeStats;
        this.enablePlanCacheAnalysis = enablePlanCacheAnalysis;
        this.debug = debug;
    }

    @Override
    public ProcessingStats call() throws Exception {
        long localParseErrors = 0;
        long localNoAttr = 0;
        long localNoCommand = 0;
        long localNoNs = 0;
        long localFoundOps = 0;
        int debugCount = 0;

        for (String currentLine : linesChunk) {
            JSONObject jo = null;
            try {
                jo = new JSONObject(currentLine);
            } catch (JSONException jse) {
                if (currentLine.length() > 0) {
                    localParseErrors++;
                    if (debug && localParseErrors <= 3) {
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
                if (debug && localNoAttr <= 3) {
                    LogParser.logger.info("No 'attr' field (thread {}): {}", 
                               Thread.currentThread().getName(),
                               currentLine.substring(0, Math.min(300, currentLine.length())));
                }
                continue;
            }
            
            Namespace ns = null;
            SlowQuery slowQuery = new SlowQuery();
            
            // Check for INDEX operations first (TTL operations, index maintenance)
            if (jo.has("c") && "INDEX".equals(jo.getString("c"))) {
                if (processIndexOperation(jo, attr, slowQuery)) {
                    synchronized (accumulator) {
                        accumulator.accumulate(slowQuery);
                    }
                    localFoundOps++;
                    incrementOperationStat("index_operation");
                    continue;
                }
            }
            
            // Handle different log entry types
            if (attr.has("command")) {
                // This is a COMMAND type log entry
                if (attr.has("ns")) {
                    ns = new Namespace(attr.getString("ns"));
                    slowQuery.ns = ns;
                } else {
                    localNoNs++;
                    if (debug && localNoNs <= 3) {
                        LogParser.logger.info("No 'ns' field in command (thread {}): {}", 
                                   Thread.currentThread().getName(),
                                   attr.toString().substring(0, Math.min(200, attr.toString().length())));
                    }
                    continue;
                }
                
                // Set common attributes
                setCommonAttributes(attr, slowQuery);
                
                // Extract plan cache information
                extractPlanCacheInfo(attr, slowQuery);
                
                // NEW: Extract replanning information
                extractReplanningInfo(attr, slowQuery);
                
                JSONObject command = attr.getJSONObject("command");
                
                // Enhanced operation type detection
                if (processCommandOperation(command, slowQuery, ns)) {
                    // Handle execution stats and storage metrics
                    processExecutionStats(attr, slowQuery);
                    processStorageMetrics(attr, slowQuery);
                    
                    slowQuery.durationMillis = getMetric(attr, "durationMillis");
                    
                    synchronized (accumulator) {
                        accumulator.accumulate(slowQuery);
                    }
                    
                    // Add to plan cache accumulator if enabled and has plan cache key
                    if (enablePlanCacheAnalysis && planCacheAccumulator != null && slowQuery.planCacheKey != null) {
                        synchronized (planCacheAccumulator) {
                            planCacheAccumulator.accumulate(slowQuery);
                        }
                    }
                    
                    localFoundOps++;
                }
                
            } else if (attr.has("type")) {
                // This is a WRITE operation log entry
                if (processWriteOperation(attr, slowQuery)) {
                    extractPlanCacheInfo(attr, slowQuery);
                    extractReplanningInfo(attr, slowQuery);
                    processExecutionStats(attr, slowQuery);
                    processStorageMetrics(attr, slowQuery);
                    
                    slowQuery.durationMillis = getMetric(attr, "durationMillis");
                    
                    synchronized (accumulator) {
                        accumulator.accumulate(slowQuery);
                    }
                    
                    // Add to plan cache accumulator if enabled and has plan cache key
                    if (enablePlanCacheAnalysis && planCacheAccumulator != null && slowQuery.planCacheKey != null) {
                        synchronized (planCacheAccumulator) {
                            planCacheAccumulator.accumulate(slowQuery);
                        }
                    }
                    
                    localFoundOps++;
                }
                
            } else {
                // Check if this is an incomplete/internal entry that should be filtered
                if (isIncompleteLogEntry(attr)) {
                    if (debug && localFoundOps <= 3) {
                        LogParser.logger.debug("Filtered incomplete entry (thread {}): {}", 
                                   Thread.currentThread().getName(),
                                   attr.toString().substring(0, Math.min(200, attr.toString().length())));
                    }
                    continue;
                }
                
                localNoCommand++;
                if (debug && localNoCommand <= 3) {
                    LogParser.logger.info("No 'command' field in attr (thread {}): {}", 
                               Thread.currentThread().getName(),
                               attr.toString().substring(0, Math.min(200, attr.toString().length())));
                }
            }
        }
        
        LogParser.logger.info("Thread {} processed {} lines: {} ops found, {} parse errors, {} no attr, {} no command, {} no ns", 
                   Thread.currentThread().getName(), linesChunk.size(), localFoundOps, 
                   localParseErrors, localNoAttr, localNoCommand, localNoNs);
        
        this.linesChunk = null;
        return new ProcessingStats(localParseErrors, localNoAttr, localNoCommand, localNoNs, localFoundOps);
    }
    
    private void extractPlanCacheInfo(JSONObject attr, SlowQuery slowQuery) {
        // Extract planCacheKey
        if (attr.has("planCacheKey")) {
            try {
                slowQuery.planCacheKey = attr.getString("planCacheKey");
            } catch (JSONException e) {
                if (debug) {
                    LogParser.logger.debug("Error extracting planCacheKey: {}", e.getMessage());
                }
            }
        }
        
        // Extract planSummary
        if (attr.has("planSummary")) {
            try {
                slowQuery.planSummary = attr.getString("planSummary");
            } catch (JSONException e) {
                if (debug) {
                    LogParser.logger.debug("Error extracting planSummary: {}", e.getMessage());
                }
            }
        }
        
        // Extract planningTimeMicros
        if (attr.has("planningTimeMicros")) {
            try {
                slowQuery.planningTimeMicros = getMetric(attr, "planningTimeMicros");
            } catch (JSONException e) {
                if (debug) {
                    LogParser.logger.debug("Error extracting planningTimeMicros: {}", e.getMessage());
                }
            }
        }
    }
    
    // NEW: Extract replanning information from the log attributes
    private void extractReplanningInfo(JSONObject attr, SlowQuery slowQuery) {
        try {
            // Extract replanned flag
            if (attr.has("replanned")) {
                slowQuery.replanned = attr.getBoolean("replanned");
            }
            
            // Extract replan reason
            if (attr.has("replanReason")) {
                slowQuery.replanReason = attr.getString("replanReason");
            }
            
            // Extract fromMultiPlanner flag
            if (attr.has("fromMultiPlanner")) {
                slowQuery.fromMultiPlanner = attr.getBoolean("fromMultiPlanner");
            }
            
        } catch (JSONException e) {
            if (debug) {
                LogParser.logger.debug("Error extracting replanning info: {}", e.getMessage());
            }
        }
    }
    
    private boolean processIndexOperation(JSONObject jo, JSONObject attr, SlowQuery slowQuery) {
        try {
            // Handle TTL operations
            if (attr.has("msg")) {
                String msg = attr.getString("msg");
                if (msg.contains("Deleted expired documents")) {
                    slowQuery.opType = OpType.REMOVE;  // TTL deletion is a remove operation
                    
                    if (attr.has("namespace")) {
                        slowQuery.ns = new Namespace(attr.getString("namespace"));
                    }
                    
                    if (attr.has("numDeleted")) {
                        slowQuery.nreturned = getMetric(attr, "numDeleted");
                    }
                    
                    if (attr.has("durationMillis")) {
                        slowQuery.durationMillis = getMetric(attr, "durationMillis");
                    }
                    
                    incrementOperationStat("ttl_delete");
                    return true;
                }
            }
            
            // Handle other index operations
            if (attr.has("namespace")) {
                slowQuery.ns = new Namespace(attr.getString("namespace"));
                slowQuery.opType = OpType.CMD;  // Index operations are command-type
                
                if (attr.has("durationMillis")) {
                    slowQuery.durationMillis = getMetric(attr, "durationMillis");
                }
                
                // Categorize index operation type
                if (attr.has("msg")) {
                    String msg = attr.getString("msg");
                    if (msg.contains("Index build")) {
                        incrementOperationStat("index_build");
                    } else if (msg.contains("Index drop")) {
                        incrementOperationStat("index_drop");
                    } else {
                        incrementOperationStat("index_other");
                    }
                } else {
                    incrementOperationStat("index_maintenance");
                }
                
                return true;
            }
            
        } catch (JSONException e) {
            if (debug) {
                LogParser.logger.warn("Error processing INDEX operation: {}", e.getMessage());
            }
        }
        
        return false;
    }
    
    private void setCommonAttributes(JSONObject attr, SlowQuery slowQuery) {
        if (attr.has("queryHash")) {
            slowQuery.queryHash = attr.getString("queryHash");
        }
        
        if (attr.has("reslen")) {
            slowQuery.reslen = getMetric(attr, "reslen");
        }
        
        if (attr.has("remote")) {
            slowQuery.remote = attr.getString("remote");
        }
    }
    
    private boolean processCommandOperation(JSONObject command, SlowQuery slowQuery, Namespace ns) {
        // Enhanced operation type detection
        if (command.has("find")) {
            slowQuery.opType = OpType.QUERY;
            Object findVal = command.get("find");
            if (findVal instanceof String) {
                ns.setCollectionName((String) findVal);
            }
            incrementOperationStat("find");
            return true;
            
        } else if (command.has("aggregate")) {
            slowQuery.opType = OpType.AGGREGATE;
            Object aggregateVal = command.get("aggregate");
            if (aggregateVal instanceof String) {
                String coll = (String) aggregateVal;
                if (!coll.equals("1")) {  // aggregate: 1 means database-level aggregation
                    ns.setCollectionName(coll);
                }
            }
            incrementOperationStat("aggregate");
            return true;
            
        } else if (command.has("findAndModify")) {
            slowQuery.opType = OpType.FIND_AND_MODIFY;
            Object findAndModifyVal = command.get("findAndModify");
            if (findAndModifyVal instanceof String) {
                ns.setCollectionName((String) findAndModifyVal);
            }
            incrementOperationStat("findAndModify");
            return true;
            
        } else if (command.has("update")) {
            slowQuery.opType = OpType.UPDATE;
            Object updateVal = command.get("update");
            if (updateVal instanceof String) {
                ns.setCollectionName((String) updateVal);
            }
            incrementOperationStat("update");
            return true;
            
        } else if (command.has("insert")) {
            slowQuery.opType = OpType.INSERT;
            Object insertVal = command.get("insert");
            if (insertVal instanceof String) {
                ns.setCollectionName((String) insertVal);
            }
            incrementOperationStat("insert");
            return true;
            
        } else if (command.has("delete")) {
            slowQuery.opType = OpType.REMOVE;
            Object deleteVal = command.get("delete");
            if (deleteVal instanceof String) {
                ns.setCollectionName((String) deleteVal);
            }
            incrementOperationStat("delete");
            return true;
            
        } else if (command.has("getMore")) {
            slowQuery.opType = OpType.GETMORE;
            if (command.has("collection")) {
                Object collectionVal = command.get("collection");
                if (collectionVal instanceof String) {
                    ns.setCollectionName((String) collectionVal);
                }
            }
            incrementOperationStat("getMore");
            return true;
            
        } else if (command.has("count")) {
            slowQuery.opType = OpType.COUNT;
            Object countVal = command.get("count");
            if (countVal instanceof String) {
                ns.setCollectionName((String) countVal);
            }
            incrementOperationStat("count");
            return true;
            
        } else if (command.has("distinct")) {
            slowQuery.opType = OpType.DISTINCT;
            Object distinctVal = command.get("distinct");
            if (distinctVal instanceof String) {
                ns.setCollectionName((String) distinctVal);
            }
            incrementOperationStat("distinct");
            return true;
        }
        
        // Handle other operations...
        return processOtherOperations(command, slowQuery);
    }
    
    private boolean processOtherOperations(JSONObject command, SlowQuery slowQuery) {
        // Check for MongoDB shard operations and other commands
        Iterator<String> keys = command.keys();
        while (keys.hasNext()) {
            String key = keys.next();
            
            // Shard operations
            if (key.startsWith("_shardsv")) {
                slowQuery.opType = OpType.CMD;
                incrementOperationStat("shard_" + key);
                return true;
            }
            
            // Administrative operations
            if (isAdministrativeOperation(key)) {
                slowQuery.opType = OpType.CMD;
                incrementOperationStat(key);
                return true;
            }
        }
        
        return false;
    }
    
    private boolean isAdministrativeOperation(String key) {
        return key.equals("drop") || key.equals("dropDatabase") || key.equals("dropIndexes") ||
               key.equals("createIndexes") || key.equals("collMod") || key.equals("renameCollection") ||
               key.equals("validate") || key.equals("compact") || key.equals("reIndex") ||
               key.equals("explain") || key.equals("currentOp") || key.equals("killOp") ||
               key.equals("fsync") || key.equals("eval") || key.equals("listCollections") ||
               key.equals("planCacheClear") || key.equals("configureFailPoint") ||
               key.equals("killCursors") || key.equals("abortTransaction") || 
               key.equals("commitTransaction") || key.equals("startTransaction");
    }
    
    private boolean processWriteOperation(JSONObject attr, SlowQuery slowQuery) {
        String operationType = attr.getString("type");
        
        if (attr.has("ns")) {
            slowQuery.ns = new Namespace(attr.getString("ns"));
        } else {
            return false;
        }
        
        // Set operation type based on the "type" field
        if (operationType.equals("update")) {
            slowQuery.opType = OpType.UPDATE_W;
            incrementOperationStat("update_w");
        } else if (operationType.equals("remove") || operationType.equals("delete")) {
            slowQuery.opType = OpType.REMOVE;
            incrementOperationStat("delete_w");
        } else if (operationType.equals("insert")) {
            slowQuery.opType = OpType.INSERT;
            incrementOperationStat("insert_w");
        } else {
            slowQuery.opType = OpType.CMD;
            incrementOperationStat("write_" + operationType);
        }
        
        setCommonAttributes(attr, slowQuery);
        return true;
    }
    
    private void processExecutionStats(JSONObject attr, SlowQuery slowQuery) {
        if (attr.has("nreturned")) {
            slowQuery.docsExamined = getMetric(attr, "docsExamined");
            slowQuery.keysExamined = getMetric(attr, "keysExamined");
            slowQuery.nreturned = getMetric(attr, "nreturned");
        }
        
        // Handle write operation metrics
        if (attr.has("nMatched") || attr.has("nModified") || attr.has("nUpserted")) {
            Long nModified = getMetric(attr, "nModified");
            Long nUpserted = getMetric(attr, "nUpserted");
            if (nModified != null) {
                slowQuery.nreturned = nModified;
            } else if (nUpserted != null) {
                slowQuery.nreturned = nUpserted;
            }
        }
        
        // Handle delete metrics
        if (attr.has("ndeleted")) {
            slowQuery.nreturned = getMetric(attr, "ndeleted");
        }
        
        // Handle insert metrics
        if (attr.has("ninserted")) {
            slowQuery.nreturned = getMetric(attr, "ninserted");
        }
    }
    
    private void processStorageMetrics(JSONObject attr, SlowQuery slowQuery) {
        if (attr.has("storage")) {
            JSONObject storage = attr.getJSONObject("storage");
            if (storage != null) {
                if (storage.has("bytesRead")) {
                    slowQuery.bytesRead = getMetric(storage, "bytesRead");
                } else if (storage.has("data")) {
                    JSONObject data = storage.getJSONObject("data");
                    if (data.has("bytesRead")) {
                        slowQuery.bytesRead = getMetric(data, "bytesRead");
                    }
                }
            }
        }
    }
    
    private boolean isIncompleteLogEntry(JSONObject attr) {
        // Filter out entries that are clearly incomplete or internal
        if (attr.has("message")) {
            return true;
        }
        
        if (attr.has("elapsedMicros") && !attr.has("command") && !attr.has("q")) {
            return true;
        }
        
        if (attr.has("opId") && attr.length() == 1) {
            return true;
        }
        
        if (attr.has("session") && !attr.has("command") && !attr.has("q")) {
            return true;
        }
        
        return false;
    }
    
    private void incrementOperationStat(String operationType) {
        synchronized (operationTypeStats) {
            operationTypeStats.computeIfAbsent(operationType, k -> new AtomicLong(0)).incrementAndGet();
        }
    }
    
    private static Long getMetric(JSONObject attr, String key) {
        if (attr.has(key)) {
            return Long.valueOf(attr.getInt(key));
        }
        return null;
    }
}