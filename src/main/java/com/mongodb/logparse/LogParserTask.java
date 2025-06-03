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
    private final File file;
    private final Map<String, AtomicLong> operationTypeStats;
    private final boolean debug;

    public LogParserTask(List<String> linesChunk, File file, Accumulator accumulator, 
                        Map<String, AtomicLong> operationTypeStats, boolean debug) {
        this.linesChunk = linesChunk;
        this.file = file;
        this.accumulator = accumulator;
        this.operationTypeStats = operationTypeStats;
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
                    // Log first few parse errors for debugging
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
                // Log sample of non-attr entries
                if (debug && localNoAttr <= 3) {
                    LogParser.logger.info("No 'attr' field (thread {}): {}", 
                               Thread.currentThread().getName(),
                               currentLine.substring(0, Math.min(300, currentLine.length())));
                }
                continue;
            }
            
            Namespace ns = null;
            SlowQuery slowQuery = new SlowQuery();
            
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
                
                // Enhanced operation type detection
                if (command.has("find")) {
                    Object findVal = command.get("find");
                    if (findVal instanceof String) {
                        String find = (String) findVal;
                        slowQuery.opType = OpType.QUERY;
                        ns.setCollectionName(find);
                    } else {
                        slowQuery.opType = OpType.QUERY;
                    }
                    incrementOperationStat("find");
                } else if (command.has("aggregate")) {
                    slowQuery.opType = OpType.AGGREGATE;
                    Object aggregateVal = command.get("aggregate");
                    if (aggregateVal instanceof String) {
                        String coll = (String) aggregateVal;
                        if (!coll.equals("1")) {  // aggregate: 1 means database-level aggregation
                            ns.setCollectionName(coll);
                        }
                    }
                    // If aggregate value is not a string (e.g., array), we'll use the ns from the log
                    incrementOperationStat("aggregate");
                } else if (command.has("findAndModify")) {
                    slowQuery.opType = OpType.FIND_AND_MODIFY;
                    Object findAndModifyVal = command.get("findAndModify");
                    if (findAndModifyVal instanceof String) {
                        ns.setCollectionName((String) findAndModifyVal);
                    }
                    incrementOperationStat("findAndModify");
                } else if (command.has("update")) {
                    slowQuery.opType = OpType.UPDATE;
                    Object updateVal = command.get("update");
                    if (updateVal instanceof String) {
                        ns.setCollectionName((String) updateVal);
                    }
                    incrementOperationStat("update");
                } else if (command.has("insert")) {
                    slowQuery.opType = OpType.INSERT;
                    Object insertVal = command.get("insert");
                    if (insertVal instanceof String) {
                        ns.setCollectionName((String) insertVal);
                    }
                    incrementOperationStat("insert");
                } else if (command.has("delete")) {
                    slowQuery.opType = OpType.REMOVE;
                    Object deleteVal = command.get("delete");
                    if (deleteVal instanceof String) {
                        ns.setCollectionName((String) deleteVal);
                    }
                    incrementOperationStat("delete");
                } else if (command.has("getMore")) {
                    slowQuery.opType = OpType.GETMORE;
                    if (command.has("collection")) {
                        Object collectionVal = command.get("collection");
                        if (collectionVal instanceof String) {
                            ns.setCollectionName((String) collectionVal);
                        }
                    }
                    incrementOperationStat("getMore");
                } else if (command.has("count")) {
                    slowQuery.opType = OpType.COUNT;
                    // Handle both count: "collection" and count: 1 formats
                    Object countVal = command.get("count");
                    if (countVal instanceof String) {
                        ns.setCollectionName((String) countVal);
                    }
                    incrementOperationStat("count");
                } else if (command.has("distinct")) {
                    slowQuery.opType = OpType.DISTINCT;
                    Object distinctVal = command.get("distinct");
                    if (distinctVal instanceof String) {
                        ns.setCollectionName((String) distinctVal);
                    }
                    incrementOperationStat("distinct");
                }
                // NEW: Handle WRITE-level operations that have command.q structure
                else if (command.has("q")) {
                    // This is a WRITE-level operation (remove, update, etc.)
                    // The operation type is determined by attr.type, not the command structure
                    
                    if (attr.has("type")) {
                        String writeType = attr.getString("type");
                        
                        if (writeType.equals("remove")) {
                            slowQuery.opType = OpType.REMOVE;
                            incrementOperationStat("delete_write");
                        } else if (writeType.equals("update")) {
                            slowQuery.opType = OpType.UPDATE_W;
                            incrementOperationStat("update_write");
                        } else if (writeType.equals("insert")) {
                            slowQuery.opType = OpType.INSERT;
                            incrementOperationStat("insert_write");
                        } else {
                            slowQuery.opType = OpType.CMD;
                            incrementOperationStat("write_" + writeType);
                        }
                        
                        // Handle common WRITE operation metrics
                        if (attr.has("planSummary")) {
                            // This indicates it's a query-like operation, extract metrics
                            if (attr.has("keysExamined")) {
                                slowQuery.keysExamined = LogParser.getMetric(attr, "keysExamined");
                            }
                            
                            if (attr.has("docsExamined")) {
                                slowQuery.docsExamined = LogParser.getMetric(attr, "docsExamined");
                            }
                            
                            // For delete operations, use ndeleted as nreturned
                            if (attr.has("ndeleted")) {
                                slowQuery.nreturned = LogParser.getMetric(attr, "ndeleted");
                            }
                            
                            // For update operations, use nModified
                            if (attr.has("nModified")) {
                                slowQuery.nreturned = LogParser.getMetric(attr, "nModified");
                            }
                            
                            // For insert operations, use ninserted
                            if (attr.has("ninserted")) {
                                slowQuery.nreturned = LogParser.getMetric(attr, "ninserted");
                            }
                        }
                        
                        // Extract standard metrics
                        if (attr.has("reslen")) {
                            slowQuery.reslen = LogParser.getMetric(attr, "reslen");
                        }
                        
                        if (attr.has("remote")) {
                            slowQuery.remote = attr.getString("remote");
                        }
                        
                        // Handle storage metrics for WRITE operations
                        if (attr.has("storage")) {
                            JSONObject storage = attr.getJSONObject("storage");
                            if (storage != null && storage.has("data")) {
                                JSONObject data = storage.getJSONObject("data");
                                if (data.has("bytesRead")) {
                                    slowQuery.bytesRead = LogParser.getMetric(data, "bytesRead");
                                }
                            }
                        }
                        
                        slowQuery.durationMillis = LogParser.getMetric(attr, "durationMillis");
                        
                        synchronized (accumulator) {
                            accumulator.accumulate(slowQuery);
                        }
                        localFoundOps++;
                        
                    } else {
                        // command.q exists but no type - shouldn't happen in modern logs
                        slowQuery.opType = OpType.CMD;
                        incrementOperationStat("unknown_q_command");
                        
                        slowQuery.durationMillis = LogParser.getMetric(attr, "durationMillis");
                        
                        synchronized (accumulator) {
                            accumulator.accumulate(slowQuery);
                        }
                        localFoundOps++;
                    }
                }
                else if (command.has("remove")) {  // Legacy format
                    slowQuery.opType = OpType.REMOVE;
                    incrementOperationStat("remove");
                } else if (command.has("u")) {  // Write operations format
                    slowQuery.opType = OpType.UPDATE_W;
                    incrementOperationStat("update_w");
                } else if (command.has("d")) {  // Delete operations format  
                    slowQuery.opType = OpType.REMOVE;
                    incrementOperationStat("delete_w");
                } else if (command.has("i")) {  // Insert operations format
                    slowQuery.opType = OpType.INSERT;
                    incrementOperationStat("insert_w");
                } else {
                    // Check for MongoDB shard operations first
                    boolean foundOperation = false;
                    
                    if (command.has("_shardsvrMoveRange")) {
                        slowQuery.opType = OpType.CMD;
                        incrementOperationStat("shardMigration");
                        foundOperation = true;
                    } else if (command.has("_shardsvrCloneCatalogData")) {
                        slowQuery.opType = OpType.CMD;
                        incrementOperationStat("shardClone");
                        foundOperation = true;
                    } else if (command.has("_shardsvrCommitChunkMigration")) {
                        slowQuery.opType = OpType.CMD;
                        incrementOperationStat("shardCommit");
                        foundOperation = true;
                    } else {
                        // Check for other common operations that might be missed
                        Iterator<String> keys = command.keys();
                        while (keys.hasNext()) {
                            String key = keys.next();
                            if (key.equals("drop") || key.equals("dropDatabase") || key.equals("dropIndexes") ||
                                key.equals("createIndexes") || key.equals("collMod") || key.equals("renameCollection") ||
                                key.equals("validate") || key.equals("compact") || key.equals("reIndex") ||
                                key.equals("explain") || key.equals("profile") || key.equals("currentOp") ||
                                key.equals("killOp") || key.equals("fsync") || key.equals("eval") ||
                                key.equals("listCollections") || key.equals("listIndexes") || key.equals("isMaster") ||
                                key.equals("ping") || key.equals("buildInfo") || key.equals("serverStatus") ||
                                key.equals("replSetGetStatus") || key.equals("hello") || key.equals("collStats") ||
                                key.equals("dbStats") || key.equals("replSetUpdatePosition") || key.equals("getParameter") ||
                                key.equals("setParameter") || key.equals("planCacheClear") || key.equals("configureFailPoint") ||
                                key.equals("getCmdLineOpts") || key.equals("logRotate") || key.equals("shutdown") ||
                                key.equals("killCursors") || key.equals("endSessions") || key.equals("startSession") ||
                                key.equals("abortTransaction") || key.equals("commitTransaction") || key.equals("startTransaction")) {
                                
                                // These are administrative/system operations, mark as CMD
                                slowQuery.opType = OpType.CMD;
                                incrementOperationStat(key);
                                foundOperation = true;
                                break;
                            }
                        }
                    }
                    
                    if (!foundOperation) {
                        // Log unknown command structure for debugging - but only first few per thread
                        if (debug && debugCount < 10) {
                            LogParser.logger.warn("Unknown command structure (thread {}) #{}: {}", 
                                       Thread.currentThread().getName(), debugCount + 1,
                                       command.toString().substring(0, Math.min(500, command.toString().length())));
                            debugCount++;
                        }
                        
                        // Still count it as an operation, but mark as unknown
                        slowQuery.opType = OpType.CMD;  // Treat unknown as command for now
                        incrementOperationStat("unknown");
                    }
                }
                
                // Handle storage metrics
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
                    // Handle execution stats
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
            } else if (attr.has("type")) {
                // This might be a WRITE operation log entry
                String operationType = attr.getString("type");
                
                if (attr.has("ns")) {
                    ns = new Namespace(attr.getString("ns"));
                    slowQuery.ns = ns;
                } else {
                    localNoNs++;
                    continue;
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
                
                // Handle common WRITE operation fields
                if (attr.has("queryHash")) {
                    slowQuery.queryHash = attr.getString("queryHash");
                }
                
                if (attr.has("reslen")) {
                    slowQuery.reslen = LogParser.getMetric(attr, "reslen");
                }
                
                if (attr.has("remote")) {
                    slowQuery.remote = attr.getString("remote");
                }
                
                if (attr.has("keysExamined")) {
                    slowQuery.keysExamined = LogParser.getMetric(attr, "keysExamined");
                }
                
                if (attr.has("docsExamined")) {
                    slowQuery.docsExamined = LogParser.getMetric(attr, "docsExamined");
                }
                
                if (attr.has("nMatched") || attr.has("nModified") || attr.has("nUpserted")) {
                    // For write operations, use nModified or nUpserted as nreturned equivalent
                    Long nModified = LogParser.getMetric(attr, "nModified");
                    Long nUpserted = LogParser.getMetric(attr, "nUpserted");
                    if (nModified != null) {
                        slowQuery.nreturned = nModified;
                    } else if (nUpserted != null) {
                        slowQuery.nreturned = nUpserted;
                    }
                }
                
                // Handle storage metrics
                if (attr.has("storage")) {
                    JSONObject storage = attr.getJSONObject("storage");
                    if (storage != null && storage.has("data")) {
                        JSONObject data = storage.getJSONObject("data");
                        if (data.has("bytesRead")) {
                            slowQuery.bytesRead = LogParser.getMetric(data, "bytesRead");
                        }
                    }
                }
                
                slowQuery.durationMillis = LogParser.getMetric(attr, "durationMillis");
                
                synchronized (accumulator) {
                    accumulator.accumulate(slowQuery);
                }
                localFoundOps++;
            } else {
                // Check if this is an incomplete/internal entry that should be filtered
                if (isIncompleteLogEntry(attr)) {
                    // Don't count these as "no command" - they're filtered internal messages
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
        
        // Log summary for this chunk
        LogParser.logger.info("Thread {} processed {} lines: {} ops found, {} parse errors, {} no attr, {} no command, {} no ns", 
                   Thread.currentThread().getName(), linesChunk.size(), localFoundOps, 
                   localParseErrors, localNoAttr, localNoCommand, localNoNs);
        
        this.linesChunk = null;
        return new ProcessingStats(localParseErrors, localNoAttr, localNoCommand, localNoNs, localFoundOps);
    }
    
    // NEW: Method to check for incomplete log entries
    private boolean isIncompleteLogEntry(JSONObject attr) {
        // Filter out entries that are clearly incomplete or internal
        
        // WiredTiger storage engine messages
        if (attr.has("message")) {
            try {
                JSONObject message = attr.getJSONObject("message");
                if (message.has("msg")) {
                    return true;
                }
            } catch (JSONException e) {
                // If message is not a JSON object, it might be a string - still filter it
                return true;
            }
        }
        
        // Performance timing without operation context
        if (attr.has("elapsedMicros") && !attr.has("command") && !attr.has("q")) {
            return true;
        }
        
        // Standalone operation IDs without context
        if (attr.has("opId") && attr.length() == 1) {
            return true;
        }
        
        // Sessions without operations
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
}