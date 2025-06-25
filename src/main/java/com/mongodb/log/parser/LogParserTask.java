package com.mongodb.log.parser;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

import org.json.JSONException;
import org.json.JSONObject;

import com.mongodb.log.parser.accumulator.Accumulator;
import com.mongodb.log.parser.accumulator.ErrorCodeAccumulator;
import com.mongodb.log.parser.accumulator.IndexStatsAccumulator;
import com.mongodb.log.parser.accumulator.PlanCacheAccumulator;
import com.mongodb.log.parser.accumulator.QueryHashAccumulator;
import com.mongodb.log.parser.accumulator.TransactionAccumulator;

class LogParserTask implements Callable<ProcessingStats> {
	private List<String> linesChunk;
	private final Accumulator accumulator;
	private final PlanCacheAccumulator planCacheAccumulator;
	private final QueryHashAccumulator queryHashAccumulator;
	private final ErrorCodeAccumulator errorCodeAccumulator;
	private final TransactionAccumulator transactionAccumulator;
	private final IndexStatsAccumulator indexStatsAccumulator;
	private final Map<String, AtomicLong> operationTypeStats;
	private final boolean debug;
	private final Set<String> namespaceFilters;
	private final AtomicLong totalFilteredByNamespace;
	private final boolean redactQueries;
	private final LogParser logParser;

	public LogParserTask(List<String> linesChunk, Accumulator accumulator,
			PlanCacheAccumulator planCacheAccumulator, QueryHashAccumulator queryHashAccumulator,
			ErrorCodeAccumulator errorCodeAccumulator,
			TransactionAccumulator transactionAccumulator,
			IndexStatsAccumulator indexStatsAccumulator,
			Map<String, AtomicLong> operationTypeStats, boolean debug,
			Set<String> namespaceFilters, AtomicLong totalFilteredByNamespace, boolean redactQueries, LogParser logParser) {
		this.linesChunk = linesChunk;
		this.accumulator = accumulator;
		this.planCacheAccumulator = planCacheAccumulator;
		this.queryHashAccumulator = queryHashAccumulator;
		this.errorCodeAccumulator = errorCodeAccumulator;
		this.transactionAccumulator = transactionAccumulator;
		this.indexStatsAccumulator = indexStatsAccumulator;
		this.operationTypeStats = operationTypeStats;
		this.debug = debug;
		this.namespaceFilters = namespaceFilters;
		this.totalFilteredByNamespace = totalFilteredByNamespace;
		this.redactQueries = redactQueries;
		this.logParser = logParser;
	}

	@Override
	public ProcessingStats call() throws Exception {
		long localParseErrors = 0;
		long localNoAttr = 0;
		long localNoCommand = 0;
		long localNoNs = 0;
		long localFoundOps = 0;
		long localFilteredByNamespace = 0;

		for (String currentLine : linesChunk) {
			
			JSONObject jo = null;
			try {
				jo = new JSONObject(currentLine);
				
				// Extract timestamp for tracking
				if (jo.has("t") && logParser != null) {
					try {
						Object tObj = jo.get("t");
						if (tObj instanceof JSONObject) {
							JSONObject tJson = (JSONObject) tObj;
							if (tJson.has("$date")) {
								logParser.updateTimestamps(tJson.getString("$date"));
							}
						}
					} catch (Exception e) {
						// Ignore timestamp extraction errors
					}
				}
				
				if (jo.has("attr")) {
	                JSONObject attr = jo.getJSONObject("attr");
	                processErrorCode(jo, attr);
	                processTransaction(jo, attr);
	            }
				
			} catch (JSONException jse) {
				if (currentLine.length() > 0) {
					localParseErrors++;
					if (debug && localParseErrors <= 3) {
						LogParser.logger.warn("Parse error in thread {}: {}", Thread.currentThread().getName(),
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
					LogParser.logger.info("No 'attr' field (thread {}): {}", Thread.currentThread().getName(),
							currentLine.substring(0, Math.min(300, currentLine.length())));
				}
				continue;
			}

			Namespace ns = null;
			SlowQuery slowQuery = new SlowQuery();

			// Check for INDEX operations first (TTL operations, index maintenance)
			if (jo.has("c") && "INDEX".equals(jo.getString("c"))) {
				if (processIndexOperation(jo, attr, slowQuery)) {
					// Apply namespace filtering
					if (!matchesNamespaceFilter(slowQuery.ns)) {
						localFilteredByNamespace++;
						continue;
					}

					synchronized (accumulator) {
						accumulator.accumulate(slowQuery, currentLine);
					}

					// Add to query hash accumulator
					if (queryHashAccumulator != null) {
			            synchronized (queryHashAccumulator) {
			                queryHashAccumulator.accumulate(slowQuery, currentLine);
			            }
			        }

					// Add to index stats accumulator
					if (indexStatsAccumulator != null) {
						synchronized (indexStatsAccumulator) {
							indexStatsAccumulator.accumulate(slowQuery);
						}
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

				// Apply namespace filtering
				if (!matchesNamespaceFilter(ns)) {
					localFilteredByNamespace++;
					continue;
				}

				// Set common attributes
				setCommonAttributes(attr, slowQuery);

				// Extract plan cache information
				extractPlanCacheInfo(attr, slowQuery);

				// Extract read preference and sanitize filter
				extractReadPreferenceAndFilter(attr, slowQuery);

				// Extract replanning information
				extractReplanningInfo(attr, slowQuery);

				JSONObject command = attr.getJSONObject("command");

				// Enhanced operation type detection
				if (processCommandOperation(command, slowQuery, ns)) {
					// Handle execution stats and storage metrics
					processExecutionStats(attr, slowQuery);
					processStorageMetrics(attr, slowQuery);

					slowQuery.durationMillis = getMetric(attr, "durationMillis");

					synchronized (accumulator) {
						accumulator.accumulate(slowQuery, currentLine);
					}

					if (queryHashAccumulator != null) {
					    synchronized (queryHashAccumulator) {
					        queryHashAccumulator.accumulate(slowQuery, currentLine);
					    }
					}

					// Add to index stats accumulator
					if (indexStatsAccumulator != null) {
						synchronized (indexStatsAccumulator) {
							indexStatsAccumulator.accumulate(slowQuery);
						}
					}

					// Add to plan cache accumulator if enabled and has plan cache key
					if (planCacheAccumulator != null && slowQuery.planCacheKey != null) {
						synchronized (planCacheAccumulator) {
							planCacheAccumulator.accumulate(slowQuery, currentLine);
						}
					}

					localFoundOps++;
				}

			} else if (attr.has("type")) {
				// This is a WRITE operation log entry
				if (processWriteOperation(attr, slowQuery)) {
					// Apply namespace filtering
					if (!matchesNamespaceFilter(slowQuery.ns)) {
						localFilteredByNamespace++;
						continue;
					}

					extractPlanCacheInfo(attr, slowQuery);
					extractReadPreferenceAndFilter(attr, slowQuery);
					extractReplanningInfo(attr, slowQuery);
					processExecutionStats(attr, slowQuery);
					processStorageMetrics(attr, slowQuery);

					slowQuery.durationMillis = getMetric(attr, "durationMillis");

					synchronized (accumulator) {
						accumulator.accumulate(slowQuery, currentLine);
					}

					// Add to query hash accumulator
					if (queryHashAccumulator != null) {
						synchronized (queryHashAccumulator) {
							queryHashAccumulator.accumulate(slowQuery, currentLine);
						}
					}

					// Add to index stats accumulator
					if (indexStatsAccumulator != null) {
						synchronized (indexStatsAccumulator) {
							indexStatsAccumulator.accumulate(slowQuery);
						}
					}

					// Add to plan cache accumulator if enabled and has plan cache key
					if (planCacheAccumulator != null && slowQuery.planCacheKey != null) {
						synchronized (planCacheAccumulator) {
							planCacheAccumulator.accumulate(slowQuery, currentLine);
						}
					}

					localFoundOps++;
				}

			}
		}

		// Update the global filtered counter
		if (totalFilteredByNamespace != null) {
			totalFilteredByNamespace.addAndGet(localFilteredByNamespace);
		}

		this.linesChunk = null;
		
		return new ProcessingStats(localParseErrors, localNoAttr, localNoCommand, localNoNs, localFoundOps);
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
				String regex = filter.replace(".", "\\.").replace("*", ".*");
				if (fullNamespace.matches(regex)) {
					return true;
				}
			}
		}

		return false;
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

	// Extract read preference and sanitize filter from the command
	private void extractReadPreferenceAndFilter(JSONObject attr, SlowQuery slowQuery) {
	    try {
	        if (attr.has("command")) {
	            JSONObject command = attr.getJSONObject("command");

	            // Extract read preference - convert to string for storage
	            if (command.has("$readPreference")) {
	                Object readPrefObj = command.get("$readPreference");
	                if (readPrefObj instanceof JSONObject) {
	                    JSONObject readPref = (JSONObject) readPrefObj;
	                    // Convert the JSONObject to a string representation for storage
	                    slowQuery.readPreference = readPref.toString();
	                } else if (readPrefObj instanceof String) {
	                    slowQuery.readPreference = (String) readPrefObj;
	                }
	            }

	            // Extract and optionally sanitize filter for find operations
	            if (command.has("filter")) {
	                Object filterObj = command.get("filter");
	                if (filterObj instanceof JSONObject) {
	                    JSONObject filter = (JSONObject) filterObj;
	                    slowQuery.sanitizedFilter = LogRedactionUtil.sanitizeFilter(filter, redactQueries);
	                }
	            }

	            // Extract and optionally sanitize query for other operations that use "q" field
	            if (command.has("q")) {
	                Object queryObj = command.get("q");
	                if (queryObj instanceof JSONObject) {
	                    JSONObject query = (JSONObject) queryObj;
	                    slowQuery.sanitizedFilter = LogRedactionUtil.sanitizeFilter(query, redactQueries);
	                }
	            }

	            // For aggregate operations, try to extract the first $match stage
	            if (command.has("pipeline") && slowQuery.sanitizedFilter == null) {
	                try {
	                    Object pipelineObj = command.get("pipeline");
	                    if (pipelineObj instanceof org.json.JSONArray) {
	                        org.json.JSONArray pipeline = (org.json.JSONArray) pipelineObj;
	                        for (int i = 0; i < pipeline.length(); i++) {
	                            Object stageObj = pipeline.get(i);
	                            if (stageObj instanceof JSONObject) {
	                                JSONObject stage = (JSONObject) stageObj;
	                                if (stage.has("$match")) {
	                                    Object matchObj = stage.get("$match");
	                                    if (matchObj instanceof JSONObject) {
	                                        JSONObject matchFilter = (JSONObject) matchObj;
	                                        slowQuery.sanitizedFilter = LogRedactionUtil.sanitizeFilter(matchFilter, redactQueries);
	                                        break; // Use the first $match stage
	                                    }
	                                }
	                            }
	                        }
	                    }
	                } catch (Exception e) {
	                    if (debug) {
	                        LogParser.logger.debug("Error extracting $match from pipeline: {}", e.getMessage());
	                    }
	                }
	            }
	        }
	        
	        // Handle getMore operations - extract query from originatingCommand
	        if (attr.has("originatingCommand") && slowQuery.sanitizedFilter == null) {
	            try {
	                JSONObject originatingCommand = attr.getJSONObject("originatingCommand");
	                
	                // Extract filter from find operation in originatingCommand
	                if (originatingCommand.has("filter")) {
	                    Object filterObj = originatingCommand.get("filter");
	                    if (filterObj instanceof JSONObject) {
	                        JSONObject filter = (JSONObject) filterObj;
	                        slowQuery.sanitizedFilter = LogRedactionUtil.sanitizeFilter(filter, redactQueries);
	                    }
	                }
	                
	                // Also extract read preference from originatingCommand if not already set
	                if (slowQuery.readPreference == null && originatingCommand.has("$readPreference")) {
	                    Object readPrefObj = originatingCommand.get("$readPreference");
	                    if (readPrefObj instanceof JSONObject) {
	                        JSONObject readPref = (JSONObject) readPrefObj;
	                        slowQuery.readPreference = readPref.toString();
	                    } else if (readPrefObj instanceof String) {
	                        slowQuery.readPreference = (String) readPrefObj;
	                    }
	                }
	            } catch (Exception e) {
	                if (debug) {
	                    LogParser.logger.debug("Error extracting query from originatingCommand: {}", e.getMessage());
	                }
	            }
	        }
	    } catch (JSONException e) {
	        if (debug) {
	            LogParser.logger.debug("Error extracting read preference and filter: {}", e.getMessage());
	        }
	    }
	}

	// Extract replanning information from the log attributes
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
		
		if (attr.has("nShards")) {
			slowQuery.nShards = getMetric(attr, "nShards");
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
			
			// For getMore operations, we'll try to extract the query from originatingCommand
			// This will be handled in extractReadPreferenceAndFilter method which has access to attr
			
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
		return key.equals("drop") || key.equals("dropDatabase") || key.equals("dropIndexes")
				|| key.equals("createIndexes") || key.equals("collMod") || key.equals("renameCollection")
				|| key.equals("validate") || key.equals("compact") || key.equals("reIndex") || key.equals("explain")
				|| key.equals("currentOp") || key.equals("killOp") || key.equals("fsync") || key.equals("eval")
				|| key.equals("listCollections") || key.equals("planCacheClear") || key.equals("configureFailPoint")
				|| key.equals("killCursors") || key.equals("abortTransaction") || key.equals("commitTransaction")
				|| key.equals("startTransaction");
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
	
	private void processErrorCode(JSONObject jo, JSONObject attr) {
	    try {
	        // Handle explicit error objects in attr
	        if (attr.has("error")) {
	            JSONObject error = attr.getJSONObject("error");
	            
	            String codeName = null;
	            Integer errorCode = null;
	            String errorMessage = null;
	            
	            if (error.has("codeName")) {
	                codeName = error.getString("codeName");
	            }
	            
	            if (error.has("code")) {
	                errorCode = error.getInt("code");
	            }
	            
	            if (error.has("errmsg")) {
	                errorMessage = error.getString("errmsg");
	            }
	            
	            if (codeName != null) {
	                errorCodeAccumulator.accumulate(codeName, errorCode, errorMessage);
	            }
	        }
	        
	        // Handle client disconnect interruption messages
	        if (jo.has("msg")) {
	            String msg = jo.getString("msg");
	            if ("Interrupted operation as its client disconnected".equals(msg)) {
	                // Extract operation ID if available for context
	                String contextInfo = null;
	                if (attr.has("opId")) {
	                    contextInfo = "opId: " + attr.get("opId").toString();
	                }
	                
	                errorCodeAccumulator.accumulate("InterruptedByClientDisconnect", null, 
	                    "Interrupted operation as its client disconnected" + 
	                    (contextInfo != null ? " (" + contextInfo + ")" : ""));
	            }
	        }
	    } catch (Exception e) {
	        if (debug) {
	            LogParser.logger.debug("Error processing error code: {}", e.getMessage());
	        }
	    }
	}
	
	/**
	 * Process transaction log entries
	 */
	private void processTransaction(JSONObject jo, JSONObject attr) {
	    try {
	        if (jo.has("c") && "TXN".equals(jo.getString("c")) && 
	            jo.has("msg") && "transaction".equals(jo.getString("msg"))) {
	            
	            Integer txnRetryCounter = null;
	            String terminationCause = null;
	            String commitType = null;
	            Long durationMillis = null;
	            Long commitDurationMicros = null;
	            Long timeActiveMicros = null;
	            Long timeInactiveMicros = null;
	            
	            // Extract txnRetryCounter from parameters if available
	            if (attr.has("parameters")) {
	                JSONObject parameters = attr.getJSONObject("parameters");
	                if (parameters.has("txnRetryCounter")) {
	                    txnRetryCounter = parameters.getInt("txnRetryCounter");
	                }
	            }
	            
	            // Extract direct fields from attr
	            if (attr.has("terminationCause")) {
	                terminationCause = attr.getString("terminationCause");
	            }
	            
	            if (attr.has("commitType")) {
	                commitType = attr.getString("commitType");
	            }
	            
	            if (attr.has("durationMillis")) {
	                durationMillis = attr.getLong("durationMillis");
	            }
	            
	            if (attr.has("commitDurationMicros")) {
	                commitDurationMicros = attr.getLong("commitDurationMicros");
	            }
	            
	            if (attr.has("timeActiveMicros")) {
	                timeActiveMicros = attr.getLong("timeActiveMicros");
	            }
	            
	            if (attr.has("timeInactiveMicros")) {
	                timeInactiveMicros = attr.getLong("timeInactiveMicros");
	            }
	            
	            // Only accumulate if we have at least one meaningful field
	            if (txnRetryCounter != null || terminationCause != null || 
	                commitType != null || durationMillis != null) {
	                
	                synchronized (transactionAccumulator) {
	                    transactionAccumulator.accumulate(txnRetryCounter, terminationCause, 
	                        commitType, durationMillis, commitDurationMicros, 
	                        timeActiveMicros, timeInactiveMicros);
	                }
	            }
	        }
	    } catch (Exception e) {
	        if (debug) {
	            LogParser.logger.debug("Error processing transaction: {}", e.getMessage());
	        }
	    }
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