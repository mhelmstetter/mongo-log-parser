package com.mongodb.log.parser;

public class SlowQuery {
	
	public Namespace ns = null;
	public OpType opType;
	public Long docsExamined = null;
	public Long keysExamined = null;
	public Long nreturned = null;
	public Long durationMillis = null;
	public Long reslen = null;
	public Long bytesRead = null;
	public Long bytesWritten = null;
	public String timestamp = null;
	public String queryHash = null;
	public String appName = null;
	public String remote = null;
	public String planCacheKey = null;
	public String planSummary = null;
	public Long planningTimeMicros = null;
	public Boolean replanned = null;
	public String replanReason = null;
	public Boolean fromMultiPlanner = null;
	public Boolean hasSortStage = null;
	public String readPreference = null;
	public String readPreferenceTags = null;
	public String sanitizedFilter = null;
	public Long nShards = null;
	public Long writeConflicts = null;
	public Boolean isChangeStream = null;
	public Long cpuNanos = null;
	public Boolean fromMongos = null;
	public Long remoteOpWaitMillis = null;
	public Long nMatched = null;
	public Long nModified = null;
	public Long nInserted = null;
	public Long nDeleted = null;
	public Long nUpserted = null;
	public Long keysInserted = null;
	public Long keysDeleted = null;
	public Long txnBytesDirty = null;
	public Long flowControlWaitMicros = null;
	public Long writeConcernWaitMillis = null;
	// Mirrored reads are sent by a primary to secondaries to warm their caches
	public Boolean mirrored = null;
	// Statements in a batched write command (updates/documents/deletes array length)
	public Long batchSize = null;
	// Process the log line came from (see NodeIdentity)
	public String node = null;
	// Application tag parsed from the operation's comment (see CommentTag); null until extracted
	public CommentTag commentTag = null;
}