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
	public String queryHash = null;
	public String appName = null;
	public String remote = null;
	public String planCacheKey = null;
	public String planSummary = null;
	public Long planningTimeMicros = null;
	public Boolean replanned = null;
	public String replanReason = null;
	public Boolean fromMultiPlanner = null;
	public String readPreference = null;
	public String readPreferenceTags = null;
	public String sanitizedFilter = null;
	public Long nShards = null;
	public Long writeConflicts = null;
	public Boolean isChangeStream = null;
}