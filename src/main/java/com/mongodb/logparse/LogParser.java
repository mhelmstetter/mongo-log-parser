package com.mongodb.logparse;

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
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipInputStream;

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
@Command(name = "logParser", mixinStandardHelpOptions = true, version = "1.0", description = "Parse MongoDB log files and generate performance reports")
public class LogParser implements Callable<Integer> {

	static final Logger logger = LoggerFactory.getLogger(LogParser.class);

	// Add these fields to LogParser class
	private AtomicLong totalParseErrors = new AtomicLong(0);
	private AtomicLong totalNoAttr = new AtomicLong(0);
	private AtomicLong totalNoCommand = new AtomicLong(0);
	private AtomicLong totalNoNs = new AtomicLong(0);
	private AtomicLong totalFoundOps = new AtomicLong(0);
	private Map<String, AtomicLong> operationTypeStats = new HashMap<>();

	@Option(names = { "-f", "--files" }, description = "MongoDB log file(s)", required = true)
	private String[] fileNames;

	@Option(names = { "-q", "--queries" }, description = "Parse queries")
	private boolean parseQueries = false;

	@Option(names = { "--replay" }, description = "Replay operations")
	private boolean replay = false;

	@Option(names = { "--uri" }, description = "MongoDB connection string URI")
	private String uri;

	@Option(names = { "-c", "--csv" }, description = "CSV output file")
	private String csvOutputFile;

	@Option(names = { "--debug" }, description = "Enable debug logging")
	private boolean debug = false;

	// NEW: Ignored lines analysis options
	@Option(names = {"--ignored-output"}, description = "Output file for ignored lines analysis")
	private String ignoredOutputFile;

	@Option(names = {"--categorize-ignored"}, description = "Categorize and count ignored line types")
	private boolean categorizeIgnored = false;

	@Option(names = {"--sample-ignored-rate"}, description = "Sample rate for ignored lines (1-100, default 1)")
	private int ignoredSampleRate = 1;

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

	// NEW: Fields for ignored lines analysis
	private Map<String, AtomicLong> ignoredCategories = new HashMap<>();
	private List<String> sampledIgnoredLines = new ArrayList<>();
	private AtomicLong ignoredLineCounter = new AtomicLong(0);
	private PrintWriter ignoredWriter = null;

	// Improved ignore list - more specific filtering
	private static List<String> ignore = Arrays.asList("\"c\":\"NETWORK\"", "\"c\":\"ACCESS\"", "\"c\":\"CONNPOOL\"",
			"\"c\":\"STORAGE\"", "\"c\":\"SHARDING\"", "\"c\":\"CONTROL\"", "\"hello\":1", "\"isMaster\":1",
			"\"ping\":1", "\"saslContinue\":1", "\"replSetHeartbeat\":\"", "\"serverStatus\":1",
			"\"replSetGetStatus\":1", "\"buildInfo\"", "\"getParameter\":", "\"getCmdLineOpts\":1", "\"logRotate\":\"",
			"\"getDefaultRWConcern\":1", "\"listDatabases\":1", "\"endSessions\":", "\"ctx\":\"TTLMonitor\"",
			"\"ns\":\"local.clustermanager\"", "\"ns\":\"local.oplog.rs\"", "\"ns\":\"config.system.sessions\"",
			"\"ns\":\"config.mongos\"", "\"dbstats\":1", "\"listIndexes\":\"", "\"collStats\":\"", "\"profile\":0",
			"\"profile\":1", "\"profile\":2", "\"ns\":\"local.oplog.rs\"", "replSetUpdatePosition");

	public LogParser() {
		accumulator = new Accumulator();
	}

	@Override
	public Integer call() throws Exception {
		logger.info("Starting MongoDB log parsing with {} processors", Runtime.getRuntime().availableProcessors());

		read();

		logger.info("Parsing complete. Total processed: {}, Ignored: {}", processedCount, ignoredCount);

		// Report operation type statistics
		logger.info("Operation type breakdown:");
		operationTypeStats.entrySet().stream()
				.sorted((e1, e2) -> Long.compare(e2.getValue().get(), e1.getValue().get()))
				.forEach(entry -> logger.info("  {}: {}", entry.getKey(), entry.getValue()));

		// NEW: Report ignored lines analysis
		reportIgnoredAnalysis();

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
		logger.info("MIME type guess: {}", guess);

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

		// NEW: Initialize ignored lines output if requested
		if (ignoredOutputFile != null) {
			try {
				ignoredWriter = new PrintWriter(new FileWriter(ignoredOutputFile));
			} catch (IOException e) {
				logger.error("Failed to create ignored lines output file: {}", ignoredOutputFile, e);
				ignoredOutputFile = null; // Disable if we can't create the file
			}
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

		// Add sampling of ignored lines for analysis
		int ignoredSamples = 0;

		while ((currentLine = in.readLine()) != null) {
			lineNum++;

			if (ignoreLine(currentLine)) {
				ignoredCount++;

				// NEW: Process ignored line for analysis
				processIgnoredLine(currentLine);

				// Sample some ignored lines for analysis
				if (debug && ignoredSamples < 10) {
					logger.info("IGNORED LINE SAMPLE {}: {}", ignoredSamples + 1,
							currentLine.substring(0, Math.min(300, currentLine.length())));
					ignoredSamples++;
				}
				continue;
			}

			lines.add(currentLine);
			processedCount++;

			if (lines.size() >= 25000) {
				// Submit the chunk for processing
				completionService.submit(
						new LogParserTask(new ArrayList<>(lines), file, accumulator, operationTypeStats, debug));
				submittedTasks++;
				lines.clear();
			}

			if (lineNum % 50000 == 0) {
				logger.info("Read {} lines (ignored: {}, queued for processing: {})", lineNum, ignoredCount,
						processedCount);
			}
		}

		// Submit the last chunk if there are remaining lines
		if (!lines.isEmpty()) {
			completionService
					.submit(new LogParserTask(new ArrayList<>(lines), file, accumulator, operationTypeStats, debug));
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

		// NEW: Close ignored lines writer
		if (ignoredWriter != null) {
			ignoredWriter.close();
		}

		in.close();
		long end = System.currentTimeMillis();
		long dur = (end - start);

		// Enhanced logging with detailed statistics
		logger.info("File processing complete - Duration: {}ms", dur);
		logger.info("Lines read: {}, Ignored: {}, Processed: {}", lineNum, ignoredCount, processedCount);
		logger.info("Parse errors: {}, No attr: {}, No command: {}, No namespace: {}", totalParseErrors.get(),
				totalNoAttr.get(), totalNoCommand.get(), totalNoNs.get());
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

	// Improved ignoreLine method to be more selective
	private static boolean ignoreLine(String line) {
		// Don't ignore lines that don't look like JSON at all
		if (!line.trim().startsWith("{")) {
			return true;
		}

		// Don't ignore lines that contain our target operations
		if (line.contains("\"find\":") || line.contains("\"aggregate\":") || line.contains("\"update\":")
				|| line.contains("\"insert\":") || line.contains("\"delete\":") || line.contains("\"findAndModify\":")
				|| line.contains("\"getMore\":") || line.contains("\"count\":") || line.contains("\"distinct\":")) {
			return false;  // Always process these
		}

		// Apply the ignore list
		for (String keyword : ignore) {
			if (line.contains(keyword)) {
				return true;
			}
		}
		return false;
	}

	// NEW: Method to categorize ignored lines
	private String categorizeIgnoredLine(String line) {
		// Network operations
		if (line.contains("\"c\":\"NETWORK\"")) {
			if (line.contains("Connection accepted")) {
				return "NETWORK_CONNECTION_ACCEPTED";
			} else if (line.contains("Connection ended")) {
				return "NETWORK_CONNECTION_ENDED";
			} else if (line.contains("client metadata")) {
				return "NETWORK_CLIENT_METADATA";
			} else if (line.contains("TLS handshake")) {
				return "NETWORK_TLS_HANDSHAKE";
			} else if (line.contains("Interrupted operation as its client disconnected")) {
				return "NETWORK_CLIENT_DISCONNECTED";
			} else if (line.contains("connectionCount")) {
				return "NETWORK_CONNECTION_COUNT";
			} else {
				return "NETWORK_OTHER";
			}
		}
		
		// Access/Authentication operations
		if (line.contains("\"c\":\"ACCESS\"")) {
			if (line.contains("Successfully authenticated")) {
				return "ACCESS_AUTH_SUCCESS";
			} else if (line.contains("Auth metrics report")) {
				return "ACCESS_AUTH_METRICS";
			} else if (line.contains("Failed to authenticate")) {
				return "ACCESS_AUTH_FAILED";
			} else {
				return "ACCESS_OTHER";
			}
		}
		
		// Storage operations
		if (line.contains("\"c\":\"STORAGE\"")) {
			if (line.contains("WiredTiger")) {
				return "STORAGE_WIREDTIGER";
			} else {
				return "STORAGE_OTHER";
			}
		}
		
		// Control operations
		if (line.contains("\"c\":\"CONTROL\"")) {
			return "CONTROL_OPERATIONS";
		}
		
		// Connection pool
		if (line.contains("\"c\":\"CONNPOOL\"")) {
			return "CONNPOOL_OPERATIONS";
		}
		
		// Sharding operations
		if (line.contains("\"c\":\"SHARDING\"")) {
			return "SHARDING_OPERATIONS";
		}
		
		// Administrative commands
		if (line.contains("\"hello\":1") || line.contains("\"isMaster\":1")) {
			return "ADMIN_HELLO_ISMASTER";
		}
		
		if (line.contains("\"ping\":1")) {
			return "ADMIN_PING";
		}
		
		if (line.contains("\"serverStatus\":1")) {
			return "ADMIN_SERVER_STATUS";
		}
		
		if (line.contains("\"replSetHeartbeat\"")) {
			return "REPLICATION_HEARTBEAT";
		}
		
		if (line.contains("\"replSetGetStatus\":1")) {
			return "REPLICATION_STATUS";
		}
		
		// Profile operations
		if (line.contains("\"profile\":")) {
			return "PROFILING_OPERATIONS";
		}
		
		// TTL Monitor
		if (line.contains("\"ctx\":\"TTLMonitor\"")) {
			return "TTL_MONITOR";
		}
		
		// Session operations
		if (line.contains("\"endSessions\"")) {
			return "SESSION_END";
		}
		
		// Database stats
		if (line.contains("\"dbstats\":1") || line.contains("\"collStats\"")) {
			return "DATABASE_STATS";
		}
		
		// Index operations
		if (line.contains("\"listIndexes\"")) {
			return "INDEX_OPERATIONS";
		}
		
		// Build info and parameters
		if (line.contains("\"buildInfo\"") || line.contains("\"getParameter\"") || 
			line.contains("\"getCmdLineOpts\"")) {
			return "SYSTEM_INFO";
		}
		
		// Log rotation
		if (line.contains("\"logRotate\"")) {
			return "LOG_ROTATION";
		}
		
		// Default write concern
		if (line.contains("\"getDefaultRWConcern\"")) {
			return "WRITE_CONCERN_CONFIG";
		}
		
		// List databases
		if (line.contains("\"listDatabases\"")) {
			return "DATABASE_LIST";
		}
		
		// Non-JSON lines
		if (!line.trim().startsWith("{")) {
			return "NON_JSON";
		}
		
		return "UNCATEGORIZED";
	}

	// NEW: Method to increment category counts
	private void incrementIgnoredCategory(String category) {
		synchronized (ignoredCategories) {
			ignoredCategories.computeIfAbsent(category, k -> new AtomicLong(0)).incrementAndGet();
		}
	}

	// NEW: Method to process ignored lines
	private void processIgnoredLine(String line) {
		if (categorizeIgnored) {
			String category = categorizeIgnoredLine(line);
			incrementIgnoredCategory(category);
		}
		
		if (ignoredOutputFile != null) {
			// Sample lines based on the sampling rate
			long lineNumber = ignoredLineCounter.incrementAndGet();
			if (lineNumber % ignoredSampleRate == 0) {
				if (ignoredWriter != null) {
					ignoredWriter.println(line);
				}
			}
		}
	}

	// NEW: Method to report ignored lines analysis
	private void reportIgnoredAnalysis() {
		if (categorizeIgnored && !ignoredCategories.isEmpty()) {
			logger.info("=== IGNORED LINES ANALYSIS ===");
			
			// Sort by count (descending)
			ignoredCategories.entrySet().stream()
				.sorted((e1, e2) -> Long.compare(e2.getValue().get(), e1.getValue().get()))
				.forEach(entry -> {
					String category = entry.getKey();
					long count = entry.getValue().get();
					double percentage = (count * 100.0) / ignoredCount;
					logger.info("  {}: {} ({:.1f}%)", category, count, percentage);
				});
			
			logger.info("Total ignored lines: {}", ignoredCount);
			
			// Identify potential issues
			analyzeForIssues();
		}
		
		if (ignoredOutputFile != null) {
			logger.info("Ignored lines sample written to: {} (sampling rate: 1 in {})", 
					   ignoredOutputFile, ignoredSampleRate);
		}
	}

	// NEW: Method to analyze for potential issues
	private void analyzeForIssues() {
		logger.info("=== POTENTIAL ISSUES DETECTED ===");
		
		long totalIgnored = ignoredCount;
		
		// Check for excessive connections
		long connectionAccepted = ignoredCategories.getOrDefault("NETWORK_CONNECTION_ACCEPTED", new AtomicLong(0)).get();
		long connectionEnded = ignoredCategories.getOrDefault("NETWORK_CONNECTION_ENDED", new AtomicLong(0)).get();
		
		if (connectionAccepted > 1000) {
			logger.warn("⚠️  HIGH CONNECTION ACTIVITY: {} connections accepted", connectionAccepted);
		}
		
		if (Math.abs(connectionAccepted - connectionEnded) > connectionAccepted * 0.1) {
			logger.warn("⚠️  CONNECTION IMBALANCE: {} accepted vs {} ended ({}% difference)", 
					   connectionAccepted, connectionEnded, 
					   Math.abs(connectionAccepted - connectionEnded) * 100 / Math.max(connectionAccepted, 1));
		}
		
		// Check for client disconnections
		long clientDisconnected = ignoredCategories.getOrDefault("NETWORK_CLIENT_DISCONNECTED", new AtomicLong(0)).get();
		if (clientDisconnected > 100) {
			logger.warn("⚠️  HIGH CLIENT DISCONNECTIONS: {} interrupted operations", clientDisconnected);
		}
		
		// Check for authentication issues
		long authFailed = ignoredCategories.getOrDefault("ACCESS_AUTH_FAILED", new AtomicLong(0)).get();
		if (authFailed > 10) {
			logger.warn("⚠️  AUTHENTICATION FAILURES: {} failed authentications", authFailed);
		}
		
		// Check for excessive heartbeats
		long heartbeats = ignoredCategories.getOrDefault("REPLICATION_HEARTBEAT", new AtomicLong(0)).get();
		if (heartbeats > totalIgnored * 0.3) {
			logger.warn("⚠️  EXCESSIVE REPLICATION HEARTBEATS: {} ({}% of ignored lines)", 
					   heartbeats, heartbeats * 100 / totalIgnored);
		}
		
		// Check for admin command spam
		long adminPings = ignoredCategories.getOrDefault("ADMIN_PING", new AtomicLong(0)).get();
		if (adminPings > totalIgnored * 0.2) {
			logger.warn("⚠️  EXCESSIVE ADMIN PINGS: {} ({}% of ignored lines)", 
					   adminPings, adminPings * 100 / totalIgnored);
		}
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