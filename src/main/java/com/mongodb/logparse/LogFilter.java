package com.mongodb.logparse;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.mongodb.util.MimeTypes;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Log Filter
 */
@Command(name = "logFilter", mixinStandardHelpOptions = true, version = "0.1", description = "MongoDB log filter")
public class LogFilter implements Callable<Integer> {

	private static Logger logger = LoggerFactory.getLogger(LogFilter.class);

	@Option(names = "-f", description = "File names", required = true)
	private List<String> files;

	private String currentLine;

	private JsonFactory jsonFactory;
	private JsonParser jp;

	private static List<String> ignoreTruncateText = Arrays.asList("ns", "planSummary");

	private static List<String> ignoreTruncateArray = Arrays.asList("pipeline");

	private static List<String> ignoreKeys = Arrays.asList("writeConcern", "$audit", "$client", "$clusterTime",
			"$configTime", "$db", "$topologyTime", "advanced", "bypassDocumentValidation", "clientOperationKey",
			"clusterTime", "collation", "cpuNanos", "cursor", "cursorid", "cursorExhausted", "databaseVersion",
			"flowControl", "fromMongos", "fromMultiPlanner", "let", "locks", "lsid", "maxTimeMS", "maxTimeMSOpOnly",
			"mayBypassWriteBlocking", "multiKeyPaths", "needsMerge", "needTime", "numYields", "planningTimeMicros",
			"protocol", "queryFramework", "readConcern", "readConcern", "runtimeConstants", "remote", "shardVersion",
			"totalOplogSlotDurationMicros", "waitForWriteConcernDurationMillis", "works", "writeConcern");

	private static List<String> ignore = Arrays.asList("\"c\":\"NETWORK\"", "\"c\":\"ACCESS\"", "\"c\":\"CONNPOOL\"",
			"\"c\":\"STORAGE\"", "\"c\":\"SHARDING\"", "\"profile\":", "\"killCursors\":", "\"c\":\"CONTROL\"",
			"\"hello\":1", "\"isMaster\":1", "\"ping\":1", "\"saslContinue\":1", "\"replSetHeartbeat\":\"",
			"\"serverStatus\":1", "\"replSetGetStatus\":1", "\"buildInfo\"", "\"getParameter\":",
			"\"getCmdLineOpts\":1", "\"logRotate\":\"", "\"getDefaultRWConcern\":1", "\"listDatabases\":1",
			"\"endSessions\":", "\"ctx\":\"TTLMonitor\"", "\"Failed to gather storage statistics for slow operation\"",
			"\"$db\":\"admin\"", "\"$db\":\"local\"", "\"$db\":\"config\"", "\"ns\":\"local.clustermanager\"",
			"\"dbstats\":1", "\"listIndexes\":\"", "\"collStats\":\"");

	@Override
	public Integer call() throws Exception {
		jsonFactory = JsonFactory.builder().build();
		read();
		return 0;
	}

	public void read() throws IOException, ParseException, InterruptedException, ExecutionException {
		for (String fileName : files) {
			File f = new File(fileName);
			read(f);
		}
	}

	public void read(File file) throws IOException, ParseException, InterruptedException, ExecutionException {

		String originalFileName = file.getName();
		String originalFileExtension = "";
		int dotIndex = originalFileName.lastIndexOf('.');
		if (dotIndex > 0 && dotIndex < originalFileName.length() - 1) {
			originalFileExtension = originalFileName.substring(dotIndex);
			originalFileName = originalFileName.substring(0, dotIndex);
		}

		String filteredFileName = originalFileName + "_filtered" + originalFileExtension;
		File filteredFile = new File(file.getParent(), filteredFileName);

		String guess = MimeTypes.guessContentTypeFromName(originalFileName);
		logger.debug("mime type guess: {}", guess);

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

		PrintStream os = new PrintStream(new BufferedOutputStream(new FileOutputStream(filteredFile)));

		int filteredCount = 0;
		int lineNum = 0;
		long start = System.currentTimeMillis();

		while ((currentLine = in.readLine()) != null) {
			lineNum++;

			if (ignoreLine(currentLine) || !currentLine.startsWith("{\"t\":{\"$date\"")) {
				filteredCount++;
				continue;
			}

			jp = jsonFactory.createParser(currentLine);

			ObjectMapper mapper = new ObjectMapper();

			JsonNode rootNode = mapper.readTree(currentLine);
			JsonNode filteredNode = filterJson(rootNode);

			if (filteredNode != null && !filteredNode.isEmpty()) {
				os.println(filteredNode.toString());
			}

			// os.println(jo.toString());

			if (lineNum % 25000 == 0) {
				System.out.print(".");
				if (lineNum % 250000 == 0) {
					System.out.println();
				}
			}
		}

		in.close();
		long end = System.currentTimeMillis();
		long dur = (end - start);
		logger.debug(
				String.format("Elapsed millis: %s, lineCount: %s, filteredCount: %s", dur, lineNum, filteredCount));
	}

	private JsonNode filterJson(JsonNode node) {
		if (node.isObject()) {
			Iterator<String> fieldNames = node.fieldNames();
			List<String> keysToRemove = new ArrayList<>();

			while (fieldNames.hasNext()) {
				String fieldName = fieldNames.next();
				JsonNode childNode = node.get(fieldName);

				if (ignoreKeys.contains(fieldName)) {
					keysToRemove.add(fieldName);
				} else {
					// Apply value-based filters
					if (childNode.isTextual()) {
						String textValue = childNode.asText();
						if (!ignoreTruncateText.contains(fieldName)) {
							if (textValue.length() > 35) {
								((ObjectNode) node).set(fieldName, new TextNode(textValue.substring(0, 35)));
							} else {
								((ObjectNode) node).set(fieldName, new TextNode(textValue));
							}
						}
						
					} else if (childNode.isArray()) {
						if (childNode.size() > 3 && !ignoreTruncateArray.contains(fieldName)
								&& !fieldName.equals("$and") && !fieldName.equals("$or")) {
							ArrayNode arr = (ArrayNode) childNode;
							JsonNode first = arr.get(0);
							int size = arr.size();
							arr.removeAll();
							arr.add(first);
							JsonNode truncInfo = new TextNode("<truncated " + (size - 1) + " elements>");
							arr.add(truncInfo);
//	                        ArrayNode truncatedArray = ((ArrayNode) childNode).removeAll().add(childNode.get(0));
//	                        ((ObjectNode) node).set(fieldName, truncatedArray);
						}
						// Recursively filter the array elements
						for (JsonNode arrayElement : childNode) {
							filterJson(arrayElement);
						}
					} else {
						// Recursively filter the child node

						if (childNode.isObject() && childNode.isEmpty()) {
							keysToRemove.add(fieldName);
						}

						filterJson(childNode);
					}

				}
			}

			for (String key : keysToRemove) {
				((ObjectNode) node).remove(key);
			}
		} else if (node.isArray()) {
			for (JsonNode arrayElement : node) {
				filterJson(arrayElement);
			}
		}
		return node;
	}

	private static boolean ignoreLine(String line) {
		for (String keyword : ignore) {
			if (line.contains(keyword)) {
				return true;
			}
		}
		return false;
	}

	public static void main(String[] args) {
		int exitCode = new CommandLine(new LogFilter()).execute(args);
		System.exit(exitCode);
	}

}
