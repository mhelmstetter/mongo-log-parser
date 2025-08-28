package com.mongodb.log.parser.accumulator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import com.mongodb.log.parser.LogLineAccumulator;
import com.mongodb.log.parser.Namespace;
import com.mongodb.log.parser.SlowQuery;

public class Accumulator {

    private Map<AccumulatorKey, LogLineAccumulator> accumulators = new HashMap<AccumulatorKey, LogLineAccumulator>();

    private String[] headers = new String[] { 
    	    "Namespace", "Operation", "Count", "MinMs", "MaxMs", "AvgMs", 
    	    "P95Ms", "TotalSec", "AvgKeysEx", "AvgDocsEx", "KeysP95", 
    	    "DocsP95", "TotalKeysK", "TotalDocsK", "AvgReturn", "ExRetRatio" 
    	};

    protected void accumulate(File file, String command, String dbName, String collName, Integer execTime) {
        // TODO add an option to accumulate per file, for now glob all files
        // together
        AccumulatorKey key = new AccumulatorKey(null, dbName, collName, command);
        LogLineAccumulator accum = accumulators.get(key);
        if (accum == null) {
            accum = new LogLineAccumulator(null, command, new Namespace(dbName, collName));
            accumulators.put(key, accum);
        }
        accum.addExecution(execTime);
    }

    protected void accumulate(File file, String command, Namespace namespace, Long execTime) {
        accumulate(file, command, namespace, execTime, null, null, null, null, null, null, null, null, null);
    }
    
    public void accumulate(File file, String command, Namespace namespace, Long execTime, Long keysExamined,
            Long docsExamined, Long nReturned, Long reslen, Long bytesRead) {
        accumulate(file, command, namespace, execTime, keysExamined, docsExamined, nReturned, reslen, bytesRead, null, null, null, null);
    }

    int count = 0;
    public synchronized void accumulate(SlowQuery slowQuery) {
        if (slowQuery.opType == null) {
            return;
        }
        accumulate(null, slowQuery.opType.getType(), slowQuery.ns, slowQuery.durationMillis, slowQuery.keysExamined,
                slowQuery.docsExamined, slowQuery.nreturned, slowQuery.reslen, slowQuery.bytesRead, slowQuery.bytesWritten, slowQuery.writeConflicts, slowQuery.nShards, null);
    }
    
    public synchronized void accumulate(SlowQuery slowQuery, String logMessage) {
        if (slowQuery.opType == null) {
            return;
        }
        accumulate(null, slowQuery.opType.getType(), slowQuery.ns, slowQuery.durationMillis, slowQuery.keysExamined,
                slowQuery.docsExamined, slowQuery.nreturned, slowQuery.reslen, slowQuery.bytesRead, slowQuery.bytesWritten, slowQuery.writeConflicts, slowQuery.nShards, logMessage);
    }

    public void accumulate(File file, String command, Namespace namespace, Long execTime, Long keysExamined,
            Long docsExamined, Long nReturned, Long reslen, Long bytesRead, Long nShards) {
        accumulate(file, command, namespace, execTime, keysExamined, docsExamined, nReturned, reslen, bytesRead, null, null, nShards, null);
    }

    public void accumulate(File file, String command, Namespace namespace, Long execTime, Long keysExamined,
            Long docsExamined, Long nReturned, Long reslen, Long bytesRead, Long bytesWritten, Long nShards) {
        accumulate(file, command, namespace, execTime, keysExamined, docsExamined, nReturned, reslen, bytesRead, bytesWritten, null, nShards, null);
    }

    public void accumulate(File file, String command, Namespace namespace, Long execTime, Long keysExamined,
            Long docsExamined, Long nReturned, Long reslen, Long bytesRead, Long bytesWritten, Long writeConflicts, Long nShards, String logMessage) {
        // TODO add an option to accumulate per file, for now glob all files
        // together
        AccumulatorKey key = new AccumulatorKey(null, namespace, command);
        LogLineAccumulator accum = accumulators.get(key);
        if (accum == null) {
            accum = new LogLineAccumulator(null, command, namespace);
            accumulators.put(key, accum);
        }

        if (execTime != null) {
            accum.addExecution(execTime);
        }

        if (reslen != null) {
            accum.addReslen(reslen);
        }

        if (bytesRead != null) {
            accum.addBytesRead(bytesRead);
        }

        if (bytesRead != null) {
            accum.addStorageBytesRead(bytesRead);
        }

        if (bytesWritten != null) {
            accum.addStorageBytesWritten(bytesWritten);
        }

        if (writeConflicts != null) {
            accum.addWriteConflicts(writeConflicts);
        }

        if (keysExamined != null) {
            accum.addExamined(keysExamined, docsExamined);
        }

        if (nReturned != null) {
            accum.addReturned(nReturned);
        }
        
        if (nShards != null) {
            accum.addShards(nShards);
        }
        
        if (logMessage != null && execTime != null) {
            accum.addSampleLogMessage(logMessage, execTime);
        } else if (logMessage != null) {
            accum.addSampleLogMessage(logMessage);
        }
    }

    public LogLineAccumulator getAccumulator(AccumulatorKey key) {
        return accumulators.get(key);
    }

    public void report() {
        System.out.println(String.format("%-65s %-20s %10s %10s %10s %10s %10s %10s %10s %10s %10s %10s %10s", 
                "Namespace", "operation", "count", "reslenMB", "readMB", "min_ms", "max_ms", "avg_ms",
                "totalSec", "avgKeysEx", "avgDocsEx", "avgReturn", "exRetRatio"));
        System.out.println("=".repeat(208));
        accumulators.values().stream().sorted(Comparator.comparingLong(LogLineAccumulator::getCount).reversed())
                .forEach(acc -> System.out.println(acc));
    }

    public void reportCsv(String fileName) throws FileNotFoundException {
        PrintWriter writer = new PrintWriter(fileName);
        writer.println(String.join(",", headers));
        accumulators.values().stream()
            .sorted(Comparator.comparingLong(LogLineAccumulator::getCount).reversed())
            .forEach(acc -> writer.println(acc.toCsvString()));
        
        writer.close();
    }

    public Map<AccumulatorKey, LogLineAccumulator> getAccumulators() {
        return accumulators;
    }
    
    public int getAccumulatorSize() {
        return accumulators.size();
    }
}