package com.mongodb.logparse;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class Accumulator {

    private Map<AccumulatorKey, LogLineAccumulator> accumulators = new HashMap<AccumulatorKey, LogLineAccumulator>();

    private String[] headers = new String[] { "Namespace", "operation", "count", "reslenMB", "readMB", "min_ms", "max_ms", "avg_ms",
            "totalSec", "avgKeysEx", "avgDocsEx",
            "avgReturn", "exRetRatio" };

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
        accumulate(file, command, namespace, execTime, null, null, null, null, null);
    }

    int count = 0;
    protected synchronized void accumulate(SlowQuery slowQuery) {
        accumulate(null, slowQuery.opType.getType(), slowQuery.ns, slowQuery.durationMillis, slowQuery.keysExamined,
                slowQuery.docsExamined, slowQuery.nreturned, slowQuery.reslen, slowQuery.bytesRead);
    }

    protected void accumulate(File file, String command, Namespace namespace, Long execTime, Long keysExamined,
            Long docsExamined, Long nReturned, Long reslen, Long bytesRead) {
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

        if (keysExamined != null) {
            accum.addExamined(keysExamined, docsExamined);
        }

        if (nReturned != null) {
            accum.addReturned(nReturned);
        }
    }

    public LogLineAccumulator getAccumulator(AccumulatorKey key) {
        return accumulators.get(key);
    }

    public void report() {
        System.out.println(String
                .format("%-65s %-20s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s", headers));

        accumulators.values().stream().sorted(Comparator.comparingLong(LogLineAccumulator::getCount).reversed())
                .forEach(acc -> System.out.println(acc));
    }

    public void reportCsv(String fileName) throws FileNotFoundException {
        PrintWriter writer = new PrintWriter(fileName);
        for (int i = 0; i < headers.length; i++) {
            writer.print(headers[i]);
            if (i + 1 < headers.length) {
                writer.print(",");
            }
        }
        writer.println();

        accumulators.values().stream()
            .sorted(Comparator.comparingLong(LogLineAccumulator::getCount).reversed())
            .forEach(acc -> writer.println(acc.toCsvString()));
        
        writer.close();
    }

    public Map<AccumulatorKey, LogLineAccumulator> getAccumulators() {
        return accumulators;
    }
}