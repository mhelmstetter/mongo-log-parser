package com.mongodb.logparse;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.json.simple.parser.ParseException;

public interface LogParser {

    void read(File file) throws IOException, ParseException, InterruptedException, ExecutionException;

    void report();

    Accumulator getAccumulator();
    
    int getUnmatchedCount();

}