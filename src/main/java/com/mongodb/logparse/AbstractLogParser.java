package com.mongodb.logparse;

import java.io.FileNotFoundException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractLogParser {

    protected static final Logger logger = LoggerFactory.getLogger(AbstractLogParser.class);
    protected String currentLine = null;
    protected String[] fileNames;
    //protected File file;
    
    protected boolean parseQueries;
    protected boolean replay;
    protected String uri;
    protected String csvOutputFile;
    
    public static final String FIND = "find";
    public static final String FIND_AND_MODIFY = "findAndModify";
    public static final String UPDATE = "update";
    public static final String INSERT = "insert";
    public static final String DELETE = "delete";
    public static final String DELETE_W = "delete_w";
    public static final String COUNT = "count";
    public static final String UPDATE_W = "update_w";
    public static final String GETMORE = "getMore";
    
    protected Accumulator accumulator;
    
    protected int unmatchedCount = 0;
    
    
    public Accumulator getAccumulator() {
        return accumulator;
    }

    public void report() {
        accumulator.report();
    }
    
    public void reportCsv() throws FileNotFoundException {
        accumulator.reportCsv(csvOutputFile);
    }

    public AbstractLogParser() {
        super();
        accumulator = new Accumulator();
    }

    public String[] getFileNames() {
        return fileNames;
    }

    public void setFileNames(String[] fileNames) {
        this.fileNames = fileNames;
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


   

}