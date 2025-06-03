package com.mongodb.logparse;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.json.simple.parser.ParseException;

public class LogParserApp {
    
    
    @SuppressWarnings("static-access")
    private static CommandLine initializeAndParseCommandLineOptions(String[] args) {
        Options options = new Options();
        options.addOption(new Option("help", "print this message"));
        options.addOption(
                OptionBuilder.withArgName("mongod log file(s)").hasArgs().withLongOpt("files").create("f"));
        
        options.addOption(OptionBuilder.withArgName("connection uri")
                .hasArg()
                .withDescription(  "mongodb connection string uri" )
                .withLongOpt("uri")
                .create());
        
        options.addOption(
                OptionBuilder.withArgName("replay operations").withLongOpt("replay").create());
        options.addOption(
                OptionBuilder.withArgName("parse queries").withLongOpt("queries").create("q"));
        options.addOption(
                OptionBuilder.withArgName("csv output").withLongOpt("csv").create("c"));
    
        CommandLineParser parser = new GnuParser();
        CommandLine line = null;
        try {
            line = parser.parse(options, args);
            if (line.hasOption("help")) {
                printHelpAndExit(options);
            }
        } catch (org.apache.commons.cli.ParseException e) {
            System.out.println(e.getMessage());
            printHelpAndExit(options);
        } catch (Exception e) {
            e.printStackTrace();
            printHelpAndExit(options);
        }
        
        String[] fileNames = line.getOptionValues("f");
        
        if (fileNames == null) {
            printHelpAndExit(options);
        }
        
        return line;
    }
    
    private static void printHelpAndExit(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("logParser", options);
        System.exit(-1);
    }

    public static void main(String[] args) throws IOException, ParseException, InterruptedException, ExecutionException {
        CommandLine line = initializeAndParseCommandLineOptions(args);
        
        int numThreads = Runtime.getRuntime().availableProcessors(); // Use all available processors
        
        System.out.println(numThreads);
        
        //String mongoVersion = "3.4";
        
        AbstractLogParser parser = null;
        
        //parser = new LogParserJson();
        
        parser = new LogParserJson();
        //parser = new LogParser3x();
        
//        if (line.hasOption("m")) {
//            mongoVersion = line.getOptionValue("m");
//        }
//        if (mongoVersion.startsWith("3.")) {
//            //parser = new LogParser3x();
//            parser = new LogParserNew();
//        } else if (mongoVersion.startsWith("2.4")) {
//            parser = new LogParser2_4();
//        } else {
//            throw new IllegalArgumentException("Version " + mongoVersion + " not supported");
//        }
        
        if (line.hasOption("q")) {
            parser.setParseQueries(true);
        }
        if (line.hasOption("replay")) {
        	parser.setReplay(true);
        }
        
        if (line.hasOption("uri")) {
        	String uri = line.getOptionValue("uri");
        	parser.setUri(uri);
        }
        
        String[] fileNames = line.getOptionValues("f");
        
        parser.setFileNames(fileNames);
        
        parser.read();
        
        if (line.hasOption("c")) {
            parser.reportCsv();
        } else {
            parser.report();
        }

    }

}
