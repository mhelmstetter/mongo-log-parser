#!/bin/bash

# Simple debug patch that uses simpler sed commands and temp files

echo "🔧 Adding simple debug logging for query hash..."

# 1. First, let's add debug to HtmlReportGenerator
GENERATOR_FILE="src/main/java/com/mongodb/log/parser/HtmlReportGenerator.java"

if [ -f "$GENERATOR_FILE" ]; then
    echo "📝 Adding debug to HtmlReportGenerator..."
    cp "$GENERATOR_FILE" "${GENERATOR_FILE}.backup"
    
    # Create a simple patch using awk instead of sed
    awk '
    /public static void generateReport/ {
        print $0
        print "        // DEBUG: HTML Generation"
        print "        System.out.println(\"=== HTML DEBUG ===\");"
        print "        System.out.println(\"QueryHash accumulator: \" + (queryHashAccumulator != null ? \"not null\" : \"null\"));"
        print "        if (queryHashAccumulator != null) {"
        print "            System.out.println(\"QueryHash entries: \" + queryHashAccumulator.getQueryHashEntries().size());"
        print "        }"
        next
    }
    /if \(queryHashAccumulator != null && !queryHashAccumulator\.getQueryHashEntries\(\)\.isEmpty\(\)\)/ {
        print "        // DEBUG: Checking query hash condition"
        print "        System.out.println(\"DEBUG: About to check query hash condition\");"
        print "        if (queryHashAccumulator != null) {"
        print "            System.out.println(\"DEBUG: QueryHash entries count: \" + queryHashAccumulator.getQueryHashEntries().size());"
        print "        }"
        print "        // MODIFIED: Force query hash table for debug"
        print "        if (queryHashAccumulator != null) {"
        next
    }
    { print }
    ' "$GENERATOR_FILE" > "${GENERATOR_FILE}.tmp" && mv "${GENERATOR_FILE}.tmp" "$GENERATOR_FILE"
    
    echo "✅ HtmlReportGenerator debug added"
fi

# 2. Add debug to LogParserTask
TASK_FILE="src/main/java/com/mongodb/log/parser/LogParserTask.java"

if [ -f "$TASK_FILE" ]; then
    echo "📝 Adding debug to LogParserTask..."
    cp "$TASK_FILE" "${TASK_FILE}.backup"
    
    # Add a simple counter and debug line
    awk '
    /public ProcessingStats call\(\) throws Exception/ {
        print $0
        print "        long queryHashFound = 0; // DEBUG counter"
        next
    }
    /slowQuery\.queryHash = attr\.getString\("queryHash"\);/ {
        print $0
        print "                queryHashFound++;"
        print "                if (debug && queryHashFound <= 3) {"
        print "                    LogParser.logger.info(\"DEBUG: Found queryHash: {} for ns: {}\", slowQuery.queryHash, slowQuery.ns);"
        print "                }"
        next
    }
    /LogParser\.logger\.info.*Thread.*processed.*lines/ {
        print "        LogParser.logger.info(\"DEBUG: Thread {} found {} queryHash entries\", Thread.currentThread().getName(), queryHashFound);"
        print $0
        next
    }
    { print }
    ' "$TASK_FILE" > "${TASK_FILE}.tmp" && mv "${TASK_FILE}.tmp" "$TASK_FILE"
    
    echo "✅ LogParserTask debug added"
fi

# 3. Add debug to main LogParser
PARSER_FILE="src/main/java/com/mongodb/log/parser/LogParser.java"

if [ -f "$PARSER_FILE" ]; then
    echo "📝 Adding debug to LogParser..."
    cp "$PARSER_FILE" "${PARSER_FILE}.backup"
    
    # Add debug before HTML generation
    awk '
    /if \(htmlOutputFile != null\)/ {
        print "        // DEBUG: QueryHash accumulator status before HTML generation"
        print "        if (queryHashAccumulator != null) {"
        print "            logger.info(\"QueryHash accumulator has {} entries\", queryHashAccumulator.getQueryHashEntries().size());"
        print "        } else {"
        print "            logger.info(\"QueryHash accumulator is null!\");"
        print "        }"
        print ""
        print $0
        next
    }
    { print }
    ' "$PARSER_FILE" > "${PARSER_FILE}.tmp" && mv "${PARSER_FILE}.tmp" "$PARSER_FILE"
    
    echo "✅ LogParser debug added"
fi

echo
echo "🎯 Debug logging added successfully!"
echo
echo "📋 Now run:"
echo "1. mvn clean compile"
echo "2. java -jar target/logparser.jar -f your_log.log --html report.html --debug"
echo
echo "📊 Look for debug output:"
echo "- '=== HTML DEBUG ===' at start of HTML generation"
echo "- 'DEBUG: Found queryHash: ...' from LogParserTask"
echo "- 'QueryHash accumulator has X entries' from LogParser"
echo "- 'DEBUG: Thread pool-X found X queryHash entries'"
echo
echo "🔄 To restore original files:"
echo "mv src/main/java/com/mongodb/log/parser/HtmlReportGenerator.java.backup src/main/java/com/mongodb/log/parser/HtmlReportGenerator.java"
echo "mv src/main/java/com/mongodb/log/parser/LogParserTask.java.backup src/main/java/com/mongodb/log/parser/LogParserTask.java"
echo "mv src/main/java/com/mongodb/log/parser/LogParser.java.backup src/main/java/com/mongodb/log/parser/LogParser.java"
