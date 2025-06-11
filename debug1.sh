#!/bin/bash

# Comprehensive debugging script for query hash table in HTML report

echo "=== MongoDB Log Parser - Query Hash Table Debug ==="
echo

# 1. Check if HTML file exists and has query hash content
echo "1. Checking HTML output file..."
read -p "Enter the path to your HTML report file: " HTML_FILE

if [ ! -f "$HTML_FILE" ]; then
    echo "❌ HTML file not found: $HTML_FILE"
    exit 1
fi

echo "✅ Found HTML file: $HTML_FILE"
echo

# 2. Check if query hash content exists in HTML
echo "2. Searching for query hash content in HTML..."

if grep -q "Query Hash Analysis" "$HTML_FILE"; then
    echo "✅ Found 'Query Hash Analysis' section header"
else
    echo "❌ Missing 'Query Hash Analysis' section header"
fi

if grep -q "queryHashTable" "$HTML_FILE"; then
    echo "✅ Found queryHashTable element"
else
    echo "❌ Missing queryHashTable element"
fi

if grep -q "queryHashFilter" "$HTML_FILE"; then
    echo "✅ Found queryHashFilter element"
else
    echo "❌ Missing queryHashFilter element"
fi

# Count query hash entries
QUERY_HASH_ROWS=$(grep -c "queryHashTable.*<tr>" "$HTML_FILE" || echo 0)
echo "📊 Query hash table rows found: $QUERY_HASH_ROWS"

echo

# 3. Check the actual HTML structure
echo "3. Analyzing HTML structure..."
echo "Looking for Query Hash section:"
grep -A 10 -B 2 "Query Hash Analysis" "$HTML_FILE" | head -15

echo
echo "Looking for queryHashTable:"
grep -A 5 -B 2 "queryHashTable" "$HTML_FILE" | head -10

echo

# 4. Check for "No query hash entries" message
echo "4. Checking for empty state message..."
if grep -q "No query hash entries found" "$HTML_FILE"; then
    echo "⚠️  Found 'No query hash entries found' message"
    echo "   This means the QueryHashAccumulator is empty"
else
    echo "✅ No empty state message found"
fi

echo

# 5. Check log parsing process
echo "5. Debugging log parsing process..."
PARSER_FILE="src/main/java/com/mongodb/log/parser/LogParser.java"

if [ -f "$PARSER_FILE" ]; then
    echo "Checking LogParser queryHashAccumulator initialization..."
    if grep -q "queryHashAccumulator.*new.*QueryHashAccumulator" "$PARSER_FILE"; then
        echo "✅ QueryHashAccumulator initialization found"
    else
        echo "❌ QueryHashAccumulator initialization missing or incorrect"
    fi
    
    echo "Checking if queryHashAccumulator is passed to HTML generator..."
    if grep -A 10 "HtmlReportGenerator.generateReport" "$PARSER_FILE" | grep -q "queryHashAccumulator"; then
        echo "✅ queryHashAccumulator passed to HTML generator"
    else
        echo "❌ queryHashAccumulator NOT passed to HTML generator"
    fi
else
    echo "❌ LogParser.java not found"
fi

echo

# 6. Check LogParserTask accumulation
TASK_FILE="src/main/java/com/mongodb/log/parser/LogParserTask.java"

if [ -f "$TASK_FILE" ]; then
    echo "Checking LogParserTask queryHashAccumulator usage..."
    if grep -q "queryHashAccumulator.*accumulate" "$TASK_FILE"; then
        echo "✅ QueryHashAccumulator.accumulate() called in LogParserTask"
    else
        echo "❌ QueryHashAccumulator.accumulate() NOT called in LogParserTask"
    fi
    
    # Check if queryHashAccumulator is properly handled in constructor
    if grep -q "this.queryHashAccumulator.*queryHashAccumulator" "$TASK_FILE"; then
        echo "✅ queryHashAccumulator properly assigned in constructor"
    else
        echo "❌ queryHashAccumulator NOT properly assigned in constructor"
    fi
else
    echo "❌ LogParserTask.java not found"
fi

echo

# 7. Generate debugging commands
echo "=== Debugging Commands ==="
echo

echo "A. Add debug logging to LogParser.java (add before HTML generation):"
cat << 'EOF'
if (queryHashAccumulator != null) {
    logger.info("QueryHashAccumulator entries: {}", queryHashAccumulator.getQueryHashEntries().size());
    queryHashAccumulator.getQueryHashEntries().entrySet().stream()
        .limit(5)
        .forEach(entry -> logger.info("Sample entry: {} -> {}", entry.getKey(), entry.getValue().getCount()));
} else {
    logger.info("QueryHashAccumulator is null!");
}
EOF

echo
echo "B. Check if log lines have queryHash field:"
echo "   grep -m 10 'queryHash' your_log_file.log"

echo
echo "C. Run parser with debug enabled:"
echo "   java -jar target/logparser.jar -f your_log.log --html report.html --debug"

echo
echo "D. Check browser developer tools:"
echo "   - Open report.html in browser"
echo "   - Press F12 -> Console tab"
echo "   - Look for JavaScript errors"

echo
echo "=== Potential Issues and Solutions ==="
echo

echo "1. QueryHashAccumulator not initialized:"
echo "   - Check LogParser constructor and call() method"
echo "   - Ensure 'queryHashAccumulator = new QueryHashAccumulator();' exists"

echo
echo "2. QueryHashAccumulator not passed to tasks:"
echo "   - Check LogParserTask constructor calls"
echo "   - Ensure queryHashAccumulator parameter is passed"

echo
echo "3. No queryHash in log entries:"
echo "   - MongoDB 4.4+ required for queryHash field"
echo "   - Check if logs contain 'queryHash' field"

echo
echo "4. Namespace filtering:"
echo "   - Check if --ns filters are excluding all query hash entries"

echo
echo "5. HTML generation condition:"
echo "   - Check if condition 'queryHashAccumulator != null && !queryHashAccumulator.getQueryHashEntries().isEmpty()' is failing"

echo
echo "=== Quick Test ==="
echo "Run this command to check if your logs contain queryHash:"
echo "head -1000 your_log_file.log | grep -c queryHash"
echo "If this returns 0, your logs don't contain queryHash fields"

