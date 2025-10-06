# MongoDB Log Parser

A comprehensive MongoDB log analysis tool that parses log files and generates detailed reports on database operations, performance metrics, and transaction analysis.

## Quick Start

The simplest way to get started is to analyze your log files:

```bash
java -jar bin/MongoLogParser.jar -f *.log
```

This will analyze all `.log` files in the current directory and automatically generate an interactive HTML report (`report.html`) with sortable tables, filtering capabilities, and comprehensive analytics.

**Note:** The parser can process compressed files directly - there's no need to extract `.gz` or `.zip` files before analysis. Simply point the parser at the compressed files and it will handle the decompression automatically.

## Features

- **Interactive HTML Reports** - Sortable and filterable tables with navigation
- **Query Redaction** - `--redact` option protects sensitive data while preserving analysis value
- **Index Usage Statistics** - Comprehensive index utilization analysis and optimization insights
- **Sample Log Accordions** - Click table rows to view actual log messages for debugging
- **Operation Analysis** - Detailed metrics for find, aggregate, update, insert, delete operations
- **Enhanced getMore Support** - Extract queries from originatingCommand for cursor operations
- **Transaction Analysis** - Transaction performance and termination cause tracking
- **Query Hash Analysis** - Consolidated query pattern identification with planning time metrics
- **Error Code Analysis** - Error frequency and categorization
- **TTL Operations** - Time-to-live operation monitoring
- **Config Database Filtering** - Automatically excludes internal MongoDB operations
- **Namespace Filtering** - Focus analysis on specific databases or collections
- **Multiple Output Formats** - HTML, CSV, and console output
- **Memory Efficient** - Optimized for large log files with minimal memory footprint

## Installation

### Prerequisites
- Java 17 or higher
- Maven 3.6+ (for building from source)

### Quick Install (Mac/Linux)
The easiest way to install the log parser is using the install script:

```bash
git clone https://github.com/mhelmstetter/mongo-log-parser.git
cd mongo-log-parser
./install.sh
```

This will:
- Build the project if needed
- Install the `log-parser` command to `~/.local/bin`
- Add the directory to your PATH (if not already present)

After installation, you can use the tool from anywhere:
```bash
log-parser mongod.log
log-parser *.log -o analysis.html
log-parser --help
```

### Manual Installation
Download the latest release from the [releases page](../../releases) or build from source.

### Build from Source
```bash
git clone https://github.com/mhelmstetter/mongo-log-parser.git
cd mongo-log-parser
mvn package
```

The executable JAR will be created at `bin/MongoLogParser.jar`.

## Usage

### Basic Usage

#### Using the installed command (after running install.sh):
```bash
# Analyze a single log file
log-parser server.log

# Analyze multiple logs with custom output
log-parser *.log -o my-report.html

# Enable query redaction
log-parser server.log --redact
```

#### Using the JAR directly:
```bash
# Default behavior (generates HTML report)
java -jar bin/MongoLogParser.jar -f server.log
```

**Custom HTML report filename:**
```bash
java -jar bin/MongoLogParser.jar -f server.log --html my-report.html
```

**Generate CSV report:**
```bash
java -jar bin/MongoLogParser.jar -f server.log --csv output.csv
```

**Console text output:**
```bash
java -jar bin/MongoLogParser.jar -f server.log --text
```

**Enable query redaction for sensitive data:**
```bash
java -jar bin/MongoLogParser.jar -f server.log --redact
```

**Analyze compressed log files directly:**
```bash
java -jar bin/MongoLogParser.jar -f server.log.gz
java -jar bin/MongoLogParser.jar -f logs.zip
```

### Advanced Usage

**Multiple log files (including compressed):**
```bash
java -jar bin/MongoLogParser.jar -f server1.log server2.log.gz server3.zip
```

**Namespace filtering:**
```bash
# Analyze only myapp database
java -jar bin/MongoLogParser.jar -f *.log --ns myapp

# Analyze specific collection
java -jar bin/MongoLogParser.jar -f *.log --ns myapp.users

# Multiple namespaces with wildcards
java -jar bin/MongoLogParser.jar -f *.log --ns "myapp.*" --ns "analytics.events"
```

**Comprehensive analysis with all outputs:**
```bash
java -jar bin/MongoLogParser.jar -f *.log \
  --html full-report.html \
  --csv operations.csv \
  --planCacheCsv plan-cache.csv \
  --queryHashCsv query-hash.csv \
  --errorCodesCsv errors.csv \
  --transactionCsv transactions.csv
```

**Production analysis with query redaction:**
```bash
java -jar bin/MongoLogParser.jar -f *.log --redact --ns "myapp.*"
```

**Text output with custom filtering:**
```bash
java -jar bin/MongoLogParser.jar -f *.log --text --config filter-config.properties
```

## Command Line Options

| Option | Description |
|--------|-------------|
| `-f, --files <files>` | MongoDB log file(s) to analyze (required) |
| `--html <file>` | Generate interactive HTML report (default: report.html) |
| `--redact` | Enable query redaction/sanitization (default: false) |
| `--text` | Enable text output to console |
| `-c, --csv <file>` | Generate CSV output for main operations |
| `--planCacheCsv <file>` | Generate CSV for plan cache analysis |
| `--queryHashCsv <file>` | Generate CSV for query hash analysis |
| `--errorCodesCsv <file>` | Generate CSV for error code analysis |
| `--transactionCsv <file>` | Generate CSV for transaction analysis |
| `--ns, --namespace <namespace>` | Filter to specific namespace(s), supports wildcards |
| `--config <file>` | Load filter configuration from properties file |
| `--ignoredAnalysis <file>` | Output file for ignored lines analysis |
| `--debug` | Enable debug logging |
| `-h, --help` | Show help message |
| `-V, --version` | Show version information |

## Supported Log Formats

The parser supports MongoDB log files in JSON format (MongoDB 4.4+) from both **mongod** (database server) and **mongos** (sharded cluster router) processes.

Supported file formats:
- Plain text `.log` files
- Gzip compressed `.gz` files (processed directly without extraction)
- Zip compressed `.zip` files (processed directly without extraction)

## Report Sections

### HTML Report Features
- **Sticky Navigation** - Quick access to all report sections
- **Sortable Tables** - Click column headers to sort by any metric
- **Live Filtering** - Real-time search within each table
- **Expandable Rows** - Click Query Hash or Plan Cache rows to view sample log messages
- **Performance Highlights** - Collection scans and slow operations are highlighted
- **Summary Statistics** - Key metrics for each analysis type
- **MongoDB Styling** - Modern interface using MongoDB's color scheme

### Analysis Types

1. **Main Operations** - Core database operations with timing and examination metrics
2. **TTL Operations** - Time-to-live deletion operations and document counts
3. **Operation Statistics** - Breakdown by operation type (find, update, etc.)
4. **Error Codes** - Error frequency analysis with sample messages
5. **Query Hash Analysis** - Consolidated query pattern performance with planning time and replan metrics
6. **Transaction Analysis** - Transaction duration, commit types, and termination causes
7. **Index Usage Statistics** - Index utilization patterns, efficiency metrics, and collection scan detection

## Configuration

### Filter Configuration File
Create a `filter-config.properties` file to customize which log lines are processed:

```properties
# Ignore specific operations
ignore.operation.hello=true
ignore.operation.isMaster=true

# Ignore by component
ignore.component.NETWORK=true
ignore.component.ACCESS=true

# Ignore admin database operations
ignore.database.admin=true
ignore.database.local=true
```

### Namespace Filtering Examples

```bash
# Single database
--ns myapp

# Database with wildcard for all collections
--ns "myapp.*"

# Specific collection
--ns myapp.users

# Multiple namespaces
--ns myapp --ns analytics.events

# Pattern matching
--ns "logs_*"
```

## Query Redaction and Privacy

### Overview
The `--redact` option provides configurable privacy protection for sensitive query data while preserving analytical value.

### Default Behavior (No Redaction)
```bash
java -jar bin/MongoLogParser.jar -f server.log
```
- Query values are preserved in their original form
- Complete log messages are shown in accordions
- Ideal for development and testing environments

### Redacted Mode
```bash
java -jar bin/MongoLogParser.jar -f server.log --redact
```
- Sensitive query values are obfuscated (e.g., `"username": "xxx"`)
- Log messages are trimmed to remove verbose fields
- Field names and query structure are preserved for analysis
- Safe for production log analysis and sharing

### What Gets Redacted
- **String values** → replaced with `"xxx"`
- **Numeric values** → digits replaced with `9` (e.g., `123` → `999`)
- **Regular expressions** → pattern content obfuscated, anchors preserved
- **Verbose log fields** → removed to reduce noise (locks, lsid, etc.)

### What's Preserved
- **Field names** and query structure
- **Performance metrics** (duration, docs examined, etc.)
- **Timestamp and date fields** for timeline analysis
- **Namespace information**
- **Operation types** and plan summaries
- **Error codes** and messages
- **Index usage** and plan cache data

### Sample Log Accordions
Click any row in the **Query Hash Analysis** table to expand and view:
- Actual log message that generated the entry
- Formatted for easy reading with syntax highlighting
- Automatically redacted based on `--redact` setting
- Useful for debugging and understanding query patterns

## Performance Tips

- Use namespace filtering (`--ns`) to focus on specific databases/collections
- For large log files, consider filtering by time range first
- The HTML report loads faster than processing multiple CSV outputs
- Use compressed log files (`.gz`, `.zip`) to reduce disk I/O and storage space
- The parser handles decompression automatically, so no need to extract files first

## Example Output

The HTML report provides an interactive dashboard with:
- Summary cards showing total operations, time spent, and unique namespaces
- Sortable tables with performance metrics
- Visual highlighting of collection scans and slow operations
- Filtering capabilities for detailed analysis
- **Expandable rows** showing sample log messages for debugging
- MongoDB-styled interface with professional color scheme

### New Features in Latest Version
- **Enhanced Query Redaction** - Comprehensive `--redact` option protects sensitive data while preserving analysis value
- **Index Usage Statistics** - New table showing index utilization patterns and efficiency metrics across all operations
- **Consolidated Query Analysis** - Combined Query Hash and Plan Cache Analysis tables to eliminate redundancy
- **Config Database Filtering** - Automatically excludes internal MongoDB operations (config.* collections) from analysis
- **Improved Accordion UI** - "Expand All/Collapse All" functionality with stable table layouts
- **Date Preservation in Redaction** - Timestamps and date fields remain visible for timeline analysis

## Troubleshooting

**No operations found:**
- Check that log files are in JSON format (MongoDB 4.4+)
- Verify namespace filters match actual database/collection names
- Use `--debug` flag for detailed parsing information

**Out of memory errors:**
- Increase JVM heap size: `java -Xmx4g -jar bin/MongoLogParser.jar ...`
- Process smaller log files or use time-based filtering

**Performance issues:**
- Use namespace filtering to reduce data volume
- Consider processing log files individually for very large datasets
- Enable redaction (`--redact`) to reduce memory usage with trimmed log messages

**Sample log messages not showing:**
- Ensure log files contain JSON-formatted entries (MongoDB 4.4+)
- Sample messages are stored for Query Hash entries
- Click table rows to expand and view accordion content

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.