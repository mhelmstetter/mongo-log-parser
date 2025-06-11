# MongoDB Log Parser

A comprehensive MongoDB log analysis tool that parses log files and generates detailed reports on database operations, performance metrics, and transaction analysis.

## Quick Start

The simplest way to get started is to generate an interactive HTML report:

```bash
java -jar bin/MongoLogParser.jar -f *.log --html logParseReport.html
```

This will analyze all `.log` files in the current directory and create an interactive HTML report with sortable tables, filtering capabilities, and comprehensive analytics.

**Note:** The parser can process compressed files directly - there's no need to extract `.gz` or `.zip` files before analysis. Simply point the parser at the compressed files and it will handle the decompression automatically.

## Features

- **Interactive HTML Reports** - Sortable and filterable tables with navigation
- **Operation Analysis** - Detailed metrics for find, aggregate, update, insert, delete operations
- **Transaction Analysis** - Transaction performance and termination cause tracking
- **Plan Cache Analysis** - Query plan effectiveness and collection scan detection
- **Query Hash Analysis** - Query pattern identification and performance profiling
- **Error Code Analysis** - Error frequency and categorization
- **TTL Operations** - Time-to-live operation monitoring
- **Namespace Filtering** - Focus analysis on specific databases or collections
- **Multiple Output Formats** - HTML, CSV, and console output

## Installation

### Prerequisites
- Java 17 or higher
- Maven 3.6+ (for building from source)

### Download
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

Generate an HTML report (recommended):
```bash
java -jar bin/MongoLogParser.jar -f server.log --html report.html
```

Generate a CSV report:
```bash
java -jar bin/MongoLogParser.jar -f server.log --csv output.csv
```

Console output only:
```bash
java -jar bin/MongoLogParser.jar -f server.log
```

Analyze compressed log files directly:
```bash
java -jar bin/MongoLogParser.jar -f server.log.gz --html report.html
java -jar bin/MongoLogParser.jar -f logs.zip --html report.html
```

### Advanced Usage

**Multiple log files (including compressed):**
```bash
java -jar bin/MongoLogParser.jar -f server1.log server2.log.gz server3.zip --html report.html
```

**Namespace filtering:**
```bash
# Analyze only myapp database
java -jar bin/MongoLogParser.jar -f *.log --ns myapp --html report.html

# Analyze specific collection
java -jar bin/MongoLogParser.jar -f *.log --ns myapp.users --html report.html

# Multiple namespaces with wildcards
java -jar bin/MongoLogParser.jar -f *.log --ns "myapp.*" --ns "analytics.events" --html report.html
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

**Custom filtering configuration:**
```bash
java -jar bin/MongoLogParser.jar -f *.log --config filter-config.properties --html report.html
```

## Command Line Options

| Option | Description |
|--------|-------------|
| `-f, --files <files>` | MongoDB log file(s) to analyze (required) |
| `--html <file>` | Generate interactive HTML report |
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
- **Performance Highlights** - Collection scans and slow operations are highlighted
- **Summary Statistics** - Key metrics for each analysis type

### Analysis Types

1. **Main Operations** - Core database operations with timing and examination metrics
2. **TTL Operations** - Time-to-live deletion operations and document counts
3. **Operation Statistics** - Breakdown by operation type (find, update, etc.)
4. **Error Codes** - Error frequency analysis with sample messages
5. **Query Hash Analysis** - Query pattern performance with sanitized queries
6. **Plan Cache Analysis** - Query plan effectiveness and replan frequency
7. **Transaction Analysis** - Transaction duration, commit types, and termination causes

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

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.