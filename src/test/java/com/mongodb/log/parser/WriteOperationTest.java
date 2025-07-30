package com.mongodb.log.parser;

import org.junit.Test;
import org.junit.Before;
import static org.junit.Assert.*;

import java.util.List;

import com.mongodb.log.parser.accumulator.Accumulator;
import com.mongodb.log.parser.model.MainOperationEntry;
import com.mongodb.log.parser.service.MainOperationService;

/**
 * Unit test to verify WRITE message processing
 */
public class WriteOperationTest {
    
    private Accumulator accumulator;
    
    @Before
    public void setUp() {
        accumulator = new Accumulator();
    }
    
    @Test
    public void testWriteOperationDirectAccumulation() {
        // Test by directly adding to accumulator to verify MVC pattern
        Namespace ns = new Namespace("production", "tasks");
        SlowQuery slowQuery = new SlowQuery();
        slowQuery.opType = OpType.UPDATE_W;
        slowQuery.ns = ns;
        slowQuery.durationMillis = 249L;
        slowQuery.keysExamined = 7L;
        slowQuery.docsExamined = 7L;
        slowQuery.nreturned = 0L;
        
        // Sample log message
        String sampleLog = "{\"t\":{\"$date\":\"2025-07-15T10:08:48.507+00:00\"},\"s\":\"I\",\"c\":\"WRITE\",\"id\":51803,\"ctx\":\"conn164343\",\"msg\":\"Slow query\",\"attr\":{\"type\":\"update\",\"ns\":\"production.tasks\",\"durationMillis\":249}}";
        
        // Add to accumulator
        accumulator.accumulate(slowQuery, sampleLog);
        
        // Verify using MVC pattern
        List<MainOperationEntry> operations = MainOperationService.getMainOperationEntries(accumulator);
        assertFalse("No operations were parsed", operations.isEmpty());
        
        // Look for the update_w operation specifically
        List<MainOperationEntry> writeOps = MainOperationService.getWriteOperations(operations);
        assertFalse("No write operations were found", writeOps.isEmpty());
        
        // Find the specific update_w operation
        MainOperationEntry updateOp = writeOps.stream()
                .filter(op -> "update_w".equals(op.getOperation()))
                .findFirst()
                .orElse(null);
        
        assertNotNull("update_w operation not found", updateOp);
        assertEquals("Namespace should be production.tasks", 
                    "production.tasks", updateOp.getNamespace());
        assertEquals("Operation should be update_w", "update_w", updateOp.getOperation());
        assertEquals("Duration should be 249ms", 249, updateOp.getAvgMs());
        assertEquals("Keys examined should be 7", 7, updateOp.getAvgKeysExamined());
        assertEquals("Docs examined should be 7", 7, updateOp.getAvgDocsExamined());
        assertEquals("Count should be 1", 1, updateOp.getCount());
        
        // Verify the sample log message is stored
        assertNotNull("Sample log message should be stored", updateOp.getSampleLogMessage());
        assertTrue("Sample log message should contain the original message", 
                  updateOp.getSampleLogMessage().contains("production.tasks"));
    }
    
    @Test
    public void testWriteOperationsInSummary() {
        // Add different types of operations
        Namespace ns = new Namespace("test", "collection");
        
        SlowQuery updateQuery = new SlowQuery();
        updateQuery.opType = OpType.UPDATE_W;
        updateQuery.ns = ns;
        updateQuery.durationMillis = 100L;
        
        SlowQuery findQuery = new SlowQuery();
        findQuery.opType = OpType.QUERY;
        findQuery.ns = ns;
        findQuery.durationMillis = 50L;
        
        accumulator.accumulate(updateQuery);
        accumulator.accumulate(findQuery);
        
        // Get operations and verify write operations are counted
        List<MainOperationEntry> operations = MainOperationService.getMainOperationEntries(accumulator);
        List<MainOperationEntry> writeOps = MainOperationService.getWriteOperations(operations);
        List<MainOperationEntry> readOps = MainOperationService.getReadOperations(operations);
        
        assertEquals("Should have 1 write operation", 1, writeOps.size());
        assertEquals("Should have 1 read operation", 1, readOps.size());
        
        // Verify the summary stats method
        String summaryStats = MainOperationService.getSummaryStats(operations);
        assertTrue("Summary should show write operations", summaryStats.contains("Writes: 1"));
        assertTrue("Summary should show read operations", summaryStats.contains("Reads: 1"));
        assertTrue("Summary should show total of 2", summaryStats.contains("Total: 2"));
    }
    
    @Test
    public void testMultipleWriteOperations() {
        Namespace ns = new Namespace("test", "collection");
        
        SlowQuery updateQuery = new SlowQuery();
        updateQuery.opType = OpType.UPDATE_W;
        updateQuery.ns = ns;
        updateQuery.durationMillis = 100L;
        
        SlowQuery insertQuery = new SlowQuery();
        insertQuery.opType = OpType.INSERT;
        insertQuery.ns = ns;
        insertQuery.durationMillis = 50L;
        
        SlowQuery deleteQuery = new SlowQuery();
        deleteQuery.opType = OpType.REMOVE;
        deleteQuery.ns = ns;
        deleteQuery.durationMillis = 75L;
        
        accumulator.accumulate(updateQuery);
        accumulator.accumulate(insertQuery);
        accumulator.accumulate(deleteQuery);
        
        List<MainOperationEntry> operations = MainOperationService.getMainOperationEntries(accumulator);
        List<MainOperationEntry> writeOps = MainOperationService.getWriteOperations(operations);
        
        assertEquals("Should have 3 write operations", 3, writeOps.size());
        
        // Verify each operation type
        boolean hasUpdate = writeOps.stream().anyMatch(op -> "update_w".equals(op.getOperation()));
        boolean hasInsert = writeOps.stream().anyMatch(op -> "insert".equals(op.getOperation()));
        boolean hasDelete = writeOps.stream().anyMatch(op -> "remove".equals(op.getOperation()));
        
        assertTrue("Should have update_w operation", hasUpdate);
        assertTrue("Should have insert operation", hasInsert);
        assertTrue("Should have remove operation", hasDelete);
    }
    
    @Test
    public void testMvcPatternFunctionality() {
        // Test the MVC pattern by adding various operations
        Namespace ns1 = new Namespace("db1", "collection1");
        Namespace ns2 = new Namespace("db2", "collection2");
        
        SlowQuery[] queries = {
            createSlowQuery(OpType.UPDATE_W, ns1, 100L),
            createSlowQuery(OpType.QUERY, ns1, 50L),
            createSlowQuery(OpType.INSERT, ns2, 75L),
            createSlowQuery(OpType.AGGREGATE, ns2, 200L)
        };
        
        for (SlowQuery query : queries) {
            accumulator.accumulate(query);
        }
        
        // Test service methods
        List<MainOperationEntry> allOps = MainOperationService.getMainOperationEntries(accumulator);
        assertEquals("Should have 4 operations", 4, allOps.size());
        
        // Test filtering
        List<MainOperationEntry> writeOps = MainOperationService.getWriteOperations(allOps);
        assertEquals("Should have 2 write operations", 2, writeOps.size());
        
        List<MainOperationEntry> readOps = MainOperationService.getReadOperations(allOps);
        assertEquals("Should have 2 read operations", 2, readOps.size());
        
        // Test namespace filtering
        List<MainOperationEntry> ns1Ops = MainOperationService.filterByNamespace(allOps, "db1.collection1");
        assertEquals("Should have 2 operations in db1.collection1", 2, ns1Ops.size());
        
        // Test summary stats
        String stats = MainOperationService.getSummaryStats(allOps);
        assertTrue("Should show total of 4", stats.contains("Total: 4"));
        assertTrue("Should show 2 writes", stats.contains("Writes: 2"));
        assertTrue("Should show 2 reads", stats.contains("Reads: 2"));
    }
    
    private SlowQuery createSlowQuery(OpType opType, Namespace namespace, long durationMillis) {
        SlowQuery query = new SlowQuery();
        query.opType = opType;
        query.ns = namespace;
        query.durationMillis = durationMillis;
        return query;
    }
}
