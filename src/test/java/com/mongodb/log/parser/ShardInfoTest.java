package com.mongodb.log.parser;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class ShardInfoTest {
    
    @Test
    public void testExtractFromFilename() {
        // Test the example filename provided by user
        String filename = "rollups-3-shard-01-01.xnttl.mongodb.net_2025-09-09T16_05_00_2025-09-09T16_10_00_MONGODB.log.gz";
        ShardInfo info = ShardInfo.extractFromFilename(filename);
        
        assertNotNull(info);
        assertEquals("01", info.getShard());
        assertEquals("01", info.getNode());
        assertEquals("shard-01-01", info.getDisplayName());
    }
    
    @Test
    public void testExtractFromDifferentShardNode() {
        String filename = "someprefix-shard-05-03.server.mongodb.net_logfile.log";
        ShardInfo info = ShardInfo.extractFromFilename(filename);
        
        assertNotNull(info);
        assertEquals("05", info.getShard());
        assertEquals("03", info.getNode());
        assertEquals("shard-05-03", info.getDisplayName());
    }
    
    @Test
    public void testExtractFromFilenameNoMatch() {
        String filename = "regular-mongodb-log-file.log";
        ShardInfo info = ShardInfo.extractFromFilename(filename);
        
        assertNull(info);
    }
    
    @Test
    public void testExtractFromNullFilename() {
        ShardInfo info = ShardInfo.extractFromFilename(null);
        assertNull(info);
    }
    
    @Test
    public void testEquals() {
        ShardInfo info1 = new ShardInfo("01", "02");
        ShardInfo info2 = new ShardInfo("01", "02");
        ShardInfo info3 = new ShardInfo("01", "03");
        
        assertEquals(info1, info2);
        assertNotEquals(info1, info3);
    }
    
    @Test
    public void testHashCode() {
        ShardInfo info1 = new ShardInfo("01", "02");
        ShardInfo info2 = new ShardInfo("01", "02");
        
        assertEquals(info1.hashCode(), info2.hashCode());
    }
}