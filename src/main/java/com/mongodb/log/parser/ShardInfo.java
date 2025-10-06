package com.mongodb.log.parser;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents shard and node information extracted from filename
 */
public class ShardInfo {
    private static final Pattern SHARD_PATTERN = Pattern.compile("shard-(\\d+)-(\\d+)");
    
    private final String shard;
    private final String node;
    private final String displayName;
    
    public ShardInfo(String shard, String node) {
        this.shard = shard;
        this.node = node;
        this.displayName = String.format("shard-%s-%s", shard, node);
    }
    
    /**
     * Extract shard info from filename
     * Example: rollups-3-shard-01-01.xnttl.mongodb.net_2025-09-09T16_05_00_2025-09-09T16_10_00_MONGODB.log.gz
     * Returns: ShardInfo with shard="01", node="01"
     */
    public static ShardInfo extractFromFilename(String filename) {
        if (filename == null) {
            return null;
        }
        
        Matcher matcher = SHARD_PATTERN.matcher(filename);
        if (matcher.find()) {
            String shardNum = matcher.group(1);
            String nodeNum = matcher.group(2);
            return new ShardInfo(shardNum, nodeNum);
        }
        
        return null;
    }
    
    public String getShard() {
        return shard;
    }
    
    public String getNode() {
        return node;
    }
    
    public String getDisplayName() {
        return displayName;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShardInfo shardInfo = (ShardInfo) o;
        return Objects.equals(shard, shardInfo.shard) && 
               Objects.equals(node, shardInfo.node);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(shard, node);
    }
    
    @Override
    public String toString() {
        return displayName;
    }
}