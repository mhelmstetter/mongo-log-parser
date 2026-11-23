package com.mongodb.log.parser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Identifies the process a log file came from, based on its file name.
 * Atlas names log files {@code <host>_<start>_<end>_MONGODB.log.gz} (or {@code _MONGOS}).
 */
public class NodeIdentity {

    private static final Pattern CONFIG_PATTERN = Pattern.compile("-config-(\\d+)-(\\d+)");

    private final String node;
    private final String processType;
    private final String shard;
    private final String member;

    private NodeIdentity(String node, String processType, String shard, String member) {
        this.node = node;
        this.processType = processType;
        this.shard = shard;
        this.member = member;
    }

    public static NodeIdentity fromFilename(String filename) {
        String upper = filename.toUpperCase();
        String processType;
        if (upper.contains("_MONGOS") || upper.startsWith("MONGOS")) {
            processType = "mongos";
        } else if (upper.contains("_MONGODB") || upper.startsWith("MONGOD")) {
            processType = "mongod";
        } else {
            processType = "unknown";
        }

        // Atlas: host is everything before the first '_'; otherwise the file name identifies the node
        int underscore = filename.indexOf('_');
        String host = underscore > 0 ? filename.substring(0, underscore) : filename;
        String node = "mongos".equals(processType) ? host + " (mongos)" : host;

        ShardInfo shardInfo = ShardInfo.extractFromFilename(filename);
        Matcher config = CONFIG_PATTERN.matcher(filename);
        String shard = null;
        String member = null;
        if (config.find()) {
            shard = "config";
            member = config.group(2);
        } else if (shardInfo != null) {
            shard = shardInfo.getShard();
            member = shardInfo.getNode();
        }
        return new NodeIdentity(node, processType, shard, member);
    }

    public String getNode() {
        return node;
    }

    public String getProcessType() {
        return processType;
    }

    /** Shard number ("00"), "config" for config servers, or null when the file name has no shard. */
    public String getShard() {
        return shard;
    }

    public String getMember() {
        return member;
    }
}
