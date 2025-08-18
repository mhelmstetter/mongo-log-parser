package com.mongodb.log.parser;

import static org.junit.jupiter.api.Assertions.*;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class LogRedactionUtilTest {

    @Test
    public void testBasicUserDataRedaction() {
        String logMessage = """
            {
                "msg": "Slow query",
                "s": "I",
                "c": "COMMAND", 
                "t": {"$date": "2025-06-19T07:29:38.695+00:00"},
                "ctx": "conn224",
                "id": 51803,
                "attr": {
                    "type": "command",
                    "command": {
                        "filter": {
                            "accountId": "12345678901",
                            "dataCenter": "REGION1", 
                            "context": "/company/dept/subdept/region"
                        },
                        "find": "Events"
                    }
                }
            }
            """;

        String redacted = LogRedactionUtil.redactLogMessage(logMessage, true);
        JSONObject result = new JSONObject(redacted);
        
        // System fields should be preserved
        assertEquals("Slow query", result.getString("msg"));
        assertEquals("I", result.getString("s"));
        assertEquals("COMMAND", result.getString("c"));
        assertEquals("conn224", result.getString("ctx"));
        assertEquals(51803, result.getInt("id"));
        assertEquals("command", result.getJSONObject("attr").getString("type"));
        
        // User data in filter should be redacted
        JSONObject filter = result.getJSONObject("attr").getJSONObject("command").getJSONObject("filter");
        assertEquals("xxx", filter.getString("accountId"));
        assertEquals("xxx", filter.getString("dataCenter"));
        assertEquals("xxx", filter.getString("context"));
        
        // Collection name should be preserved for MongoDB analysis
        assertEquals("Events", result.getJSONObject("attr").getJSONObject("command").getString("find"));
    }

    @Test
    public void testClientFieldPreservation() {
        String logMessage = """
            {
                "attr": {
                    "command": {
                        "$client": {
                            "mongos": {
                                "host": "atlas-abcdef-shard-00-00.xyz123.mongodb.net",
                                "client": "192.168.254.8:29260",
                                "version": "7.0.21"
                            },
                            "driver": {
                                "name": "mongo-java-driver|legacy",
                                "version": "4.11.2"
                            },
                            "os": {
                                "name": "Linux",
                                "type": "Linux",
                                "version": "6.1.134",
                                "architecture": "amd64"
                            },
                            "platform": "Java/Amazon.com Inc./11.0.27+6-LTS"
                        }
                    }
                }
            }
            """;

        String redacted = LogRedactionUtil.redactLogMessage(logMessage, true);
        JSONObject result = new JSONObject(redacted);
        
        // All $client fields should be preserved
        JSONObject client = result.getJSONObject("attr").getJSONObject("command").getJSONObject("$client");
        assertEquals("atlas-xxx-shard-00-00.xxx.mongodb.net", client.getJSONObject("mongos").getString("host"));
        assertEquals("192.168.254.8:29260", client.getJSONObject("mongos").getString("client"));
        assertEquals("7.0.21", client.getJSONObject("mongos").getString("version"));
        assertEquals("mongo-java-driver|legacy", client.getJSONObject("driver").getString("name"));
        assertEquals("4.11.2", client.getJSONObject("driver").getString("version"));
        assertEquals("Linux", client.getJSONObject("os").getString("name"));
        assertEquals("Linux", client.getJSONObject("os").getString("type"));
        assertEquals("amd64", client.getJSONObject("os").getString("architecture"));
    }

    @Test
    public void testRegularExpressionInQuery() {
        String logMessage = """
            {
                "attr": {
                    "command": {
                        "filter": {
                            "context": {
                                "$regularExpression": {
                                    "pattern": "^\\\\/company\\\\/dept",
                                    "options": ""
                                }
                            }
                        }
                    }
                }
            }
            """;

        String redacted = LogRedactionUtil.redactLogMessage(logMessage, true);
        JSONObject result = new JSONObject(redacted);
        
        // Regular expression should preserve regex operators but redact user data
        JSONObject regex = result.getJSONObject("attr").getJSONObject("command")
                .getJSONObject("filter").getJSONObject("context")
                .getJSONObject("$regularExpression");
        String redactedPattern = regex.getString("pattern");
        assertTrue(redactedPattern.startsWith("^"), "Pattern should preserve ^ operator");
        assertTrue(redactedPattern.contains("\\"), "Pattern should preserve \\ escape characters");
        assertTrue(redactedPattern.contains("xxx"), "Pattern should redact user data");
        assertEquals("", regex.getString("options")); // options preserved
    }

    @Test
    public void testArrayInQuery() {
        String logMessage = """
            {
                "attr": {
                    "command": {
                        "filter": {
                            "context": {
                                "$in": [
                                    "/org/div/region_a/subdiv_a",
                                    "/org/div/region_b/subdiv_b",
                                    "/org/div/region_c/subdiv_c"
                                ]
                            }
                        }
                    }
                }
            }
            """;

        String redacted = LogRedactionUtil.redactLogMessage(logMessage, true);
        JSONObject result = new JSONObject(redacted);
        
        // Array values in query context should be redacted
        JSONObject filter = result.getJSONObject("attr").getJSONObject("command").getJSONObject("filter");
        var contextArray = filter.getJSONObject("context").getJSONArray("$in");
        assertEquals("xxx", contextArray.getString(0));
        assertEquals("xxx", contextArray.getString(1));
        assertEquals("xxx", contextArray.getString(2));
    }

    @Test
    public void testDatePreservationInQuery() {
        String logMessage = """
            {
                "attr": {
                    "command": {
                        "filter": {
                            "timestamp": {
                                "$gte": {
                                    "$date": "2025-06-18T16:00:00.000Z"
                                }
                            }
                        }
                    }
                }
            }
            """;

        String redacted = LogRedactionUtil.redactLogMessage(logMessage, true);
        JSONObject result = new JSONObject(redacted);
        
        // $date values should be preserved even in query contexts
        String dateValue = result.getJSONObject("attr").getJSONObject("command")
                .getJSONObject("filter").getJSONObject("timestamp")
                .getJSONObject("$gte").getString("$date");
        assertEquals("2025-06-18T16:00:00.000Z", dateValue);
    }

    @Test
    public void testPipelineRedaction() {
        String logMessage = """
            {
                "attr": {
                    "command": {
                        "aggregate": "Items",
                        "pipeline": [
                            {
                                "$match": {
                                    "accountId": "98765432101",
                                    "context": "/sensitive/path"
                                }
                            },
                            {
                                "$project": {
                                    "accountId": 1,
                                    "publicField": 1
                                }
                            }
                        ]
                    }
                }
            }
            """;

        String redacted = LogRedactionUtil.redactLogMessage(logMessage, true);
        JSONObject result = new JSONObject(redacted);
        
        // Pipeline is a query field, so user data should be redacted
        var pipeline = result.getJSONObject("attr").getJSONObject("command").getJSONArray("pipeline");
        var matchStage = pipeline.getJSONObject(0).getJSONObject("$match");
        assertEquals("xxx", matchStage.getString("accountId"));
        assertEquals("xxx", matchStage.getString("context"));
        
        // Collection name should be preserved for MongoDB analysis
        assertEquals("Items", result.getJSONObject("attr").getJSONObject("command").getString("aggregate"));
    }

    @Test
    public void testReadConcernPreservation() {
        String logMessage = """
            {
                "attr": {
                    "readConcern": {
                        "level": "local",
                        "provenance": "implicitDefault"
                    }
                }
            }
            """;

        String redacted = LogRedactionUtil.redactLogMessage(logMessage, true);
        JSONObject result = new JSONObject(redacted);
        
        // readConcern fields should be preserved
        JSONObject readConcern = result.getJSONObject("attr").getJSONObject("readConcern");
        assertEquals("local", readConcern.getString("level"));
        assertEquals("implicitDefault", readConcern.getString("provenance"));
    }

    @Test
    public void testStorageMetricsPreservation() {
        String logMessage = """
            {
                "attr": {
                    "storage": {
                        "data": {
                            "timeReadingMicros": 999999,
                            "bytesRead": 99999999
                        }
                    }
                }
            }
            """;

        String redacted = LogRedactionUtil.redactLogMessage(logMessage, true);
        JSONObject result = new JSONObject(redacted);
        
        // Storage metrics should be preserved
        JSONObject data = result.getJSONObject("attr").getJSONObject("storage").getJSONObject("data");
        assertEquals(999999, data.getInt("timeReadingMicros"));
        assertEquals(99999999, data.getLong("bytesRead"));
    }

    @Test
    public void testPerformanceMetricsPreservation() {
        String logMessage = """
            {
                "attr": {
                    "durationMillis": 144,
                    "cpuNanos": 1675054,
                    "keysExamined": 100,
                    "docsExamined": 50,
                    "nreturned": 25,
                    "reslen": 587697,
                    "queryHash": "162B8C18",
                    "planCacheKey": "ABC123"
                }
            }
            """;

        String redacted = LogRedactionUtil.redactLogMessage(logMessage, true);
        JSONObject result = new JSONObject(redacted);
        
        // Performance metrics should be preserved
        JSONObject attr = result.getJSONObject("attr");
        assertEquals(144, attr.getInt("durationMillis"));
        assertEquals(1675054, attr.getLong("cpuNanos"));
        assertEquals(100, attr.getInt("keysExamined"));
        assertEquals(50, attr.getInt("docsExamined"));
        assertEquals(25, attr.getInt("nreturned"));
        assertEquals(587697, attr.getInt("reslen"));
        assertEquals("162B8C18", attr.getString("queryHash"));
        assertEquals("ABC123", attr.getString("planCacheKey"));
    }

    @Test
    public void testRedactionDisabled() {
        String logMessage = """
            {
                "attr": {
                    "command": {
                        "filter": {
                            "accountId": "11122233301",
                            "sensitiveData": "should-not-be-redacted"
                        }
                    }
                }
            }
            """;

        String result = LogRedactionUtil.redactLogMessage(logMessage, false);
        
        // When redaction is disabled, original message should be returned
        assertEquals(logMessage.replaceAll("\\s+", ""), result.replaceAll("\\s+", ""));
    }

    @Test
    public void testNameFieldContextAwareness() {
        String logMessage = """
            {
                "attr": {
                    "command": {
                        "$client": {
                            "application": {
                                "name": "mongosh 2.0.2"
                            }
                        },
                        "filter": {
                            "name": "John Doe"
                        }
                    }
                }
            }
            """;

        String redacted = LogRedactionUtil.redactLogMessage(logMessage, true);
        JSONObject result = new JSONObject(redacted);
        
        // System name should be preserved
        String appName = result.getJSONObject("attr").getJSONObject("command")
                .getJSONObject("$client").getJSONObject("application").getString("name");
        assertEquals("mongosh 2.0.2", appName);
        
        // User data name should be redacted
        String userName = result.getJSONObject("attr").getJSONObject("command")
                .getJSONObject("filter").getString("name");
        assertEquals("xxx", userName);
    }
    
    @Test
    public void testReplanReasonPreservation() {
        String logMessage = """
            {
                "attr": {
                    "replanReason": "cached plan was pinned",
                    "replanned": true,
                    "durationMillis": 459
                }
            }
            """;

        String redacted = LogRedactionUtil.redactLogMessage(logMessage, true);
        JSONObject result = new JSONObject(redacted);
        
        // MongoDB system fields should be preserved
        JSONObject attr = result.getJSONObject("attr");
        assertEquals("cached plan was pinned", attr.getString("replanReason"));
        assertEquals(true, attr.getBoolean("replanned"));
        assertEquals(459, attr.getInt("durationMillis"));
    }
    
    @Test
    public void testLimitFieldPreservation() {
        String logMessage = """
            {
                "attr": {
                    "command": {
                        "find": "Items",
                        "limit": 100,
                        "skip": 50,
                        "maxTimeMS": 5000,
                        "filter": {
                            "accountId": "54321"
                        }
                    }
                }
            }
            """;

        String redacted = LogRedactionUtil.redactLogMessage(logMessage, true);
        JSONObject result = new JSONObject(redacted);
        
        // Query parameters should be preserved
        JSONObject command = result.getJSONObject("attr").getJSONObject("command");
        assertEquals("Items", command.getString("find"));
        assertEquals(100, command.getInt("limit"));
        assertEquals(50, command.getInt("skip"));
        assertEquals(5000, command.getInt("maxTimeMS"));
        
        // But user data in filter should still be redacted
        assertEquals("xxx", command.getJSONObject("filter").getString("accountId"));
    }
    
    @Test
    public void testReadConcernInNestedCommands() {
        String logMessage = """
            {
                "attr": {
                    "originatingCommand": {
                        "readConcern": {
                            "level": "local",
                            "provenance": "implicitDefault"
                        },
                        "aggregate": "Events"
                    },
                    "command": {
                        "readConcern": {
                            "level": "majority", 
                            "provenance": "clientSupplied"
                        },
                        "getMore": 12345,
                        "cursorid": 4546003653462669267,
                        "queryHash": "0AFFD3BF",
                        "planCacheKey": "D386A8BA",
                        "cursorExhausted": true
                    }
                }
            }
            """;

        String redacted = LogRedactionUtil.redactLogMessage(logMessage, true);
        JSONObject result = new JSONObject(redacted);
        
        // ReadConcern should be preserved in all contexts
        JSONObject origReadConcern = result.getJSONObject("attr")
                .getJSONObject("originatingCommand").getJSONObject("readConcern");
        assertEquals("local", origReadConcern.getString("level"));
        assertEquals("implicitDefault", origReadConcern.getString("provenance"));
        
        JSONObject cmdReadConcern = result.getJSONObject("attr")
                .getJSONObject("command").getJSONObject("readConcern");
        assertEquals("majority", cmdReadConcern.getString("level"));
        assertEquals("clientSupplied", cmdReadConcern.getString("provenance"));
        
        // Other command fields should also be preserved
        JSONObject command = result.getJSONObject("attr").getJSONObject("command");
        assertEquals(12345, command.getInt("getMore"));
        assertEquals(4546003653462669267L, command.getLong("cursorid"));
        assertEquals("0AFFD3BF", command.getString("queryHash"));
        assertEquals("D386A8BA", command.getString("planCacheKey"));
        assertEquals(true, command.getBoolean("cursorExhausted"));
        
        // Collection names should be preserved for MongoDB analysis
        assertEquals("Events", result.getJSONObject("attr")
                .getJSONObject("originatingCommand").getString("aggregate"));
    }
    
    @Test
    public void testCustomerDataLeakageFix() {
        String logMessage = """
            {
                "msg": "Slow query",
                "s": "I",
                "c": "COMMAND",
                "id": 51803,
                "attr": {
                    "placementVersionRefreshDurationMillis": 1234,
                    "ns": "CustomerDB.CustomerCollection",
                    "errCode": 13388,
                    "errMsg": "sharding status of collection CustomerName is stale",
                    "errName": "StaleConfig",
                    "cpuNanos": 599352,
                    "durationMillis": 2003,
                    "type": "command",
                    "ok": 0,
                    "command": {
                        "filter": {
                            "accountId": "12345",
                            "dataCenter": "US-EAST"
                        },
                        "find": "CustomerCollection",
                        "limit": 100,
                        "comment": "customer query for dashboard",
                        "clientOperationKey": {"$uuid": "6f73a4b2-b49f-4aab-a37d-7d60a8fe4c12"}
                    }
                }
            }
            """;

        String redacted = LogRedactionUtil.redactLogMessage(logMessage, true);
        JSONObject result = new JSONObject(redacted);
        
        JSONObject attr = result.getJSONObject("attr");
        
        // Customer data should be redacted  
        assertEquals("xxx", attr.getString("errMsg"));
        
        // But ns should be preserved for MongoDB analysis
        assertEquals("CustomerDB.CustomerCollection", attr.getString("ns"));
        
        // System fields should be preserved
        assertEquals(1234, attr.getInt("placementVersionRefreshDurationMillis"));
        assertEquals(13388, attr.getInt("errCode"));
        assertEquals("StaleConfig", attr.getString("errName"));
        assertEquals(0, attr.getInt("ok"));
        
        // Command fields
        JSONObject command = attr.getJSONObject("command");
        assertEquals("CustomerCollection", command.getString("find")); // Collection name preserved
        assertEquals(100, command.getInt("limit")); // Limit preserved
        assertEquals("xxx", command.getString("comment")); // User comment redacted
        
        // ClientOperationKey should be preserved
        assertTrue(command.has("clientOperationKey"));
        assertTrue(command.getJSONObject("clientOperationKey").has("$uuid"));
        
        // Filter data redacted
        JSONObject filter = command.getJSONObject("filter");
        assertEquals("xxx", filter.getString("accountId"));
        assertEquals("xxx", filter.getString("dataCenter"));
    }
    
    @Test
    public void testSystemFieldsNotOverRedacted() {
        String logMessage = """
            {
                "attr": {
                    "remote": "192.168.1.1:27017",
                    "protocol": "op_msg",
                    "locks": {
                        "FeatureCompatibilityVersion": {"acquireCount": {"r": 99}},
                        "Mutex": {"acquireCount": {"r": 9}},
                        "Global": {"acquireCount": {"r": 99}}
                    },
                    "originatingCommand": {
                        "$db": "testDB",
                        "mayBypassWriteBlocking": false,
                        "$audit": {
                            "$impersonatedUser": {"user": "testUser", "db": "admin"},
                            "$impersonatedRoles": [{"role": "readWrite", "db": "testDB"}]
                        },
                        "collation": {"locale": "en_US"},
                        "$client": {
                            "mongos": {"host": "localhost:27017", "version": "7.0.21"},
                            "application": {"name": "testApp"}
                        }
                    },
                    "command": {
                        "$db": "testDB",
                        "queryFramework": "classic",
                        "$audit": {
                            "$impersonatedUser": {"user": "testUser", "db": "admin"}
                        },
                        "collation": {"locale": "en_US"}
                    }
                }
            }
            """;

        String redacted = LogRedactionUtil.redactLogMessage(logMessage, true);
        JSONObject result = new JSONObject(redacted);
        
        JSONObject attr = result.getJSONObject("attr");
        
        // System fields should be preserved
        assertEquals("192.168.1.1:27017", attr.getString("remote"));
        assertEquals("op_msg", attr.getString("protocol"));
        
        // Locks should be preserved
        JSONObject locks = attr.getJSONObject("locks");
        assertEquals(99, locks.getJSONObject("FeatureCompatibilityVersion").getJSONObject("acquireCount").getInt("r"));
        assertEquals(9, locks.getJSONObject("Mutex").getJSONObject("acquireCount").getInt("r"));
        
        // Originating command system fields
        JSONObject origCmd = attr.getJSONObject("originatingCommand");
        assertEquals("testDB", origCmd.getString("$db"));
        assertEquals(false, origCmd.getBoolean("mayBypassWriteBlocking"));
        assertEquals("testUser", origCmd.getJSONObject("$audit").getJSONObject("$impersonatedUser").getString("user"));
        assertEquals("en_US", origCmd.getJSONObject("collation").getString("locale"));
        assertEquals("localhost:27017", origCmd.getJSONObject("$client").getJSONObject("mongos").getString("host"));
        assertEquals("testApp", origCmd.getJSONObject("$client").getJSONObject("application").getString("name"));
        
        // Command system fields
        JSONObject command = attr.getJSONObject("command");
        assertEquals("testDB", command.getString("$db"));
        assertEquals("classic", command.getString("queryFramework"));
        assertEquals("testUser", command.getJSONObject("$audit").getJSONObject("$impersonatedUser").getString("user"));
        assertEquals("en_US", command.getJSONObject("collation").getString("locale"));
    }
    
    @Test
    public void testGeoNearAndPipelineOperators() {
        String logMessage = """
            {
                "attr": {
                    "ns": "GeoData.Locations",
                    "command": {
                        "aggregate": "Locations",
                        "pipeline": [
                            {
                                "$geoNear": {
                                    "spherical": true,
                                    "distanceField": "distance",
                                    "maxDistance": 1000,
                                    "near": [-122.4194, 37.7749],
                                    "distanceMultiplier": 6378137,
                                    "query": {"category": "restaurant"}
                                }
                            },
                            {"$skip": 20},
                            {"$limit": 100}
                        ]
                    }
                }
            }
            """;

        String redacted = LogRedactionUtil.redactLogMessage(logMessage, true);
        JSONObject result = new JSONObject(redacted);
        
        JSONObject attr = result.getJSONObject("attr");
        JSONObject command = attr.getJSONObject("command");
        
        // Namespace and collection should be preserved
        assertEquals("GeoData.Locations", attr.getString("ns"));
        assertEquals("Locations", command.getString("aggregate"));
        
        // Pipeline stages should preserve MongoDB operators
        var pipeline = command.getJSONArray("pipeline");
        
        // $geoNear stage fields preserved
        var geoNear = pipeline.getJSONObject(0).getJSONObject("$geoNear");
        assertEquals(true, geoNear.getBoolean("spherical"));
        assertEquals("distance", geoNear.getString("distanceField"));
        assertEquals(1000, geoNear.getInt("maxDistance"));
        assertEquals(6378137, geoNear.getInt("distanceMultiplier"));
        
        // $skip and $limit stages preserved
        assertEquals(20, pipeline.getJSONObject(1).getInt("$skip"));
        assertEquals(100, pipeline.getJSONObject(2).getInt("$limit"));
        
        // But user data in query still redacted
        assertEquals("xxx", geoNear.getJSONObject("query").getString("category"));
    }
    
    @Test
    public void testAtlasHostnameRedaction() {
        String logMessage = """
            {
                "attr": {
                    "command": {
                        "$client": {
                            "mongos": {
                                "host": "atlas-122z4c-shard-00-00.ew921.mongodb.net",
                                "client": "192.168.250.83:21031"
                            }
                        }
                    },
                    "originatingCommand": {
                        "$client": {
                            "mongos": {
                                "host": "atlas-abc123-shard-01-02.region1.mongodb.net"
                            }
                        }
                    }
                }
            }
            """;

        String redacted = LogRedactionUtil.redactLogMessage(logMessage, true);
        JSONObject result = new JSONObject(redacted);
        
        // Atlas hostnames should have cluster IDs and regions redacted
        JSONObject command = result.getJSONObject("attr").getJSONObject("command");
        String commandHost = command.getJSONObject("$client").getJSONObject("mongos").getString("host");
        assertEquals("atlas-xxx-shard-00-00.xxx.mongodb.net", commandHost);
        
        JSONObject origCommand = result.getJSONObject("attr").getJSONObject("originatingCommand");
        String origHost = origCommand.getJSONObject("$client").getJSONObject("mongos").getString("host");
        assertEquals("atlas-xxx-shard-01-02.xxx.mongodb.net", origHost);
        
        // Other client fields should still be preserved
        assertEquals("192.168.250.83:21031", command.getJSONObject("$client").getJSONObject("mongos").getString("client"));
    }
    
    @Test
    public void testFieldTrimming() {
        String logMessage = """
            {
                "attr": {
                    "command": {
                        "find": "collection",
                        "runtimeConstants": {"localNow": {"$date": "2025-07-30T17:19:37.259Z"}},
                        "shardVersion": {"e": {"$oid": "00000000ffffffffffffffff"}},
                        "lsid": {"id": {"$uuid": "f5f322d2-c403-46c5-9b04-00c4c230a027"}},
                        "$clusterTime": {"clusterTime": {"$timestamp": {"t": 1753895977, "i": 11}}},
                        "$configTime": {"$timestamp": {"t": 1753895976, "i": 1}},
                        "$topologyTime": {"$timestamp": {"t": 1727374844, "i": 18}},
                        "normalField": "should be kept"
                    },
                    "durationMillis": 1000
                }
            }
            """;

        String trimmed = LogRedactionUtil.trimLogMessage(logMessage);
        JSONObject result = new JSONObject(trimmed);
        
        JSONObject command = result.getJSONObject("attr").getJSONObject("command");
        
        // These fields should be removed
        assertFalse(command.has("runtimeConstants"), "runtimeConstants should be removed");
        assertFalse(command.has("shardVersion"), "shardVersion should be removed");
        assertFalse(command.has("lsid"), "lsid should be removed");
        assertFalse(command.has("$clusterTime"), "$clusterTime should be removed");
        assertFalse(command.has("$configTime"), "$configTime should be removed");
        assertFalse(command.has("$topologyTime"), "$topologyTime should be removed");
        
        // These fields should be kept
        assertTrue(command.has("find"), "find should be kept");
        assertTrue(command.has("normalField"), "normalField should be kept");
        assertEquals("should be kept", command.getString("normalField"));
        
        // Non-command fields should be preserved
        assertTrue(result.getJSONObject("attr").has("durationMillis"), "durationMillis should be kept");
        assertEquals(1000, result.getJSONObject("attr").getInt("durationMillis"));
    }
    
    @Test
    public void testMongosFieldTrimming() {
        // Test case from user's actual log message showing fields that should be filtered
        String logMessage = """
            {
                "t": {"$date": "2025-08-03T20:00:02.582+00:00"},
                "s": "I",
                "c": "COMMAND",
                "id": 51803,
                "ctx": "conn127293",
                "msg": "Slow query",
                "attr": {
                    "type": "command",
                    "ns": "m3_production.horsemen_instances",
                    "command": {
                        "find": "horsemen_instances",
                        "filter": {"insanityInstanceId": "0000a49458165a1754250668"},
                        "limit": 1,
                        "shardVersion": {"e": {"$oid": "623a27c07cfa9bf85b4e2174"}},
                        "clientOperationKey": {"$uuid": "a010d8d8-bbf3-4d3e-a605-f5ea1b9f8c47"},
                        "lsid": {"id": {"$uuid": "f0c99002-f0d0-4838-a62a-f75b1224c90c"}},
                        "$clusterTime": {"clusterTime": {"$timestamp": {"t": 1754251200, "i": 7125}}},
                        "$configTime": {"$timestamp": {"t": 1754251200, "i": 5872}},
                        "$topologyTime": {"$timestamp": {"t": 0, "i": 1}},
                        "$db": "m3_production"
                    },
                    "durationMillis": 1857
                }
            }
            """;

        String trimmed = LogRedactionUtil.trimLogMessage(logMessage);
        JSONObject result = new JSONObject(trimmed);
        
        JSONObject command = result.getJSONObject("attr").getJSONObject("command");
        
        // These fields should be removed from attr.command
        assertFalse(command.has("shardVersion"), "shardVersion should be removed from mongos logs");
        assertFalse(command.has("clientOperationKey"), "clientOperationKey should be removed");
        assertFalse(command.has("lsid"), "lsid should be removed from mongos logs");
        assertFalse(command.has("$clusterTime"), "$clusterTime should be removed from mongos logs");
        assertFalse(command.has("$configTime"), "$configTime should be removed from mongos logs");
        assertFalse(command.has("$topologyTime"), "$topologyTime should be removed from mongos logs");
        
        // These fields should be kept
        assertTrue(command.has("find"), "find should be kept");
        assertTrue(command.has("filter"), "filter should be kept");
        assertTrue(command.has("limit"), "limit should be kept");
        assertTrue(command.has("$db"), "$db should be kept");
        
        // Non-command fields should be preserved
        assertTrue(result.getJSONObject("attr").has("durationMillis"), "durationMillis should be kept");
        assertEquals(1857, result.getJSONObject("attr").getInt("durationMillis"));
    }
}