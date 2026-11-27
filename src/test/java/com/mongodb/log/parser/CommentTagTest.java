package com.mongodb.log.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class CommentTagTest {

    private static final String APP = "7:code_ns,7:billing,8:code_fqn,10:pkg.module,"
            + "10:origin_uri,12:job://x.y:zz,16:query_shape_hash,16:0123456789abcdef";

    @Test
    public void appTagFromCommandComment() {
        JSONObject attr = new JSONObject().put("command", new JSONObject().put("find", "c").put("comment", APP));
        CommentTag t = CommentTag.fromAttr(attr);
        assertEquals(CommentTag.Status.TAGGED, t.status);
        assertEquals("job://x.y:zz", t.originUri);
        assertEquals("pkg.module", t.codeFqn);
        assertEquals("command", t.source);
    }

    @Test
    public void proxyTagReportsDeployment() {
        CommentTag t = CommentTag.parse(
                "origin_uri=billing-worker-6d4cf9b8d5-x7k2p-mongobetween;query_shape_hash=fedcba9876543210", "command");
        assertEquals(CommentTag.Status.TAGGED, t.status);
        assertEquals(CommentTag.PROXY_ONLY + " deployment=billing-worker", t.originUri);
        assertEquals(CommentTag.PROXY_ONLY, t.codeFqn);
    }

    @Test
    public void precedenceCommandThenFilterThenOriginating() {
        JSONObject attr = new JSONObject()
                .put("command", new JSONObject().put("getMore", 1).put("comment", APP))
                .put("originatingCommand", new JSONObject().put("comment", "2:origin_uri,1:o"));
        assertEquals("command", CommentTag.fromAttr(attr).source);

        attr = new JSONObject()
                .put("command", new JSONObject().put("getMore", 1))
                .put("originatingCommand", new JSONObject().put("filter", new JSONObject().put("$comment", "10:origin_uri,1:o")));
        CommentTag t = CommentTag.fromAttr(attr);
        assertEquals("originatingCommand", t.source);
        assertEquals("o", t.originUri);
        assertEquals(CommentTag.NO_CODE_FQN, t.codeFqn);
    }

    @Test
    public void untaggedAndUnrecognized() {
        assertEquals(CommentTag.Status.UNTAGGED, CommentTag.fromAttr(new JSONObject().put("command", new JSONObject().put("find", "c"))).status);
        CommentTag t = CommentTag.fromAttr(new JSONObject().put("command", new JSONObject().put("comment", new JSONObject().put("a", 1))));
        assertEquals(CommentTag.Status.UNRECOGNIZED, t.status);
        assertEquals(CommentTag.UNRECOGNIZED, t.originUri);
        assertEquals(CommentTag.Status.UNRECOGNIZED, CommentTag.parse("3:abc,2:x", "command").status);
        assertNull(CommentTag.parseLengthPrefixed("5:ab"));
    }

    @Test
    public void freeTextAndKeyValueComments() {
        CommentTag t = CommentTag.parse("free text comment", "command");
        assertEquals(CommentTag.Status.TEXT, t.status);
        assertEquals("comment:free text comment", t.originUri);
        assertEquals(CommentTag.NO_CODE_FQN, t.codeFqn);
        // per-request ids group into one tag
        assertEquals("comment:<hex> checkout-svc web <n> <uuid>",
                CommentTag.parse("3f9c2a7b81de checkout-svc web 01.02.202601011200 0b7e4c1a-2d3f-4e5a-9b8c-7d6e5f4a3b2c", "command").originUri);
        assertEquals("comment:report <n> run <n> user <n>", CommentTag.parse("report v2.3 run 2026-11-27T07:00:00Z user 12345", "command").originUri);
        // key=value: origin_uri when present (not a mongobetween pod), otherwise the text
        CommentTag kv = CommentTag.parse("origin_uri=not-a-pod", "command");
        assertEquals(CommentTag.Status.TAGGED, kv.status);
        assertEquals("not-a-pod", kv.originUri);
        assertEquals("comment:job=nightly-export", CommentTag.parse("job=nightly-export", "command").originUri);
    }

    @Test
    public void commentInAggregatePipelineAndBooleanFilters() {
        JSONObject match = new JSONObject().put("updateDate", new JSONObject().put("$gt", 1)).put("$comment", "oldest_by_status");
        JSONObject agg = new JSONObject().put("aggregate", "Orders")
                .put("pipeline", new org.json.JSONArray().put(new JSONObject().put("$match", match)));
        CommentTag t = CommentTag.fromAttr(new JSONObject().put("command", agg));
        assertEquals(CommentTag.Status.TEXT, t.status);
        assertEquals("comment:oldest_by_status", t.originUri);
        assertEquals("command", t.source);

        // getMore: from the originating aggregate, with $comment inside a top-level $and
        JSONObject andMatch = new JSONObject().put("$and", new org.json.JSONArray()
                .put(new JSONObject().put("a", 1)).put(new JSONObject().put("$comment", "10:origin_uri,3:svc")));
        JSONObject orig = new JSONObject().put("aggregate", "Orders")
                .put("pipeline", new org.json.JSONArray().put(new JSONObject().put("$match", andMatch)));
        CommentTag g = CommentTag.fromAttr(new JSONObject().put("command", new JSONObject().put("getMore", 1)).put("originatingCommand", orig));
        assertEquals(CommentTag.Status.TAGGED, g.status);
        assertEquals("svc", g.originUri);
        assertEquals("originatingCommand", g.source);

        // only the first stage is consulted
        JSONObject late = new JSONObject().put("aggregate", "x").put("pipeline", new org.json.JSONArray()
                .put(new JSONObject().put("$project", new JSONObject())).put(new JSONObject().put("$match", new JSONObject().put("$comment", "late"))));
        assertEquals(CommentTag.Status.UNTAGGED, CommentTag.fromAttr(new JSONObject().put("command", late)).status);
    }
}
