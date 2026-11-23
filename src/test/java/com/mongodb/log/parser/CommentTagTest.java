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
        assertEquals(CommentTag.Status.UNRECOGNIZED, CommentTag.parse("free text comment", "command").status);
        assertEquals(CommentTag.Status.UNRECOGNIZED, CommentTag.parse("3:abc,2:x", "command").status);
        assertEquals(CommentTag.Status.UNRECOGNIZED, CommentTag.parse("origin_uri=not-a-pod", "command").status);
        assertNull(CommentTag.parseLengthPrefixed("5:ab"));
    }
}
