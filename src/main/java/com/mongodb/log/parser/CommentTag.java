package com.mongodb.log.parser;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONObject;

/**
 * Application tag carried in an operation's comment, in one of two formats:
 * <ul>
 * <li>App length-prefixed: {@code 7:code_ns,7:billing,8:code_fqn,...} — repeated
 * {@code <len>:<text>} tokens separated by commas, alternating key, value.</li>
 * <li>Proxy (mongobetween): {@code origin_uri=<pod>-mongobetween;query_shape_hash=<h>} — the origin is
 * reported as the pod's deployment, labeled as a proxy-only tag.</li>
 * </ul>
 * The comment is taken from attr.command (comment, then filter.$comment / query.$comment), and only
 * when absent there from attr.originatingCommand (getMore). A comment that is not a string or matches
 * neither format is reported as unrecognized, never guessed.
 */
public final class CommentTag {

    public enum Status { TAGGED, UNTAGGED, UNRECOGNIZED }

    public static final String UNTAGGED = "(untagged)";
    public static final String UNRECOGNIZED = "(unrecognized comment format)";
    public static final String NO_ORIGIN_URI = "(tag without origin_uri)";
    public static final String NO_CODE_FQN = "(tag without code_fqn)";
    public static final String PROXY_ONLY = "(mongobetween tag only)";

    private static final Pattern KV_FORMAT = Pattern.compile("^[a-z_]+=.*", Pattern.DOTALL);
    private static final Pattern MONGOBETWEEN_POD = Pattern.compile("(.+)-[0-9a-f]{8,10}-[a-z0-9]{5}-mongobetween");

    public final Status status;
    public final String originUri;
    public final String codeFqn;
    /** "command" or "originatingCommand"; null when untagged. */
    public final String source;

    private CommentTag(Status status, String originUri, String codeFqn, String source) {
        this.status = status;
        this.originUri = originUri;
        this.codeFqn = codeFqn;
        this.source = source;
    }

    private static final CommentTag UNTAGGED_TAG = new CommentTag(Status.UNTAGGED, UNTAGGED, UNTAGGED, null);

    /** Comment found in a command object, possibly not a string. */
    private static final class Found {
        final Object value;
        Found(Object value) { this.value = value; }
    }

    public static CommentTag fromAttr(JSONObject attr) {
        String source = "command";
        Found found = commentOf(attr.opt("command"));
        if (found == null) {
            source = "originatingCommand";
            found = commentOf(attr.opt("originatingCommand"));
        }
        if (found == null) {
            return UNTAGGED_TAG;
        }
        if (!(found.value instanceof String)) {
            return new CommentTag(Status.UNRECOGNIZED, UNRECOGNIZED, UNRECOGNIZED, source);
        }
        return parse((String) found.value, source);
    }

    private static Found commentOf(Object cmdObj) {
        if (!(cmdObj instanceof JSONObject)) {
            return null;
        }
        JSONObject cmd = (JSONObject) cmdObj;
        if (cmd.has("comment")) {
            return new Found(cmd.get("comment"));
        }
        for (String key : new String[] { "filter", "query" }) {
            Object f = cmd.opt(key);
            if (f instanceof JSONObject && ((JSONObject) f).has("$comment")) {
                return new Found(((JSONObject) f).get("$comment"));
            }
        }
        return null;
    }

    /** Parse a comment string; returns an UNRECOGNIZED tag when it matches neither format. */
    public static CommentTag parse(String comment, String source) {
        Map<String, String> fields = KV_FORMAT.matcher(comment).matches() ? parseKv(comment) : parseLengthPrefixed(comment);
        if (fields == null) {
            return new CommentTag(Status.UNRECOGNIZED, UNRECOGNIZED, UNRECOGNIZED, source);
        }
        if (KV_FORMAT.matcher(comment).matches()) {
            String pod = fields.get("origin_uri");
            Matcher m = pod != null ? MONGOBETWEEN_POD.matcher(pod) : null;
            if (m == null || !m.matches()) {
                return new CommentTag(Status.UNRECOGNIZED, UNRECOGNIZED, UNRECOGNIZED, source);
            }
            return new CommentTag(Status.TAGGED, PROXY_ONLY + " deployment=" + m.group(1), PROXY_ONLY, source);
        }
        String origin = fields.containsKey("origin_uri") ? fields.get("origin_uri") : NO_ORIGIN_URI;
        String fqn = fields.containsKey("code_fqn") ? fields.get("code_fqn") : NO_CODE_FQN;
        return new CommentTag(Status.TAGGED, origin, fqn, source);
    }

    /** {@code k=v;k=v}; null when a part has no '='. */
    static Map<String, String> parseKv(String s) {
        Map<String, String> out = new LinkedHashMap<>();
        for (String part : s.split(";", -1)) {
            int eq = part.indexOf('=');
            if (eq < 0) {
                return null;
            }
            out.put(part.substring(0, eq), part.substring(eq + 1));
        }
        return out;
    }

    /** {@code <len>:<text>,<len>:<text>,...} alternating key, value; lengths count code points. Null when malformed. */
    static Map<String, String> parseLengthPrefixed(String s) {
        List<String> vals = new ArrayList<>();
        int i = 0;
        int n = s.length();
        while (i < n) {
            int digitsStart = i;
            while (i < n && Character.isDigit(s.charAt(i)) && s.charAt(i) < 128) {
                i++;
            }
            if (i == digitsStart || i >= n || s.charAt(i) != ':') {
                return null;
            }
            long len;
            try {
                len = Long.parseLong(s.substring(digitsStart, i));
            } catch (NumberFormatException e) {
                return null;
            }
            i++; // ':'
            int end;
            try {
                end = s.offsetByCodePoints(i, (int) Math.min(len, Integer.MAX_VALUE));
            } catch (IndexOutOfBoundsException e) {
                return null;
            }
            vals.add(s.substring(i, end));
            i = end;
            if (i < n) {
                if (s.charAt(i) != ',') {
                    return null;
                }
                i++;
            }
        }
        if (vals.size() % 2 != 0) {
            return null;
        }
        Map<String, String> out = new LinkedHashMap<>();
        for (int k = 0; k < vals.size(); k += 2) {
            out.put(vals.get(k), vals.get(k + 1));
        }
        return out;
    }
}
