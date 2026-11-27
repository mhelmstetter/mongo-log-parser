package com.mongodb.log.parser;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Application tag carried in an operation's comment:
 * <ul>
 * <li>App length-prefixed: {@code 7:code_ns,7:billing,8:code_fqn,...} — repeated
 * {@code <len>:<text>} tokens separated by commas, alternating key, value.</li>
 * <li>Proxy (mongobetween): {@code origin_uri=<pod>-mongobetween;query_shape_hash=<h>} — the origin is
 * reported as the pod's deployment, labeled as a proxy-only tag. Other {@code key=value;...} comments
 * report their origin_uri, or are treated as text when they have none.</li>
 * <li>Free text (status TEXT): reported as {@code comment:<normalized text>}, where per whitespace token
 * UUIDs become {@code <uuid>}, numbers / versions / timestamps {@code <n>}, and hex ids of 8+ characters
 * with a digit {@code <hex>}, so per-request ids group into one tag.</li>
 * </ul>
 * The comment is taken from attr.command: comment, then filter/query $comment, then $comment in the first
 * $match stage of an aggregate pipeline ($comment at the top level of a filter or $match, or in an element
 * of a top-level $and / $or); only when absent there, from attr.originatingCommand (getMore) the same way.
 * A comment that is not a string, or that starts like the length-prefixed form but doesn't parse, is
 * reported as unrecognized, never guessed.
 */
public final class CommentTag {

    public enum Status { TAGGED, TEXT, UNTAGGED, UNRECOGNIZED }

    public static final String UNTAGGED = "(untagged)";
    public static final String UNRECOGNIZED = "(unrecognized comment format)";
    public static final String NO_ORIGIN_URI = "(tag without origin_uri)";
    public static final String NO_CODE_FQN = "(tag without code_fqn)";
    public static final String PROXY_ONLY = "(mongobetween tag only)";

    private static final Pattern KV_FORMAT = Pattern.compile("^[a-z_]+=.*", Pattern.DOTALL);
    private static final Pattern MONGOBETWEEN_POD = Pattern.compile("(.+)-[0-9a-f]{8,10}-[a-z0-9]{5}-mongobetween");
    private static final Pattern LENGTH_PREFIXED_START = Pattern.compile("^\\d+:.*", Pattern.DOTALL);
    private static final Pattern UUID = Pattern.compile("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");
    private static final Pattern NUM_TOKEN = Pattern.compile("[vV]?\\d[\\dTZ._:+-]*");
    private static final Pattern HEX_TOKEN = Pattern.compile("(?=[0-9a-fA-F]*\\d)[0-9a-fA-F]{8,}");
    private static final int MAX_TEXT = 80;

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
            Found f = commentInFilter(cmd.opt(key));
            if (f != null) {
                return f;
            }
        }
        Object pipeline = cmd.opt("pipeline");
        if (pipeline instanceof JSONArray && ((JSONArray) pipeline).length() > 0) {
            Object first = ((JSONArray) pipeline).opt(0);
            if (first instanceof JSONObject) {
                return commentInFilter(((JSONObject) first).opt("$match"));
            }
        }
        return null;
    }

    /** $comment at the top level of a filter / $match, or in an element of a top-level $and / $or. */
    private static Found commentInFilter(Object fObj) {
        if (!(fObj instanceof JSONObject)) {
            return null;
        }
        JSONObject f = (JSONObject) fObj;
        if (f.has("$comment")) {
            return new Found(f.get("$comment"));
        }
        for (String op : new String[] { "$and", "$or" }) {
            Object arr = f.opt(op);
            if (arr instanceof JSONArray) {
                for (int i = 0; i < ((JSONArray) arr).length(); i++) {
                    Object el = ((JSONArray) arr).opt(i);
                    if (el instanceof JSONObject && ((JSONObject) el).has("$comment")) {
                        return new Found(((JSONObject) el).get("$comment"));
                    }
                }
            }
        }
        return null;
    }

    /** Free-text comment with per-request ids replaced, per whitespace token. */
    static String normalizeText(String s) {
        StringBuilder out = new StringBuilder();
        for (String tok : s.trim().split("\\s+")) {
            if (tok.isEmpty()) {
                continue;
            }
            tok = UUID.matcher(tok).replaceAll("<uuid>");
            if (NUM_TOKEN.matcher(tok).matches()) {
                tok = "<n>";
            } else if (HEX_TOKEN.matcher(tok).matches()) {
                tok = "<hex>";
            }
            if (out.length() > 0) {
                out.append(' ');
            }
            out.append(tok);
        }
        String r = out.toString();
        return r.length() > MAX_TEXT ? r.substring(0, MAX_TEXT) : r;
    }

    private static CommentTag text(String comment, String source) {
        return new CommentTag(Status.TEXT, "comment:" + normalizeText(comment), NO_CODE_FQN, source);
    }

    /** Parse a comment string. A malformed length-prefixed comment is UNRECOGNIZED; other free text is TEXT. */
    public static CommentTag parse(String comment, String source) {
        if (KV_FORMAT.matcher(comment).matches()) {
            Map<String, String> kv = parseKv(comment);
            if (kv == null) {
                return text(comment, source);
            }
            String pod = kv.get("origin_uri");
            if (pod == null) {
                return text(comment, source);
            }
            Matcher m = MONGOBETWEEN_POD.matcher(pod);
            if (m.matches()) {
                return new CommentTag(Status.TAGGED, PROXY_ONLY + " deployment=" + m.group(1), PROXY_ONLY, source);
            }
            return new CommentTag(Status.TAGGED, pod, kv.containsKey("code_fqn") ? kv.get("code_fqn") : NO_CODE_FQN, source);
        }
        if (!LENGTH_PREFIXED_START.matcher(comment).matches()) {
            return text(comment, source);
        }
        Map<String, String> fields = parseLengthPrefixed(comment);
        if (fields == null) {
            return new CommentTag(Status.UNRECOGNIZED, UNRECOGNIZED, UNRECOGNIZED, source);
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
