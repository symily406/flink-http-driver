package com.easy.etl.flink.http.model;

import com.easy.etl.flink.http.util.Json;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * 集群日志行，来自 {@code /jobmanager/log}、{@code /taskmanagers/{id}/log}
 * 这类日志端点。
 *
 * <p>两种响应形态都要兼容：
 * <ul>
 *   <li>Flink 1.x：{@code {"out":["a","b"]}}（元素也可能是 {@code {"line":"..."}} 对象）</li>
 *   <li>Flink 2.x：直接返回 {@code text/plain} 的原始日志文本</li>
 * </ul>
 */
public final class ClusterLog {

    private static final ClusterLog EMPTY = new ClusterLog(Collections.<String>emptyList());

    /** 日志条目可能携带的字段名，取决于 Flink 版本。 */
    private static final String[] LINE_KEYS = {"line", "message", "log", "value", "text"};

    private final List<String> lines;

    public ClusterLog(List<String> lines) {
        this.lines = Collections.unmodifiableList(new ArrayList<String>(
                lines == null ? Collections.<String>emptyList() : lines));
    }

    /**
     * 解析日志端点的响应体：先按 {@code {"out":[...]}} JSON 尝试，不是 JSON
     * （Flink 2.x 返回 {@code text/plain} 原始日志）就按行切分。
     */
    public static ClusterLog parse(String body) {
        if (body == null) {
            return EMPTY;
        }
        String trimmed = stripBom(body).trim();
        if (trimmed.isEmpty()) {
            return EMPTY;
        }
        if (trimmed.charAt(0) == '{') {
            try {
                return fromJson(Json.parseObject(trimmed));
            } catch (RuntimeException e) {
                // 不是我们认识的 JSON 形态，按纯文本处理
            }
        }
        return fromText(stripBom(body));
    }

    /** 按行切分 {@code text/plain} 形式的日志文本。 */
    public static ClusterLog fromText(String text) {
        if (text == null || text.isEmpty()) {
            return EMPTY;
        }
        List<String> out = new ArrayList<String>();
        for (String line : stripBom(text).split("\r?\n")) {
            if (!line.isEmpty()) {
                out.add(line);
            }
        }
        return new ClusterLog(out);
    }

    private static String stripBom(String text) {
        return text != null && text.startsWith("\uFEFF") ? text.substring(1) : text;
    }

    public static ClusterLog fromJson(Map<String, Object> json) {
        Object list = Json.get(json, "out");
        if (!(list instanceof List)) {
            return EMPTY;
        }
        List<String> out = new ArrayList<String>();
        for (Object entry : (List<?>) list) {
            String line = entry == null ? null
                    : entry instanceof String ? (String) entry : firstText(entry);
            if (line != null && !line.isEmpty()) {
                out.add(line);
            }
        }
        return new ClusterLog(out);
    }

    public static ClusterLog empty() {
        return EMPTY;
    }

    @SuppressWarnings("unchecked")
    private static String firstText(Object entry) {
        if (!(entry instanceof Map)) {
            return null;
        }
        Map<String, Object> m = (Map<String, Object>) entry;
        for (String key : LINE_KEYS) {
            String v = Json.getStr(m, key);
            if (v != null && !v.isEmpty()) {
                return v;
            }
        }
        return null;
    }

    public List<String> getLines() {
        return lines;
    }

    /** 第一条非空白行——通常是日志的开头——或 null。 */
    public String getFirstLine() {
        for (String line : lines) {
            if (line != null && !line.trim().isEmpty()) {
                return line.trim();
            }
        }
        return null;
    }

    /**
     * 日志末尾的 {@code n} 行，用于限制日志体积。
     *
     * <p>存在的理由：日志端点虽然接受 {@code ?size=N}，但 Flink 2.x 实测**会忽略它**
     * （{@code GET /jobmanager/log?size=5} 与不带参数返回的行数完全一致，都是整份
     * 日志），所以想要"只看最后几行"只能在客户端截断，例如
     * {@code fetchJobManagerLog(url).tail(500)}。
     *
     * @param n 保留的行数；{@code <= 0} 时返回空日志，大于总行数时返回自身
     */
    public ClusterLog tail(int n) {
        if (n <= 0 || lines.isEmpty()) {
            return EMPTY;
        }
        if (n >= lines.size()) {
            return this;
        }
        return new ClusterLog(new ArrayList<String>(lines.subList(lines.size() - n, lines.size())));
    }

    public boolean isEmpty() {
        return lines.isEmpty();
    }

    @Override
    public String toString() {
        return "ClusterLog{lines=" + lines.size() + "}";
    }
}
