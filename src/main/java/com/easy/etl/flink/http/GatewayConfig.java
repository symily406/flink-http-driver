package com.easy.etl.flink.http;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * {@link com.easy.etl.flink.http.gateway.SqlGatewayClient} 的连接设置。
 *
 * <p>地址格式（{@code jdbc:flink:} 前缀、末尾的 {@code /v1} 以及查询参数都是可选的）：
 * <pre>
 *   http://host:port[?key=value&amp;key=value...]
 *   https://host:port[?key=value...]
 *   jdbc:flink:http://host:port[?key=value...]   - 同义写法，JDBC 风格拼写
 * </pre>
 *
 * <p>可识别的参数：
 * <ul>
 *   <li>user / password - HTTP Basic 认证凭据</li>
 *   <li>connectTimeoutMs - HTTP 连接超时，默认 10000</li>
 *   <li>readTimeoutMs - HTTP 套接字读取超时，默认 60000</li>
 *   <li>pollIntervalMs - 操作状态/结果轮询间隔，默认 500</li>
 *   <li>operationTimeoutMs - 等待操作完成的最大时长，0 表示不限时</li>
 *   <li>maxRows - 光标最多抓取的行数，0（默认）表示不限</li>
 *   <li>rowFormat - 向网关请求的结果行格式：table（默认，等价于网关的 PLAIN_TEXT，
 *       每个值都序列化成字符串）或 json（数值保持 Long / BigDecimal）。
 *       两种拼写都接受：table / plain_text、json / JSON</li>
 *   <li>sessionProperty.XXX - 打开会话时作为会话属性 XXX 转发（例如
 *       sessionProperty.execution.runtime-mode=batch），等价于
 *       {@code SET 'execution.runtime-mode' = 'batch'}</li>
 * </ul>
 */
public final class GatewayConfig {

    /** 可选的 JDBC 风格前缀，接受它可以让已有 URL 继续工作。 */
    public static final String URL_PREFIX = "jdbc:flink:";

    /** 面向使用方的取值名：每行一个单元格、所有值都是字符串。 */
    public static final String ROW_FORMAT_TABLE = "table";

    /** 面向使用方的取值名：JSON 行，数值保持 Long / BigDecimal。 */
    public static final String ROW_FORMAT_JSON = "json";

    /** 网关在 HTTP 上真正认识的取值名（与上面对应）。 */
    public static final String WIRE_ROW_FORMAT_PLAIN_TEXT = "PLAIN_TEXT";

    /** 网关在 HTTP 上真正认识的取值名（与上面对应）。 */
    public static final String WIRE_ROW_FORMAT_JSON = "JSON";

    private String endpoint;                 // 例如 http://localhost:8083
    private String baseUrl;                  // endpoint + "/v1"
    private String user;
    private String password;
    private int connectTimeoutMs = 10_000;
    private int readTimeoutMs = 60_000;
    private int pollIntervalMs = 500;
    private long operationTimeoutMs = 0L;
    private int maxRows = 0;
    private String rowFormat = ROW_FORMAT_TABLE;
    private final Map<String, String> sessionProperties = new LinkedHashMap<String, String>();

    public GatewayConfig(String address) {
        applyAddress(address);
    }

    /** 解析一个地址。 */
    public static GatewayConfig parse(String address) {
        return new GatewayConfig(address);
    }

    /** {@link #parse(String)}，并携带额外设置，例如调用方提供的凭据。 */
    public static GatewayConfig parse(String address, Map<String, String> overrides) {
        GatewayConfig c = new GatewayConfig(address);
        if (overrides != null) {
            for (Map.Entry<String, String> e : overrides.entrySet()) {
                c.apply(e.getKey(), e.getValue());
            }
        }
        return c;
    }

    /** 为已持有 {@link Properties} 的调用方提供的 {@link #parse(String, Map)} 变体。 */
    public static GatewayConfig parse(String address, Properties overrides) {
        GatewayConfig c = new GatewayConfig(address);
        if (overrides != null) {
            for (String k : overrides.stringPropertyNames()) {
                c.apply(k, overrides.getProperty(k));
            }
        }
        return c;
    }

    /** 判断本配置能否处理某个地址。 */
    public static boolean acceptsAddress(String address) {
        if (address == null) {
            return false;
        }
        String s = stripPrefix(address.trim()).toLowerCase(java.util.Locale.ROOT);
        return s.startsWith("http://") || s.startsWith("https://");
    }

    /** 去掉开头的 {@code jdbc:flink:} 前缀（不区分大小写）。 */
    private static String stripPrefix(String address) {
        String lower = address.toLowerCase(java.util.Locale.ROOT);
        return lower.startsWith(URL_PREFIX) ? address.substring(URL_PREFIX.length()) : address;
    }

    private void applyAddress(String address) {
        String rest = stripPrefix(address == null ? "" : address.trim());
        if (!acceptsAddress(rest)) {
            throw new IllegalArgumentException(
                    "无效的地址。期望 http://host:port、https://host:port"
                            + " 或 jdbc:flink:http://host:port，但得到的是：" + address);
        }
        int queryIdx = rest.indexOf('?');
        String query = null;
        if (queryIdx >= 0) {
            query = rest.substring(queryIdx + 1);
            rest = rest.substring(0, queryIdx);
        }
        while (rest.endsWith("/")) {
            rest = rest.substring(0, rest.length() - 1);
        }
        if (rest.endsWith("/v1") || rest.endsWith("/v2")) {
            rest = rest.substring(0, rest.lastIndexOf('/'));
        }
        this.endpoint = rest;
        this.baseUrl = rest + "/v1";

        if (query != null) {
            for (String pair : query.split("&")) {
                if (pair.isEmpty()) {
                    continue;
                }
                int eq = pair.indexOf('=');
                String k = eq < 0 ? pair : pair.substring(0, eq);
                String v = eq < 0 ? "" : pair.substring(eq + 1);
                apply(decode(k), decode(v));
            }
        }
    }

    private static String decode(String v) {
        try {
            return URLDecoder.decode(v, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }
    }

    private void apply(String key, String value) {
        if (key == null || value == null) {
            return;
        }
        if (key.startsWith("sessionProperty.")) {
            sessionProperties.put(key.substring("sessionProperty.".length()), value);
            return;
        }
        if ("user".equals(key) || "username".equalsIgnoreCase(key)) {
            user = value;
        } else if ("password".equalsIgnoreCase(key)) {
            password = value;
        } else if ("connectTimeoutMs".equalsIgnoreCase(key)) {
            connectTimeoutMs = parseInt(key, value);
        } else if ("readTimeoutMs".equalsIgnoreCase(key)) {
            readTimeoutMs = parseInt(key, value);
        } else if ("pollIntervalMs".equalsIgnoreCase(key)) {
            pollIntervalMs = Math.max(50, parseInt(key, value));
        } else if ("operationTimeoutMs".equalsIgnoreCase(key)) {
            operationTimeoutMs = parseLong(key, value);
        } else if ("maxRows".equalsIgnoreCase(key)) {
            maxRows = parseInt(key, value);
        } else if ("rowFormat".equalsIgnoreCase(key)) {
            this.rowFormat = normalizeRowFormat(value);
        }
        // 未知的键被忽略。
    }

    /**
     * 归一化 {@code rowFormat}：同时接受面向使用方的 {@code table} / {@code json}
     * 与网关自身的 {@code PLAIN_TEXT} / {@code JSON} 拼写。
     *
     * @throws IllegalArgumentException 取值不在上述集合内
     */
    public static String normalizeRowFormat(String value) {
        String format = value == null ? "" : value.trim().toLowerCase(java.util.Locale.ROOT);
        if (ROW_FORMAT_TABLE.equals(format) || "plain_text".equals(format)) {
            return ROW_FORMAT_TABLE;
        }
        if (ROW_FORMAT_JSON.equals(format)) {
            return ROW_FORMAT_JSON;
        }
        throw new IllegalArgumentException(
                "属性 'rowFormat' 必须是 'table' / 'plain_text' / 'json' 之一，但得到的是：" + value);
    }

    private static int parseInt(String key, String value) {
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "属性 '" + key + "' 期望一个整数，但得到的是：" + value, e);
        }
    }

    private static long parseLong(String key, String value) {
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "属性 '" + key + "' 期望一个长整数，但得到的是：" + value, e);
        }
    }

    public boolean hasCredentials() {
        return user != null && !user.isEmpty();
    }

    public String getBasicAuthHeader() {
        if (!hasCredentials()) {
            return null;
        }
        String raw = user + ":" + (password == null ? "" : password);
        return "Basic " + java.util.Base64.getEncoder()
                .encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    }

    // ------------------------------------------------------------------
    // getter / setter
    // ------------------------------------------------------------------

    public String getEndpoint() {
        return endpoint;
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getConnectTimeoutMs() {
        return connectTimeoutMs;
    }

    public void setConnectTimeoutMs(int connectTimeoutMs) {
        this.connectTimeoutMs = connectTimeoutMs;
    }

    public int getReadTimeoutMs() {
        return readTimeoutMs;
    }

    public void setReadTimeoutMs(int readTimeoutMs) {
        this.readTimeoutMs = readTimeoutMs;
    }

    public int getPollIntervalMs() {
        return pollIntervalMs;
    }

    public void setPollIntervalMs(int pollIntervalMs) {
        this.pollIntervalMs = pollIntervalMs;
    }

    public long getOperationTimeoutMs() {
        return operationTimeoutMs;
    }

    public void setOperationTimeoutMs(long operationTimeoutMs) {
        this.operationTimeoutMs = operationTimeoutMs;
    }

    /** 游标最多读取的行数；{@code 0} 表示不限。 */
    public int getMaxRows() {
        return maxRows;
    }

    public void setMaxRows(int maxRows) {
        this.maxRows = maxRows;
    }

    /** 配置的取值名：{@code table}（默认）或 {@code json}。 */
    public String getRowFormat() {
        return rowFormat;
    }

    /**
     * 真正拼到请求 URL 上的 {@code rowFormat} 取值：{@code PLAIN_TEXT} / {@code JSON}。
     *
     * <p>网关只认这两个枚举名（Flink 的 {@code RowFormat}），传 {@code table} 之类的
     * 别名会被校验型网关直接拒绝，所以这里做一次映射。
     */
    public String getResultRowFormat() {
        return ROW_FORMAT_JSON.equals(rowFormat)
                ? WIRE_ROW_FORMAT_JSON : WIRE_ROW_FORMAT_PLAIN_TEXT;
    }

    /** 设置结果行格式；接受 {@code table} / {@code plain_text} / {@code json}。 */
    public void setRowFormat(String rowFormat) {
        this.rowFormat = normalizeRowFormat(rowFormat);
    }

    /** 打开会话时发送的会话属性；从不为 null。 */
    public Map<String, String> getSessionProperties() {
        return Collections.unmodifiableMap(sessionProperties);
    }

    /** 新增（或替换）一个会话属性，例如 {@code execution.runtime-mode}。 */
    public void setSessionProperty(String key, String value) {
        sessionProperties.put(key, value);
    }

    @Override
    public String toString() {
        return "GatewayConfig{endpoint=" + endpoint + ", user=" + user
                + ", rowFormat=" + rowFormat + ", sessionProperties=" + sessionProperties + "}";
    }
}
