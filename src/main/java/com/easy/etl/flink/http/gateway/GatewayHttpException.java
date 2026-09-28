package com.easy.etl.flink.http.gateway;

import com.easy.etl.flink.http.util.Json;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * HTTP 层失败：状态码不是 2xx。
 *
 * <p>Flink 的 REST 端点在失败时返回的 body 形如 {@code {"errors":[...]}}，
 * 其中往往带着真正的根因（尤其当 SQL Gateway 的 operation 以 ERROR 结束时，
 * {@code GET .../result/{token}} 会直接回 500，错误堆栈就放在 body 里）。
 * 普通 {@link IOException} 只能看到一行拼接出来的文本，所以这里单独把一个
 * 异常类型拎出来，让调用方可以拿到 {@link #getStatusCode()} 与
 * {@link #getBody()}，并通过 {@link #getErrorMessage()} 直接取到提炼后的根因。
 *
 * <p>它继承 {@link IOException}，因此对既有代码是透明的：所有
 * {@code throws IOException} 的签名与 {@code catch (IOException)} 的写法都不需要改。
 */
public class GatewayHttpException extends IOException {

    private static final long serialVersionUID = 1L;

    /** Flink 在内部异常时使用的占位文本，本身不携带任何信息。 */
    private static final String GENERIC_ERROR = "Internal server error.";

    private final int statusCode;
    private final String url;
    private final String body;

    public GatewayHttpException(int statusCode, String statusMessage, String url, String body) {
        super("HTTP " + statusCode
                + (statusMessage == null || statusMessage.isEmpty() ? "" : " " + statusMessage)
                + "，请求 " + url
                + (body == null || body.trim().isEmpty() ? "" : " - " + body));
        this.statusCode = statusCode;
        this.url = url;
        this.body = body;
    }

    /** HTTP 状态码，例如 404 / 500。 */
    public int getStatusCode() {
        return statusCode;
    }

    /** 发起请求的完整 URL。 */
    public String getUrl() {
        return url;
    }

    /** 响应体原文；没有响应体时为 {@code null}。 */
    public String getBody() {
        return body;
    }

    /**
     * 从响应体里提炼出的可读错误文本：优先取 Flink {@code errors} 数组里第一条
     * 有实质内容的记录，再从中抽出最深一层的 {@code Caused by}（真正的根因）。
     *
     * @return 提炼后的错误文本；无法提炼时返回 {@code null}
     */
    public String getErrorMessage() {
        String text = extractErrors(body);
        if (text == null) {
            return null;
        }
        String root = rootCause(text);
        return root == null ? firstNonEmptyLine(text) : root;
    }

    /**
     * 从 Flink 返回的 {@code {"errors":[...]}} 中取出第一条实质错误文本，
     * 跳过 {@code "Internal server error."} 这种占位内容。
     *
     * @return 错误文本；body 不是 JSON、没有 errors 数组、或数组里全是占位文本时返回 {@code null}
     */
    @SuppressWarnings("unchecked")
    public static String extractErrors(String body) {
        if (body == null || body.trim().isEmpty()) {
            return null;
        }
        Object parsed;
        try {
            parsed = Json.parse(body);
        } catch (RuntimeException e) {
            // 不是 JSON：整段 body 就是错误信息（例如反向代理返回的 HTML）
            String trimmed = body.trim();
            return trimmed.isEmpty() ? null : trimmed;
        }
        if (!(parsed instanceof Map)) {
            return null;
        }
        Object errors = Json.get((Map<String, Object>) parsed, "errors");
        if (!(errors instanceof List)) {
            return null;
        }
        for (Object error : (List<?>) errors) {
            if (error == null) {
                continue;
            }
            String text = String.valueOf(error).trim();
            if (text.isEmpty() || GENERIC_ERROR.equalsIgnoreCase(text)) {
                continue;
            }
            return text;
        }
        return null;
    }

    /**
     * 从 Java 异常堆栈文本里抽出真正的根因：最深一层的
     * {@code Caused by: xxx} 所在行的内容。
     *
     * <p>找不到 {@code Caused by} 时回退到第一行异常声明，
     * 形如 {@code org.apache.flink...ValidationException: SQL validation failed. ...}。
     *
     * @return 根因文本；文本里没有任何异常信息时返回 {@code null}
     */
    public static String rootCause(String text) {
        if (text == null || text.trim().isEmpty()) {
            return null;
        }
        String[] lines = text.split("\n");
        String deepest = null;
        String firstDeclaration = null;
        for (String raw : lines) {
            String line = raw.trim();
            if (line.startsWith("Caused by: ")) {
                deepest = line.substring("Caused by: ".length()).trim();
            } else if (firstDeclaration == null && line.matches("^[\\w.$]+\\.(\\w+Exception|\\w*Error|Throwable)\\b.*")) {
                firstDeclaration = line;
            }
        }
        if (deepest != null && !deepest.isEmpty()) {
            return deepest;
        }
        return firstDeclaration;
    }

    private static String firstNonEmptyLine(String text) {
        for (String raw : text.split("\n")) {
            String line = raw.trim();
            if (!line.isEmpty()) {
                return line;
            }
        }
        return null;
    }
}
