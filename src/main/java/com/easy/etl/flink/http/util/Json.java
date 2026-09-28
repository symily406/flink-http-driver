package com.easy.etl.flink.http.util;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 无第三方依赖的极简 JSON 解析 / 序列化工具（基于 JDK 1.8）。
 *
 * <p>解析得到的类型包括：{@code Map<String,Object>}、{@code List<Object>}、
 * {@code String}、{@code Long}、{@code BigDecimal}、{@code Boolean}、{@code null}。
 * 整数字面量会被解析为 {@code Long}（超出 long 范围时解析为 {@code BigDecimal}），
 * 以保证 BIGINT 和 DECIMAL 值不失精度；带小数或指数的字面量会被解析为
 * {@code BigDecimal}。访问数字时请使用 {@link #asLong(Object)} / {@link #asInt(Object)}。
 */
public final class Json {

    private Json() {
    }

    // ------------------------------------------------------------------
    // 解析
    // ------------------------------------------------------------------

    public static Object parse(String text) {
        Parser p = new Parser(text);
        p.skipWs();
        Object v = p.value();
        p.skipWs();
        if (!p.eof()) {
            throw new IllegalArgumentException("位置 " + p.pos + " 之后存在尾随字符");
        }
        return v;
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> parseObject(String text) {
        Object v = parse(text);
        if (!(v instanceof Map)) {
            throw new IllegalArgumentException("期望 JSON 对象，但得到：" + text);
        }
        return (Map<String, Object>) v;
    }

    @SuppressWarnings("unchecked")
    public static List<Object> parseArray(String text) {
        Object v = parse(text);
        if (!(v instanceof List)) {
            throw new IllegalArgumentException("期望 JSON 数组，但得到：" + text);
        }
        return (List<Object>) v;
    }

    private static final class Parser {
        final String s;
        int pos;

        Parser(String s) {
            this.s = s == null ? "" : s;
        }

        boolean eof() {
            return pos >= s.length();
        }

        char peek() {
            if (eof()) {
                throw new IllegalArgumentException("JSON 输入意外结束");
            }
            return s.charAt(pos);
        }

        void expect(char c) {
            if (eof() || s.charAt(pos) != c) {
                throw new IllegalArgumentException("位置 " + pos + " 期望 '" + c + "'");
            }
            pos++;
        }

        void skipWs() {
            while (!eof()) {
                char c = s.charAt(pos);
                if (c == ' ' || c == '\t' || c == '\r' || c == '\n') {
                    pos++;
                } else {
                    break;
                }
            }
        }

        Object value() {
            skipWs();
            char c = peek();
            switch (c) {
                case '{':
                    return object();
                case '[':
                    return array();
                case '"':
                    return string();
                case 't':
                    literal("true");
                    return Boolean.TRUE;
                case 'f':
                    literal("false");
                    return Boolean.FALSE;
                case 'n':
                    literal("null");
                    return null;
                default:
                    return number();
            }
        }

        Map<String, Object> object() {
            expect('{');
            Map<String, Object> m = new LinkedHashMap<String, Object>();
            skipWs();
            if (!eof() && peek() == '}') {
                pos++;
                return m;
            }
            while (true) {
                skipWs();
                String key = string();
                skipWs();
                expect(':');
                Object v = value();
                m.put(key, v);
                skipWs();
                char c = peek();
                if (c == ',') {
                    pos++;
                } else if (c == '}') {
                    pos++;
                    return m;
                } else {
                    throw new IllegalArgumentException("位置 " + pos + " 期望 ',' 或 '}'");
                }
            }
        }

        List<Object> array() {
            expect('[');
            List<Object> l = new ArrayList<Object>();
            skipWs();
            if (!eof() && peek() == ']') {
                pos++;
                return l;
            }
            while (true) {
                Object v = value();
                l.add(v);
                skipWs();
                char c = peek();
                if (c == ',') {
                    pos++;
                } else if (c == ']') {
                    pos++;
                    return l;
                } else {
                    throw new IllegalArgumentException("位置 " + pos + " 期望 ',' 或 ']'");
                }
            }
        }

        String string() {
            expect('"');
            StringBuilder sb = new StringBuilder();
            while (true) {
                if (eof()) {
                    throw new IllegalArgumentException("字符串未终止");
                }
                char c = s.charAt(pos++);
                if (c == '"') {
                    return sb.toString();
                }
                if (c == '\\') {
                    if (eof()) {
                        throw new IllegalArgumentException("转义序列未终止");
                    }
                    char e = s.charAt(pos++);
                    switch (e) {
                        case '"': sb.append('"'); break;
                        case '\\': sb.append('\\'); break;
                        case '/': sb.append('/'); break;
                        case 'b': sb.append('\b'); break;
                        case 'f': sb.append('\f'); break;
                        case 'n': sb.append('\n'); break;
                        case 'r': sb.append('\r'); break;
                        case 't': sb.append('\t'); break;
                        case 'u':
                            if (pos + 4 > s.length()) {
                                throw new IllegalArgumentException("无效的 unicode 转义");
                            }
                            sb.append((char) Integer.parseInt(s.substring(pos, pos + 4), 16));
                            pos += 4;
                            break;
                        default:
                            throw new IllegalArgumentException("无效的转义 '\\" + e + "'");
                    }
                } else {
                    sb.append(c);
                }
            }
        }

        Object number() {
            int start = pos;
            if (!eof() && (peek() == '-' || peek() == '+')) {
                pos++;
            }
            boolean isFloating = false;
            while (!eof()) {
                char c = s.charAt(pos);
                if (c >= '0' && c <= '9') {
                    pos++;
                } else if (c == '.' || c == 'e' || c == 'E' || c == '+' || c == '-') {
                    isFloating = isFloating || c == '.' || c == 'e' || c == 'E';
                    pos++;
                } else {
                    break;
                }
            }
            if (pos == start) {
                throw new IllegalArgumentException("位置 " + pos + " 存在无效的 JSON token");
            }
            String num = s.substring(start, pos);
            if (!isFloating) {
                try {
                    return Long.valueOf(num);
                } catch (NumberFormatException e) {
                    // 超出 long 范围（例如 DECIMAL(38,0)）；保留每一位数字。
                    return new BigDecimal(num);
                }
            }
            return new BigDecimal(num);
        }

        void literal(String word) {
            if (!s.startsWith(word, pos)) {
                throw new IllegalArgumentException("位置 " + pos + " 存在无效的字面量");
            }
            pos += word.length();
        }
    }

    // ------------------------------------------------------------------
    // 序列化
    // ------------------------------------------------------------------

    public static String write(Map<String, Object> obj) {
        StringBuilder sb = new StringBuilder(32);
        writeValue(sb, obj);
        return sb.toString();
    }

    private static void writeValue(StringBuilder sb, Object v) {
        if (v == null) {
            sb.append("null");
        } else if (v instanceof String) {
            writeString(sb, (String) v);
        } else if (v instanceof BigDecimal) {
            sb.append(((BigDecimal) v).toPlainString());
        } else if (v instanceof Boolean || v instanceof Number) {
            sb.append(v.toString());
        } else if (v instanceof Map) {
            sb.append('{');
            boolean first = true;
            for (Map.Entry<?, ?> e : ((Map<?, ?>) v).entrySet()) {
                if (!first) {
                    sb.append(',');
                }
                first = false;
                writeString(sb, String.valueOf(e.getKey()));
                sb.append(':');
                writeValue(sb, e.getValue());
            }
            sb.append('}');
        } else if (v instanceof Iterable) {
            sb.append('[');
            boolean first = true;
            for (Object o : (Iterable<?>) v) {
                if (!first) {
                    sb.append(',');
                }
                first = false;
                writeValue(sb, o);
            }
            sb.append(']');
        } else {
            writeString(sb, v.toString());
        }
    }

    private static void writeString(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"': sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\b': sb.append("\\b"); break;
                case '\f': sb.append("\\f"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                default:
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        sb.append('"');
    }

    // ------------------------------------------------------------------
    // 类型化访问
    // ------------------------------------------------------------------

    public static Object get(Map<String, Object> m, String key) {
        return m == null ? null : m.get(key);
    }

    public static String getStr(Map<String, Object> m, String key) {
        Object v = get(m, key);
        return v == null ? null : String.valueOf(v);
    }

    public static int asInt(Object v) {
        return (int) asLong(v);
    }

    public static long asLong(Object v) {
        if (v == null) {
            return 0L;
        }
        if (v instanceof Number) {
            return ((Number) v).longValue();
        }
        return Long.parseLong(v.toString().trim());
    }
}
