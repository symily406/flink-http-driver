package com.easy.etl.flink.http.model;

import com.easy.etl.flink.http.util.Json;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 结果页中的一行。
 *
 * <p>值是网关发送的任意内容：{@code String}、{@code Long}、{@code BigDecimal}、
 * {@code Boolean} 或 {@code null}（参见 {@code rowFormat}）。
 */
public final class Row {

    private final List<Object> values;

    public Row(List<Object> values) {
        this.values = Collections.unmodifiableList(new ArrayList<Object>(
                values == null ? Collections.<Object>emptyList() : values));
    }

    /**
     * 解析网关各种行格式中的一个 {@code data} 元素：
     * <ul>
     *   <li>{@code ["1","x"]} - {@code rowFormat=table}</li>
     *   <li>{@code {"fields":{"id":"1","name":"x"}}} - {@code rowFormat=json}，
     *       fields 按键列名（Flink 1.18/1.19）</li>
     *   <li>{@code {"kind":"INSERT","fields":["1","x"]}} - {@code rowFormat=json}，
     *       fields 作为位置数组（Flink 1.20+/2.x）</li>
     * </ul>
     */
    @SuppressWarnings("unchecked")
    public static Row fromJson(Object json) {
        if (json instanceof Map) {
            Object fields = Json.get((Map<String, Object>) json, "fields");
            if (fields instanceof Map) {
                // Json.parseObject 构建的是 LinkedHashMap，因此字段顺序与列顺序一致。
                return new Row(new ArrayList<Object>(((Map<String, Object>) fields).values()));
            }
            if (fields instanceof List) {
                // 位置数组：已经与结果集的列对齐。
                return new Row((List<Object>) fields);
            }
            return new Row(Collections.emptyList());
        }
        if (json instanceof List) {
            return new Row((List<Object>) json);
        }
        return new Row(Collections.emptyList());
    }

    /** 原始单元格值，索引与结果集的列对齐。 */
    public List<Object> getValues() {
        return values;
    }

    public Object get(int index) {
        return index >= 0 && index < values.size() ? values.get(index) : null;
    }

    public int size() {
        return values.size();
    }

    /** 按列名键控的该行；{@code null} 列名会被跳过。 */
    public Map<String, Object> toMap(List<Column> columns) {
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        int n = columns == null ? 0 : Math.min(columns.size(), values.size());
        for (int i = 0; i < n; i++) {
            Column c = columns.get(i);
            if (c != null && c.getName() != null) {
                m.put(c.getName(), values.get(i));
            }
        }
        return m;
    }

    /** {@link #toMap(List)} 基于纯列名的便捷重载。 */
    public Map<String, Object> toMap(String... columnNames) {
        List<Column> cols = new ArrayList<Column>(columnNames == null ? 0 : columnNames.length);
        if (columnNames != null) {
            for (String name : columnNames) {
                cols.add(new Column(name, null));
            }
        }
        return toMap(cols);
    }

    @Override
    public String toString() {
        return values.toString();
    }
}
