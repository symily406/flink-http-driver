package com.easy.etl.flink.http.model;

import com.easy.etl.flink.http.util.Json;

import java.util.Collections;
import java.util.Map;

/**
 * 结果集中的一列：{@code {"name":"id","logicalType":{...}}}。
 *
 * <p>{@code logicalType} 以其原始 JSON 文本保存，因为它在不同 Flink 版本之间形状不同
 * （{@code INT} 与 {@code {"type":"INT","nullable":true}}）。
 */
public final class Column {

    private final String name;
    private final String logicalType;

    public Column(String name, String logicalType) {
        this.name = name;
        this.logicalType = logicalType;
    }

    @SuppressWarnings("unchecked")
    public static Column fromJson(Object json) {
        if (json instanceof Map) {
            Map<String, Object> m = (Map<String, Object>) json;
            Object type = Json.get(m, "logicalType");
            String typeText = type == null ? null
                    : type instanceof Map ? Json.write((Map<String, Object>) type)
                    : String.valueOf(type);
            return new Column(Json.getStr(m, "name"), typeText);
        }
        // 旧版网关发送的是纯字符串。
        return new Column(json == null ? null : String.valueOf(json), null);
    }

    public String getName() {
        return name;
    }

    /** 逻辑类型的原始 JSON；网关省略时返回 {@code null}。 */
    public String getLogicalType() {
        return logicalType;
    }

    /** 把这一列视为单元素列表的不可修改视图——方便 {@link Row#toMap} 使用。 */
    public java.util.List<Column> asList() {
        return Collections.singletonList(this);
    }

    @Override
    public String toString() {
        return logicalType == null ? String.valueOf(name) : name + ":" + logicalType;
    }
}
