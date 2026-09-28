package com.easy.etl.flink.http.model;

/** 结果页所携带的内容类型，按其 {@code resultType} 报告。 */
public enum ResultType {

    /** 本页包含行数据。 */
    PAYLOAD,
    /** 后面可能还有更多行；本页本身为空。 */
    AVAILABLE,
    /** 结果集结束。 */
    EOS,
    /** 操作尚未产生任何内容（流式作业的典型状态）。 */
    NOT_READY,
    /** 未来网关版本可能新增的任何类型。 */
    UNKNOWN;

    public static ResultType of(String value) {
        if (value == null) {
            return UNKNOWN;
        }
        for (ResultType t : values()) {
            if (t.name().equalsIgnoreCase(value.trim())) {
                return t;
            }
        }
        return UNKNOWN;
    }
}
