package com.easy.etl.flink.http.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 一个操作产生的文本：错误文本、异常堆栈、{@code row count}、提交的
 * {@code job id}、{@code EXPLAIN} / {@code CALL} 输出。
 *
 * <p>SQL Gateway REST API 没有专门的日志端点——所有这些都会作为结果负载的行返回，
 * 因此这也是客户端能读取日志等价输出的唯一位置。一行中的各单元格用制表符连接。
 */
public final class OperationLog {

    private static final OperationLog EMPTY = new OperationLog(Collections.<String>emptyList());

    private final List<String> lines;

    public OperationLog(List<String> lines) {
        this.lines = Collections.unmodifiableList(new ArrayList<String>(
                lines == null ? Collections.<String>emptyList() : lines));
    }

    public static OperationLog of(List<String> lines) {
        return new OperationLog(lines);
    }

    /** 用制表符连接一行结果中的各单元格。 */
    public static String join(Row row) {
        StringBuilder sb = new StringBuilder();
        for (Object v : row.getValues()) {
            if (sb.length() > 0) {
                sb.append('\t');
            }
            sb.append(v == null ? "" : v.toString());
        }
        return sb.toString();
    }

    public static OperationLog empty() {
        return EMPTY;
    }

    public List<String> getLines() {
        return lines;
    }

    /** 第一条非空白行——通常是网关自己的错误文本——或 null。 */
    public String getFirstLine() {
        for (String line : lines) {
            if (line != null && !line.trim().isEmpty()) {
                return line.trim();
            }
        }
        return null;
    }

    public boolean isEmpty() {
        return lines.isEmpty();
    }

    @Override
    public String toString() {
        return "OperationLog{lines=" + lines.size() + "}";
    }
}
