package com.easy.etl.flink.http.model;

import com.easy.etl.flink.http.util.Json;

import java.util.Map;

/**
 * 作业的 {@code exceptionHistory} 中的一条记录。
 *
 * <p>Flink 两种字段形态都要兼容：
 * <ul>
 *   <li>Flink 1.x：{@code {"root":"...","taskName":"A -> B","timestamp":123}}</li>
 *   <li>Flink 2.x：{@code {"exceptionName":"...","stacktrace":"...","timestamp":123}}
 *       ——没有 {@code root} / {@code taskName}，根因要从堆栈首行取</li>
 * </ul>
 */
public final class JobExceptionEntry {

    private final String root;
    private final String taskName;
    private final Long timestamp;
    private final String exceptionName;
    private final String stackTrace;

    public JobExceptionEntry(String root, String taskName, Long timestamp) {
        this(root, taskName, timestamp, null, null);
    }

    public JobExceptionEntry(String root, String taskName, Long timestamp, String exceptionName,
                             String stackTrace) {
        this.root = root;
        this.taskName = taskName;
        this.timestamp = timestamp;
        this.exceptionName = exceptionName;
        this.stackTrace = stackTrace;
    }

    public static JobExceptionEntry fromJson(Map<String, Object> json) {
        Object ts = Json.get(json, "timestamp");
        String exceptionName = Json.getStr(json, "exceptionName");
        String stackTrace = Json.getStr(json, "stacktrace");
        if (stackTrace == null) {
            stackTrace = Json.getStr(json, "stackTrace");
        }
        String root = Json.getStr(json, "root");
        if (isBlank(root)) {
            // Flink 2.x：没有 root 字段，堆栈首行就是根因声明
            root = firstLine(stackTrace);
        }
        if (isBlank(root)) {
            root = exceptionName;
        }
        String taskName = Json.getStr(json, "taskName");
        if (isBlank(taskName)) {
            taskName = Json.getStr(json, "task-name");
        }
        return new JobExceptionEntry(root, taskName, ts == null ? null : Json.asLong(ts),
                exceptionName, stackTrace);
    }

    private static boolean isBlank(String v) {
        return v == null || v.trim().isEmpty();
    }

    private static String firstLine(String text) {
        if (text == null) {
            return null;
        }
        int nl = text.indexOf('\n');
        String line = (nl < 0 ? text : text.substring(0, nl)).trim();
        return line.isEmpty() ? null : line;
    }

    /** 根因文本，例如 {@code java.lang.RuntimeException: boom}。 */
    public String getRoot() {
        return root;
    }

    /** 失败发生的任务，例如 {@code Source -> Sink}；Flink 2.x 未提供时为 {@code null}。 */
    public String getTaskName() {
        return taskName;
    }

    /** 失败的毫秒级时间戳，缺失时为 {@code null}。 */
    public Long getTimestamp() {
        return timestamp;
    }

    /** 异常类名（Flink 2.x 才有），例如 {@code org.apache.flink.runtime.JobException}。 */
    public String getExceptionName() {
        return exceptionName;
    }

    /** 完整异常堆栈（Flink 2.x 才有）；Flink 1.x 的条目为 {@code null}。 */
    public String getStackTrace() {
        return stackTrace;
    }

    @Override
    public String toString() {
        return "JobExceptionEntry{task=" + taskName + ", root=" + root + "}";
    }
}
