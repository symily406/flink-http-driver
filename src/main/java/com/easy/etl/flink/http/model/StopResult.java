package com.easy.etl.flink.http.model;

import com.easy.etl.flink.http.util.Json;

import java.util.Map;

/**
 * 停止流任务（stop-with-savepoint）的结果。
 *
 * <p>对应 Flink 集群（JobManager）REST 端点的
 * {@code GET /jobs/{jobId}/savepoints/{triggerId}} 返回内容。触发停止之后，
 * Flink 会为流作业额外做一次 checkpoint（savepoint），并返回该 checkpoint 在
 * 文件系统上的位置，之后可用这个位置从停止点恢复（restore）。
 */
public final class StopResult {

    /** 停止触发的状态 id。 */
    public static final String IN_PROGRESS = "IN_PROGRESS";
    public static final String COMPLETED = "COMPLETED";
    public static final String FAILED = "FAILED";

    private final String jobId;
    /** 停止请求的触发 id（request-id），用于轮询查询结果。 */
    private final String requestId;
    /** 本次停止的原始状态文本，例如 IN_PROGRESS / COMPLETED / FAILED。 */
    private final String status;
    /** 生成的 checkpoint / savepoint 路径；未完成或失败时为 null。 */
    private final String location;
    /** 失败原因；成功时为 null。 */
    private final String failureCause;

    public StopResult(String jobId, String requestId, String status, String location,
                      String failureCause) {
        this.jobId = jobId;
        this.requestId = requestId;
        this.status = status;
        this.location = location;
        this.failureCause = failureCause;
    }

    @SuppressWarnings("unchecked")
    public static StopResult fromJson(String jobId, String requestId, Map<String, Object> json) {
        String status = null;
        String failureCause = null;
        Object statusObj = Json.get(json, "status");
        if (statusObj instanceof Map) {
            Map<String, Object> st = (Map<String, Object>) statusObj;
            status = Json.getStr(st, "id");
            failureCause = Json.getStr(st, "failureCause");
            if (failureCause == null) {
                failureCause = Json.getStr(st, "failure-cause"); // snake_case 形式
            }
        }

        // Flink 2.x：{"status":{"id":"COMPLETED"},"operation":{"location":"s3a://..."}}
        // 位置与失败原因都嵌在 operation 里，而不是顶层。
        String location = null;
        Object operationObj = Json.get(json, "operation");
        if (operationObj instanceof Map) {
            Map<String, Object> op = (Map<String, Object>) operationObj;
            location = text(op, "location");
            if (location == null && failureCause == null) {
                failureCause = text(op, "failure-cause");
            }
            if (location == null && failureCause == null) {
                failureCause = text(op, "failureCause");
            }
        }

        // 兼容老版本把 location 放在顶层的形态
        if (location == null) {
            Object locationObj = Json.get(json, "location");
            if (locationObj instanceof Map) {
                location = Json.getStr((Map<String, Object>) locationObj, "location");
            } else {
                location = Json.getStr(json, "location");
            }
        }
        if (status == null) {
            status = Json.getStr(json, "status");
        }
        return new StopResult(jobId, requestId, status, location, failureCause);
    }

    /** 取一个字段的文本；字段是对象时取其中的 message / exception 字段。 */
    @SuppressWarnings("unchecked")
    private static String text(Map<String, Object> map, String key) {
        Object value = Json.get(map, key);
        if (value == null) {
            return null;
        }
        if (value instanceof Map) {
            Map<String, Object> nested = (Map<String, Object>) value;
            String message = Json.getStr(nested, "message");
            if (message != null && !message.trim().isEmpty()) {
                return message;
            }
            String exception = Json.getStr(nested, "exception");
            return exception != null && !exception.trim().isEmpty() ? exception : null;
        }
        String text = String.valueOf(value).trim();
        return text.isEmpty() ? null : text;
    }

    public String getJobId() {
        return jobId;
    }

    public String getRequestId() {
        return requestId;
    }

    public String getStatus() {
        return status;
    }

    /** checkpoint / savepoint 的路径，只有 {@link #isCompleted()} 时才有值。 */
    public String getLocation() {
        return location;
    }

    public String getFailureCause() {
        return failureCause;
    }

    private boolean is(String expected) {
        return status != null && status.equalsIgnoreCase(expected);
    }

    /** 停止仍在进行中。 */
    public boolean isInProgress() {
        return is(IN_PROGRESS);
    }

    /** 停止已完成，{@link #getLocation()} 即为停顿点 checkpoint 的位置。 */
    public boolean isCompleted() {
        return is(COMPLETED);
    }

    public boolean isFailed() {
        return is(FAILED);
    }

    /** 已经到达终结状态（已完成或失败）。 */
    public boolean isTerminal() {
        return isCompleted() || isFailed();
    }

    @Override
    public String toString() {
        return "StopResult{jobId=" + jobId + ", status=" + status
                + ", location=" + location + "}";
    }
}
