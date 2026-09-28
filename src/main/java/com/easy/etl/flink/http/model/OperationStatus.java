package com.easy.etl.flink.http.model;

import com.easy.etl.flink.http.util.Json;

import java.util.Locale;
import java.util.Map;

/**
 * {@code GET /v1/sessions/{s}/operations/{op}/status}。
 *
 * <p>网关可能新增状态，因此原始文本总是保留在 {@link #getStatus()} 中，并且只进行
 * 不区分大小写的比较。
 */
public final class OperationStatus {

    public static final String RUNNING = "RUNNING";
    public static final String FINISHED = "FINISHED";
    public static final String ERROR = "ERROR";
    public static final String CANCELED = "CANCELED";
    public static final String CLOSED = "CLOSED";
    public static final String TIMEOUT = "TIMEOUT";

    private final String status;

    public OperationStatus(String status) {
        this.status = status;
    }

    public static OperationStatus fromJson(Map<String, Object> json) {
        return new OperationStatus(Json.getStr(json, "status"));
    }

    public String getStatus() {
        return status;
    }

    private boolean is(String expected) {
        return status != null && status.equalsIgnoreCase(expected);
    }

    public boolean isRunning() {
        return is(RUNNING);
    }

    public boolean isFinished() {
        return is(FINISHED);
    }

    public boolean isError() {
        return is(ERROR);
    }

    /** 一旦网关不会再改变状态，即为终结状态。 */
    public boolean isTerminal() {
        return is(FINISHED) || is(ERROR) || is(CANCELED) || is(CLOSED) || is(TIMEOUT);
    }

    /** 操作以非 FINISHED 的状态结束时即为失败。 */
    public boolean isFailure() {
        return isTerminal() && !is(FINISHED);
    }

    @Override
    public String toString() {
        return "OperationStatus{status=" + (status == null ? null
                : status.toUpperCase(Locale.ROOT)) + "}";
    }
}
