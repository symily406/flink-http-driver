package com.easy.etl.flink.http.model;

import com.easy.etl.flink.http.util.Json;

import java.util.Map;

/** {@code POST /v1/sessions/{s}/statements}：一条语句变成的操作。 */
public final class OperationInfo {

    private final String operationHandle;

    public OperationInfo(String operationHandle) {
        this.operationHandle = operationHandle;
    }

    public static OperationInfo fromJson(Map<String, Object> json) {
        return new OperationInfo(Json.getStr(json, "operationHandle"));
    }

    public String getOperationHandle() {
        return operationHandle;
    }

    @Override
    public String toString() {
        return "OperationInfo{operationHandle=" + operationHandle + "}";
    }
}
