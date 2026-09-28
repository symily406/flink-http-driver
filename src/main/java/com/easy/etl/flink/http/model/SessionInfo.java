package com.easy.etl.flink.http.model;

import com.easy.etl.flink.http.util.Json;

import java.util.Map;

/** {@code POST /v1/sessions}：网关创建的会话。 */
public final class SessionInfo {

    private final String sessionHandle;

    public SessionInfo(String sessionHandle) {
        this.sessionHandle = sessionHandle;
    }

    public static SessionInfo fromJson(Map<String, Object> json) {
        return new SessionInfo(Json.getStr(json, "sessionHandle"));
    }

    public String getSessionHandle() {
        return sessionHandle;
    }

    @Override
    public String toString() {
        return "SessionInfo{sessionHandle=" + sessionHandle + "}";
    }
}
