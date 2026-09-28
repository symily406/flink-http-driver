package com.easy.etl.flink.http.model;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * {@code POST /v1/sessions/{sessionHandle}/statements}。
 *
 * <p>网关每次调用只接受一条语句；发送前请先拆分脚本。
 */
public final class ExecuteStatementRequest implements JsonBody {

    private final String statement;

    public ExecuteStatementRequest(String statement) {
        this.statement = statement;
    }

    public String getStatement() {
        return statement;
    }

    @Override
    public Map<String, Object> toJson() {
        Map<String, Object> body = new LinkedHashMap<String, Object>();
        body.put("statement", statement);
        return body;
    }

    @Override
    public String toString() {
        return "ExecuteStatementRequest{statement=" + statement + "}";
    }
}
