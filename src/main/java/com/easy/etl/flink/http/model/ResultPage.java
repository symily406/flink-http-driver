package com.easy.etl.flink.http.model;

import com.easy.etl.flink.http.util.Json;

import java.util.Map;

/**
 * 一个操作结果的一页（{@code GET .../result/{token}}）。
 *
 * <p>{@link #getType()} 说明这一页携带了什么：{@code PAYLOAD} 表示行数据在
 * {@link #getData()} 中，{@code EOS} 表示结果集结束，{@code NOT_READY} 表示
 * 客户端应该用同一个 token 再次轮询。
 */
public final class ResultPage {

    private final ResultType type;
    private final String resultKind;
    private final ResultSetData data;
    private final String nextResultUri;
    private final boolean queryResult;
    private final String jobId;

    public ResultPage(ResultType type, String resultKind, ResultSetData data, String nextResultUri) {
        this(type, resultKind, data, nextResultUri, false, null);
    }

    public ResultPage(ResultType type, String resultKind, ResultSetData data, String nextResultUri,
                      boolean queryResult, String jobId) {
        this.type = type == null ? ResultType.UNKNOWN : type;
        this.resultKind = resultKind;
        this.data = data == null ? ResultSetData.empty() : data;
        this.nextResultUri = nextResultUri;
        this.queryResult = queryResult;
        this.jobId = jobId;
    }

    public static ResultPage fromJson(Map<String, Object> json) {
        Object queryResult = Json.get(json, "isQueryResult");
        String jobId = Json.getStr(json, "jobID");
        if (jobId == null) {
            jobId = Json.getStr(json, "jobId");
        }
        return new ResultPage(
                ResultType.of(Json.getStr(json, "resultType")),
                Json.getStr(json, "resultKind"),
                ResultSetData.fromJson(Json.get(json, "results")),
                Json.getStr(json, "nextResultUri"),
                queryResult instanceof Boolean && (Boolean) queryResult,
                jobId);
    }

    public ResultType getType() {
        return type;
    }

    /** SUCCESS / SUCCESS_WITH_CONTENT / ERROR，当页面省略该字段时为 null。 */
    public String getResultKind() {
        return resultKind;
    }

    public ResultSetData getData() {
        return data;
    }

    public String getNextResultUri() {
        return nextResultUri;
    }

    /** 该语句是否是查询（SELECT）；网关未提供时按 {@code false} 处理。 */
    public boolean isQueryResult() {
        return queryResult;
    }

    /**
     * 该结果对应的 Flink 作业 ID（32 位 hex）；网关未提供时为 {@code null}。
     *
     * <p>Flink 2.x 把作业 ID 放在页的顶层 {@code jobID} 字段里，DML 的结果列不再
     * 保证叫 {@code job id}，所以这里是拿作业 ID 最可靠的来源。
     */
    public String getJobId() {
        return jobId;
    }

    /**
     * 下一页的 token，或当这一页是最后一页时返回 {@code -1}
     * （从 {@code nextResultUri} 解析出来，这是它唯一出现的位置）。
     */
    public long getNextToken() {
        if (nextResultUri == null || nextResultUri.isEmpty()) {
            return -1L;
        }
        int idx = nextResultUri.lastIndexOf("/result/");
        if (idx < 0) {
            return -1L;
        }
        String tail = nextResultUri.substring(idx + "/result/".length());
        int q = tail.indexOf('?');
        if (q >= 0) {
            tail = tail.substring(0, q);
        }
        try {
            return Long.parseLong(tail.trim());
        } catch (NumberFormatException e) {
            return -1L;
        }
    }

    public boolean isEos() {
        return type == ResultType.EOS;
    }

    public boolean hasPayload() {
        return (type == ResultType.PAYLOAD || type == ResultType.AVAILABLE) && !data.isEmpty();
    }

    @Override
    public String toString() {
        return "ResultPage{type=" + type + ", resultKind=" + resultKind + ", " + data + "}";
    }}
