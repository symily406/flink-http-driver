package com.easy.etl.flink.http.gateway;

import com.easy.etl.flink.http.model.Column;
import com.easy.etl.flink.http.model.GatewayResult;
import com.easy.etl.flink.http.model.ResultPage;
import com.easy.etl.flink.http.model.ResultType;
import com.easy.etl.flink.http.model.Row;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 基于已完成的网关操作的、惰性的、只向前移动的光标。
 *
 * <p>通过跟随 {@code nextResultUri} 一页一页地抓取数据，因此大体量的结果
 * 从不会在客户端堆内存中被整体物化。首页在构造时被急切抓取（{@link #prime()}），
 * 因为只有等到负载到达后才知道列名和 {@code resultKind}。
 *
 * <p>实例不是线程安全的，并且必须被关闭：{@link #close()} 会释放网关操作。
 */
public final class GatewayResultCursor implements Closeable {

    private final SqlGatewayClient client;
    private final String sessionHandle;
    private final String operationHandle;
    private final int maxRows;
    private final long pollIntervalMs;
    private final long operationTimeoutMs;
    private final long start;

    private List<Column> columns = Collections.emptyList();
    private String resultKind = "SUCCESS";
    private String jobId;
    private boolean queryResult;
    private long token = 0L;
    private boolean primed;
    /** 网上已经没有更多页了（EOS，或下一页 uri 缺失）。当前页剩余的行仍然会被读出。 */
    private boolean drained;
    /** 已读满 {@code maxRows}：必须**立即**停止，连当前页剩下的行也不能再读。 */
    private boolean limitReached;
    private boolean closed;

    private List<Row> page = Collections.emptyList();
    private int pagePos;
    private Row currentRow;
    private int rowsRead;

    GatewayResultCursor(SqlGatewayClient client, String sessionHandle, String operationHandle,
                        int maxRows, long pollIntervalMs, long operationTimeoutMs) {
        this.client = client;
        this.sessionHandle = sessionHandle;
        this.operationHandle = operationHandle;
        this.maxRows = maxRows;
        this.pollIntervalMs = pollIntervalMs;
        this.operationTimeoutMs = operationTimeoutMs;
        this.start = System.currentTimeMillis();
    }

    /** 抓取第一页，使 {@link #columns()} 在 {@link #next()} 之前即可使用。 */
    void prime() throws IOException, InterruptedException {
        if (!primed) {
            primed = true;
            loadPage();
        }
    }

    /** 预抓取时解析得到的列。不执行任何 I/O。 */
    public List<Column> columnsSnapshot() {
        return columns;
    }

    /** 预抓取时解析得到的列标签。不执行任何 I/O。 */
    public List<String> columnNames() {
        List<String> names = new ArrayList<String>(columns.size());
        for (Column c : columns) {
            names.add(c.getName());
        }
        return names;
    }

    /** 预抓取时解析得到的结果类型。不执行任何 I/O。 */
    String resultKindSnapshot() {
        return resultKind;
    }

    /**
     * 网关在结果页顶层给出的作业 ID；尚未预抓取、或网关未提供时为 {@code null}。
     * 不执行任何 I/O。
     */
    public String jobId() {
        return jobId;
    }

    /** 该语句是否是查询（SELECT）；尚未预抓取时按 {@code false} 处理。 */
    public boolean isQueryResult() {
        return queryResult;
    }

    /** 该光标读取的网关操作句柄，因此它可以被取消。 */
    public String operationHandle() {
        return operationHandle;
    }

    public List<Column> columns() throws IOException, InterruptedException {
        prime();
        return columns;
    }

    public String resultKind() throws IOException, InterruptedException {
        prime();
        return resultKind;
    }

    /**
     * 前进到下一条记录，需要时抓取后续页面。
     *
     * @return 到达 EOS 或已读取 {@code maxRows} 行时返回 {@code false}
     */
    public boolean next() throws IOException, InterruptedException {
        prime();
        while (true) {
            // maxRows 的检查必须排在"当前页还有剩余行"之前：网关一页往往有好几百行
            // （实测 100 行/页），先交出当前页再判断上限的话，maxRows=25 会一下子吐出
            // 整整一页，最终读到 300 行。这里保证返回的行数**精确**等于 maxRows。
            if (limitReached) {
                currentRow = null;
                return false;
            }
            if (pagePos < page.size()) {
                currentRow = page.get(pagePos++);
                rowsRead++;
                if (maxRows > 0 && rowsRead >= maxRows) {
                    limitReached = true;
                }
                return true;
            }
            if (drained) {
                currentRow = null;
                return false;
            }
            loadPage();
        }
    }

    /** {@link #next()} 最后前进到的记录行。 */
    public Row row() {
        return currentRow;
    }

    /** 按列名键控的当前行；在第一次 {@link #next()} 之前为 null。 */
    public java.util.Map<String, Object> rowAsMap() {
        return currentRow == null ? null : currentRow.toMap(columns);
    }

    /** 将剩余所有行读入一个快照，并关闭光标。 */
    public GatewayResult drain() throws IOException, InterruptedException {
        prime();
        List<Row> all = new ArrayList<Row>();
        while (next()) {
            all.add(currentRow);
        }
        return new GatewayResult(columnNames(), all, resultKind, jobId);
    }

    private void loadPage() throws IOException, InterruptedException {
        while (true) {
            ResultPage result = client.fetchResultPage(sessionHandle, operationHandle, token);

            if (result.isEos()) {
                drained = true;
                page = Collections.emptyList();
                pagePos = 0;
                return;
            }

            if (result.getType() == ResultType.PAYLOAD || result.getType() == ResultType.AVAILABLE) {
                String kind = result.getResultKind();
                if (kind != null) {
                    resultKind = kind;
                }
                if (result.getJobId() != null) {
                    jobId = result.getJobId();
                }
                queryResult = result.isQueryResult();
                if (columns.isEmpty() && !result.getData().getColumns().isEmpty()) {
                    columns = Collections.unmodifiableList(result.getData().getColumns());
                }
                page = result.getData().getRows();
                pagePos = 0;

                long nextToken = result.getNextToken();
                if (nextToken < 0) {
                    drained = true;
                } else if (nextToken == token && page.isEmpty()) {
                    // 网关没有前进；立即停止，避免无限空转。
                    drained = true;
                } else {
                    token = nextToken;
                }
                return;
            }

            // NOT_READY（流式操作的典型状态）：用同一个 token 重试。
            client.checkElapsed(operationTimeoutMs, start);
            Thread.sleep(pollIntervalMs);
        }
    }

    /** 释放网关操作。可安全地多次调用。 */
    @Override
    public void close() {
        if (!closed) {
            closed = true;
            page = Collections.emptyList();
            currentRow = null;
            client.closeQuietly(sessionHandle, operationHandle);
        }
    }
}
