package com.easy.etl.flink.http.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 网关操作的物化结果（所有行已全部抓取完成）。
 */
public class GatewayResult {

    /** 网关返回的列标签。 */
    private final List<String> columns;

    /** 原始值的行（String/Boolean/Long/BigDecimal/null）。 */
    private final List<Row> rows;

    /** 结果类型：SUCCESS / SUCCESS_WITH_CONTENT / ERROR。 */
    private final String resultKind;

    /** 当语句是 DML（首列是标记列）时为 true。 */
    private final boolean dml;

    /** 网关在结果页顶层给出的作业 ID；网关未提供时为 null。 */
    private final String jobId;

    public GatewayResult(List<String> columns, List<Row> rows, String resultKind) {
        this(columns, rows, resultKind, null);
    }

    public GatewayResult(List<String> columns, List<Row> rows, String resultKind, String jobId) {
        List<String> cols = columns == null
                ? Collections.<String>emptyList()
                : Collections.unmodifiableList(new ArrayList<String>(columns));
        List<Row> rs = rows == null
                ? Collections.<Row>emptyList()
                : Collections.unmodifiableList(new ArrayList<Row>(rows));
        this.columns = cols;
        this.rows = rs;
        this.resultKind = resultKind == null ? "SUCCESS" : resultKind;
        this.dml = !cols.isEmpty() && isDmlColumn(cols.get(0));
        this.jobId = jobId;
    }

    /**
     * DML 的首列是不是网关的标记列。
     *
     * <p>两种运行模式下标记列的名字不同，必须都认：
     * <ul>
     *   <li>{@code row count} / {@code row_count} —— 批模式下 INSERT 的影响行数；</li>
     *   <li>{@code job id} / {@code job_id} —— 流模式下 INSERT 提交的作业 ID
     *       （Flink 2.x 实测列名就是 {@code job id}）。只认前者的话，
     *       {@link #isDml()} 在流模式下永远是 false。</li>
     * </ul>
     */
    private static boolean isDmlColumn(String name) {
        return "row count".equalsIgnoreCase(name) || "row_count".equalsIgnoreCase(name)
                || "job id".equalsIgnoreCase(name) || "job_id".equalsIgnoreCase(name);
    }

    public List<String> getColumns() {
        return columns;
    }

    public List<Row> getRows() {
        return rows;
    }

    public String getResultKind() {
        return resultKind;
    }

    public boolean isDml() {
        return dml;
    }

    /**
     * DML 的影响行数，否则为 0。
     *
     * <p>只有批模式的 {@code row count} 列才是行数；流模式的标记列是
     * {@code job id}（32 位 hex），不是数字，因此流式 INSERT 一律返回 0——
     * 想知道作业状态请用 {@code job id} 配合
     * {@code SqlGatewayClient#fetchJobStatus}。
     */
    public long getUpdateCount() {
        if (!dml || rows.isEmpty() || rows.get(0).size() == 0 || rows.get(0).get(0) == null) {
            return 0L;
        }
        Object v = rows.get(0).get(0);
        if (v instanceof Number) {
            return ((Number) v).longValue();
        }
        try {
            return Long.parseLong(v.toString().trim());
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    /**
     * 语句提交的作业 ID，或 null。
     *
     * <p>优先采用网关在结果页顶层给出的 {@code jobID}（Flink 2.x 的稳定来源），
     * 拿不到时再回退为从结果列里推断：先是列名含 {@code job} 的列，
     * 最后是符合 Flink JobID 形状（32 位 hex）的单元格。
     */
    public String getJobId() {
        if (jobId != null && !jobId.trim().isEmpty()) {
            return jobId.trim();
        }
        if (rows.isEmpty()) {
            return null;
        }
        // 优先使用网关实际标记为作业 ID 的列。
        for (int c = 0; c < columns.size(); c++) {
            String name = columns.get(c);
            if (name != null && name.toLowerCase(java.util.Locale.ROOT).contains("job")) {
                String v = cell(0, c);
                if (v != null) {
                    return v;
                }
            }
        }
        // 回退到 Flink JobID 的形状：32 个十六进制字符。
        for (int c = 0; c < rows.get(0).size(); c++) {
            String v = cell(0, c);
            if (v != null && v.matches("[0-9a-fA-F]{32}")) {
                return v;
            }
        }
        return null;
    }

    /**
     * 以 {@code column -> value} 映射的形式返回各行，每个 map 对应一行，顺序与\n     * {@link #getColumns()} 一致。\n     *\n     * <pre>\n     * for (Map&lt;String, Object&gt; row : result.toMaps()) {\n     *     Object amount = row.get("amount");\n     * }\n     * </pre>\n     *\n     * <p>重复的列标签会合并为单个条目（后出现的覆盖之前的），{@code null} 标签会被跳过；\n     * 比列列表短的行只会携带较少的键。\n     */
    public List<Map<String, Object>> toMaps() {
        List<Map<String, Object>> out = new ArrayList<Map<String, Object>>(rows.size());
        for (Row row : rows) {
            out.add(row.toMap(columns.toArray(new String[columns.size()])));
        }
        return out;
    }

    /**
     * {@link #toMaps()} 的单行便捷变体，用于已知至多返回一行的查询\n     * （例如没有 GROUP BY 的聚合）。\n     *\n     * @return 第一行作为 map；当结果为空时返回 {@code null}\n     */
    public Map<String, Object> toMap() {
        List<Map<String, Object>> maps = toMaps();
        return maps.isEmpty() ? null : maps.get(0);
    }

    private String cell(int row, int col) {
        Object v = rows.get(row).get(col);
        if (v == null) {
            return null;
        }
        String s = v.toString().trim();
        return s.isEmpty() ? null : s;
    }

    @Override
    public String toString() {
        Map<String, Object> summary = new LinkedHashMap<String, Object>();
        summary.put("resultKind", resultKind);
        summary.put("columns", columns);
        summary.put("rowCount", rows.size());
        return "GatewayResult" + summary;
    }
}
