package com.easy.etl.flink.http;

import com.easy.etl.flink.http.gateway.SqlGatewayClient;
import com.easy.etl.flink.http.model.GatewayResult;
import com.easy.etl.flink.http.model.Row;
import com.easy.etl.flink.http.model.StopResult;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * {@link GatewayResult} 的空值安全、DML/作业 ID 解析以及\n * {@code toMaps()} / {@code toMap()} 视图的回归测试。
 */
public class GatewayResultTest {

    private static Row row(Object... values) {
        return new Row(Arrays.asList(values));
    }

    @Test
    public void nullColumnsAndRowsDoNotThrow() {
        GatewayResult r = new GatewayResult(null, null, null);
        assertTrue(r.getColumns().isEmpty());
        assertTrue(r.getRows().isEmpty());
        assertEquals("SUCCESS", r.getResultKind());
        assertFalse(r.isDml());
        assertEquals(0L, r.getUpdateCount());
        assertNull(r.getJobId());
    }

    @Test
    public void detectsRowCountDmlAndUpdateCount() {
        List<Row> rows = new ArrayList<Row>();
        rows.add(row(7L));
        GatewayResult r = new GatewayResult(
                Collections.singletonList("row count"), rows, "SUCCESS");
        assertTrue(r.isDml());
        assertEquals(7L, r.getUpdateCount());
    }

    @Test
    public void updateCountParsesStringCell() {
        List<Row> rows = new ArrayList<Row>();
        rows.add(row("3"));
        GatewayResult r = new GatewayResult(
                Collections.singletonList("row count"), rows, "SUCCESS");
        assertTrue(r.isDml());
        assertEquals(3L, r.getUpdateCount());
    }

    /**
     * 回归点：流模式的 INSERT 不在批模式，网关给的标记列是 {@code "job id"} 而不是
     * {@code "row count"}，只认后者的实现会让 {@code isDml()} 在流模式下永远为 false。
     */
    @Test
    public void detectsJobIdDmlForStreamingInsert() {
        List<Row> rows = new ArrayList<Row>();
        rows.add(row("0123456789abcdef0123456789abcdef"));
        GatewayResult r = new GatewayResult(
                Collections.singletonList("job id"), rows, "SUCCESS_WITH_CONTENT");
        assertTrue(r.isDml());
        // job id 不是数字，没有 row count 可言
        assertEquals(0L, r.getUpdateCount());
        assertEquals("0123456789abcdef0123456789abcdef", r.getJobId());
    }

    @Test
    public void detectsUnderscoredJobIdDmlColumn() {
        List<Row> rows = new ArrayList<Row>();
        rows.add(row("0123456789abcdef0123456789abcdef"));
        GatewayResult r = new GatewayResult(
                Collections.singletonList("job_id"), rows, "SUCCESS_WITH_CONTENT");
        assertTrue(r.isDml());
    }

    @Test
    public void jobIdPrefersLabelledColumn() {
        List<Row> rows = new ArrayList<Row>();
        rows.add(row("ignored", "job-abc"));
        GatewayResult r = new GatewayResult(
                Arrays.asList("status", "job id"), rows, "SUCCESS");
        assertEquals("job-abc", r.getJobId());
    }

    @Test
    public void jobIdFallsBackToHexShape() {
        String hex = "0123456789abcdef0123456789abcdef";
        List<Row> rows = new ArrayList<Row>();
        rows.add(row(hex));
        GatewayResult r = new GatewayResult(
                Collections.singletonList("result"), rows, "SUCCESS");
        assertEquals(hex, r.getJobId());
    }

    @Test
    public void columnsAreImmutable() {
        GatewayResult r = new GatewayResult(
                Collections.singletonList("a"), new ArrayList<Row>(), "SUCCESS");
        try {
            r.getColumns().add("b");
            // 某些 JDK 会抛出 UnsupportedOperationException；即使不抛出，
            // 快照也必须不受源列表变更的影响。
            assertEquals(1, r.getColumns().size());
        } catch (UnsupportedOperationException expected) {
            // 期望抛出该异常，属正常情况
        }
    }

    @Test
    public void toMapsKeysRowsByColumnName() {
        List<Row> rows = new ArrayList<Row>();
        rows.add(row(1L, 42L, new BigDecimal("9.90")));
        rows.add(row(2L, 43L, null));
        GatewayResult r = new GatewayResult(Arrays.asList("id", "user_id", "amount"), rows,
                "SUCCESS");

        List<Map<String, Object>> maps = r.toMaps();
        assertEquals(2, maps.size());
        assertEquals(Arrays.asList("id", "user_id", "amount"),
                new ArrayList<String>(maps.get(0).keySet()));   // 保持列顺序
        assertEquals(Long.valueOf(1L), maps.get(0).get("id"));
        assertEquals(Long.valueOf(42L), maps.get(0).get("user_id"));
        assertEquals(new BigDecimal("9.90"), maps.get(0).get("amount"));
        assertTrue(maps.get(1).containsKey("amount"));          // null 值仍然保留这个 key
        assertNull(maps.get(1).get("amount"));
        assertEquals(Long.valueOf(2L), maps.get(1).get("id"));
    }

    @Test
    public void toMapsToleratesShortRowsDuplicateAndNullColumns() {
        List<Row> rows = new ArrayList<Row>();
        rows.add(row("a", "b"));                    // 比列数短
        rows.add(row("x", "y", "z", "w"));          // 比列数长
        GatewayResult r = new GatewayResult(
                Arrays.asList("c1", null, "c1", "c4"), rows, "SUCCESS");

        List<Map<String, Object>> maps = r.toMaps();
        assertEquals(1, maps.get(0).size());        // 短行只放得下的部分
        assertEquals("a", maps.get(0).get("c1"));
        assertEquals(2, maps.get(1).size());        // null 列名被跳过、多余单元格被忽略
        assertEquals("z", maps.get(1).get("c1"));   // 重复列名：后者（第 3 列）覆盖前者
        assertEquals("w", maps.get(1).get("c4"));
    }

    @Test
    public void toMapReturnsFirstRowOrNull() {
        GatewayResult empty = new GatewayResult(Arrays.asList("a"),
                new ArrayList<Row>(), "SUCCESS");
        assertNull(empty.toMap());
        assertTrue(empty.toMaps().isEmpty());

        List<Row> rows = new ArrayList<Row>();
        rows.add(row("only"));
        assertEquals("only",
                new GatewayResult(Arrays.asList("a"), rows, "SUCCESS").toMap().get("a"));
    }

    @Test
    public void stopTask() throws IOException, InterruptedException {
        StopResult result = SqlGatewayClient.stopJob("http://flink01:8081", "c621efdacffe2a6e9f5faf0771e557fb");
        System.out.println(result);
    }
}
