package com.easy.etl.flink.http;

import com.easy.etl.flink.http.model.ClusterLog;
import com.easy.etl.flink.http.model.Column;
import com.easy.etl.flink.http.model.ExecuteStatementRequest;
import com.easy.etl.flink.http.model.GatewayInfo;
import com.easy.etl.flink.http.model.JobExceptions;
import com.easy.etl.flink.http.model.OpenSessionRequest;
import com.easy.etl.flink.http.model.OperationInfo;
import com.easy.etl.flink.http.model.OperationStatus;
import com.easy.etl.flink.http.model.ResultPage;
import com.easy.etl.flink.http.model.ResultType;
import com.easy.etl.flink.http.model.Row;
import com.easy.etl.flink.http.model.SessionInfo;
import com.easy.etl.flink.http.util.Json;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** 客户端能理解的每种负载的 JSON -> 模型映射。 */
public class ModelMappingTest {

    private static Map<String, Object> json(String text) {
        return Json.parseObject(text);
    }

    @Test
    public void gatewayInfo() {
        GatewayInfo info = GatewayInfo.fromJson(
                json("{\"productName\":\"Apache Flink\",\"version\":\"1.18.0\"}"));
        assertEquals("Apache Flink", info.getProductName());
        assertEquals("1.18.0", info.getVersion());
    }

    @Test
    public void sessionAndOperationHandles() {
        assertEquals("s1", SessionInfo.fromJson(json("{\"sessionHandle\":\"s1\"}"))
                .getSessionHandle());
        assertEquals("op1", OperationInfo.fromJson(json("{\"operationHandle\":\"op1\"}"))
                .getOperationHandle());
    }

    @Test
    public void operationStatusFlags() {
        OperationStatus running = OperationStatus.fromJson(json("{\"status\":\"RUNNING\"}"));
        assertFalse(running.isTerminal());
        assertFalse(running.isFailure());
        assertTrue(running.isRunning());

        OperationStatus finished = OperationStatus.fromJson(json("{\"status\":\"finished\"}"));
        assertTrue(finished.isFinished());
        assertTrue(finished.isTerminal());
        assertFalse(finished.isFailure());

        OperationStatus error = new OperationStatus("ERROR");
        assertTrue(error.isError());
        assertTrue(error.isFailure());
        assertNull(new OperationStatus(null).getStatus());
    }

    @Test
    public void resultPageCarriesColumnsRowsAndNextToken() {
        ResultPage page = ResultPage.fromJson(json("{\"resultType\":\"PAYLOAD\","
                + "\"resultKind\":\"SUCCESS_WITH_CONTENT\","
                + "\"results\":{\"columns\":[{\"name\":\"id\",\"logicalType\":"
                + "{\"type\":\"INT\",\"nullable\":false}},{\"name\":\"name\"}],"
                + "\"data\":[[\"1\",\"a\"],[\"2\",\"b\"]]},"
                + "\"nextResultUri\":\"/v1/sessions/s/operations/op/result/1?rowFormat=table\"}"));

        assertEquals(ResultType.PAYLOAD, page.getType());
        assertEquals("SUCCESS_WITH_CONTENT", page.getResultKind());
        assertEquals(Arrays.asList("id", "name"), page.getData().getColumnNames());
        assertEquals(2, page.getData().getRowCount());
        assertEquals("{\"type\":\"INT\",\"nullable\":false}",
                page.getData().getColumns().get(0).getLogicalType());
        assertNull(page.getData().getColumns().get(1).getLogicalType());
        assertEquals("1", page.getData().getRows().get(0).get(0));
        assertEquals(1L, page.getNextToken());
        assertTrue(page.hasPayload());
        assertFalse(page.isEos());

        Map<String, Object> asMap = page.getData().getRows().get(1)
                .toMap(page.getData().getColumns());
        assertEquals("2", asMap.get("id"));
        assertEquals("b", asMap.get("name"));
    }

    @Test
    public void emptyAndEosPages() {
        assertTrue(ResultPage.fromJson(json("{\"resultType\":\"EOS\"}")).isEos());
        assertFalse(ResultPage.fromJson(json("{\"resultType\":\"EOS\"}")).hasPayload());
        assertTrue(ResultPage.fromJson(json("{}")).getData().isEmpty());
        assertEquals(-1L, ResultPage.fromJson(json("{}")).getNextToken());
        assertEquals(ResultType.NOT_READY,
                ResultPage.fromJson(json("{\"resultType\":\"NOT_READY\"}")).getType());
        assertEquals(ResultType.UNKNOWN,
                ResultPage.fromJson(json("{\"resultType\":\"SOMETHING_NEW\"}")).getType());
    }

    @Test
    public void rowParsesBothGatewayRowFormats() {
        // rowFormat=table 发送数组，rowFormat=json 发送 {"fields": {...}}。
        Row table = Row.fromJson(Json.parse("[\"1\",\"a\"]"));
        assertEquals(Arrays.<Object>asList("1", "a"), table.getValues());
        assertEquals("a", table.get(1));
        assertNull(table.get(9));

        Row jsonRow = Row.fromJson(Json.parse("{\"fields\":{\"id\":\"1\",\"name\":\"a\"}}"));
        assertEquals(Arrays.<Object>asList("1", "a"), jsonRow.getValues());
        assertEquals(2, jsonRow.size());

        // Flink 1.20+/2.x 增加了 changelog 的 kind，并把 fields 以位置数组的形式发送。
        Row kinded = Row.fromJson(Json.parse("{\"kind\":\"INSERT\",\"fields\":[\"1\",\"a\"]}"));
        assertEquals(Arrays.<Object>asList("1", "a"), kinded.getValues());
        assertEquals(2, kinded.size());
        assertEquals(Collections.<Object>emptyList(),
                Row.fromJson(Json.parse("{\"kind\":\"INSERT\"}")).getValues());

        assertEquals(0, Row.fromJson(null).size());
        Map<String, Object> named = jsonRow.toMap("id", "name");
        assertEquals("1", named.get("id"));
    }

    @Test
    public void columnFromPlainString() {
        Column c = Column.fromJson("id");
        assertEquals("id", c.getName());
        assertNull(c.getLogicalType());
    }

    @Test
    public void jobExceptionsReadsBothHistoryKeys() {
        JobExceptions modern = JobExceptions.fromJson(json("{\"exceptionHistory\":{\"entries\":["
                + "{\"root\":\"java.lang.RuntimeException: boom\",\"taskName\":\"A -> B\","
                + "\"timestamp\":1700000000000},"
                + "{\"root\":\"java.lang.ArithmeticException: / by zero\"}]}}"));
        assertEquals(2, modern.getEntries().size());
        assertEquals("java.lang.RuntimeException: boom", modern.getRoots().get(0));
        assertEquals("A -> B", modern.getEntries().get(0).getTaskName());
        assertEquals(Long.valueOf(1700000000000L), modern.getEntries().get(0).getTimestamp());
        assertNull(modern.getEntries().get(1).getTimestamp());

        JobExceptions legacy = JobExceptions.fromJson(json("{\"exception-history\":{\"entries\":["
                + "{\"root\":\"old key\"}]}}"));
        assertEquals(Arrays.asList("old key"), legacy.getRoots());
        assertTrue(JobExceptions.fromJson(json("{}")).isEmpty());
    }

    @Test
    public void clusterLogReadsStringsAndObjects() {
        ClusterLog strings = ClusterLog.fromJson(json("{\"out\":[\"a\",\"b\"]}"));
        assertEquals(Arrays.asList("a", "b"), strings.getLines());

        ClusterLog objects = ClusterLog.fromJson(
                json("{\"out\":[{\"line\":\"l1\"},{\"message\":\"l2\"},{}]}"));
        assertEquals(Arrays.asList("l1", "l2"), objects.getLines());
        assertTrue(ClusterLog.fromJson(json("{}")).isEmpty());
    }

    /**
     * 日志端点的 {@code ?size=N} 在 Flink 2.x 上被忽略，限流只能靠客户端自己截断。
     */
    @Test
    public void clusterLogTailBoundsLineCount() {
        ClusterLog log = ClusterLog.fromText("a\nb\nc\nd\ne");

        assertEquals(Arrays.asList("d", "e"), log.tail(2).getLines());
        assertEquals(5, log.tail(5).getLines().size());
        assertEquals(5, log.tail(99).getLines().size());
        assertTrue(log.tail(0).isEmpty());
        assertTrue(log.tail(-1).isEmpty());
        assertTrue(ClusterLog.empty().tail(3).isEmpty());
    }

    @Test
    public void requestBodiesSerializeToJson() {
        Map<String, String> props = new LinkedHashMap<String, String>();
        props.put("execution.runtime-mode", "batch");
        assertEquals("{\"properties\":{\"execution.runtime-mode\":\"batch\"}}",
                Json.write(new OpenSessionRequest(props).toJson()));
        assertEquals("{}", Json.write(OpenSessionRequest.withoutProperties().toJson()));
        assertEquals("{\"statement\":\"SELECT 1\"}",
                Json.write(new ExecuteStatementRequest("SELECT 1").toJson()));
    }
}
