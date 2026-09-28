package com.easy.etl.flink.http;

import com.easy.etl.flink.http.em.JobState;
import com.easy.etl.flink.http.gateway.GatewayExecutionException;
import com.easy.etl.flink.http.gateway.GatewayHttpException;
import com.easy.etl.flink.http.gateway.GatewayResultCursor;
import com.easy.etl.flink.http.gateway.SqlGatewayClient;
import com.easy.etl.flink.http.model.ClusterLog;
import com.easy.etl.flink.http.model.GatewayInfo;
import com.easy.etl.flink.http.model.GatewayResult;
import com.easy.etl.flink.http.model.JobExceptionEntry;
import com.easy.etl.flink.http.model.JobExceptions;
import com.easy.etl.flink.http.model.JobStatus;
import com.easy.etl.flink.http.model.OperationInfo;
import com.easy.etl.flink.http.model.OperationLog;
import com.easy.etl.flink.http.model.OperationStatus;
import com.easy.etl.flink.http.model.Row;
import com.easy.etl.flink.http.model.SessionInfo;
import com.easy.etl.flink.http.model.StopResult;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * 针对**真实** Flink SQL Gateway 的接口集成测试：逐一点调用
 * {@link SqlGatewayClient} 的全部公开接口，并把每一项的结果记录到
 * {@code target/flink-live-report.txt}。
 *
 * <p>类名以 {@code IT} 结尾，因此不会被 surefire 的默认包含规则
 * （{@code *Test}）命中，{@code mvn test} 保持离线可跑。要跑它：
 * <pre>
 *   mvn -o test -Dtest=GatewayLiveIT -DfailIfNoTests=false
 * </pre>
 * 可覆盖的地址（默认就是本项目的测试环境）：
 * <pre>
 *   -Dflink.gateway=http://flink01:32325     SQL Gateway
 *   -Dflink.cluster=http://flink01:8081      集群 JobManager REST
 * </pre>
 * 网关不可达时整个类会被跳过（{@link Assume}），不会把构建搞红。
 */
public class GatewayLiveIT {

    private static final String GATEWAY =
            System.getProperty("flink.gateway", "http://flink01:32325");
    private static final String CLUSTER =
            System.getProperty("flink.cluster", "http://flink01:8081");

    private static final List<String> REPORT = new ArrayList<String>();
    /** 单个用例自己开的会话，用例结束即关闭。 */
    private static final List<String> SESSIONS = new ArrayList<String>();
    /** 类级别常驻的会话（承载 healthyJobId），只在 @AfterClass 关闭。 */
    private static final List<String> PINNED_SESSIONS = new ArrayList<String>();
    private static final List<String> JOBS = new ArrayList<String>();
    private static final AtomicInteger SEQ = new AtomicInteger();

    private static boolean pinning;
    private static String healthyJobId;

    @BeforeClass
    public static void assumeGatewayReachable() {
        boolean gatewayUp = SqlGatewayClient.testConnection(GATEWAY, 5_000);
        boolean clusterUp = clusterReachable();
        Assume.assumeTrue("跳过：SQL Gateway 不可达 " + GATEWAY, gatewayUp);
        Assume.assumeTrue("跳过：集群 REST 不可达 " + CLUSTER, clusterUp);

        record("INFO", "测试环境",
                "SQL Gateway=" + GATEWAY + "，集群 REST=" + CLUSTER);
        GatewayInfo info = new SqlGatewayClient(GATEWAY).getInfo();
        record("INFO", "网关版本", info == null ? "未暴露 /v1/info" : info.toString());
        try {
            pinning = true;
            healthyJobId = createStreamingJob(true);
            record("INFO", "常驻流作业（供集群接口用例复用）", healthyJobId);
        } catch (Exception e) {
            record("INFO", "常驻流作业", "创建失败：" + e.getMessage());
        } finally {
            pinning = false;
        }
    }

    @AfterClass
    public static void cleanupAndWriteReport() throws IOException {
        for (String jobId : JOBS) {
            if (healthyJobId != null && healthyJobId.equals(jobId)) {
                continue;
            }
            try {
                SqlGatewayClient.cancelJob(CLUSTER, jobId);
            } catch (Exception ignore) {
                // 作业可能已经结束，忽略
            }
        }
        if (healthyJobId != null) {
            try {
                SqlGatewayClient.stopJob(CLUSTER, healthyJobId);
            } catch (Exception ignore) {
                // 作业可能已经结束，忽略
            }
        }
        SqlGatewayClient client = new SqlGatewayClient(GATEWAY);
        List<String> all = new ArrayList<String>(SESSIONS);
        all.addAll(PINNED_SESSIONS);
        for (String handle : all) {
            try {
                client.closeSession(handle);
            } catch (Exception ignore) {
                // 尽力而为
            }
        }
        Files.createDirectories(Paths.get("target"));
        Files.write(Paths.get("target", "flink-live-report.txt"), REPORT, StandardCharsets.UTF_8);
    }

    @After
    public void closeSessions() {
        SqlGatewayClient client = new SqlGatewayClient(GATEWAY);
        for (String handle : new ArrayList<String>(SESSIONS)) {
            try {
                client.closeSession(handle);
            } catch (Exception ignore) {
                // 尽力而为
            }
            SESSIONS.remove(handle);
        }
    }

    // ==================================================================
    // 网关信息 / 探活 / 会话
    // ==================================================================

    @Test
    public void getInfoAndEscapeHatch() throws Exception {
        SqlGatewayClient client = new SqlGatewayClient(GATEWAY);

        GatewayInfo info = client.getInfo();
        assertNotNull("getInfo() 不应为 null（该网关暴露了 /v1/info）", info);
        assertFalse("productName 不应为空", info.getProductName().isEmpty());
        assertFalse("version 不应为空", info.getVersion().isEmpty());
        record("PASS", "client.getInfo()",
                "productName=" + info.getProductName() + ", version=" + info.getVersion());

        Map<String, Object> raw = client.getJson("/info");
        assertEquals(info.getVersion(), String.valueOf(raw.get("version")));
        record("PASS", "client.getJson(\"/info\")", "version=" + raw.get("version"));

        // getJson() 逃生口指向集群时同样可用（脚本会补 /v1，JM REST 也支持 /v1 前缀）
        SqlGatewayClient cluster = new SqlGatewayClient(CLUSTER);
        Map<String, Object> overview = cluster.getJson("/jobs/overview");
        assertTrue("集群 /jobs/overview 应带 jobs 字段", overview.containsKey("jobs"));
        Map<String, Object> tms = cluster.getJson("/taskmanagers");
        assertTrue("集群 /taskmanagers 应带 taskmanagers 字段", tms.containsKey("taskmanagers"));
        record("PASS", "client.getJson(\"/jobs/overview\")（集群侧逃生口）",
                "jobs=" + ((List<?>) overview.get("jobs")).size()
                        + ", taskmanagers=" + ((List<?>) tms.get("taskmanagers")).size());
    }

    @Test
    public void pingCheckReachableAndStaticTestConnection() throws Exception {
        SqlGatewayClient client = new SqlGatewayClient(GATEWAY);
        assertTrue("ping() 应为 true", client.ping());
        assertTrue("ping(3000) 应为 true", client.ping(3_000));
        client.checkReachable();
        client.checkReachable(3_000);
        assertTrue("静态 testConnection(address)", SqlGatewayClient.testConnection(GATEWAY));
        assertTrue("静态 testConnection(address, timeout)",
                SqlGatewayClient.testConnection(GATEWAY, 3_000));
        assertTrue("jdbc:flink: 前缀写法也应可用",
                SqlGatewayClient.testConnection("jdbc:flink:" + GATEWAY, 3_000));
        record("PASS", "client.ping() / checkReachable() / testConnection()",
                "全部通过（含 jdbc:flink: 前缀写法）");

        int deadPort = findFreePort();
        SqlGatewayClient dead = new SqlGatewayClient("http://127.0.0.1:" + deadPort);
        assertFalse("不可达地址 ping() 应为 false", dead.ping(2_000));
        assertFalse("不可达地址 testConnection() 应为 false",
                SqlGatewayClient.testConnection("http://127.0.0.1:" + deadPort, 2_000));
        try {
            dead.checkReachable(2_000);
            fail("不可达地址 checkReachable() 应抛出 IOException");
        } catch (IOException e) {
            assertTrue("异常信息里应包含网关地址，实际：" + e.getMessage(),
                    e.getMessage().contains("http://127.0.0.1:" + deadPort));
            record("PASS", "client.checkReachable()（不可达路径）",
                    "IOException => " + shortText(e.getMessage()));
        }
    }

    @Test
    public void sessionLifecycle() throws Exception {
        SqlGatewayClient client = new SqlGatewayClient(GATEWAY);

        SessionInfo session = client.openSession();
        String handle = session.getSessionHandle();
        SESSIONS.add(handle);
        assertNotNull("openSession() 应返回 sessionHandle", handle);
        assertEquals(handle, session.getSessionHandle());
        record("PASS", "client.openSession()", "sessionHandle=" + handle);

        SessionInfo timed = client.openSession(5_000);
        SESSIONS.add(timed.getSessionHandle());
        assertNotNull("openSession(int) 应返回 sessionHandle", timed.getSessionHandle());
        record("PASS", "client.openSession(readTimeoutMs)",
                "sessionHandle=" + timed.getSessionHandle());

        client.heartbeat(handle);
        client.heartbeat(handle, 5_000);
        record("PASS", "client.heartbeat(s) / heartbeat(s, readTimeoutMs)", "两次心跳均 2xx");

        client.closeSession(handle);
        SESSIONS.remove(handle);
        client.closeSession(handle); // 关闭已不存在的会话不应抛异常
        record("PASS", "client.closeSession(s)", "正常关闭；重复关闭不抛异常");
    }

    // ==================================================================
    // 语句执行 / 结果
    // ==================================================================

    @Test
    public void executeReadsTypedRowsAndDoesNotTruncateByDefault() throws Exception {
        SqlGatewayClient client = batchClient();
        String handle = open(client);
        String table = "big_" + SEQ.incrementAndGet();
        client.execute(handle, "CREATE TABLE " + table + " (id BIGINT, name STRING, amount DOUBLE)"
                + " WITH ('connector'='datagen', 'number-of-rows'='1500')");

        GatewayResult result = client.execute(handle, "SELECT id, name, amount FROM " + table);
        assertEquals(3, result.getColumns().size());
        // 回归点：maxRows 默认必须是 0（不限），否则 execute() 会静默截断到 1000 行
        assertEquals("默认不应截断结果（maxRows=0 表示不限）", 1500, result.getRows().size());
        assertFalse("SELECT 不是 DML", result.isDml());
        assertEquals(0L, result.getUpdateCount());
        // 事实：批模式下 SELECT 也会被集群当成一个作业来跑，网关会在结果页顶层
        // 回 jobID（实测 isQueryResult=true 且 jobID 为 32 位 hex），因此这里不是 null
        assertNotNull("批 SELECT 也会返回作业 ID", result.getJobId());
        assertTrue("作业 ID 应是 32 位 hex，实际：" + result.getJobId(),
                result.getJobId().matches("[0-9a-fA-F]{32}"));
        assertEquals("SUCCESS_WITH_CONTENT", result.getResultKind());

        List<Map<String, Object>> maps = result.toMaps();
        assertEquals(1500, maps.size());
        assertEquals("toMaps() 的 key 应保持列顺序", "id",
                maps.get(0).keySet().iterator().next());
        Row row = result.getRows().get(0);
        assertEquals(3, row.size());
        assertNotNull(row.get(0));
        assertNotNull(result.toMap());
        record("PASS", "client.execute(s, sql)",
                "columns=" + result.getColumns() + ", rows=" + result.getRows().size()
                        + ", resultKind=" + result.getResultKind() + ", jobId=" + result.getJobId());
        record("PASS", "GatewayResult.getColumns/getRows/toMaps/toMap", "首行=" + row);
    }

    @Test
    public void executeWithExplicitMaxRowsPollAndTimeout() throws Exception {
        SqlGatewayClient client = batchClient();
        String handle = open(client);
        String table = "lim_" + SEQ.incrementAndGet();
        client.execute(handle, "CREATE TABLE " + table + " (id BIGINT) WITH ("
                + "'connector'='datagen', 'number-of-rows'='500')");

        GatewayResult result = client.execute(handle, "SELECT * FROM " + table, 10, 200, 120_000);
        assertEquals("显式 maxRows=10 时应精确返回 10 行", 10, result.getRows().size());
        record("PASS", "client.execute(s, sql, maxRows, pollIntervalMs, operationTimeoutMs)",
                "maxRows=10 => rows=" + result.getRows().size());
    }

    @Test
    public void submitCursorStreamsPagesAndHonoursMaxRows() throws Exception {
        SqlGatewayClient client = batchClient();
        String handle = open(client);
        String table = "cur_" + SEQ.incrementAndGet();
        client.execute(handle, "CREATE TABLE " + table + " (id BIGINT, tag STRING) WITH ("
                + "'connector'='datagen', 'number-of-rows'='300')");

        int rows = 0;
        String firstRow = null;
        try (GatewayResultCursor cursor = client.submit(
                handle, "SELECT * FROM " + table, 25, 200, 120_000)) {
            assertNotNull("submit() 返回的游标应带 operationHandle", cursor.operationHandle());
            assertEquals("columns() 可用", 2, cursor.columns().size());
            assertEquals("columnNames() 可用", cursor.columns().get(0).getName(),
                    cursor.columnNames().get(0));
            assertEquals("resultKind() 可用", "SUCCESS_WITH_CONTENT", cursor.resultKind());
            while (cursor.next()) {
                rows++;
                if (firstRow == null) {
                    firstRow = String.valueOf(cursor.row());
                    assertNotNull("rowAsMap() 可用", cursor.rowAsMap());
                }
            }
        }
        assertEquals("maxRows=25 时应精确读到 25 行", 25, rows);
        record("PASS", "client.submit(s, sql) -> GatewayResultCursor",
                "columns/columnNames/resultKind/next/row/rowAsMap/operationHandle 均可用，"
                        + "rows=" + rows + "，首行=" + firstRow);

        try (GatewayResultCursor cursor = client.submit(handle, "SELECT * FROM " + table)) {
            GatewayResult drained = cursor.drain();
            assertEquals(300, drained.getRows().size());
            record("PASS", "GatewayResultCursor.drain()", "rows=" + drained.getRows().size());
        }
    }

    @Test
    public void manualOrchestrationWithStatementAndStatus() throws Exception {
        SqlGatewayClient client = batchClient();
        String handle = open(client);

        OperationInfo op = client.executeStatement(handle, "SELECT 1 AS a, 'x' AS b");
        assertNotNull("executeStatement() 应返回 operationHandle", op.getOperationHandle());
        record("PASS", "client.executeStatement(s, sql)",
                "operationHandle=" + op.getOperationHandle());

        OperationStatus status = null;
        for (int i = 0; i < 40; i++) {
            status = client.getOperationStatus(handle, op.getOperationHandle());
            if (status.isTerminal()) {
                break;
            }
            assertTrue("轮询期间应为 RUNNING", status.isRunning());
            Thread.sleep(200);
        }
        assertNotNull(status);
        assertTrue("操作应结束，实际：" + status.getStatus(), status.isFinished());
        assertTrue(status.isTerminal());
        assertFalse(status.isError());
        assertFalse(status.isFailure());
        record("PASS", "client.getOperationStatus(s, op)",
                "status=" + status.getStatus() + ", isFinished=" + status.isFinished()
                        + ", isTerminal=" + status.isTerminal());

        client.closeQuietly(handle, op.getOperationHandle());
        record("PASS", "client.closeQuietly(s, op)", "已发出 DELETE .../operations/{op}/close");
    }

    @Test
    public void closeAndCancelActuallyReleaseTheOperation() throws Exception {
        SqlGatewayClient client = batchClient();
        String handle = open(client);

        OperationInfo op = client.executeStatement(handle, "SELECT 1");
        waitTerminal(client, handle, op.getOperationHandle());
        client.closeQuietly(handle, op.getOperationHandle());

        // 回归点：Flink 2.x 只认 .../operations/{op}/close，旧路径会 404 并被静默吞掉，
        // 操作会一直挂在网关上。这里断言操作确实被释放了。
        boolean released;
        try {
            client.getJson("/sessions/" + handle + "/operations/" + op.getOperationHandle()
                    + "/status");
            released = false;
        } catch (IOException e) {
            released = true;
        }
        assertTrue("closeQuietly() 之后操作应已从网关上释放", released);
        record("PASS", "closeQuietly() 的资源释放效果", "关闭后再查该操作 => 报错（已释放）");

        client.cancelOperation(handle, op.getOperationHandle());
        record("PASS", "client.cancelOperation(s, op)", "对已结束的操作调用不抛异常");
    }

    @Test
    public void operationLogAndGatewayErrorText() throws Exception {
        SqlGatewayClient client = batchClient();
        String handle = open(client);

        OperationInfo op = client.executeStatement(handle, "EXPLAIN SELECT 1");
        waitTerminal(client, handle, op.getOperationHandle());
        OperationLog log = client.fetchOperationLog(handle, op.getOperationHandle());
        assertFalse("EXPLAIN 应产生日志行", log.isEmpty());
        assertNotNull("getFirstLine() 应有内容", log.getFirstLine());
        record("PASS", "client.fetchOperationLog(s, op)",
                "lines=" + log.getLines().size() + "，首行=" + shortText(log.getFirstLine()));

        OperationLog onePage = client.fetchOperationLog(handle, op.getOperationHandle(), 1, 200);
        assertTrue(onePage.getLines().size() <= log.getLines().size());
        record("PASS", "client.fetchOperationLog(s, op, maxPages, pollIntervalMs)",
                "maxPages=1 => lines=" + onePage.getLines().size());

        // 回归点：失败操作时网关回 HTTP 500 + {"errors":[...]}，
        // 客户端必须把真实根因还原出来，而不是「网关未返回错误详情」
        String missing = "no_such_table_" + SEQ.incrementAndGet();
        OperationInfo bad = client.executeStatement(handle, "SELECT * FROM " + missing);
        waitTerminal(client, handle, bad.getOperationHandle());
        OperationStatus badStatus = client.getOperationStatus(handle, bad.getOperationHandle());
        assertEquals("不存在的表应以 ERROR 结束", "ERROR", badStatus.getStatus());
        assertTrue(badStatus.isError());
        assertTrue(badStatus.isFailure());

        String detail = client.fetchOperationError(handle, bad.getOperationHandle());
        assertNotNull("fetchOperationError() 应还原出网关根因", detail);
        assertTrue("根因应指出表不存在，实际：" + detail,
                detail.contains(missing) && detail.contains("not found"));
        record("PASS", "client.fetchOperationError(s, op)", shortText(detail));

        try {
            client.execute(handle, "SELECT * FROM " + missing);
            fail("失败语句应抛 GatewayExecutionException");
        } catch (GatewayExecutionException e) {
            assertTrue("异常 message 应是网关的真实错误文本，实际：" + e.getMessage(),
                    e.getMessage().contains("not found"));
            record("PASS", "client.execute() 失败路径 => GatewayExecutionException",
                    shortText(e.getMessage()));
        }
    }

    @Test
    public void nonQueryStatementsAndSessionProperties() throws Exception {
        SqlGatewayClient client = batchClient();
        String handle = open(client);

        GatewayResult set = client.execute(handle, "SET 'pipeline.name' = 'gateway-live-it'");
        assertEquals("SUCCESS", set.getResultKind());
        assertEquals("OK", String.valueOf(set.getRows().get(0).get(0)));
        record("PASS", "SET 语句（会话属性）", "resultKind=" + set.getResultKind()
                + ", 首行=" + set.toMap());

        String table = "show_" + SEQ.incrementAndGet();
        client.execute(handle, "CREATE TABLE " + table + " (id INT) WITH ('connector'='datagen')");
        GatewayResult tables = client.execute(handle, "SHOW TABLES");
        assertTrue("SHOW TABLES 应列出刚建的表",
                String.valueOf(tables.toMaps()).contains(table));
        record("PASS", "SHOW TABLES", "columns=" + tables.getColumns()
                + ", rows=" + tables.getRows().size());

        GatewayResult explain = client.execute(handle, "EXPLAIN SELECT 1");
        assertEquals("SUCCESS_WITH_CONTENT", explain.getResultKind());
        record("PASS", "EXPLAIN", "resultKind=" + explain.getResultKind());
    }

    @Test
    public void dmlReturnsFlinkJobId() throws Exception {
        SqlGatewayClient client = streamingClient(false);
        String handle = open(client);
        String suffix = String.valueOf(SEQ.incrementAndGet());
        client.execute(handle, "CREATE TABLE dml_src_" + suffix
                + " (id BIGINT) WITH ('connector'='datagen', 'rows-per-second'='1')");
        client.execute(handle, "CREATE TABLE dml_snk_" + suffix
                + " (id BIGINT) WITH ('connector'='print')");

        GatewayResult dml = client.execute(handle,
                "INSERT INTO dml_snk_" + suffix + " SELECT * FROM dml_src_" + suffix);
        String jobId = dml.getJobId();
        assertNotNull("DML 结果应带作业 ID", jobId);
        assertTrue("作业 ID 应是 32 位 hex，实际：" + jobId, jobId.matches("[0-9a-fA-F]{32}"));
        // 回归点：流模式 INSERT 的标记列是 "job id" 而不是 "row count"，
        // 只认后者的实现会让 isDml() 在流模式下永远为 false
        assertTrue("流式 INSERT 也是 DML，实际首列：" + dml.getColumns(), dml.isDml());
        assertEquals("流式 INSERT 没有 row count，updateCount 应为 0", 0L, dml.getUpdateCount());
        JOBS.add(jobId);
        record("PASS", "DML => GatewayResult.getJobId()",
                "jobId=" + jobId + ", columns=" + dml.getColumns()
                        + ", resultKind=" + dml.getResultKind()
                        + ", isDml=" + dml.isDml() + ", updateCount=" + dml.getUpdateCount());
    }

    @Test
    public void rowFormatOptionAndGatewayWireBehaviour() throws Exception {
        assertEquals("PLAIN_TEXT",
                GatewayConfig.parse(GATEWAY + "?rowFormat=table").getResultRowFormat());
        assertEquals("PLAIN_TEXT",
                GatewayConfig.parse(GATEWAY + "?rowFormat=plain_text").getResultRowFormat());
        assertEquals("JSON",
                GatewayConfig.parse(GATEWAY + "?rowFormat=json").getResultRowFormat());
        assertEquals("JSON",
                GatewayConfig.parse(GATEWAY + "?rowFormat=JSON").getResultRowFormat());
        record("PASS", "GatewayConfig#getResultRowFormat() 取值映射",
                "table/plain_text => PLAIN_TEXT，json => JSON");

        SqlGatewayClient json = client("rowFormat=json");
        String handle = open(json);
        // 注意：别名不能用 dec —— Flink 的 SQL parser 把 DEC 当保留字，会直接
        // 报 "Encountered \"dec\"" 而解析失败（实测）
        GatewayResult result = json.execute(handle,
                "SELECT CAST(1 AS BIGINT) AS big, CAST(2.5 AS DECIMAL(10,2)) AS d1");
        Object big = result.getRows().get(0).get(0);
        Object dec = result.getRows().get(0).get(1);
        assertTrue("BIGINT 应为整数类型，实际：" + big.getClass(),
                big instanceof Number);
        assertTrue("DECIMAL 应为 BigDecimal，实际：" + dec.getClass(),
                dec instanceof java.math.BigDecimal);
        record("PASS", "rowFormat=json 读取带类型结果",
                "big=" + big + "(" + big.getClass().getSimpleName() + ")"
                        + ", d1=" + dec + "(" + dec.getClass().getSimpleName() + ")");

        // 事实记录：Flink 2.2.1 的 /v1/.../result 会忽略 rowFormat 参数（恒返回
        // "rowFormat":"JSON"、值带类型），只有不带 /v1 前缀的同名路径才真正生效。
        // 用裸探针把两种行为都记录在报告里。
        OperationInfo op = json.executeStatement(handle,
                "SELECT 1 AS a, CAST(2.5 AS DECIMAL(10,2)) AS b");
        waitTerminal(json, handle, op.getOperationHandle());
        String v1Probe = rawGet(GATEWAY + "/v1/sessions/" + handle + "/operations/"
                + op.getOperationHandle() + "/result/0?rowFormat=PLAIN_TEXT");
        String defaultProbe = rawGet(GATEWAY + "/sessions/" + handle + "/operations/"
                + op.getOperationHandle() + "/result/0?rowFormat=PLAIN_TEXT");
        record("INFO", "网关 /v1 结果接口：rowFormat=PLAIN_TEXT 被忽略（仍是 JSON）",
                "V1 => " + shortText(v1Probe));
        record("INFO", "网关无版本前缀结果接口：rowFormat=PLAIN_TEXT 生效（值为字符串）",
                shortText(defaultProbe));
    }

    // ==================================================================
    // 集群（JobManager REST，默认 8081）
    // ==================================================================

    @Test
    public void jobStatusApis() throws Exception {
        Assume.assumeTrue("跳过：常驻流作业未创建成功", healthyJobId != null);

        JobStatus status = SqlGatewayClient.fetchJobStatus(CLUSTER, healthyJobId);
        assertEquals("作业应处于 RUNNING", "RUNNING", status.getState());
        assertTrue(status.isRunning());
        assertTrue(status.isActive());
        assertFalse(status.isTerminal());
        assertFalse(status.isFailed());
        assertEquals(healthyJobId, status.getJobId());
        assertNotNull("作业名不应为空", status.getName());
        assertTrue("start-time 应为正数", status.getStartTime() > 0);
        assertTrue("运行中的作业 end-time 应为 -1", status.getEndTime() < 0);
        record("PASS", "SqlGatewayClient.fetchJobStatus(url, jobId)",
                "state=" + status.getState() + ", name=" + status.getName()
                        + ", duration=" + status.getDuration() + "ms");

        assertEquals("fetchJobState 应与 getJobState() 一致", status.getJobState(),
                SqlGatewayClient.fetchJobState(CLUSTER, healthyJobId));
        assertEquals(JobState.RUNNING, SqlGatewayClient.fetchJobState(CLUSTER, healthyJobId));
        assertEquals(3, status.getStateCode());
        assertTrue(status.getJobState().isActive());
        assertFalse(status.getJobState().isFailure());
        record("PASS", "SqlGatewayClient.fetchJobState(url, jobId)",
                "JobState=" + status.getJobState() + ", code=" + status.getStateCode()
                        + ", desc=" + status.getJobState().getDescription());

        assertTrue("isJobRunning() 对运行中的作业应为 true",
                SqlGatewayClient.isJobRunning(CLUSTER, healthyJobId));
        record("PASS", "SqlGatewayClient.isJobRunning(url, jobId)", "true");

        List<JobStatus> all = SqlGatewayClient.fetchJobStatuses(CLUSTER);
        assertFalse("集群上应有作业", all.isEmpty());
        boolean found = false;
        for (JobStatus job : all) {
            if (healthyJobId.equals(job.getJobId())) {
                found = true;
            }
        }
        assertTrue("fetchJobStatuses 的结果应包含该作业", found);
        record("PASS", "SqlGatewayClient.fetchJobStatuses(url)", "作业总数=" + all.size());

        assertEquals(JobState.FAILED, JobState.fromState("failed"));
        assertEquals(JobState.CANCELED, JobState.fromState("CANCELLED"));
        assertEquals(JobState.UNKNOWN, JobState.fromState(null));
        assertTrue(JobState.SUSPENDED.isTerminal());
        assertFalse(JobState.SUSPENDED.isSettled());
        assertEquals(JobState.FINISHED, JobState.fromCode(7));
        assertEquals(JobState.UNKNOWN, JobState.fromCode(999));
        record("PASS", "JobState 枚举映射",
                "fromState(failed)=" + JobState.fromState("failed")
                        + ", fromState(CANCELLED)=" + JobState.fromState("CANCELLED")
                        + ", fromCode(7)=" + JobState.fromCode(7)
                        + ", fromCode(999)=" + JobState.fromCode(999));

        try {
            SqlGatewayClient.waitForJob(CLUSTER, healthyJobId, 200, 2_000);
            fail("未结束的作业在 2s 超时后应抛 GatewayExecutionException");
        } catch (GatewayExecutionException e) {
            record("PASS", "SqlGatewayClient.waitForJob(url, jobId, poll, timeout)（超时路径）",
                    shortText(e.getMessage()));
        }
    }

    @Test
    public void jobExceptionsAndClusterLogs() throws Exception {
        String failingJobId = createFailingStreamingJob();
        JobStatus failed = SqlGatewayClient.waitForJob(CLUSTER, failingJobId, 500, 90_000);
        assertTrue("作业应进入 FAILED，实际：" + failed.getState(), failed.isFailed());
        assertFalse("终态作业 isActive 应为 false", failed.isActive());
        record("PASS", "SqlGatewayClient.waitForJob(url, jobId, poll, timeout)",
                "state=" + failed.getState() + ", endTime=" + failed.getEndTime());

        // 回归点：Flink 2.x 的异常条目是 exceptionName/stacktrace（没有 root），
        // 旧解析会把所有条目丢掉，fetchJobExceptions 永远返回空
        JobExceptions exceptions = SqlGatewayClient.fetchJobExceptions(CLUSTER, failingJobId);
        assertFalse("fetchJobExceptions 应还原出异常历史", exceptions.isEmpty());
        JobExceptionEntry entry = exceptions.getEntries().get(0);
        assertNotNull("根因文本不应为 null", entry.getRoot());
        assertFalse(entry.getRoot().isEmpty());
        record("PASS", "SqlGatewayClient.fetchJobExceptions(url, jobId)",
                "entries=" + exceptions.getEntries().size()
                        + "，根因=" + shortText(entry.getRoot())
                        + "，exceptionName=" + entry.getExceptionName());
        record("INFO", "JobExceptions.getRoots()", shortText(exceptions.getRoots().toString()));

        Assume.assumeTrue("跳过：常驻流作业未创建成功", healthyJobId != null);
        assertTrue("健康的作业不应有异常",
                SqlGatewayClient.fetchJobExceptions(CLUSTER, healthyJobId).isEmpty());
        record("PASS", "fetchJobExceptions 于健康作业 => 空列表", "isEmpty=true");

        // 回归点：Flink 2.x 的日志端点是 text/plain 原始日志，不是 {"out":[...]} JSON，
        // 旧解析会直接抛 IllegalArgumentException
        ClusterLog jmLog = SqlGatewayClient.fetchJobManagerLog(CLUSTER);
        assertFalse("JobManager 日志不应为空", jmLog.isEmpty());
        assertNotNull(jmLog.getFirstLine());
        record("PASS", "SqlGatewayClient.fetchJobManagerLog(url)",
                "lines=" + jmLog.getLines().size() + "，首行=" + shortText(jmLog.getFirstLine()));

        // 事实：日志端点虽然接受 ?size=N，但 Flink 2.2.1 实测会忽略它——带与不带该参数
        // 返回的行数完全一致（都是整份日志）。所以客户端不靠它限流，改为在本地用
        // ClusterLog#tail(int) 截断。
        ClusterLog bounded = SqlGatewayClient.fetchClusterLog(CLUSTER, "/jobmanager/log?size=5");
        assertFalse("带 ?size=5 的日志不应为空", bounded.isEmpty());
        assertTrue("?size=5 被 Flink 2.x 忽略，仍应返回整份日志，实际行数："
                        + bounded.getLines().size(), bounded.getLines().size() > 5);
        record("INFO", "SqlGatewayClient.fetchClusterLog(url, logPath)",
                "?size=5 被网关忽略：仍返回 " + bounded.getLines().size() + " 行");

        ClusterLog tail = jmLog.tail(5);
        assertEquals("ClusterLog#tail(5) 应精确保留 5 行", 5, tail.getLines().size());
        assertEquals("tail() 应取日志末尾",
                jmLog.getLines().get(jmLog.getLines().size() - 1), tail.getLines().get(4));
        record("PASS", "ClusterLog#tail(n)（客户端本地截断日志）",
                "lines=" + tail.getLines().size() + "，末行=" + shortText(tail.getLines().get(4)));

        SqlGatewayClient cluster = new SqlGatewayClient(CLUSTER);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> tms =
                (List<Map<String, Object>>) cluster.getJson("/taskmanagers").get("taskmanagers");
        assertFalse("集群上应有 TaskManager", tms.isEmpty());
        String tmId = String.valueOf(tms.get(0).get("id"));
        ClusterLog tmLog = SqlGatewayClient.fetchTaskManagerLog(CLUSTER, tmId);
        assertFalse("TaskManager 日志不应为空", tmLog.isEmpty());
        record("PASS", "SqlGatewayClient.fetchTaskManagerLog(url, tmId)",
                "tmId=" + tmId + "，lines=" + tmLog.getLines().size());
    }

    @Test
    public void stopStreamingJobAndCollectCheckpoint() throws Exception {
        String jobId = createStreamingJob(true);
        assertNotNull("流作业创建失败", jobId);
        assertEquals("RUNNING", SqlGatewayClient.fetchJobStatus(CLUSTER, jobId).getState());

        String requestId = SqlGatewayClient.triggerStop(CLUSTER, jobId, true);
        assertNotNull("triggerStop 应返回 request-id", requestId);
        record("PASS", "SqlGatewayClient.triggerStop(url, jobId, withDrain)",
                "requestId=" + requestId);

        StopResult result = null;
        for (int i = 0; i < 60; i++) {
            result = SqlGatewayClient.getStopResult(CLUSTER, jobId, requestId);
            if (result.isTerminal()) {
                break;
            }
            assertTrue("轮询期间应处于 IN_PROGRESS", result.isInProgress());
            Thread.sleep(500);
        }
        assertNotNull(result);
        assertTrue("停止应以 COMPLETED 结束，实际：" + result.getStatus(), result.isCompleted());
        // 回归点：Flink 2.x 把 location 嵌在 operation.location 里
        assertNotNull("getLocation() 应给出可恢复的 checkpoint 路径", result.getLocation());
        assertFalse(result.getLocation().isEmpty());
        assertEquals(jobId, result.getJobId());
        assertEquals(requestId, result.getRequestId());
        record("PASS", "SqlGatewayClient.getStopResult(url, jobId, requestId)",
                "status=" + result.getStatus() + ", location=" + result.getLocation());

        String jobId2 = createStreamingJob(true);
        StopResult simple = SqlGatewayClient.stopJob(CLUSTER, jobId2);
        assertTrue("stopJob 便利版应以 COMPLETED 结束", simple.isCompleted());
        assertNotNull("stopJob 便利版也应给出 checkpoint 路径", simple.getLocation());
        record("PASS", "SqlGatewayClient.stopJob(url, jobId)",
                "status=" + simple.getStatus() + ", location=" + simple.getLocation());

        String jobId3 = createStreamingJob(true);
        StopResult full = SqlGatewayClient.stopJob(CLUSTER, jobId3, true, 500, 120_000);
        assertTrue(full.isCompleted());
        assertFalse(full.isInProgress());
        assertFalse(full.isFailed());
        assertNull("成功时不应有 failureCause", full.getFailureCause());
        record("PASS", "SqlGatewayClient.stopJob(url, jobId, withDrain, poll, timeout)",
                "status=" + full.getStatus() + ", location=" + full.getLocation());
    }

    @Test
    public void cancelJobStopsStreamingJob() throws Exception {
        String jobId = createStreamingJob(false);
        assertNotNull("流作业创建失败", jobId);
        assertTrue(SqlGatewayClient.isJobRunning(CLUSTER, jobId));

        SqlGatewayClient.cancelJob(CLUSTER, jobId);
        JobStatus after = SqlGatewayClient.waitForJob(CLUSTER, jobId, 500, 60_000);
        assertTrue("取消后应进入终态，实际：" + after.getState(), after.isTerminal());
        assertTrue("取消后状态应为 CANCELED，实际：" + after.getState(), after.isCanceled());
        assertFalse("取消后 isJobRunning 应为 false",
                SqlGatewayClient.isJobRunning(CLUSTER, jobId));
        record("PASS", "SqlGatewayClient.cancelJob(url, jobId)",
                "state=" + after.getState() + ", isCanceled=" + after.isCanceled());
    }

    @Test
    public void gatewayHttpExceptionCarriesTheRealCause() throws Exception {
        SqlGatewayClient client = new SqlGatewayClient(GATEWAY);
        try {
            client.getJson("/sessions/not-a-session/operations/not-an-operation/status");
            fail("不存在的资源应抛异常");
        } catch (GatewayHttpException e) {
            assertTrue("状态码应 >= 400，实际：" + e.getStatusCode(), e.getStatusCode() >= 400);
            assertNotNull("应保留响应体原文", e.getBody());
            assertNotNull("应能提炼出可读错误文本", e.getErrorMessage());
            record("PASS", "GatewayHttpException 携带 状态码/响应体/根因",
                    "status=" + e.getStatusCode() + "，根因=" + shortText(e.getErrorMessage()));
        }
    }

    // ==================================================================
    // 辅助方法
    // ==================================================================

    /**
     * 所有会跑语句的测试客户端共用的参数。
     *
     * <ul>
     *   <li>{@code operationTimeoutMs=120000}：默认是 0（不限时），一旦作业拿不到
     *       资源就会永久阻塞；测试里给个上限，让失败快速暴露出来。</li>
     *   <li>{@code parallelism.default=1}：集群每个 TaskManager 只有 2 个槽位，
     *       默认并行度会按槽位数拉起，多个作业并行时会把槽位占满。</li>
     * </ul>
     */
    private static final String COMMON_PARAMS =
            "?operationTimeoutMs=120000&sessionProperty.parallelism.default=1";

    private static boolean clusterReachable() {
        try {
            new SqlGatewayClient(CLUSTER).getJson("/jobs/overview");
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private static SqlGatewayClient client(String extraParams) {
        return new SqlGatewayClient(GATEWAY + COMMON_PARAMS
                + (extraParams == null || extraParams.isEmpty() ? "" : "&" + extraParams));
    }

    private static SqlGatewayClient batchClient() {
        return client("sessionProperty.execution.runtime-mode=batch");
    }

    private static SqlGatewayClient streamingClient(boolean withCheckpointing) {
        return client("sessionProperty.execution.runtime-mode=streaming"
                + "&sessionProperty.restart-strategy=none"
                + (withCheckpointing
                ? "&sessionProperty.execution.checkpointing.interval=2s" : ""));
    }

    private static String open(SqlGatewayClient client) throws IOException {
        String handle = client.openSession().getSessionHandle();
        (pinning ? PINNED_SESSIONS : SESSIONS).add(handle);
        return handle;
    }

    private static void waitTerminal(SqlGatewayClient client, String handle, String op)
            throws IOException, InterruptedException {
        for (int i = 0; i < 120; i++) {
            OperationStatus status = client.getOperationStatus(handle, op);
            if (status.isTerminal()) {
                return;
            }
            Thread.sleep(200);
        }
        fail("操作在 24 秒内没有进入终态");
    }

    /** 建一个健康的流作业（datagen -> print），返回作业 ID。 */
    private static String createStreamingJob(boolean withCheckpointing)
            throws IOException, InterruptedException {
        String suffix = "live_" + SEQ.incrementAndGet();
        SqlGatewayClient client = streamingClient(withCheckpointing);
        String handle = open(client);
        client.execute(handle, "CREATE TABLE src_" + suffix
                + " (id BIGINT) WITH ('connector'='datagen', 'rows-per-second'='2')");
        client.execute(handle, "CREATE TABLE snk_" + suffix
                + " (id BIGINT) WITH ('connector'='print')");
        GatewayResult result = client.execute(handle,
                "INSERT INTO snk_" + suffix + " SELECT * FROM src_" + suffix);
        String jobId = result.getJobId();
        if (jobId != null) {
            JOBS.add(jobId);
        }
        return jobId;
    }

    /** 建一个必然失败且不重启的流作业（id 恒为 0 => 1/id 除零），返回作业 ID。 */
    private static String createFailingStreamingJob() throws IOException, InterruptedException {
        String suffix = "fail_" + SEQ.incrementAndGet();
        SqlGatewayClient client = streamingClient(false);
        String handle = open(client);
        client.execute(handle, "CREATE TABLE zero_src_" + suffix
                + " (id BIGINT) WITH ('connector'='datagen', 'fields.id.kind'='random',"
                + " 'fields.id.min'='0', 'fields.id.max'='0')");
        client.execute(handle, "CREATE TABLE fail_snk_" + suffix
                + " (v BIGINT) WITH ('connector'='print')");
        GatewayResult result = client.execute(handle,
                "INSERT INTO fail_snk_" + suffix + " SELECT 1 / id FROM zero_src_" + suffix);
        String jobId = result.getJobId();
        assertNotNull("失败作业也应返回作业 ID", jobId);
        JOBS.add(jobId);
        return jobId;
    }

    private static int findFreePort() throws IOException {
        ServerSocket socket = new ServerSocket(0);
        int port = socket.getLocalPort();
        socket.close();
        return port;
    }

    /** 裸 HTTP 探针（绕过客户端），用于记录网关自身的原始行为。 */
    private static String rawGet(String url) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setConnectTimeout(5_000);
        conn.setReadTimeout(15_000);
        conn.setRequestProperty("Accept", "application/json");
        int code = conn.getResponseCode();
        InputStream in = code >= 200 && code < 300 ? conn.getInputStream() : conn.getErrorStream();
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        if (in != null) {
            byte[] chunk = new byte[8192];
            int n;
            while ((n = in.read(chunk)) != -1) {
                buffer.write(chunk, 0, n);
            }
            in.close();
        }
        return code + " " + new String(buffer.toByteArray(), StandardCharsets.UTF_8);
    }

    private static String shortText(String text) {
        if (text == null) {
            return "null";
        }
        String flat = text.replace('\n', ' ').replaceAll("\\s+", " ").trim();
        return flat.length() <= 260 ? flat : flat.substring(0, 260) + "…";
    }

    private static void record(String status, String api, String detail) {
        String line = String.format("[%-4s] %-58s %s", status, api, detail);
        System.out.println(line);
        synchronized (REPORT) {
            REPORT.add(line);
        }
    }
}
