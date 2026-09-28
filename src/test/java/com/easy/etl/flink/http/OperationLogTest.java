package com.easy.etl.flink.http;

import com.easy.etl.flink.http.gateway.SqlGatewayClient;
import com.easy.etl.flink.http.model.ClusterLog;
import com.easy.etl.flink.http.model.JobExceptions;
import com.easy.etl.flink.http.model.OperationLog;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** 针对桩端点（stub endpoint）的操作日志 / 集群 REST 日志测试。 */
public class OperationLogTest {

    private HttpServer server;
    private final AtomicInteger resultRequests = new AtomicInteger();

    @Before
    public void startStub() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(null);
        server.createContext("/", new HttpHandler() {
            @Override
            public void handle(HttpExchange ex) throws IOException {
                String path = ex.getRequestURI().getPath();
                if (path.startsWith("/v1/sessions/s1/operations/op1/result/")) {
                    // 第一次结果调用表现为一个尚未完成的流式作业。
                    int n = resultRequests.incrementAndGet();
                    if (n == 1) {
                        respond(ex, 200, "{\"resultType\":\"NOT_READY\"}");
                    } else if (path.endsWith("/result/0")) {
                        respond(ex, 200, "{\"resultType\":\"PAYLOAD\",\"resultKind\":\"SUCCESS_WITH_CONTENT\","
                                + "\"results\":{\"columns\":[{\"name\":\"job id\"}],"
                                + "\"data\":[[\"d1a0f0e0deadbeefd1a0f0e0deadbeef\"]]},"
                                + "\"nextResultUri\":\"/v1/sessions/s1/operations/op1/result/1?rowFormat=table\"}");
                    } else {
                        respond(ex, 200, "{\"resultType\":\"EOS\"}");
                    }
                    return;
                }
                if (path.equals("/v1/jobs/abc/exceptions")) {
                    respond(ex, 200, "{\"exceptionHistory\":{\"entries\":["
                            + "{\"root\":\"java.lang.RuntimeException: boom\",\"taskName\":\"A -> B\"},"
                            + "{\"root\":\"java.lang.ArithmeticException: / by zero\"}]}}");
                    return;
                }
                if (path.equals("/v1/taskmanagers/tm1/log")) {
                    respond(ex, 200, "{\"out\":[{\"line\":\"first tm line\"},{\"line\":\"second tm line\"}]}");
                    return;
                }
                respond(ex, 404, "{\"errors\":[\"unknown path " + path + "\"]}");
            }
        });
        server.start();
    }

    @After
    public void stopStub() {
        if (server != null) {
            server.stop(0);
        }
    }

    private String base() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    private static void respond(HttpExchange ex, int code, String body) throws IOException {
        byte[] payload = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().add("Content-Type", "application/json");
        ex.sendResponseHeaders(code, payload.length);
        OutputStream out = ex.getResponseBody();
        try {
            out.write(payload);
        } finally {
            out.close();
        }
    }

    @Test
    public void readsEveryLineOfTheOperationPayload() throws Exception {
        SqlGatewayClient client = new SqlGatewayClient(base() + "?pollIntervalMs=50");
        OperationLog log = client.fetchOperationLog("s1", "op1");
        assertEquals(1, log.getLines().size());
        assertEquals("d1a0f0e0deadbeefd1a0f0e0deadbeef", log.getLines().get(0));
        // NOT_READY 会等待并用同一个 token 重试，因此会发出不止一次调用。
        assertTrue(resultRequests.get() > 1);
    }

    @Test
    public void firstLineIsTheErrorText() throws Exception {
        SqlGatewayClient client = new SqlGatewayClient(base() + "?pollIntervalMs=50");
        assertEquals("d1a0f0e0deadbeefd1a0f0e0deadbeef", client.fetchOperationError("s1", "op1"));
    }

    @Test
    public void readsJobExceptions() throws Exception {
        JobExceptions exceptions = SqlGatewayClient.fetchJobExceptions(base(), "abc");
        assertEquals(2, exceptions.getEntries().size());
        assertEquals("java.lang.RuntimeException: boom", exceptions.getRoots().get(0));
        assertEquals("A -> B", exceptions.getEntries().get(0).getTaskName());
        assertEquals("java.lang.ArithmeticException: / by zero", exceptions.getRoots().get(1));
    }

    @Test
    public void readsTaskManagerLogLines() throws Exception {
        ClusterLog log = SqlGatewayClient.fetchTaskManagerLog(base(), "tm1");
        assertEquals(2, log.getLines().size());
        assertEquals("first tm line", log.getLines().get(0));
        assertEquals("second tm line", log.getLines().get(1));
    }
}
