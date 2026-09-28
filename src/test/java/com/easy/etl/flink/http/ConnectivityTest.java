package com.easy.etl.flink.http;

import com.easy.etl.flink.http.gateway.SqlGatewayClient;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** 针对桩网关（stub gateway）的连通性探测测试。 */
public class ConnectivityTest {

    private HttpServer server;
    private final List<String> seen = new ArrayList<String>();
    private volatile boolean infoEnabled = true;
    private volatile int unauthorized = 0;

    @Before
    public void startStub() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", new HttpHandler() {
            @Override
            public void handle(HttpExchange ex) throws IOException {
                String key = ex.getRequestMethod() + " " + ex.getRequestURI().getPath();
                seen.add(key);
                if (unauthorized > 0) {
                    unauthorized--;
                    respond(ex, 401, "{\"errors\":[\"Unauthorized\"]}");
                    return;
                }
                if (infoEnabled && key.equals("GET /v1/info")) {
                    respond(ex, 200, "{\"productName\":\"Apache Flink\",\"version\":\"1.18.0\"}");
                } else if (key.startsWith("POST /v1/sessions")) {
                    respond(ex, 200, "{\"sessionHandle\":\"stub-session\"}");
                } else if (key.startsWith("DELETE /v1/sessions")) {
                    ex.sendResponseHeaders(204, -1);
                    ex.close();
                } else {
                    respond(ex, 404, "{\"errors\":[\"Not found\"]}");
                }
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

    private String url() {
        return "http://127.0.0.1:" + server.getAddress().getPort() + "?user=admin&password=secret";
    }

    @Test
    public void pingPrefersInfoEndpoint() {
        SqlGatewayClient client = new SqlGatewayClient(url());
        assertTrue(client.ping(3000));
        assertEquals("GET /v1/info", seen.get(0));
    }

    @Test
    public void pingFallsBackToSessionAndReleasesIt() {
        infoEnabled = false;
        SqlGatewayClient client = new SqlGatewayClient(url());
        assertTrue(client.ping(3000));
        assertTrue(seen.contains("POST /v1/sessions"));
        assertTrue(seen.contains("DELETE /v1/sessions/stub-session"));
    }

    @Test
    public void pingIsFalseWhenCredentialsAreRejected() {
        // 两个探测都被以 401 响应 -> 对该用户来说什么都不可达。
        unauthorized = 4;
        SqlGatewayClient client = new SqlGatewayClient(url());
        assertFalse(client.ping(3000));
    }

    @Test
    public void checkReachableExplainsTheFailure() throws IOException {
        ServerSocket probe = new ServerSocket(0);
        int port = probe.getLocalPort();
        probe.close(); // 之后没有再绑定；我们只是想要一个空闲端口
        SqlGatewayClient client = new SqlGatewayClient("http://127.0.0.1:" + port);
        assertFalse(client.ping(3000));
        try {
            client.checkReachable(3000);
            fail("expected an IOException for an unreachable gateway");
        } catch (IOException e) {
            // 消息里必须带上网关地址与实际失败原因（实现用的是中文文案）
            assertTrue(e.getMessage(),
                    e.getMessage().contains("无法访问位于 http://127.0.0.1:" + port)
                            && e.getMessage().contains("Flink SQL Gateway"));
        }
    }

    @Test
    public void staticTestConnectionAcceptsAddressWithAndWithoutCredentials() {
        int port = server.getAddress().getPort();
        assertTrue(SqlGatewayClient.testConnection("http://127.0.0.1:" + port, 3000));
        assertTrue(SqlGatewayClient.testConnection(
                "jdbc:flink:http://127.0.0.1:" + port, 3000));
        assertTrue(SqlGatewayClient.testConnection(
                "http://127.0.0.1:" + port + "?user=admin&password=secret", 3000));
    }
}
