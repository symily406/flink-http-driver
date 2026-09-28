package com.easy.etl.flink.http;

import com.easy.etl.flink.http.em.JobState;
import com.easy.etl.flink.http.gateway.SqlGatewayClient;
import com.easy.etl.flink.http.model.JobStatus;
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
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** 针对桩集群（stub dispatcher）的作业运行状态查询测试。 */
public class JobStatusTest {

    private HttpServer server;
    private final List<String> seen = new ArrayList<String>();
    /** 前 n 次 {@code /jobs/job-running} 返回 RUNNING，之后返回 FINISHED。 */
    private volatile int runningPolls = 0;

    @Before
    public void startStub() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", new HttpHandler() {
            @Override
            public void handle(HttpExchange ex) throws IOException {
                String path = ex.getRequestURI().getPath();
                seen.add(ex.getRequestMethod() + " " + path);
                if (path.equals("/jobs/job-running")) {
                    boolean stillRunning = runningPolls-- > 0;
                    respond(ex, 200, stillRunning
                            ? "{\"jid\":\"job-running\",\"name\":\"sync\",\"state\":\"RUNNING\","
                              + "\"start-time\":1700000000000,\"end-time\":-1,\"duration\":12345}"
                            : "{\"jid\":\"job-running\",\"name\":\"sync\",\"state\":\"FINISHED\","
                              + "\"start-time\":1700000000000,\"end-time\":1700000060000,"
                              + "\"duration\":60000}");
                } else if (path.equals("/jobs/job-always-running")) {
                    respond(ex, 200, "{\"jid\":\"job-always-running\",\"name\":\"stream-sync\","
                            + "\"state\":\"RUNNING\",\"start-time\":1700000000000,"
                            + "\"end-time\":-1,\"duration\":12345}");
                } else if (path.equals("/jobs/job-failed")) {
                    respond(ex, 200, "{\"jid\":\"job-failed\",\"name\":\"nightly\","
                            + "\"state\":\"FAILED\",\"start-time\":1700000000000,"
                            + "\"end-time\":1700000030000,\"duration\":30000}");
                } else if (path.equals("/jobs/overview")) {
                    respond(ex, 200, "{\"jobs\":["
                            + "{\"jid\":\"job-running\",\"name\":\"sync\",\"state\":\"RUNNING\","
                            + "\"start-time\":1700000000000,\"end-time\":-1,\"duration\":12345},"
                            + "{\"jid\":\"job-canceled\",\"name\":\"old\",\"state\":\"CANCELED\","
                            + "\"start-time\":1700000000000,\"end-time\":1700000020000,"
                            + "\"duration\":20000}]}");
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
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @Test
    public void fetchJobStatusReadsClusterEndpointWithoutV1Prefix() throws IOException {
        JobStatus failed = SqlGatewayClient.fetchJobStatus(url(), "job-failed");
        assertEquals("GET /jobs/job-failed", seen.get(0));
        assertEquals("job-failed", failed.getJobId());
        assertEquals("nightly", failed.getName());
        assertEquals("FAILED", failed.getState());
        assertTrue(failed.isFailed());
        assertTrue(failed.isTerminal());
        assertFalse(failed.isActive());
        assertEquals(1700000000000L, failed.getStartTime());
        assertEquals(1700000030000L, failed.getEndTime());
        assertEquals(30000L, failed.getDuration());
    }

    @Test
    public void runningJobIsActive() throws IOException {
        JobStatus running = SqlGatewayClient.fetchJobStatus(url(), "job-always-running");
        assertTrue(running.isRunning());
        assertTrue(running.isActive());
        assertFalse(running.isTerminal());
        assertEquals(-1L, running.getEndTime());
        assertTrue(SqlGatewayClient.isJobRunning(url(), "job-always-running"));
    }

    @Test
    public void fetchJobStatusesReadsOverview() throws IOException {
        List<JobStatus> jobs = SqlGatewayClient.fetchJobStatuses(url());
        assertEquals("GET /jobs/overview", seen.get(0));
        assertEquals(2, jobs.size());
        assertTrue(jobs.get(0).isRunning());
        assertTrue(jobs.get(1).isCanceled());
        assertTrue(jobs.get(1).isTerminal());
    }

    @Test
    public void stateMapsToCodedEnum() throws IOException {
        assertEquals(JobState.FAILED, SqlGatewayClient.fetchJobState(url(), "job-failed"));
        assertEquals(-1, SqlGatewayClient.fetchJobStatus(url(), "job-failed").getStateCode());
        assertTrue(JobState.fromState("FAILED").isFailure());
        assertTrue(JobState.FAILED.isTerminal());
        assertTrue(JobState.FAILED.isSettled());

        assertEquals(JobState.RUNNING, SqlGatewayClient.fetchJobState(url(), "job-always-running"));
        assertEquals(3, SqlGatewayClient.fetchJobStatus(url(), "job-always-running").getStateCode());
        assertTrue(JobState.RUNNING.isActive());
        assertFalse(JobState.RUNNING.isTerminal());

        assertEquals(JobState.FINISHED, JobState.fromState("finished"));
        assertEquals(7, JobState.FINISHED.getCode());
        assertEquals("已完成", JobState.FINISHED.getDescription());
        assertEquals("CANCELED", JobState.CANCELED.getState());

        // 编码与枚举一一对应
        assertEquals(1, JobState.INITIALIZING.getCode());
        assertEquals(2, JobState.CREATED.getCode());
        assertEquals(3, JobState.RUNNING.getCode());
        assertEquals(4, JobState.FAILING.getCode());
        assertEquals(5, JobState.CANCELLING.getCode());
        assertEquals(6, JobState.CANCELED.getCode());
        assertEquals(7, JobState.FINISHED.getCode());
        assertEquals(8, JobState.RESTARTING.getCode());
        assertEquals(9, JobState.SUSPENDED.getCode());
        assertEquals(10, JobState.RECONCILING.getCode());
        assertEquals(-10, JobState.UNKNOWN.getCode());

        // SUSPENDED：已停下（terminal）但还能恢复，不是最终结果（settled）
        assertTrue(JobState.SUSPENDED.isTerminal());
        assertFalse(JobState.SUSPENDED.isSettled());
        assertFalse(JobState.SUSPENDED.isActive());

        assertEquals(JobState.CANCELED, JobState.fromState("CANCELLED")); // 英式拼写
        assertEquals(JobState.RUNNING, JobState.fromState("running"));    // 大小写不敏感
        assertEquals(JobState.UNKNOWN, JobState.fromState("SOMETHING_NEW"));
        assertEquals(JobState.UNKNOWN, JobState.fromState(null));
        assertEquals(JobState.FAILED, JobState.fromCode(-1));
        assertEquals(JobState.SUSPENDED, JobState.fromCode(9));
        assertEquals(JobState.UNKNOWN, JobState.fromCode(99));
    }

    @Test
    public void waitForJobPollsUntilTerminal() throws IOException, InterruptedException {
        runningPolls = 1; // 第一次 RUNNING，第二次 FINISHED
        JobStatus done = SqlGatewayClient.waitForJob(url(), "job-running", 50L, 5000L);
        assertTrue(done.isFinished());
        assertEquals(60000L, done.getDuration());
        assertEquals(2, seen.size());
    }
}
