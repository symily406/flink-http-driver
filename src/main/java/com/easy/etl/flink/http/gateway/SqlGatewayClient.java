package com.easy.etl.flink.http.gateway;

import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpResponse;
import cn.hutool.json.JSON;
import cn.hutool.json.JSONUtil;
import com.easy.etl.flink.http.GatewayConfig;
import com.easy.etl.flink.http.model.ClusterLog;
import com.easy.etl.flink.http.model.ExecuteStatementRequest;
import com.easy.etl.flink.http.model.GatewayInfo;
import com.easy.etl.flink.http.model.GatewayResult;
import com.easy.etl.flink.http.em.JobState;
import com.easy.etl.flink.http.model.JobExceptions;
import com.easy.etl.flink.http.model.JobStatus;
import com.easy.etl.flink.http.model.JsonBody;
import com.easy.etl.flink.http.model.OpenSessionRequest;
import com.easy.etl.flink.http.model.OperationInfo;
import com.easy.etl.flink.http.model.OperationLog;
import com.easy.etl.flink.http.model.OperationStatus;
import com.easy.etl.flink.http.model.ResultPage;
import com.easy.etl.flink.http.model.ResultType;
import com.easy.etl.flink.http.model.Row;
import com.easy.etl.flink.http.model.SessionInfo;
import com.easy.etl.flink.http.model.StopResult;
import com.easy.etl.flink.http.util.Json;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Flink SQL Gateway HTTP API (v1) 的 REST 客户端。每个响应都会被映射为
 * {@code com.easy.etl.flink.http.model} 下的模型；唯一会直接暴露原始 JSON
 * 的地方是刻意保留的逃生通道 {@link #getJson(String)}。
 *
 * <p>这里使用的 v1 端点在 Flink 1.18 到 2.x 之间是稳定的：
 * <pre>
 *   GET    /v1/info                                                 获取产品版本
 *   POST   /v1/sessions                                             打开会话
 *   DELETE /v1/sessions/{sessionHandle}                             关闭会话
 *   POST   /v1/sessions/{sessionHandle}/heartbeat                   心跳保活
 *   POST   /v1/sessions/{sessionHandle}/statements                  执行语句
 *   GET    /v1/sessions/{sessionHandle}/operations/{op}/status      查询操作状态
 *   DELETE /v1/sessions/{sessionHandle}/operations/{op}/close       关闭操作（释放结果）
 *   POST   /v1/sessions/{sessionHandle}/operations/{op}/cancel      取消操作
 *   GET    /v1/sessions/{sessionHandle}/operations/{op}/result/{token}?rowFormat=PLAIN_TEXT|JSON
 * </pre>
 */
public class SqlGatewayClient {

    private static final Logger LOG = Logger.getLogger(SqlGatewayClient.class.getName());

    /**
     * JDK 8 的 {@link HttpURLConnection} 唯一不支持的标准方法。取消作业必须用它，
     * 详见 {@link #httpPatch}。
     */
    private static final String METHOD_PATCH = "PATCH";

    private final GatewayConfig config;
    private final String authHeader;

    public SqlGatewayClient(GatewayConfig config) {
        this.config = config;
        this.authHeader = config.getBasicAuthHeader();
    }

    /**
     * @param address 网关地址，例如
     *                {@code http://host:8083?user=admin&password=secret&rowFormat=json}
     */
    public SqlGatewayClient(String address) {
        this(GatewayConfig.parse(address));
    }

    public GatewayConfig getConfig() {
        return config;
    }

    // ------------------------------------------------------------------
    // 连通性检测
    // ------------------------------------------------------------------

    /**
     * 连通性检测：网关能响应时返回 {@code true}，否则返回 {@code false}。
     * 该方法从不抛异常；需要知道具体失败原因时请使用 {@link #checkReachable(int)}。
     *
     * <p>探测方式：先尝试 {@code GET /v1/info}（成本最低，在服务端不产生任何资源），
     * 对于未暴露 {@code /v1/info} 的网关或代理，会回退到 {@code POST /v1/sessions}。
     * 每次探测都会携带认证信息，因此被拒绝的 {@code Authorization} 请求头会被
     * 判定为不可达。
     *
     * @param readTimeoutMs 探测超时时间；{@code <= 0} 时使用配置的读取超时时间
     */
    public boolean ping(int readTimeoutMs) {
        return unreachableReason(readTimeoutMs) == null;
    }

    /** 使用配置的读取超时时间执行 {@link #ping}。 */
    public boolean ping() {
        return ping(0);
    }

    /**
     * 与 {@link #ping} 相同的检测，但会抛出异常说明网关为何无法访问
     * （主机/端口错误、TLS 失败、认证被拒绝等）。
     *
     * @throws IOException 当探测失败时抛出
     */
    public void checkReachable(int readTimeoutMs) throws IOException {
        IOException failure = unreachableReason(readTimeoutMs);
        if (failure != null) {
            throw failure;
        }
    }

    /** 使用配置的读取超时时间执行 {@link #checkReachable(int)}。 */
    public void checkReachable() throws IOException {
        checkReachable(0);
    }

    /** {@link #ping} 的静态便捷方法：探测网关后释放所有资源。 */
    public static boolean testConnection(String address, int readTimeoutMs) {
        return new SqlGatewayClient(address).ping(readTimeoutMs);
    }

    /** 使用默认读取超时时间（60 秒）执行 {@link #testConnection(String, int)}。 */
    public static boolean testConnection(String address) {
        return testConnection(address, 0);
    }

    /** 可达时返回 {@code null}，否则返回探测失败的原因。 */
    private IOException unreachableReason(int readTimeoutMs) {
        IOException infoFailure;
        try {
            request("GET", "/info", null, readTimeoutMs);
            return null;
        } catch (IOException e) {
            infoFailure = e; // 某些部署未暴露 /v1/info，改尝试创建会话
        }

        String session = null;
        try {
            session = openSession(readTimeoutMs).getSessionHandle();
            return null;
        } catch (IOException e) {
            return new IOException("无法访问位于 " + config.getEndpoint()
                    + " 的 Flink SQL Gateway：" + e.getMessage() + "（GET /v1/info 同样失败："
                    + infoFailure.getMessage() + "）", e);
        } finally {
            if (session != null) {
                try {
                    request("DELETE", "/sessions/" + session, null, readTimeoutMs);
                } catch (IOException ignore) {
                    // 探测已经成功，这里只是尽力清理
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // 网关 / 集群信息
    // ------------------------------------------------------------------

    /**
     * 通过 {@code GET /v1/info} 获取产品名称与版本。
     *
     * @return 解析后的模型；当该端点不可用时返回 {@code null}
     */
    public GatewayInfo getInfo() {
        try {
            return GatewayInfo.fromJson(request("GET", "/info", null));
        } catch (IOException e) {
            LOG.log(Level.FINE, "GET /info 失败；该网关未暴露此端点", e);
            return null;
        }
    }

    /**
     * 对当前客户端 base url 之后的任意 v1 路径进行原始 {@code GET} 请求——这是
     * 访问本客户端未建模端点的逃生通道。将该客户端指向集群 REST 地址
     * （{@code http://host:8081}）即可访问 JobManager 端点；{@code /v1/} 会被自动
     * 补齐，因此传入 {@code /jobs/overview} 即可。
     */
    public Map<String, Object> getJson(String path) throws IOException {
        return request("GET", path.startsWith("/") ? path : "/" + path, null);
    }

    /**
     * 对当前客户端 base url 之后的任意 v1 路径进行原始 {@code GET}，并返回
     * **未经解析的响应体文本**。用于返回纯文本而非 JSON 的端点——例如集群的
     * {@code /jobmanager/log}、{@code /taskmanagers/{id}/log} 都是
     * {@code text/plain}，直接 {@code getJson()} 会解析失败。
     *
     * <p>与 {@link #getJson(String)} 一样会补上 {@code /v1} 前缀，并且支持在
     * path 里直接带查询串，例如
     * {@code fetchClusterLog(url, "/jobmanager/log?size=500")}。
     */
    public String getText(String path) throws IOException {
        String p = path.startsWith("/") ? path : "/" + path;
        return http(config.getBaseUrl() + p, "GET", p, null, 0);
    }

    /**
     * 读取 Flink 作业的异常历史，来自**集群** REST 端点
     * （{@code http://host:8081}，而非 SQL Gateway 端口）。当一个 INSERT 语句已被
     * 网关报告为 FINISHED、但之后真正执行时才失败时，真正的根因就在这里。
     *
     * @param dispatcherUrl {@code http(s)://host:8081[?user=..&password=..]}
     */
    public static JobExceptions fetchJobExceptions(String dispatcherUrl, String jobId)
            throws IOException {
        Map<String, Object> resp = new SqlGatewayClient(dispatcherUrl)
                .getJson("/jobs/" + encode(jobId) + "/exceptions");
        return JobExceptions.fromJson(resp);
    }

    /**
     * 从任何日志端点读取日志行。既支持集群直接返回 {@code text/plain} 原始日志
     * （Flink 2.x 的 {@code /jobmanager/log}、{@code /taskmanagers/{id}/log}），
     * 也兼容老版本返回 {@code {"out":[...]}} JSON 的形态。
     *
     * <p><b>注意</b>：日志端点虽然接受 {@code ?size=N}，但 Flink 2.2.1 实测会忽略它
     * （带与不带该参数返回的行数完全一致），因此一次调用会把**整份日志**读进内存
     * （实测 JobManager 日志上万个行）。日志文件很大时请传完再截断：
     * {@code fetchJobManagerLog(url).tail(500)}。
     *
     * @param dispatcherUrl {@code http(s)://host:8081[?user=..&password=..]}
     * @param logPath       {@code /jobmanager/log} 或 {@code /taskmanagers/{id}/log}
     */
    public static ClusterLog fetchClusterLog(String dispatcherUrl, String logPath)
            throws IOException {
        return ClusterLog.parse(new SqlGatewayClient(dispatcherUrl).getText(logPath));
    }

    /** 读取 {@code dispatcherUrl} 对应集群的 {@code GET /jobmanager/log}。 */
    public static ClusterLog fetchJobManagerLog(String dispatcherUrl) throws IOException {
        return fetchClusterLog(dispatcherUrl, "/jobmanager/log");
    }

    /** 读取对应 TaskManager 的 {@code GET /taskmanagers/{taskManagerId}/log}。 */
    public static ClusterLog fetchTaskManagerLog(String dispatcherUrl, String taskManagerId)
            throws IOException {
        return fetchClusterLog(dispatcherUrl, "/taskmanagers/" + encode(taskManagerId) + "/log");
    }

    /**
     * 给定作业 ID，一次性拉取该作业涉及的所有 TaskManager 的日志，映射为
     * {@code tmId -> ClusterLog}。
     *
     * <p>实现分三步，全部走集群 REST（{@code http://host:8081}，不是网关端口）：
     * <ol>
     *   <li>{@code GET /jobs/{jobId}} —— 取作业的顶点（vertices）列表；</li>
     *   <li>对每个顶点 {@code GET /jobs/{jobId}/vertices/{vertexId}} —— 取
     *       {@code subtasks[].host}（即 TaskManager 的资源 ID，Flink 2.x 里
     *       该字段与 {@code taskmanager-id} 等价）；</li>
     *   <li>逐个 {@code GET /taskmanagers/{tmId}/log} 拉日志。</li>
     * </ol>
     * 一个作业通常跑在多个 TM 上，结果按键出现的先后顺序去重。
     *
     * <p>与 {@link #fetchTaskManagerLog} 一样，日志端点会整份读入内存
     * （Flink 2.x 的 {@code ?size=} 被忽略），节点日志很大时请对返回的每个
     * {@link ClusterLog} 调用 {@code tail(n)} 截断。
     *
     * @param dispatcherUrl 集群 REST 地址，例如 {@code http://host:8081}（非网关端口）
     * @param jobId         作业 ID
     * @return tmId 到其完整日志的映射；作业尚未在任何 TM 上运行（如刚创建）时为空映射
     * @throws IOException 网络 / HTTP 层失败，或作业不存在（已被集群清理时返回 404）
     */
    public static Map<String, ClusterLog> fetchTaskManagerLogsForJob(String dispatcherUrl,
                                                                     String jobId)
            throws IOException {
        SqlGatewayClient c = new SqlGatewayClient(dispatcherUrl);

        // 1. 作业顶点
        Map<String, Object> job = c.jobManager("GET", "/jobs/" + encode(jobId), 0);
        Object vertices = Json.get(job, "vertices");
        if (!(vertices instanceof List)) {
            return Collections.emptyMap();
        }

        // 2. 收集所有出现过的 tmId（保持顺序、去重）
        LinkedHashMap<String, ClusterLog> logs = new LinkedHashMap<String, ClusterLog>();
        for (Object v : (List<?>) vertices) {
            if (!(v instanceof Map)) {
                continue;
            }
            Map<String, Object> vertex = (Map<String, Object>) v;
            String vId = Json.getStr(vertex, "id");
            if (vId == null || vId.isEmpty()) {
                continue;
            }

            // 3. 该顶点的 subtasks，host / taskmanager-id 即 TM 资源 ID
            Map<String, Object> detail = c.jobManager("GET",
                    "/jobs/" + encode(jobId) + "/vertices/" + encode(vId), 0);
            Object subtasks = Json.get(detail, "subtasks");
            if (!(subtasks instanceof List)) {
                continue;
            }
            for (Object s : (List<?>) subtasks) {
                if (!(s instanceof Map)) {
                    continue;
                }
                String tmId = taskManagerIdOf((Map<String, Object>) s);
                if (tmId == null || tmId.isEmpty() || logs.containsKey(tmId)) {
                    continue;
                }
                logs.put(tmId, fetchTaskManagerLog(dispatcherUrl, tmId));
            }
        }
        return logs;
    }

    /**
     * 从 subtask 节点里取出 TaskManager 的资源 ID：优先用 {@code host}
     * （Flink 2.x 的 {@code SubtaskExecutionAttemptDetailsInfo#host} 即资源 ID），
     * 缺失时回退到已废弃的 {@code taskmanager-id}。
     */
    private static String taskManagerIdOf(Map<String, Object> subtask) {
        String host = Json.getStr(subtask, "host");
        if (host != null && !host.isEmpty()) {
            return host;
        }
        return Json.getStr(subtask, "taskmanager-id");
    }

    // ------------------------------------------------------------------
    // 作业运行状态（集群 REST）
    // ------------------------------------------------------------------

    /**
     * 查询一个作业的**运行状态**：{@code GET /jobs/{jobId}}。
     *
     * <p>operation 状态只说明"语句有没有被网关接受"——流式 INSERT 提交后
     * operation 很快就是 FINISHED，作业却还在后台跑，后来的失败也回不到
     * operation 上。要看作业是 RUNNING / FINISHED / FAILED / CANCELED，就得查这里。
     *
     * @param dispatcherUrl 集群 REST 地址，例如 {@code http://host:8081}（非网关端口）
     * @param jobId         作业 ID，取自 {@code GatewayResult#getJobId()}
     * @throws IOException 网络 / HTTP 层失败，或作业不存在（已被集群清理时返回 404）
     */
    public static JobStatus fetchJobStatus(String dispatcherUrl, String jobId)
            throws IOException {
        Map<String, Object> resp = new SqlGatewayClient(dispatcherUrl)
                .jobManager("GET", "/jobs/" + encode(jobId), 0);
        return JobStatus.fromJson(resp);
    }

    /**
     * 集群上所有作业的状态快照：{@code GET /jobs/overview}。
     *
     * @return 作业状态列表；集群上没有作业时为空列表
     */
    public static List<JobStatus> fetchJobStatuses(String dispatcherUrl) throws IOException {
        Map<String, Object> resp = new SqlGatewayClient(dispatcherUrl)
                .jobManager("GET", "/jobs/overview", 0);
        List<JobStatus> out = new ArrayList<JobStatus>();
        Object jobs = Json.get(resp, "jobs");
        if (jobs instanceof List) {
            for (Object job : (List<?>) jobs) {
                if (job instanceof Map) {
                    out.add(JobStatus.fromJson((Map<String, Object>) job));
                }
            }
        }
        return out;
    }

    /**
     * 只取作业的**状态编码**（{@code GET /jobs/{jobId}} 后映射为 {@link JobState}）：
     * -1 已失败，1 初始化中，2 已创建，3 运行中，4 失败处理中，5 取消中，6 已取消，
     * 7 已完成，8 重启中，9 已挂起，10 协调中，-10 未知。
     *
     * <p>需要作业名、时间戳等更多信息时用 {@link #fetchJobStatus}。
     */
    public static JobState fetchJobState(String dispatcherUrl, String jobId) throws IOException {
        return fetchJobStatus(dispatcherUrl, jobId).getJobState();
    }

    /**
     * 作业是否还没结束——即处于 CREATED / RUNNING / RESTARTING / CANCELLING 等
     * 任何非终态。
     */
    public static boolean isJobRunning(String dispatcherUrl, String jobId) throws IOException {
        return fetchJobStatus(dispatcherUrl, jobId).isActive();
    }

    /**
     * 轮询作业状态直到它进入终态（FINISHED / FAILED / CANCELED）。
     *
     * @param dispatcherUrl  集群 REST 地址，例如 {@code http://host:8081}
     * @param jobId          作业 ID
     * @param pollIntervalMs 轮询间隔；{@code <=0} 使用 1000ms
     * @param timeoutMs      最大等待时长；{@code <=0} 表示不限时
     * @return 终态时的状态快照
     * @throws GatewayExecutionException 超过 {@code timeoutMs} 仍未结束
     */
    public static JobStatus waitForJob(String dispatcherUrl, String jobId, long pollIntervalMs,
                                       long timeoutMs)
            throws IOException, InterruptedException {
        long interval = pollIntervalMs > 0 ? pollIntervalMs : 1000L;
        long start = System.currentTimeMillis();
        while (true) {
            JobStatus status = fetchJobStatus(dispatcherUrl, jobId);
            if (status.isTerminal()) {
                return status;
            }
            if (timeoutMs > 0 && System.currentTimeMillis() - start > timeoutMs) {
                throw new GatewayExecutionException("等待作业 " + jobId + " 结束超时："
                        + timeoutMs + " ms（当前状态：" + status.getState() + "）。");
            }
            Thread.sleep(interval);
        }
    }

    /** 不限时地等待作业结束，调用 {@link #waitForJob(String, String, long, long)}。 */
    public static JobStatus waitForJob(String dispatcherUrl, String jobId)
            throws IOException, InterruptedException {
        return waitForJob(dispatcherUrl, jobId, 0L, 0L);
    }

    // ------------------------------------------------------------------
    // 停止流任务 / checkpoint
    // ------------------------------------------------------------------

    /**
     * 触发停止一个仍在运行的流任务（stop）。停止（stop）与取消（cancel）不同：
     * cancel 直接丢弃任务，而 stop 会先触发一次 savepoint / checkpoint，
     * 从而留下可恢复的停顿点，之后可用返回的 checkpoint 位置从中恢复。
     *
     * @param dispatcherUrl 集群 REST 地址，例如 {@code http://host:8081}（非网关端口）
     * @param jobId         被停止的作业 ID
     * @param withDrain     是否带 drain 停止（等待已缓冲数据全部写出后停机）
     * @return 触发的 request-id，用于随后轮询结果
     */
    public static String triggerStop(String dispatcherUrl, String jobId, boolean withDrain)
            throws IOException {
        Map<String, Object> resp = new SqlGatewayClient(dispatcherUrl).jobManager("POST",
                "/jobs/" + encode(jobId) + "/stop" + (withDrain ? "?mode=drain" : ""), 0);
        return requireStopId(resp);
    }

    /**
     * 查询一次 stop 触发的结果，得到生成的 checkpoint / savepoint 位置。
     *
     * @param requestId 由 {@link #triggerStop} 返回的 request-id
     */
    public static StopResult getStopResult(String dispatcherUrl, String jobId, String requestId)
            throws IOException {
        Map<String, Object> resp = new SqlGatewayClient(dispatcherUrl).jobManager("GET",
                "/jobs/" + encode(jobId) + "/savepoints/" + encode(requestId), 0);
        return StopResult.fromJson(jobId, requestId, resp);
    }

    /**
     * 停止一个流任务（便捷版）：只传集群地址与作业 ID 即可。
     * 等价于 {@link #stopJob(String, String, boolean, long, long)} 且
     * {@code withDrain=false}、轮询间隔与超时均取默认值（interval 500ms、超时不限时）。
     *
     * @param dispatcherUrl 集群 REST 地址，例如 {@code http://host:8081}
     * @param jobId         被停止的作业 ID
     * @return 停止结果；{@code isCompleted()} 为 true 时，{@code getLocation()}
     *         即为可恢复的 checkpoint 路径
     */
    public static StopResult stopJob(String dispatcherUrl, String jobId)
            throws IOException, InterruptedException {
        return stopJob(dispatcherUrl, jobId, false, 0L, 0L);
    }

    /**
     * 停止一个流任务并轮询直至完成，返回包含 checkpoint 位置的 {@link StopResult}。
     *
     * @param dispatcherUrl      集群 REST 地址，例如 {@code http://host:8081}
     * @param jobId              被停止的作业 ID
     * @param withDrain          是否带 drain 停止
     * @param pollIntervalMs     轮询间隔；{@code <=0} 使用默认值 500ms
     * @param operationTimeoutMs 最大等待时长；{@code <=0} 表示不限时
     * @return 停止结果；{@code isCompleted()} 为 true 时，{@code getLocation()}
     *         即为可恢复的 checkpoint 路径
     */
    public static StopResult stopJob(String dispatcherUrl, String jobId, boolean withDrain,
                                     long pollIntervalMs, long operationTimeoutMs)
            throws IOException, InterruptedException {
        String requestId = triggerStop(dispatcherUrl, jobId, withDrain);
        long interval = pollIntervalMs > 0 ? pollIntervalMs : 500L;
        long start = System.currentTimeMillis();
        while (true) {
            StopResult result = getStopResult(dispatcherUrl, jobId, requestId);
            if (result.isCompleted() || result.isFailed()) {
                return result;
            }
            if (operationTimeoutMs > 0 && System.currentTimeMillis() - start > operationTimeoutMs) {
                throw new GatewayExecutionException("等待停止作业超时：" + operationTimeoutMs + " ms。");
            }
            Thread.sleep(interval);
        }
    }

    /**
     * 停止一个**批任务**（cancel）。
     *
     * <p>批任务跑完会自动结束，因此"停止"直接对应取消：走集群 REST
     * {@code PATCH /jobs/{jobId}}（Flink 默认取消作业的动作），不执行 stop 那套
     * "先触发 checkpoint 再停机"的流程，也不产生可恢复点。仅对处于
     * {@code RUNNING} 状态的作业生效；对已 {@code FINISHED}/{@code FAILED}
     * 的作业调用会返回 409。
     *
     * <p>Flink 2.x 只认 {@code PATCH}（{@code POST .../cancel} 与 {@code DELETE}
     * 都是 404），而 JDK 8 的 {@link HttpURLConnection} 不支持 PATCH，所以这一路
     * 请求由 {@link #httpPatch} 用 hutool 发出——对调用方完全透明。
     *
     * @param dispatcherUrl 集群 REST 地址，例如 {@code http://host:8081}
     * @param jobId         被取消的批作业 ID
     * @throws IOException 网络 / HTTP 层失败，或作业无法取消
     */
    public static void cancelJob(String dispatcherUrl, String jobId) throws IOException {
        new SqlGatewayClient(dispatcherUrl).jobManager("PATCH",
                "/jobs/" + encode(jobId), 0);
    }

    /** 从 stop 触发响应中解析 request-id，缺失则抛出异常。 */
    private static String requireStopId(Map<String, Object> resp) throws IOException {
        String id = Json.getStr(resp, "request-id");
        if (id == null || id.isEmpty()) {
            throw new IOException("stop 触发未返回 request-id：" + resp);
        }
        return id;
    }

    // ------------------------------------------------------------------
    // 会话生命周期
    // ------------------------------------------------------------------

    /** {@code POST /v1/sessions}：使用配置的属性打开一个会话。 */
    public SessionInfo openSession() throws IOException {
        return openSession(0);
    }

    /**
     * 与 {@link #openSession()} 相同，但受调用方指定的套接字读取超时时间约束。
     *
     * @param readTimeoutMs 仅本次调用的超时时间；{@code <= 0} 时使用配置的默认值
     */
    public SessionInfo openSession(int readTimeoutMs) throws IOException {
        Map<String, Object> resp = request("POST", "/sessions",
                new OpenSessionRequest(config.getSessionProperties()), readTimeoutMs);
        SessionInfo session = SessionInfo.fromJson(resp);
        if (session.getSessionHandle() == null || session.getSessionHandle().isEmpty()) {
            throw new IOException("网关未返回 sessionHandle： " + resp);
        }
        return session;
    }

    public void closeSession(String sessionHandle) throws IOException {
        try {
            request("DELETE", "/sessions/" + sessionHandle, null);
        } catch (IOException e) {
            // 关闭一个已经不存在的会话不应让调用方失败
        }
    }

    /** 保持空闲会话存活；对于长期会话需要周期性调用它。 */
    public void heartbeat(String sessionHandle) throws IOException {
        request("POST", "/sessions/" + sessionHandle + "/heartbeat", null);
    }

    /** 受调用方指定的套接字读取超时时间约束的 {@link #heartbeat}。 */
    public void heartbeat(String sessionHandle, int readTimeoutMs) throws IOException {
        request("POST", "/sessions/" + sessionHandle + "/heartbeat", null, readTimeoutMs);
    }

    // ------------------------------------------------------------------
    // 操作（Operation）
    // ------------------------------------------------------------------

    /**
     * 提交一条语句。网关每次调用只接受一条语句——发送前请先拆分脚本。
     */
    public OperationInfo executeStatement(String sessionHandle, String statement)
            throws IOException {
        Map<String, Object> resp = request("POST",
                "/sessions/" + sessionHandle + "/statements",
                new ExecuteStatementRequest(statement));
        OperationInfo op = OperationInfo.fromJson(resp);
        if (op.getOperationHandle() == null || op.getOperationHandle().isEmpty()) {
            throw new IOException("网关未返回 operationHandle： " + resp);
        }
        return op;
    }

    /** {@code GET .../operations/{op}/status}。 */
    public OperationStatus getOperationStatus(String sessionHandle, String operationHandle)
            throws IOException {
        return OperationStatus.fromJson(request("GET",
                "/sessions/" + sessionHandle + "/operations/" + operationHandle + "/status",
                null));
    }

    /**
     * 取消一个操作：中断仍在运行的语句，并释放其结果。
     *
     * <p>走 {@code POST /v1/sessions/{s}/operations/{op}/cancel}。网关未暴露该端点
     * （老版本）时退化为 {@link #closeQuietly}。与 {@link #closeQuietly} 一样是
     * 「尽力而为」，不会因为操作已经被关闭 / 已经结束而抛异常。
     */
    public void cancelOperation(String sessionHandle, String operationHandle) {
        if (operationHandle == null) {
            return;
        }
        try {
            request("POST", operationBasePath(sessionHandle, operationHandle) + "/cancel", null);
        } catch (GatewayHttpException e) {
            if (e.getStatusCode() == 404) {
                closeQuietly(sessionHandle, operationHandle);
            }
            // 其它错误（例如操作已经结束）：尽力而为，忽略
        } catch (IOException ignore) {
            // 尽力而为
        }
    }

    // ------------------------------------------------------------------
    // 操作日志
    // ------------------------------------------------------------------

    /**
     * 操作的日志等价输出：错误文本、异常堆栈、{@code row count}、提交的
     * {@code job id}、{@code EXPLAIN} / {@code CALL} 输出——行的各单元格用制表符连接。
     *
     * @param maxPages       超过这么多页后停止；{@code <= 0} 表示一直读到 EOS
     * @param pollIntervalMs 操作仍在运行时，重试之间等待的时间
     */
    public OperationLog fetchOperationLog(String sessionHandle, String operationHandle,
                                          int maxPages, long pollIntervalMs)
            throws IOException, InterruptedException {
        List<String> lines = new ArrayList<String>();
        long token = 0L;
        long start = System.currentTimeMillis();
        int pages = 0;
        while (true) {
            ResultPage page = fetchResultPage(sessionHandle, operationHandle, token);

            if (page.isEos()) {
                return OperationLog.of(lines);
            }
            if (page.getType() == ResultType.PAYLOAD || page.getType() == ResultType.AVAILABLE) {
                for (Row row : page.getData().getRows()) {
                    lines.add(OperationLog.join(row));
                }
                if (maxPages > 0 && ++pages >= maxPages) {
                    return OperationLog.of(lines);
                }
                long nextToken = page.getNextToken();
                if (nextToken < 0 || nextToken == token) {
                    return OperationLog.of(lines);
                }
                token = nextToken;
                continue;
            }
            // NOT_READY（流式作业仍在运行时的典型状态）：用同一个 token 重试。
            checkElapsed(config.getOperationTimeoutMs(), start);
            Thread.sleep(pollIntervalMs);
        }
    }

    /** 用配置的轮询间隔读取每一页，调用 {@link #fetchOperationLog}。 */
    public OperationLog fetchOperationLog(String sessionHandle, String operationHandle)
            throws IOException, InterruptedException {
        return fetchOperationLog(sessionHandle, operationHandle, 0, config.getPollIntervalMs());
    }

    /**
     * 操作的第一条非空日志行——通常是网关自己的错误文本——当操作没有产生
     * 任何可读内容时返回 {@code null}。
     *
     * <p>operation 以 ERROR 结束时，网关并不会回一个结果页，而是直接返回
     * HTTP 500 + {@code {"errors":[...]}}，错误堆栈就在 body 里。这种情况下
     * 会从错误响应体中提炼出根因作为返回值，而不是把 HTTP 异常抛给调用方。
     */
    public String fetchOperationError(String sessionHandle, String operationHandle)
            throws IOException {
        try {
            return fetchOperationLog(sessionHandle, operationHandle, 1,
                    config.getPollIntervalMs()).getFirstLine();
        } catch (GatewayHttpException e) {
            // operation 失败时真正的原因在 HTTP 错误响应体里
            String detail = e.getErrorMessage();
            if (detail != null && !detail.isEmpty()) {
                return detail;
            }
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return null;
    }

    // ------------------------------------------------------------------
    // 结果
    // ------------------------------------------------------------------

    /**
     * 提交一条语句，等待其进入终结状态，然后返回一个开放的光标，其首页
     * 已被预先抓取（因此列名立即可用）。调用方拥有该光标，使用后必须关闭它。
     */
    public GatewayResultCursor submit(String sessionHandle, String statement, int maxRows,
                                      long pollIntervalMs, long operationTimeoutMs)
            throws IOException, InterruptedException {
        String operationHandle = executeStatement(sessionHandle, statement)
                .getOperationHandle();
        GatewayResultCursor cursor = new GatewayResultCursor(this, sessionHandle, operationHandle,
                maxRows, pollIntervalMs, operationTimeoutMs);
        boolean ok = false;
        try {
            waitForTerminalStatus(sessionHandle, operationHandle, pollIntervalMs,
                    operationTimeoutMs, System.currentTimeMillis());
            cursor.prime();
            ok = true;
            return cursor;
        } finally {
            if (!ok) {
                cursor.close();
            }
        }
    }

    /** 使用配置的 maxRows / 轮询间隔 / 超时时间调用 {@link #submit}。 */
    public GatewayResultCursor submit(String sessionHandle, String statement)
            throws IOException, InterruptedException {
        return submit(sessionHandle, statement, config.getMaxRows(),
                config.getPollIntervalMs(), config.getOperationTimeoutMs());
    }

    /**
     * 同步执行一条语句并物化整个结果。
     * 对于可能很大的结果集，建议优先使用 {@link #submit}。
     */
    public GatewayResult execute(String sessionHandle, String statement, int maxRows,
                                 long pollIntervalMs, long operationTimeoutMs)
            throws IOException, InterruptedException {
        GatewayResultCursor cursor = submit(sessionHandle, statement, maxRows,
                pollIntervalMs, operationTimeoutMs);
        try {
            return cursor.drain();
        } finally {
            cursor.close();
        }
    }

    /** 使用配置的 maxRows / 轮询间隔 / 超时时间调用 {@link #execute}。 */
    public GatewayResult execute(String sessionHandle, String statement)
            throws IOException, InterruptedException {
        return execute(sessionHandle, statement, config.getMaxRows(),
                config.getPollIntervalMs(), config.getOperationTimeoutMs());
    }

    private void waitForTerminalStatus(String sessionHandle, String operationHandle,
                                       long pollIntervalMs, long operationTimeoutMs, long start)
            throws IOException, InterruptedException {
        while (true) {
            OperationStatus status = getOperationStatus(sessionHandle, operationHandle);
            if (status.isFinished()) {
                return;
            }
            if (status.isError()) {
                // 网关会把真正的失败文本放在结果负载里。
                String detail = fetchErrorMessage(sessionHandle, operationHandle);
                throw new GatewayExecutionException(detail != null ? detail
                        : "操作以 ERROR 状态结束（操作：" + operationHandle
                          + "）。网关未返回错误详情，请检查网关日志。");
            }
            if (status.isFailure()) {
                throw new GatewayExecutionException("操作在完成前变成了 " + status.getStatus()
                        + " 状态（操作：" + operationHandle + "）。");
            }
            checkElapsed(operationTimeoutMs, start);
            Thread.sleep(pollIntervalMs);
        }
    }

    /** 尽力提取网关存放在结果负载中的失败文本。 */
    private String fetchErrorMessage(String sessionHandle, String operationHandle) {
        try {
            return fetchOperationError(sessionHandle, operationHandle);
        } catch (RuntimeException e) {
            LOG.log(Level.FINE, "无法读取网关错误负载", e);
        } catch (IOException e) {
            LOG.log(Level.FINE, "无法读取网关错误负载", e);
        }
        return null;
    }

    /**
     * 读取一页结果。包私有：供 {@link GatewayResultCursor} 使用。
     *
     * <p>路径挂在 {@code /v1} 下，同时带上 {@code rowFormat}。实测 Flink 2.2.1 的
     * {@code /v1/.../result/{token}} 会**忽略** {@code rowFormat}（传
     * {@code PLAIN_TEXT} 也照样回 {@code "rowFormat":"JSON"}、数值保持
     * Integer/BigDecimal），只有不带 {@code /v1} 前缀的同名路径才真正生效。
     * 因此这里实际拿到的一律是带类型的 JSON——比 PLAIN_TEXT 那种"所有值都变成
     * 字符串"更适合下游使用，客户端对两种形态都能解析。
     */
    ResultPage fetchResultPage(String sessionHandle, String operationHandle, long token)
            throws IOException {
        String path = "/sessions/" + sessionHandle + "/operations/" + operationHandle
                + "/result/" + token + "?rowFormat=" + encode(config.getResultRowFormat());
        return ResultPage.fromJson(request("GET", path, null));
    }

    /**
     * 关闭（释放）一个操作，丢弃它尚未读取完的结果；忽略错误。
     *
     * <p>走 {@code DELETE /v1/sessions/{s}/operations/{op}/close}。Flink 2.x 起，
     * 不带 {@code /close} 后缀的简写形式已经不存在（会返回 404），所以这里对 404
     * 做了一次老版本回退。
     */
    public void closeQuietly(String sessionHandle, String operationHandle) {
        if (operationHandle == null) {
            return;
        }
        String base = operationBasePath(sessionHandle, operationHandle);
        try {
            request("DELETE", base + "/close", null);
        } catch (GatewayHttpException e) {
            if (e.getStatusCode() != 404) {
                return; // 尽力而为：其它错误直接放弃
            }
            try {
                // 老版本网关只认这个简写形式
                request("DELETE", base, null);
            } catch (IOException ignore) {
                // 尽力而为
            }
        } catch (IOException ignore) {
            // 尽力而为
        }
    }

    /** 操作资源在网关上的路径前缀。 */
    private static String operationBasePath(String sessionHandle, String operationHandle) {
        return "/sessions/" + sessionHandle + "/operations/" + operationHandle;
    }

    void checkElapsed(long timeoutMs, long start) throws GatewayExecutionException {
        if (timeoutMs > 0 && System.currentTimeMillis() - start > timeoutMs) {
            throw new GatewayExecutionException("操作运行超过 " + timeoutMs + " 毫秒后超时。");
        }
    }

    // ------------------------------------------------------------------
    // HTTP 相关
    // ------------------------------------------------------------------

    private Map<String, Object> request(String method, String path, JsonBody body)
            throws IOException {
        return request(method, path, body, 0);
    }

    private Map<String, Object> request(String method, String path, JsonBody body,
                                        int readTimeoutMsOverride)
            throws IOException {
        String response = http(config.getBaseUrl() + path, method, path, body,
                readTimeoutMsOverride);
        return Json.parseObject(response.trim().isEmpty() ? "{}" : response);
    }

    /**
     * 发送一次 HTTP 请求并解析返回的 JSON 对象。
     *
     * @param url                完整的请求 URL（不在此处拼接 baseUrl）
     * @param method             HTTP 方法，例如 GET / POST / DELETE
     * @param displayPath        仅用于日志展示的路径片段
     * @param body               请求体；为 null 时不发送
     * @param readTimeoutMsOverride 本次请求的读超时；{@code <=0} 使用配置值
     */
    private String http(String url, String method, String displayPath, JsonBody body,
                        int readTimeoutMsOverride)
            throws IOException {
        if (METHOD_PATCH.equalsIgnoreCase(method)) {
            return httpPatch(url, method, displayPath, body, readTimeoutMsOverride);
        }
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        boolean drained = false;
        try {
            conn.setRequestMethod(method);
            conn.setConnectTimeout(config.getConnectTimeoutMs());
            conn.setReadTimeout(readTimeoutMsOverride > 0
                    ? readTimeoutMsOverride : config.getReadTimeoutMs());
            conn.setRequestProperty("Accept", "application/json");
            if (authHeader != null) {
                conn.setRequestProperty("Authorization", authHeader);
            }
            if (body != null) {
                conn.setDoOutput(true);
                conn.setRequestProperty("Content-Type", "application/json");
                byte[] payload = Json.write(body.toJson()).getBytes(StandardCharsets.UTF_8);
                conn.setFixedLengthStreamingMode(payload.length);
                OutputStream out = conn.getOutputStream();
                try {
                    out.write(payload);
                } finally {
                    out.close();
                }
            }

            int code = conn.getResponseCode();
            String respBody = readAll(code >= 200 && code < 300
                    ? conn.getInputStream() : conn.getErrorStream());
            drained = true;
            if (code >= 200 && code < 300) {
                // 原样返回；空响应体由 JSON 侧统一兜底成 "{}"（文本端点需要拿到真正的空串）
                return respBody == null ? "" : respBody;
            }
            LOG.log(Level.FINE, "HTTP {0}，请求 {1} {2}", new Object[]{code, method, displayPath});
            // 带上状态码与响应体：Flink 把真正的失败原因放在 {"errors":[...]} 里，
            // 上层（例如 operation 以 ERROR 结束时的取错逻辑）需要靠它还原根因。
            throw new GatewayHttpException(code, conn.getResponseMessage(), url, respBody);
        } finally {
            // 只有响应未被完整读取时才断开连接。完整读取的响应会让
            // HttpURLConnection 把套接字归还给 keep-alive 连接池——这一点很关键，
            // 因为状态轮询在查询的整个生命周期内每个 pollIntervalMs 都会发一次请求。
            if (!drained) {
                conn.disconnect();
            }
        }
    }

    /**
     * 发送 PATCH 请求。
     *
     * <p>之所以不能复用 {@link HttpURLConnection}：JDK 8 把它自己的方法白名单写死在
     * {@code setRequestMethod} 里（GET / POST / HEAD / OPTIONS / PUT / DELETE / TRACE），
     * 调 {@code setRequestMethod("PATCH")} 会当场抛
     * {@code ProtocolException: Invalid HTTP method: PATCH}（已实测）。而 Flink 集群
     * REST 取消作业恰恰只认 {@code PATCH /jobs/{jobId}}——Flink 2.2.1 上
     * {@code POST /jobs/{id}/cancel} 与 {@code DELETE /jobs/{id}} 都是 404，没有替代端点。
     *
     * <p>所以这里改用本项目已依赖的 hutool 发 PATCH：hutool 的做法是反射把
     * {@code "PATCH"} 追加进 {@link HttpURLConnection} 的方法白名单，再走原生
     * {@code setRequestMethod}，因此连接复用、超时等行为与本类其它请求保持一致。
     * 只有 PATCH 走这条路，GET / POST / DELETE 仍然直接用 {@link HttpURLConnection}。
     */
    private String httpPatch(String url, String method, String displayPath, JsonBody body,
                             int readTimeoutMsOverride)
            throws IOException {
        HttpRequest req = HttpRequest.patch(url)
                .setConnectionTimeout(config.getConnectTimeoutMs())
                .timeout(readTimeoutMsOverride > 0
                        ? readTimeoutMsOverride : config.getReadTimeoutMs())
                .header("Accept", "application/json");
        if (authHeader != null) {
            req.header("Authorization", authHeader);
        }
        if (body != null) {
            req.body(Json.write(body.toJson()), "application/json");
        }

        HttpResponse resp;
        try {
            resp = req.execute();
        } catch (RuntimeException e) {
            // hutool 把连接层故障包成 IORuntimeException，这里还原成 IOException
            throw new IOException("请求 " + method + " " + displayPath + " 失败："
                    + e.getMessage(), e);
        }
        int code = resp.getStatus();
        String respBody = resp.body();
        if (code >= 200 && code < 300) {
            return respBody == null ? "" : respBody;
        }
        LOG.log(Level.FINE, "HTTP {0}，请求 {1} {2}", new Object[]{code, method, displayPath});
        // hutool 拿不到 reason phrase；状态码 + 响应体已经足够上层提炼根因
        throw new GatewayHttpException(code, null, url, respBody);
    }

    /**
     * 向集群（JobManager）REST 端点发起 GET/POST 请求，不走网关的 /v1 前缀。
     *
     * @param method    HTTP 方法，例如 GET / POST
     * @param path      相对于集群端点的路径，例如 {@code /jobs/{id}/stop}
     * @param readTimeoutMs 请求读超时；{@code <=0} 使用配置值
     */
    private Map<String, Object> jobManager(String method, String path, int readTimeoutMs)
            throws IOException {
        String response = http(config.getEndpoint() + path, method, path, null, readTimeoutMs);
        return Json.parseObject(response.trim().isEmpty() ? "{}" : response);
    }

    private static String readAll(InputStream in) throws IOException {
        if (in == null) {
            return null;
        }
        try {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            byte[] chunk = new byte[8192];
            int n;
            while ((n = in.read(chunk)) != -1) {
                buf.write(chunk, 0, n);
            }
            return new String(buf.toByteArray(), StandardCharsets.UTF_8);
        } finally {
            in.close();
        }
    }

    private static String encode(String v) {
        try {
            return URLEncoder.encode(v, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            return v; // UTF-8 总是可用的
        }
    }
}
