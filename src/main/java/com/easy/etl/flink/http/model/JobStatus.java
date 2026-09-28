package com.easy.etl.flink.http.model;

import com.easy.etl.flink.http.em.JobState;
import com.easy.etl.flink.http.util.Json;

import java.util.Locale;
import java.util.Map;

/**
 * **集群** REST 端点的作业运行状态（{@code GET /jobs/{jobId}}，
 * 或 {@code GET /jobs/overview} 中的一条记录）。
 *
 * <p>SQL Gateway 的 {@code operation} 状态只能反映"语句是否被网关接受"：流式
 * INSERT 提交后 operation 很快就是 FINISHED，作业却仍在后台运行，后续的失败也
 * 不会回到 operation 上。作业真正的运行状态要看这里。
 *
 * <p>Flink 可能新增状态，因此原始文本总是保留在 {@link #getState()} 中，并且只
 * 做不区分大小写的比较。
 */
public final class JobStatus {

    public static final String INITIALIZING = "INITIALIZING";
    public static final String CREATED = "CREATED";
    public static final String RUNNING = "RUNNING";
    public static final String RESTARTING = "RESTARTING";
    public static final String SUSPENDED = "SUSPENDED";
    public static final String FAILING = "FAILING";
    public static final String CANCELLING = "CANCELLING";
    public static final String FINISHED = "FINISHED";
    public static final String FAILED = "FAILED";
    public static final String CANCELED = "CANCELED";

    private final String jobId;
    private final String name;
    private final String state;
    /** 启动时间的毫秒时间戳；集群未返回时为 -1。 */
    private final long startTime;
    /** 结束时间的毫秒时间戳；作业仍在运行（或集群未返回）时为 -1。 */
    private final long endTime;
    /** 已运行时长（毫秒）；集群未返回时为 -1。 */
    private final long duration;

    public JobStatus(String jobId, String name, String state, long startTime, long endTime,
                     long duration) {
        this.jobId = jobId;
        this.name = name;
        this.state = state;
        this.startTime = startTime;
        this.endTime = endTime;
        this.duration = duration;
    }

    /**
     * 解析 {@code GET /jobs/{jobId}} 或 {@code /jobs/overview} 中的一条记录。
     * 兼容 {@code jid}/{@code jobId} 与 {@code start-time}/{@code startTime} 两种写法。
     */
    public static JobStatus fromJson(Map<String, Object> json) {
        String jobId = Json.getStr(json, "jid");
        if (jobId == null) {
            jobId = Json.getStr(json, "jobId");
        }
        long start = firstLong(json, "start-time", "startTime");
        long end = firstLong(json, "end-time", "endTime");
        long duration = firstLong(json, "duration");
        if (duration < 0 && start >= 0 && end >= 0) {
            duration = end - start;
        }
        return new JobStatus(jobId, Json.getStr(json, "name"), Json.getStr(json, "state"),
                start, end, duration);
    }

    /** 依次尝试若干个 key，取第一个存在的值；都不存在时返回 -1。 */
    private static long firstLong(Map<String, Object> json, String... keys) {
        for (String key : keys) {
            Object v = Json.get(json, key);
            if (v != null) {
                return Json.asLong(v);
            }
        }
        return -1L;
    }

    /** 作业 ID（32 位 hex），与 {@code GatewayResult#getJobId()} 一致。 */
    public String getJobId() {
        return jobId;
    }

    /** 作业名，通常是 {@code pipeline.name} 或 Flink 自动生成的名字。 */
    public String getName() {
        return name;
    }

    /** 原始状态文本，例如 {@code RUNNING} / {@code FINISHED} / {@code FAILED}。 */
    public String getState() {
        return state;
    }

    /**
     * 统一编码后的运行状态（{@link JobState}），永不返回 {@code null}——
     * 无法识别时是 {@link JobState#UNKNOWN}。
     */
    public JobState getJobState() {
        return JobState.fromState(state);
    }

    /**
     * 状态编码：-1 已失败，1 初始化中，2 已创建，3 运行中，4 失败处理中，5 取消中，
     * 6 已取消，7 已完成，8 重启中，9 已挂起，10 协调中，-10 未知。
     * 等价于 {@code getJobState().getCode()}。
     */
    public int getStateCode() {
        return getJobState().getCode();
    }

    /** 启动时间戳（毫秒）；未知时为 -1。 */
    public long getStartTime() {
        return startTime;
    }

    /** 结束时间戳（毫秒）；作业仍在运行或未知时为 -1。 */
    public long getEndTime() {
        return endTime;
    }

    /** 已运行时长（毫秒）；未知时为 -1。 */
    public long getDuration() {
        return duration;
    }

    private boolean is(String expected) {
        return state != null && state.equalsIgnoreCase(expected);
    }

    public boolean isInitializing() {
        return is(INITIALIZING);
    }

    public boolean isCreated() {
        return is(CREATED);
    }

    public boolean isRunning() {
        return is(RUNNING);
    }

    public boolean isRestarting() {
        return is(RESTARTING);
    }

    public boolean isFailing() {
        return is(FAILING);
    }

    /** 正在取消（{@code CANCELLING}，也可能是美式拼写 {@code CANCELING}）。 */
    public boolean isCancelling() {
        return is(CANCELLING) || is("CANCELING");
    }

    public boolean isFinished() {
        return is(FINISHED);
    }

    public boolean isFailed() {
        return is(FAILED);
    }

    /** 已取消（{@code CANCELED}，也可能是英式拼写 {@code CANCELLED}）。 */
    public boolean isCanceled() {
        return is(CANCELED) || is("CANCELLED");
    }

    /** 已挂起：作业已停下，但还能被恢复（resume），因此不是最终结果。 */
    public boolean isSuspended() {
        return is(SUSPENDED);
    }

    public boolean isReconciling() {
        return is("RECONCILING");
    }

    /** 作业已停止运行；与 {@link JobState#isTerminal()} 保持一致的判定口径。 */
    public boolean isTerminal() {
        return getJobState().isTerminal();
    }

    /** 还没到终态——作业仍在跑，或在被调度 / 重启 / 取消中。 */
    public boolean isActive() {
        return !isTerminal();
    }

    @Override
    public String toString() {
        return "JobStatus{jid=" + jobId + ", name=" + name + ", state="
                + (state == null ? null : state.toUpperCase(Locale.ROOT)) + "}";
    }
}
