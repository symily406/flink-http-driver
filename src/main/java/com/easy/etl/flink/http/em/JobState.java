package com.easy.etl.flink.http.em;

import java.util.Locale;

/**
 * 作业运行状态的编码枚举：一个数字 + 一个 Flink 原始状态文本，便于上层
 * （调度平台、数据库字段、前端）直接落库或展示。
 *
 * <p>两个布尔位的含义：
 * <ul>
 *   <li>{@code terminal}：作业已停止运行（不再有活跃的执行）。注意
 *       {@link #SUSPENDED} 也算终态——它已经停下来了。</li>
 *   <li>{@code settled}：结果已最终确定，不会再回到运行态。{@link #SUSPENDED}
 *       是 {@code terminal} 但非 {@code settled}，因为它还能被恢复（resume）。</li>
 * </ul>
 *
 * <p>状态文本解析见 {@link #fromState(String)}：不区分大小写，兼容
 * {@code CANCELED}/{@code CANCELLED} 两种拼写；无法识别或缺失时返回
 * {@link #UNKNOWN}，因此返回值永不为 {@code null}。
 */
public enum JobState {

    INITIALIZING(1, "INITIALIZING", "初始化中", false, false),
    CREATED(2, "CREATED", "已创建", false, false),
    RUNNING(3, "RUNNING", "运行中", false, false),
    /** 正在执行失败处理（触发 failover / 重试之前的中间态），还没到终态。 */
    FAILING(4, "FAILING", "失败处理中", false, false),
    /** 失败：状态值固定为 -1，便于「小于 0 即失败」的快速判断。 */
    FAILED(-1, "FAILED", "已失败", true, true),
    CANCELLING(5, "CANCELLING", "取消中", false, false),
    CANCELED(6, "CANCELED", "已取消", true, true),
    FINISHED(7, "FINISHED", "已完成", true, true),
    RESTARTING(8, "RESTARTING", "重启中", false, false),
    SUSPENDED(9, "SUSPENDED", "已挂起", true, false),
    RECONCILING(10, "RECONCILING", "协调中", false, false),

    UNKNOWN(-10, "UNKNOWN", "未知", false, false);

    private final int code;
    private final String state;
    private final String description;
    private final boolean terminal;
    private final boolean settled;


    JobState(int code, String state, String description, boolean terminal, boolean settled) {
        this.code = code;
        this.state = state;
        this.description = description;
        this.terminal = terminal;
        this.settled = settled;
    }

    /** 状态编码：-1 失败，1~10 见枚举定义，-10 未知。 */
    public int getCode() {
        return code;
    }

    /** Flink 原始状态文本，例如 {@code RUNNING}。 */
    public String getState() {
        return state;
    }

    /** 中文说明，可直接展示。 */
    public String getDescription() {
        return description;
    }

    /** 是否已停止运行（含 {@link #SUSPENDED}）。 */
    public boolean isTerminal() {
        return terminal;
    }

    /** 结果是否已最终确定，不会再恢复到运行态。 */
    public boolean isSettled() {
        return settled;
    }

    /** 是否仍在运行（未到终态）。 */
    public boolean isActive() {
        return !terminal;
    }

    /** 是否运行失败（编码 -1）。 */
    public boolean isFailure() {
        return this == FAILED;
    }

    /**
     * 从集群返回的原始状态文本解析（不区分大小写）；无法识别时返回 {@link #UNKNOWN}。
     */
    public static JobState fromState(String state) {
        if (state == null) {
            return UNKNOWN;
        }
        String s = state.trim().toUpperCase(Locale.ROOT);
        if ("CANCELLED".equals(s)) { // 英式拼写
            return CANCELED;
        }
        for (JobState value : values()) {
            if (value.state.equals(s)) {
                return value;
            }
        }
        return UNKNOWN;
    }

    /** 按编码反查；未知编码返回 {@link #UNKNOWN}。 */
    public static JobState fromCode(int code) {
        for (JobState value : values()) {
            if (value.code == code) {
                return value;
            }
        }
        return UNKNOWN;
    }

    @Override
    public String toString() {
        return name() + "(" + code + ":" + description + ")";
    }
}
