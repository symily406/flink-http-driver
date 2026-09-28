package com.easy.etl.flink.http.model;

import com.easy.etl.flink.http.util.Json;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * **集群** REST 端点的 {@code GET /jobs/{jobId}/exceptions}。
 *
 * <p>当一个 INSERT 已被网关报告为 FINISHED、但之后真正执行时才失败时，
 * 真正的根因就在这里。
 */
public final class JobExceptions {

    private static final JobExceptions EMPTY =
            new JobExceptions(Collections.<JobExceptionEntry>emptyList());

    private final List<JobExceptionEntry> entries;

    public JobExceptions(List<JobExceptionEntry> entries) {
        this.entries = Collections.unmodifiableList(new ArrayList<JobExceptionEntry>(
                entries == null ? Collections.<JobExceptionEntry>emptyList() : entries));
    }

    @SuppressWarnings("unchecked")
    public static JobExceptions fromJson(Map<String, Object> json) {
        Object history = Json.get(json, "exceptionHistory");
        if (history == null) {
            history = Json.get(json, "exception-history"); // Flink <= 1.11 使用 snake_case
        }
        if (!(history instanceof Map)) {
            return EMPTY;
        }
        Object entries = Json.get((Map<String, Object>) history, "entries");
        if (!(entries instanceof List)) {
            return EMPTY;
        }
        List<JobExceptionEntry> out = new ArrayList<JobExceptionEntry>();
        for (Object e : (List<?>) entries) {
            if (e instanceof Map) {
                JobExceptionEntry entry = JobExceptionEntry.fromJson((Map<String, Object>) e);
                if (entry.getRoot() != null && !entry.getRoot().isEmpty()) {
                    out.add(entry);
                }
            }
        }
        return new JobExceptions(out);
    }

    public static JobExceptions empty() {
        return EMPTY;
    }

    /** 历史记录，按时间从旧到新排列。 */
    public List<JobExceptionEntry> getEntries() {
        return entries;
    }

    /** 根因文本，按时间从旧到新排列。 */
    public List<String> getRoots() {
        List<String> out = new ArrayList<String>(entries.size());
        for (JobExceptionEntry e : entries) {
            out.add(e.getRoot());
        }
        return out;
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    @Override
    public String toString() {
        return "JobExceptions" + getRoots();
    }
}
