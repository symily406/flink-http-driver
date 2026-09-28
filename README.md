# Flink SQL Gateway HTTP Client

Flink SQL Gateway REST API（v1）的 **Java 客户端**：零依赖、JDK 1.8 可跑，
所有请求/响应都是**强类型 model**，不再出现 `Map<String, Object>` 式的裸 JSON。

适用于 Flink 1.18 ~ 2.x。

## 特性

- **零运行时依赖**：只依赖 JDK 1.8（自带 JSON 解析/序列化），丢进任何 classpath 都不会引冲突。
- **全 model 化**：`SessionInfo` / `OperationInfo` / `OperationStatus` / `ResultPage` /
  `GatewayResult` / `JobExceptions` / `ClusterLog` … 每个响应都有对应 model，
  请求也是 model（`OpenSessionRequest`、`ExecuteStatementRequest`）。
- **两种读法**：`execute()` 一次性取完；`submit()` 拿流式游标，按页拉取，堆占用恒定。
- **连通性检测**：`ping()` / `checkReachable()`，先试 `GET /v1/info`，不通再回退建会话。
- **作业诊断**：Gateway 说 FINISHED、作业其实失败时，去集群 REST 取真实异常
  （`fetchJobExceptions`）。

## 安装

```xml
<dependency>
    <groupId>com.easy.etl</groupId>
    <artifactId>flink-http-driver</artifactId>
    <version>1.0.0</version>
</dependency>
```

或直接用 jar：`mvn package` → `target/flink-http-driver-1.0.0.jar`。

## 30 秒上手

```java
SqlGatewayClient client = new SqlGatewayClient("http://localhost:8083?user=admin&password=secret");

SessionInfo session = client.openSession();
try {
    String handle = session.getSessionHandle();
    client.execute(handle, "SET 'execution.runtime-mode' = 'batch'");
    client.execute(handle, "CREATE TABLE orders (id BIGINT, user_id STRING, amount DOUBLE)"
            + " WITH ('connector' = 'datagen')");

    GatewayResult result = client.execute(handle,
            "SELECT id, user_id, amount FROM orders LIMIT 10");
    for (Map<String, Object> row : result.toMaps()) {   // key = 列名
        System.out.println(row.get("id") + "\t" + row.get("user_id") + "\t" + row.get("amount"));
    }
} finally {
    client.closeSession(session.getSessionHandle());
}
```

## 客户端与配置

```java
// 地址里直接带参数（前缀 jdbc:flink: 可选，大小写不敏感，两种写法等价）
SqlGatewayClient client = new SqlGatewayClient("http://gw:8083?user=admin&password=secret&rowFormat=json");
SqlGatewayClient same   = new SqlGatewayClient("jdbc:flink:http://gw:8083?user=admin&password=secret&rowFormat=json");

// 或者显式配置
GatewayConfig config = GatewayConfig.parse("http://gw:8083");
config.setUser("admin");
config.setPassword("secret");
config.setSessionProperty("execution.runtime-mode", "batch");   // 等价 SET
SqlGatewayClient c2 = new SqlGatewayClient(config);
```

地址参数（都可在 `GatewayConfig` 上直接 setter）：

| 参数 | 说明 | 默认 |
|---|---|---|
| `user` / `password` | HTTP Basic 鉴权 | 无 |
| `connectTimeoutMs` | 连接超时 | 10000 |
| `readTimeoutMs` | 读取超时 | 60000 |
| `pollIntervalMs` | 状态/结果轮询间隔 | 500 |
| `operationTimeoutMs` | 等待 operation 结束的上限，0 = 不限 | 0 |
| `maxRows` | 游标最多读取的行数，0 = 不限 | 0 |
| `rowFormat` | 向网关请求的返回格式：`table`（默认，等价网关的 `PLAIN_TEXT`，所有值都序列化成字符串）/ `json`（数值落成 `Long` / `BigDecimal`）。两种拼写都接受：`table` / `plain_text`、`json` / `JSON` | table |
| `sessionProperty.XXX` | 建会话时下发的会话属性，即 `SET 'XXX' = '...'` | 无 |

## API 一览

| HTTP 请求 | 方法 | 返回 model |
|---|---|---|
| `GET /v1/info` | `client.getInfo()` | `GatewayInfo`（不可用时 `null`） |
| –（探活） | `client.ping()` / `client.checkReachable()` | boolean / 抛 `IOException` |
| `POST /v1/sessions` | `client.openSession()` | `SessionInfo` |
| `DELETE /v1/sessions/{s}` | `client.closeSession(s)` | – |
| `POST /v1/sessions/{s}/heartbeat` | `client.heartbeat(s)` | – |
| `POST /v1/sessions/{s}/statements` | `client.executeStatement(s, sql)` | `OperationInfo` |
| `GET …/operations/{op}/status` | `client.getOperationStatus(s, op)` | `OperationStatus` |
| `GET …/operations/{op}/result/{token}` | `client.submit(...)` / `execute(...)` 内部按 token 翻页 | `ResultPage` / `GatewayResult` |
| `DELETE …/operations/{op}/close` | `client.closeQuietly(s, op)` | – |
| `POST …/operations/{op}/cancel` | `client.cancelOperation(s, op)` | – |
| operation 输出（结果负载） | `client.fetchOperationLog(s, op)` | `OperationLog` |
| `GET /jobs/{id}`（8081） | `SqlGatewayClient.fetchJobStatus(url, jobId)` | `JobStatus` |
| `GET /jobs/{id}`（8081） | `SqlGatewayClient.fetchJobState(url, jobId)` | `JobState`（编码枚举） |
| `GET /jobs/overview`（8081） | `SqlGatewayClient.fetchJobStatuses(url)` | `List<JobStatus>` |
| 轮询 `GET /jobs/{id}` | `SqlGatewayClient.waitForJob(url, jobId, poll, timeout)` | `JobStatus`（终态） |
| –（作业是否在跑） | `SqlGatewayClient.isJobRunning(url, jobId)` | boolean |
| `GET /jobs/{id}/exceptions`（8081） | `SqlGatewayClient.fetchJobExceptions(url, jobId)` | `JobExceptions` |
| `GET /jobmanager/log`（8081） | `SqlGatewayClient.fetchJobManagerLog(url)` | `ClusterLog` |
| `GET /taskmanagers/{id}/log`（8081） | `SqlGatewayClient.fetchTaskManagerLog(url, tm)` | `ClusterLog` |
| 给定 jobId 批量抓该作业涉及的所有 TM 日志（8081） | `SqlGatewayClient.fetchTaskManagerLogsForJob(url, jobId)` | `Map<String, ClusterLog>`（tmId → 日志） |
| 任意路径（纯文本端点） | `client.getText(path)` | `String`（原始文本逃生口） |
| `POST /jobs/{id}/stop`（8081） | `SqlGatewayClient.triggerStop(url, jobId, drain)` | String（request-id） |
| `GET /jobs/{id}/savepoints/{rid}`（8081） | `SqlGatewayClient.getStopResult(url, jobId, requestId)` | `StopResult` |
| `POST /jobs/{id}/stop` + 轮询 | `SqlGatewayClient.stopJob(url, jobId, drain, poll, timeout)` | `StopResult` |
| `PATCH /jobs/{id}`（8081） | `SqlGatewayClient.cancelJob(url, jobId)` | void（取消批任务；JDK 8 的 `HttpURLConnection` 不支持 PATCH，该请求由 hutool 发出，对调用方透明） |
| 任意 v1 路径 | `client.getJson(path)` | `Map<String,Object>`（原始逃生口） |

## 用法

### 1. 探活 / 健康检查

```java
if (!client.ping(3000)) { ... }        // 只关心通不通
client.checkReachable(3000);           // 不通时抛 IOException，message 说明原因
SqlGatewayClient.testConnection("http://gw:8083", 3000);   // 静态快捷方式
```

探活先发最轻的 `GET /v1/info`；该端点未暴露时回退 `POST /v1/sessions`（用完即删）。
凭据一直会带上，所以鉴权失败 = 不可达。

### 2. 会话与 SET

```java
SessionInfo session = client.openSession();
String handle = session.getSessionHandle();

client.execute(handle, "SET 'pipeline.name' = 'mysql-jdbc-batch-sync'");
client.heartbeat(handle);                // 长空闲会话自己定期保活
client.closeSession(handle);             // 会话不会自动回收，务必 finally 关掉
```

`SET` 只在**当前会话**生效，所以 SET / DDL / DML 必须走同一个 `sessionHandle`。

### 3. 执行语句

`execute()` = 提交 + 轮询到终态 + 按页拉完 + 关闭操作；出错抛
`GatewayExecutionException`（继承 `IOException`，message 是网关结果负载里的真实错误文本）。

```java
GatewayResult result = client.execute(handle,
        "SELECT id, user_id, amount FROM orders LIMIT 10");
```

需要自定义超时/轮询时用完整签名：

```java
client.execute(handle, sql,
        0,      // maxRows：0 表示不限
        500,    // pollIntervalMs
        0);     // operationTimeoutMs：0 表示不限
```

也可以自己编排（提交 → 轮询状态 → 关操作）：

```java
OperationInfo op = client.executeStatement(handle, "SELECT * FROM orders");
OperationStatus status = client.getOperationStatus(handle, op.getOperationHandle());
if (status.isFinished()) { ... }
if (status.isError()) { System.out.println(client.fetchOperationError(handle, op.getOperationHandle())); }
client.closeQuietly(handle, op.getOperationHandle());
```

**网关一次只接受一条语句**（多语句会被服务端拒绝）。跑脚本就自己按分号拆开后逐条提交：

```java
for (String sql : splitScript(script)) {
    try {
        client.execute(handle, sql);
    } catch (GatewayExecutionException e) {
        throw new IllegalStateException("失败语句: " + sql + "\n原因: " + e.getMessage(), e);
    }
}
```

拆的时候跳过 `--` 注释和字符串字面量里的分号，别用 `script.split(";")` 裸拆。

### 4. 大结果集：流式游标

```java
try (GatewayResultCursor cursor = client.submit(handle, "SELECT id, user_id FROM orders")) {
    for (Column c : cursor.columns()) {        // 首页已预取，列名立即可用
        System.out.print(c.getName() + "\t");
    }
    System.out.println();
    while (cursor.next()) {                    // 自动跟随 nextResultUri 翻页
        Row row = cursor.row();                // List<Object> 风格取值
        System.out.println(row + " / " + cursor.rowAsMap());
    }
}                                              // close() -> DELETE /operations/{op}
```

### 5. 结果 model

```java
GatewayResult r = client.execute(handle, "SELECT id, amount FROM orders LIMIT 5");

r.getColumns();                 // [id, amount]
r.getResultKind();              // SUCCESS_WITH_CONTENT
r.getRows();                    // List<Row>
r.getRows().get(0).get(0);      // 第一格
r.toMaps().get(0).get("id");    // 按列名取值（LinkedHashMap，保持列顺序）
r.toMap();                      // 只有一行时的便捷写法，空结果返回 null
```

`toMaps()` 的约定：重复列名只保留一个（后者覆盖前者）、`null` 列名跳过、比列数短的行
只带取得到的 key；值为 `null` 的列**仍保留 key**（`containsKey` 为 true，`get` 返回 `null`）。

### 6. DML：job id 与失败诊断

```java
GatewayResult dml = client.execute(handle, "INSERT INTO sink SELECT * FROM orders");
dml.isDml();            // 首列是标记列时为 true：批模式是 "row count"，流模式是 "job id"
dml.getUpdateCount();   // 批模式的影响行数；流模式没有 row count，固定为 0
dml.getJobId();         // 提交的 Flink 作业 ID（32 位 hex）
```

`runtime-mode=streaming` 时 INSERT 会很快返回 FINISHED、作业在后台继续跑；**作业后来失败
不会体现在 operation 上**，真实原因在集群 REST（默认 `http://host:8081`，不是 8083）：

```java
JobExceptions failures = SqlGatewayClient.fetchJobExceptions("http://host:8081", dml.getJobId());
for (JobExceptionEntry e : failures.getEntries()) {
    System.out.println(e.getTaskName() + " -> " + e.getRoot());
}
```

日志同理：

```java
SqlGatewayClient.fetchJobManagerLog("http://host:8081").getLines();
SqlGatewayClient.fetchTaskManagerLog("http://host:8081", tmId).getLines();
client.fetchOperationLog(handle, op.getOperationHandle()).getLines();

// 给定 jobId，自动解析作业涉及的所有 TaskManager 并批量抓日志：tmId -> ClusterLog
Map<String, ClusterLog> logs =
        SqlGatewayClient.fetchTaskManagerLogsForJob("http://host:8081", jobId);
for (Map.Entry<String, ClusterLog> e : logs.entrySet()) {
    String tmId = e.getKey();                  // 例如 node01:34644-0718c3
    List<String> lines = e.getValue().getLines();
}
```

集群的日志端点在 Flink 2.x 上直接返回 `text/plain` 的原始日志（不是 JSON），客户端两种形态都解析。

`fetchTaskManagerLogsForJob` 内部走三步：先 `GET /jobs/{jobId}` 取作业的顶点列表；再对每个顶点
`GET /jobs/{jobId}/vertices/{vertexId}` 取 `subtasks[].host`（即 TaskManager 的资源 ID，
Flink 2.x 里与废弃的 `taskmanager-id` 等价）；最后逐个 `GET /taskmanagers/{tmId}/log` 拉日志。
一个作业通常跑在多个 TM 上，结果按 tmId 出现的先后顺序**去重**，日志同样会整份读入内存，记得用 `tail(n)` 截断。

只想要某一个 TM 的日志时，先 `GET /taskmanagers` 拿 `id` 列表，再调 `fetchTaskManagerLog`：

```java
Map<String, Object> tms = new SqlGatewayClient("http://host:8081").getJson("/taskmanagers");
List<Map<String, Object>> list = (List<Map<String, Object>>) tms.get("taskmanagers");
String tmId = (String) list.get(0).get("id");   // 不同部署形态：host:port / container_e..@host:port
ClusterLog log = SqlGatewayClient.fetchTaskManagerLog("http://host:8081", tmId);
```

`tmId` 千万别自己拼——不同部署形态长得不一样（standalone/K8s 是 `host:port`，YARN 是
`container_e03_…@host:port`），一律从 `GET /taskmanagers` 或作业 subtask 的 `host` 字段里取。

**注意**：日志端点虽然接受 `?size=N`，但 Flink 2.2.1 实测会**忽略**它（`GET /jobmanager/log?size=5`
与不带参数返回的行数完全一致，都是整份日志），所以限流必须在客户端做——用 `ClusterLog#tail(n)`
只取末尾若干行：

```java
SqlGatewayClient.fetchJobManagerLog("http://host:8081").tail(500).getLines();   // 只看最后 500 行
SqlGatewayClient.fetchTaskManagerLog("http://host:8081", tmId).tail(500).getLines();
SqlGatewayClient.fetchClusterLog("http://host:8081", "/jobmanager/log").tail(500).getLines();
new SqlGatewayClient("http://host:8081").getText("/jobmanager/log");            // 原始文本逃生口
```

### 7. 查看作业运行状态

operation 的 FINISHED 只表示"语句被网关接受"，**不等于作业跑完了**（流式 INSERT 尤其如此）。
作业真正的状态在集群 REST（默认 `http://host:8081`）上查：

```java
JobStatus status = SqlGatewayClient.fetchJobStatus("http://host:8081", dml.getJobId());

status.getState();        // RUNNING / FINISHED / FAILED / CANCELED …（原始文本）
status.isRunning();       // 仅 RUNNING
status.isActive();        // 未到终态：CREATED / RUNNING / RESTARTING / CANCELLING … 都算
status.isTerminal();      // FINISHED / FAILED / CANCELED
status.isFailed();
status.getName();         // 作业名（pipeline.name）
status.getStartTime();    // 启动时间戳；未知为 -1
status.getEndTime();      // 结束时间戳；仍在运行为 -1
status.getDuration();     // 已运行毫秒；未知为 -1
```

需要给上层（调度平台、数据库字段、前端）一个数字状态时，用
`com.easy.etl.flink.http.em.JobState` 枚举：

```java
JobState state = SqlGatewayClient.fetchJobState("http://host:8081", jobId);
// 或 status.getJobState() / status.getStateCode()

state.getCode();          // 状态编码
state.getState();         // Flink 原始状态文本，例如 "RUNNING"
state.getDescription();   // 中文说明，可直接展示
state.isFailure();        // 编码 == -1
state.isTerminal();       // 作业已停止运行（含 SUSPENDED）
state.isSettled();        // 结果已最终确定，不会再恢复到运行态
state.isActive();         // 仍在运行（未到终态）
JobState.fromCode(code);  // 反查；未知编码得到 UNKNOWN
```

| 编码 | 枚举 | 说明 | terminal | settled |
|---|---|---|---|---|
| 1 | `INITIALIZING` | 初始化中 | false | false |
| 2 | `CREATED` | 已创建 | false | false |
| 3 | `RUNNING` | 运行中 | false | false |
| 4 | `FAILING` | 失败处理中（failover 前的中间态） | false | false |
| -1 | `FAILED` | 已失败 | true | true |
| 5 | `CANCELLING` | 取消中 | false | false |
| 6 | `CANCELED` | 已取消 | true | true |
| 7 | `FINISHED` | 已完成 | true | true |
| 8 | `RESTARTING` | 重启中 | false | false |
| 9 | `SUSPENDED` | 已挂起 | true | **false**（还能 resume） |
| 10 | `RECONCILING` | 协调中 | false | false |
| -10 | `UNKNOWN` | 未知 | false | false |

`terminal` = 作业已停止运行；`settled` = 结果已最终确定、不会再回到运行态
（`SUSPENDED` 是唯一「terminal 但非 settled」的状态）。失败固定为 `-1`，
所以「小于 0 即失败」的判断对 `FAILED` 与 `UNKNOWN` 都成立。

状态文本解析不区分大小写，`CANCELED` / `CANCELLED` 两种拼写都能识别；
`fromState(null)` 与无法识别的文本都返回 `UNKNOWN`（不会返回 `null`）。

常用写法：

```java
// 只看"还在不在跑"
if (SqlGatewayClient.isJobRunning("http://host:8081", jobId)) { ... }

// 集群上所有作业的状态快照（GET /jobs/overview）
for (JobStatus job : SqlGatewayClient.fetchJobStatuses("http://host:8081")) {
    System.out.println(job.getJobId() + " " + job.getState() + " " + job.getName());
}

// 阻塞等到作业结束（默认 1000ms 轮询、不限时）；超时抛 GatewayExecutionException
JobStatus done = SqlGatewayClient.waitForJob("http://host:8081", jobId);
JobStatus done2 = SqlGatewayClient.waitForJob("http://host:8081", jobId,
        500,        // pollIntervalMs，<=0 用 1000
        60_000);    // timeoutMs，<=0 表示不限
if (done.isFailed()) {
    for (String root : SqlGatewayClient.fetchJobExceptions("http://host:8081", jobId).getRoots()) {
        System.err.println(root);
    }
}
```

### 8. 停止流任务并取回 checkpoint

流式作业（`runtime-mode=streaming`）需要"停而不丢数据"时用 **stop（停止）** 而不是 cancel（取消）：
stop 会先触发一次 savepoint / checkpoint，留下可恢复的停顿点；之后可用返回的位置 `restore`。
这些方法直接访问**集群（JobManager）REST**（默认 `http://host:8081`，不是 8083）。

分两步手动编排：

```java
// 1) 触发停止，拿到 request-id
String requestId = SqlGatewayClient.triggerStop("http://host:8081", jobId, true);
//    true  = stop-with-drain（等待已缓冲数据全部写出后停机）
//    false = 直接停

// 2) 用 request-id 轮询结果，直到 COMPLETED / FAILED
StopResult result = SqlGatewayClient.getStopResult("http://host:8081", jobId, requestId);
if (result.isCompleted()) {
    String checkpoint = result.getLocation();   // 可恢复的 checkpoint / savepoint 路径
    System.out.println("停顿点：已生成 " + checkpoint);
} else if (result.isFailed()) {
    System.err.println("停止失败：" + result.getFailureCause());
}
```

最简单的一行调用（只传地址 + 作业 ID，直接停、自动轮询取回 checkpoint）：

```java
// 最简：只传地址 + 作业 ID，直接停、自动轮询取回 checkpoint
StopResult result = SqlGatewayClient.stopJob("http://host:8081", jobId);
String checkpoint = result.getLocation();   // isCompleted() 时即为停顿点路径
```

需要自定义 drain / 轮询参数时用完整签名：

```java
StopResult result = SqlGatewayClient.stopJob("http://host:8081", jobId,
        true,      // withDrain
        500,       // pollIntervalMs（轮询间隔，<=0 用默认 500）
        60_000);   // operationTimeoutMs（最大等待，<=0 表示不限）

String checkpoint = result.getLocation();   // COMPLETED 时即为停顿点路径
```

`StopResult` 提供 `isInProgress()` / `isCompleted()` / `isFailed()` / `isTerminal()`，
以及 `getRequestId()` / `getStatus()` / `getLocation()` / `getFailureCause()`。

#### 批任务能不能用这套方法？

**技术上能**：stop 走的是 JobManager REST，不区分批/流，本质都是「先给正在运行的作业补一个
checkpoint 再停机」。但只对**处于 `RUNNING` 状态的作业**有效：

| 场景 | 能否调用 | 说明 |
|---|---|---|
| 批作业正在运行中途（RUNNING） | ✅ | 会触发 checkpoint 后停止，`getLocation()` 返回停顿点，可用于断点续跑 |
| 批作业已跑完（FINISHED） | ❌ | 没有可停的 RUNNING 作业，JobManager 直接返回错误 |
| 批作业已失败（FAILED） | ❌ | 同上 |

不过**实践中一般不建议对批任务用 stop**，原因：

1. 批任务正常靠「跑完自停」收尾，要中止时直接 `cancel` 更直接，没必要绕 stop 取 checkpoint；
2. 批处理默认不开 checkpoint（需 `execution.checkpointing.interval` 等配置），否则停下来的点位没有可恢复数据，
   留了点也没用；
3. `withDrain`（stop-with-drain）对批处理无意义——drain 是等流式 source 排空缓冲，批任务应传 `false`
   或直接 cancel。

所以这套方法是**为流任务设计的**；批任务仅在「想中途打断并保留断点」这一少数场景下才值得用它。

#### 批任务如何停止？

批任务直接取消即可，不需要 stop/checkpoint 那套流程，用专门的取消方法：

```java
// 取消一个正在运行的批任务（走集群 REST PATCH /jobs/{jobId}，不产生恢复点）
SqlGatewayClient.cancelJob("http://host:8081", jobId);
```

仅对 `RUNNING` 状态的作业生效；已跑完（FINISHED）/失败（FAILED）的作业会返回错误。

### 9. 原始逃生口

未建模的端点用 `getJson()`，它返回解析后的 `Map`（这是唯一会看到裸 JSON 的地方）：

```java
Map<String, Object> overview = new SqlGatewayClient("http://localhost:8081").getJson("/jobs/overview");
```

### 10. 集群 REST 鉴权（用户名 / 密码）

Flink 的 JobManager REST **原生不支持**用户名/密码这种 Basic Auth——要么裸奔，要么靠前置反代
（如 nginx 加 basic auth）或 Flink 自身的安全机制（Kerberos、SSL）来保护。一旦你的集群前置了
Basic Auth，**本客户端已经能透传凭据**：把 `user` / `password` 直接写进集群 REST 地址即可，
所有作业状态、日志、`stop`、`cancel` 请求都会自动带上 `Authorization` 头（和网关写法完全一致）。

```java
// 集群 REST 地址同样支持 ?user= / ?password=，与网关等价
String rest = "http://flink01:8081?user=admin&password=secret";

SqlGatewayClient.fetchJobStatus(rest, jobId);            // 带 Basic Auth
SqlGatewayClient.fetchJobManagerLog(rest);               // 带 Basic Auth
SqlGatewayClient.fetchTaskManagerLogsForJob(rest, jobId);// 带 Basic Auth
SqlGatewayClient.cancelJob(rest, jobId);                 // 带 Basic Auth
```

鉴权失败时（凭据错、或没带凭据但服务端要求）这些请求会抛 `IOException`，message 带状态码与
响应体；`ping()` / `testConnection()` 只对 SQL Gateway 有效，不能用来探集群 REST
（见「Flink 2.x 行为差异」）。

**注意**：给集群"加上" Basic Auth 是**服务端 / 运维**配置（反代或 Flink 安全），不是这个 jar 能做的；
jar 这边的职责只是把地址里的 `user` / `password` 转成 Basic Auth 头发出去。

## Model 一览（`com.easy.etl.flink.http.model`）

| Model | 内容 |
|---|---|
| `GatewayInfo` | `productName`、`version` |
| `SessionInfo` | `sessionHandle` |
| `OperationInfo` | `operationHandle` |
| `OperationStatus` | `status` + `isFinished/isError/isTerminal/isFailure` |
| `ResultPage` | `type`（`ResultType` 枚举）、`resultKind`、`data`、`nextResultUri`、`nextToken` |
| `ResultSetData` | `columns`（`List<Column>`）、`rows`（`List<Row>`）、`columnNames()` |
| `Column` | `name`、`logicalType`（原始 JSON 文本） |
| `Row` | `values`、`get(i)`、`toMap(columns)`、`toMap("a","b")` |
| `GatewayResult` | `columns`、`rows`、`resultKind`、`isDml`、`getUpdateCount`、`getJobId`、`toMaps/toMap` |
| `JobExceptions` | `entries`（`JobExceptionEntry`：`root`/`taskName`/`timestamp`，Flink 2.x 另有 `exceptionName`/`stackTrace`）、`roots()` |
| `JobStatus` | 作业运行状态：`jobId`/`name`/`state`、时间戳、`isRunning/isActive/isTerminal/isFailed/isCanceled`、`getJobState()`/`getStateCode()` |
| `JobState`（`com.easy.etl.flink.http.em` 包） | 状态编码枚举：`1 INITIALIZING`、`2 CREATED`、`3 RUNNING`、`4 FAILING`、`-1 FAILED`、`5 CANCELLING`、`6 CANCELED`、`7 FINISHED`、`8 RESTARTING`、`9 SUSPENDED`、`10 RECONCILING`、`-10 UNKNOWN`；`getCode/getState/getDescription/isTerminal/isSettled/fromState/fromCode` |
| `ClusterLog` / `OperationLog` | `lines` / `getFirstLine()` |
| `OpenSessionRequest` / `ExecuteStatementRequest` | 请求体，实现 `JsonBody.toJson()` |
| `StopResult` | stop 结果：`requestId`、`status`、`location`（checkpoint 路径）、`isCompleted/isFailed` |

## 错误处理

| 异常 | 场景 |
|---|---|
| `IOException` | 网络/HTTP 层失败（含 4xx/5xx，message 带状态码与响应体） |
| `GatewayExecutionException` | operation 以 ERROR/CANCELED/CLOSED/TIMEOUT 结束，或超过客户端超时；message 是网关的真实错误文本 |
| `IllegalArgumentException` | 地址/参数不合法（如 `rowFormat` 不是 table/json） |

提示：出错时 HTTP 状态码通常仍是 `2xx`，错误详情在**结果负载**里（`resultKind` 为 `ERROR`），
所以要看 operation 状态 + 结果内容，别只看状态码。

## 注意事项

- **会话不会自动回收**：用完 `closeSession()`；游标必须 `close()`（或 try-with-resources），
  否则服务端 operation 一直占着。`SqlGatewayClient` 本身无可变共享状态，可复用；一个游标
  只能由单个线程顺序读取。
- `rowFormat=table` 会把所有值序列化成字符串，超过 2^53 的 BIGINT 转 double 会丢精度；
  需要精确数值时用 `rowFormat=json`（数值落成 `Long` / `BigDecimal`）。
- `fetchOperationLog` 从 `token = 0` 开始按页读取，**会消耗该 operation 的结果**：
  同一个 operation 不要既读日志又读数据。operation 关闭后就不能再读了：先取日志，再关闭。
- 集群 REST（8081）与 SQL Gateway（8083）是**两个服务**，别混用；需要鉴权时同样在地址里
  带 `user` / `password`。
- **`operationTimeoutMs` 默认是 `0`（不限时）**：作业提交后被卡在等待资源（槽位不足、
  YARN/K8s 排队）时，`execute()` / `submit()` 会**一直阻塞**，不会报错。生产环境建议显式
  设置一个上限（例如 `?operationTimeoutMs=600000`），别依赖默认值。
- 依赖 jar（`flink-connector-jdbc`、`mysql-connector-java` …）要放在 Flink 集群的 `lib/`
  或 `pipeline.jars`，否则 DDL 阶段就报 `Could not find any factory for identifier 'jdbc'`。

## Flink 2.x 上的行为差异（按 2.2.1 实测）

网关的 v1 端点整体是稳定的，但这几处 2.x 的形态变化会影响客户端行为，已在实现里兼容：

| 项 | Flink 1.18 / 1.19 | Flink 2.x |
|---|---|---|
| 关闭 operation | `DELETE …/operations/{op}` | `DELETE …/operations/{op}/close`（客户端两种都试，遇到 404 自动回退） |
| 结果行的 `rowFormat` 查询参数 | 生效 | **`/v1/...` 结果接口不解析该参数，恒返回 JSON**；只有不带版本前缀的接口才生效。客户端仍会发出该参数，并把取值映射成网关认识的 `PLAIN_TEXT` / `JSON` |
| 失败 operation 的错误文本 | 结果负载里带 `ERROR` 说明 | `GET …/result/{token}` 直接回 **HTTP 500 + `{"errors":[...]}`**，根因在响应体里；客户端会从错误响应体里还原根因 |
| 作业 ID | DML 结果列 `job id` | 页顶层多一个 `jobID` 字段，`getJobId()` 优先用它，取不到再回退到列。注意**批模式 SELECT 也会带 `jobID`**（它同样被当成一个作业跑），不要用 `getJobId() == null` 来区分 SELECT 与 DML |
| DML 的标记列 | 首列 `row count` | 批模式仍是 `row count`，流模式变成了 **`job id`**；`isDml()` 两种拼写都认，只认 `row count` 的实现会让流式 INSERT 的 `isDml()` 永远为 false |
| 取消作业 | `PATCH /jobs/{id}` | 仍然**只有** `PATCH /jobs/{id}`：`POST /jobs/{id}/cancel` 与 `DELETE /jobs/{id}` 实测都是 404。JDK 8 的 `HttpURLConnection` 会直接拒绝 PATCH（`ProtocolException: Invalid HTTP method: PATCH`），所以这一路请求由 hutool 发出，对调用方透明 |
| 日志端点的 `?size=N` | 生效 | **被忽略**：`GET /jobmanager/log?size=5` 与不带参数返回的行数完全一致（都是整份日志）。限流请在客户端做，用 `ClusterLog#tail(n)` |
| 异常历史条目 | `root` / `taskName` | `exceptionName` / `stacktrace`（没有 `root`，客户端取堆栈首行作为根因） |
| stop 结果 | `{"location":"…"}` | `{"status":{"id":"COMPLETED"},"operation":{"location":"…"}}` |
| 集群 REST 探活 | – | JobManager REST（8081）没有 `/v1/info`，所以 `ping()` / `testConnection()` **只适用于 SQL Gateway**，不要拿来探集群 |

多语句在两端都会被拒绝，只是拒绝**是异步的**：`POST …/statements` 依然返回 200 +
`operationHandle`，随后 operation 变成 `ERROR`。所以判断成败要看 operation 状态，
而不是看提交语句那一次的 HTTP 状态码。

## 构建

```bash
mvn -o clean package       # 编译 + 跑测试 + 打 jar
```
