# Eureka Server Spring Cloud（Tomcat 集群复制）

模块：`microsphere-netflix-eureka-server-spring-cloud`
GroupId：`io.github.microsphere-projects` · 自：`1.0.0` · 包根：`io.microsphere.netflix.eureka.server.spring.cloud`

## 用途

使用 **Apache Tomcat Tribes** 集群（取代或与 Eureka 原生的点对点复制并存），将 Spring Cloud Netflix
的 **Eureka Server** 变成一个自愈合集群：

- 一台服务上的实例注册、取消注册和续约，会以 JSON 载荷的形式通过 Tomcat 集群通道广播给所有集群成员。
- 关闭时，服务会在过期扫描器将其判定过期之前**主动注销自身**（优雅停机支持）。
- 一个定时清理任务会重新广播本地注册表，以便后加入的成员能够收敛一致。

```xml
<dependency>
    <groupId>io.github.microsphere-projects</groupId>
    <artifactId>microsphere-netflix-eureka-server-spring-cloud</artifactId>
</dependency>
```

无需额外注解：将该 JAR 加入一个 `@EnableEurekaServer` 应用，即可为基于 servlet 的 Spring Boot 应用
激活 `TomcatEurekaServerReplicationAutoConfiguration`。

## 配置属性

### 应用属性（`microsphere.eureka.server.*`）

| 属性 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `microsphere.eureka.server.enabled` | `boolean` | `true`（见随附的 `META-INF/config/default/server.yaml`） | 服务端复制特性的总开关 |
| `microsphere.eureka.server.replication.instance-name-prefix` | `String` | `ReplicatedInstance-` | 用于在 `ServletContext` 上标记被复制实例属性的前缀 |
| `microsphere.eureka.server.replication.timeout` | `int`（毫秒） | `15000` | 复制映射的发送超时 |
| `microsphere.eureka.server.replication.metadata.action-key` | `String` | `_action_` | 承载动作（`Register`/`Cancel`/`Heartbeat`）的 `InstanceInfo` 元数据键 |
| `microsphere.eureka.server.replication.threads.number` | `int` | `2` | 复制线程池的大小 |
| `microsphere.eureka.server.replication.threads.thread-name-prefix` | `String` | `Eureka-Server-Replication-Thread-` | 线程命名前缀 |
| `microsphere.eureka.server.deregistration.delay` | `long`（毫秒） | `5000` | 关闭时广播自身取消注册之后的等待时间 |

### Tomcat 集群属性（`microsphere.tomcat.cluster.*`）

这些占位符会在 Tomcat 的 digester 解析随附的 `META-INF/conf/cluster.xml` 之前于其中被解析：

| 属性 | 默认值 | 说明 |
|------|--------|------|
| `microsphere.tomcat.cluster.membership.multicast.address` | `228.0.0.4` | 多播组地址 |
| `microsphere.tomcat.cluster.membership.multicast.port` | `45564` | 多播组端口 |
| `microsphere.tomcat.cluster.membership.multicast.frequency` | `500` | 成员宣告间隔（毫秒） |
| `microsphere.tomcat.cluster.membership.multicast.drop-time` | `3000` | 成员判定掉线时间（毫秒） |
| `microsphere.tomcat.cluster.transport.receiver.address` | `auto` | 接收器绑定地址 |
| `microsphere.tomcat.cluster.transport.receiver.port` | `4000` | 接收器基准端口 |
| `microsphere.tomcat.cluster.transport.receiver.auto-bind` | `100` | 端口自动绑定范围 |
| `microsphere.tomcat.cluster.transport.receiver.selector-timeout` | `5000` | Selector 超时（毫秒） |
| `microsphere.tomcat.cluster.transport.receiver.max-threads` | `6` | 接收器线程数 |

## 代码示例 —— 双节点集群测试

取自 `TomcatEurekaServerReplicationAutoConfigurationTest`：在同一 JVM 中以不同端口启动两台 Eureka 服务，
并验证两个注册表都能看到两个自注册的实例：

```java
@EnableEurekaServer
@EnableAutoConfiguration
class ClusterTest {

    @Test
    void replication() throws Throwable {
        int count = 2;
        for (int i = 0; i < count; i++) {
            final int port = 10000 + i;
            executor.submit(() -> run(ClusterTest.class, "--server.port=" + port));
        }
        // Each server registers itself ("eureka-server"); wait for convergence
        for (...) {
            Application app = registry.getApplication("EUREKA-SERVER");
            if (app.getInstances().size() == count) break;  // replicated via Tomcat cluster
            sleep(500L);
        }
    }
}
```

### 单节点的最小 `application.yaml`

```yaml
spring:
  application:
    name: eureka-server
server:
  port: 12345
  shutdown: GRACEFUL            # lets EurekaServerListener run before exit
eureka:
  client:
    service-url:
      defaultZone: http://127.0.0.1:${server.port}/eureka
    fetch-registry: false       # registry is supplied by cluster replication instead
  instance:
    instanceId: ${spring.cloud.client.ip-address}:${server.port}
microsphere:
  eureka:
    server:
      deregistration:
        delay: 500              # test value; default 5000
```

## 行为说明

- 复制依赖 Tomcat 集群通道；节点之间必须能够使用多播（否则需针对你的环境调整
  `cluster.xml` 的成员发现配置）。
- 动作会去重：已被标记 `isReplication()` 的事件不会再被重新广播。
- 对于未知实例的 `Heartbeat`，在接收节点上会被提升为 `Register`。
- 对于未知实例的取消（Cancel）会被忽略（幂等）。
- 本模块从 `spring-cloud-starter-netflix-eureka-server` 中排除了 `spring-boot-starter-tomcat`，
  并将 `tomcat-tribes` / `tomcat-catalina-ha` 作为可选依赖加入；请保持内嵌 Tomcat 位于
  类路径上（默认的 Boot starter），集群功能才会激活。

## 版本与兼容性

| 模块版本 | Spring Boot | Spring Cloud | Tomcat | Servlet API |
|----------|-------------|--------------|--------|-------------|
| `0.2.x`（本文档） | 3.x | 2022.0.x – 2025.0.x | 10.1+ | `jakarta.servlet` |
| `0.1.x` | 2.x | Hoxton – 2021.0.x | 8.5/9 | `javax.servlet` |

## English

[docs/current/user-guide/eureka-server-spring-cloud.md](../../../current/user-guide/eureka-server-spring-cloud.md)
