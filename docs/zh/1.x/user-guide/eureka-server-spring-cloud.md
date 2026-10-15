# Eureka Server Spring Cloud（`1.x`）

模块：`microsphere-netflix-eureka-server-spring-cloud`（分支 `release-1.x`）· 起始版本：`1.0.0` ·
包根：`io.microsphere.netflix.eureka.spring.cloud`（注意：该分支的包名中没有 `.server.` 层级）

## 用途

与现代版本线中的 [current 模块](../../current/user-guide/eureka-server-spring-cloud.md) 一样，
基于 Tomcat Tribes 为 Eureka Server 提供集群复制：实例的 Register/Cancel/Heartbeat 广播在各节点之间
收敛注册表，并在关闭时主动注销自身。

```xml
<dependency>
    <groupId>io.github.microsphere-projects</groupId>
    <artifactId>microsphere-netflix-eureka-server-spring-cloud</artifactId>
</dependency>
```

该 JAR 从 `spring-cloud-starter-netflix-eureka-server` 中排除了 `spring-boot-starter-tomcat`，并加入
可选的 `tomcat-catalina`、`tomcat-tribes`、`tomcat-catalina-ha`；你的应用必须在 classpath 中保留内嵌
Tomcat，集群复制才会生效。

## 配置属性

`EurekaServerProperties` 绑定 `microsphere.eureka.server.*`，属性名与代码默认值完全一致：

| 属性 | 默认值 |
|----------|---------|
| `microsphere.eureka.server.enabled` | 代码中为 `false`（未设置）；该分支没有内置的默认 YAML |
| `microsphere.eureka.server.replication.instance-name-prefix` | `ReplicatedInstance-` |
| `microsphere.eureka.server.replication.timeout` | `15000` |
| `microsphere.eureka.server.replication.metadata.action-key` | `_action_` |
| `microsphere.eureka.server.replication.threads.number` | `2` |
| `microsphere.eureka.server.replication.threads.thread-name-prefix` | `Eureka-Server-Replication-Thread-` |
| `microsphere.eureka.server.deregistration.delay` | `5000` |

内置的 `META-INF/conf/cluster.xml` 中的 Tomcat 集群占位符（`microsphere.tomcat.cluster.*`：
组播地址 `228.0.0.4`、端口 `45564`、频率 `500`、drop-time `3000`；接收器地址 `auto`、
端口 `4000`、auto-bind `100`、selector-timeout `5000`、max-threads `6`）与 current 版本线相同。

与 `release` 不同，该分支**不**内置 `META-INF/config/default/server.yaml` 或 `features.yaml`，
因此属性值来自代码默认值或你自己的配置。

## 引导服务端

该版本线没有可执行的引导模块。可以沿用模块测试中的示例模式 ——
`io.microsphere.netflix.eureka.spring.cloud.tomcat.sample.EurekaServerApplication`：

```java
@EnableEurekaServer
@EnableScheduling
@EnableAutoConfiguration
@ServletComponentScan
public class EurekaServerApplication extends SpringBootServletInitializer {

    public static void main(String[] args) {
        SpringApplication.run(EurekaServerApplication.class, args);
    }

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder builder) {
        builder.sources(EurekaServerApplication.class);
        return builder;
    }
}
```

在以 WAR 形式部署时，该分支上的 `@ServletComponentScan` 很关键：它会激活 `@WebListener` 风格的
注册路径；分支测试中还包含一个 `src/test/webapp` 骨架（带 `<distributable/>` 的 `WEB-INF/web.xml`，
以及声明 `className="org.apache.catalina.ha.context.ReplicatedContext"` 的
`META-INF/context.xml`），用于演示在外部 Tomcat 中部署。

## 测试用例示例（application.yaml）

模块的 `src/test/resources/application.yaml` 给出了最小化的服务端配置：

```yaml
spring:
  application:
    name: eureka-server
server:
  port: 12345
  shutdown: GRACEFUL
eureka:
  client:
    service-url:
      defaultZone: http://127.0.0.1:${server.port}/eureka
    fetch-registry: false
  instance:
    instanceId: ${spring.cloud.client.ip-address}:${server.port}
```

## 与 `current` 的行为差异

| 方面 | 1.x 行为 |
|------|--------------|
| 自动配置注册 | 存在 `AutoConfiguration.imports`；同时保留了一个遗留的空 `META-INF/spring.factories` 作为脚手架 |
| `EurekaServerListener` 关闭 | `contextDestroyed` 直接调用 `deregister()`（借助私有的 `volatile` 标志保证幂等） |
| `ReplicatedInstanceListener` 的 codec | 每次解码时从 `EurekaServerContext` 惰性解析（空值安全），而不是在构造时捕获 |
| 周期性清扫 | 定时任务的方法体被包裹在 `async(...)` 提交中，而不是原地执行 |
| 测试可见性 | 处理方法为 private；没有静态的 `get(ServletContext)` 访问方法 |

复制的功能语义保持不变（action 元数据 `_action_`、前缀 `ReplicatedInstance-`、
heartbeat→register 升级、幂等 cancel、`isReplication` 环路截断）。

## 版本与兼容性

已发布的 `0.1.x` 构件面向 Spring Cloud Hoxton – 2021.0.x / Spring Boot 2.x；本次分析的分支快照
使用 `jakarta.servlet` 以及 Java 17 的 POM 元数据——参见[分支 README](../README.md)中的说明。

## English

[docs/1.x/user-guide/eureka-server-spring-cloud.md](../../../1.x/user-guide/eureka-server-spring-cloud.md)
