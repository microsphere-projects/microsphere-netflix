# Eureka Server Spring Cloud (`1.x`)

Module: `microsphere-netflix-eureka-server-spring-cloud` (branch `release-1.x`) · Since: `1.0.0` ·
Package root: `io.microsphere.netflix.eureka.spring.cloud` (note: no `.server.` segment on this branch)

## Purpose

Same Tomcat Tribes cluster replication for Eureka Server as the
[current-line module](../../current/user-guide/eureka-server-spring-cloud.md): instance Register/Cancel/Heartbeat
broadcasts converge registries across nodes, with proactive self-deregistration on shutdown.

```xml
<dependency>
    <groupId>io.github.microsphere-projects</groupId>
    <artifactId>microsphere-netflix-eureka-server-spring-cloud</artifactId>
</dependency>
```

The JAR excludes `spring-boot-starter-tomcat` from `spring-cloud-starter-netflix-eureka-server` and adds
optional `tomcat-catalina`, `tomcat-tribes`, `tomcat-catalina-ha`; your application must keep an embedded
Tomcat on the classpath for clustering to activate.

## Configuration Properties

`EurekaServerProperties` binds `microsphere.eureka.server.*` with identical names and code defaults:

| Property | Default |
|----------|---------|
| `microsphere.eureka.server.enabled` | `false` in code (unset); no shipped default YAML on this branch |
| `microsphere.eureka.server.replication.instance-name-prefix` | `ReplicatedInstance-` |
| `microsphere.eureka.server.replication.timeout` | `15000` |
| `microsphere.eureka.server.replication.metadata.action-key` | `_action_` |
| `microsphere.eureka.server.replication.threads.number` | `2` |
| `microsphere.eureka.server.replication.threads.thread-name-prefix` | `Eureka-Server-Replication-Thread-` |
| `microsphere.eureka.server.deregistration.delay` | `5000` |

Tomcat cluster placeholders in the shipped `META-INF/conf/cluster.xml` (`microsphere.tomcat.cluster.*`:
multicast address `228.0.0.4`, port `45564`, frequency `500`, drop-time `3000`; receiver address `auto`,
port `4000`, auto-bind `100`, selector-timeout `5000`, max-threads `6`) are the same as the current line.

Unlike `release`, this branch does **not** ship `META-INF/config/default/server.yaml` or `features.yaml`,
so property values come from code defaults or your own configuration.

## Bootstrapping a Server

There is no executable bootstrap module on this line. Use the sample pattern from the module test —
`io.microsphere.netflix.eureka.spring.cloud.tomcat.sample.EurekaServerApplication`:

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

`@ServletComponentScan` matters on this branch when deploying as a WAR: it activates `@WebListener`-style
registration paths; the branch tests include a `src/test/webapp` skeleton (`WEB-INF/web.xml` with
`<distributable/>`, `META-INF/context.xml` declaring
`className="org.apache.catalina.ha.context.ReplicatedContext"`) demonstrating external-Tomcat deployment.

## Test-Case Example (application.yaml)

The module's `src/test/resources/application.yaml` shows the minimal server config:

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

## Behavior Differences vs. `current`

| Area | 1.x behavior |
|------|--------------|
| Auto-configuration registration | `AutoConfiguration.imports` present; a legacy empty `META-INF/spring.factories` remains as scaffolding |
| `EurekaServerListener` shutdown | `contextDestroyed` invokes `deregister()` directly (idempotent via a private `volatile` flag) |
| `ReplicatedInstanceListener` codec | resolved lazily per decode from `EurekaServerContext` (null-safe), not captured at construction |
| Periodic sweep | scheduled task body is wrapped in `async(...)` submission instead of executing inline |
| Test visibility | processing members are private; no static `get(ServletContext)` accessor |

Functional replication semantics (action metadata `_action_`, prefix `ReplicatedInstance-`, heartbeat→register
promotion, idempotent cancel, `isReplication` loop-cut) are unchanged.

## Version and Compatibility

Published `0.1.x` artifacts target Spring Cloud Hoxton – 2021.0.x / Spring Boot 2.x; the analyzed branch
snapshot uses `jakarta.servlet` and Java 17 POM metadata — see the [branch README](../README.md) note.

## 中文

[docs/zh/1.x/user-guide/eureka-server-spring-cloud.md](../../zh/1.x/user-guide/eureka-server-spring-cloud.md)
