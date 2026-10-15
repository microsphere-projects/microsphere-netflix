# Eureka Server Spring Cloud (Tomcat Cluster Replication)

Module: `microsphere-netflix-eureka-server-spring-cloud`
GroupId: `io.github.microsphere-projects` · Since: `1.0.0` · Package root: `io.microsphere.netflix.eureka.server.spring.cloud`

## Purpose

Turns a Spring Cloud Netflix **Eureka Server** into a self-healing cluster using **Apache Tomcat Tribes**
clustering instead of (or alongside) Eureka's native peer-to-peer replication:

- Instance registrations, cancellations, and renewals on one server are broadcast to all cluster members as
  JSON payloads over the Tomcat cluster channel.
- On shutdown, the server **deregisters itself proactively** (graceful shutdown support) before the expiry
  scanner would expire it.
- A scheduled sweep re-broadcasts the local registry so late-joining members converge.

```xml
<dependency>
    <groupId>io.github.microsphere-projects</groupId>
    <artifactId>microsphere-netflix-eureka-server-spring-cloud</artifactId>
</dependency>
```

No extra annotation is required: adding the JAR to a `@EnableEurekaServer` application activates
`TomcatEurekaServerReplicationAutoConfiguration` for servlet-based Spring Boot applications.

## Configuration Properties

### Application properties (`microsphere.eureka.server.*`)

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `microsphere.eureka.server.enabled` | `boolean` | `true` (in shipped `META-INF/config/default/server.yaml`) | Master switch for server replication features |
| `microsphere.eureka.server.replication.instance-name-prefix` | `String` | `ReplicatedInstance-` | Prefix used to mark replicated instance attributes on the `ServletContext` |
| `microsphere.eureka.server.replication.timeout` | `int` (ms) | `15000` | Replicated-map send timeout |
| `microsphere.eureka.server.replication.metadata.action-key` | `String` | `_action_` | `InstanceInfo` metadata key carrying the action (`Register`/`Cancel`/`Heartbeat`) |
| `microsphere.eureka.server.replication.threads.number` | `int` | `2` | Size of the replication thread pool |
| `microsphere.eureka.server.replication.threads.thread-name-prefix` | `String` | `Eureka-Server-Replication-Thread-` | Thread naming prefix |
| `microsphere.eureka.server.deregistration.delay` | `long` (ms) | `5000` | Wait after broadcasting the self-cancel on shutdown |

### Tomcat cluster properties (`microsphere.tomcat.cluster.*`)

These placeholders are resolved inside the shipped `META-INF/conf/cluster.xml` before Tomcat's digester parses it:

| Property | Default | Description |
|----------|---------|-------------|
| `microsphere.tomcat.cluster.membership.multicast.address` | `228.0.0.4` | Multicast group address |
| `microsphere.tomcat.cluster.membership.multicast.port` | `45564` | Multicast group port |
| `microsphere.tomcat.cluster.membership.multicast.frequency` | `500` | Membership announcement interval (ms) |
| `microsphere.tomcat.cluster.membership.multicast.drop-time` | `3000` | Member drop time (ms) |
| `microsphere.tomcat.cluster.transport.receiver.address` | `auto` | Receiver bind address |
| `microsphere.tomcat.cluster.transport.receiver.port` | `4000` | Base receiver port |
| `microsphere.tomcat.cluster.transport.receiver.auto-bind` | `100` | Port auto-bind range |
| `microsphere.tomcat.cluster.transport.receiver.selector-timeout` | `5000` | Selector timeout (ms) |
| `microsphere.tomcat.cluster.transport.receiver.max-threads` | `6` | Receiver thread count |

## Code Example — two-node cluster test

From `TomcatEurekaServerReplicationAutoConfigurationTest`: start two Eureka servers in the same JVM
(distinct ports) and verify both registries see both self-registered instances:

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

### Minimal `application.yaml` for one node

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

## Behavior Notes

- Replication rides the Tomcat cluster channel; multicast must be usable between nodes (or adjust
  `cluster.xml` membership for your environment).
- Actions are deduplicated: events already flagged `isReplication()` are not re-broadcast.
- A `Heartbeat` for an unknown instance is promoted to a `Register` on the receiving node.
- Cancels for unknown instances are ignored (idempotent).
- The module excludes `spring-boot-starter-tomcat` from `spring-cloud-starter-netflix-eureka-server`
  and adds `tomcat-tribes` / `tomcat-catalina-ha` as optional dependencies; keep embedded Tomcat on the
  classpath (default Boot starter) for clustering to activate.

## Version and Compatibility

| Module Version | Spring Boot | Spring Cloud | Tomcat | Servlet API |
|----------------|-------------|--------------|--------|-------------|
| `0.2.x` (this document) | 3.x | 2022.0.x – 2025.0.x | 10.1+ | `jakarta.servlet` |
| `0.1.x` | 2.x | Hoxton – 2021.0.x | 8.5/9 | `javax.servlet` |

## 中文

[docs/zh/current/user-guide/eureka-server-spring-cloud.md](../../zh/current/user-guide/eureka-server-spring-cloud.md)
