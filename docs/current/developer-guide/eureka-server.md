# Eureka Server (Executable Bootstrap) — Developer Guide

Module: `microsphere-netflix-eureka-server` · Package: `io.microsphere.netflix.eureka.server.bootstrap`

## Architecture Role

Thin distribution layer, deliberately logic-free: it packages the clustered-server auto-configuration module
as an executable Spring Boot application so users can run a replication-ready Eureka server without writing
code.

## Source Analysis

`EurekaServerApplication`:

- `@EnableEurekaServer` — imports Spring Cloud Netflix `EurekaServerAutoConfiguration` (marker bean, registry,
  jersey resources).
- `@EnableAutoConfiguration` — pulls in `TomcatEurekaServerReplicationAutoConfiguration` from the
  `AutoConfiguration.imports` of the dependency module.
- Extends `SpringBootServletInitializer` with `configure(builder)` registering the source class — supports
  both `java -jar` and WAR deployment into an external Tomcat (the classic Eureka clustering deployment).

Build plugins:

| Plugin | Purpose |
|--------|---------|
| `spring-boot-maven-plugin` | repackages to an executable fat JAR |
| `maven-deploy-plugin` (`skip=true`) | the artifact is an application; nothing to publish to Central |

## Shipped Configuration Design

`application.yaml` encodes the clustering-friendly defaults discussed in the
[user guide](../user-guide/eureka-server.md): graceful shutdown (so `EurekaServerListener.deregister()`
runs before exit), self-registration with `fetch-registry=false` (the cluster channel, not HTTP polling,
supplies peer registry content), and `instanceId` = `ip:port` so two nodes on one host stay distinct.

`logback.xml` provides console logging defaults for standalone runs.

The test `EurekaServerApplicationTest` boots the app with `main(new String[0])` and verifies the WAR
builder path returns the same `SpringApplicationBuilder`.

## 中文

[docs/zh/current/developer-guide/eureka-server.md](../../zh/current/developer-guide/eureka-server.md)
