# `1.x` Release Documentation (`release-1.x` branch)

This directory documents the code of the **`release-1.x`** branch — the maintenance line of microsphere-netflix.

## Version Facts

| Item | Value |
|------|-------|
| Source branch | `release-1.x` |
| Published version line | `0.1.x` (latest published release `0.1.0`), declared compatible with **Spring Cloud Hoxton – 2021.0.x** |
| Branch POM (analyzed snapshot) | `0.2.0-SNAPSHOT`, `java.version=17`, `microsphere-spring-cloud.version=0.2.25` |
| Servlet API in snapshot | `jakarta.servlet` |

> Note: the `release-1.x` branch snapshot analyzed here still carries modern-line POM metadata
> (`0.2.0-SNAPSHOT` / Java 17 / jakarta APIs) while the published `0.1.x` artifacts target the older
> Spring Cloud range. The structural differences that matter for the 1.x line — module naming, package
> layout, and listener implementation — are documented as they appear on the branch.

## Modules

| Module | Type | Purpose |
|--------|------|---------|
| `microsphere-netflix-parent` | POM | Common build configuration |
| `microsphere-netflix-dependencies` | BOM | Manages `eureka-commons`, `eureka-client-core`, `eureka-client-spring-cloud`, `eureka-server-spring-cloud` |
| `microsphere-netflix-eureka-commons` | JAR | Configuration property constants |
| `microsphere-netflix-eureka-client-core` | JAR | Core client enhancements, Spring-free (equivalent of `current`'s `eureka-client`) |
| `microsphere-netflix-eureka-client-spring-cloud` | JAR | Client auto-configurations |
| `microsphere-netflix-eureka-server-spring-cloud` | JAR | Tomcat cluster replication for Eureka Server |

There is **no** `microsphere-netflix-eureka-server` executable module on this line; applications supply
their own `@EnableEurekaServer` bootstrap class (the module test ships one as a sample).

## Differences vs. `current`

| Area | `release-1.x` | `release` (`current`) |
|------|---------------|------------------------|
| Spring-free client module | `microsphere-netflix-eureka-client-core` | `microsphere-netflix-eureka-client` |
| Server packages | `io.microsphere.netflix.eureka.spring.cloud.*` | `io.microsphere.netflix.eureka.server.spring.cloud.*` |
| Default server properties file | code defaults only | additionally ships `META-INF/config/default/server.yaml` and `features.yaml` |
| Executable server | not provided | `microsphere-netflix-eureka-server` module |
| `EnhancedEurekaClientAutoConfiguration` | null-check early return on `Registration` | no null-check |
| `EurekaServerListener` | `contextDestroyed` calls `deregister()` directly; codec/registry captured via private `init*` methods | `contextDestroyed` calls `destroy()`; codec/registry via package-visible static helpers reused by tests |
| `ReplicatedInstanceListener` | codec wrapper resolved lazily per decode; sweep wraps its body in `async(...)`; members private, no static `get(ServletContext)` accessor | codec resolved eagerly in constructor; sweep itself scheduled then body re-submitted; exposes `get(ServletContext)` and package-visible `process*` for tests |

## Guides

- [User Guide](user-guide/README.md)
- [Developer Guide](developer-guide/README.md)

## 中文

[docs/zh/1.x/README.md](../zh/1.x/README.md)
