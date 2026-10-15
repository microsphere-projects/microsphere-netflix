# Current Release Documentation (`release` branch)

This directory documents the code of the **`release`** branch — the current mainline of microsphere-netflix.

## Version Facts

| Item | Value |
|------|-------|
| Source branch | `release` |
| Maven groupId | `io.github.microsphere-projects` |
| Version line | `0.2.x` (branch POMs carry `0.2.0-SNAPSHOT`; latest published release is `0.2.0`) |
| Java | 17 |
| Spring Cloud | 2022.0.x – 2025.0.x |
| Spring Boot | 3.x (managed transitively by `microsphere-spring-cloud-dependencies`) |
| Microsphere Spring Cloud | `0.2.26` |
| Microsphere BOM (`microsphere-javaee-bom`, `microsphere-tomcat-bom`) | `0.3.9` |
| Netflix Eureka client/server | Managed by Spring Cloud Netflix release train |

## Modules

| Module | Type | Purpose |
|--------|------|---------|
| `microsphere-netflix-parent` | POM | Common build configuration, imports Microsphere dependency BOMs |
| `microsphere-netflix-dependencies` | BOM | Version management for all Microsphere Netflix artifacts |
| `microsphere-netflix-eureka-commons` | JAR | Shared configuration property constants |
| `microsphere-netflix-eureka-client` | JAR | Core Eureka client enhancements (no Spring dependency) |
| `microsphere-netflix-eureka-client-spring-cloud` | JAR | Spring Cloud auto-configuration: metadata sync, multiple clients, event propagation |
| `microsphere-netflix-eureka-server-spring-cloud` | JAR | Spring Cloud auto-configuration: Tomcat cluster replication for Eureka Server |
| `microsphere-netflix-eureka-server` | Executable JAR | Ready-to-run Eureka Server bootstrap application |

## Guides

- [User Guide](user-guide/README.md) — installation, APIs, configuration properties, code examples
- [Developer Guide](developer-guide/README.md) — architecture, source analysis, design patterns, flows

## 中文

[docs/zh/current/README.md](../zh/current/README.md)
