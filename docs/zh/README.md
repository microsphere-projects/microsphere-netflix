# Microsphere Netflix 文档

欢迎来到 [microsphere-netflix](https://github.com/microsphere-projects/microsphere-netflix) 的文档中心。
这是一个 Java 集成框架，为 Netflix OSS Eureka 增强了生产级特性：基于 Tomcat 的服务端集群与实例复制、
增强的客户端元数据同步，以及多 Eureka 客户端管理。

> English documentation: [../README.md](../README.md)。

## 文档结构

文档按发布分支组织。每个分支目录都包含一份**用户指南**（安装、API、配置属性、代码示例、版本兼容性）
和一份**开发者指南**（架构设计、源码分析、设计模式、流程图）。

| 分支目录 | 源码分支 | 版本线 | 兼容性 | 用户指南 | 开发者指南 |
|----------|----------|--------|--------|----------|------------|
| [`current/`](current/README.md) | `release` | `0.2.x` | Spring Cloud 2022.0.x – 2025.0.x，Java 17 | [用户指南](current/user-guide/README.md) | [开发者指南](current/developer-guide/README.md) |
| [`1.x/`](1.x/README.md) | `release-1.x` | `0.1.x` | Spring Cloud Hoxton – 2021.0.x（已发布版本线；所分析分支的快照详见 [1.x README](1.x/README.md)） | [用户指南](1.x/user-guide/README.md) | [开发者指南](1.x/developer-guide/README.md) |

## 覆盖的模块

| 模块 | Artifact ID | 引入于 | 文档位置 |
|------|-------------|--------|----------|
| Parent POM | `microsphere-netflix-parent` | 两条版本线 | [current](current/user-guide/parent-and-dependencies.md) / [1.x](1.x/user-guide/parent-and-dependencies.md) |
| Dependencies BOM | `microsphere-netflix-dependencies` | 两条版本线 | [current](current/user-guide/parent-and-dependencies.md) / [1.x](1.x/user-guide/parent-and-dependencies.md) |
| Eureka Commons | `microsphere-netflix-eureka-commons` | 两条版本线 | [current](current/user-guide/eureka-commons.md) / [1.x](1.x/user-guide/eureka-commons.md) |
| Eureka Client | `microsphere-netflix-eureka-client` | 仅 `current` | [current](current/user-guide/eureka-client.md) |
| Eureka Client Core | `microsphere-netflix-eureka-client-core` | 仅 `1.x` | [1.x](1.x/user-guide/eureka-client-core.md) |
| Eureka Client Spring Cloud | `microsphere-netflix-eureka-client-spring-cloud` | 两条版本线 | [current](current/user-guide/eureka-client-spring-cloud.md) / [1.x](1.x/user-guide/eureka-client-spring-cloud.md) |
| Eureka Server Spring Cloud | `microsphere-netflix-eureka-server-spring-cloud` | 两条版本线 | [current](current/user-guide/eureka-server-spring-cloud.md) / [1.x](1.x/user-guide/eureka-server-spring-cloud.md) |
| Eureka Server Bootstrap | `microsphere-netflix-eureka-server` | 仅 `current` | [current](current/user-guide/eureka-server.md) |

## 两条版本线之间的主要差异

- `release`（`current`）将 `microsphere-netflix-eureka-client-core` 重命名为 `microsphere-netflix-eureka-client`，
  并新增可执行的 `microsphere-netflix-eureka-server` 模块。
- `release-1.x` 将服务端自动配置类保留在 `io.microsphere.netflix.eureka.spring.cloud` 包下，而 `release` 将其
  迁移至 `io.microsphere.netflix.eureka.server.spring.cloud`。
- `release` 依赖 Microsphere Spring Cloud `0.2.26`；`release-1.x` 依赖 `0.2.25`。

## 许可证

Apache License 2.0。
