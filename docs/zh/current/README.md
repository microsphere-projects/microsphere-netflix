# 当前发布版文档（`release` 分支）

本目录记录 **`release`** 分支的代码——即 microsphere-netflix 的当前主线。

## 版本信息

| 项目 | 值 |
|------|-----|
| 源码分支 | `release` |
| Maven groupId | `io.github.microsphere-projects` |
| 版本线 | `0.2.x`（分支 POM 采用 `0.2.0-SNAPSHOT`；最新已发布版本为 `0.2.0`） |
| Java | 17 |
| Spring Cloud | 2022.0.x – 2025.0.x |
| Spring Boot | 3.x（由 `microsphere-spring-cloud-dependencies` 间接管理） |
| Microsphere Spring Cloud | `0.2.26` |
| Microsphere BOM（`microsphere-javaee-bom`、`microsphere-tomcat-bom`） | `0.3.9` |
| Netflix Eureka 客户端/服务端 | 由 Spring Cloud Netflix 发布火车（release train）管理 |

## 模块

| 模块 | 类型 | 用途 |
|------|------|------|
| `microsphere-netflix-parent` | POM | 通用构建配置，导入 Microsphere 依赖 BOM |
| `microsphere-netflix-dependencies` | BOM | 为所有 Microsphere Netflix 构件统一管理版本 |
| `microsphere-netflix-eureka-commons` | JAR | 共享的配置属性常量 |
| `microsphere-netflix-eureka-client` | JAR | 核心 Eureka 客户端增强（不依赖 Spring） |
| `microsphere-netflix-eureka-client-spring-cloud` | JAR | Spring Cloud 自动配置：元数据同步、多客户端、事件传播 |
| `microsphere-netflix-eureka-server-spring-cloud` | JAR | Spring Cloud 自动配置：为 Eureka Server 提供 Tomcat 集群复制 |
| `microsphere-netflix-eureka-server` | 可执行 JAR | 开箱即用的 Eureka Server 引导应用 |

## 指南

- [用户指南](user-guide/README.md) —— 安装、API、配置属性、代码示例
- [开发者指南](developer-guide/README.md) —— 架构、源码分析、设计模式、流程

## English

[docs/current/README.md](../../current/README.md)
