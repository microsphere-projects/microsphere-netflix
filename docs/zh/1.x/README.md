# `1.x` 发布版文档（`release-1.x` 分支）

本目录记录 **`release-1.x`** 分支的代码——即 microsphere-netflix 的维护版本线。

## 版本事实

| 项目 | 值 |
|------|-------|
| 源码分支 | `release-1.x` |
| 已发布版本线 | `0.1.x`（最新已发布版本为 `0.1.0`），声明兼容 **Spring Cloud Hoxton – 2021.0.x** |
| 分支 POM（本次分析的快照） | `0.2.0-SNAPSHOT`、`java.version=17`、`microsphere-spring-cloud.version=0.2.25` |
| 快照中的 Servlet API | `jakarta.servlet` |

> 说明：本次分析的 `release-1.x` 分支快照仍然携带现代版本线的 POM 元数据
> （`0.2.0-SNAPSHOT` / Java 17 / jakarta API），而已发布的 `0.1.x` 构件面向的是较旧的
> Spring Cloud 版本区间。对 1.x 版本线真正有意义的结构性差异——模块命名、包结构
> 与监听器实现——均按分支上的现状如实记录。

## 模块

| 模块 | 类型 | 用途 |
|--------|------|---------|
| `microsphere-netflix-parent` | POM | 通用构建配置 |
| `microsphere-netflix-dependencies` | BOM | 管理 `eureka-commons`、`eureka-client-core`、`eureka-client-spring-cloud`、`eureka-server-spring-cloud` |
| `microsphere-netflix-eureka-commons` | JAR | 配置属性常量 |
| `microsphere-netflix-eureka-client-core` | JAR | 核心客户端增强，不依赖 Spring（等价于 `current` 线的 `eureka-client`） |
| `microsphere-netflix-eureka-client-spring-cloud` | JAR | 客户端自动配置 |
| `microsphere-netflix-eureka-server-spring-cloud` | JAR | Eureka Server 的 Tomcat 集群复制 |

该版本线**没有** `microsphere-netflix-eureka-server` 可执行模块；应用需要自行提供
`@EnableEurekaServer` 引导类（该模块的测试内置了一个示例）。

## 与 `current` 的差异

| 方面 | `release-1.x` | `release`（`current`） |
|------|---------------|------------------------|
| 不依赖 Spring 的客户端模块 | `microsphere-netflix-eureka-client-core` | `microsphere-netflix-eureka-client` |
| Server 包名 | `io.microsphere.netflix.eureka.spring.cloud.*` | `io.microsphere.netflix.eureka.server.spring.cloud.*` |
| 默认 server 配置文件 | 仅有代码默认值 | 额外内置 `META-INF/config/default/server.yaml` 与 `features.yaml` |
| 可执行 server | 未提供 | `microsphere-netflix-eureka-server` 模块 |
| `EnhancedEurekaClientAutoConfiguration` | 对 `Registration` 做空值检查并提前返回 | 无空值检查 |
| `EurekaServerListener` | `contextDestroyed` 直接调用 `deregister()`；codec/registry 通过私有的 `init*` 方法捕获 | `contextDestroyed` 调用 `destroy()`；codec/registry 通过包可见的静态辅助方法获取，并被测试复用 |
| `ReplicatedInstanceListener` | codec 包装器在每次解码时惰性解析；周期性清扫把方法体包裹在 `async(...)` 中；成员为 private，没有静态的 `get(ServletContext)` 访问方法 | codec 在构造函数中即时解析；清扫任务先被调度、方法体再次提交；暴露 `get(ServletContext)` 以及供测试使用的包可见 `process*` 方法 |

## 指南

- [用户指南](user-guide/README.md)
- [开发者指南](developer-guide/README.md)

## English

[docs/1.x/README.md](../../1.x/README.md)
