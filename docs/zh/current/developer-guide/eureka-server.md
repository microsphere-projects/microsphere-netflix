# Eureka Server（可执行引导模块）—— 开发者指南

模块：`microsphere-netflix-eureka-server` · 包：`io.microsphere.netflix.eureka.server.bootstrap`

## 架构定位

轻薄的分发层，刻意不含任何逻辑：它把带集群能力的服务端自动配置模块打包成一个可执行的 Spring Boot 应用，
使用户无需编写代码就能运行一个支持复制的 Eureka 服务端。

## 源码分析

`EurekaServerApplication`：

- `@EnableEurekaServer`——导入 Spring Cloud Netflix 的 `EurekaServerAutoConfiguration`
  （marker bean、注册表、jersey 资源）。
- `@EnableAutoConfiguration`——从所依赖模块的 `AutoConfiguration.imports` 中拉起
  `TomcatEurekaServerReplicationAutoConfiguration`。
- 继承 `SpringBootServletInitializer`，并在 `configure(builder)` 中注册本类——同时支持
  `java -jar` 与部署为 WAR 放入外部 Tomcat（经典的 Eureka 集群部署方式）。

构建插件：

| 插件 | 用途 |
|--------|---------|
| `spring-boot-maven-plugin` | 重新打包为可执行的 fat JAR |
| `maven-deploy-plugin`（`skip=true`） | 该产物是一个应用，没有需要发布到 Central 的内容 |

## 内置配置的设计

`application.yaml` 中固化了[用户指南](../user-guide/eureka-server.md)所讨论的、面向集群场景的默认配置：
优雅停机（以便 `EurekaServerListener.deregister()` 在退出前执行）、以 `fetch-registry=false` 自注册
（对端注册表内容由集群通道而非 HTTP 轮询提供），以及 `instanceId` = `ip:port`，
从而保证同一台主机上的两个节点依然可区分。

`logback.xml` 为独立运行提供控制台日志的默认配置。

测试 `EurekaServerApplicationTest` 通过 `main(new String[0])` 启动应用，
并验证 WAR 构建器路径返回的是同一个 `SpringApplicationBuilder`。

## English

[docs/current/developer-guide/eureka-server.md](../../../current/developer-guide/eureka-server.md)
