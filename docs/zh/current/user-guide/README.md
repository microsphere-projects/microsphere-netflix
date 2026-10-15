# 用户指南 —— `release` 分支（0.2.x）

本用户指南说明如何在当前发布版本线中安装和使用每个 Microsphere Netflix 模块。
每个 Maven 模块都有一份专属文档，包含 API 说明、配置属性、取自项目测试用例的代码示例，以及版本兼容性。

## 我需要哪个模块？

| 如果你希望…… | 使用 |
|--------------|------|
| 集中统一管理 Microsphere Netflix 各构件的版本 | [`microsphere-netflix-dependencies` (BOM)](parent-and-dependencies.md) |
| 将你的 Spring Boot 应用注册到 Eureka，并把 Spring `Registration` 元数据同步进 `InstanceInfo` | [`microsphere-netflix-eureka-client-spring-cloud`](eureka-client-spring-cloud.md) |
| 聚合来自多个 Eureka 服务端可用区/URL 的服务发现结果 | [`microsphere-netflix-eureka-client-spring-cloud`](eureka-client-spring-cloud.md#2-多-eureka-客户端) |
| 在不使用 Eureka 点对点复制的情况下，运行一个高可用、可复制的 Eureka Server 集群 | [`microsphere-netflix-eureka-server-spring-cloud`](eureka-server-spring-cloud.md) |
| 立即启动一个独立的 Eureka Server | [`microsphere-netflix-eureka-server`](eureka-server.md) |
| 在纯（非 Spring）Eureka 客户端应用中贡献 `PreRegistrationHandler` bean | [`microsphere-netflix-eureka-client`](eureka-client.md) |
| 在代码中引用属性名/常量 | [`microsphere-netflix-eureka-commons`](eureka-commons.md) |

## 快速开始

1. 导入 BOM：

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>io.github.microsphere-projects</groupId>
            <artifactId>microsphere-netflix-dependencies</artifactId>
            <version>0.2.0</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>
```

2. 将增强客户端加入某个 Spring Boot 服务：

```xml
<dependency>
    <groupId>io.github.microsphere-projects</groupId>
    <artifactId>microsphere-netflix-eureka-client-spring-cloud</artifactId>
</dependency>
```

3. 或者将集群化服务端加入某个 Eureka Server 应用：

```xml
<dependency>
    <groupId>io.github.microsphere-projects</groupId>
    <artifactId>microsphere-netflix-eureka-server-spring-cloud</artifactId>
</dependency>
```

这两个模块都会自动完成全部配置：除了 Spring Cloud Netflix 的
`@EnableEurekaClient` / `@EnableEurekaServer` 之外，无需 `@Import` 或额外注解。

## 版本与兼容性

| 组件 | 支持范围 |
|------|----------|
| JDK | 17+ |
| Spring Boot | 3.x（由 `microsphere-spring-cloud-dependencies` `0.2.26` 管理） |
| Spring Cloud | 2022.0.x – 2025.0.x |
| Spring Cloud Netflix（Eureka 客户端/服务端 starter） | 由 Spring Cloud 发布火车提供 |
| Apache Tomcat（内嵌、集群） | 10.1+（`jakarta.servlet` 命名空间，`tomcat-tribes` / `tomcat-catalina-ha`） |
| Microsphere 系列项目 | `microsphere-java` 2.x 版本线，`microsphere-spring-*` `0.2.x` |

若需要了解较早的兼容版本线（Spring Cloud Hoxton – 2021.0.x），请参见
[1.x 文档](../../1.x/user-guide/README.md)。

## 文档

| 文档 | 内容 |
|------|------|
| [parent-and-dependencies.md](parent-and-dependencies.md) | Parent POM 与 BOM 的用法、受管构件 |
| [eureka-commons.md](eureka-commons.md) | 配置属性常量 API |
| [eureka-client.md](eureka-client.md) | `CompositePreRegistrationHandler` API 与示例 |
| [eureka-client-spring-cloud.md](eureka-client-spring-cloud.md) | 自动配置 API、属性、示例 |
| [eureka-server-spring-cloud.md](eureka-server-spring-cloud.md) | 集群复制属性、Tomcat 集群调优、示例 |
| [eureka-server.md](eureka-server.md) | 可执行服务端引导、默认配置 |

## English

[docs/current/user-guide/README.md](../../../current/user-guide/README.md)
