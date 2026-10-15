# 用户指南 —— `release-1.x` 分支（1.x 版本线）

microsphere-netflix 维护版本线的用户指南。内容涵盖安装、API、配置属性、代码示例（来自分支的测试用例），
以及各 Maven 模块的版本兼容性。

## 我该使用哪个模块？

| 如果你想…… | 使用 |
|-----------------|-----|
| 统一 Microsphere Netflix 各构件的版本号 | [`microsphere-netflix-dependencies`（BOM）](parent-and-dependencies.md) |
| 将 Spring `Registration` 元数据同步进 `InstanceInfo`，挂载 handler/listener Bean | [`microsphere-netflix-eureka-client-spring-cloud`](eureka-client-spring-cloud.md) |
| 跨多个 Eureka server URL 聚合注册表读取 | [`microsphere-netflix-eureka-client-spring-cloud`](eureka-client-spring-cloud.md) |
| 通过 Apache Tomcat Tribes 复制 Eureka Server 集群 | [`microsphere-netflix-eureka-server-spring-cloud`](eureka-server-spring-cloud.md) |
| 在不使用 Spring 的情况下使用 `CompositePreRegistrationHandler` | [`microsphere-netflix-eureka-client-core`](eureka-client-core.md) |
| 引用属性名常量 | [`microsphere-netflix-eureka-commons`](eureka-commons.md) |

## 快速开始

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>io.github.microsphere-projects</groupId>
            <artifactId>microsphere-netflix-dependencies</artifactId>
            <version>0.1.0</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>

<dependencies>
    <!-- choose client or server enhancement -->
    <dependency>
        <groupId>io.github.microsphere-projects</groupId>
        <artifactId>microsphere-netflix-eureka-client-spring-cloud</artifactId>
    </dependency>
</dependencies>
```

## 版本与兼容性

| 组件 | 支持范围（已发布的 `0.1.x` 版本线） |
|-----------|------------------------------------------|
| Spring Cloud | Hoxton – 2021.0.x（Spring Boot 2.x） |
| JDK | 已发布构件为 8 / 11；分支快照 POM 的编译目标为 Java 17 |
| Netflix Eureka 客户端/服务端 | 由 Spring Cloud Netflix 版本列车（release train）统一管理 |

> `release-1.x` 分支目前处于进行中的快照状态（`0.2.0-SNAPSHOT`、jakarta API）——
> 在引入分支构建产物之前，请先阅读[分支 README](../README.md)中的说明。现代版本线请使用
> [current 用户指南](../../current/user-guide/README.md)。

## 与 `current` 的功能对齐

| 功能 | 1.x 是否提供 |
|---------|------------------|
| 元数据同步 | 提供（`EnhancedEurekaClientAutoConfiguration`，并额外带有 registration 空值保护） |
| 多个 Eureka 客户端 | 提供（实现完全一致） |
| `PreRegistrationHandler`/`EurekaEventListener` Bean 收集 | 提供 |
| Netflix → Spring 事件传播 | 提供 |
| Tomcat 集群复制 | 提供（包名不同：`io.microsphere.netflix.eureka.spring.cloud`） |
| 可执行服务端引导 | 不提供 —— 需自行编写 `@EnableEurekaServer` 类 |

## 文档

| 文档 | 内容 |
|----------|----------|
| [parent-and-dependencies.md](parent-and-dependencies.md) | 父 POM 与 BOM |
| [eureka-commons.md](eureka-commons.md) | 属性常量 |
| [eureka-client-core.md](eureka-client-core.md) | `CompositePreRegistrationHandler` |
| [eureka-client-spring-cloud.md](eureka-client-spring-cloud.md) | 客户端自动配置 |
| [eureka-server-spring-cloud.md](eureka-server-spring-cloud.md) | 服务端复制自动配置 |

## English

[docs/1.x/user-guide/README.md](../../../1.x/user-guide/README.md)
