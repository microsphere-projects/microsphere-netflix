# Parent POM 与依赖 BOM

模块：`microsphere-netflix-parent`、`microsphere-netflix-dependencies`
GroupId：`io.github.microsphere-projects` · 版本：`0.2.0`（`0.2.x` 版本线） · 打包方式：`pom`

## microsphere-netflix-parent

Parent POM 用于以一致的方式构建本仓库的每个模块。它继承自
`microsphere-spring-cloud-parent`，并锁定 Microsphere 生态的各版本：

| 属性 | 值 | 含义 |
|------|-----|------|
| `revision` | `0.2.0-SNAPSHOT` | 通过 Maven CI 友好版本（CI-friendly versions）设置项目版本 |
| `java.version` | `17` | 编译器与字节码目标 |
| `microsphere-spring-cloud.version` | `0.2.26` | 导入 `microsphere-spring-cloud-dependencies`（Spring Boot / Spring Cloud / Netflix Eureka 版本） |
| `microsphere-bom.version` | `0.3.9` | 导入 `microsphere-javaee-bom` 与 `microsphere-tomcat-bom`（Jakarta Servlet、Tomcat Tribes / Catalina-HA 版本） |

使用者**不应**将本 POM 作为自己的父工程；它仅供仓库自身的构建使用
（flatten-maven-plugin 会在 install/deploy 时解析 `${revision}`）。

## microsphere-netflix-dependencies（BOM）

导入此 BOM，即可用一个坐标对齐所有 Microsphere Netflix 构件的版本：

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

### 受管构件

| 构件 | 典型使用者 |
|------|------------|
| `microsphere-netflix-eureka-commons` | 需要属性常量的应用 |
| `microsphere-netflix-eureka-client` | 纯（非 Spring）Eureka 客户端应用 |
| `microsphere-netflix-eureka-client-spring-cloud` | 注册到 Eureka 的 Spring Boot 服务 |
| `microsphere-netflix-eureka-server-spring-cloud` | 带有 Tomcat 复制功能的 Eureka Server 应用 |

> `microsphere-netflix-eureka-server` 是一个可执行应用，而非库；因此有意
> 不纳入 BOM 管理。请直接运行它（参见 [eureka-server.md](eureka-server.md)）。

## 版本兼容性

| BOM 版本 | 依赖要求 |
|----------|----------|
| `0.2.x` | Java 17、Spring Cloud 2022.0.x – 2025.0.x、Spring Boot 3.x |
| `0.1.x` | 参见 [1.x parent-and-dependencies 指南](../../1.x/user-guide/parent-and-dependencies.md) |

由于 BOM 自身继承了 `microsphere-spring-cloud-dependencies`，在 `<dependencyManagement>` 中
**于 Spring Cloud BOM 之后**导入 Microsphere Netflix BOM，即可保证 Netflix Eureka
与 Tomcat 版本的一致性。

## English

[docs/current/user-guide/parent-and-dependencies.md](../../../current/user-guide/parent-and-dependencies.md)
