# 父 POM 与依赖 BOM（`1.x`）

模块：`microsphere-netflix-parent`、`microsphere-netflix-dependencies`（分支 `release-1.x`）

## 父 POM 属性

| 属性 | 值 | 含义 |
|----------|-------|---------|
| `revision` | `0.2.0-SNAPSHOT` | 分支快照标识（已发布的 1.x 版本线为 `0.1.x`） |
| `java.version` | `17` | 分支快照的编译目标 |
| `microsphere-spring-cloud.version` | `0.2.25` | 导入 `microsphere-spring-cloud-dependencies` |
| `microsphere-bom.version` | `0.3.9` | `microsphere-javaee-bom`、`microsphere-tomcat-bom` |

该版本比 `release` 分支低一个小版本（`0.2.26`）——差异细节参见
[current 父 POM 指南](../../current/user-guide/parent-and-dependencies.md)。

## BOM 管理的构件

`microsphere-netflix-dependencies` 在该分支上管理四个构件：

| 构件 | 说明 |
|----------|------|
| `microsphere-netflix-eureka-commons` | 与 current 版本线相同 |
| `microsphere-netflix-eureka-client-core` | 替代 current 的 `microsphere-netflix-eureka-client` |
| `microsphere-netflix-eureka-client-spring-cloud` | 依赖 `eureka-client-core` |
| `microsphere-netflix-eureka-server-spring-cloud` | 依赖 `eureka-client-spring-cloud` |

使用方式：

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
```

## English

[docs/1.x/user-guide/parent-and-dependencies.md](../../../1.x/user-guide/parent-and-dependencies.md)
