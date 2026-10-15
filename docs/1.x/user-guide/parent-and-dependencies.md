# Parent POM and Dependencies BOM (`1.x`)

Modules: `microsphere-netflix-parent`, `microsphere-netflix-dependencies` (branch `release-1.x`)

## Parent POM Properties

| Property | Value | Meaning |
|----------|-------|---------|
| `revision` | `0.2.0-SNAPSHOT` | Branch snapshot identifier (published 1.x line is `0.1.x`) |
| `java.version` | `17` | Branch snapshot compiler target |
| `microsphere-spring-cloud.version` | `0.2.25` | Imports `microsphere-spring-cloud-dependencies` |
| `microsphere-bom.version` | `0.3.9` | `microsphere-javaee-bom`, `microsphere-tomcat-bom` |

One version lower than the `release` branch (`0.2.26`) — see
[current parent guide](../../current/user-guide/parent-and-dependencies.md) for the difference detail.

## BOM Managed Artifacts

`microsphere-netflix-dependencies` manages four artifacts on this branch:

| Artifact | Note |
|----------|------|
| `microsphere-netflix-eureka-commons` | same as current line |
| `microsphere-netflix-eureka-client-core` | replaces current's `microsphere-netflix-eureka-client` |
| `microsphere-netflix-eureka-client-spring-cloud` | depends on `eureka-client-core` |
| `microsphere-netflix-eureka-server-spring-cloud` | depends on `eureka-client-spring-cloud` |

Usage:

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

## 中文

[docs/zh/1.x/user-guide/parent-and-dependencies.md](../../zh/1.x/user-guide/parent-and-dependencies.md)
