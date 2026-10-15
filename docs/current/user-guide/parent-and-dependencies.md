# Parent POM and Dependencies BOM

Modules: `microsphere-netflix-parent`, `microsphere-netflix-dependencies`
GroupId: `io.github.microsphere-projects` · Version: `0.2.0` (`0.2.x` line) · Packaging: `pom`

## microsphere-netflix-parent

The parent POM builds every module of this repository consistently. It inherits from
`microsphere-spring-cloud-parent` and pins the Microsphere ecosystem versions:

| Property | Value | Meaning |
|----------|-------|---------|
| `revision` | `0.2.0-SNAPSHOT` | Project version via Maven CI-friendly versions |
| `java.version` | `17` | Compiler and bytecode target |
| `microsphere-spring-cloud.version` | `0.2.26` | Imports `microsphere-spring-cloud-dependencies` (Spring Boot / Spring Cloud / Netflix Eureka versions) |
| `microsphere-bom.version` | `0.3.9` | Imports `microsphere-javaee-bom` and `microsphere-tomcat-bom` (Jakarta Servlet, Tomcat Tribes / Catalina-HA versions) |

Consumers should **not** use this POM as their parent; it is for the repository build itself
(flatten-maven-plugin resolves `${revision}` on install/deploy).

## microsphere-netflix-dependencies (BOM)

Import this BOM to align all Microsphere Netflix artifact versions with one coordinate:

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

### Managed Artifacts

| Artifact | Typical Consumer |
|----------|------------------|
| `microsphere-netflix-eureka-commons` | Applications that need the property constants |
| `microsphere-netflix-eureka-client` | Plain (non-Spring) Eureka client applications |
| `microsphere-netflix-eureka-client-spring-cloud` | Spring Boot services registered to Eureka |
| `microsphere-netflix-eureka-server-spring-cloud` | Eureka Server applications with Tomcat replication |

> `microsphere-netflix-eureka-server` is an executable application, not a library; it is intentionally
> not managed by the BOM. Run it directly (see [eureka-server.md](eureka-server.md)).

## Version Compatibility

| BOM version | Requires |
|-------------|----------|
| `0.2.x` | Java 17, Spring Cloud 2022.0.x – 2025.0.x, Spring Boot 3.x |
| `0.1.x` | See [1.x parent-and-dependencies guide](../../1.x/user-guide/parent-and-dependencies.md) |

Because the BOM itself inherits `microsphere-spring-cloud-dependencies`, importing the Microsphere
Netflix BOM **after** the Spring Cloud BOM in `<dependencyManagement>` guarantees consistent
Netflix Eureka and Tomcat versions.

## 中文

[docs/zh/current/user-guide/parent-and-dependencies.md](../../zh/current/user-guide/parent-and-dependencies.md)
