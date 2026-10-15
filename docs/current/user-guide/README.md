# User Guide — `release` Branch (0.2.x)

This user guide explains how to install and use each Microsphere Netflix module in the current release line.
Every Maven module has a dedicated document with API explanations, configuration properties, code examples taken
from the project's test-cases, and version compatibility.

## Which Module Do I Need?

| If you want to… | Use |
|-----------------|-----|
| Manage versions of Microsphere Netflix artifacts in one place | [`microsphere-netflix-dependencies` (BOM)](parent-and-dependencies.md) |
| Register your Spring Boot application with Eureka and sync Spring `Registration` metadata into `InstanceInfo` | [`microsphere-netflix-eureka-client-spring-cloud`](eureka-client-spring-cloud.md) |
| Aggregate discovery results from multiple Eureka server zones/URLs | [`microsphere-netflix-eureka-client-spring-cloud`](eureka-client-spring-cloud.md#2-multiple-eureka-clients) |
| Run a highly-available, replicated Eureka Server cluster without Eureka peer-to-peer replication | [`microsphere-netflix-eureka-server-spring-cloud`](eureka-server-spring-cloud.md) |
| Start a standalone Eureka Server immediately | [`microsphere-netflix-eureka-server`](eureka-server.md) |
| Contribute `PreRegistrationHandler` beans in a plain (non-Spring) Eureka client app | [`microsphere-netflix-eureka-client`](eureka-client.md) |
| Reference property names/constants in code | [`microsphere-netflix-eureka-commons`](eureka-commons.md) |

## Quick Start

1. Import the BOM:

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

2. Add the enhanced client to a Spring Boot service:

```xml
<dependency>
    <groupId>io.github.microsphere-projects</groupId>
    <artifactId>microsphere-netflix-eureka-client-spring-cloud</artifactId>
</dependency>
```

3. Or add the clustered server to a Eureka Server application:

```xml
<dependency>
    <groupId>io.github.microsphere-projects</groupId>
    <artifactId>microsphere-netflix-eureka-server-spring-cloud</artifactId>
</dependency>
```

Both modules are fully auto-configured: no `@Import` or extra annotation is required beyond
`@EnableEurekaClient` / `@EnableEurekaServer` from Spring Cloud Netflix.

## Version and Compatibility

| Component | Supported Range |
|-----------|-----------------|
| JDK | 17+ |
| Spring Boot | 3.x (managed by `microsphere-spring-cloud-dependencies` `0.2.26`) |
| Spring Cloud | 2022.0.x – 2025.0.x |
| Spring Cloud Netflix (Eureka client/server starters) | As provided by the Spring Cloud release train |
| Apache Tomcat (embedded, clustering) | 10.1+ (`jakarta.servlet` namespace, `tomcat-tribes` / `tomcat-catalina-ha`) |
| Microsphere projects | `microsphere-java` 2.x line, `microsphere-spring-*` `0.2.x` |

For the older compatibility line (Spring Cloud Hoxton – 2021.0.x), see the
[1.x documentation](../../1.x/user-guide/README.md).

## Documents

| Document | Contents |
|----------|----------|
| [parent-and-dependencies.md](parent-and-dependencies.md) | Parent POM and BOM usage, managed artifacts |
| [eureka-commons.md](eureka-commons.md) | Configuration property constants API |
| [eureka-client.md](eureka-client.md) | `CompositePreRegistrationHandler` API and examples |
| [eureka-client-spring-cloud.md](eureka-client-spring-cloud.md) | Auto-configuration APIs, properties, examples |
| [eureka-server-spring-cloud.md](eureka-server-spring-cloud.md) | Cluster replication properties, Tomcat cluster tuning, examples |
| [eureka-server.md](eureka-server.md) | Executable server bootstrap, default configuration |

## 中文

[docs/zh/current/user-guide/README.md](../../zh/current/user-guide/README.md)
