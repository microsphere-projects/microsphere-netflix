# User Guide — `release-1.x` Branch (1.x line)

User guide for the maintenance line of microsphere-netflix. It covers installation, APIs, configuration
properties, code examples (from the branch's test-cases), and version compatibility per Maven module.

## Which Module Do I Need?

| If you want to… | Use |
|-----------------|-----|
| Align Microsphere Netflix artifact versions | [`microsphere-netflix-dependencies` (BOM)](parent-and-dependencies.md) |
| Sync Spring `Registration` metadata into `InstanceInfo`, plug handler/listener beans | [`microsphere-netflix-eureka-client-spring-cloud`](eureka-client-spring-cloud.md) |
| Aggregate registry reads across multiple Eureka server URLs | [`microsphere-netflix-eureka-client-spring-cloud`](eureka-client-spring-cloud.md) |
| Replicate a Eureka Server cluster through Apache Tomcat Tribes | [`microsphere-netflix-eureka-server-spring-cloud`](eureka-server-spring-cloud.md) |
| Use `CompositePreRegistrationHandler` without Spring | [`microsphere-netflix-eureka-client-core`](eureka-client-core.md) |
| Reference property-name constants | [`microsphere-netflix-eureka-commons`](eureka-commons.md) |

## Quick Start

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

## Version and Compatibility

| Component | Supported Range (published `0.1.x` line) |
|-----------|------------------------------------------|
| Spring Cloud | Hoxton – 2021.0.x (Spring Boot 2.x) |
| JDK | 8 / 11 for published artifacts; branch snapshot POM targets Java 17 |
| Netflix Eureka client/server | as managed by the Spring Cloud Netflix release train |

> The `release-1.x` branch currently carries an in-progress snapshot (`0.2.0-SNAPSHOT`, jakarta APIs) —
> see the [branch README](../README.md) note before consuming branch builds. For the modern line use
> the [current user guide](../../current/user-guide/README.md).

## Feature Parity With `current`

| Feature | Available in 1.x |
|---------|------------------|
| Metadata synchronization | Yes (`EnhancedEurekaClientAutoConfiguration`, plus a null-registration guard) |
| Multiple Eureka clients | Yes (identical implementation) |
| `PreRegistrationHandler`/`EurekaEventListener` bean collection | Yes |
| Netflix → Spring event propagation | Yes |
| Tomcat cluster replication | Yes (package differs: `io.microsphere.netflix.eureka.spring.cloud`) |
| Executable server bootstrap | No — write your own `@EnableEurekaServer` class |

## Documents

| Document | Contents |
|----------|----------|
| [parent-and-dependencies.md](parent-and-dependencies.md) | Parent POM and BOM |
| [eureka-commons.md](eureka-commons.md) | Property constants |
| [eureka-client-core.md](eureka-client-core.md) | `CompositePreRegistrationHandler` |
| [eureka-client-spring-cloud.md](eureka-client-spring-cloud.md) | Client auto-configurations |
| [eureka-server-spring-cloud.md](eureka-server-spring-cloud.md) | Server replication auto-configuration |

## 中文

[docs/zh/1.x/user-guide/README.md](../../zh/1.x/user-guide/README.md)
