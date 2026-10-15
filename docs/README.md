# Microsphere Netflix Documentation

Welcome to the documentation hub of [microsphere-netflix](https://github.com/microsphere-projects/microsphere-netflix),
a Java integration framework that enhances Netflix OSS Eureka with production-ready features: Tomcat-based server
clustering and instance replication, enhanced client metadata synchronization, and multiple Eureka client management.

> 中文文档请见 [docs/zh/README.md](zh/README.md)。

## Documentation Structure

Documentation is organized per release branch. Each branch directory contains a **User Guide** (installation, APIs,
configuration properties, code examples, version compatibility) and a **Developer Guide** (architecture design,
source analysis, design patterns, flow diagrams).

| Branch Directory | Source Branch | Version Line | Compatibility | User Guide | Developer Guide |
|------------------|---------------|--------------|---------------|------------|-----------------|
| [`current/`](current/README.md) | `release` | `0.2.x` | Spring Cloud 2022.0.x – 2025.0.x, Java 17 | [User Guide](current/user-guide/README.md) | [Developer Guide](current/developer-guide/README.md) |
| [`1.x/`](1.x/README.md) | `release-1.x` | `0.1.x` | Spring Cloud Hoxton – 2021.0.x (published line; see [1.x README](1.x/README.md) for the analyzed branch snapshot) | [User Guide](1.x/user-guide/README.md) | [Developer Guide](1.x/developer-guide/README.md) |

## Modules Covered

| Module | Artifact ID | Introduced In | Documented At |
|--------|-------------|---------------|---------------|
| Parent POM | `microsphere-netflix-parent` | Both lines | [current](current/user-guide/parent-and-dependencies.md) / [1.x](1.x/user-guide/parent-and-dependencies.md) |
| Dependencies BOM | `microsphere-netflix-dependencies` | Both lines | [current](current/user-guide/parent-and-dependencies.md) / [1.x](1.x/user-guide/parent-and-dependencies.md) |
| Eureka Commons | `microsphere-netflix-eureka-commons` | Both lines | [current](current/user-guide/eureka-commons.md) / [1.x](1.x/user-guide/eureka-commons.md) |
| Eureka Client | `microsphere-netflix-eureka-client` | `current` only | [current](current/user-guide/eureka-client.md) |
| Eureka Client Core | `microsphere-netflix-eureka-client-core` | `1.x` only | [1.x](1.x/user-guide/eureka-client-core.md) |
| Eureka Client Spring Cloud | `microsphere-netflix-eureka-client-spring-cloud` | Both lines | [current](current/user-guide/eureka-client-spring-cloud.md) / [1.x](1.x/user-guide/eureka-client-spring-cloud.md) |
| Eureka Server Spring Cloud | `microsphere-netflix-eureka-server-spring-cloud` | Both lines | [current](current/user-guide/eureka-server-spring-cloud.md) / [1.x](1.x/user-guide/eureka-server-spring-cloud.md) |
| Eureka Server Bootstrap | `microsphere-netflix-eureka-server` | `current` only | [current](current/user-guide/eureka-server.md) |

## Key Differences Between the Two Lines

- `release` (`current`) renames `microsphere-netflix-eureka-client-core` to `microsphere-netflix-eureka-client`, and
  adds the executable `microsphere-netflix-eureka-server` module.
- `release-1.x` keeps the server auto-configuration classes under the
  `io.microsphere.netflix.eureka.spring.cloud` package, while `release` moves them to
  `io.microsphere.netflix.eureka.server.spring.cloud`.
- `release` depends on Microsphere Spring Cloud `0.2.26`; `release-1.x` on `0.2.25`.

## License

Apache License 2.0.
