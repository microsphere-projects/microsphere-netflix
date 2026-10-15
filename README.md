# Microsphere Netflix

> Microsphere Projects for Netflix

[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/microsphere-projects/microsphere-netflix)
[![Maven Build](https://github.com/microsphere-projects/microsphere-netflix/actions/workflows/maven-build.yml/badge.svg)](https://github.com/microsphere-projects/microsphere-netflix/actions/workflows/maven-build.yml)
[![Codecov](https://codecov.io/gh/microsphere-projects/microsphere-netflix/branch/main/graph/badge.svg)](https://app.codecov.io/gh/microsphere-projects/microsphere-netflix)
![Maven](https://img.shields.io/maven-central/v/io.github.microsphere-projects/microsphere-netflix.svg)
![License](https://img.shields.io/github/license/microsphere-projects/microsphere-netflix.svg)

The microsphere-netflix project is a Java-based integration framework that provides enhanced functionality for Netflix
OSS components, specifically focusing on Eureka service discovery with clustering capabilities and advanced client
features. The project extends the standard Netflix Eureka implementation with production-ready enhancements including
high availability server clustering via Apache Tomcat, enhanced client metadata synchronization, and multiple client
management capabilities.

## Modules

| **Module**       | **Artifact ID**                                    | **Purpose**                                                                 |
|------------------|----------------------------------------------------|-----------------------------------------------------------------------------|
| Parent POM       | **microsphere-netflix-parent**                     | Provides common configuration and dependency management for all modules     |
| Dependencies BOM | **microsphere-netflix-dependencies**               | Centralized dependency version management using BOM pattern                 |
| Core Client      | **microsphere-netflix-eureka-client-core**         | Foundation Eureka client functionality independent of Spring Cloud          |
| Enhanced Client  | **microsphere-netflix-eureka-client-spring-cloud** | Spring Cloud integration with enhanced features and multiple client support |
| Clustered Server | **microsphere-netflix-eureka-server-spring-cloud** | High availability Eureka server with Tomcat clustering capabilities         |

## Getting Started

The easiest way to get started is by adding the Microsphere Netflix BOM (Bill of Materials) to your project's pom.xml:

```xml

<dependencyManagement>
    <dependencies>
        ...
        <!-- Microsphere Netflix Dependencies -->
        <dependency>
            <groupId>io.github.microsphere-projects</groupId>
            <artifactId>microsphere-netflix-dependencies</artifactId>
            <version>${microsphere-netflix.version}</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
        ...
    </dependencies>
</dependencyManagement>
```

`${microsphere-netflix.version}` has two branches:

| **Branches** | **Purpose**                                      | **Latest Version** |
|--------------|--------------------------------------------------|--------------------|
| **main**     | Compatible with Spring Cloud 2022.0.x - 2025.0.x | `0.2.0`            |
| **1.x**      | Compatible with Spring Cloud Hoxton - 2021.0.x   | `0.1.0`            |

## Building from Source

You don't need to build from source unless you want to try out the latest code or contribute to the project.

To build the project, follow these steps:

1. Clone the repository:

```bash
git clone https://github.com/microsphere-projects/microsphere-netflix.git
```

2. Build the source:

- Linux/MacOS:

```bash
./mvnw package
```

- Windows:

```powershell
mvnw.cmd package
```

## Contributing

We welcome your contributions! Please read [Code of Conduct](./CODE_OF_CONDUCT.md) before submitting a pull request.

## Reporting Issues

* Before you log a bug, please search the [issues](https://github.com/microsphere-projects/microsphere-netflix/issues)
  to see if someone has already reported the problem.
* If the issue doesn't already
  exist, [create a new issue](https://github.com/microsphere-projects/microsphere-netflix/issues/new).
* Please provide as much information as possible with the issue report.

## Documentation

Documentation hub: [docs/README.md](docs/README.md) (中文：[docs/zh/README.md](docs/zh/README.md))

### User Guide

| Branch Line | English | 中文 |
|-------------|---------|------|
| `release` (current, 0.2.x) | [docs/current/user-guide](docs/current/user-guide/README.md) | [docs/zh/current/user-guide](docs/zh/current/user-guide/README.md) |
| `release-1.x` (1.x) | [docs/1.x/user-guide](docs/1.x/user-guide/README.md) | [docs/zh/1.x/user-guide](docs/zh/1.x/user-guide/README.md) |

### Developer Guide

| Branch Line | English | 中文 |
|-------------|---------|------|
| `release` (current, 0.2.x) | [docs/current/developer-guide](docs/current/developer-guide/README.md) | [docs/zh/current/developer-guide](docs/zh/current/developer-guide/README.md) |
| `release-1.x` (1.x) | [docs/1.x/developer-guide](docs/1.x/developer-guide/README.md) | [docs/zh/1.x/developer-guide](docs/zh/1.x/developer-guide/README.md) |

### Online Documentation

[DeepWiki Host](https://deepwiki.com/microsphere-projects/microsphere-netflix)

### Wiki

[Github Host](https://github.com/microsphere-projects/microsphere-netflix/wiki)

### JavaDoc

TODO

## License

The Microsphere Spring is released under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).