# Eureka Server (Executable Bootstrap)

Module: `microsphere-netflix-eureka-server`
GroupId: `io.github.microsphere-projects` · Since: `2.0` line (`0.2.x`) only · Package: `io.microsphere.netflix.eureka.server.bootstrap`

## Purpose

A ready-to-run Eureka Server application that bundles
[`microsphere-netflix-eureka-server-spring-cloud`](eureka-server-spring-cloud.md), i.e. an executable
Spring Boot JAR whose server instances replicate through an Apache Tomcat cluster.

The `1.x` line does not ship this module; build your own bootstrap class instead (see the
[1.x guide](../../1.x/user-guide/README.md)).

## Usage

### Run directly with Maven

```bash
mvn -pl microsphere-netflix-eureka-server spring-boot:run
```

### Run the packaged JAR

```bash
java -jar microsphere-netflix-eureka-server/target/microsphere-netflix-eureka-server-0.2.0.jar
```

### Main class

`io.microsphere.netflix.eureka.server.bootstrap.EurekaServerApplication` — the standard Spring Boot WAR
deployment pattern (`SpringBootServletInitializer` + `@EnableEurekaServer` + `@EnableAutoConfiguration`):

```java
@EnableEurekaServer
@EnableAutoConfiguration
public class EurekaServerApplication extends SpringBootServletInitializer {

    public static void main(String[] args) {
        SpringApplication.run(EurekaServerApplication.class, args);
    }

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder builder) {
        builder.sources(EurekaServerApplication.class);
        return builder;
    }
}
```

## Default Configuration

Shipped `application.yaml`:

```yaml
spring:
  application:
    name: microsphere-eureka-server

server:
  port: 8080               # Eureka port
  shutdown: GRACEFUL       # triggers self-deregistration on SIGTERM

eureka:
  client:
    service-url:
      defaultZone: http://127.0.0.1:${server.port}/eureka  # register self
    fetch-registry: false                                 # do not fetch from self
  instance:
    instanceId: ${spring.cloud.client.ip-address}:${server.port}

management:
  metrics:
    tags:
      application: ${spring.application.name}
```

Override anything with command-line properties, e.g.:

```bash
java -jar microsphere-netflix-eureka-server.jar --server.port=8761 \
     --microsphere.tomcat.cluster.transport.receiver.port=4001
```

## Starting a Cluster

Start several instances on different ports; Tomcat Tribes multicast membership
(`228.0.0.4:45564` by default) discovers peers automatically, and each instance registers itself with
its own embedded registry (`defaultZone` points at itself). Instance registry content converges through
the replication channel — set `eureka.client.fetch-registry=false` as shipped, since replication supplies
the full registry.

The test `EurekaServerApplicationTest` shows the minimal invocation:

```java
@Test
void test() {
    main(new String[0]);   // boots with application.yaml above
}
```

## Version and Compatibility

| Module Version | Requires |
|----------------|----------|
| `0.2.0` | Java 17, Spring Boot 3.x, Spring Cloud 2022.0.x – 2025.0.x |

## 中文

[docs/zh/current/user-guide/eureka-server.md](../../zh/current/user-guide/eureka-server.md)
