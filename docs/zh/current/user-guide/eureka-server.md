# Eureka Server（可执行引导）

模块：`microsphere-netflix-eureka-server`
GroupId：`io.github.microsphere-projects` · 仅存在于 `2.0` 版本线（`0.2.x`） · 包：`io.microsphere.netflix.eureka.server.bootstrap`

## 用途

一个开箱即用的 Eureka Server 应用，它打包了
[`microsphere-netflix-eureka-server-spring-cloud`](eureka-server-spring-cloud.md)，
即一个可执行的 Spring Boot JAR，其服务实例通过 Apache Tomcat 集群进行复制。

`1.x` 版本线不提供此模块；请自行构建引导类（参见
[1.x 指南](../../1.x/user-guide/README.md)）。

## 使用方式

### 通过 Maven 直接运行

```bash
mvn -pl microsphere-netflix-eureka-server spring-boot:run
```

### 运行打包后的 JAR

```bash
java -jar microsphere-netflix-eureka-server/target/microsphere-netflix-eureka-server-0.2.0.jar
```

### 主类

`io.microsphere.netflix.eureka.server.bootstrap.EurekaServerApplication` —— 标准的 Spring Boot WAR
部署模式（`SpringBootServletInitializer` + `@EnableEurekaServer` + `@EnableAutoConfiguration`）：

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

## 默认配置

随附的 `application.yaml`：

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

可通过命令行属性覆盖任意配置，例如：

```bash
java -jar microsphere-netflix-eureka-server.jar --server.port=8761 \
     --microsphere.tomcat.cluster.transport.receiver.port=4001
```

## 启动一个集群

在不同端口上启动多个实例；Tomcat Tribes 多播成员发现（默认为 `228.0.0.4:45564`）会自动发现对等节点，
并且每个实例都会向自身内嵌的注册表注册（`defaultZone` 指向自己）。实例注册表的内容会通过复制通道收敛
—— 请按随附配置设置 `eureka.client.fetch-registry=false`，因为复制机制会提供完整的注册表。

测试 `EurekaServerApplicationTest` 给出了最小化调用方式：

```java
@Test
void test() {
    main(new String[0]);   // boots with application.yaml above
}
```

## 版本与兼容性

| 模块版本 | 依赖要求 |
|----------|----------|
| `0.2.0` | Java 17、Spring Boot 3.x、Spring Cloud 2022.0.x – 2025.0.x |

## English

[docs/current/user-guide/eureka-server.md](../../../current/user-guide/eureka-server.md)
