# Eureka Client Spring Cloud

模块：`microsphere-netflix-eureka-client-spring-cloud`
GroupId：`io.github.microsphere-projects` · 自：`1.0.0` · 包根：`io.microsphere.netflix.eureka.client.spring.cloud`

## 用途

对 Spring Cloud Netflix 的 Eureka 客户端的即插即用增强。将该 JAR 加入类路径即可在不改动任何代码的情况下
激活三项自动配置特性：

1. **元数据同步** —— 在注册之前，将 Spring Cloud `Registration` 的元数据复制到 Netflix
   `InstanceInfo`，从而确保自定义元数据始终能送达 Eureka 服务端。
2. **多 Eureka 客户端** —— 启用后，会为每个配置的 Eureka 服务端 URL 创建一个 `DiscoveryClient`，
   而注册表查询（`getApplications`、`getInstancesByVipAddress`）会返回所有服务端的**聚合**视图。
3. **基于 bean 的扩展点** —— 你上下文中的每个 `PreRegistrationHandler` 和 `EurekaEventListener`
   bean 都会被自动接入 Netflix 客户端，且每个 Netflix `EurekaEvent`（如 `CacheRefreshedEvent`）
   都会被重新发布为 Spring 应用事件。

```xml
<dependency>
    <groupId>io.github.microsphere-projects</groupId>
    <artifactId>microsphere-netflix-eureka-client-spring-cloud</artifactId>
</dependency>
```

所有特性都是有条件的；当 `eureka.client.enabled=false`、
`microsphere.eureka.client.enabled=false`，或 Netflix/Spring Cloud 类缺失时，它们会干净地关闭。

## 配置属性

| 属性 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `microsphere.eureka.enabled` | `boolean` | `true` | Microsphere Eureka 全局开关 |
| `microsphere.eureka.client.enabled` | `boolean` | `true` | 所有客户端自动配置的开关 |
| `microsphere.eureka.client.multiple` | `boolean` | `false`（除非为 `true` 否则特性关闭） | 启用 `MultipleEurekaClientAutoConfiguration` |
| `eureka.client.serviceUrl.defaultZone` | `String`（URL 列表） | — | 逗号分隔的 URL；**当 `microsphere.eureka.client.multiple=true` 时，每个 URL 成为一个受管 `EurekaClient`** |

## 自动配置参考

通过 `META-INF/spring/...AutoConfiguration.imports`（Boot 3 风格）和
`META-INF/spring.factories`（应用上下文初始化器）注册：

| 类 | 激活条件 | 行为 |
|----|----------|------|
| `EnhancedEurekaClientAutoConfiguration` | `@ConditionalOnDiscoveryEnabled` 且 `@ConditionalOnEurekaClientAvailable`，在 Spring 的 `EurekaClientAutoConfiguration` 之后 | 监听 `RegistrationPreRegisteredEvent`；将 `MultipleRegistration` 解包为其 `EurekaRegistration`；将 `registration.getMetadata()` 合并进 `InstanceInfo.getMetadata()` |
| `MultipleEurekaClientAutoConfiguration` | 相同条件**外加** `microsphere.eureka.client.multiple` 存在 | 为每个服务 URL 构建一个 `DiscoveryClient`；在 `CloudEurekaClient` 上安装一个 AOP `PointcutAdvisor`，以在所有客户端之间聚合 `getApplications()` 和 `getInstancesByVipAddress()` |
| `EurekaClientApplicationContextInitializer` | 始终（spring.factories） | 在上下文刷新之前注册 `AbstractDiscoveryClientOptionalArgsBeanPostProcessor` 和 `PropagatingEurekaEventListener` |
| `ConditionalOnEurekaClientAvailable` / `ConditionalOnEurekaClientEnabled` | 元注解 | 为自定义配置组合类路径条件与属性条件 |

### 公共 API

| 类型 / 方法 | 说明 |
|-------------|------|
| `MultipleEurekaClientAutoConfiguration#getEurekaClients()` | `List<EurekaClient>` —— 每个配置 URL 对应一个懒创建的客户端（首个元素是 Spring 管理的客户端） |
| `CompositePreRegistrationHandler`（来自核心模块） | 安装进 `AbstractDiscoveryClientOptionalArgs` 的组合处理器 |
| `PropagatingEurekaEventListener#onEvent(EurekaEvent)` | 将 Netflix 事件发布到 Spring `ApplicationContext` |

## 代码示例

### 1. 元数据同步 + 预注册处理器

改编自 `EnhancedEurekaClientAutoConfigurationTest`：

```java
@SpringBootTest(
        classes = MyApp.class,
        properties = {
                "spring.application.name=test-eureka",
                "eureka.client.serviceUrl.defaultZone=http://127.0.0.1:8761/eureka",
                "microsphere.spring.cloud.multiple-registration.enabled=true",
                "microsphere.spring.cloud.default-registration.type=org.springframework.cloud.netflix.eureka.serviceregistry.EurekaRegistration",
                "microsphere.spring.cloud.default-service-registry.type=org.springframework.cloud.netflix.eureka.serviceregistry.EurekaServiceRegistry"
        })
@EnableAutoConfiguration
public class MetadataSyncDemo {

    @Configuration
    static class Config {
        @Bean
        public PreRegistrationHandler warmUpHandler() {
            return () -> System.out.println("runs before Eureka registration");
        }

        @Bean
        public HealthCheckHandler healthCheckHandler() {
            return currentStatus -> currentStatus;
        }

        // Netflix EurekaEvent re-published as a Spring event — thanks to PropagatingEurekaEventListener
        @EventListener(CacheRefreshedEvent.class)
        public void onCacheRefreshedEvent(CacheRefreshedEvent event) {
            // registry cache refreshed; safe to read local registry
        }
    }

    @Autowired
    private Registration registration;

    public void publishExtraMetadata() {
        registration.getMetadata().put("key", "value"); // synced into InstanceInfo before registration
    }
}
```

### 2. 多 Eureka 客户端

取自 `MultipleEurekaClientAutoConfigurationTest`：

```java
@SpringBootTest(
        classes = MyApp.class,
        properties = {
                "spring.application.name=test-eureka",
                "eureka.client.serviceUrl.defaultZone=http://127.0.0.1:8761/eureka,http://127.0.0.1:8762/eureka",
                "microsphere.eureka.client.multiple=true"
        })
@EnableAutoConfiguration
public class MultipleClientsDemo {

    @Autowired
    private MultipleEurekaClientAutoConfiguration config;

    @Test
    void twoClientsAreManaged() {
        List<EurekaClient> clients = config.getEurekaClients();
        assertEquals(2, clients.size());
    }
}
```

在此配置下，对注入的 `CloudEurekaClient` 调用 `eurekaClient.getApplications()` 会返回注册在**两台**
服务端上的应用的并集，即便这两台服务端之间并不互相复制。

## 行为说明

- 额外可用区的客户端在首次被拦截的调用时，或在 `InstanceRegisteredEvent` 时懒创建。
- `BeanUtils.copyProperties` 会为每个额外客户端克隆原始的 `EurekaClientConfigBean`；仅服务 URL 不同，
  因此超时/注册表轮询等设置会应用于所有客户端。
- 未被拦截的方法（`getByDelta`、`healthCheck` 等）仍然只在客户端上操作。
- 这些额外的 `DiscoveryClient` 实例是普通的 Netflix 客户端，并非 Spring bean；它们会启动各自的
  缓存刷新/心跳调度器。

## 版本与兼容性

| 模块版本 | Spring Cloud | Spring Boot | Java |
|----------|--------------|-------------|------|
| `0.2.x`（本文档） | 2022.0.x – 2025.0.x | 3.x | 17 |
| `0.1.x` | Hoxton – 2021.0.x | 2.x | 8/11 |

## English

[docs/current/user-guide/eureka-client-spring-cloud.md](../../../current/user-guide/eureka-client-spring-cloud.md)
