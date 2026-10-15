# Eureka Client Spring Cloud

Module: `microsphere-netflix-eureka-client-spring-cloud`
GroupId: `io.github.microsphere-projects` · Since: `1.0.0` · Package root: `io.microsphere.netflix.eureka.client.spring.cloud`

## Purpose

Drop-in enhancement of Spring Cloud Netflix's Eureka client. Adding this JAR to the classpath activates
three auto-configured features without any code change:

1. **Metadata synchronization** — copies Spring Cloud `Registration` metadata into the Netflix
   `InstanceInfo` right before registration, so custom metadata always reaches the Eureka server.
2. **Multiple Eureka clients** — when enabled, one `DiscoveryClient` is created per configured Eureka
   server URL and registry queries (`getApplications`, `getInstancesByVipAddress`) return the
   **aggregated** view of all servers.
3. **Bean-driven extension points** — every `PreRegistrationHandler` and `EurekaEventListener` bean in
   your context is automatically plugged into the Netflix client, and every Netflix `EurekaEvent`
   (e.g. `CacheRefreshedEvent`) is re-published as a Spring application event.

```xml
<dependency>
    <groupId>io.github.microsphere-projects</groupId>
    <artifactId>microsphere-netflix-eureka-client-spring-cloud</artifactId>
</dependency>
```

All features are conditional; they switch off cleanly when `eureka.client.enabled=false`,
`microsphere.eureka.client.enabled=false`, or the Netflix/Spring Cloud classes are absent.

## Configuration Properties

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `microsphere.eureka.enabled` | `boolean` | `true` | Global Microsphere Eureka switch |
| `microsphere.eureka.client.enabled` | `boolean` | `true` | Switch for all client auto-configurations |
| `microsphere.eureka.client.multiple` | `boolean` | `false` (feature off unless `true`) | Enables `MultipleEurekaClientAutoConfiguration` |
| `eureka.client.serviceUrl.defaultZone` | `String` (URL list) | — | Comma-separated URLs; **each URL becomes one managed `EurekaClient` when `microsphere.eureka.client.multiple=true`** |

## Auto-Configuration Reference

Registered via `META-INF/spring/...AutoConfiguration.imports` (Boot 3 style) and
`META-INF/spring.factories` (application context initializer):

| Class | Activated When | Behavior |
|-------|----------------|----------|
| `EnhancedEurekaClientAutoConfiguration` | `@ConditionalOnDiscoveryEnabled` and `@ConditionalOnEurekaClientAvailable`, after Spring's `EurekaClientAutoConfiguration` | Listens for `RegistrationPreRegisteredEvent`; unwraps `MultipleRegistration` to its `EurekaRegistration`; merges `registration.getMetadata()` into `InstanceInfo.getMetadata()` |
| `MultipleEurekaClientAutoConfiguration` | Same conditions **plus** `microsphere.eureka.client.multiple` present | Builds a `DiscoveryClient` per service URL; installs an AOP `PointcutAdvisor` on `CloudEurekaClient` to aggregate `getApplications()` and `getInstancesByVipAddress()` across all clients |
| `EurekaClientApplicationContextInitializer` | Always (spring.factories) | Registers `AbstractDiscoveryClientOptionalArgsBeanPostProcessor` and `PropagatingEurekaEventListener` before context refresh |
| `ConditionalOnEurekaClientAvailable` / `ConditionalOnEurekaClientEnabled` | Meta-annotations | Compose classpath and property conditions for custom configuration |

### Public API

| Type / Method | Description |
|---------------|-------------|
| `MultipleEurekaClientAutoConfiguration#getEurekaClients()` | `List<EurekaClient>` — the lazily-created client per configured URL (first entry is the Spring-managed primary client) |
| `CompositePreRegistrationHandler` (from core module) | The composite installed into `AbstractDiscoveryClientOptionalArgs` |
| `PropagatingEurekaEventListener#onEvent(EurekaEvent)` | Publishes the Netflix event to the Spring `ApplicationContext` |

## Code Examples

### 1. Metadata synchronization + pre-registration handler

Adapted from `EnhancedEurekaClientAutoConfigurationTest`:

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

### 2. Multiple Eureka clients

From `MultipleEurekaClientAutoConfigurationTest`:

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

With this configuration, `eurekaClient.getApplications()` on the injected `CloudEurekaClient` returns the union of
applications registered in **both** servers, even if the two servers do not replicate to each other.

## Behavior Notes

- Clients for extra zones are created lazily on the first intercepted call or on `InstanceRegisteredEvent`.
- `BeanUtils.copyProperties` clones the original `EurekaClientConfigBean` for each extra client; only the
  service URL differs, so timeouts/registry poll settings apply to all clients.
- Non-intercepted methods (`getByDelta`, `healthCheck`, …) still operate on the primary client only.
- The extra `DiscoveryClient` instances are plain Netflix clients, not Spring beans; they start their own
  cache-refresh/heartbeat schedulers.

## Version and Compatibility

| Module Version | Spring Cloud | Spring Boot | Java |
|----------------|--------------|-------------|------|
| `0.2.x` (this document) | 2022.0.x – 2025.0.x | 3.x | 17 |
| `0.1.x` | Hoxton – 2021.0.x | 2.x | 8/11 |

## 中文

[docs/zh/current/user-guide/eureka-client-spring-cloud.md](../../zh/current/user-guide/eureka-client-spring-cloud.md)
