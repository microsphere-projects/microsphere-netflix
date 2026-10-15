# Eureka Client Spring Cloud (`1.x`)

Module: `microsphere-netflix-eureka-client-spring-cloud` (branch `release-1.x`) · Since: `1.0.0` ·
Package root: `io.microsphere.netflix.eureka.client.spring.cloud`

## Purpose and Feature Set

Identical feature set to the [current-line module](../../current/user-guide/eureka-client-spring-cloud.md):
metadata synchronization, multiple-zone client aggregation, bean-driven handler/listener collection and
Netflix→Spring event propagation. Classes, property names, and registration files (`spring.factories`
initializer + `AutoConfiguration.imports`) are the same except where noted below.

## Configuration Properties

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `microsphere.eureka.client.enabled` | `boolean` | `true` | Switch for all Microsphere client auto-configurations |
| `microsphere.eureka.client.multiple` | `boolean` | `false` (off unless `true`) | Enables multiple-client aggregation |
| `eureka.client.serviceUrl.defaultZone` | URL list | — | Each URL becomes one managed `EurekaClient` in multiple mode |

## 1.x-Specific Differences

| Class | Difference on this branch |
|-------|---------------------------|
| `EnhancedEurekaClientAutoConfiguration` | Guards against a null `Registration` in the event: `if (registration == null) return;` before the `instanceof` handling |
| `MultipleEurekaClientAutoConfiguration` | Identical to the current line |
| Bean post-processor / initializer / listener / conditions | Identical to the current line |

## Code Examples

### Multiple clients (from `MultipleEurekaClientAutoConfigurationTest`)

```java
@SpringBootTest(
        classes = MyApp.class,
        properties = {
                "spring.application.name=test-eureka",
                "eureka.client.serviceUrl.defaultZone=http://127.0.0.1:8761/eureka,http://127.0.0.1:8761/eureka",
                "microsphere.eureka.client.multiple=true"
        })
@EnableAutoConfiguration
public class MultipleClientsTest {

    @Autowired
    private MultipleEurekaClientAutoConfiguration config;

    @Test
    public void test() throws Throwable {
        sleep(SECONDS.toMillis(1));
        List<EurekaClient> eurekaClients = config.getEurekaClients();
        assertEquals(2, eurekaClients.size());
    }
}
```

### Metadata sync and handlers (from `EnhancedEurekaClientAutoConfigurationTest`)

Same pattern as the current line — declare `PreRegistrationHandler`, `HealthCheckHandler` beans and
`@EventListener(CacheRefreshedEvent.class)` methods; Spring Cloud registration events carry
`Registration` metadata into the Netflix `InstanceInfo` automatically. Full sample:
[current guide example](../../current/user-guide/eureka-client-spring-cloud.md#1-metadata-synchronization--pre-registration-handler).

## Version and Compatibility

| Published 1.x artifacts | Spring Cloud Hoxton – 2021.0.x, Spring Boot 2.x, JDK 8/11 |
|-------------------------|------------------------------------------------------------|
| Branch snapshot analyzed | POMs state Java 17; requires care when building from source |

## 中文

[docs/zh/1.x/user-guide/eureka-client-spring-cloud.md](../../zh/1.x/user-guide/eureka-client-spring-cloud.md)
