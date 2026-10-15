# Eureka Commons

Module: `microsphere-netflix-eureka-commons`
GroupId: `io.github.microsphere-projects` · Since: `1.0.0` · Java package root: `io.microsphere.netflix.eureka`

## Purpose

Provides the configuration property name constants shared by all Microsphere Netflix Eureka modules.
Constants are annotated with `@io.microsphere.annotation.ConfigurationProperty`, so the Microsphere
annotation processor can export them as documented configuration metadata.

Add it when you want to reference property names in code instead of string literals:

```xml
<dependency>
    <groupId>io.github.microsphere-projects</groupId>
    <artifactId>microsphere-netflix-eureka-commons</artifactId>
</dependency>
```

## API

### `io.microsphere.netflix.eureka.commons.constants.PropertyConstants`

| Constant | Value | Type / Default | Description |
|----------|-------|----------------|-------------|
| `EUREKA_PROPERTY_NAME_PREFIX` | `microsphere.eureka.` | — | Prefix for all Microsphere Eureka properties |
| `EUREKA_ENABLED_PROPERTY_NAME` | `microsphere.eureka.enabled` | `boolean`, default `true` | Global switch for Microsphere Eureka enhancements |
| `DEFAULT_EUREKA_ENABLED_PROPERTY_VALUE` | `true` | — | Default value used by `@ConditionalOnProperty(matchIfMissing = true)` semantics |

### `io.microsphere.netflix.eureka.client.constants.PropertyConstants`

| Constant | Value | Type / Default | Description |
|----------|-------|----------------|-------------|
| `EUREKA_CLIENT_PROPERTY_NAME_PREFIX` | `microsphere.eureka.client.` | — | Prefix for client-side enhancements |
| `EUREKA_CLIENT_ENABLED_PROPERTY_NAME` | `microsphere.eureka.client.enabled` | `boolean`, default `true` | Disables all Microsphere client auto-configurations when `false` |
| `EUREKA_CLIENT_MULTIPLE_PROPERTY_NAME` | `microsphere.eureka.client.multiple` | `boolean`, default `false` | Enables multiple-zone Eureka client aggregation (see [eureka-client-spring-cloud](eureka-client-spring-cloud.md#2-multiple-eureka-clients)) |

## Code Example

Using the constants in a condition (pattern taken from `ConditionalOnEurekaClientEnabled`):

```java
import static io.microsphere.netflix.eureka.client.constants.PropertyConstants.EUREKA_CLIENT_MULTIPLE_PROPERTY_NAME;

@ConditionalOnProperty(name = EUREKA_CLIENT_MULTIPLE_PROPERTY_NAME)
public class MyMultipleClientConfiguration {
    ...
}
```

The module's own test (`PropertyConstantsTest`) asserts every constant's value and annotation metadata:

```java
assertEquals("microsphere.eureka.client.", EUREKA_CLIENT_PROPERTY_NAME_PREFIX);
assertEquals("microsphere.eureka.client.enabled", EUREKA_CLIENT_ENABLED_PROPERTY_NAME);
assertEquals("microsphere.eureka.client.multiple", EUREKA_CLIENT_MULTIPLE_PROPERTY_NAME);

Field field = findField(PropertyConstants.class, "EUREKA_CLIENT_MULTIPLE_PROPERTY_NAME");
ConfigurationProperty annotation = field.getAnnotation(ConfigurationProperty.class);
assertEquals(boolean.class, annotation.type());
assertEquals("false", annotation.defaultValue());
```

## Dependencies

| Dependency | Scope |
|------------|-------|
| `microsphere-java-core` | compile |
| `microsphere-annotation-processor` | optional (build-time) |

## Version Compatibility

`0.2.x` modules require Java 17 and Spring Cloud 2022.0.x – 2025.0.x; `microsphere-netflix-eureka-commons`
itself depends only on `microsphere-java-core`, so it works on any Java 17 runtime without Spring.

## 中文

[docs/zh/current/user-guide/eureka-commons.md](../../zh/current/user-guide/eureka-commons.md)
