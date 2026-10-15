# Eureka Client Core (`1.x`)

Module: `microsphere-netflix-eureka-client-core` (branch `release-1.x`) · Since: `1.0.0` · Package: `io.microsphere.netflix.eureka.client`

## Purpose

Spring-free core of the client enhancement, built on the Netflix `eureka-client` API. On this branch the
module is named **`eureka-client-core`**; on `release` (`current`) it was renamed to `eureka-client`. The
code of `CompositePreRegistrationHandler` is byte-for-byte identical between the two lines — only the
artifact ID, POM name/description, and test scaffolding differ (the 1.x test module omits the `logback-classic`
test dependency).

```xml
<dependency>
    <groupId>io.github.microsphere-projects</groupId>
    <artifactId>microsphere-netflix-eureka-client-core</artifactId>
</dependency>
```

## API

### `CompositePreRegistrationHandler`

| Member | Description |
|--------|-------------|
| `CompositePreRegistrationHandler(PreRegistrationHandler...)` / `(List<PreRegistrationHandler>)` | Create composite from var-args or list |
| `add(PreRegistrationHandler)` → `this` | Append a handler (fluent, `@Nonnull`) |
| `beforeRegistration()` | Sort by `io.microsphere.lang.Prioritized.COMPARATOR` (ascending), then invoke each handler; exceptions abort the chain |
| `getPreRegistrationHandlers()` | Unmodifiable, `@Immutable` view |

## Code Example (from the branch's test)

```java
CompositePreRegistrationHandler handler = new CompositePreRegistrationHandler();
assertDoesNotThrow(handler::beforeRegistration);          // empty composite is safe

PreRegistrationHandler noop = () -> { };
assertSame(handler, handler.add(noop));                  // fluent API

PreRegistrationHandler failing = () -> { throw new UnsupportedOperationException("For testing"); };
handler.add(failing);
assertThrows(UnsupportedOperationException.class, handler::beforeRegistration);
```

For the full API narrative and Spring-less wiring sample, see
[current eureka-client guide](../../current/user-guide/eureka-client.md) — replace the artifact ID with
`microsphere-netflix-eureka-client-core`.

## Dependencies

| Dependency | Scope |
|------------|-------|
| `microsphere-netflix-eureka-commons` | compile |
| `microsphere-java-core` | compile |
| `com.netflix.eureka:eureka-client` | compile |

## 中文

[docs/zh/1.x/user-guide/eureka-client-core.md](../../zh/1.x/user-guide/eureka-client-core.md)
