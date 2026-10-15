# Eureka Client (Core)

Module: `microsphere-netflix-eureka-client`
GroupId: `io.github.microsphere-projects` · Since: `1.0.0` · Package: `io.microsphere.netflix.eureka.client`

> In the `1.x` line this module is named `microsphere-netflix-eureka-client-core`; the API is identical.
> See [1.x eureka-client-core guide](../../1.x/user-guide/eureka-client-core.md).

## Purpose

Core Eureka client enhancements with **no Spring dependency**, built directly on the Netflix
`eureka-client` API. It provides `CompositePreRegistrationHandler`, a composite implementation of
Netflix's `com.netflix.discovery.PreRegistrationHandler` that lets an application register several
pre-registration callbacks and execute them in priority order before the instance registers with
the Eureka server.

```xml
<dependency>
    <groupId>io.github.microsphere-projects</groupId>
    <artifactId>microsphere-netflix-eureka-client</artifactId>
</dependency>
```

In Spring Cloud applications you rarely use this module directly —
`microsphere-netflix-eureka-client-spring-cloud` wires every `PreRegistrationHandler` bean into the
composite automatically.

## API

### `CompositePreRegistrationHandler`

Implements `com.netflix.discovery.PreRegistrationHandler`.

| Member | Signature | Description |
|--------|-----------|-------------|
| Constructor | `CompositePreRegistrationHandler(PreRegistrationHandler... handlers)` | Creates the composite from a var-arg array |
| Constructor | `CompositePreRegistrationHandler(List<PreRegistrationHandler> handlers)` | Creates the composite from a list (copied into a mutable list) |
| `add` | `CompositePreRegistrationHandler add(PreRegistrationHandler handler)` | Appends a handler; fluent, returns `this` (`@Nonnull`) |
| `beforeRegistration` | `void beforeRegistration()` | Sorts handlers with `io.microsphere.lang.Prioritized.COMPARATOR` (lowest order first) and invokes each handler; a throwing handler aborts the chain |
| `getPreRegistrationHandlers` | `List<PreRegistrationHandler> getPreRegistrationHandlers()` | Returns an **unmodifiable** view (`@Immutable`) of the current handlers |

Semantics:
- Handlers implementing `io.microsphere.lang.Prioritized` are ordered by `getOrder()`; others keep
  relative insertion order (stable sort).
- An exception from one handler prevents subsequent handlers (fail-fast).

## Code Example

From `CompositePreRegistrationHandlerTest`:

```java
CompositePreRegistrationHandler handler = new CompositePreRegistrationHandler();

handler.beforeRegistration();                 // no-op when empty
assertTrue(handler.getPreRegistrationHandlers().isEmpty());

PreRegistrationHandler noop = () -> { };
assertSame(handler, handler.add(noop));       // fluent add

PreRegistrationHandler failing = () -> { throw new UnsupportedOperationException("For testing"); };
handler.add(failing);
assertThrows(UnsupportedOperationException.class, handler::beforeRegistration);
```

Wiring it into a plain Netflix client:

```java
DiscoveryClientConfig config = ...;
ApplicationInfoManager infoManager = ...;
AbstractDiscoveryClientOptionalArgs<?> optionalArgs = new DiscoveryClientOptionalArgs();
optionalArgs.setPreRegistrationHandler(
        new CompositePreRegistrationHandler(myHandlerA, myHandlerB));
EurekaClient client = new DiscoveryClient(infoManager, config, optionalArgs);
```

## Dependencies

| Dependency | Scope |
|------------|-------|
| `microsphere-netflix-eureka-commons` | compile |
| `microsphere-java-core` | compile |
| `com.netflix.eureka:eureka-client` | compile (provided by Spring Cloud BOM normally) |
| `logback-classic` | test |

## Version and Compatibility

| This module | Requires |
|-------------|----------|
| `0.2.x` | Java 17; Netflix `eureka-client` as managed by Spring Cloud 2022.0.x – 2025.0.x |

## 中文

[docs/zh/current/user-guide/eureka-client.md](../../zh/current/user-guide/eureka-client.md)
