# Eureka Client (Core) — Developer Guide

Module: `microsphere-netflix-eureka-client` · Package: `io.microsphere.netflix.eureka.client`

## Architecture Role

Spring-free layer. It adapts application-supplied `PreRegistrationHandler` callbacks to the single-handler
slot offered by Netflix's `AbstractDiscoveryClientOptionalArgs`, and defines the ordering contract reused by
the Spring Cloud module.

## Source Analysis: `CompositePreRegistrationHandler`

```java
public class CompositePreRegistrationHandler implements PreRegistrationHandler {

    private final List<PreRegistrationHandler> preRegistrationHandlers;

    @Override
    public void beforeRegistration() {
        sort(this.preRegistrationHandlers, COMPARATOR);   // io.microsphere.lang.Prioritized.COMPARATOR
        for (PreRegistrationHandler preRegistrationHandler : this.preRegistrationHandlers) {
            preRegistrationHandler.beforeRegistration();
        }
    }
}
```

Key implementation facts:

| Aspect | Decision | Rationale |
|--------|----------|-----------|
| Storage | mutable `ArrayList` copy of the input list (`ListUtils.newArrayList`) | The composite is populated incrementally via `add(...)` after construction |
| Ordering | `Collections.sort(list, Prioritized.COMPARATOR)` on **every** `beforeRegistration()` call | Sort-on-use avoids re-sorting when handlers are added between calls; `Prioritized.COMPARATOR` sorts ascending by order, non-`Prioritized` elements treated as `LOWEST_PRECEDENCE` |
| Failure model | exception from a handler propagates and stops the chain | Fail-fast before registration; matches the test `assertThrows(UnsupportedOperationException.class, ...)` |
| Mutability contract | `getPreRegistrationHandlers()` returns `unmodifiableList`, annotated `@Immutable`; `add()` returns `this` annotated `@Nonnull` | Read-safe exposure while keeping fluent building |
| Var-arg constructor | delegates through `Lists.ofList(...)` to the list constructor | Single copy path |

## Design Patterns

- **Composite** — clients pass one `PreRegistrationHandler` (the Netflix API constraint) while the application
  keeps many beans/lambdas.
- **Chain of Responsibility (ordered)** — invocation sequence governed by `Prioritized`.
- **Fluent Builder** — `add(...)` returns the composite itself.

## Extension Guidance

- A handler that must run first implements `io.microsphere.lang.Prioritized` with a low order value.
- Handlers should be idempotent: `beforeRegistration()` runs once per registration attempt, and re-registration
  after Eureka status changes invokes the chain again.
- Do not throw for non-critical work; the abort prevents instance registration entirely.

## 中文

[docs/zh/current/developer-guide/eureka-client.md](../../zh/current/developer-guide/eureka-client.md)
