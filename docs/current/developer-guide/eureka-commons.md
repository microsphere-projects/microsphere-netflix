# Eureka Commons — Developer Guide

Module: `microsphere-netflix-eureka-commons` · Package: `io.microsphere.netflix.eureka.commons`, `io.microsphere.netflix.eureka.client`

## Architecture Role

The leaf module of the repository: no dependency on other Microsphere Netflix modules except being
referenced by them. It centralizes property-name constants so conditions across client and server
auto-configurations read the *same* strings.

## Source Analysis

Two `PropertyConstants` interfaces split by namespace:

| Interface | Prefix | Constants |
|-----------|--------|-----------|
| `io.microsphere.netflix.eureka.commons.constants.PropertyConstants` | `microsphere.eureka.` | `EUREKA_ENABLED_PROPERTY_NAME`, `DEFAULT_EUREKA_ENABLED_PROPERTY_VALUE` |
| `io.microsphere.netflix.eureka.client.constants.PropertyConstants` | `microsphere.eureka.client.` | `EUREKA_CLIENT_ENABLED_PROPERTY_NAME`, `EUREKA_CLIENT_MULTIPLE_PROPERTY_NAME` |

Names are composed, not duplicated — the client interface builds its prefix from the commons prefix plus
`"client."`, and each "enabled" name from `io.microsphere.constants.PropertyConstants.ENABLED_PROPERTY_NAME`:

```java
String EUREKA_CLIENT_PROPERTY_NAME_PREFIX = EUREKA_PROPERTY_NAME_PREFIX + "client.";
String EUREKA_CLIENT_ENABLED_PROPERTY_NAME = EUREKA_CLIENT_PROPERTY_NAME_PREFIX + ENABLED_PROPERTY_NAME;
```

## Design Patterns & Idioms

- **Constant interface** (types are `interface`, not `final class`) — idiomatic in the Microsphere codebase,
  allows static imports without instantiation and inheritance by other constant interfaces.
- **Annotation-driven documentation**: every property constant carries
  `@ConfigurationProperty(type, defaultValue, source)`. Combined with `microsphere-annotation-processor`
  (declared optional in the POM), this exports configuration metadata for IDE completion.
  The unit test asserts the annotation attributes via reflection, so a changed default cannot silently drift.

## Testing Notes

`PropertyConstantsTest` (both packages) verifies:
- exact property strings (`"microsphere.eureka.client.multiple"`, …),
- `@ConfigurationProperty` `type()`, `defaultValue()` and `source()` contents.

These tests are the guardrail that downstream conditional annotations (`ConditionalOnEurekaClientEnabled`)
match documented behavior.

## 中文

[docs/zh/current/developer-guide/eureka-commons.md](../../zh/current/developer-guide/eureka-commons.md)
