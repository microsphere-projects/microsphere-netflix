# Eureka Client Core — Developer Guide (`1.x`)

Module: `microsphere-netflix-eureka-client-core` (branch `release-1.x`) · Package: `io.microsphere.netflix.eureka.client`

## Design Analysis

`CompositePreRegistrationHandler` is identical to the current line's `microsphere-netflix-eureka-client`
implementation — Composite over Netflix's single-slot `PreRegistrationHandler`, `Prioritized.COMPARATOR`
sort-on-use, fail-fast chain, fluent `add`, unmodifiable view. Full analysis:
[current developer guide](../../current/developer-guide/eureka-client.md).

## Branch Deltas

| Item | Difference |
|------|------------|
| Artifact ID / POM name | `microsphere-netflix-eureka-client-core`, `Microsphere :: Netflix :: Eureka Client :: Core` |
| Test module | no `logback-classic` test dependency (the current line added it) |

Migration note for contributors: changes to this class must be kept in sync across both branches since the
published APIs are identical.

## 中文

[docs/zh/1.x/developer-guide/eureka-client-core.md](../../zh/1.x/developer-guide/eureka-client-core.md)
