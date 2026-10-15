# Parent POM and Dependencies BOM — Developer Guide (`1.x`)

Modules: `microsphere-netflix-parent`, `microsphere-netflix-dependencies` (branch `release-1.x`)

## Build Model

The build model matches the current line — `${revision}` CI-friendly versioning with upstream flatten
config, and three imported BOMs (`microsphere-spring-cloud-dependencies` `0.2.25`,
`microsphere-javaee-bom` / `microsphere-tomcat-bom` `0.3.9`).

Deltas:

| Item | `release-1.x` | `release` |
|------|---------------|-----------|
| `microsphere-spring-cloud.version` | `0.2.25` | `0.2.26` |
| BOM-managed artifacts | `eureka-commons`, `eureka-client-core`, `eureka-client-spring-cloud`, `eureka-server-spring-cloud` | `eureka-client-core` is replaced by `eureka-client`; four libraries total |
| Root `<modules>` | no executable server module | includes `microsphere-netflix-eureka-server` |
| Maven build workflow | separate `maven-build.yml` tuned for this line | current workflow |

## Design Rationale for the `client-core` Name

On this branch the Spring-free client library keeps the `…-core` suffix to distinguish it from the planned
Spring Cloud module naming scheme; the current line later shortened it to `microsphere-netflix-eureka-client`.
Applications migrating between lines must change only the artifact ID — group, package
(`io.microsphere.netflix.eureka.client`), and classes are unchanged.

## 中文

[docs/zh/1.x/developer-guide/parent-and-dependencies.md](../../zh/1.x/developer-guide/parent-and-dependencies.md)
