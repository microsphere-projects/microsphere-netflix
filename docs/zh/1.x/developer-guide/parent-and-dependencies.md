# Parent POM 与 Dependencies BOM —— 开发者指南（`1.x`）

模块：`microsphere-netflix-parent`、`microsphere-netflix-dependencies`（分支 `release-1.x`）

## 构建模型

构建模型与 current 线一致 —— 使用 `${revision}` 的 CI 友好版本管理并配合上游 flatten
配置，以及三个导入的 BOM（`microsphere-spring-cloud-dependencies` `0.2.25`、
`microsphere-javaee-bom` / `microsphere-tomcat-bom` `0.3.9`）。

差异点：

| 条目 | `release-1.x` | `release` |
|------|---------------|-----------|
| `microsphere-spring-cloud.version` | `0.2.25` | `0.2.26` |
| BOM 托管的构件 | `eureka-commons`, `eureka-client-core`, `eureka-client-spring-cloud`, `eureka-server-spring-cloud` | `eureka-client-core` 被 `eureka-client` 取代；共四个库 |
| 根 `<modules>` | 没有可执行的 server 模块 | 包含 `microsphere-netflix-eureka-server` |
| Maven 构建工作流 | 独立的 `maven-build.yml`，针对本分支线做过调优 | 当前的工作流 |

## `client-core` 命名的设计考量

在本分支上，不依赖 Spring 的客户端库保留 `…-core` 后缀，以便与规划中的 Spring Cloud
模块命名方案区分开；current 线后来将其简称为 `microsphere-netflix-eureka-client`。
在两条分支线之间迁移的应用只需修改 artifact ID —— group、包名
（`io.microsphere.netflix.eureka.client`）与类都未发生变化。

## English

[docs/1.x/developer-guide/parent-and-dependencies.md](../../../1.x/developer-guide/parent-and-dependencies.md)
