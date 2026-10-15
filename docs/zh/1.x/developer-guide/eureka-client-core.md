# Eureka Client Core —— 开发者指南（`1.x`）

模块：`microsphere-netflix-eureka-client-core`（分支 `release-1.x`）· 包：`io.microsphere.netflix.eureka.client`

## 设计分析

`CompositePreRegistrationHandler` 与 current 线的 `microsphere-netflix-eureka-client`
实现完全一致 —— 在 Netflix 单槽位（single-slot）的 `PreRegistrationHandler` 之上施加组合（Composite）
模式、使用 `Prioritized.COMPARATOR` 的用即排序（sort-on-use）、快速失败（fail-fast）的责任链、流式（fluent）
`add` 方法、不可变视图。完整分析见：
[current 开发者指南](../../current/developer-guide/eureka-client.md)。

## 分支差异

| 条目 | 差异 |
|------|------|
| Artifact ID / POM 名称 | `microsphere-netflix-eureka-client-core`, `Microsphere :: Netflix :: Eureka Client :: Core` |
| 测试模块 | 没有 `logback-classic` 测试依赖（current 线补充了该依赖） |

给贡献者的迁移提示：由于对外发布的 API 完全相同，对本类所做的改动必须在两个分支上保持同步。

## English

[docs/1.x/developer-guide/eureka-client-core.md](../../../1.x/developer-guide/eureka-client-core.md)
