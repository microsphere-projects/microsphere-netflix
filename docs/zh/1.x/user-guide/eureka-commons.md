# Eureka Commons（`1.x`）

模块：`microsphere-netflix-eureka-commons`（分支 `release-1.x`）· 起始版本：`1.0.0`

属性常量模块与现代版本线（current）**完全一致**：包、常量、默认值以及
`@ConfigurationProperty` 元数据均相同。

## API 概览

| 常量 | 值 | 类型 / 默认值 |
|----------|-------|----------------|
| `io.microsphere.netflix.eureka.commons.constants.PropertyConstants.EUREKA_PROPERTY_NAME_PREFIX` | `microsphere.eureka.` | — |
| `...commons...EUREKA_ENABLED_PROPERTY_NAME` | `microsphere.eureka.enabled` | `boolean` / `true` |
| `...commons...DEFAULT_EUREKA_ENABLED_PROPERTY_VALUE` | `true` | — |
| `io.microsphere.netflix.eureka.client.constants.PropertyConstants.EUREKA_CLIENT_PROPERTY_NAME_PREFIX` | `microsphere.eureka.client.` | — |
| `...client...EUREKA_CLIENT_ENABLED_PROPERTY_NAME` | `microsphere.eureka.client.enabled` | `boolean` / `true` |
| `...client...EUREKA_CLIENT_MULTIPLE_PROPERTY_NAME` | `microsphere.eureka.client.multiple` | `boolean` / `false` |

完整说明、使用示例与测试注记请参见
[current eureka-commons 指南](../../current/user-guide/eureka-commons.md)
（其中的全部结论对 1.x 分支同样适用，无需任何改动）。

## 依赖坐标

```xml
<dependency>
    <groupId>io.github.microsphere-projects</groupId>
    <artifactId>microsphere-netflix-eureka-commons</artifactId>
</dependency>
```

## English

[docs/1.x/user-guide/eureka-commons.md](../../../1.x/user-guide/eureka-commons.md)
