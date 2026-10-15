# Eureka Commons —— 开发者指南

模块：`microsphere-netflix-eureka-commons` · 包：`io.microsphere.netflix.eureka.commons`、`io.microsphere.netflix.eureka.client`

## 架构定位

仓库中的叶子模块：除被其他模块引用之外，它不依赖任何其他 Microsphere Netflix 模块。它集中管理属性名常量，
使得客户端与服务端各处自动配置的条件（condition）读取的是*同一批*字符串。

## 源码分析

按命名空间划分的两个 `PropertyConstants` 接口：

| 接口 | 前缀 | 常量 |
|-----------|--------|-----------|
| `io.microsphere.netflix.eureka.commons.constants.PropertyConstants` | `microsphere.eureka.` | `EUREKA_ENABLED_PROPERTY_NAME`、`DEFAULT_EUREKA_ENABLED_PROPERTY_VALUE` |
| `io.microsphere.netflix.eureka.client.constants.PropertyConstants` | `microsphere.eureka.client.` | `EUREKA_CLIENT_ENABLED_PROPERTY_NAME`、`EUREKA_CLIENT_MULTIPLE_PROPERTY_NAME` |

常量名是组合而成的，而不是复制粘贴——客户端接口的前缀由 commons 前缀加上 `"client."` 构成，
而每个 enabled 名称都取自 `io.microsphere.constants.PropertyConstants.ENABLED_PROPERTY_NAME`：

```java
String EUREKA_CLIENT_PROPERTY_NAME_PREFIX = EUREKA_PROPERTY_NAME_PREFIX + "client.";
String EUREKA_CLIENT_ENABLED_PROPERTY_NAME = EUREKA_CLIENT_PROPERTY_NAME_PREFIX + ENABLED_PROPERTY_NAME;
```

## 设计模式与惯用法

- **常量接口（Constant interface）**——类型声明为 `interface` 而非 `final class`，这是 Microsphere 代码库中的惯用做法：
  无需实例化即可静态导入，也便于被其他常量接口继承。
- **注解驱动的文档化**——每个属性常量都携带
  `@ConfigurationProperty(type, defaultValue, source)`。配合 `microsphere-annotation-processor`
  （在 POM 中声明为 optional），可导出供 IDE 补全使用的配置元数据。
  单元测试通过反射断言注解属性，因此默认值一旦被改动就无法悄无声息地漂移。

## 测试要点

`PropertyConstantsTest`（两个包中各有一个）验证：
- 精确的属性字符串（如 `"microsphere.eureka.client.multiple"`），
- `@ConfigurationProperty` 的 `type()`、`defaultValue()` 与 `source()` 内容。

这些测试构成一道护栏，确保下游的条件注解（`ConditionalOnEurekaClientEnabled`）与文档描述的行为保持一致。

## English

[docs/current/developer-guide/eureka-commons.md](../../../current/developer-guide/eureka-commons.md)
