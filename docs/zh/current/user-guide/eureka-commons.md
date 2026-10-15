# Eureka Commons

模块：`microsphere-netflix-eureka-commons`
GroupId：`io.github.microsphere-projects` · 自：`1.0.0` · Java 包根：`io.microsphere.netflix.eureka`

## 用途

提供所有 Microsphere Netflix Eureka 模块共享的配置属性名常量。
这些常量都标注了 `@io.microsphere.annotation.ConfigurationProperty`，因此 Microsphere
注解处理器可以将它们导出为带文档的配置元数据。

当你希望在代码中引用属性名而非字符串字面量时，加入该依赖：

```xml
<dependency>
    <groupId>io.github.microsphere-projects</groupId>
    <artifactId>microsphere-netflix-eureka-commons</artifactId>
</dependency>
```

## API

### `io.microsphere.netflix.eureka.commons.constants.PropertyConstants`

| 常量 | 值 | 类型 / 默认值 | 说明 |
|------|-----|---------------|------|
| `EUREKA_PROPERTY_NAME_PREFIX` | `microsphere.eureka.` | — | 所有 Microsphere Eureka 属性的前缀 |
| `EUREKA_ENABLED_PROPERTY_NAME` | `microsphere.eureka.enabled` | `boolean`，默认 `true` | Microsphere Eureka 增强的全局开关 |
| `DEFAULT_EUREKA_ENABLED_PROPERTY_VALUE` | `true` | — | 供 `@ConditionalOnProperty(matchIfMissing = true)` 语义使用的默认值 |

### `io.microsphere.netflix.eureka.client.constants.PropertyConstants`

| 常量 | 值 | 类型 / 默认值 | 说明 |
|------|-----|---------------|------|
| `EUREKA_CLIENT_PROPERTY_NAME_PREFIX` | `microsphere.eureka.client.` | — | 客户端侧增强的前缀 |
| `EUREKA_CLIENT_ENABLED_PROPERTY_NAME` | `microsphere.eureka.client.enabled` | `boolean`，默认 `true` | 为 `false` 时禁用所有 Microsphere 客户端自动配置 |
| `EUREKA_CLIENT_MULTIPLE_PROPERTY_NAME` | `microsphere.eureka.client.multiple` | `boolean`，默认 `false` | 启用多可用区 Eureka 客户端聚合（参见 [eureka-client-spring-cloud](eureka-client-spring-cloud.md#2-多-eureka-客户端)） |

## 代码示例

在条件注解中使用这些常量（模式取自 `ConditionalOnEurekaClientEnabled`）：

```java
import static io.microsphere.netflix.eureka.client.constants.PropertyConstants.EUREKA_CLIENT_MULTIPLE_PROPERTY_NAME;

@ConditionalOnProperty(name = EUREKA_CLIENT_MULTIPLE_PROPERTY_NAME)
public class MyMultipleClientConfiguration {
    ...
}
```

该模块自身的测试（`PropertyConstantsTest`）断言了每个常量的取值与注解元数据：

```java
assertEquals("microsphere.eureka.client.", EUREKA_CLIENT_PROPERTY_NAME_PREFIX);
assertEquals("microsphere.eureka.client.enabled", EUREKA_CLIENT_ENABLED_PROPERTY_NAME);
assertEquals("microsphere.eureka.client.multiple", EUREKA_CLIENT_MULTIPLE_PROPERTY_NAME);

Field field = findField(PropertyConstants.class, "EUREKA_CLIENT_MULTIPLE_PROPERTY_NAME");
ConfigurationProperty annotation = field.getAnnotation(ConfigurationProperty.class);
assertEquals(boolean.class, annotation.type());
assertEquals("false", annotation.defaultValue());
```

## 依赖

| 依赖 | 作用域 |
|------|--------|
| `microsphere-java-core` | compile |
| `microsphere-annotation-processor` | optional（构建期） |

## 版本兼容性

`0.2.x` 模块要求 Java 17 和 Spring Cloud 2022.0.x – 2025.0.x；而 `microsphere-netflix-eureka-commons`
自身只依赖 `microsphere-java-core`，因此它可以在任意 Java 17 运行时上工作，无需 Spring。

## English

[docs/current/user-guide/eureka-commons.md](../../../current/user-guide/eureka-commons.md)
