# Eureka Client（核心）

模块：`microsphere-netflix-eureka-client`
GroupId：`io.github.microsphere-projects` · 自：`1.0.0` · 包：`io.microsphere.netflix.eureka.client`

> 在 `1.x` 版本线中，本模块名为 `microsphere-netflix-eureka-client-core`；API 完全一致。
> 参见 [1.x eureka-client-core 指南](../../1.x/user-guide/eureka-client-core.md)。

## 用途

不依赖 Spring 的核心 Eureka 客户端增强，直接构建于 Netflix
`eureka-client` API 之上。它提供 `CompositePreRegistrationHandler`——
Netflix `com.netflix.discovery.PreRegistrationHandler` 的一个组合（composite）实现，
让应用可以注册多个预注册回调，并在实例向 Eureka 服务端注册之前按优先级顺序依次执行。

```xml
<dependency>
    <groupId>io.github.microsphere-projects</groupId>
    <artifactId>microsphere-netflix-eureka-client</artifactId>
</dependency>
```

在 Spring Cloud 应用中，你很少会直接使用本模块——
`microsphere-netflix-eureka-client-spring-cloud` 会自动将每个 `PreRegistrationHandler`
bean 装配进组合处理器。

## API

### `CompositePreRegistrationHandler`

实现 `com.netflix.discovery.PreRegistrationHandler`。

| 成员 | 签名 | 说明 |
|------|------|------|
| 构造器 | `CompositePreRegistrationHandler(PreRegistrationHandler... handlers)` | 由可变参数数组创建组合处理器 |
| 构造器 | `CompositePreRegistrationHandler(List<PreRegistrationHandler> handlers)` | 由列表创建组合处理器（会复制到一个可变列表） |
| `add` | `CompositePreRegistrationHandler add(PreRegistrationHandler handler)` | 追加一个处理器；流式 API，返回 `this`（`@Nonnull`） |
| `beforeRegistration` | `void beforeRegistration()` | 使用 `io.microsphere.lang.Prioritized.COMPARATOR` 对处理器排序（order 越小越靠前）并依次调用；某个处理器抛异常会中断整条链 |
| `getPreRegistrationHandlers` | `List<PreRegistrationHandler> getPreRegistrationHandlers()` | 返回当前处理器的**不可修改**视图（`@Immutable`） |

语义：
- 实现了 `io.microsphere.lang.Prioritized` 的处理器按 `getOrder()` 排序；其余处理器保持
  相对的插入顺序（稳定排序）。
- 某个处理器抛出的异常会阻止后续处理器执行（快速失败 fail-fast）。

## 代码示例

取自 `CompositePreRegistrationHandlerTest`：

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

将其接入一个纯 Netflix 客户端：

```java
DiscoveryClientConfig config = ...;
ApplicationInfoManager infoManager = ...;
AbstractDiscoveryClientOptionalArgs<?> optionalArgs = new DiscoveryClientOptionalArgs();
optionalArgs.setPreRegistrationHandler(
        new CompositePreRegistrationHandler(myHandlerA, myHandlerB));
EurekaClient client = new DiscoveryClient(infoManager, config, optionalArgs);
```

## 依赖

| 依赖 | 作用域 |
|------|--------|
| `microsphere-netflix-eureka-commons` | compile |
| `microsphere-java-core` | compile |
| `com.netflix.eureka:eureka-client` | compile（通常由 Spring Cloud BOM 提供） |
| `logback-classic` | test |

## 版本与兼容性

| 本模块 | 依赖要求 |
|--------|----------|
| `0.2.x` | Java 17；由 Spring Cloud 2022.0.x – 2025.0.x 管理的 Netflix `eureka-client` |

## English

[docs/current/user-guide/eureka-client.md](../../../current/user-guide/eureka-client.md)
