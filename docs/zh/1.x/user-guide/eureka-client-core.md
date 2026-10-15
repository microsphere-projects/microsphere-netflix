# Eureka Client Core（`1.x`）

模块：`microsphere-netflix-eureka-client-core`（分支 `release-1.x`）· 起始版本：`1.0.0` · 包：`io.microsphere.netflix.eureka.client`

## 用途

客户端增强中不依赖 Spring 的核心部分，构建在 Netflix `eureka-client` API 之上。在该分支上模块名为
**`eureka-client-core`**；在 `release`（`current`）上它已重命名为 `eureka-client`。
`CompositePreRegistrationHandler` 的代码在两条版本线之间逐字节相同——差异仅在构件 ID、POM 的名称/描述
以及测试脚手架（1.x 的测试模块省略了 `logback-classic` 测试依赖）。

```xml
<dependency>
    <groupId>io.github.microsphere-projects</groupId>
    <artifactId>microsphere-netflix-eureka-client-core</artifactId>
</dependency>
```

## API

### `CompositePreRegistrationHandler`

| 成员 | 说明 |
|--------|-------------|
| `CompositePreRegistrationHandler(PreRegistrationHandler...)` / `(List<PreRegistrationHandler>)` | 由可变参数或列表创建组合处理器 |
| `add(PreRegistrationHandler)` → `this` | 追加一个处理器（流式 API，`@Nonnull`） |
| `beforeRegistration()` | 先按 `io.microsphere.lang.Prioritized.COMPARATOR` 升序排序，然后依次调用每个处理器；抛出异常会中断整条调用链 |
| `getPreRegistrationHandlers()` | 返回不可修改的 `@Immutable` 视图 |

## 代码示例（来自分支的测试）

```java
CompositePreRegistrationHandler handler = new CompositePreRegistrationHandler();
assertDoesNotThrow(handler::beforeRegistration);          // empty composite is safe

PreRegistrationHandler noop = () -> { };
assertSame(handler, handler.add(noop));                  // fluent API

PreRegistrationHandler failing = () -> { throw new UnsupportedOperationException("For testing"); };
handler.add(failing);
assertThrows(UnsupportedOperationException.class, handler::beforeRegistration);
```

完整的 API 叙述与不依赖 Spring 的装配示例，请参见
[current eureka-client 指南](../../current/user-guide/eureka-client.md)——只需把构件 ID 替换为
`microsphere-netflix-eureka-client-core`。

## 依赖

| 依赖 | 作用域 |
|------------|-------|
| `microsphere-netflix-eureka-commons` | compile |
| `microsphere-java-core` | compile |
| `com.netflix.eureka:eureka-client` | compile |

## English

[docs/1.x/user-guide/eureka-client-core.md](../../../1.x/user-guide/eureka-client-core.md)
