# Eureka Client（核心）—— 开发者指南

模块：`microsphere-netflix-eureka-client` · 包：`io.microsphere.netflix.eureka.client`

## 架构定位

不依赖 Spring 的一层。它把应用提供的 `PreRegistrationHandler` 回调适配到 Netflix
`AbstractDiscoveryClientOptionalArgs` 仅提供的那一个处理器槽位上，并定义了被 Spring Cloud 模块复用的排序契约。

## 源码分析：`CompositePreRegistrationHandler`

```java
public class CompositePreRegistrationHandler implements PreRegistrationHandler {

    private final List<PreRegistrationHandler> preRegistrationHandlers;

    @Override
    public void beforeRegistration() {
        sort(this.preRegistrationHandlers, COMPARATOR);   // io.microsphere.lang.Prioritized.COMPARATOR
        for (PreRegistrationHandler preRegistrationHandler : this.preRegistrationHandlers) {
            preRegistrationHandler.beforeRegistration();
        }
    }
}
```

关键实现事实：

| 方面 | 决策 | 理由 |
|--------|----------|-----------|
| 存储 | 对入参列表做一层可变的 `ArrayList` 拷贝（`ListUtils.newArrayList`） | 组合对象在构造完成后通过 `add(...)` 逐步填充 |
| 排序 | **每次** `beforeRegistration()` 调用都执行 `Collections.sort(list, Prioritized.COMPARATOR)` | 使用时排序可避免处理器在两次调用之间被新增时的重复排序；`Prioritized.COMPARATOR` 按 order 升序排列，非 `Prioritized` 元素按 `LOWEST_PRECEDENCE` 处理 |
| 失败模型 | 某个处理器抛出的异常会向上传播并终止整条链 | 在注册之前 fail-fast；与测试 `assertThrows(UnsupportedOperationException.class, ...)` 的断言一致 |
| 可变性契约 | `getPreRegistrationHandlers()` 返回 `unmodifiableList`，并标注 `@Immutable`；`add()` 返回 `this` 并标注 `@Nonnull` | 既能安全地对外暴露只读视图，又保留流式（fluent）构建能力 |
| 可变参数构造器 | 通过 `Lists.ofList(...)` 委托给列表构造器 | 只保留一条拷贝路径 |

## 设计模式

- **组合（Composite）**——Netflix API 只允许传入一个 `PreRegistrationHandler`，而应用侧可以保有众多 Bean 或 lambda。
- **责任链（Chain of Responsibility，有序）**——调用次序由 `Prioritized` 决定。
- **流式构建器（Fluent Builder）**——`add(...)` 返回组合对象自身。

## 扩展指引

- 必须最先执行的处理器应实现 `io.microsphere.lang.Prioritized`，并给出较小的 order 值。
- 处理器应当保证幂等：`beforeRegistration()` 每次注册尝试执行一次，而 Eureka 状态变化后的重新注册会再次调用整条链。
- 不要为非关键工作抛出异常；一旦中断，实例注册将彻底失败。

## English

[docs/current/developer-guide/eureka-client.md](../../../current/developer-guide/eureka-client.md)
