# Eureka Client Spring Cloud（`1.x`）

模块：`microsphere-netflix-eureka-client-spring-cloud`（分支 `release-1.x`）· 起始版本：`1.0.0` ·
包根：`io.microsphere.netflix.eureka.client.spring.cloud`

## 用途与功能集

功能集与 [current 版本线的模块](../../current/user-guide/eureka-client-spring-cloud.md) 完全一致：
元数据同步、多区域客户端聚合、由 Bean 驱动的处理程序/监听器收集，以及 Netflix→Spring 事件传播。
除下文特别注明的以外，类、属性名与注册文件（`spring.factories` initializer 加上
`AutoConfiguration.imports`）都相同。

## 配置属性

| 属性 | 类型 | 默认值 | 说明 |
|----------|------|---------|-------------|
| `microsphere.eureka.client.enabled` | `boolean` | `true` | 全部 Microsphere 客户端自动配置的总开关 |
| `microsphere.eureka.client.multiple` | `boolean` | `false`（除非设为 `true` 否则不生效） | 启用多客户端聚合 |
| `eureka.client.serviceUrl.defaultZone` | URL 列表 | — | 在 multiple 模式下，每个 URL 都会变成一个被管理的 `EurekaClient` |

## 1.x 特有差异

| 类 | 该分支上的差异 |
|-------|---------------------------|
| `EnhancedEurekaClientAutoConfiguration` | 对事件中的空 `Registration` 做了保护：在执行 `instanceof` 处理之前先 `if (registration == null) return;` |
| `MultipleEurekaClientAutoConfiguration` | 与 current 版本线一致 |
| Bean 后置处理器 / initializer / 监听器 / 条件类 | 与 current 版本线一致 |

## 代码示例

### 多客户端（来自 `MultipleEurekaClientAutoConfigurationTest`）

```java
@SpringBootTest(
        classes = MyApp.class,
        properties = {
                "spring.application.name=test-eureka",
                "eureka.client.serviceUrl.defaultZone=http://127.0.0.1:8761/eureka,http://127.0.0.1:8761/eureka",
                "microsphere.eureka.client.multiple=true"
        })
@EnableAutoConfiguration
public class MultipleClientsTest {

    @Autowired
    private MultipleEurekaClientAutoConfiguration config;

    @Test
    public void test() throws Throwable {
        sleep(SECONDS.toMillis(1));
        List<EurekaClient> eurekaClients = config.getEurekaClients();
        assertEquals(2, eurekaClients.size());
    }
}
```

### 元数据同步与处理程序（来自 `EnhancedEurekaClientAutoConfigurationTest`）

与 current 版本线的模式相同——声明 `PreRegistrationHandler`、`HealthCheckHandler` Bean 以及
`@EventListener(CacheRefreshedEvent.class)` 方法；Spring Cloud 的注册事件会把
`Registration` 元数据自动带入 Netflix 的 `InstanceInfo`。完整示例见
[current 指南示例](../../current/user-guide/eureka-client-spring-cloud.md#1-元数据同步--预注册处理器)。

## 版本与兼容性

| 已发布的 1.x 构件 | Spring Cloud Hoxton – 2021.0.x、Spring Boot 2.x、JDK 8/11 |
|-------------------------|------------------------------------------------------------|
| 本次分析的分支快照 | POM 声明 Java 17；从源码构建时需要留意 |

## English

[docs/1.x/user-guide/eureka-client-spring-cloud.md](../../../1.x/user-guide/eureka-client-spring-cloud.md)
