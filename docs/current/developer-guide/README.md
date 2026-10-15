# Developer Guide — `release` Branch (0.2.x)

This guide is for contributors and engineers who need to understand or extend microsphere-netflix.
Each Maven module has a dedicated document covering architecture, source analysis, design patterns,
and mermaid flow diagrams for complex processes.

## System Architecture

```mermaid
flowchart TB
    subgraph Client Side
        APP[Spring Boot Application]
        MR[MultipleRegistration<br/>metadata +<br/>InstanceInfo<br/>sync]
        MEC[MultipleEurekaClientAutoConfiguration<br/>AOP aggregation across zones]
        DC[(Netflix DiscoveryClient<br/>+ Eureka server URLs)]
    end
    subgraph Eureka Server Cluster
        S1[Eureka Server Node 1<br/>embedded Tomcat + Tribes]
        S2[Eureka Server Node 2<br/>embedded Tomcat + Tribes]
        S1 <-.->|ReplicatedMap over<br/>Tribes channel| S2
    end
    APP --> MR --> DC
    APP --> MEC --> DC
    DC -->|register / heartbeat / cancel| S1
    S1 -.-> S2
```

## Module Dependency Graph

Build-time (Maven) dependencies between repository modules — arrows point to the dependency:

```mermaid
flowchart TD
    parent["microsphere-netflix-parent<br/>(parent POM)"]
    deps["microsphere-netflix-dependencies<br/>(BOM)"]
    commons["microsphere-netflix-eureka-commons"]
    client["microsphere-netflix-eureka-client"]
    clientSC["microsphere-netflix-eureka-client-spring-cloud"]
    serverSC["microsphere-netflix-eureka-server-spring-cloud"]
    server["microsphere-netflix-eureka-server<br/>(executable)"]

    deps -. manages versions .-> commons
    deps -. manages versions .-> client
    deps -. manages versions .-> clientSC
    deps -. manages versions .-> serverSC

    client --> commons
    clientSC --> client
    serverSC --> clientSC
    server --> serverSC
```

External foundations: `microsphere-java-core`, `microsphere-spring-cloud-commons`,
`com.netflix.eureka:eureka-client` / `eureka-server` (via Spring Cloud Netflix starters),
`tomcat-tribes` + `tomcat-catalina-ha` (clustering), `jakarta.servlet-api`.

## Cross-Module Runtime Flow: Instance Registration End-to-End

```mermaid
sequenceDiagram
    participant App as Spring Boot App<br/>(client-spring-cloud)
    participant DC as DiscoveryClient
    participant S1 as Server node 1<br/>(server-spring-cloud)
    participant Tribes as Tomcat channel
    participant S2 as Server node 2

    App->>App: RegistrationPreRegisteredEvent<br/>metadata → InstanceInfo
    App->>DC: beforeRegistration (composite handler)
    DC->>S1: HTTP Register
    S1->>S1: EurekaInstanceRegisteredEvent
    S1->>S1: encode InstanceInfo JSON<br/>metadata[_action_]=Register
    S1->>Tribes: ServletContext attribute<br/>→ ReplicatedMap.put
    Tribes->>S2: MapMessage(MSG_COPY)
    S2->>S2: decode → registry.register(..., isReplication=true)
```

## Module Documents

| Document | Focus |
|----------|-------|
| [parent-and-dependencies.md](parent-and-dependencies.md) | Build model, `${revision}` flattening, BOM strategy |
| [eureka-commons.md](eureka-commons.md) | Constants design, `@ConfigurationProperty` metadata generation |
| [eureka-client.md](eureka-client.md) | Composite + Prioritized source analysis |
| [eureka-client-spring-cloud.md](eureka-client-spring-cloud.md) | Auto-configuration ordering, BeanPostProcessor, AOP aggregation flows |
| [eureka-server-spring-cloud.md](eureka-server-spring-cloud.md) | Tomcat cluster bootstrap, replication protocol, shutdown deregistration flows |
| [eureka-server.md](eureka-server.md) | Bootstrap packaging design |

## Testing Strategy

- Unit tests (`CompositePreRegistrationHandlerTest`, `PropertyConstantsTest`) run without infrastructure.
- Integration tests boot real Spring contexts: `EnhancedEurekaClientAutoConfigurationTest` and
  `MultipleEurekaClientAutoConfigurationTest` target a Eureka server at `127.0.0.1:8761`
  (docker compose file `docker/test-services.yml` provides an image for it).
- `TomcatEurekaServerReplicationAutoConfigurationTest` spins up **two** in-process Eureka servers
  (ports 10000/10001) and asserts cluster convergence by polling the registry.
- `EurekaServerTest` is the shared base class: it boots a servlet web context with
  `EurekaServerMarkerConfiguration`, exposes `registry`, `servletContext`, `instanceInfo` fixtures.

## 中文

[docs/zh/current/developer-guide/README.md](../../zh/current/developer-guide/README.md)
