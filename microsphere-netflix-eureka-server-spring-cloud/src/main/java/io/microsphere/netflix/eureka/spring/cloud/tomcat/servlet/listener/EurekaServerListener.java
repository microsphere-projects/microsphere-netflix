/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.microsphere.netflix.eureka.spring.cloud.tomcat.servlet.listener;

import com.netflix.appinfo.EurekaInstanceConfig;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.converters.wrappers.CodecWrapper;
import com.netflix.eureka.EurekaServerContext;
import com.netflix.eureka.registry.PeerAwareInstanceRegistry;
import com.netflix.eureka.registry.PeerAwareInstanceRegistryImpl.Action;
import com.netflix.eureka.resources.ServerCodecs;
import io.microsphere.logging.Logger;
import io.microsphere.netflix.eureka.spring.cloud.EurekaServerProperties;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletContextEvent;
import jakarta.servlet.ServletContextListener;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.cloud.netflix.eureka.server.event.EurekaInstanceCanceledEvent;
import org.springframework.cloud.netflix.eureka.server.event.EurekaInstanceRegisteredEvent;
import org.springframework.cloud.netflix.eureka.server.event.EurekaInstanceRenewedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.io.IOException;
import java.util.Map;

import static com.netflix.eureka.registry.PeerAwareInstanceRegistryImpl.Action.Cancel;
import static com.netflix.eureka.registry.PeerAwareInstanceRegistryImpl.Action.Heartbeat;
import static com.netflix.eureka.registry.PeerAwareInstanceRegistryImpl.Action.Register;
import static io.microsphere.logging.LoggerFactory.getLogger;
import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.springframework.web.context.request.RequestContextHolder.getRequestAttributes;

/**
 * Customized EurekaServer Listener
 *
 * @author <a href="mailto:mercyblitz@gmail.com">Mercy</a>
 * @since 1.0.0
 */
public class EurekaServerListener implements ServletContextListener {

    private static final Logger logger = getLogger(EurekaServerListener.class);

    public static final String EUREKA_SERVER_LISTENER_ATTRIBUTE_NAME = "EurekaServerListener";

    public static final String EUREKA_SERVER_CONTEXT_ATTRIBUTE_NAME = "EurekaServerContext";

    private final EurekaServerContext eurekaServerContext;

    private final EurekaInstanceConfig eurekaInstanceConfig;

    private final CodecWrapper codecWrapper;

    private final PeerAwareInstanceRegistry registry;

    private ServletContext servletContext;

    private final String instanceNamePrefix;

    private final String actionMetadataKey;

    private final long deregistionDelay;

    private volatile boolean deregistered = false;

    public EurekaServerListener(EurekaServerContext eurekaServerContext, EurekaInstanceConfig eurekaInstanceConfig,
                                EurekaServerProperties eurekaServerProperties) {
        this.eurekaServerContext = eurekaServerContext;
        this.eurekaInstanceConfig = eurekaInstanceConfig;
        this.codecWrapper = this.initCodecWrapper(eurekaServerContext);
        this.registry = this.initPeerAwareInstanceRegistry(eurekaServerContext);
        this.instanceNamePrefix = eurekaServerProperties.getInstanceNamePrefix();
        this.actionMetadataKey = eurekaServerProperties.getActionKey();
        this.deregistionDelay = eurekaServerProperties.getDeregistrationDelay();
    }

    @Override
    public void contextInitialized(ServletContextEvent event) {
        this.servletContext = event.getServletContext();
        this.servletContext.setAttribute(EUREKA_SERVER_LISTENER_ATTRIBUTE_NAME, this);
        initEurekaServerContext();
    }

    @Override
    public void contextDestroyed(ServletContextEvent event) {
        deregister();
    }

    public void deregister() {
        if (deregistered) {
            return;
        }
        String appName = eurekaInstanceConfig.getAppname().toUpperCase();
        String id = eurekaInstanceConfig.getInstanceId();
        InstanceInfo instance = registry.getInstanceByAppAndId(appName, id);
        if (instance == null) {
            logger.warn("No InstanceInfo was found by appName : {} and id : {}!", appName, id);
            return;
        }
        try {
            for (int i = 0; i < deregistionDelay; i++) {
                doReplicateInstance(instance, Cancel);
                sleep(SECONDS.toMillis(1));
            }
            logger.info("The current instance[appName : '{}' , id : '{}' ] was deregistered before {}s!", appName, id, deregistionDelay);
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }
        deregistered = true;
    }

    @EventListener(EurekaInstanceRegisteredEvent.class)
    public void onEurekaInstanceRegisteredEvent(EurekaInstanceRegisteredEvent event) throws Throwable {
        if (event.isReplication()) {
            logger.trace("[Replication] The {} will be ignored!", event);
            return;
        }
        InstanceInfo instance = event.getInstanceInfo();
        replicateInstance(instance, Register);
    }

    @EventListener(EurekaInstanceCanceledEvent.class)
    public void onEurekaInstanceCanceledEvent(EurekaInstanceCanceledEvent event) throws Throwable {
        if (event.isReplication()) {
            logger.trace("[Replication] The {} will be ignored!", event);
            return;
        }
        String appName = event.getAppName();
        String serviceInstanceId = event.getServerId();
        InstanceInfo instance = registry.getInstanceByAppAndId(appName, serviceInstanceId);
        replicateInstance(instance, Cancel);
    }

    @EventListener(EurekaInstanceRenewedEvent.class)
    public void onEurekaInstanceRenewedEvent(EurekaInstanceRenewedEvent event) throws Throwable {
        if (event.isReplication()) {
            logger.trace("[Replication] The {} will be ignored!", event);
            return;
        }
        InstanceInfo instance = event.getInstanceInfo();
        replicateInstance(instance, Heartbeat);
    }

    private void initEurekaServerContext() {
        String name = EUREKA_SERVER_CONTEXT_ATTRIBUTE_NAME;
        servletContext.setAttribute(name, eurekaServerContext);
        logger.info("The EurekaServerContext has been initialized into the ServletContext with name : {}", name);
    }

    private CodecWrapper initCodecWrapper(EurekaServerContext eurekaServerContext) {
        ServerCodecs serverCodecs = eurekaServerContext.getServerCodecs();
        CodecWrapper codecWrapper = serverCodecs.getFullJsonCodec();
        logger.info("The CodecWrapper has been initialized");
        return codecWrapper;
    }

    private PeerAwareInstanceRegistry initPeerAwareInstanceRegistry(EurekaServerContext eurekaServerContext) {
        PeerAwareInstanceRegistry registry = eurekaServerContext.getRegistry();
        logger.info("The PeerAwareInstanceRegistry has been initialized");
        return registry;
    }

    private void replicateInstance(InstanceInfo instance, Action action) throws IOException {
        if (instance == null) {
            return;
        }
        HttpServletRequest request = getHttpServletRequest();
        if (request == null) {
            return;
        }
        doReplicateInstance(instance, action);
        logger.info("[Action : '{}'] {} is about to be replicated!", action, instance);
    }

    private void doReplicateInstance(InstanceInfo instance, Action action) throws IOException {
        Map<String, String> metadata = instance.getMetadata();
        metadata.put(actionMetadataKey, action.name());

        ServletContext servletContext = this.servletContext;
        String json = codecWrapper.encode(instance);
        String name = this.instanceNamePrefix + instance.getId();
        servletContext.setAttribute(name, json);
        // remove "action" metadata and attribute after replication
        metadata.remove(actionMetadataKey);
        servletContext.removeAttribute(name);
    }

    private HttpServletRequest getHttpServletRequest() {
        RequestAttributes requestAttributes = getRequestAttributes();
        if (requestAttributes instanceof ServletRequestAttributes) {
            ServletRequestAttributes attributes = (ServletRequestAttributes) requestAttributes;
            HttpServletRequest request = attributes.getRequest();
            return request;
        }
        // Non-Web Request
        return null;
    }

    public static boolean isEurekaServerContextAttributeName(String name) {
        return EUREKA_SERVER_CONTEXT_ATTRIBUTE_NAME.equals(name);
    }

    public static EurekaServerContext getEurekaServerContext(ServletContext servletContext) {
        return (EurekaServerContext) servletContext.getAttribute(EUREKA_SERVER_CONTEXT_ATTRIBUTE_NAME);
    }

    public static EurekaServerListener getEurekaServerListener(ServletContext servletContext) {
        return (EurekaServerListener) servletContext.getAttribute(EUREKA_SERVER_LISTENER_ATTRIBUTE_NAME);
    }
}