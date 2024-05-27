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
import com.netflix.eureka.registry.PeerAwareInstanceRegistryImpl;
import com.netflix.eureka.resources.ServerCodecs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.netflix.eureka.server.event.EurekaInstanceCanceledEvent;
import org.springframework.cloud.netflix.eureka.server.event.EurekaInstanceRegisteredEvent;
import org.springframework.cloud.netflix.eureka.server.event.EurekaInstanceRenewedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Map;

import static com.netflix.eureka.registry.PeerAwareInstanceRegistryImpl.Action.Cancel;
import static io.microsphere.netflix.eureka.spring.cloud.tomcat.servlet.listener.ReplicatedInstanceListener.ACTION_METADATA_KEY;
import static io.microsphere.netflix.eureka.spring.cloud.tomcat.servlet.listener.ReplicatedInstanceListener.REPLICATION_INSTANCE_NAME_PREFIX;
import static org.springframework.web.context.request.RequestContextHolder.getRequestAttributes;

/**
 * Customized EurekaServer Listener
 *
 * @author <a href="mailto:mercyblitz@gmail.com">Mercy</a>
 * @since 1.0.0
 */
public class EurekaServerListener implements ServletContextListener {

    private static final Logger logger = LoggerFactory.getLogger(EurekaServerListener.class);

    public static final String EUREKA_SERVER_LISTENER_ATTRIBUTE_NAME = "EurekaServerListener";

    public static final String EUREKA_SERVER_CONTEXT_ATTRIBUTE_NAME = "EurekaServerContext";

    private EurekaServerContext eurekaServerContext;

    private EurekaInstanceConfig eurekaInstanceConfig;

    private CodecWrapper codecWrapper;

    private PeerAwareInstanceRegistry registry;

    private ServletContext servletContext;

    private volatile boolean deregistered = false;

    @Value(("${microsphere.eureka.instance.deregister.delay:3}"))
    private long deregisterDelay;

    @Autowired
    public void init(EurekaServerContext eurekaServerContext,
                     EurekaInstanceConfig eurekaInstanceConfig) {
        this.eurekaServerContext = eurekaServerContext;
        this.eurekaInstanceConfig = eurekaInstanceConfig;
        initCodecWrapper(eurekaServerContext);
        initPeerAwareInstanceRegistry(eurekaServerContext);
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

    private void initEurekaServerContext() {
        String name = EUREKA_SERVER_CONTEXT_ATTRIBUTE_NAME;
        servletContext.setAttribute(name, eurekaServerContext);
        logger.info("The EurekaServerContext has been initialized into the ServletContext with name : {}", name);
    }

    private void initCodecWrapper(EurekaServerContext eurekaServerContext) {
        ServerCodecs serverCodecs = eurekaServerContext.getServerCodecs();
        this.codecWrapper = serverCodecs.getFullJsonCodec();
        logger.info("The CodecWrapper has been initialized");
    }

    private void initPeerAwareInstanceRegistry(EurekaServerContext eurekaServerContext) {
        this.registry = eurekaServerContext.getRegistry();
        logger.info("The PeerAwareInstanceRegistry has been initialized");
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
            for (int i = 0; i < deregisterDelay; i++) {
                doReplicateInstance(instance, Cancel);
                Thread.sleep(1000L);
            }
            logger.info("The current instance[appName : {} , id : {}] was deregistered before {}s!", appName, id, deregisterDelay);
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }
        deregistered = true;
    }

    @EventListener(EurekaInstanceRegisteredEvent.class)
    public void onEurekaInstanceRegisteredEvent(EurekaInstanceRegisteredEvent event) throws Throwable {
        if (event.isReplication()) {
            return;
        }
        InstanceInfo instance = event.getInstanceInfo();
        replicateInstance(instance, PeerAwareInstanceRegistryImpl.Action.Register);
    }

    @EventListener(EurekaInstanceCanceledEvent.class)
    public void onEurekaInstanceCanceledEvent(EurekaInstanceCanceledEvent event) throws Throwable {
        if (event.isReplication()) {
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
            return;
        }
        InstanceInfo instance = event.getInstanceInfo();
        replicateInstance(instance, PeerAwareInstanceRegistryImpl.Action.Heartbeat);
    }

    private void replicateInstance(InstanceInfo instance, PeerAwareInstanceRegistryImpl.Action action) throws IOException {
        if (instance == null) {
            return;
        }
        HttpServletRequest request = getHttpServletRequest();
        if (request == null) {
            return;
        }
        doReplicateInstance(instance, action);
        logger.info("InstanceInfo[appName : {} , id : {} , action : {}] has been replicated",
                instance.getAppName(), instance.getId(), action);
    }

    private void doReplicateInstance(InstanceInfo instance, PeerAwareInstanceRegistryImpl.Action action) throws IOException {
        Map<String, String> metadata = instance.getMetadata();
        metadata.put(ACTION_METADATA_KEY, action.name());

        ServletContext servletContext = this.servletContext;
        String json = codecWrapper.encode(instance);
        String name = REPLICATION_INSTANCE_NAME_PREFIX + instance.getId();
        servletContext.setAttribute(name, json);
        // remove "action" metadata and attribute after replication
        metadata.remove(ACTION_METADATA_KEY);
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
}
