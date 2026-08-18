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

import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.converters.wrappers.CodecWrapper;
import com.netflix.discovery.shared.Application;
import com.netflix.discovery.shared.Applications;
import com.netflix.eureka.EurekaServerConfig;
import com.netflix.eureka.EurekaServerContext;
import com.netflix.eureka.registry.PeerAwareInstanceRegistry;
import com.netflix.eureka.registry.PeerAwareInstanceRegistryImpl.Action;
import com.netflix.eureka.resources.ServerCodecs;
import io.microsphere.annotation.Nonnull;
import io.microsphere.annotation.Nullable;
import io.microsphere.logging.Logger;
import io.microsphere.netflix.eureka.spring.cloud.EurekaServerProperties;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletContextAttributeListener;
import jakarta.servlet.ServletContextEvent;
import jakarta.servlet.ServletContextListener;
import org.apache.catalina.tribes.ChannelListener;
import org.apache.catalina.tribes.Member;
import org.apache.catalina.tribes.tipis.AbstractReplicatedMap.MapMessage;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static com.netflix.eureka.registry.PeerAwareInstanceRegistryImpl.Action.Register;
import static com.netflix.eureka.registry.PeerAwareInstanceRegistryImpl.Action.valueOf;
import static io.microsphere.logging.LoggerFactory.getLogger;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.catalina.tribes.tipis.AbstractReplicatedMap.MapMessage.MSG_COPY;

/**
 * Replicated Instance Listener implements
 * <ul>
 *     <li>{@link ServletContextListener}</li>
 *     <li>{@link ServletContextAttributeListener}</li>
 *     <li>{@link ChannelListener}</li>
 * </ul>
 *
 * @author <a href="mailto:mercyblitz@gmail.com">Mercy</a>
 * @see EurekaServerListener
 * @see ServletContextListener
 * @see ServletContextAttributeListener
 * @see ChannelListener
 * @since 1.0.0
 */
public class ReplicatedInstanceListener implements ServletContextListener, ChannelListener {

    private static final Logger logger = getLogger(ReplicatedInstanceListener.class);

    private final EurekaServerContext eurekaServerContext;

    private final EurekaServerProperties eurekaServerProperties;

    private final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

    private ServletContext servletContext;

    public ReplicatedInstanceListener(EurekaServerContext eurekaServerContext, EurekaServerProperties eurekaServerProperties) {
        this.eurekaServerContext = eurekaServerContext;
        this.eurekaServerProperties = eurekaServerProperties;
        this.scheduledThreadPoolExecutor = newScheduledThreadPoolExecutor();
    }

    private ScheduledThreadPoolExecutor newScheduledThreadPoolExecutor() {
        int threads = this.eurekaServerProperties.getThreadsNumber();
        CustomizableThreadFactory threadFactory = new CustomizableThreadFactory(this.eurekaServerProperties.getThreadNamePrefix());
        threadFactory.setDaemon(true);
        ScheduledThreadPoolExecutor scheduledExecutorService = new ScheduledThreadPoolExecutor(threads, threadFactory);
        return scheduledExecutorService;
    }

    @Override
    public void contextInitialized(ServletContextEvent event) {
        this.scheduledThreadPoolExecutor.prestartCoreThread();
        this.servletContext = event.getServletContext();
        processRegisteredInstancesOnSchedule();
    }

    private void processRegisteredInstancesOnSchedule() {
        EurekaServerContext eurekaServerContext = getEurekaServerContext();
        EurekaServerConfig serverConfig = eurekaServerContext.getServerConfig();
        // the value of period is 1/3 of the peer eureka status refresh time interval, whose default value is 10 seconds
        long period = serverConfig.getPeerEurekaStatusRefreshTimeIntervalMs() / 3;
        this.scheduledThreadPoolExecutor.scheduleAtFixedRate(this::processRegisteredInstances, 0, period, MILLISECONDS);
    }

    private void processRegisteredInstances() {
        async(() -> {
            PeerAwareInstanceRegistry registry = getRegistry();
            Applications applications = registry.getApplications();
            List<Application> registeredApplications = applications.getRegisteredApplications();
            for (Application application : registeredApplications) {
                for (InstanceInfo registeredInstance : application.getInstances()) {
                    process(registeredInstance, null);
                }
            }
        });
    }

    @Override
    public void contextDestroyed(ServletContextEvent event) {
        scheduledThreadPoolExecutor.shutdown();
        logger.info("The {} is shutting down", this.scheduledThreadPoolExecutor);
    }

    @Override
    public void messageReceived(Serializable msg, Member sender) {
        MapMessage mapMessage = (MapMessage) msg;
        if (mapMessage.getMsgType() != MSG_COPY) {
            return;
        }

        Object key = mapMessage.getKey();

        if (key instanceof String) {
            String name = (String) key;
            if (isReplicateInstanceName(name)) {
                async(() -> {
                    String json = (String) mapMessage.getValue();
                    process(json);
                });
            }
        }
    }

    private InstanceInfo decodeReplicatedInstance(String json) throws IOException {
        CodecWrapper codecWrapper = getCodecWrapper();
        return codecWrapper.decode(json, InstanceInfo.class);
    }

    @FunctionalInterface
    private interface Task {
        void execute() throws Exception;
    }

    private void async(Task task) {
        scheduledThreadPoolExecutor.submit(() -> {
            task.execute();
            return null;
        });
    }

    private boolean isReplicateInstanceName(String name) {
        return name.startsWith(this.eurekaServerProperties.getInstanceNamePrefix());
    }

    private void process(String json) throws IOException {
        InstanceInfo replicatedInstance = decodeReplicatedInstance(json);
        process(replicatedInstance, json);
    }

    private void process(InstanceInfo replicatedInstance, @Nullable String json) {
        PeerAwareInstanceRegistry registry = getRegistry();
        Action action = getAction(replicatedInstance);
        String appName = replicatedInstance.getAppName();
        String id = replicatedInstance.getId();
        boolean isReplication = json != null;

        logger.info("The replicated instance[appName : '{}' , id : '{}' , action : '{}' , isReplication : {}] is processing, json : '{}'",
                appName, id, action, isReplication, json);

        switch (action) {
            case Register:
                register(registry, replicatedInstance, isReplication);
                break;
            case Cancel:
                cancel(registry, replicatedInstance, isReplication);
                break;
            case Heartbeat:
                renew(registry, replicatedInstance, isReplication);
                break;
        }
        servletContext.removeAttribute(id);
    }

    private Action getAction(InstanceInfo replicatedInstance) {
        Map<String, String> metadata = replicatedInstance.getMetadata();
        // remove "action" metadata after replication
        String actionName = metadata.remove(this.eurekaServerProperties.getActionKey());
        return actionName == null ? Register : valueOf(actionName);
    }

    private void register(PeerAwareInstanceRegistry registry, InstanceInfo replicatedInstance, boolean isReplication) {
        registry.register(replicatedInstance, isReplication);
        logger.info("The replicated instance[id : {}] has been registered", replicatedInstance.getId());
    }

    private void cancel(PeerAwareInstanceRegistry registry, InstanceInfo replicatedInstance, boolean isReplication) {
        String appName = replicatedInstance.getAppName();
        String serviceInstanceId = replicatedInstance.getId();
        InstanceInfo instanceInfo = registry.getInstanceByAppAndId(appName, serviceInstanceId);
        if (instanceInfo == null) {
            logger.info("The replicated instance[id : {}] was not found, the cancel will be ignored", serviceInstanceId);
            return;
        }
        registry.cancel(appName, serviceInstanceId, isReplication);
        logger.info("The replicated instance[id : {}] has been cancelled", serviceInstanceId);
    }

    private void renew(PeerAwareInstanceRegistry registry, InstanceInfo replicatedInstance, boolean isReplication) {
        String appName = replicatedInstance.getAppName();
        String serviceInstanceId = replicatedInstance.getId();
        InstanceInfo instanceInfo = registry.getInstanceByAppAndId(appName, serviceInstanceId);
        if (instanceInfo == null) {
            logger.info("The replicated instance[id : {}] was not found, thus it will be registered", serviceInstanceId);
            register(registry, replicatedInstance, isReplication);
        } else {
            registry.renew(appName, serviceInstanceId, isReplication);
            logger.info("The replicated instance[id : {}] was renewed", serviceInstanceId);
        }
    }

    @Override
    public boolean accept(Serializable msg, Member sender) {
        return msg instanceof MapMessage;
    }

    private CodecWrapper getCodecWrapper() {
        EurekaServerContext eurekaServerContext = getEurekaServerContext();
        CodecWrapper codecWrapper = null;
        if (eurekaServerContext != null) {
            ServerCodecs serverCodecs = eurekaServerContext.getServerCodecs();
            codecWrapper = serverCodecs.getFullJsonCodec();
        }
        return codecWrapper;
    }

    @Nonnull
    private PeerAwareInstanceRegistry getRegistry() {
        return getEurekaServerContext().getRegistry();
    }

    @Nonnull
    private EurekaServerContext getEurekaServerContext() {
        return this.eurekaServerContext;
    }
}