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
package io.microsphere.netflix.eureka.server.spring.cloud.tomcat.servlet.listener;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.converters.wrappers.CodecWrapper;
import com.netflix.discovery.shared.Application;
import com.netflix.discovery.shared.Applications;
import com.netflix.eureka.EurekaServerConfig;
import com.netflix.eureka.EurekaServerContext;
import com.netflix.eureka.registry.PeerAwareInstanceRegistry;
import com.netflix.eureka.registry.PeerAwareInstanceRegistryImpl.Action;
import io.microsphere.annotation.Nonnull;
import io.microsphere.annotation.Nullable;
import io.microsphere.logging.Logger;
import io.microsphere.netflix.eureka.server.spring.cloud.EurekaServerProperties;
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
import static io.microsphere.netflix.eureka.server.spring.cloud.tomcat.servlet.listener.EurekaServerListener.getCodecWrapper;
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

    private static final String ATTRIBUTE_NAME = ReplicatedInstanceListener.class.getName();

    private final EurekaServerContext eurekaServerContext;

    private final EurekaServerProperties eurekaServerProperties;

    private final CodecWrapper codecWrapper;

    private final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

    private ServletContext servletContext;

    public ReplicatedInstanceListener(EurekaServerContext eurekaServerContext, EurekaServerProperties eurekaServerProperties) {
        this.eurekaServerContext = eurekaServerContext;
        this.eurekaServerProperties = eurekaServerProperties;
        this.codecWrapper = getCodecWrapper(eurekaServerContext);
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
        this.servletContext.setAttribute(ATTRIBUTE_NAME, this);
        processRegisteredInstancesOnSchedule();
    }

    private void processRegisteredInstancesOnSchedule() {
        EurekaServerContext eurekaServerContext = getEurekaServerContext();
        EurekaServerConfig serverConfig = eurekaServerContext.getServerConfig();
        // the value of period is 1/3 of the peer eureka status refresh time interval, whose default value is 10 seconds
        long period = serverConfig.getPeerEurekaStatusRefreshTimeIntervalMs() / 3;
        this.scheduledThreadPoolExecutor.prestartAllCoreThreads();
        this.scheduledThreadPoolExecutor.scheduleAtFixedRate(this::processRegisteredInstancesAsync, 0, period, MILLISECONDS);
    }

    private void processRegisteredInstancesAsync() {
        async(this::processRegisteredInstances);
    }

    void processRegisteredInstances() {
        PeerAwareInstanceRegistry registry = getRegistry();
        Applications applications = registry.getApplications();
        List<Application> registeredApplications = applications.getRegisteredApplications();
        for (Application application : registeredApplications) {
            for (InstanceInfo registeredInstance : application.getInstances()) {
                process(registeredInstance, null);
            }
        }
    }

    @Override
    public void contextDestroyed(ServletContextEvent event) {
        stop();
    }

    public void stop() {
        if (!scheduledThreadPoolExecutor.isShutdown()) {
            scheduledThreadPoolExecutor.shutdown();
            logger.info("The {} is shutting down", this.scheduledThreadPoolExecutor);
        }
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

    InstanceInfo decodeReplicatedInstance(String json) throws IOException {
        return this.codecWrapper.decode(json, InstanceInfo.class);
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

    void process(String json) throws IOException {
        InstanceInfo replicatedInstance = decodeReplicatedInstance(json);
        process(replicatedInstance, json);
    }

    void process(InstanceInfo replicatedInstance, @Nullable String json) {
        Action action = getAction(replicatedInstance);
        process(replicatedInstance, action, json);
    }

    private void process(InstanceInfo replicatedInstance, Action action, @Nullable String json) {
        PeerAwareInstanceRegistry registry = getRegistry();
        String appName = replicatedInstance.getAppName();
        String id = replicatedInstance.getId();
        boolean isReplication = json != null;

        logger.info("The replicated instance[appName : '{}' , id : '{}' , action : '{}' , isReplication : {}] is processing, json : '{}'",
                appName, id, action, isReplication, json);

        switch (action) {
            case Heartbeat:
                renew(registry, replicatedInstance, isReplication);
                break;
            case Cancel:
                cancel(registry, replicatedInstance, isReplication);
                break;
            default:
                register(registry, replicatedInstance, isReplication);
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

    @Nonnull
    public PeerAwareInstanceRegistry getRegistry() {
        return getEurekaServerContext().getRegistry();
    }

    @Nonnull
    public EurekaServerContext getEurekaServerContext() {
        return this.eurekaServerContext;
    }

    /**
     * Get the {@link ReplicatedInstanceListener} from the {@link ServletContext}
     *
     * @param servletContext the {@link ServletContext}
     * @return the {@link ReplicatedInstanceListener} if found, or {@code null} if not found
     */
    @Nullable
    public static ReplicatedInstanceListener get(ServletContext servletContext) {
        return (ReplicatedInstanceListener) servletContext.getAttribute(ATTRIBUTE_NAME);
    }

    @FunctionalInterface
    interface Task {
        void execute() throws Exception;
    }
}