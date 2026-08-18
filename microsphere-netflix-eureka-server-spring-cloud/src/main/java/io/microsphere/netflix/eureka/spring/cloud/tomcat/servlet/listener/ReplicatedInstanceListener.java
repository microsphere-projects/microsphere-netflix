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
import com.netflix.eureka.EurekaServerContext;
import com.netflix.eureka.registry.PeerAwareInstanceRegistry;
import com.netflix.eureka.registry.PeerAwareInstanceRegistryImpl.Action;
import com.netflix.eureka.resources.ServerCodecs;
import io.microsphere.logging.Logger;
import io.microsphere.netflix.eureka.spring.cloud.EurekaServerProperties;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletContextAttributeEvent;
import jakarta.servlet.ServletContextAttributeListener;
import jakarta.servlet.ServletContextEvent;
import jakarta.servlet.ServletContextListener;
import org.apache.catalina.tribes.ChannelListener;
import org.apache.catalina.tribes.Member;
import org.apache.catalina.tribes.tipis.AbstractReplicatedMap.MapMessage;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.DiscardOldestPolicy;

import static com.netflix.eureka.registry.PeerAwareInstanceRegistryImpl.Action.valueOf;
import static io.microsphere.logging.LoggerFactory.getLogger;
import static io.microsphere.netflix.eureka.spring.cloud.tomcat.servlet.listener.EurekaServerListener.isEurekaServerContextAttributeName;
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
 * @see ServletContextListener
 * @see ServletContextAttributeListener
 * @see ChannelListener
 * @since 1.0.0
 */
public class ReplicatedInstanceListener implements ServletContextListener, ServletContextAttributeListener, ChannelListener {

    private static final Logger logger = getLogger(ReplicatedInstanceListener.class);

    private final Object mutex = new Object();

    private volatile EurekaServerContext eurekaServerContext;

    private final EurekaServerProperties eurekaServerProperties;

    private final ThreadPoolExecutor threadPoolExecutor;

    private ServletContext servletContext;

    public ReplicatedInstanceListener(EurekaServerProperties eurekaServerProperties) {
        this.eurekaServerProperties = eurekaServerProperties;
        this.threadPoolExecutor = createThreadPoolExecutor();
    }

    private ThreadPoolExecutor createThreadPoolExecutor() {
        BlockingQueue<Runnable> blockingQueue = new ArrayBlockingQueue<>(this.eurekaServerProperties.getMaxQueueCapacity());
        CustomizableThreadFactory threadFactory = new CustomizableThreadFactory(this.eurekaServerProperties.getThreadNamePrefix());
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(this.eurekaServerProperties.getMinThreads(), this.eurekaServerProperties.getMaxThreads(), 0, MILLISECONDS,
                blockingQueue, threadFactory, new DiscardOldestPolicy());
        return threadPoolExecutor;
    }

    @Override
    public void contextInitialized(ServletContextEvent event) {
        this.threadPoolExecutor.prestartCoreThread();
        this.servletContext = event.getServletContext();
        processReceivedReplicationInstances(this.servletContext);
    }

    private void processReceivedReplicationInstances(ServletContext servletContext) {
        Enumeration<String> attributeNames = servletContext.getAttributeNames();
        while (attributeNames.hasMoreElements()) {
            String attributeName = attributeNames.nextElement();
            if (isReplicateInstanceName(attributeName)) {
                async(() -> {
                    String json = (String) servletContext.getAttribute(attributeName);
                    InstanceInfo replicatedInstance = decodeReplicatedInstance(attributeName);
                    process(replicatedInstance, json);
                });
            }
        }
    }

    @Override
    public void attributeAdded(ServletContextAttributeEvent event) {
        String name = event.getName();
        Object value = event.getValue();
        logger.info("The ServletContext attribute[name : {} , value : {}] was added!", name, value);

        if (isEurekaServerContextAttributeName(name)) {
            EurekaServerContext eurekaServerContext = getEurekaServerContext();
            if (eurekaServerContext != null) {
                synchronized (mutex) {
                    mutex.notifyAll();
                }
                logger.info("EurekaServerContext is ready , the process will be resumed");
            }
        }
    }

    @Override
    public void attributeRemoved(ServletContextAttributeEvent event) {
        String name = event.getName();
        Object value = event.getValue();
        logger.info("The ServletContext attribute[name : {} , value : {}] was removed!", name, value);
    }

    @Override
    public void attributeReplaced(ServletContextAttributeEvent event) {
        String name = event.getName();
        Object value = event.getValue();
        logger.info("The ServletContext attribute[name : {} , value : {}] was replaced!", name, value);
    }

    @Override
    public void contextDestroyed(ServletContextEvent event) {
        threadPoolExecutor.shutdown();
        logger.info("The {} is shutting down", this.threadPoolExecutor);
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
                    synchronized (mutex) {
                        while ((getEurekaServerContext()) == null) {
                            try {
                                logger.info("EurekaServerContext is not ready , Handling {} will be blocked", name);
                                mutex.wait();
                            } catch (InterruptedException e) {
                                logger.error(e.getMessage(), e);
                            }
                        }
                    }

                    String json = (String) mapMessage.getValue();
                    InstanceInfo replicatedInstance = decodeReplicatedInstance(json);
                    process(replicatedInstance, json);
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
        void execute() throws Throwable;
    }

    private void async(Task task) {
        threadPoolExecutor.execute(() -> {
            try {
                task.execute();
            } catch (Throwable e) {
                logger.error("Handling The replicated instance is failed", e);
            }
        });
    }

    private boolean isReplicateInstanceName(String name) {
        return name.startsWith(this.eurekaServerProperties.getInstanceNamePrefix());
    }

    private void process(InstanceInfo replicatedInstance, String json) {
        Action action = getAction(replicatedInstance);
        String appName = replicatedInstance.getAppName();
        String id = replicatedInstance.getId();
        PeerAwareInstanceRegistry registry = getRegistry();
        logger.info("The replicated instance[appName : {} , id : {} , action : {}] is processing, json : {}", appName, id, action, json);
        if (registry == null) {
            logger.warn("The PeerAwareInstanceRegistry is not ready, the process will be cancelled");
            return;
        }

        switch (action) {
            case Register:
                register(registry, replicatedInstance);
                break;
            case Cancel:
                cancel(registry, replicatedInstance);
                break;
            case Heartbeat:
                renew(registry, replicatedInstance);
                break;
        }
        servletContext.removeAttribute(id);
    }

    private Action getAction(InstanceInfo replicatedInstance) {
        Map<String, String> metadata = replicatedInstance.getMetadata();
        // remove "action" metadata after replication
        String actionName = metadata.remove(this.eurekaServerProperties.getActionKey());
        return valueOf(actionName);
    }

    private void register(PeerAwareInstanceRegistry registry, InstanceInfo replicatedInstance) {
        registry.register(replicatedInstance, true);
        logger.info("The replicated instance[id : {}] has been registered", replicatedInstance.getId());
    }

    private void cancel(PeerAwareInstanceRegistry registry, InstanceInfo replicatedInstance) {
        String appName = replicatedInstance.getAppName();
        String serviceInstanceId = replicatedInstance.getId();
        InstanceInfo instanceInfo = registry.getInstanceByAppAndId(appName, serviceInstanceId);
        if (instanceInfo == null) {
            logger.info("The replicated instance[id : {}] was not found, the cancel will be ignored", serviceInstanceId);
            return;
        }
        registry.cancel(appName, serviceInstanceId, true);
        logger.info("The replicated instance[id : {}] has been cancelled", serviceInstanceId);
    }

    private void renew(PeerAwareInstanceRegistry registry, InstanceInfo replicatedInstance) {
        String appName = replicatedInstance.getAppName();
        String serviceInstanceId = replicatedInstance.getId();
        InstanceInfo instanceInfo = registry.getInstanceByAppAndId(appName, serviceInstanceId);
        if (instanceInfo == null) {
            logger.info("The replicated instance[id : {}] was not found, thus it will be registered", serviceInstanceId);
            register(registry, replicatedInstance);
        } else {
            registry.renew(appName, serviceInstanceId, true);
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

    private PeerAwareInstanceRegistry getRegistry() {
        EurekaServerContext eurekaServerContext = getEurekaServerContext();
        if (eurekaServerContext != null) {
            return eurekaServerContext.getRegistry();
        }
        return null;
    }

    private EurekaServerContext getEurekaServerContext() {
        EurekaServerContext eurekaServerContext = this.eurekaServerContext;
        if (eurekaServerContext == null) {
            synchronized (mutex) {
                eurekaServerContext = this.eurekaServerContext;
                if (eurekaServerContext == null) {
                    eurekaServerContext = EurekaServerListener.getEurekaServerContext(this.servletContext);
                    this.eurekaServerContext = eurekaServerContext;
                }
            }
        }
        return eurekaServerContext;
    }
}