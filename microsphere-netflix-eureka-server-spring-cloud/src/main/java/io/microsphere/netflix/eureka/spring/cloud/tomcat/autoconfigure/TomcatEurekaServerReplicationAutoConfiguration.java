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
package io.microsphere.netflix.eureka.spring.cloud.tomcat.autoconfigure;

import com.netflix.eureka.EurekaServerContext;
import io.microsphere.logging.Logger;
import io.microsphere.netflix.eureka.spring.cloud.EurekaServerProperties;
import io.microsphere.netflix.eureka.spring.cloud.tomcat.servlet.listener.EurekaServerListener;
import io.microsphere.netflix.eureka.spring.cloud.tomcat.servlet.listener.ReplicatedInstanceListener;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletContextAttributeEvent;
import jakarta.servlet.ServletContextAttributeListener;
import jakarta.servlet.ServletContextEvent;
import jakarta.servlet.ServletContextListener;
import org.apache.catalina.Cluster;
import org.apache.catalina.Context;
import org.apache.catalina.Host;
import org.apache.catalina.connector.Connector;
import org.apache.catalina.core.StandardContext;
import org.apache.catalina.ha.CatalinaCluster;
import org.apache.catalina.ha.ClusterRuleSet;
import org.apache.catalina.ha.tcp.SimpleTcpCluster;
import org.apache.catalina.tribes.Channel;
import org.apache.catalina.tribes.tipis.AbstractReplicatedMap;
import org.apache.catalina.tribes.tipis.ReplicatedMap;
import org.apache.tomcat.util.digester.Digester;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.embedded.tomcat.TomcatContextCustomizer;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.servlet.ServletContextInitializer;
import org.springframework.boot.web.servlet.server.ServletWebServerFactory;
import org.springframework.context.EmbeddedValueResolverAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.Resource;
import org.springframework.util.StringValueResolver;
import org.xml.sax.InputSource;

import java.io.InputStream;
import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.microsphere.logging.LoggerFactory.getLogger;
import static java.lang.Thread.currentThread;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication.Type.SERVLET;
import static org.springframework.util.ObjectUtils.isEmpty;
import static org.springframework.util.StreamUtils.copyToString;

/**
 * The Auto-{@link Configuration @Configuration} class for Netflix Eureka Server Replication on the Embedded Tomcat
 *
 * @author <a href="mailto:mercyblitz@gmail.com">Mercy</a>
 * @see EurekaServerListener
 * @see ReplicatedInstanceListener
 * @see org.springframework.boot.autoconfigure.web.servlet.ServletWebServerFactoryAutoConfiguration
 * @see org.springframework.boot.tomcat.autoconfigure.servlet.TomcatServletWebServerAutoConfiguration
 * @see org.springframework.cloud.netflix.eureka.server.EurekaServerAutoConfiguration
 * @see TomcatServletWebServerFactory
 * @see org.apache.catalina.startup.Tomcat
 * @see org.apache.catalina.ha.tcp.SimpleTcpCluster
 * @see org.apache.catalina.ha.ClusterRuleSet
 * @since 1.0.0
 */
@ConditionalOnClass(
        name = {
                "org.apache.catalina.startup.Tomcat",
                "org.apache.catalina.ha.tcp.SimpleTcpCluster",
                "org.apache.catalina.ha.ClusterRuleSet"
        })
@ConditionalOnWebApplication(type = SERVLET)
@AutoConfigureAfter(
        name = {
                "org.springframework.boot.autoconfigure.web.servlet.ServletWebServerFactoryAutoConfiguration",   // Spring Boot [2.0, 4.0)
                "org.springframework.boot.tomcat.autoconfigure.servlet.TomcatServletWebServerAutoConfiguration", // Spring Boot [4.0,)
                "org.springframework.cloud.netflix.eureka.server.EurekaServerAutoConfiguration"                  // Spring Cloud Netflix Eureka Server API
        }
)
@Import(value = {
        EurekaServerListener.class
})
@EnableConfigurationProperties(
        value = {
                EurekaServerProperties.class
        }
)
public class TomcatEurekaServerReplicationAutoConfiguration implements EmbeddedValueResolverAware, BeanClassLoaderAware,
        BeanNameAware, DisposableBean, AbstractReplicatedMap.MapOwner {

    private static final Logger logger = getLogger(TomcatEurekaServerReplicationAutoConfiguration.class);

    @Value("classpath:/META-INF/conf/cluster.xml")
    private Resource resource;

    private final EurekaServerContext eurekaServerContext;

    private final EurekaServerProperties eurekaServerProperties;

    private StringValueResolver resolver;

    private ClassLoader classLoader;

    private SimpleTcpCluster cluster;

    private ReplicatedMap<String, Object> replicatedMap;

    private ReplicatedInstanceListener replicatedInstanceListener;

    private String beanName;

    public TomcatEurekaServerReplicationAutoConfiguration(EurekaServerContext eurekaServerContext,
                                                          EurekaServerProperties eurekaServerProperties) {
        this.eurekaServerContext = eurekaServerContext;
        this.eurekaServerProperties = eurekaServerProperties;
    }

    @Bean
    public TomcatContextCustomizer installSimpleTcpClusterCustomizer(ConfigurableListableBeanFactory beanFactory) {
        return context -> {
            String[] beanNames = beanFactory.getBeanNamesForType(ServletWebServerFactory.class, false, false);
            if (isEmpty(beanNames)) {
                // Standard Tomcat
                logger.info("Current Eureka Server is running on the standard Tomcat Web Server");
                return;
            }
            logger.info("Current Eureka Server is initializing on the embedded Tomcat Web Server[name : {}]", beanNames[0]);
            // Embedded Tomcat
            initEmbeddedTomcat(context);
            beanFactory.registerSingleton(beanName + ".listener", new Listener());
        };
    }

    @Override
    public void destroy() throws Exception {
        stopReplication();
    }

    private void stopReplication() throws Exception {
        if (this.replicatedMap != null) {
            this.replicatedMap.breakdown();
        }
        if (this.cluster != null) {
            this.cluster.stop();
        }
    }

    private void initEmbeddedTomcat(Context context) {
        Host host = (Host) context.getParent();
        try {
            SimpleTcpCluster cluster = buildCluster();
            host.setCluster(cluster);
            this.cluster = cluster;
            initReplicatedInstanceListener(cluster);
            initReplicatedMap(context, cluster);
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void initReplicatedInstanceListener(Cluster cluster) {
        if (cluster instanceof CatalinaCluster) {
            CatalinaCluster catalinaCluster = (CatalinaCluster) cluster;
            Channel channel = catalinaCluster.getChannel();
            ReplicatedInstanceListener listener = new ReplicatedInstanceListener(this.eurekaServerContext, this.eurekaServerProperties);
            channel.addChannelListener(listener);

            this.replicatedInstanceListener = listener;
            logger.info("The ReplicatedInstanceListener was added");
        }
    }

    private void initReplicatedMap(Context context, SimpleTcpCluster cluster) {
        Channel channel = cluster.getChannel();
        String name = context.getName();
        ClassLoader[] classLoaders = getClassLoaders();
        long replicationTimeout = this.eurekaServerProperties.getReplicationTimeout();
        ReplicatedMap<String, Object> replicatedMap = new ReplicatedMap<>(this, channel, replicationTimeout, name, classLoaders);
        replicatedMap.setChannelSendOptions(cluster.getChannelSendOptions());
        this.replicatedMap = replicatedMap;
    }

    private ClassLoader[] getClassLoaders() {
        ClassLoader classLoader = this.classLoader;
        if (classLoader == null) {
            classLoader = currentThread().getContextClassLoader();
        }
        if (classLoader == currentThread().getContextClassLoader()) {
            return new ClassLoader[]{classLoader};
        } else {
            return new ClassLoader[]{classLoader, currentThread().getContextClassLoader()};
        }
    }

    private SimpleTcpCluster buildCluster() throws Throwable {
        SimpleTcpCluster cluster = new SimpleTcpCluster();
        parseCluster(cluster);
        return cluster;
    }

    private void parseCluster(SimpleTcpCluster cluster) throws Throwable {
        try (InputStream inputStream = resource.getInputStream()) {
            String xmlContent = copyToString(inputStream, UTF_8);
            Digester digester = createStartDigester();
            String resolvedXmlContent = resolver.resolveStringValue(xmlContent);
            InputSource inputSource = new InputSource(resource.getURI().toURL().toString());
            inputSource.setCharacterStream(new StringReader(resolvedXmlContent));
            digester.push(cluster);
            digester.parse(inputSource);
        }
    }

    private Digester createStartDigester() {
        // Initialize the digester
        Digester digester = new Digester();
        digester.setValidating(false);
        digester.setRulesValidation(true);
        Map<Class<?>, List<String>> fakeAttributes = new HashMap<>();
        // Ignore className on all elements
        List<String> objectAttrs = new ArrayList<>();
        objectAttrs.add("className");
        fakeAttributes.put(Object.class, objectAttrs);
        // Ignore attribute added by Eclipse for its internal tracking
        List<String> contextAttrs = new ArrayList<>();
        contextAttrs.add("source");
        fakeAttributes.put(StandardContext.class, contextAttrs);
        // Ignore Connector attribute used internally but set on Server
        List<String> connectorAttrs = new ArrayList<>();
        connectorAttrs.add("portOffset");
        fakeAttributes.put(Connector.class, connectorAttrs);
        digester.setFakeAttributes(fakeAttributes);
        digester.setUseContextClassLoader(true);

        // Configure the actions we will be using
        ClusterRuleSet clusterRuleSet = new ClusterRuleSet("Cluster/");
        digester.addRuleSet(clusterRuleSet);
        return digester;

    }

    @Override
    public void setEmbeddedValueResolver(StringValueResolver resolver) {
        this.resolver = resolver;
    }

    @Override
    public void setBeanClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    @Override
    public void setBeanName(String name) {
        this.beanName = name;
    }

    @Override
    public void objectMadePrimary(Object key, Object value) {
        // DO NOTHING
    }

    private class Listener implements ServletContextInitializer, ServletContextListener, ServletContextAttributeListener {

        @Override
        public void onStartup(ServletContext servletContext) {
            servletContext.addListener(replicatedInstanceListener);
            servletContext.addListener(this);
        }

        @Override
        public void contextInitialized(ServletContextEvent sce) {
        }

        @Override
        public void contextDestroyed(ServletContextEvent sce) {
            try {
                stopReplication();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void attributeAdded(ServletContextAttributeEvent event) {
            processServletContextAttributeEvent(event, false);
        }

        @Override
        public void attributeRemoved(ServletContextAttributeEvent event) {
            processServletContextAttributeEvent(event, true);
        }

        @Override
        public void attributeReplaced(ServletContextAttributeEvent event) {
            processServletContextAttributeEvent(event, false);
        }

        private void processServletContextAttributeEvent(ServletContextAttributeEvent event, boolean removed) {
            if (replicatedMap == null) {
                logger.warn("The ReplicatedMap is not ready!");
                return;
            }
            Object value = event.getValue();
            if (!(value instanceof Serializable)) {
                return;
            }

            String name = event.getName();

            if (removed) {
                replicatedMap.remove(name, value);
            } else {
                replicatedMap.put(name, value);
            }
            logger.info("The ServletContextAttributeEvent[name : {} , value : {} , removed : {}] has been processed!"
                    , name, value, removed);
        }
    }

}
