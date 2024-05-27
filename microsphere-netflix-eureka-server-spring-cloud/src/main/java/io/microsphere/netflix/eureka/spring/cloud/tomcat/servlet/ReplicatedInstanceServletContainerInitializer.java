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
package io.microsphere.netflix.eureka.spring.cloud.tomcat.servlet;

import io.microsphere.netflix.eureka.spring.cloud.tomcat.servlet.listener.EurekaServerListener;
import io.microsphere.netflix.eureka.spring.cloud.tomcat.servlet.listener.ReplicatedInstanceListener;
import org.apache.catalina.Cluster;
import org.apache.catalina.Container;
import org.apache.catalina.Context;
import org.apache.catalina.Engine;
import org.apache.catalina.Host;
import org.apache.catalina.Lifecycle;
import org.apache.catalina.Service;
import org.apache.catalina.core.ApplicationContext;
import org.apache.catalina.ha.CatalinaCluster;
import org.apache.catalina.tribes.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContainerInitializer;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import java.util.Set;

import static org.springframework.util.ReflectionUtils.doWithFields;
import static org.springframework.util.ReflectionUtils.makeAccessible;

/**
 * Replicated Instance {@link ServletContainerInitializer}
 *
 * @author <a href="mailto:mercyblitz@gmail.com">Mercy</a>
 * @see ServletContainerInitializer
 * @since 1.0.0
 */
public class ReplicatedInstanceServletContainerInitializer implements ServletContainerInitializer {

    private static final Logger logger = LoggerFactory.getLogger(ReplicatedInstanceServletContainerInitializer.class);

    @Override
    public void onStartup(Set<Class<?>> c, ServletContext servletContext) throws ServletException {
        Class<?> servletContextClass = servletContext.getClass();
        String className = servletContextClass.getName();
        if ("org.apache.catalina.core.ApplicationContextFacade".equals(className)) {
            doWithFields(servletContextClass, field -> {
                makeAccessible(field);
                ApplicationContext applicationContext = (ApplicationContext) field.get(servletContext);
                doWithFields(applicationContext.getClass(), f -> {
                            makeAccessible(f);
                            Context context = (Context) f.get(applicationContext);
                            if (context != null) {
                                Cluster cluster = context.getCluster();
                                initCluster(cluster, servletContext);
                                initService(context, servletContext);
                            }
                        },
                        f -> "context".equals(f.getName())
                                && Context.class.isAssignableFrom(f.getType()));
            }, field -> "context".equals(field.getName())
                    && ApplicationContext.class.isAssignableFrom(field.getType()));
        }
    }

    private void initCluster(Cluster cluster, ServletContext servletContext) {
        if (cluster instanceof CatalinaCluster) {
            CatalinaCluster catalinaCluster = (CatalinaCluster) cluster;
            Channel channel = catalinaCluster.getChannel();
            ReplicatedInstanceListener listener = new ReplicatedInstanceListener(servletContext);
            servletContext.addListener(listener);
            channel.addChannelListener(listener);
            logger.info("The ReplicatedInstanceListener was added");
        }
    }

    private void initService(Context context, ServletContext servletContext) {
        final Service service = findService(context);
        if (service != null) {
            service.addLifecycleListener(event -> {
                Object source = event.getSource();
                if (source == service) {
                    String type = event.getType();
                    if (Lifecycle.BEFORE_STOP_EVENT.equals(type)) {
                        EurekaServerListener eurekaServerListener = EurekaServerListener.getEurekaServerListener(servletContext);
                        eurekaServerListener.deregister();
                    }
                }
            });
            logger.info("The LifecycleListener was added");
        }
    }

    private Service findService(Context context) {
        Service service = null;

        Container parent = context.getParent();
        if (parent instanceof Host) {
            parent = parent.getParent();
            if (parent instanceof Engine) {
                Engine engine = (Engine) parent;
                service = engine.getService();
            }
        }

        return service;
    }
}
