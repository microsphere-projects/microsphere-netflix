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


import com.netflix.appinfo.ApplicationInfoManager;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.eureka.registry.PeerAwareInstanceRegistry;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletContextEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.netflix.eureka.server.EnableEurekaServer;
import org.springframework.cloud.netflix.eureka.serviceregistry.EurekaRegistration;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

/**
 * {@link EurekaServerListener} Test
 *
 * @author <a href="mailto:mercyblitz@gmail.com">Mercy</a>
 * @see EurekaServerListener
 * @since 1.0.0
 */
@SpringBootTest(
        classes = {
                EurekaServerListenerTest.class
        },
        properties = {
                "microsphere.eureka.server.deregistration.delay=500"
        },
        webEnvironment = RANDOM_PORT
)
@EnableEurekaServer
@EnableAutoConfiguration
class EurekaServerListenerTest {

    @Autowired
    private EurekaRegistration eurekaRegistration;

    @Autowired
    private PeerAwareInstanceRegistry registry;

    @Autowired
    private EurekaServerListener eurekaServerListener;

    @Autowired
    private ServletContext servletContext;

    private ApplicationInfoManager applicationInfoManager;

    private InstanceInfo instanceInfo;

    @BeforeEach
    void setUp() {
        this.applicationInfoManager = this.eurekaRegistration.getApplicationInfoManager();
        this.instanceInfo = this.applicationInfoManager.getInfo();
    }

    @Test
    void test() {
        assertInstanceInfo(false);
        assertInstanceInfo(true);
    }

    void assertInstanceInfo(boolean isReplication) {
        this.eurekaServerListener.deregistered = false;
        String appName = this.instanceInfo.getAppName();
        String instanceId = this.instanceInfo.getInstanceId();
        assertDoesNotThrow(() -> this.registry.register(this.instanceInfo, isReplication));
        assertTrue(this.registry.renew(appName, instanceId, isReplication));
        assertTrue(this.registry.cancel(appName, instanceId, isReplication));

        ServletContextEvent event = new ServletContextEvent(this.servletContext);
        this.eurekaServerListener.contextDestroyed(event);

        assertDoesNotThrow(() -> this.registry.register(this.instanceInfo, isReplication));
        this.eurekaServerListener.destroy();
        this.eurekaServerListener.deregister();
    }
}