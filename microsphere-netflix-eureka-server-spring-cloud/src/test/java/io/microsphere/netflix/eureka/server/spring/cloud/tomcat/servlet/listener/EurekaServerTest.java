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
import com.netflix.eureka.EurekaServerContext;
import com.netflix.eureka.registry.PeerAwareInstanceRegistry;
import io.microsphere.netflix.eureka.server.spring.cloud.EurekaServerProperties;
import jakarta.servlet.Servlet;
import jakarta.servlet.ServletContext;
import jakarta.servlet.annotation.WebListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.netflix.eureka.server.EnableEurekaServer;
import org.springframework.cloud.netflix.eureka.server.EurekaServerMarkerConfiguration;
import org.springframework.cloud.netflix.eureka.serviceregistry.EurekaRegistration;
import org.springframework.web.context.ConfigurableWebApplicationContext;

import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.boot.Banner.Mode.OFF;
import static org.springframework.boot.SpringApplication.exit;
import static org.springframework.boot.WebApplicationType.SERVLET;

/**
 * Abstract class for EurekaServer testing.
 *
 * @author <a href="mailto:mercyblitz@gmail.com">Mercy</a>
 * @see Servlet
 * @see WebListener
 * @see EnableAutoConfiguration
 * @see EnableEurekaServer
 * @since 1.0.0
 */
@EnableAutoConfiguration
public abstract class EurekaServerTest {

    protected ConfigurableWebApplicationContext webApplicationContext;

    protected ServletContext servletContext;

    protected EurekaServerContext eurekaServerContext;

    protected EurekaServerProperties eurekaServerProperties;

    protected PeerAwareInstanceRegistry registry;

    private EurekaRegistration eurekaRegistration;

    private ApplicationInfoManager applicationInfoManager;

    protected InstanceInfo instanceInfo;

    @BeforeEach
    void setUp() throws Throwable {
        SpringApplicationBuilder builder = new SpringApplicationBuilder(getClass())
                .sources(EurekaServerMarkerConfiguration.class) // The Configuration class was imported by @EnableEurekaServer
                .web(SERVLET)
                .bannerMode(OFF)
                .headless(true)
                .properties(getDefaultProperties());

        this.webApplicationContext = (ConfigurableWebApplicationContext) builder.run();
        this.eurekaServerContext = this.webApplicationContext.getBean(EurekaServerContext.class);
        this.eurekaServerProperties = this.webApplicationContext.getBean(EurekaServerProperties.class);
        this.servletContext = this.webApplicationContext.getServletContext();
        this.registry = this.webApplicationContext.getBean(PeerAwareInstanceRegistry.class);
        this.eurekaRegistration = this.webApplicationContext.getBean(EurekaRegistration.class);
        this.applicationInfoManager = this.eurekaRegistration.getApplicationInfoManager();
        this.instanceInfo = this.applicationInfoManager.getInfo();
        init();
    }

    @AfterEach
    void tearDown() throws Throwable {
        destroy();
        assertEquals(0, exit(this.webApplicationContext));
    }

    protected void init() throws Throwable {
    }

    protected Map<String, Object> getDefaultProperties() {
        return emptyMap();
    }

    private void destroy() throws Throwable {
    }

}