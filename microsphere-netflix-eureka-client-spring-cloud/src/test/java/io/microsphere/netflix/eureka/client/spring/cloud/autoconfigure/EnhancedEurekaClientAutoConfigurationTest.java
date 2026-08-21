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
package io.microsphere.netflix.eureka.client.spring.cloud.autoconfigure;

import com.netflix.appinfo.HealthCheckHandler;
import com.netflix.discovery.CacheRefreshedEvent;
import com.netflix.discovery.PreRegistrationHandler;
import io.microsphere.spring.cloud.client.service.registry.DefaultRegistration;
import io.microsphere.spring.cloud.client.service.registry.MultipleRegistration;
import io.microsphere.spring.cloud.client.service.registry.event.RegistrationPreRegisteredEvent;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.client.serviceregistry.Registration;
import org.springframework.cloud.client.serviceregistry.ServiceRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.microsphere.collection.Lists.ofList;
import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link EnhancedEurekaClientAutoConfiguration} Test
 *
 * @author <a href="mailto:mercyblitz@gmail.com">Mercy</a>
 * @since 1.0.0
 */
@SpringBootTest(
        classes = {
                EnhancedEurekaClientAutoConfigurationTest.class,
                EnhancedEurekaClientAutoConfigurationTest.Config.class
        },
        properties = {
                "spring.application.name=test-eureka",
                "eureka.client.serviceUrl.defaultZone=http://127.0.0.1:8761/eureka",
                "microsphere.spring.cloud.multiple-registration.enabled=true",
                "microsphere.spring.cloud.default-registration.type=org.springframework.cloud.netflix.eureka.serviceregistry.EurekaRegistration",
                "microsphere.spring.cloud.default-service-registry.type=org.springframework.cloud.netflix.eureka.serviceregistry.EurekaServiceRegistry",
        }
)
@EnableAutoConfiguration
public class EnhancedEurekaClientAutoConfigurationTest {

    private static final AtomicBoolean preRegistered = new AtomicBoolean(false);

    @Autowired
    private Registration registration;

    @Autowired
    private ServiceRegistry registry;

    @Autowired
    private EnhancedEurekaClientAutoConfiguration enhancedEurekaClientAutoConfiguration;

    static class Config {
        @Bean
        public PreRegistrationHandler preRegistrationHandler() {
            return () -> {
                preRegistered.set(true);
            };
        }

        @EventListener(CacheRefreshedEvent.class)
        public void onCacheRefreshedEvent(CacheRefreshedEvent event) {
            assertNotNull(event);
        }

        @Bean
        public HealthCheckHandler healthCheckHandler() {
            return currentStatus -> currentStatus;
        }
    }

    @Test
    public void test() throws Throwable {
        enhancedEurekaClientAutoConfiguration.onRegistrationPreRegisteredEvent(new RegistrationPreRegisteredEvent(this.registry, this.registration));

        MultipleRegistration multipleRegistration = new MultipleRegistration(ofList(new DefaultRegistration()));
        enhancedEurekaClientAutoConfiguration.onRegistrationPreRegisteredEvent(new RegistrationPreRegisteredEvent(this.registry, multipleRegistration));

        registration.getMetadata().put("key", "value");
        sleep(SECONDS.toMillis(1));
        assertTrue(preRegistered.get());
    }
}
