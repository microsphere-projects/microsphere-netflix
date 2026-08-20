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

package io.microsphere.netflix.eureka.server.spring.cloud.tomcat.autoconfigure;


import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.shared.Application;
import com.netflix.eureka.EurekaServerContext;
import com.netflix.eureka.registry.PeerAwareInstanceRegistry;
import io.microsphere.netflix.eureka.server.spring.cloud.tomcat.sample.EurekaServerBootstrap;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static io.microsphere.collection.ListUtils.newArrayList;
import static java.lang.Thread.sleep;
import static java.util.Collections.emptyList;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.boot.SpringApplication.exit;
import static org.springframework.boot.SpringApplication.run;

/**
 * {@link TomcatEurekaServerReplicationAutoConfiguration} Integration Test
 *
 * @author <a href="mailto:mercyblitz@gmail.com">Mercy</a>
 * @see TomcatEurekaServerReplicationAutoConfiguration
 * @since 1.0.0
 */
class TomcatEurekaServerReplicationAutoConfigurationTest {

    @Test
    void test() throws Throwable {
        int count = 2;
        ExecutorService executorService = newFixedThreadPool(count);

        CompletionService<ConfigurableApplicationContext> completionService = new ExecutorCompletionService<>(executorService);
        for (int i = 0; i < count; i++) {
            final int port = 12345 + i;
            completionService.submit(() -> run(EurekaServerBootstrap.class, "--server.port=" + port));
        }

        List<ConfigurableApplicationContext> contexts = newArrayList(count);

        String applicationName = "eureka-server".toUpperCase();

        for (int i = 0; i < count; i++) {
            Future<ConfigurableApplicationContext> future = completionService.take();
            ConfigurableApplicationContext context = future.get();
            contexts.add(context);

            EurekaServerContext eurekaServerContext = context.getBean(EurekaServerContext.class);
            PeerAwareInstanceRegistry registry = eurekaServerContext.getRegistry();
            List<InstanceInfo> instances;

            for (int j = 0; j < 50; j++) {
                Application application = registry.getApplication(applicationName);
                instances = application == null ? emptyList() : application.getInstances();
                if (instances.size() == count) {
                    break;
                }
                sleep(500L);
            }
        }

        for (int i = 0; i < count; i++) {
            ConfigurableApplicationContext context = contexts.get(i);
            assertEquals(0, exit(context));
        }

        executorService.shutdown();
    }
}