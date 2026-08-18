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


import io.microsphere.netflix.eureka.spring.cloud.tomcat.sample.EurekaServerApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

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

    // @Test
    void test() throws Throwable {
        int count = 2;
        ExecutorService executorService = newFixedThreadPool(count);

        CompletionService<ConfigurableApplicationContext> completionService = new ExecutorCompletionService<>(executorService);
        for (int i = 0; i < count; i++) {
            final int port = 12345 + i;
            completionService.submit(() -> run(EurekaServerApplication.class, "--server.port=" + port));
        }

        // TODO Check the registered applications
        // sleep(MAX_VALUE);

        for (int i = 0; i < count; i++) {
            Future<ConfigurableApplicationContext> future = completionService.take();
            ConfigurableApplicationContext context = future.get();
            int exit = exit(context);
            assertEquals(0, exit);
        }

        executorService.shutdown();
    }
}