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
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static java.lang.Long.MAX_VALUE;
import static java.lang.Thread.sleep;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.boot.SpringApplication.exit;
import static org.springframework.boot.SpringApplication.run;

/**
 * {@link EurekaServerReplicationEmbeddedTomcatAutoConfiguration} Integration Test
 *
 * @author <a href="mailto:mercyblitz@gmail.com">Mercy</a>
 * @see EurekaServerReplicationEmbeddedTomcatAutoConfiguration
 * @since 1.0.0
 */
class EurekaServerReplicationEmbeddedTomcatAutoConfigurationTest {

    @Test
    void test() throws Throwable {
        int count = 2;
        ExecutorService executorService = newFixedThreadPool(count);

        AtomicReferenceArray<ConfigurableApplicationContext> contexts = new AtomicReferenceArray<>(count);

        for (int i = 0; i < count; i++) {
            final int port = 12345 + i;
            final int index = i;
            executorService.submit(() -> {
                ConfigurableApplicationContext context = run(EurekaServerApplication.class, "--server.port=" + port);
                contexts.set(index, context);
            });
        }

        sleep(MAX_VALUE);

        for (int i = 0; i < count; i++) {
            ConfigurableApplicationContext context = contexts.get(i);
            int exit = exit(context);
            assertEquals(0, exit);
        }

        executorService.shutdown();
    }
}