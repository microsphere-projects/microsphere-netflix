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

import com.netflix.discovery.EurekaClient;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@link MultipleEurekaClientAutoConfiguration} Test
 *
 * @author <a href="mailto:mercyblitz@gmail.com">Mercy</a>
 * @since 1.0.0
 */
@SpringBootTest(
        classes = {
                MultipleEurekaClientAutoConfigurationTest.class
        },
        properties = {
                "spring.application.name=test-eureka",
                "eureka.client.serviceUrl.defaultZone=http://127.0.0.1:8761/eureka,http://127.0.0.1:8761/eureka",
                "microsphere.spring.cloud.eureka.client.multiple=true"
        }
)
@EnableAutoConfiguration
public class MultipleEurekaClientAutoConfigurationTest {

    @Autowired
    private MultipleEurekaClientAutoConfiguration config;

    @Test
    public void test() throws Throwable {
        sleep(SECONDS.toMillis(1));
        List<EurekaClient> eurekaClients = config.getEurekaClients();
        assertEquals(2, eurekaClients.size());
    }
}
