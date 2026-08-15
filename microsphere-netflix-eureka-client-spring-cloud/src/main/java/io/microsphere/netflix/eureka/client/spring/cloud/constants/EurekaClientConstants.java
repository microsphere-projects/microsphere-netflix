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
package io.microsphere.netflix.eureka.client.spring.cloud.constants;

import com.netflix.discovery.EurekaClient;
import io.microsphere.annotation.ConfigurationProperty;
import org.springframework.cloud.netflix.eureka.CloudEurekaClient;

import static io.microsphere.annotation.ConfigurationProperty.APPLICATION_SOURCE;
import static io.microsphere.constants.PropertyConstants.ENABLED_PROPERTY_NAME;
import static io.microsphere.spring.cloud.commons.constants.CommonsPropertyConstants.MICROSPHERE_SPRING_CLOUD_PROPERTY_NAME_PREFIX;

/**
 * The constants for {@link EurekaClient}
 *
 * @author <a href="mailto:mercyblitz@gmail.com">Mercy</a>
 * @since 1.0.0
 */
public interface EurekaClientConstants {

    /**
     * The property prefix of {@link EurekaClient}: "microsphere.spring.cloud.eureka.client."
     */
    String EUREKA_CLIENT_PROPERTY_PREFIX = MICROSPHERE_SPRING_CLOUD_PROPERTY_NAME_PREFIX + "eureka.client.";

    /**
     * The "enabled" property name of Microsphere {@link EurekaClient} Features: "microsphere.spring.cloud.eureka.client.enabled"
     */
    @ConfigurationProperty(
            type = boolean.class,
            defaultValue = "true",
            source = APPLICATION_SOURCE
    )
    String EUREKA_CLIENT_ENABLED_PROPERTY_NAME = EUREKA_CLIENT_PROPERTY_PREFIX + ENABLED_PROPERTY_NAME;

    /**
     * The property name of "multiple"
     */
    String MULTIPLE_PROPERTY_NAME = "multiple";

    /**
     * The "enabled" property name of Microsphere multiple {@link EurekaClient} feature: "microsphere.spring.cloud.eureka.client.multiple"
     */
    @ConfigurationProperty(
            type = boolean.class,
            defaultValue = "false",
            source = APPLICATION_SOURCE
    )
    String EUREKA_CLIENT_MULTIPLE_PROPERTY_NAME = EUREKA_CLIENT_PROPERTY_PREFIX + MULTIPLE_PROPERTY_NAME;

    /**
     * The class name of {@link EurekaClient}
     *
     * @see EurekaClient
     */
    String EUREKA_CLIENT_CLASS_NAME = "com.netflix.discovery.EurekaClient";

    /**
     * The class name of {@link CloudEurekaClient}
     *
     * @see CloudEurekaClient
     */
    String CLOUD_EUREKA_CLIENT_CLASS_NAME = "org.springframework.cloud.netflix.eureka.CloudEurekaClient";

}