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
package io.microsphere.netflix.eureka.commons.constants;

import io.microsphere.annotation.ConfigurationProperty;

import static io.microsphere.annotation.ConfigurationProperty.APPLICATION_SOURCE;
import static io.microsphere.constants.PropertyConstants.ENABLED_PROPERTY_NAME;
import static io.microsphere.constants.PropertyConstants.MICROSPHERE_PROPERTY_NAME_PREFIX;

/**
 * The constants for Netflix Eureka
 *
 * @author <a href="mailto:mercyblitz@gmail.com">Mercy</a>
 * @since 1.0.0
 */
public interface PropertyConstants {

    /**
     * The property prefix of Netflix Eureka : "microsphere.eureka."
     */
    String EUREKA_PROPERTY_NAME_PREFIX = MICROSPHERE_PROPERTY_NAME_PREFIX + "eureka.";

    /**
     * The default value of "enabled" property for Netflix Eureka : "true"
     */
    String DEFAULT_EUREKA_ENABLED_PROPERTY_VALUE = "true";

    /**
     * The "enabled" property name of Microsphere Netflix Eureka: "microsphere.eureka.enabled"
     */
    @ConfigurationProperty(
            type = boolean.class,
            defaultValue = DEFAULT_EUREKA_ENABLED_PROPERTY_VALUE,
            source = APPLICATION_SOURCE
    )
    String EUREKA_ENABLED_PROPERTY_NAME = EUREKA_PROPERTY_NAME_PREFIX + ENABLED_PROPERTY_NAME;

}