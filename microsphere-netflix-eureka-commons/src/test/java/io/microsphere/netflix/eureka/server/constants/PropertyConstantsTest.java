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

package io.microsphere.netflix.eureka.server.constants;


import io.microsphere.annotation.ConfigurationProperty;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static io.microsphere.annotation.ConfigurationProperty.APPLICATION_SOURCE;
import static io.microsphere.netflix.eureka.commons.constants.PropertyConstants.DEFAULT_EUREKA_ENABLED_PROPERTY_VALUE;
import static io.microsphere.netflix.eureka.server.constants.PropertyConstants.DEFAULT_EUREKA_SERVER_DEREGISTRATION_DELAY_PROPERTY_VALUE;
import static io.microsphere.netflix.eureka.server.constants.PropertyConstants.DEFAULT_EUREKA_SERVER_REPLICATION_QUEUE_CAPACITY_PROPERTY_VALUE;
import static io.microsphere.netflix.eureka.server.constants.PropertyConstants.DEFAULT_EUREKA_SERVER_REPLICATION_THREADS_PROPERTY_VALUE;
import static io.microsphere.netflix.eureka.server.constants.PropertyConstants.DEFAULT_REPLICATION_TIMEOUT_PROPERTY_VALUE;
import static io.microsphere.netflix.eureka.server.constants.PropertyConstants.EUREKA_SERVER_DEREGISTRATION_DELAY_PLACEHOLDER;
import static io.microsphere.netflix.eureka.server.constants.PropertyConstants.EUREKA_SERVER_DEREGISTRATION_DELAY_PROPERTY_NAME;
import static io.microsphere.netflix.eureka.server.constants.PropertyConstants.EUREKA_SERVER_ENABLED_PROPERTY_NAME;
import static io.microsphere.netflix.eureka.server.constants.PropertyConstants.EUREKA_SERVER_PROPERTY_NAME_PREFIX;
import static io.microsphere.netflix.eureka.server.constants.PropertyConstants.EUREKA_SERVER_REPLICATION_PROPERTY_NAME_PREFIX;
import static io.microsphere.netflix.eureka.server.constants.PropertyConstants.EUREKA_SERVER_REPLICATION_QUEUE_CAPACITY_PLACEHOLDER;
import static io.microsphere.netflix.eureka.server.constants.PropertyConstants.EUREKA_SERVER_REPLICATION_QUEUE_CAPACITY_PROPERTY_NAME;
import static io.microsphere.netflix.eureka.server.constants.PropertyConstants.EUREKA_SERVER_REPLICATION_THREADS_PLACEHOLDER;
import static io.microsphere.netflix.eureka.server.constants.PropertyConstants.EUREKA_SERVER_REPLICATION_THREADS_PROPERTY_NAME;
import static io.microsphere.netflix.eureka.server.constants.PropertyConstants.EUREKA_SERVER_REPLICATION_TIMEOUT_PLACEHOLDER;
import static io.microsphere.netflix.eureka.server.constants.PropertyConstants.EUREKA_SERVER_REPLICATION_TIMEOUT_PROPERTY_NAME;
import static io.microsphere.reflect.FieldUtils.findField;
import static io.microsphere.util.ArrayUtils.ofArray;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@link PropertyConstants} Test
 *
 * @author <a href="mailto:mercyblitz@gmail.com">Mercy</a>
 * @see PropertyConstants
 * @since 1.0.0
 */
class PropertyConstantsTest {

    @Test
    void testConstants() {
        assertEquals("microsphere.eureka.server.", EUREKA_SERVER_PROPERTY_NAME_PREFIX);
        assertEquals("microsphere.eureka.server.enabled", EUREKA_SERVER_ENABLED_PROPERTY_NAME);
        assertEquals("microsphere.eureka.server.replication.", EUREKA_SERVER_REPLICATION_PROPERTY_NAME_PREFIX);

        assertEquals("15000", DEFAULT_REPLICATION_TIMEOUT_PROPERTY_VALUE);
        assertEquals("microsphere.eureka.server.replication.timeout", EUREKA_SERVER_REPLICATION_TIMEOUT_PROPERTY_NAME);
        assertEquals("${microsphere.eureka.server.replication.timeout:15000}", EUREKA_SERVER_REPLICATION_TIMEOUT_PLACEHOLDER);
        assertEquals("1", DEFAULT_EUREKA_SERVER_REPLICATION_THREADS_PROPERTY_VALUE);
        assertEquals("microsphere.eureka.server.replication.threads", EUREKA_SERVER_REPLICATION_THREADS_PROPERTY_NAME);
        assertEquals("${microsphere.eureka.server.replication.threads:1}", EUREKA_SERVER_REPLICATION_THREADS_PLACEHOLDER);
        assertEquals("100", DEFAULT_EUREKA_SERVER_REPLICATION_QUEUE_CAPACITY_PROPERTY_VALUE);
        assertEquals("microsphere.eureka.server.replication.queue.capacity", EUREKA_SERVER_REPLICATION_QUEUE_CAPACITY_PROPERTY_NAME);
        assertEquals("${microsphere.eureka.server.replication.queue.capacity:100}", EUREKA_SERVER_REPLICATION_QUEUE_CAPACITY_PLACEHOLDER);

        assertEquals("3000", DEFAULT_EUREKA_SERVER_DEREGISTRATION_DELAY_PROPERTY_VALUE);
        assertEquals("microsphere.eureka.server.deregistration.delay", EUREKA_SERVER_DEREGISTRATION_DELAY_PROPERTY_NAME);
        assertEquals("${microsphere.eureka.server.deregistration.delay:3000}", EUREKA_SERVER_DEREGISTRATION_DELAY_PLACEHOLDER);

        Field field = findField(PropertyConstants.class, "EUREKA_SERVER_ENABLED_PROPERTY_NAME");
        ConfigurationProperty annotation = field.getAnnotation(ConfigurationProperty.class);
        assertEquals(boolean.class, annotation.type());
        assertEquals(DEFAULT_EUREKA_ENABLED_PROPERTY_VALUE, annotation.defaultValue());
        assertArrayEquals(ofArray(APPLICATION_SOURCE), annotation.source());

        field = findField(PropertyConstants.class, "EUREKA_SERVER_DEREGISTRATION_DELAY_PROPERTY_NAME");
        annotation = field.getAnnotation(ConfigurationProperty.class);
        assertEquals(long.class, annotation.type());
        assertEquals(DEFAULT_EUREKA_SERVER_DEREGISTRATION_DELAY_PROPERTY_VALUE, annotation.defaultValue());
        assertArrayEquals(ofArray(APPLICATION_SOURCE), annotation.source());
    }
}