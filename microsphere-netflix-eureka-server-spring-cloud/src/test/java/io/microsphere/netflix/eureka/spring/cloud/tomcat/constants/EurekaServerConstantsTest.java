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

package io.microsphere.netflix.eureka.spring.cloud.tomcat.constants;


import io.microsphere.netflix.eureka.spring.cloud.constants.EurekaServerConstants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.microsphere.netflix.eureka.spring.cloud.constants.EurekaServerConstants.DEFAULT_REPLICATION_TIMEOUT_PROPERTY_VALUE;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@link EurekaServerConstants} Test
 *
 * @author <a href="mailto:mercyblitz@gmail.com">Mercy</a>
 * @see EurekaServerConstants
 * @since 1.0.0
 */
class EurekaServerConstantsTest {

    @BeforeEach
    void setUp() {
    }

    @Test
    void testConstants() {
        assertEquals("15000", DEFAULT_REPLICATION_TIMEOUT_PROPERTY_VALUE);
    }
}