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

package io.microsphere.netflix.eureka.client;


import com.netflix.discovery.PreRegistrationHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link CompositePreRegistrationHandler} Test
 *
 * @author <a href="mailto:mercyblitz@gmail.com">Mercy</a>
 * @see CompositePreRegistrationHandler
 * @since 1.0.0
 */
class CompositePreRegistrationHandlerTest {

    private CompositePreRegistrationHandler handler;

    @BeforeEach
    void setUp() {
        this.handler = new CompositePreRegistrationHandler();
    }

    @Test
    void test() {
        assertDoesNotThrow(this.handler::beforeRegistration);
        List<PreRegistrationHandler> preRegistrationHandlers = this.handler.getPreRegistrationHandlers();
        assertTrue(preRegistrationHandlers.isEmpty());

        PreRegistrationHandler handler = () -> {
        };

        assertSame(this.handler, this.handler.add(handler));
        assertDoesNotThrow(this.handler::beforeRegistration);

        PreRegistrationHandler errorHandler = () -> {
            throw new UnsupportedOperationException("For testing");
        };
        assertSame(this.handler, this.handler.add(errorHandler));
        assertThrows(UnsupportedOperationException.class, this.handler::beforeRegistration);

        preRegistrationHandlers = this.handler.getPreRegistrationHandlers();
        assertSame(preRegistrationHandlers.get(0), handler);
        assertSame(preRegistrationHandlers.get(1), errorHandler);
    }
}