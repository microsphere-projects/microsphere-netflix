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

import com.netflix.discovery.DiscoveryClient;
import com.netflix.discovery.PreRegistrationHandler;
import io.microsphere.annotation.Immutable;
import io.microsphere.annotation.Nonnull;
import io.microsphere.lang.Prioritized;

import java.util.List;

import static io.microsphere.collection.ListUtils.newArrayList;
import static io.microsphere.collection.Lists.ofList;
import static io.microsphere.lang.Prioritized.COMPARATOR;
import static java.util.Collections.sort;
import static java.util.Collections.unmodifiableList;

/**
 * The Composite class of {@link PreRegistrationHandler}, the element of which can implement {@link Prioritized}.
 *
 * @author <a href="mailto:mercyblitz@gmail.com">Mercy</a>
 * @see PreRegistrationHandler
 * @see DiscoveryClient
 * @see Prioritized
 * @since 1.0.0
 */
public class CompositePreRegistrationHandler implements PreRegistrationHandler {

    private final List<PreRegistrationHandler> preRegistrationHandlers;

    public CompositePreRegistrationHandler(PreRegistrationHandler... preRegistrationHandlers) {
        this(ofList(preRegistrationHandlers));
    }

    public CompositePreRegistrationHandler(List<PreRegistrationHandler> preRegistrationHandlers) {
        this.preRegistrationHandlers = newArrayList(preRegistrationHandlers);
    }

    /**
     * Add the specified {@link PreRegistrationHandler} element
     *
     * @param preRegistrationHandler the specified {@link PreRegistrationHandler} element
     * @return this {@link CompositePreRegistrationHandler}
     */
    @Nonnull
    public CompositePreRegistrationHandler add(PreRegistrationHandler preRegistrationHandler) {
        this.preRegistrationHandlers.add(preRegistrationHandler);
        return this;
    }

    @Override
    public void beforeRegistration() {
        sort(this.preRegistrationHandlers, COMPARATOR);
        for (PreRegistrationHandler preRegistrationHandler : this.preRegistrationHandlers) {
            preRegistrationHandler.beforeRegistration();
        }
    }

    /**
     * Return all {@link PreRegistrationHandler} elements
     *
     * @return the unmodifiable {@link PreRegistrationHandler} list
     */
    @Nonnull
    @Immutable
    public List<PreRegistrationHandler> getPreRegistrationHandlers() {
        return unmodifiableList(this.preRegistrationHandlers);
    }
}