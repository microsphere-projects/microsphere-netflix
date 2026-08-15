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

package io.microsphere.netflix.eureka.client.spring.cloud.bean;

import com.netflix.discovery.AbstractDiscoveryClientOptionalArgs;
import com.netflix.discovery.EurekaEventListener;
import com.netflix.discovery.PreRegistrationHandler;
import io.microsphere.netflix.eureka.client.CompositePreRegistrationHandler;
import io.microsphere.spring.beans.factory.config.GenericBeanPostProcessorAdapter;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;

import java.util.List;

import static io.microsphere.collection.SetUtils.newLinkedHashSet;
import static io.microsphere.spring.beans.BeanUtils.getSortedBeans;

/**
 * {@link AbstractDiscoveryClientOptionalArgs} Bean Post-Processor
 *
 * @author <a href="mailto:mercyblitz@gmail.com">Mercy</a>
 * @see AbstractDiscoveryClientOptionalArgs
 * @see GenericBeanPostProcessorAdapter
 * @since 1.0.0
 */
public class AbstractDiscoveryClientOptionalArgsBeanPostProcessor extends GenericBeanPostProcessorAdapter<AbstractDiscoveryClientOptionalArgs> {

    private final ConfigurableListableBeanFactory beanFactory;

    public AbstractDiscoveryClientOptionalArgsBeanPostProcessor(ConfigurableListableBeanFactory beanFactory) {
        this.beanFactory = beanFactory;
    }

    @Override
    protected void processAfterInitialization(AbstractDiscoveryClientOptionalArgs optionalArgs, String beanName) throws BeansException {
        setPreRegistrationHandler(optionalArgs);
        setEurekaEventListeners(optionalArgs);
    }

    void setPreRegistrationHandler(AbstractDiscoveryClientOptionalArgs optionalArgs) {
        List<PreRegistrationHandler> preRegistrationHandlers = getSortedBeans(this.beanFactory, PreRegistrationHandler.class);
        CompositePreRegistrationHandler compositePreRegistrationHandler = new CompositePreRegistrationHandler(preRegistrationHandlers);
        optionalArgs.setPreRegistrationHandler(compositePreRegistrationHandler);
    }

    private void setEurekaEventListeners(AbstractDiscoveryClientOptionalArgs optionalArgs) {
        List<EurekaEventListener> eurekaEventListeners = getSortedBeans(this.beanFactory, EurekaEventListener.class);
        optionalArgs.setEventListeners(newLinkedHashSet(eurekaEventListeners));
    }
}