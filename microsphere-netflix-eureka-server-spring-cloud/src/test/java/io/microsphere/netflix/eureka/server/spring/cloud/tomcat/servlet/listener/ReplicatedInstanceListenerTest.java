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

package io.microsphere.netflix.eureka.server.spring.cloud.tomcat.servlet.listener;


import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.converters.wrappers.CodecWrapper;
import com.netflix.discovery.shared.Applications;
import com.netflix.eureka.registry.PeerAwareInstanceRegistry;
import com.netflix.eureka.registry.PeerAwareInstanceRegistryImpl;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.netflix.eureka.EurekaInstanceConfigBean;
import org.springframework.cloud.netflix.eureka.InstanceInfoFactory;

import java.io.IOException;
import java.util.Map;

import static com.netflix.eureka.registry.PeerAwareInstanceRegistryImpl.Action.Cancel;
import static com.netflix.eureka.registry.PeerAwareInstanceRegistryImpl.Action.Heartbeat;
import static com.netflix.eureka.registry.PeerAwareInstanceRegistryImpl.Action.Register;
import static io.microsphere.netflix.eureka.server.spring.cloud.tomcat.servlet.listener.EurekaServerListener.getCodecWrapper;
import static io.microsphere.netflix.eureka.server.spring.cloud.tomcat.servlet.listener.ReplicatedInstanceListener.get;
import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.sleep;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.springframework.beans.BeanUtils.copyProperties;

/**
 * {@link ReplicatedInstanceListener} Test
 *
 * @author <a href="mailto:mercyblitz@gmail.com">Mercy</a>
 * @see ReplicatedInstanceListener
 * @since 1.0.0
 */
class ReplicatedInstanceListenerTest extends EurekaServerTest {

    @Test
    void test() throws Throwable {
        ReplicatedInstanceListener listener = get(this.servletContext);

        // test processRegisteredInstances method
        testProcessRegisteredInstances(listener);

        // test process method
        testProcess(listener, Register);
        testProcess(listener, Heartbeat);
        testProcess(listener, Cancel);
    }

    void testProcessRegisteredInstances(ReplicatedInstanceListener listener) throws InterruptedException {
        PeerAwareInstanceRegistry registry = listener.getRegistry();
        Applications applications = registry.getApplications();
        while (applications.size() == 0) {
            assertDoesNotThrow(listener::processRegisteredInstances);
            sleep(100);
            applications = registry.getApplications();
        }
        assertDoesNotThrow(listener::processRegisteredInstances);
    }

    void testProcess(ReplicatedInstanceListener listener, PeerAwareInstanceRegistryImpl.Action action) throws IOException {
        EurekaInstanceConfigBean eurekaInstanceConfigBean = new EurekaInstanceConfigBean(super.inetUtils);
        copyProperties(super.eurekaInstanceConfigBean, eurekaInstanceConfigBean);

        eurekaInstanceConfigBean.setAppname("test-app");
        eurekaInstanceConfigBean.setInstanceId("test-instance-" + currentTimeMillis());

        InstanceInfoFactory instanceInfoFactory = new InstanceInfoFactory();
        InstanceInfo instanceInfo = instanceInfoFactory.create(eurekaInstanceConfigBean);

        CodecWrapper codecWrapper = getCodecWrapper(super.eurekaServerContext);

        String actionKey = super.eurekaServerProperties.getActionKey();
        Map<String, String> metadata = instanceInfo.getMetadata();
        metadata.put(actionKey, action.name());

        String json = codecWrapper.encode(instanceInfo);
        listener.process(json);
        listener.process(instanceInfo, null);
    }
}