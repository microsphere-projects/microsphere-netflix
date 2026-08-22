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

import java.io.IOException;
import java.util.Map;

import static com.netflix.eureka.registry.PeerAwareInstanceRegistryImpl.Action.Cancel;
import static com.netflix.eureka.registry.PeerAwareInstanceRegistryImpl.Action.Heartbeat;
import static com.netflix.eureka.registry.PeerAwareInstanceRegistryImpl.Action.Register;
import static io.microsphere.netflix.eureka.server.spring.cloud.tomcat.servlet.listener.EurekaServerListener.getCodecWrapper;
import static io.microsphere.netflix.eureka.server.spring.cloud.tomcat.servlet.listener.ReplicatedInstanceListener.get;
import static java.lang.Thread.sleep;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * {@link ReplicatedInstanceListener} Test
 *
 * @author <a href="mailto:mercyblitz@gmail.com">Mercy</a>
 * @see ReplicatedInstanceListener
 * @since 1.0.0
 */
class ReplicatedInstanceListenerTest extends EurekaServerTest {

    // @Test
    void test() throws Throwable {
        ReplicatedInstanceListener listener = get(this.servletContext);

        // test processRegisteredInstances method
        testProcessRegisteredInstances(listener);

        // test process method
        testProcess(listener, Heartbeat);
        testProcess(listener, Cancel);
        testProcess(listener, Register);
    }

    private void testProcessRegisteredInstances(ReplicatedInstanceListener listener) throws InterruptedException {
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
        CodecWrapper codecWrapper = getCodecWrapper(super.eurekaServerContext);
        InstanceInfo clonedInstanceInfo = new InstanceInfo(this.instanceInfo);

        String actionKey = this.eurekaServerProperties.getActionKey();
        Map<String, String> metadata = clonedInstanceInfo.getMetadata();
        metadata.put(actionKey, action.name());

        String json = codecWrapper.encode(clonedInstanceInfo);
        listener.process(json);
        listener.process(clonedInstanceInfo, null);
    }
}