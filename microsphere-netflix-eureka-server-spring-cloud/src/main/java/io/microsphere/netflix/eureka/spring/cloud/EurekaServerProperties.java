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

package io.microsphere.netflix.eureka.spring.cloud;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * The {@link ConfigurationProperties} for Netflix Eureka Server
 *
 * @author <a href="mailto:mercyblitz@gmail.com">Mercy</a>
 * @see ConfigurationProperties
 * @since 1.0.0
 */
@ConfigurationProperties(prefix = "microsphere.eureka.server")
public class EurekaServerProperties {

    /**
     * Enabled
     */
    private boolean enabled;

    private Replication replication = new Replication();

    private Deregistration deregistration = new Deregistration();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public Replication getReplication() {
        return replication;
    }

    public void setReplication(Replication replication) {
        this.replication = replication;
    }

    public Deregistration getDeregistration() {
        return deregistration;
    }

    public void setDeregistration(Deregistration deregistration) {
        this.deregistration = deregistration;
    }

    public String getThreadNamePrefix() {
        return this.replication.getThreads().getThreadNamePrefix();
    }

    public int getThreadsNumber() {
        return this.replication.getThreads().getNumber();
    }

    public String getActionKey() {
        return this.replication.getMetadata().getActionKey();
    }

    public String getInstanceNamePrefix() {
        return this.replication.getInstanceNamePrefix();
    }

    public long getReplicationTimeout() {
        return this.replication.getTimeout();
    }

    public long getDeregistrationDelay() {
        return this.deregistration.getDelay();
    }

    /**
     * The replication properties of Netflix Eureka Server
     */
    public static class Replication {

        /**
         * The replication instance Name Prefix
         */
        private String instanceNamePrefix = "ReplicatedInstance-";

        /**
         * The replication timeout(unit : milliseconds)
         */
        private int timeout = 15000;

        private Metadata metadata = new Metadata();

        private Threads threads = new Threads();

        public int getTimeout() {
            return timeout;
        }

        public void setTimeout(int timeout) {
            this.timeout = timeout;
        }

        public String getInstanceNamePrefix() {
            return instanceNamePrefix;
        }

        public void setInstanceNamePrefix(String instanceNamePrefix) {
            this.instanceNamePrefix = instanceNamePrefix;
        }

        public Metadata getMetadata() {
            return metadata;
        }

        public void setMetadata(Metadata metadata) {
            this.metadata = metadata;
        }

        public Threads getThreads() {
            return threads;
        }

        public void setThreads(Threads threads) {
            this.threads = threads;
        }

        public static class Metadata {

            /**
             * The metadata 'action' key
             */
            private String actionKey = "_action_";

            public String getActionKey() {
                return actionKey;
            }

            public void setActionKey(String actionKey) {
                this.actionKey = actionKey;
            }
        }

        /**
         * Netflix Eureka Server Replication thread properties.
         */
        public static class Threads {

            /**
             * The number of replication threads.
             */
            private int number = 2;

            /**
             * The prefix of the replication thread name.
             */
            private String threadNamePrefix = "Eureka-Server-Replication-Thread-";

            public int getNumber() {
                return number;
            }

            public void setNumber(int number) {
                this.number = number;
            }

            public String getThreadNamePrefix() {
                return threadNamePrefix;
            }

            public void setThreadNamePrefix(String threadNamePrefix) {
                this.threadNamePrefix = threadNamePrefix;
            }
        }
    }

    /**
     * The deregistration properties of Netflix Eureka Server
     */
    public static class Deregistration {

        /**
         * Netflix Eureka Server Deregistration Delay(unit : milliseconds)
         */
        private long delay = 5000;

        public long getDelay() {
            return delay;
        }

        public void setDelay(long delay) {
            this.delay = delay;
        }
    }
}
