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

import static io.microsphere.annotation.ConfigurationProperty.APPLICATION_SOURCE;
import static io.microsphere.constants.PropertyConstants.ENABLED_PROPERTY_NAME;
import static io.microsphere.netflix.eureka.commons.constants.PropertyConstants.DEFAULT_EUREKA_ENABLED_PROPERTY_VALUE;
import static io.microsphere.netflix.eureka.commons.constants.PropertyConstants.EUREKA_PROPERTY_NAME_PREFIX;

/**
 * The constants for Netflix Eureka Server.
 *
 * @author <a href="mailto:mercyblitz@gmail.com">Mercy</a>
 * @since 1.0.0
 */
public interface PropertyConstants {

    /**
     * The property prefix of Netflix Eureka Server : "microsphere.eureka.server."
     */
    String EUREKA_SERVER_PROPERTY_NAME_PREFIX = EUREKA_PROPERTY_NAME_PREFIX + "server.";

    /**
     * The "enabled" property name of Microsphere Netflix Eureka Server : "microsphere.eureka.server.enabled"
     */
    @ConfigurationProperty(
            type = boolean.class,
            defaultValue = DEFAULT_EUREKA_ENABLED_PROPERTY_VALUE,
            source = APPLICATION_SOURCE
    )
    String EUREKA_SERVER_ENABLED_PROPERTY_NAME = EUREKA_SERVER_PROPERTY_NAME_PREFIX + ENABLED_PROPERTY_NAME;

    /**
     * The property prefix of Netflix Eureka Server Replication : "microsphere.eureka.server.replication."
     */
    String EUREKA_SERVER_REPLICATION_PROPERTY_NAME_PREFIX = EUREKA_SERVER_PROPERTY_NAME_PREFIX + "replication.";

    /**
     * The default property value of Replication Metadata for Action Key : "_action_"
     */
    String DEFAULT_REPLICATION_METADATA_ACTION_KEY_PROPERTY_VALUE = "_action_";

    /**
     * The Configuration Property Name of Replication Metadata for Action Key : "microsphere.eureka.server.replication.metadata.action-key"
     */
    @ConfigurationProperty(
            defaultValue = DEFAULT_REPLICATION_METADATA_ACTION_KEY_PROPERTY_VALUE,
            source = APPLICATION_SOURCE
    )
    String REPLICATION_METADATA_ACTION_KEY_PROPERTY_NAME = EUREKA_SERVER_REPLICATION_PROPERTY_NAME_PREFIX + "metadata.action-key";

    /**
     * The Placeholder of Replication Metadata for Action Key : "${microsphere.eureka.server.replication.metadata.action-key:_action_}"
     */
    String REPLICATION_METADATA_ACTION_KEY_PLACEHOLDER = "${" + REPLICATION_METADATA_ACTION_KEY_PROPERTY_NAME + ":" + DEFAULT_REPLICATION_METADATA_ACTION_KEY_PROPERTY_VALUE + "}";

    /**
     * The default property value of Netflix Eureka Server Replication Timeout(unit : milliseconds) : "15000"
     */
    String DEFAULT_REPLICATION_TIMEOUT_PROPERTY_VALUE = "15000";

    /**
     * The Configuration Property Name of Replication Timeout(unit : milliseconds) : "microsphere.eureka.server.replication.timeout"
     */
    @ConfigurationProperty(
            type = long.class,
            defaultValue = DEFAULT_REPLICATION_TIMEOUT_PROPERTY_VALUE
    )
    String EUREKA_SERVER_REPLICATION_TIMEOUT_PROPERTY_NAME = EUREKA_SERVER_REPLICATION_PROPERTY_NAME_PREFIX + "timeout";

    /**
     * The Placeholder of Replication Timeout : "${microsphere.eureka.server.replication.timeout:15000}"
     */
    String EUREKA_SERVER_REPLICATION_TIMEOUT_PLACEHOLDER = "${" + EUREKA_SERVER_REPLICATION_TIMEOUT_PROPERTY_NAME + ":" + DEFAULT_REPLICATION_TIMEOUT_PROPERTY_VALUE + "}";

    /**
     * The default property value of Netflix Eureka Server Replication Threads : "1"
     */
    String DEFAULT_EUREKA_SERVER_REPLICATION_THREADS_PROPERTY_VALUE = "1";

    /**
     * The property name of Netflix Eureka Server Replication Threads : "microsphere.eureka.server.replication.threads"
     */
    @ConfigurationProperty(
            type = int.class,
            defaultValue = DEFAULT_EUREKA_SERVER_REPLICATION_THREADS_PROPERTY_VALUE,
            source = APPLICATION_SOURCE
    )
    String EUREKA_SERVER_REPLICATION_THREADS_PROPERTY_NAME = EUREKA_SERVER_REPLICATION_PROPERTY_NAME_PREFIX + "threads";

    /**
     * The property placeholder of Netflix Eureka Server Replication Threads : "${microsphere.eureka.server.replication.threads:1}"
     */
    String EUREKA_SERVER_REPLICATION_THREADS_PLACEHOLDER = "${" + EUREKA_SERVER_REPLICATION_THREADS_PROPERTY_NAME + ":" + DEFAULT_EUREKA_SERVER_REPLICATION_THREADS_PROPERTY_VALUE + "}";

    /**
     * The default property value of Netflix Eureka Server Replication Thread Name Prefix : "Eureka-Server-Replication-Thread-"
     */
    String DEFAULT_EUREKA_SERVER_REPLICATION_THREAD_NAME_PREFIX_PROPERTY_VALUE = "Eureka-Server-Replication-Thread-";

    /**
     * The property name of Netflix Eureka Server Replication Thread Name Prefix : "microsphere.eureka.server.replication.thread-name-prefix"
     */
    @ConfigurationProperty(
            defaultValue = DEFAULT_EUREKA_SERVER_REPLICATION_THREAD_NAME_PREFIX_PROPERTY_VALUE,
            source = APPLICATION_SOURCE
    )
    String EUREKA_SERVER_REPLICATION_THREAD_NAME_PREFIX_PROPERTY_NAME = EUREKA_SERVER_REPLICATION_PROPERTY_NAME_PREFIX + "thread-name-prefix";

    /**
     * The property placeholder of Netflix Eureka Server Replication Thread Name Prefix : "${microsphere.eureka.server.replication.thread-name-prefix:Eureka-Server-Replication-Thread-}"
     */
    String EUREKA_SERVER_REPLICATION_THREAD_NAME_PREFIX_PLACEHOLDER = "${" + EUREKA_SERVER_REPLICATION_THREAD_NAME_PREFIX_PROPERTY_NAME
            + ":" + DEFAULT_EUREKA_SERVER_REPLICATION_THREAD_NAME_PREFIX_PROPERTY_VALUE + "}";

    /**
     * The default property value of Netflix Eureka Server Replication Instance Name Prefix : "ReplicatedInstance-"
     */
    String DEFAULT_REPLICATION_INSTANCE_NAME_PREFIX_PROPERTY_VALUE = "ReplicatedInstance-";

    /**
     * The property name of Netflix Eureka Server Replication Instance Name Prefix : "microsphere.eureka.server.replication.instance-name-prefix"
     */
    @ConfigurationProperty(
            defaultValue = DEFAULT_REPLICATION_INSTANCE_NAME_PREFIX_PROPERTY_VALUE,
            source = APPLICATION_SOURCE
    )
    String REPLICATION_INSTANCE_NAME_PREFIX_PROPERTY_NAME = EUREKA_SERVER_REPLICATION_PROPERTY_NAME_PREFIX + "instance-name-prefix";

    /**
     * The property placeholder of Netflix Eureka Server Replication Instance Name Prefix : "${microsphere.eureka.server.replication.instance-name-prefix:ReplicatedInstance-}"
     */
    String REPLICATION_INSTANCE_NAME_PREFIX_PLACEHOLDER = "${" + REPLICATION_INSTANCE_NAME_PREFIX_PROPERTY_NAME + ":" + DEFAULT_REPLICATION_INSTANCE_NAME_PREFIX_PROPERTY_VALUE + "}";

    /**
     * The default property value of Netflix Eureka Server Replication Queue Capacity : "100"
     */
    String DEFAULT_EUREKA_SERVER_REPLICATION_QUEUE_CAPACITY_PROPERTY_VALUE = "100";

    /**
     * The property name of Netflix Eureka Server Replication Queue Capacity : "microsphere.eureka.server.replication.queue.capacity"
     */
    @ConfigurationProperty(
            type = int.class,
            defaultValue = DEFAULT_EUREKA_SERVER_REPLICATION_QUEUE_CAPACITY_PROPERTY_VALUE,
            source = APPLICATION_SOURCE
    )
    String EUREKA_SERVER_REPLICATION_QUEUE_CAPACITY_PROPERTY_NAME = EUREKA_SERVER_REPLICATION_PROPERTY_NAME_PREFIX + "queue.capacity";

    /**
     * The property placeholder of Netflix Eureka Server Replication Queue Capacity : "${microsphere.eureka.server.replication.queue.capacity:100}"
     */
    String EUREKA_SERVER_REPLICATION_QUEUE_CAPACITY_PLACEHOLDER = "${" + EUREKA_SERVER_REPLICATION_QUEUE_CAPACITY_PROPERTY_NAME + ":" + DEFAULT_EUREKA_SERVER_REPLICATION_QUEUE_CAPACITY_PROPERTY_VALUE + "}";

    /**
     * The default property value of Netflix Eureka Server Deregistration Delay(unit : milliseconds) : "3000"
     */
    String DEFAULT_EUREKA_SERVER_DEREGISTRATION_DELAY_PROPERTY_VALUE = "3000";

    /**
     * The property name of Netflix Eureka Server Deregistration Delay(unit : milliseconds) :
     * "microsphere.eureka.server.deregistration.delay"
     */
    @ConfigurationProperty(
            type = long.class,
            defaultValue = DEFAULT_EUREKA_SERVER_DEREGISTRATION_DELAY_PROPERTY_VALUE,
            source = APPLICATION_SOURCE
    )
    String EUREKA_SERVER_DEREGISTRATION_DELAY_PROPERTY_NAME = EUREKA_SERVER_PROPERTY_NAME_PREFIX + "deregistration.delay";

    /**
     * The property placeholder of Netflix Eureka Server Deregistration Delay(unit : milliseconds) :
     * "${microsphere.eureka.server.deregistration.delay:3000}"
     */
    String EUREKA_SERVER_DEREGISTRATION_DELAY_PLACEHOLDER = "${" + EUREKA_SERVER_DEREGISTRATION_DELAY_PROPERTY_NAME + ":"
            + DEFAULT_EUREKA_SERVER_DEREGISTRATION_DELAY_PROPERTY_VALUE + "}";

}