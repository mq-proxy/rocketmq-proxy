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

package org.apache.rocketmq.proxy.broker;

import org.apache.rocketmq.proxy.controller.Controllers;

import java.io.File;

public class BrokerPathConfigHelper {
    private final static String BROKER_DEFAULT_STORE_PATH =
            System.getProperty("user.home") + File.separator + "store";

    private static String brokerConfigPath = BROKER_DEFAULT_STORE_PATH
        + File.separator + "config" + File.separator + "proxy.properties";


    public static String getBrokerConfigPath() {
        return brokerConfigPath;
    }

    public static String getStoreRootPath() {
        try {
            return Controllers.getController().getProxyConfig().getProxyStoreConfigPath();
        } catch (Throwable e) {
            return BROKER_DEFAULT_STORE_PATH;
        }
    }

    public static void setBrokerConfigPath(String path) {
        brokerConfigPath = path;
    }

    public static String getTopicConfigPath() {
        return getTopicConfigPath(getStoreRootPath());
    }

    public static String getTopicConfigPath(final String rootDir) {
        return rootDir + File.separator + "config" + File.separator + "proxy.topics.json";
    }

    public static String getConsumerOffsetPath() {
        return getConsumerOffsetPath(getStoreRootPath());
    }

    public static String getConsumerOffsetPath(final String rootDir) {
        return rootDir + File.separator + "config" + File.separator + "proxy.consumerOffset.json";
    }

    public static String getLmqConsumerOffsetPath(final String rootDir) {
        return rootDir + File.separator + "config" + File.separator + "proxy.lmqConsumerOffset.json";
    }

    public static String getSubscriptionGroupPath() {
        return getSubscriptionGroupPath(getStoreRootPath());
    }

    public static String getSubscriptionGroupPath(final String rootDir) {
        return rootDir + File.separator + "config" + File.separator + "proxy.subscriptionGroup.json";
    }

    public static String getConsumerFilterPath(final String rootDir) {
        return rootDir + File.separator + "config" + File.separator + "proxy.consumerFilter.json";
    }
}
