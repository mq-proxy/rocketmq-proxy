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
package org.apache.rocketmq.proxy.proxy;

import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.proxy.common.log.LoggerName;
import org.apache.rocketmq.proxy.controller.Controllers;
import org.apache.rocketmq.remoting.common.RemotingUtil;

import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class Proxy {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private static final String ADDR = RemotingUtil.getLocalAddress();

    private static final String PROXY_ID;

    static {
        StringBuilder sb = new StringBuilder();
        sb.append("Proxy");
        sb.append("@");
        sb.append(ADDR);
        sb.append("@");
        String name = ManagementFactory.getRuntimeMXBean().getName();
        int index = name.indexOf('@');
        if (index > 0) {
            sb.append(name, 0, index);
        }
        PROXY_ID = sb.toString();
    }

    /**
     * get proxy id
     * @return proxy id
     */
    public static String getProxyId() {
        return PROXY_ID;
    }

    /**
     * get proxy name
     * @return proxy name
     */
    public static String getProxyName() {
        return Controllers.getController().getBrokerController().getBrokerConfig().getBrokerName();
    }

    public static Map<Long, String> getBrokerAddresses() {
        return Controllers.getController().getClusterMapping().getBrokerAddress(getProxyName());
    }

    public static Set<String> getAllBrokerAddresses() {
        Map<Long, String> brokers = getBrokerAddresses();
        if (brokers != null) {
            return new HashSet<>(brokers.values());
        }
        return new HashSet<>();
    }

    public static String getMasterBrokerAddr() {
        return getBrokerAddr(false);
    }

    public static String getBrokerAddr(boolean slaveFirst) {
        Map<Long, String> brokerAddresses = getBrokerAddresses();
        if (brokerAddresses == null || brokerAddresses.isEmpty()) {
            return null;
        }

        String master = brokerAddresses.get(0L);
        if (!slaveFirst) {
            if (master != null) {
                return master;
            }
        }

        for (Map.Entry<Long, String> entry : brokerAddresses.entrySet()) {
            if (entry.getKey() != 0L) {
                return entry.getValue();
            }
        }
        return master;
    }

    /**
     * get all broker clusters
     * @return clusters
     */
    public static Set<String> getAllClusters() {
        ClusterInfo clusterInfo = getBrokers();
        if (clusterInfo == null || clusterInfo.getBrokerAddrTable() == null) {
            return null;
        }

        return clusterInfo.getClusterAddrTable().keySet().stream().filter(Objects::nonNull).collect(Collectors.toSet());
    }

    /**
     * get broker ClusterInfo
     * @return broker ClusterInfo
     */
    public static ClusterInfo getBrokers() {
        return Controllers.getController().getProxyClusterInfo().getBrokerClusterInfo();
    }

    /**
     * get proxy ClusterInfo
     * @return proxy ClusterInfo
     */
    public static ClusterInfo getProxies() {
        return Controllers.getController().getProxyClusterInfo().getProxyClusterInfo();
    }
}
