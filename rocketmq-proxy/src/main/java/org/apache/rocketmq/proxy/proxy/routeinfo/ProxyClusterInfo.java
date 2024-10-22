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
package org.apache.rocketmq.proxy.proxy.routeinfo;

import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.proxy.common.log.LoggerName;
import org.apache.rocketmq.proxy.controller.ProxyController;

public class ProxyClusterInfo {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private final ProxyController proxyController;
    private volatile ClusterInfo brokerClusterInfo = new ClusterInfo();
    private volatile ClusterInfo proxyClusterInfo = new ClusterInfo();

    public ProxyClusterInfo(ProxyController proxyController) {
        this.proxyController = proxyController;
    }

    public ClusterInfo getBrokerClusterInfo() {
        return brokerClusterInfo;
    }

    public ClusterInfo getProxyClusterInfo() {
        return proxyClusterInfo;
    }

    public void update() {
        try {
            this.brokerClusterInfo = proxyController.getProxyOuterAPI().getBrokerClusterInfo();
            this.proxyClusterInfo = proxyController.getProxyOuterAPI().getProxyClusterInfo();
        } catch (Exception e) {
            log.warn("update cluster info failed", e);
        }
    }
}
