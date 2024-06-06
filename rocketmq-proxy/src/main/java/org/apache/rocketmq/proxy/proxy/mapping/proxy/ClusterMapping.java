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
package org.apache.rocketmq.proxy.proxy.mapping.proxy;

import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.proxy.broker.netty.ProxyRemotingClient;
import org.apache.rocketmq.proxy.controller.Controllers;
import org.apache.rocketmq.proxy.proxy.mapping.client.ClientType;
import org.apache.rocketmq.proxy.proxy.mapping.client.RemotingClientManager;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.proxy.common.log.LoggerName;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class ClusterMapping {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private final NettyClientConfig nettyClientConfig;
    private volatile Map<String, Map<Long, String>> proxyNameAddress = new HashMap<>();
    private volatile Map<String, String> proxyAddressName = new HashMap<>();

    private volatile Map<String, Map<Long, String>> brokerNameAddress = new HashMap<>();
    private volatile Map<String, String> brokerAddressName = new HashMap<>();

    private final RemotingClientManager remotingClientManager;

    private final ConcurrentMap<String, RemotingClient> proxy2BrokerHeartBeatClientsA = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, RemotingClient> proxy2BrokerHeartBeatClientsB = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, ProxyRemotingClient> proxy2BrokerHeartBeatClients = new ConcurrentHashMap<>();

    private final AtomicBoolean useA = new AtomicBoolean(true);

    private final ReentrantLock lock = new ReentrantLock(true);

    public ClusterMapping(NettyClientConfig nettyClientConfig) {
        this.remotingClientManager = new RemotingClientManager(nettyClientConfig);
        this.nettyClientConfig = nettyClientConfig;
    }

    public String getProxyName(String proxyAddr) {
        return proxyAddressName.get(proxyAddr);
    }

    public Set<String> getProxyNames() {
        return new HashSet<>(proxyNameAddress.keySet());
    }

    public Map<Long, String> getBrokerAddress(String brokerName) {
        return brokerNameAddress.get(brokerName);
    }

    public String selectBrokerAddress(String brokerName, boolean slaveFirst) {
        Map<Long, String> addrMap = brokerNameAddress.get(brokerName);
        if (addrMap == null || addrMap.isEmpty()) {
            return "";
        }
        if (!slaveFirst) {
            if (addrMap.containsKey(0L)) {
                // get master address
                return addrMap.get(0L);
            }
        } else {
            for (Map.Entry<Long, String> entry : addrMap.entrySet()) {
                if (entry.getKey() != 0) {
                    // slave address first
                    return entry.getValue();
                }
            }
        }
        // not found, return the first address
        return addrMap.values().iterator().next();
    }

    public String selectBrokerAddress(String brokerName) {
        return selectBrokerAddress(brokerName, false);
    }

    public String getBrokerName(String brokerAddr) {
        return brokerAddressName.get(brokerAddr);
    }

    public Set<String> getBrokerNames() {
        return new HashSet<>(brokerNameAddress.keySet());
    }

    public Map<Long, String> getProxyAddress(String proxyName) {
        return proxyNameAddress.get(proxyName);
    }

    public RemotingClient getBrokerAddressRemotingClientByAddr(String addr) {
        return remotingClientManager.getRemotingClient(addr, ClientType.DEFAULT);
    }

    public RemotingClient getBrokerAddressRemotingClientByAddr(String addr, ClientType clientType) {
        return remotingClientManager.getRemotingClient(addr, clientType);
    }

    public ProxyRemotingClient getHeartBeatClientByAddr(String addr) {
        return proxy2BrokerHeartBeatClients.get(addr);
    }

    public synchronized void updateProxyNameAddress(ClusterInfo clusterInfo) {
        if (clusterInfo == null || clusterInfo.getBrokerAddrTable() == null) {
            this.proxyNameAddress = new HashMap<>();
            this.proxyAddressName = new HashMap<>();
            return;
        }

        Map<String, Map<Long, String>> localNameAddress = new HashMap<>();
        Map<String, String> localAddressName = new HashMap<>();
        parseNameAddress(clusterInfo, localNameAddress, localAddressName);

        this.proxyNameAddress = localNameAddress;
        this.proxyAddressName = localAddressName;
    }

    public void updateBrokerNameAddress(ClusterInfo clusterInfo) {
        try {
            lock.lock();
            if (clusterInfo == null || clusterInfo.getBrokerAddrTable() == null) {
                this.brokerNameAddress = new HashMap<>();
                this.brokerAddressName = new HashMap<>();
                return;
            }

            Map<String, Map<Long, String>> localNameAddress = new HashMap<>();
            Map<String, String> localAddressName = new HashMap<>();
            parseNameAddress(clusterInfo, localNameAddress, localAddressName);
            this.brokerNameAddress = localNameAddress;
            this.brokerAddressName = localAddressName;
            this.brokerAddressName.keySet().forEach(remotingClientManager::createRemotingClients);
            this.brokerAddressName.keySet().forEach(this::updateHeartBeatClient);
            remotingClientManager.clearUnusedRemotingClients(this.brokerAddressName.keySet());
            removeUnUseHeartBeatClient();
        } finally {
            lock.unlock();
        }
    }

    public synchronized boolean notifyAllBrokers(RemotingCommand request, long timeout) {
        return remotingClientManager.applyRemotingClients(ClientType.DEFAULT, (addr, remotingClient) ->
                remotingClient.invokeSync(addr, request, timeout));
    }

    private synchronized void parseNameAddress(ClusterInfo clusterInfo,
                                               Map<String, Map<Long, String>> nameAddress,
                                               Map<String, String> addressName) {
        for (BrokerData brokerData : clusterInfo.getBrokerAddrTable().values()) {
            String brokerName = brokerData.getBrokerName();
            Map<Long, String> brokerAddrMap = new HashMap<>(brokerData.getBrokerAddrs());
            nameAddress.put(brokerName, brokerAddrMap);
            for (String addr : brokerAddrMap.values()) {
                addressName.put(addr, brokerName);
            }
        }
    }

    private synchronized void updateHeartBeatClient(String addr) {
        proxy2BrokerHeartBeatClients.computeIfAbsent(addr, v -> {
            ProxyRemotingClient pc = new ProxyRemotingClient(nettyClientConfig, addr);
            try {
                Controllers.getController().getBrokerController().registerClientProcessor(pc);
            } catch (Exception e) {
                log.error("register client processor failed", e);
            }

            pc.start();
            return pc;
        });
    }

    private synchronized void removeUnUseHeartBeatClient() {
        proxy2BrokerHeartBeatClients.keySet().removeIf(addrOld -> {
            if (!this.brokerAddressName.containsKey(addrOld)) {
                proxy2BrokerHeartBeatClients.get(addrOld).shutdown();
                return true;
            }
            return false;
        });
    }
}
