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
package org.apache.rocketmq.proxy.proxy.mapping.client;

import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.proxy.common.log.LoggerName;
import org.apache.rocketmq.proxy.controller.Controllers;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class RemotingClientManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private static final Set<ClientType> CLIENT_TYPES = new HashSet<>();
    private final Map<ClientType, NettyClientConfig> remotingConfigs = new ConcurrentHashMap<>();
    private final Map<String, Map<ClientType, RemotingClient>> remotingClients = new ConcurrentHashMap<>();

    static {
        CLIENT_TYPES.add(ClientType.DEFAULT);
        CLIENT_TYPES.add(ClientType.PRODUCER);
        CLIENT_TYPES.add(ClientType.CONSUMER);
        CLIENT_TYPES.add(ClientType.ADMIN);
    }

    public RemotingClientManager(@Nonnull NettyClientConfig nettyClientConfig) {
        CLIENT_TYPES.forEach(clientType -> remotingConfigs.put(clientType, nettyClientConfig));
    }

    public NettyClientConfig getNettyClientConfig(@Nonnull ClientType clientType) {
        return remotingConfigs.get(clientType);
    }

    public void setNettyClientConfig(@Nonnull ClientType clientType, @Nonnull NettyClientConfig nettyClientConfig) {
        remotingConfigs.put(clientType, nettyClientConfig);
    }

    public RemotingClient getRemotingClient(@Nonnull String key, @Nonnull ClientType clientType) {
        RemotingClient client = null;
        try {
            if (remotingClients.containsKey(key)) {
                client = remotingClients.get(key).get(clientType);
            }
        } catch (Exception ignore) {

        }

        return client;
    }

    public boolean applyRemotingClients(@Nonnull ClientType clientType, RemotingClientCallback callback) {
        boolean notified = true;
        for (Map.Entry<String, Map<ClientType, RemotingClient>> entry : remotingClients.entrySet()) {
            String addr = entry.getKey();
            RemotingClient client = entry.getValue().get(clientType);
            try {
                callback.apply(addr, client);
            } catch (Throwable e) {
                log.warn("applyRemotingClients failed, addr:" + addr, e);
                notified = false;
            }
        }
        return notified;
    }

    public synchronized void createRemotingClients(@Nonnull String key) {
        if (remotingClients.containsKey(key)) {
            return;
        }
        Map<ClientType, RemotingClient> remotingClientMap = new HashMap<>();

        CLIENT_TYPES.forEach(clientType -> {
            RemotingClient remotingClient = createRemotingClient(remotingConfigs.computeIfAbsent(clientType,
                k -> new NettyClientConfig()));
            remotingClient.start();
            remotingClientMap.put(clientType, remotingClient);
        });

        remotingClients.put(key, remotingClientMap);
    }

    public synchronized void destroyRemotingClients(@Nonnull String key) {
        Map<ClientType, RemotingClient> removed = remotingClients.remove(key);
        removed.forEach((clientType, remotingClient) -> remotingClient.shutdown());
    }

    public synchronized void clearUnusedRemotingClients(Set<String> usedClients) {
        if (usedClients == null || usedClients.isEmpty()) {
            // clear all
            remotingClients.values().stream()
                    .flatMap(item -> item.values().stream())
                    .filter(Objects::nonNull)
                    .forEach(RemotingClient::shutdown);
            remotingClients.clear();
        } else {
            // clear unused
            remotingClients.entrySet().removeIf(addr -> {
                if (!usedClients.contains(addr.getKey())) {
                    addr.getValue().values().forEach(RemotingClient::shutdown);
                    return true;
                }
                return false;
            });
        }
    }

    @Override
    protected void finalize() {
        clearUnusedRemotingClients(null);
    }

    public static RemotingClient createRemotingClient(NettyClientConfig clientConfig) {
        RemotingClient remotingClient = new NettyRemotingClient(clientConfig);
        try {
            Controllers.getController().getBrokerController().registerClientProcessor(remotingClient);
        } catch (Throwable e) {
            log.error("create RemotingClient failed", e);
        }
        return remotingClient;
    }
}
