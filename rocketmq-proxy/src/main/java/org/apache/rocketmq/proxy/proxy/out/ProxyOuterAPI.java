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
package org.apache.rocketmq.proxy.proxy.out;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.namesrv.TopAddressing;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.header.namesrv.GetRouteInfoRequestHeader;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.proxy.broker.latency.BrokerFixedThreadPoolExecutor;
import org.apache.rocketmq.proxy.common.log.LoggerName;
import org.apache.rocketmq.proxy.common.protocol.ProxyRequestCode;
import org.apache.rocketmq.proxy.common.protocol.TopicRouteTable;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ProxyOuterAPI {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private static final long REQUEST_TIMEOUT = 5000;
    private final RemotingClient remotingClient;
    private final TopAddressing topAddressing = new TopAddressing(MixAll.getWSAddr());
    private String nameSrvAddr = null;
    private final BrokerFixedThreadPoolExecutor outerExecutor = new BrokerFixedThreadPoolExecutor(4, 10, 1, TimeUnit.MINUTES,
            new ArrayBlockingQueue<>(32), new ThreadFactoryImpl("proxyOutApi_thread_", true));

    public ProxyOuterAPI(final NettyClientConfig nettyClientConfig) {
        this(nettyClientConfig, null);
    }

    public ProxyOuterAPI(final NettyClientConfig nettyClientConfig, RPCHook rpcHook) {
        this.remotingClient = new NettyRemotingClient(nettyClientConfig);
        this.remotingClient.registerRPCHook(rpcHook);
    }

    public void start() {
        this.remotingClient.start();
    }

    public void shutdown() {
        this.remotingClient.shutdown();
        this.outerExecutor.shutdown();
    }

    public String fetchNameServerAddr() {
        try {
            String addrs = this.topAddressing.fetchNSAddr();
            if (addrs != null && !UtilAll.isBlank(addrs)) {
                if (!addrs.equals(this.nameSrvAddr)) {
                    log.info("name server address changed, old: {} new: {}", this.nameSrvAddr, addrs);
                    this.updateNameServerAddressList(addrs);
                    this.nameSrvAddr = addrs;
                    return nameSrvAddr;
                }
            }
        } catch (Exception e) {
            log.error("fetchNameServerAddr Exception", e);
        }
        return nameSrvAddr;
    }

    public void updateNameServerAddressList(final String addrs) {
        if (!UtilAll.isBlank(addrs)) {
            this.remotingClient.updateNameServerAddressList(Arrays.asList(addrs.split(";")));
        }
    }

    public ClusterInfo getProxyClusterInfo()
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(ProxyRequestCode.GET_PROXY_BROKER_CLUSTER_INFO, null);
        RemotingCommand response = this.remotingClient.invokeSync(null, request, REQUEST_TIMEOUT);
        if (response == null || response.getBody() == null) {
            return null;
        }
        return RemotingSerializable.decode(response.getBody(), ClusterInfo.class);
    }

    public ClusterInfo getBrokerClusterInfo()
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(ProxyRequestCode.GET_BROKER_BROKER_CLUSTER_INFO, null);
        RemotingCommand response = this.remotingClient.invokeSync(null, request, REQUEST_TIMEOUT);
        if (response == null || response.getBody() == null) {
            return null;
        }
        return RemotingSerializable.decode(response.getBody(), ClusterInfo.class);
    }

    public TopicList getBrokerTopicList() throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(ProxyRequestCode.GET_BROKER_ALL_TOPIC_LIST_FROM_NAMESERVER, null);
        RemotingCommand response = this.remotingClient.invokeSync(null, request, REQUEST_TIMEOUT);
        if (response == null || response.getBody() == null) {
            return null;
        }
        return RemotingSerializable.decode(response.getBody(), TopicList.class);
    }

    public TopicList getProxyTopicList() throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(ProxyRequestCode.GET_PROXY_ALL_TOPIC_LIST_FROM_NAMESERVER, null);
        RemotingCommand response = this.remotingClient.invokeSync(null, request, REQUEST_TIMEOUT);
        if (response == null || response.getBody() == null) {
            return null;
        }
        return RemotingSerializable.decode(response.getBody(), TopicList.class);
    }

    public TopicRouteData getProxyTopicRoute(String topic)
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        GetRouteInfoRequestHeader requestHeader = new GetRouteInfoRequestHeader();
        requestHeader.setTopic(topic);
        RemotingCommand request = RemotingCommand.createRequestCommand(ProxyRequestCode.GET_PROXY_ROUTEINFO_BY_TOPIC, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(null, request, REQUEST_TIMEOUT);
        if (response == null || response.getBody() == null) {
            return null;
        }
        return RemotingSerializable.decode(response.getBody(), TopicRouteData.class);
    }

    public TopicRouteTable getBrokerTopicRoute()
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(ProxyRequestCode.GET_BROKER_ALL_ROUTEINFO_BY_TOPIC, null);
        RemotingCommand response = this.remotingClient.invokeSync(null, request, REQUEST_TIMEOUT);
        if (response == null || response.getBody() == null) {
            return null;
        }
        return RemotingSerializable.decode(response.getBody(), TopicRouteTable.class);
    }

    public void registerRPCHook(RPCHook rpcHook) {
        remotingClient.registerRPCHook(rpcHook);
    }
}
