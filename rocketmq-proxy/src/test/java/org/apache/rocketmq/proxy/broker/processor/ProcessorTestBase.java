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
package org.apache.rocketmq.proxy.broker.processor;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.proxy.broker.BrokerController;
import org.apache.rocketmq.proxy.common.config.ProxyConfig;
import org.apache.rocketmq.proxy.controller.ProxyController;
import org.apache.rocketmq.proxy.helper.MapBuilder;
import org.apache.rocketmq.proxy.proxy.Proxy;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.RemotingResponseCallback;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Spy;

import java.net.InetSocketAddress;
import java.util.Set;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.when;

public abstract class ProcessorTestBase {
    protected AbstractProxyProcessor processor;
    @Spy
    protected BrokerController brokerController = new BrokerController(
            new ProxyController(new ProxyConfig(), new BrokerConfig(), new NettyServerConfig(), new NettyClientConfig())
    );
    @Mock
    protected ChannelHandlerContext channelHandlerContext;

    @Mock
    protected Channel channel;

    protected String clusterName = "DefaultCluster";

    protected String brokerName = "broker-a";

    protected String brokerAddr = "127.0.0.1:10911";
    protected String brokerAddrSlave = "127.0.0.1:10912";

    protected RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        if (request.getExtFields() == null) {
            request.setExtFields(Maps.newHashMap());
        }
        request.makeCustomHeaderToNet();
        return processor.processRequest(ctx, request);
    }

    protected RemotingCommand processRequest(RemotingCommand request) throws Exception {
        return processRequest(this.channelHandlerContext, request);
    }

    protected void asyncProcessRequest(RemotingCommand request, RemotingResponseCallback callback) throws Exception {
        request.makeCustomHeaderToNet();
        processor.asyncProcessRequest(this.channelHandlerContext, request, callback);
    }

    protected void mockProxy(MockedStatic<Proxy> mockedProxy, String proxyName, String brokerAddr) {
        mockedProxy.when(Proxy::getProxyName).thenReturn(
                proxyName
        );

        mockedProxy.when(Proxy::getBrokerAddresses).thenReturn(
                MapBuilder.builder(0L, brokerAddr).add(1L, brokerAddrSlave).build()
        );

        mockedProxy.when(Proxy::getAllBrokerAddresses).thenReturn(
                Sets.newHashSet(brokerAddr, brokerAddrSlave)
        );

        mockedProxy.when(Proxy::getMasterBrokerAddr).thenReturn(
                brokerAddr
        );

        mockedProxy.when(() -> Proxy.getBrokerAddr(anyBoolean())).thenAnswer(arg -> {
                if (arg.getArgument(0)) {
                    return brokerAddrSlave;
                }
                return brokerAddr;
            }
        );

        mockedProxy.when(Proxy::getAllClusters).thenReturn(
                Sets.newHashSet(clusterName)
        );

        // broker cluster info
        ClusterInfo brokerClusterInfo = new ClusterInfo();
        brokerClusterInfo.setBrokerAddrTable(MapBuilder.builder(brokerName, new BrokerData(
                clusterName, brokerName, MapBuilder.builder(0L, brokerAddr).build()
        )).build());
        brokerClusterInfo.setClusterAddrTable(MapBuilder.builder(clusterName,
                (Set<String>)Sets.newHashSet(brokerAddr)).build());
        mockedProxy.when(Proxy::getBrokers).thenReturn(brokerClusterInfo);

        // proxy cluster info
        ClusterInfo proxyClusterInfo = new ClusterInfo();
        proxyClusterInfo.setBrokerAddrTable(MapBuilder.builder(proxyName, new BrokerData(
                clusterName, proxyName, MapBuilder.builder(0L, brokerAddr).build()
        )).build());
        proxyClusterInfo.setClusterAddrTable(MapBuilder.builder(clusterName,
                (Set<String>)Sets.newHashSet(brokerAddr)).build());
        mockedProxy.when(Proxy::getProxies).thenReturn(proxyClusterInfo);
    }

    protected void mockChannel(String addr, int port) {
        when(channelHandlerContext.channel()).thenReturn(channel);
        when(channel.remoteAddress()).thenReturn(new InetSocketAddress(addr, port));
        when(channel.localAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 1));
    }
}
