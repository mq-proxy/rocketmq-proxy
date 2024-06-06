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

import com.google.common.collect.Sets;
import org.apache.rocketmq.common.namesrv.TopAddressing;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.proxy.common.protocol.TopicRouteTable;
import org.apache.rocketmq.proxy.helper.MapBuilder;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProxyOuterAPITest {
    private ProxyOuterAPI proxyOuterAPI;
    private RemotingClient remotingClient;

    @Before
    public void setUp() {
        proxyOuterAPI = new ProxyOuterAPI(new NettyClientConfig());
    }

    @Test
    public void testStart() {
        proxyOuterAPI.start();
        proxyOuterAPI.shutdown();
    }

    @Test
    public void testFetchNameServerAddr() throws NoSuchFieldException, IllegalAccessException {
//        assertThat(proxyOuterAPI.fetchNameServerAddr()).isNull();
        TopAddressing topAddressing = mock(TopAddressing.class);
        when(topAddressing.fetchNSAddr()).thenReturn("127.0.0.1:9876;127.0.0.2:9876");
        Field field = ProxyOuterAPI.class.getDeclaredField("topAddressing");
        field.setAccessible(true);
        field.set(proxyOuterAPI, topAddressing);
        assertThat(proxyOuterAPI.fetchNameServerAddr()).isEqualTo("127.0.0.1:9876;127.0.0.2:9876");
    }

    @Test
    public void testUpdateNameServerAddressList() throws NoSuchFieldException, IllegalAccessException {
        assertThat(getRemotingClient().getNameServerAddressList()).isNull();
        proxyOuterAPI.updateNameServerAddressList("127.0.0.1:9876;127.0.0.2:9876");
        assertThat(new HashSet<>(getRemotingClient().getNameServerAddressList()))
                .isEqualTo(Sets.newHashSet("127.0.0.1:9876", "127.0.0.2:9876"));
    }

    @Test
    public void testGetProxyClusterInfo() throws RemotingConnectException, RemotingSendRequestException,
            RemotingTimeoutException, InterruptedException, NoSuchFieldException, IllegalAccessException {
        mockRemotingClient();
        assertThat(proxyOuterAPI.getProxyClusterInfo()).isNull();

        ClusterInfo clusterInfo = new ClusterInfo();

        clusterInfo.setClusterAddrTable(new MapBuilder<String, Set<String>>()
                .add("test", Sets.newHashSet("a", "b"))
                .build());

        RemotingCommand response = RemotingCommand.createResponseCommand(0, "");
        response.setBody(clusterInfo.encode());
        when(remotingClient.invokeSync(any(), any(), anyLong())).thenReturn(response);
        assertThat(proxyOuterAPI.getProxyClusterInfo().getClusterAddrTable())
                .isEqualTo(new MapBuilder<String, Set<String>>()
                        .add("test", Sets.newHashSet("a", "b"))
                        .build());
    }

    @Test
    public void testGetBrokerClusterInfo() throws RemotingConnectException, RemotingSendRequestException,
            RemotingTimeoutException, InterruptedException, NoSuchFieldException, IllegalAccessException {
        mockRemotingClient();
        assertThat(proxyOuterAPI.getBrokerClusterInfo()).isNull();

        ClusterInfo clusterInfo = new ClusterInfo();

        clusterInfo.setClusterAddrTable(new MapBuilder<String, Set<String>>()
                .add("test", Sets.newHashSet("c", "d"))
                .build());

        RemotingCommand response = RemotingCommand.createResponseCommand(0, "");
        response.setBody(clusterInfo.encode());
        when(remotingClient.invokeSync(any(), any(), anyLong())).thenReturn(response);
        assertThat(proxyOuterAPI.getBrokerClusterInfo().getClusterAddrTable())
                .isEqualTo(new MapBuilder<String, Set<String>>()
                        .add("test", Sets.newHashSet("c", "d"))
                        .build());
    }

    @Test
    public void testGetBrokerTopicList() throws RemotingConnectException, RemotingSendRequestException,
            RemotingTimeoutException, InterruptedException, NoSuchFieldException, IllegalAccessException {
        mockRemotingClient();
        assertThat(proxyOuterAPI.getBrokerTopicList()).isNull();

        TopicList topicList = new TopicList();
        topicList.setTopicList(Sets.newHashSet("a", "b"));

        RemotingCommand response = RemotingCommand.createResponseCommand(0, "");
        response.setBody(topicList.encode());
        when(remotingClient.invokeSync(any(), any(), anyLong())).thenReturn(response);
        assertThat(proxyOuterAPI.getBrokerTopicList().getTopicList())
                .isEqualTo(Sets.newHashSet("a", "b"));
    }

    @Test
    public void testGetProxyTopicList() throws RemotingConnectException, RemotingSendRequestException,
            RemotingTimeoutException, InterruptedException, NoSuchFieldException, IllegalAccessException {
        mockRemotingClient();
        assertThat(proxyOuterAPI.getProxyTopicList()).isNull();

        TopicList topicList = new TopicList();
        topicList.setTopicList(Sets.newHashSet("c", "d"));

        RemotingCommand response = RemotingCommand.createResponseCommand(0, "");
        response.setBody(topicList.encode());
        when(remotingClient.invokeSync(any(), any(), anyLong())).thenReturn(response);
        assertThat(proxyOuterAPI.getProxyTopicList().getTopicList())
                .isEqualTo(Sets.newHashSet("c", "d"));
    }

    @Test
    public void testGetProxyTopicRoute() throws RemotingConnectException, RemotingSendRequestException,
            RemotingTimeoutException, InterruptedException, NoSuchFieldException, IllegalAccessException {
        mockRemotingClient();

        assertThat(proxyOuterAPI.getProxyTopicRoute("test")).isNull();

        TopicRouteData topicRouteData = new TopicRouteData();
        topicRouteData.setBrokerDatas(Arrays.asList(new BrokerData(), new BrokerData()));
        topicRouteData.setQueueDatas(Arrays.asList(new QueueData(), new QueueData()));

        RemotingCommand response = RemotingCommand.createResponseCommand(0, "");
        response.setBody(topicRouteData.encode());
        when(remotingClient.invokeSync(any(), any(), anyLong())).thenReturn(response);
        assertThat(proxyOuterAPI.getProxyTopicRoute("test"))
                .isEqualTo(topicRouteData);
    }

    @Test
    public void testGetBrokerTopicRoute() throws RemotingConnectException, RemotingSendRequestException,
            RemotingTimeoutException, InterruptedException, NoSuchFieldException, IllegalAccessException {
        mockRemotingClient();

        assertThat(proxyOuterAPI.getBrokerTopicRoute()).isNull();

        TopicRouteData topicRouteData = new TopicRouteData();
        topicRouteData.setBrokerDatas(Collections.singletonList(new BrokerData()));
        topicRouteData.setQueueDatas(Collections.singletonList(new QueueData()));
        TopicRouteTable topicRouteTable = new TopicRouteTable();
        topicRouteTable.getTopicRouteData().put("test", topicRouteData);

        RemotingCommand response = RemotingCommand.createResponseCommand(0, "");
        response.setBody(topicRouteTable.encode());
        when(remotingClient.invokeSync(any(), any(), anyLong())).thenReturn(response);
        assertThat(proxyOuterAPI.getBrokerTopicRoute().getTopicRouteData())
                .isEqualTo(topicRouteTable.getTopicRouteData());
    }

    @Test
    public void testRegisterRPCHook() {
        proxyOuterAPI.registerRPCHook(null);
    }

    private void mockRemotingClient() throws NoSuchFieldException, IllegalAccessException, RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        remotingClient = mock(NettyRemotingClient.class);
        when(remotingClient.invokeSync(anyString(), any(), anyLong())).thenReturn(
                RemotingCommand.createResponseCommand(0, "")
        );

        proxyOuterAPI = new ProxyOuterAPI(new NettyClientConfig());
        Field field = ProxyOuterAPI.class.getDeclaredField("remotingClient");
        field.setAccessible(true);
        field.set(proxyOuterAPI, remotingClient);
    }

    private RemotingClient getRemotingClient() throws IllegalAccessException, NoSuchFieldException {
        Field field = ProxyOuterAPI.class.getDeclaredField("remotingClient");
        field.setAccessible(true);
        return (RemotingClient) field.get(proxyOuterAPI);
    }
}
