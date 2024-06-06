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
package org.apache.rocketmq.namesrv.processor;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.protocol.header.namesrv.QueryDataVersionRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.QueryDataVersionResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.RegisterBrokerRequestHeader;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager;
import org.apache.rocketmq.proxy.common.protocol.ProxyRequestCode;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;
import org.assertj.core.util.Maps;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultRequestProcessorProxyTest {
    private DefaultRequestProcessor defaultRequestProcessor;

    private NamesrvController namesrvController;

    private NamesrvConfig namesrvConfig;

    private NettyServerConfig nettyServerConfig;

    private RouteInfoManager routeInfoManager;

    private RouteInfoManager proxyRouteInfoManager;

    private InternalLogger logger;

    private final int EMPTY_RESULT_LEN = "{\"topicList\":[]}".length();

    @Before
    public void init() throws Exception {
        namesrvConfig = new NamesrvConfig();
        nettyServerConfig = new NettyServerConfig();
        routeInfoManager = new RouteInfoManager();
        proxyRouteInfoManager = new RouteInfoManager();

        namesrvController = new NamesrvController(namesrvConfig, nettyServerConfig);

        Field field = NamesrvController.class.getDeclaredField("brokerRouteInfoManager");
        field.setAccessible(true);
        field.set(namesrvController, routeInfoManager);
        field = NamesrvController.class.getDeclaredField("proxyRouteInfoManager");
        field.setAccessible(true);
        field.set(namesrvController, proxyRouteInfoManager);
        namesrvController.changeRouteInfoManagerToProxy();

        defaultRequestProcessor = new DefaultRequestProcessor(namesrvController);

        registerRouteInfoManager();

        logger = mock(InternalLogger.class);
        setFinalStatic(DefaultRequestProcessor.class.getDeclaredField("log"), logger);
    }

    @Test
    public void testProcessRequest_RegisterBroker() throws RemotingCommandException,
        NoSuchFieldException, IllegalAccessException {
        RemotingCommand request = genSampleRegisterCmd(true);
        request.setVersion(35);


        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(null);

        RemotingCommand response = defaultRequestProcessor.processRequest(ctx, request);

        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(response.getRemark()).isNull();

        RouteInfoManager routes = namesrvController.getBrokerRouteInfoManager();
        Field brokerAddrTable = RouteInfoManager.class.getDeclaredField("brokerAddrTable");
        brokerAddrTable.setAccessible(true);

        BrokerData broker = new BrokerData();
        broker.setBrokerName("broker");
        broker.setBrokerAddrs((HashMap<Long, String>) Maps.newHashMap(2333L, "10.10.1.1"));

        assertThat((Map) brokerAddrTable.get(routes))
            .contains(new HashMap.SimpleEntry("broker", broker));
    }

    @Test
    public void testProcessRequest_RegisterBrokerWithFilterServer() throws RemotingCommandException,
        NoSuchFieldException, IllegalAccessException {
        RemotingCommand request = genSampleRegisterCmd(true);

        // version >= MQVersion.Version.V3_0_11.ordinal() to register with filter server
        request.setVersion(100);

        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(null);

        RemotingCommand response = defaultRequestProcessor.processRequest(ctx, request);

        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(response.getRemark()).isNull();

        RouteInfoManager routes = namesrvController.getBrokerRouteInfoManager();
        Field brokerAddrTable = RouteInfoManager.class.getDeclaredField("brokerAddrTable");
        brokerAddrTable.setAccessible(true);

        BrokerData broker = new BrokerData();
        broker.setBrokerName("broker");
        broker.setBrokerAddrs((HashMap<Long, String>) Maps.newHashMap(2333L, "10.10.1.1"));

        assertThat((Map) brokerAddrTable.get(routes))
            .contains(new HashMap.SimpleEntry("broker", broker));
    }

    @Test
    public void testProcessRequest_UnregisterBroker() throws RemotingCommandException, NoSuchFieldException, IllegalAccessException {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(null);

        //Register broker
        RemotingCommand regRequest = genSampleRegisterCmd(true);
        defaultRequestProcessor.processRequest(ctx, regRequest);

        //Unregister broker
        RemotingCommand unregRequest = genSampleRegisterCmd(false);
        RemotingCommand unregResponse = defaultRequestProcessor.processRequest(ctx, unregRequest);

        assertThat(unregResponse.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(unregResponse.getRemark()).isNull();

        RouteInfoManager routes = namesrvController.getBrokerRouteInfoManager();
        Field brokerAddrTable = RouteInfoManager.class.getDeclaredField("brokerAddrTable");
        brokerAddrTable.setAccessible(true);

        assertThat((Map) brokerAddrTable.get(routes)).isEmpty();
    }

    @Test
    public void testProcessRequest_RegisterProxy() throws RemotingCommandException,
            NoSuchFieldException, IllegalAccessException {
        RemotingCommand request = genSampleRegisterProxyCmd(true);
        request.setVersion(35);


        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(null);

        RemotingCommand response = defaultRequestProcessor.processRequest(ctx, request);

        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(response.getRemark()).isNull();

        RouteInfoManager routes = namesrvController.getRouteInfoManager();
        Field brokerAddrTable = RouteInfoManager.class.getDeclaredField("brokerAddrTable");
        brokerAddrTable.setAccessible(true);

        BrokerData proxy = new BrokerData();
        proxy.setBrokerName("proxy");
        proxy.setBrokerAddrs((HashMap<Long, String>) Maps.newHashMap(2333L, "10.10.1.1"));

        assertThat((Map) brokerAddrTable.get(routes))
                .contains(new HashMap.SimpleEntry("proxy", proxy));
    }

    @Test
    public void testProcessRequest_RegisterProxyWithFilterServer() throws RemotingCommandException,
            NoSuchFieldException, IllegalAccessException {
        RemotingCommand request = genSampleRegisterProxyCmd(true);

        // version >= MQVersion.Version.V3_0_11.ordinal() to register with filter server
        request.setVersion(100);

        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(null);

        RemotingCommand response = defaultRequestProcessor.processRequest(ctx, request);

        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(response.getRemark()).isNull();

        RouteInfoManager routes = namesrvController.getRouteInfoManager();
        Field brokerAddrTable = RouteInfoManager.class.getDeclaredField("brokerAddrTable");
        brokerAddrTable.setAccessible(true);

        BrokerData proxy = new BrokerData();
        proxy.setBrokerName("proxy");
        proxy.setBrokerAddrs((HashMap<Long, String>) Maps.newHashMap(2333L, "10.10.1.1"));

        assertThat((Map) brokerAddrTable.get(routes))
                .contains(new HashMap.SimpleEntry("proxy", proxy));
    }

    @Test
    public void testProcessRequest_UnregisterProxy() throws RemotingCommandException, NoSuchFieldException, IllegalAccessException {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(null);

        //Register broker
        RemotingCommand regRequest = genSampleRegisterProxyCmd(false);
        defaultRequestProcessor.processRequest(ctx, regRequest);

        //Unregister broker
        RemotingCommand unregRequest = genSampleRegisterCmd(false);
        RemotingCommand unregResponse = defaultRequestProcessor.processRequest(ctx, unregRequest);

        assertThat(unregResponse.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(unregResponse.getRemark()).isNull();

        RouteInfoManager routes = namesrvController.getRouteInfoManager();
        Field brokerAddrTable = RouteInfoManager.class.getDeclaredField("brokerAddrTable");
        brokerAddrTable.setAccessible(true);

        assertThat((Map) brokerAddrTable.get(routes)).isNotEmpty();
    }

    @Test
    public void testGetRouteInfoByTopic() throws RemotingCommandException {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(null);
        RemotingCommand request = getRemotingCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC);
        RemotingCommand remotingCommandSuccess = defaultRequestProcessor.processRequest(ctx, request);
        assertThat(remotingCommandSuccess.getCode()).isEqualTo(ResponseCode.SUCCESS);
        request.getExtFields().put("topic", "test");
        RemotingCommand remotingCommandNoTopicRouteInfo = defaultRequestProcessor.processRequest(ctx, request);
        assertThat(remotingCommandNoTopicRouteInfo.getCode()).isEqualTo(ResponseCode.TOPIC_NOT_EXIST);

        // broker: not exists
        RemotingCommand requestBroker = getRemotingCommand(ProxyRequestCode.GET_BROKER_ROUTEINFO_BY_TOPIC);
        remotingCommandNoTopicRouteInfo = defaultRequestProcessor.processRequest(ctx, requestBroker);
        assertThat(remotingCommandNoTopicRouteInfo.getCode()).isEqualTo(ResponseCode.TOPIC_NOT_EXIST);

        // proxy: exists
        RemotingCommand requestProxy = getRemotingCommand(ProxyRequestCode.GET_PROXY_ROUTEINFO_BY_TOPIC);
        remotingCommandSuccess = defaultRequestProcessor.processRequest(ctx, requestProxy);
        assertThat(remotingCommandSuccess.getCode()).isEqualTo(ResponseCode.SUCCESS);
        // proxy: not exists
        requestProxy.getExtFields().put("topic", "test");
        remotingCommandNoTopicRouteInfo = defaultRequestProcessor.processRequest(ctx, requestProxy);
        assertThat(remotingCommandNoTopicRouteInfo.getCode()).isEqualTo(ResponseCode.TOPIC_NOT_EXIST);
    }

    @Test
    public void testGetBrokerClusterInfo() throws RemotingCommandException {
        ClusterInfo clusterInfo = null;
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(null);
        RemotingCommand request = getRemotingCommand(RequestCode.GET_BROKER_CLUSTER_INFO);
        RemotingCommand remotingCommand = defaultRequestProcessor.processRequest(ctx, request);
        assertThat(remotingCommand.getCode()).isEqualTo(ResponseCode.SUCCESS);
        clusterInfo = ClusterInfo.decode(remotingCommand.getBody(), ClusterInfo.class);
        assertThat(clusterInfo).isNotNull();
        assertThat(clusterInfo.getBrokerAddrTable()).isNotEmpty();

        // broker, should empty
        RemotingCommand requestBroker = getRemotingCommand(ProxyRequestCode.GET_BROKER_BROKER_CLUSTER_INFO);
        remotingCommand = defaultRequestProcessor.processRequest(ctx, requestBroker);
        assertThat(remotingCommand.getCode()).isEqualTo(ResponseCode.SUCCESS);
        clusterInfo = ClusterInfo.decode(remotingCommand.getBody(), ClusterInfo.class);
        assertThat(clusterInfo).isNotNull();
        assertThat(clusterInfo.getBrokerAddrTable()).isEmpty();

        // proxy
        RemotingCommand requestBProxy = getRemotingCommand(ProxyRequestCode.GET_PROXY_BROKER_CLUSTER_INFO);
        remotingCommand = defaultRequestProcessor.processRequest(ctx, requestBProxy);
        assertThat(remotingCommand.getCode()).isEqualTo(ResponseCode.SUCCESS);
        clusterInfo = ClusterInfo.decode(remotingCommand.getBody(), ClusterInfo.class);
        assertThat(clusterInfo).isNotNull();
        assertThat(clusterInfo.getBrokerAddrTable()).isNotEmpty();
    }

    @Test
    public void testGetAllTopicListFromNameserver() throws RemotingCommandException {
        testBrokerProxyCommand(RequestCode.GET_ALL_TOPIC_LIST_FROM_NAMESERVER,
                ProxyRequestCode.GET_BROKER_ALL_TOPIC_LIST_FROM_NAMESERVER,
                ProxyRequestCode.GET_PROXY_ALL_TOPIC_LIST_FROM_NAMESERVER,
                EMPTY_RESULT_LEN, false);
    }

    @Test
    public void testGetTopicsByCluster() throws RemotingCommandException {
        RemotingCommand request = getRemotingCommand(RequestCode.GET_TOPICS_BY_CLUSTER);
        request.addExtField("cluster", "default-cluster");
        RemotingCommand brokerRequest = getRemotingCommand(ProxyRequestCode.GET_BROKER_TOPICS_BY_CLUSTER);
        brokerRequest.addExtField("cluster", "default-cluster");
        RemotingCommand proxyRequest = getRemotingCommand(ProxyRequestCode.GET_PROXY_TOPICS_BY_CLUSTER);
        proxyRequest.addExtField("cluster", "default-cluster");
        testBrokerProxyCommand(request, brokerRequest, proxyRequest, EMPTY_RESULT_LEN);
    }

    @Test
    public void testGetSystemTopicListFromNs() throws RemotingCommandException {
        testBrokerProxyCommand(RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_NS,
                ProxyRequestCode.GET_BROKER_SYSTEM_TOPIC_LIST_FROM_NS,
                ProxyRequestCode.GET_PROXY_SYSTEM_TOPIC_LIST_FROM_NS, EMPTY_RESULT_LEN, false);
    }

    @Test
    public void testGetUnitTopicList() throws RemotingCommandException {
        testBrokerProxyCommand(RequestCode.GET_UNIT_TOPIC_LIST,
                ProxyRequestCode.GET_BROKER_UNIT_TOPIC_LIST,
                ProxyRequestCode.GET_PROXY_UNIT_TOPIC_LIST,
                EMPTY_RESULT_LEN, true);
    }

    @Test
    public void testGetHasUnitSubTopicList() throws RemotingCommandException {
        testBrokerProxyCommand(RequestCode.GET_HAS_UNIT_SUB_TOPIC_LIST,
                ProxyRequestCode.GET_BROKER_HAS_UNIT_SUB_TOPIC_LIST,
                ProxyRequestCode.GET_PROXY_HAS_UNIT_SUB_TOPIC_LIST,
                EMPTY_RESULT_LEN, true);
    }

    @Test
    public void testGetHasUnitSubUnUnitTopicList() throws RemotingCommandException {
        testBrokerProxyCommand(RequestCode.GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST,
                ProxyRequestCode.GET_BROKER_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST,
                ProxyRequestCode.GET_PROXY_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST,
                EMPTY_RESULT_LEN, true);
    }

    @Test
    public void testQueryDataVersion() throws RemotingCommandException {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(null);
        QueryDataVersionRequestHeader requestHeader = new QueryDataVersionRequestHeader();
        DataVersion dataVersion = new DataVersion();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_DATA_VERSION, requestHeader);
        request.setBody(dataVersion.encode());
        // default, not empty
        RemotingCommand response = defaultRequestProcessor.processRequest(ctx, request);
        assertThat(response.getCode()).isEqualTo(RemotingSysResponseCode.SUCCESS);
        assertThat((QueryDataVersionResponseHeader)response.readCustomHeader())
                .isNotNull()
                .hasFieldOrPropertyWithValue("changed", true);
    }

    @Test
    public void testQueryProxyDataVersion() throws RemotingCommandException {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(null);
        QueryDataVersionRequestHeader requestHeader = new QueryDataVersionRequestHeader();
        DataVersion dataVersion = new DataVersion();
        RemotingCommand request = RemotingCommand.createRequestCommand(ProxyRequestCode.QUERY_PROXY_DATA_VERSION, requestHeader);
        request.setBody(dataVersion.encode());
        // default, not empty
        RemotingCommand response = defaultRequestProcessor.processRequest(ctx, request);
        assertThat(response.getCode()).isEqualTo(RemotingSysResponseCode.SUCCESS);
        assertThat((QueryDataVersionResponseHeader)response.readCustomHeader())
                .isNotNull()
                .hasFieldOrPropertyWithValue("changed", true);
    }

    @Test
    public void testUnknownCommand() throws RemotingCommandException {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(null);
        // default, not empty
        RemotingCommand response = defaultRequestProcessor.processRequest(ctx,
                RemotingCommand.createRequestCommand(-1, null));
        assertThat(response.getCode()).isEqualTo(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED);
    }

    private void testBrokerProxyCommand(RemotingCommand cmd,
                                        RemotingCommand brokerCmd,
                                        RemotingCommand proxyCmd,
                                        int resultLen) throws RemotingCommandException {
        testBrokerProxyCommand(cmd, brokerCmd, proxyCmd, resultLen, false);
    }
    private void testBrokerProxyCommand(RemotingCommand cmd,
                                        RemotingCommand brokerCmd,
                                        RemotingCommand proxyCmd,
                                        int resultLen,
                                        boolean same) throws RemotingCommandException {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(null);
        // default, not empty
        RemotingCommand response = defaultRequestProcessor.processRequest(ctx, cmd);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        if (same) {
            assertThat(response.getBody().length).isEqualTo(resultLen);
        } else {
            assertThat(response.getBody().length).isGreaterThan(resultLen);
        }


        // broker, empty
        response = defaultRequestProcessor.processRequest(ctx, brokerCmd);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(response.getBody().length).isEqualTo(resultLen);

        // proxy, not empty
        response = defaultRequestProcessor.processRequest(ctx, proxyCmd);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        if (same) {
            assertThat(response.getBody().length).isEqualTo(resultLen);
        } else {
            assertThat(response.getBody().length).isGreaterThan(resultLen);
        }
    }

    private void testBrokerProxyCommand(int code,
                                        int brokerCode,
                                        int proxyCode,
                                        int resultLen,
                                        boolean same) throws RemotingCommandException {
        testBrokerProxyCommand(RemotingCommand.createRequestCommand(code, null),
                RemotingCommand.createRequestCommand(brokerCode, null),
                RemotingCommand.createRequestCommand(proxyCode, null),
                resultLen,
                same);
    }

    private RemotingCommand getRemotingCommand(int code) {
        RegisterBrokerRequestHeader header = new RegisterBrokerRequestHeader();
        header.setBrokerName("broker");
        RemotingCommand request = RemotingCommand.createRequestCommand(code, header);
        request.addExtField("brokerName", "broker");
        request.addExtField("brokerAddr", "10.10.1.1");
        request.addExtField("clusterName", "cluster");
        request.addExtField("haServerAddr", "10.10.2.1");
        request.addExtField("brokerId", "2333");
        request.addExtField("topic", "unit-test");
        return request;
    }

    private static RemotingCommand genSampleRegisterCmd(boolean reg) {
        RegisterBrokerRequestHeader header = new RegisterBrokerRequestHeader();
        header.setBrokerName("broker");
        RemotingCommand request = RemotingCommand.createRequestCommand(
            reg ? RequestCode.REGISTER_BROKER : RequestCode.UNREGISTER_BROKER, header);
        request.addExtField("brokerName", "broker");
        request.addExtField("brokerAddr", "10.10.1.1");
        request.addExtField("clusterName", "cluster");
        request.addExtField("haServerAddr", "10.10.2.1");
        request.addExtField("brokerId", "2333");
        return request;
    }

    private static RemotingCommand genSampleRegisterProxyCmd(boolean reg) {
        RegisterBrokerRequestHeader header = new RegisterBrokerRequestHeader();
        header.setBrokerName("broker");
        RemotingCommand request = RemotingCommand.createRequestCommand(
                reg ? ProxyRequestCode.REGISTER_PROXY : ProxyRequestCode.UNREGISTER_PROXY, header);
        request.addExtField("brokerName", "proxy");
        request.addExtField("brokerAddr", "10.10.1.1");
        request.addExtField("clusterName", "cluster");
        request.addExtField("haServerAddr", "10.10.2.1");
        request.addExtField("brokerId", "2333");
        return request;
    }

    private static void setFinalStatic(Field field, Object newValue) throws Exception {
        field.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        field.set(null, newValue);
    }

    private void registerRouteInfoManager() {
        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
        ConcurrentHashMap<String, TopicConfig> topicConfigConcurrentHashMap = new ConcurrentHashMap<>();
        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setWriteQueueNums(8);
        topicConfig.setTopicName("unit-test");
        topicConfig.setPerm(6);
        topicConfig.setReadQueueNums(8);
        topicConfig.setOrder(false);
        topicConfigConcurrentHashMap.put("unit-test", topicConfig);
        topicConfigSerializeWrapper.setTopicConfigTable(topicConfigConcurrentHashMap);
        Channel channel = mock(Channel.class);
        proxyRouteInfoManager.registerBroker("default-cluster", "127.0.0.1:10911", "default-broker", 0, "127.0.0.1:1001",
            topicConfigSerializeWrapper, new ArrayList<>(), channel);
    }
}
