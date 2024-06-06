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
package org.apache.rocketmq.proxy.proxy.processor;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.proxy.broker.BrokerController;
import org.apache.rocketmq.proxy.broker.client.ConsumerManager;
import org.apache.rocketmq.proxy.common.protocol.NotifyRegisterHeader;
import org.apache.rocketmq.proxy.common.protocol.NotifySwitchHeader;
import org.apache.rocketmq.proxy.common.protocol.ProxyRequestCode;
import org.apache.rocketmq.proxy.controller.ProxyController;
import org.apache.rocketmq.proxy.proxy.HeartBeatManager;
import org.apache.rocketmq.proxy.proxy.mapping.proxy.ClusterMapping;
import org.apache.rocketmq.proxy.proxy.routeinfo.ProxyClusterInfo;
import org.apache.rocketmq.proxy.proxy.routeinfo.TopicRouteInfo;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.Silent.class)
public class ProxyProcessorTest {

    private ProxyProcessor proxyProcessor;

    @Mock
    private ProxyController proxyController;
    @Mock
    private BrokerController brokerController;
    @Mock
    private ProxyClusterInfo proxyClusterInfo;
    @Mock
    private ClusterMapping clusterMapping;
    @Mock
    private TopicRouteInfo topicRouteInfo;
    @Before
    public void setUp() throws Exception {
        when(brokerController.getRequestProcessor()).thenReturn(new UnknownProcessor());
        when(proxyController.getBrokerController()).thenReturn(brokerController);
        when(proxyController.getProxyClusterInfo()).thenReturn(proxyClusterInfo);
        when(proxyController.getClusterMapping()).thenReturn(clusterMapping);
        when(proxyController.getTopicRouteInfo()).thenReturn(topicRouteInfo);
        proxyProcessor = new ProxyProcessor(proxyController);
    }

    @Test
    public void testProcessRequest() throws Exception {
        assertThat(
                proxyProcessor.processRequest(null, RemotingCommand.createRequestCommand(1, null)).getCode())
                .isEqualTo(ResponseCode.REQUEST_CODE_NOT_SUPPORTED);
    }

    @Test
    public void testProcessRequestNotifyProxyRegisterBrokerOrProxy() throws Exception {
        NotifyRegisterHeader registerHeader = new NotifyRegisterHeader();
        RemotingCommand request = RemotingCommand.createRequestCommand(
                ProxyRequestCode.NOTIFY_PROXY_REGISTER_BROKER_OR_PROXY, registerHeader);
        assertThat(processRequest(request).getCode()).isEqualTo(ResponseCode.SUCCESS);

        registerHeader.setProxy(true);
        request = RemotingCommand.createRequestCommand(
                ProxyRequestCode.NOTIFY_PROXY_REGISTER_BROKER_OR_PROXY, registerHeader);
        assertThat(processRequest(request).getCode()).isEqualTo(ResponseCode.SUCCESS);

        registerHeader.setRegister(true);
        request = RemotingCommand.createRequestCommand(
                ProxyRequestCode.NOTIFY_PROXY_REGISTER_BROKER_OR_PROXY, registerHeader);
        assertThat(processRequest(request).getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testProcessRequestNotifyProxyBrokerSwitch() throws Exception {
        ConsumerManager consumerManager = mock(ConsumerManager.class);
        when(brokerController.getConsumerManager()).thenReturn(consumerManager);
        doAnswer((invocation) -> {
            BiConsumer<String, String> consumer = invocation.getArgument(0);
            consumer.accept("test-group", "test-client-id");
            return null;
        }).when(consumerManager).doConsumersWithGroupAndClientID(any());

        NotifySwitchHeader notifySwitchHeader = new NotifySwitchHeader();
        RemotingCommand request = RemotingCommand.createRequestCommand(
                ProxyRequestCode.NOTIFY_PROXY_BROKER_SWITCH, notifySwitchHeader);
        when(proxyController.getHeartBeatManager()).thenReturn(new HeartBeatManager());
        assertThat(processRequest(request).getCode()).isEqualTo(ResponseCode.SUCCESS);

        // switch to proxy
        notifySwitchHeader.setProxy(true);
        request = RemotingCommand.createRequestCommand(
                ProxyRequestCode.NOTIFY_PROXY_BROKER_SWITCH, notifySwitchHeader);
        assertThat(processRequest(request).getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testRejectRequest() {
        assertThat(proxyProcessor.rejectRequest()).isFalse();
    }

    private RemotingCommand processRequest(RemotingCommand request) throws Exception {
        request.makeCustomHeaderToNet();
        return proxyProcessor.processRequest(null, request);
    }

    private static class UnknownProcessor implements NettyRequestProcessor {
        @Override
        public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
            return RemotingCommand.createResponseCommand(ResponseCode.REQUEST_CODE_NOT_SUPPORTED, "");
        }

        @Override
        public boolean rejectRequest() {
            return false;
        }
    }
}