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

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager;
import org.apache.rocketmq.proxy.common.protocol.ProxyRequestCode;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultRequestProcessorSwitchTest {
    private DefaultRequestProcessor defaultRequestProcessor;

    private NamesrvController namesrvController;

    private NamesrvConfig namesrvConfig;

    private NettyServerConfig nettyServerConfig;

    private RouteInfoManager routeInfoManager;

    private RouteInfoManager proxyRouteInfoManager;

    private InternalLogger logger;

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

        defaultRequestProcessor = new DefaultRequestProcessor(namesrvController);

        logger = mock(InternalLogger.class);
        setFinalStatic(DefaultRequestProcessor.class.getDeclaredField("log"), logger);
    }

    @Test
    public void testSwitchBroker() throws RemotingCommandException {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(null);
        RemotingCommand request = RemotingCommand.createRequestCommand(ProxyRequestCode.NAMESERVER_SWITCH_TO_BROKER, null);
        RemotingCommand remotingCommand = defaultRequestProcessor.processRequest(ctx, request);
        assertThat(remotingCommand.getCode()).isEqualTo(ResponseCode.SUCCESS);

        // TYPE
        request = RemotingCommand.createRequestCommand(ProxyRequestCode.QUERY_NAMESERVER_TYPE, null);
        remotingCommand = defaultRequestProcessor.processRequest(ctx, request);
        assertThat(remotingCommand.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(remotingCommand.getRemark()).isEqualTo("BROKER");
    }

    @Test
    public void testSwitchProxy() throws RemotingCommandException {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(null);
        RemotingCommand request = RemotingCommand.createRequestCommand(ProxyRequestCode.NAMESERVER_SWITCH_TO_PROXY, null);
        RemotingCommand remotingCommand = defaultRequestProcessor.processRequest(ctx, request);
        assertThat(remotingCommand.getCode()).isEqualTo(ResponseCode.SUCCESS);

        // TYPE
        request = RemotingCommand.createRequestCommand(ProxyRequestCode.QUERY_NAMESERVER_TYPE, null);
        remotingCommand = defaultRequestProcessor.processRequest(ctx, request);
        assertThat(remotingCommand.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(remotingCommand.getRemark()).isEqualTo("PROXY");
    }

    private static void setFinalStatic(Field field, Object newValue) throws Exception {
        field.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        field.set(null, newValue);
    }
}
