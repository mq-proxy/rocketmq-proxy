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

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.proxy.broker.BrokerController;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.proxy.common.config.ProxyConfig;
import org.apache.rocketmq.proxy.controller.ProxyController;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.apache.rocketmq.common.protocol.RequestCode.GET_BROKER_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;


@RunWith(MockitoJUnitRunner.class)
public class AdminBrokerProcessorTest extends AdminProcessTestBase {
    @Spy
    private ProxyController proxyController = new ProxyController(new ProxyConfig(), new BrokerConfig(), new NettyServerConfig(), new NettyClientConfig());
    @Spy
    private BrokerController brokerController = new BrokerController(proxyController);
    @Before
    public void setUp() {
        processor = new AdminBrokerProcessor(brokerController);
    }

    @Test
    public void processResponseSuccess() throws Exception {
        RemotingCommand response = processRequest(RemotingCommand.createRequestCommand(GET_BROKER_CONFIG, null));
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void processResponseFail() throws Exception {
        RemotingCommand response = processRequest(RemotingCommand.createRequestCommand(100002, null));
        assertThat(response.getCode()).isEqualTo(ResponseCode.REQUEST_CODE_NOT_SUPPORTED);
    }
}
