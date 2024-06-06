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
package org.apache.rocketmq.proxy.broker.processor.admin;

import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.DeleteSubscriptionGroupRequestHeader;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.proxy.broker.processor.AdminProcessTestBase;
import org.apache.rocketmq.proxy.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.proxy.proxy.Proxy;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.mock;


@RunWith(MockitoJUnitRunner.Silent.class)
public class SubscriptionGroupProcessorTest extends AdminProcessTestBase {

    private final String consumerGroup = "test-group";

    @Before
    public void setUp() throws Exception {
        doMockTopicQueueMapping();
        processor = new SubscriptionGroupProcessor(brokerController);
    }

    @Test
    public void testProcessUpdateAndCreateSubscriptionGroup() throws Exception {
        try (MockedStatic<Proxy> mockedStatic = mockStatic(Proxy.class)) {
            mockProxy(mockedStatic, proxyName, brokerAddr);
            doMockSubscriptionGroupManager();

            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(consumerGroup);
            subscriptionGroupConfig.setConsumeEnable(true);
            RemotingCommand request = RemotingCommand.createRequestCommand(
                    RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP, null);
            request.setBody(RemotingSerializable.encode(subscriptionGroupConfig));
            RemotingCommand response = processRequest(request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        }
    }

    @Test
    public void testProcessGetAllSubscriptionGroupConfig() throws Exception {
        try (MockedStatic<Proxy> mockedStatic = mockStatic(Proxy.class)) {
            mockProxy(mockedStatic, proxyName, brokerAddr);
            doMockGetSubscriptionGroupConfig();

            RemotingCommand request = RemotingCommand.createRequestCommand(
                    RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG, null);
            RemotingCommand response = processRequest(request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

            SubscriptionGroupManager subscriptionGroupManager = new SubscriptionGroupManager();
            subscriptionGroupManager.decode(new String(response.getBody()));
            assertThat(subscriptionGroupManager.getSubscriptionGroupTable()).isNotEmpty();
        }
    }

    @Test
    public void testProcessDeleteSubscriptionGroup() throws Exception {
        try (MockedStatic<Proxy> mockedStatic = mockStatic(Proxy.class)) {
            mockProxy(mockedStatic, proxyName, brokerAddr);
            doMockSubscriptionGroupManager();

            DeleteSubscriptionGroupRequestHeader requestHeader = new DeleteSubscriptionGroupRequestHeader();
            requestHeader.setGroupName(consumerGroup);
            RemotingCommand request = RemotingCommand.createRequestCommand(
                    RequestCode.DELETE_SUBSCRIPTIONGROUP, requestHeader);
            RemotingCommand response = processRequest(request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        }
    }

    private void doMockSubscriptionGroupManager() throws Exception {
        SubscriptionGroupManager subscriptionGroupManager = mock(SubscriptionGroupManager.class);
        doNothing().when(subscriptionGroupManager).updateSubscriptionGroupConfig(any());
        doNothing().when(subscriptionGroupManager).deleteSubscriptionGroupConfig(anyString());
        doReturn(subscriptionGroupManager).when(brokerController).getSubscriptionGroupManager();

        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "");
        doMockClusterMapping(response);
    }

    private void doMockGetSubscriptionGroupConfig() throws Exception {
        SubscriptionGroupManager subscriptionGroupManager = new SubscriptionGroupManager();
        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName(consumerGroup);
        subscriptionGroupManager.updateSubscriptionGroupConfig(subscriptionGroupConfig);
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "");
        response.setBody(subscriptionGroupManager.encode().getBytes(StandardCharsets.UTF_8));
        doMockClusterMapping(response);
    }
}
