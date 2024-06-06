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

import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeaderV2;
import org.apache.rocketmq.proxy.broker.client.ProducerManager;
import org.apache.rocketmq.proxy.broker.client.net.Broker2Client;
import org.apache.rocketmq.proxy.proxy.Proxy;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.netty.ResponseFuture;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.Silent.class)
public class ReplyMessageProcessorTest extends ProcessorTestBase {
    @Mock
    private RemotingClient remotingClient;
    @Mock
    private Broker2Client broker2Client;
    @Mock
    private ProducerManager producerManager;

    private final String topic = "testTopic";
    private final String proxyName = "proxy-a";
    private final String brokerAddr = "127.0.0.1:10909";
    @Before
    public void setUp() throws Exception {
        RemotingCommand respOk = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "");
        doAnswer(v -> {
            ResponseFuture responseFuture = mock(ResponseFuture.class);
            doReturn(respOk)
                    .when(responseFuture).getResponseCommand();
            v.getArgument(3, InvokeCallback.class).operationComplete(responseFuture);
            return null;
        }).when(remotingClient).invokeAsync(anyString(), any(), anyLong(), any(InvokeCallback.class));

        when(broker2Client.callClient(any(),any())).thenReturn(respOk);
        when(producerManager.findChannel(anyString())).thenReturn(this.channel);

        doReturn(this.remotingClient).when(this.brokerController).getRemotingClient();
        doReturn(this.broker2Client).when(this.brokerController).getBroker2Client();
        doReturn(this.producerManager).when(this.brokerController).getProducerManager();

        this.processor = new ReplyMessageProcessor(this.brokerController);
    }

    @Test
    public void testProcessRequestSendReplyMessage() throws Exception {
        mockChannel("127.1.1.1", 1);
        mockTopic();
        try (MockedStatic<Proxy> mocked = mockStatic(Proxy.class)) {
            mockProxy(mocked, proxyName, brokerAddr);
            RemotingCommand response = sendReplyMessage(null, "hello world".getBytes(StandardCharsets.UTF_8));
            assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);

            SendMessageRequestHeader header = new SendMessageRequestHeader();
            header.setTopic(topic);
            header.setQueueId(0);
            header.setDefaultTopic("TBW102");
            header.setDefaultTopicQueueNums(1);
            header.setFlag(0);
            header.setBornTimestamp(System.currentTimeMillis());
            header.setProperties("REPLY_TO_CLIENT\0011");
            response = sendReplyMessage(header, "hello world".getBytes(StandardCharsets.UTF_8));
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        }
    }

    @Test
    public void testProcessRequestSendReplyMessageV2() throws Exception {
        mockChannel("127.1.1.1", 1);
        mockTopic();
        try (MockedStatic<Proxy> mocked = mockStatic(Proxy.class)) {
            mockProxy(mocked, proxyName, brokerAddr);
            RemotingCommand response = sendReplyMessage(null, "hello world".getBytes(StandardCharsets.UTF_8));
            assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);

            SendMessageRequestHeader header = new SendMessageRequestHeader();
            header.setTopic(topic);
            header.setQueueId(0);
            header.setDefaultTopic("TBW102");
            header.setDefaultTopicQueueNums(1);
            header.setFlag(0);
            header.setBornTimestamp(System.currentTimeMillis());
            header.setProperties("REPLY_TO_CLIENT\0011");
            response = sendReplyMessageV2(header, "hello world".getBytes(StandardCharsets.UTF_8));
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        }
    }

    private RemotingCommand sendReplyMessage(SendMessageRequestHeader header, byte[] body) throws Exception {
        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.SEND_REPLY_MESSAGE, header);
        command.setBody(body);
        return processRequest(command);
    }

    private RemotingCommand sendReplyMessageV2(SendMessageRequestHeader header, byte[] body) throws Exception {
        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.SEND_REPLY_MESSAGE_V2,
                SendMessageRequestHeaderV2.createSendMessageRequestHeaderV2(header));
        command.setBody(body);
        return processRequest(command);
    }

    private void mockTopic() {
        brokerController.getTopicConfigManager().updateTopicConfig(new TopicConfig("testTopic"));
    }
}
