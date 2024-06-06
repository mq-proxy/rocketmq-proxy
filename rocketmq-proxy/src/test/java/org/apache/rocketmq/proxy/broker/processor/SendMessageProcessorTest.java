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

import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeaderV2;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

@RunWith(MockitoJUnitRunner.class)
public class SendMessageProcessorTest extends ProcessorTestBase {
    @Mock
    private RemotingClient remotingClient;
    private final String topic = "testTopic";
    private final String proxyName = "proxy-a";
    private final String brokerAddr = "127.0.0.1:10909";
    @Before
    public void setUp() throws Exception {
        doAnswer(v -> {
            ResponseFuture responseFuture = mock(ResponseFuture.class);
            doReturn(RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, ""))
                    .when(responseFuture).getResponseCommand();
            v.getArgument(3, InvokeCallback.class).operationComplete(responseFuture);
            return null;
        }).when(remotingClient).invokeAsync(anyString(), any(), anyLong(), any(InvokeCallback.class));

        doReturn(this.remotingClient).when(this.brokerController).getRemotingClient();
        this.processor = new SendMessageProcessor(this.brokerController);
    }

    @Test
    public void testProcessRequestSendMessage() throws Exception {
        try (MockedStatic<Proxy> mocked = mockStatic(Proxy.class)) {
            mockProxy(mocked, proxyName, brokerAddr);
            RemotingCommand response = sendMessage(null, "hello world".getBytes(StandardCharsets.UTF_8));
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

            SendMessageRequestHeader header = new SendMessageRequestHeader();
            header.setTopic("testTopic");
            header.setQueueId(0);
            response = sendMessage(header, "hello world".getBytes(StandardCharsets.UTF_8));
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        }
    }

    @Test
    public void testProcessRequestSendMessageV2() throws Exception {
        try (MockedStatic<Proxy> mocked = mockStatic(Proxy.class)) {
            mockProxy(mocked, proxyName, brokerAddr);
            SendMessageRequestHeader header = new SendMessageRequestHeader();
            header.setTopic(topic);
            header.setQueueId(1);
            RemotingCommand response = sendMessageV2(header, "hello world".getBytes(StandardCharsets.UTF_8));
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        }
    }

    @Test
    public void testProcessRequestSendMessageBatch() throws Exception {
        try (MockedStatic<Proxy> mocked = mockStatic(Proxy.class)) {
            mockProxy(mocked, proxyName, brokerAddr);
            SendMessageRequestHeader header = new SendMessageRequestHeader();
            header.setTopic(topic);
            header.setQueueId(1);
            RemotingCommand response = sendMessageBatch(header, "hello world".getBytes(StandardCharsets.UTF_8));
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        }
    }

    @Test
    public void testProcessRequestSendMessageBack() throws Exception {
        try (MockedStatic<Proxy> mocked = mockStatic(Proxy.class)) {
            mockProxy(mocked, proxyName, brokerAddr);

            ConsumerSendMsgBackRequestHeader header = new ConsumerSendMsgBackRequestHeader();
            header.setOriginTopic(topic);
            header.setOriginMsgId("test");
            RemotingCommand response = sendMessageBack(header);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        }
    }

    @Test
    public void sendMessageAsync() throws Exception {
        try (MockedStatic<Proxy> mocked = mockStatic(Proxy.class)) {
            mockProxy(mocked, proxyName, brokerAddr);
            doReturn(Executors.newFixedThreadPool(1)).when(brokerController).getPutMessageFutureExecutor();
            SendMessageRequestHeader header = new SendMessageRequestHeader();
            header.setTopic(topic);
            header.setQueueId(1);
            RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, header);
            CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
            asyncProcessRequest(command, future::complete);
            RemotingCommand response = future.get();
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        }
    }

    private RemotingCommand sendMessage(SendMessageRequestHeader header, byte[] body) throws Exception {
        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, header);
        command.setBody(body);
        return processRequest(command);
    }

    private RemotingCommand sendMessageV2(SendMessageRequestHeader header, byte[] body) throws Exception {
        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE_V2,
                SendMessageRequestHeaderV2.createSendMessageRequestHeaderV2(header));
        command.setBody(body);
        return processRequest(command);
    }

    private RemotingCommand sendMessageBatch(SendMessageRequestHeader header, byte[] body) throws Exception {
        header.setBatch(true);
        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.SEND_BATCH_MESSAGE,
                SendMessageRequestHeaderV2.createSendMessageRequestHeaderV2(header));
        command.setBody(body);
        return processRequest(command);
    }

    private RemotingCommand sendMessageBack(ConsumerSendMsgBackRequestHeader header) throws Exception {
        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.CONSUMER_SEND_MSG_BACK, header);
        return processRequest(command);
    }
}
