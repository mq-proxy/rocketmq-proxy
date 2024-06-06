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
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.PullMessageResponseHeader;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.doReturn;

@RunWith(MockitoJUnitRunner.class)
public class PullMessageProcessorTest extends ProcessorTestBase {
    @Mock
    private RemotingClient remotingClient;

    @Before
    public void setUp() throws Exception {
        doAnswer(v -> {
            ResponseFuture responseFuture = mock(ResponseFuture.class);
            RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS,
                    "pull message success",
                    PullMessageResponseHeader.class);
            response.addExtField("test", "test");
            doReturn(response).when(responseFuture).getResponseCommand();
            v.getArgument(3, InvokeCallback.class).operationComplete(responseFuture);
            return null;
        }).when(remotingClient).invokeAsync(anyString(), any(), anyLong(), any(InvokeCallback.class));

        doReturn(this.remotingClient).when(this.brokerController).getRemotingClient();
        this.processor = new PullMessageProcessor(this.brokerController);
    }

    @Test
    public void testProcessRequestPullMessage() throws Exception {
        try (MockedStatic<Proxy> mocked = mockStatic(Proxy.class)) {
            mockProxy(mocked, "proxy", "127.0.0.1:10911");
            int sysFlag = PullSysFlag.buildSysFlag(
                    true, // commitOffset
                    true, // suspend
                    false, // subscription
                    false // class filter
            );
            Long suspendTimeoutMillis = 15 * 1000L;
            PullMessageRequestHeader header = new PullMessageRequestHeader();
            header.setTopic("testTopic");
            header.setQueueId(1);
            header.setSysFlag(sysFlag);
            header.setSuspendTimeoutMillis(suspendTimeoutMillis);
            RemotingCommand response = pullMessage(header);
            assertThat(response).isNull();
        }
    }

    private RemotingCommand pullMessage(PullMessageRequestHeader header) throws Exception {
        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, header);
        command.setOpaque(123);
        return processRequest(command);
    }
}
