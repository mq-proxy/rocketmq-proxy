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
import org.apache.rocketmq.common.protocol.header.QueryMessageRequestHeader;
import org.apache.rocketmq.proxy.proxy.Proxy;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;


import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mockStatic;

@RunWith(MockitoJUnitRunner.Silent.class)
public class QueryMessageProcessorTest extends AdminProcessTestBase {

    @Before
    public void setUp() throws Exception {
        doMockTopicQueueMapping();
        processor = new QueryMessageProcessor(brokerController);
    }

    @Test
    public void testQueryMessage() throws Exception {
        try (MockedStatic<Proxy> mockedStatic = mockStatic(Proxy.class)) {
            mockProxy(mockedStatic, proxyName, brokerAddr);
            doMockQueryMessage();

            QueryMessageRequestHeader requestHeader = new QueryMessageRequestHeader();
            requestHeader.setTopic(topic);
            requestHeader.setKey("1");
            requestHeader.setMaxNum(32);

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_MESSAGE, requestHeader);
            RemotingCommand response = processRequest(request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        }
    }

    @Test
    public void testViewMessageById() throws Exception {
        try (MockedStatic<Proxy> mockedStatic = mockStatic(Proxy.class)) {
            mockProxy(mockedStatic, proxyName, brokerAddr);
            doMockQueryMessage();

            QueryMessageRequestHeader requestHeader = new QueryMessageRequestHeader();
            requestHeader.setTopic(topic);
            requestHeader.setKey("1");
            requestHeader.setMaxNum(32);

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.VIEW_MESSAGE_BY_ID, requestHeader);
            RemotingCommand response = processRequest(request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        }
    }

    private void doMockQueryMessage() throws Exception {
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "");
        response.setBody(createMessage());
        doMockClusterMapping(response);
    }
}
