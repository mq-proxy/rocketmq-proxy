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

import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.QueryCorrectionOffsetBody;
import org.apache.rocketmq.common.protocol.body.ResetOffsetBody;
import org.apache.rocketmq.common.protocol.header.CloneGroupOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetMaxOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetMinOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryCorrectionOffsetHeader;
import org.apache.rocketmq.common.protocol.header.ResetOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.SearchOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.SearchOffsetResponseHeader;
import org.apache.rocketmq.proxy.broker.client.net.Broker2Client;
import org.apache.rocketmq.proxy.broker.processor.AdminProcessTestBase;
import org.apache.rocketmq.proxy.helper.MapBuilder;
import org.apache.rocketmq.proxy.proxy.Proxy;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.Silent.class)
public class OffsetProcessorTest extends AdminProcessTestBase {

    private final String consumerGroup = "test-consumer-group";

    @Before
    public void setUp() throws Exception {
        doMockTopicQueueMapping();
        processor = new OffsetProcessor(brokerController);
    }

    @Test
    public void processSearchOffsetByTimestamp() throws Exception {
        try (MockedStatic<Proxy> mockedStatic = mockStatic(Proxy.class)) {
            mockProxy(mockedStatic, proxyName, brokerAddr);
            doMockSearchOffsetByTimestamp();
            SearchOffsetRequestHeader requestHeader = new SearchOffsetRequestHeader();
            requestHeader.setTopic(topic);
            requestHeader.setQueueId(0);
            requestHeader.setTimestamp(System.currentTimeMillis());
            RemotingCommand request = RemotingCommand.createRequestCommand(
                    RequestCode.SEARCH_OFFSET_BY_TIMESTAMP, requestHeader);
            RemotingCommand response = processRequest(request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
            assertThat(Long.parseLong(response.getExtFields().get("offset"))).isEqualTo(1024);
        }
    }

    @Test
    public void processGetMinOffset() throws Exception {
        try (MockedStatic<Proxy> mockedStatic = mockStatic(Proxy.class)) {
            mockProxy(mockedStatic, proxyName, brokerAddr);
            doMockSearchOffsetByTimestamp();
            GetMinOffsetRequestHeader requestHeader = new GetMinOffsetRequestHeader();
            requestHeader.setTopic(topic);
            requestHeader.setQueueId(0);
            RemotingCommand request = RemotingCommand.createRequestCommand(
                    RequestCode.SEARCH_OFFSET_BY_TIMESTAMP, requestHeader);
            RemotingCommand response = processRequest(request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
            assertThat(Long.parseLong(response.getExtFields().get("offset"))).isEqualTo(1024);
        }
    }

    @Test
    public void processGetMaxOffset() throws Exception {
        try (MockedStatic<Proxy> mockedStatic = mockStatic(Proxy.class)) {
            mockProxy(mockedStatic, proxyName, brokerAddr);
            doMockSearchOffsetByTimestamp();
            GetMaxOffsetRequestHeader requestHeader = new GetMaxOffsetRequestHeader();
            requestHeader.setTopic(topic);
            requestHeader.setQueueId(0);
            RemotingCommand request = RemotingCommand.createRequestCommand(
                    RequestCode.SEARCH_OFFSET_BY_TIMESTAMP, requestHeader);
            RemotingCommand response = processRequest(request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
            assertThat(Long.parseLong(response.getExtFields().get("offset"))).isEqualTo(1024);
        }
    }

    @Test
    public void processGetAllConsumerOffset() throws Exception {
        try (MockedStatic<Proxy> mockedStatic = mockStatic(Proxy.class)) {
            mockProxy(mockedStatic, proxyName, brokerAddr);
            doMockGetAllConsumerOffset();

            RemotingCommand request = RemotingCommand.createRequestCommand(
                    RequestCode.GET_ALL_CONSUMER_OFFSET, null);
            RemotingCommand response = processRequest(request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        }
    }

    @Test
    public void processGetAllDelayOffset() throws Exception {
        try (MockedStatic<Proxy> mockedStatic = mockStatic(Proxy.class)) {
            mockProxy(mockedStatic, proxyName, brokerAddr);
            doMockGetAllDelayOffset();

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_DELAY_OFFSET, null);
            RemotingCommand response = processRequest(request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        }
    }

    @Test
    public void processResetOffset() throws Exception {
        try (MockedStatic<Proxy> mockedStatic = mockStatic(Proxy.class)) {
            mockProxy(mockedStatic, proxyName, brokerAddr);
            doMockResetOffset();

            ResetOffsetRequestHeader resetOffsetRequestHeader = new ResetOffsetRequestHeader();
            resetOffsetRequestHeader.setGroup(consumerGroup);
            resetOffsetRequestHeader.setTopic(topic);
            resetOffsetRequestHeader.setTimestamp(-1);
            resetOffsetRequestHeader.setForce(true);
            RemotingCommand request = RemotingCommand.createRequestCommand(
                    RequestCode.INVOKE_BROKER_TO_RESET_OFFSET, resetOffsetRequestHeader);
            RemotingCommand response = processRequest(request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
            ResetOffsetBody resetOffsetBody = ResetOffsetBody.decode(response.getBody(), ResetOffsetBody.class);
            assertThat(resetOffsetBody.getOffsetTable()).containsKey(new MessageQueue(topic, proxyName, 0));
        }
    }

    @Test
    public void processQueryCorrectionOffset() throws Exception {
        try (MockedStatic<Proxy> mockedStatic = mockStatic(Proxy.class)) {
            mockProxy(mockedStatic, proxyName, brokerAddr);
            doMockQueryCorrectionOffset();

            QueryCorrectionOffsetHeader queryCorrectionOffsetHeader = new QueryCorrectionOffsetHeader();
            queryCorrectionOffsetHeader.setTopic(topic);
            queryCorrectionOffsetHeader.setCompareGroup(consumerGroup);

            RemotingCommand request = RemotingCommand.createRequestCommand(
                    RequestCode.QUERY_CORRECTION_OFFSET, queryCorrectionOffsetHeader);
            RemotingCommand response = processRequest(request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
            QueryCorrectionOffsetBody queryCorrectionOffsetBody =
                    QueryCorrectionOffsetBody.decode(response.getBody(), QueryCorrectionOffsetBody.class);
            assertThat(queryCorrectionOffsetBody.getCorrectionOffsets()).isEqualTo(MapBuilder.builder(0, 0L).build());
        }
    }

    @Test
    public void processCloneGroupOffset() throws Exception {
        try (MockedStatic<Proxy> mockedStatic = mockStatic(Proxy.class)) {
            mockProxy(mockedStatic, proxyName, brokerAddr);
            doMockCloneGroupOffset();

            CloneGroupOffsetRequestHeader cloneOffsetRequestHeader = new CloneGroupOffsetRequestHeader();
            cloneOffsetRequestHeader.setTopic(topic);
            cloneOffsetRequestHeader.setSrcGroup(consumerGroup);
            cloneOffsetRequestHeader.setDestGroup(consumerGroup + "_clone");
            cloneOffsetRequestHeader.setOffline(false);
            RemotingCommand request = RemotingCommand.createRequestCommand(
                    RequestCode.CLONE_GROUP_OFFSET, cloneOffsetRequestHeader);
            RemotingCommand response = processRequest(request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        }
    }

    private void doMockSearchOffsetByTimestamp() throws Exception {
        SearchOffsetResponseHeader responseHeader = new SearchOffsetResponseHeader();
        responseHeader.setOffset(1024L);
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "");
        response.writeCustomHeader(responseHeader);
        response.makeCustomHeaderToNet();
        doMockClusterMapping(response);
    }

    private void doMockGetAllConsumerOffset() throws Exception {
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "");
        doMockClusterMapping(response);
    }

    private void doMockGetAllDelayOffset() throws Exception {
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "");
        doMockClusterMapping(response);
    }

    private void doMockResetOffset() throws Exception {
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "");

        ResetOffsetBody offsetBody = new ResetOffsetBody();
        offsetBody.setOffsetTable(MapBuilder.builder(new MessageQueue(
                topic, brokerName, 0
        ), 0L).build());
        response.setBody(offsetBody.encode());
        doMockClusterMapping(response);

        Broker2Client broker2Client = mock(Broker2Client.class);
        // 将参数回传
        when(broker2Client.resetOffset(anyString(), anyString(), anyLong(), anyBoolean(), any())).thenAnswer(
                (invocation) -> {
                    ResetOffsetBody clientOffsetBody = new ResetOffsetBody();
                    clientOffsetBody.setOffsetTable(invocation.getArgument(4));
                    RemotingCommand clientResponse = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "");
                    clientResponse.setBody(clientOffsetBody.encode());
                    return clientResponse;
                });
        doReturn(broker2Client).when(brokerController).getBroker2Client();
    }

    private void doMockCloneGroupOffset() throws Exception {
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "");
        doMockClusterMapping(response);
    }

    private void doMockQueryCorrectionOffset() throws Exception {
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "");
        QueryCorrectionOffsetBody queryCorrectionOffsetBody = new QueryCorrectionOffsetBody();
        queryCorrectionOffsetBody.setCorrectionOffsets(MapBuilder.builder(0, 0L).build());
        response.setBody(queryCorrectionOffsetBody.encode());
        doMockClusterMapping(response);
    }
}
