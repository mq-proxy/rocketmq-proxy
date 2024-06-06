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

import org.apache.rocketmq.common.admin.ConsumeStats;
import org.apache.rocketmq.common.admin.OffsetWrapper;
import org.apache.rocketmq.common.admin.TopicOffset;
import org.apache.rocketmq.common.admin.TopicStatsTable;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.BrokerStatsData;
import org.apache.rocketmq.common.protocol.body.GetConsumerStatusBody;
import org.apache.rocketmq.common.protocol.body.KVTable;
import org.apache.rocketmq.common.protocol.header.GetConsumeStatsRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerStatusRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetEarliestMsgStoretimeRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetEarliestMsgStoretimeResponseHeader;
import org.apache.rocketmq.common.protocol.header.GetTopicStatsInfoRequestHeader;
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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.Silent.class)
public class StatProcessorTest extends AdminProcessTestBase {

    private final String consumerGroup = "test-consumer-group";

    @Before
    public void setUp() throws Exception {
        doMockTopicQueueMapping();
        processor = new StatProcessor(brokerController);
    }

    @Test
    public void testGetBrokerRuntimeInfo() throws Exception {
        try (MockedStatic<Proxy> mockedStatic = mockStatic(Proxy.class)) {
            mockProxy(mockedStatic, brokerName, brokerAddr);
            doMockBrokerRuntimeInfo();

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_RUNTIME_INFO, null);
            RemotingCommand response = processRequest(request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
            KVTable kvTable = KVTable.decode(response.getBody(), KVTable.class);
            assertThat(kvTable).isNotNull();
            assertThat(kvTable.getTable().size()).isGreaterThan(0);
        }
    }

    @Test
    public void testGetConsumeStats() throws Exception {
        try (MockedStatic<Proxy> mockedStatic = mockStatic(Proxy.class)) {
            mockProxy(mockedStatic, proxyName, brokerAddr);
            doMockConsumeStats();

            GetConsumeStatsRequestHeader requestHeader = new GetConsumeStatsRequestHeader();
            requestHeader.setTopic(topic);
            requestHeader.setConsumerGroup(consumerGroup);
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUME_STATS, requestHeader);
            RemotingCommand response = processRequest(request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

            ConsumeStats consumeStats = ConsumeStats.decode(response.getBody(), ConsumeStats.class);
            assertThat(consumeStats).isNotNull();
            assertThat(consumeStats.getOffsetTable()).containsKey(new MessageQueue(
                    topic, proxyName, 0
            ));
        }
    }

    @Test
    public void testGetConsumerStats() throws Exception {
        try (MockedStatic<Proxy> mockedStatic = mockStatic(Proxy.class)) {
            mockProxy(mockedStatic, proxyName, brokerAddr);
            doMockConsumerStats();

            GetConsumerStatusRequestHeader requestHeader = new GetConsumerStatusRequestHeader();
            requestHeader.setTopic(topic);
            requestHeader.setGroup(consumerGroup);
            requestHeader.setClientAddr("127.0.0.1:11111");
            RemotingCommand request = RemotingCommand.createRequestCommand(
                    RequestCode.INVOKE_BROKER_TO_GET_CONSUMER_STATUS, requestHeader);
            RemotingCommand response = processRequest(request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        }
    }

    @Test
    public void testGetTopicStatsInfo() throws Exception {
        try (MockedStatic<Proxy> mockedStatic = mockStatic(Proxy.class)) {
            mockProxy(mockedStatic, proxyName, brokerAddr);
            doMockTopicStatInfo();

            GetTopicStatsInfoRequestHeader requestHeader = new GetTopicStatsInfoRequestHeader();
            requestHeader.setTopic(topic);
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_TOPIC_STATS_INFO, requestHeader);
            RemotingCommand response = processRequest(request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
            TopicStatsTable topicStatsTable = TopicStatsTable.decode(response.getBody(), TopicStatsTable.class);
            assertThat(topicStatsTable).isNotNull();
            assertThat(topicStatsTable.getOffsetTable()).containsKey(new MessageQueue(
                    topic, proxyName, 0
            ));
        }
    }

    @Test
    public void testViewBrokerStatsData() throws Exception {
        try (MockedStatic<Proxy> mockedStatic = mockStatic(Proxy.class)) {
            mockProxy(mockedStatic, proxyName, brokerAddr);
            doMockBrokerStats();
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.VIEW_BROKER_STATS_DATA, null);
            RemotingCommand response = processRequest(request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        }
    }

    @Test
    public void testGetEarliestMsgStoreTime() throws Exception {
        try (MockedStatic<Proxy> mockedStatic = mockStatic(Proxy.class)) {
            mockProxy(mockedStatic, proxyName, brokerAddr);
            doMockGetEarliestMsgStoreTime();

            GetEarliestMsgStoretimeRequestHeader requestHeader = new GetEarliestMsgStoretimeRequestHeader();
            requestHeader.setTopic(topic);
            requestHeader.setQueueId(0);
            RemotingCommand request =
                    RemotingCommand.createRequestCommand(RequestCode.GET_EARLIEST_MSG_STORETIME, requestHeader);
            RemotingCommand response = processRequest(request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
            assertThat(response.getExtFields().get("timestamp")).isEqualTo("1024");
        }
    }

    private void doMockBrokerRuntimeInfo() throws Exception {
        KVTable kvTable = new KVTable();
        kvTable.setTable(MapBuilder.builder("test", "1").build());
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "");
        response.setBody(kvTable.encode());
        doMockClusterMapping(response);
    }

    private void doMockConsumeStats() throws Exception {
        ConsumeStats consumeStats = new ConsumeStats();
        consumeStats.setOffsetTable(MapBuilder.builder(
                new MessageQueue(topic, brokerName, 0),
                new OffsetWrapper()
        ).build());
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "");
        response.setBody(consumeStats.encode());
        doMockClusterMapping(response);
    }

    private void doMockConsumerStats() throws Exception {
        GetConsumerStatusBody responseBody = new GetConsumerStatusBody();
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "");
        response.setBody(responseBody.encode());
        doMockClusterMapping(response);
        // 将参数回传
        Broker2Client broker2Client = mock(Broker2Client.class);
        when(broker2Client.getConsumeStatus(anyString(), anyString(), anyString())).thenReturn(response);
        doReturn(broker2Client).when(brokerController).getBroker2Client();
    }

    private void doMockTopicStatInfo() throws Exception {
        TopicStatsTable topicStatsTable = new TopicStatsTable();
        topicStatsTable.setOffsetTable(MapBuilder.builder(
                new MessageQueue(topic, brokerName, 0),
                new TopicOffset()
        ).build());
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "");
        response.setBody(topicStatsTable.encode());
        doMockClusterMapping(response);
    }

    private void doMockGetEarliestMsgStoreTime() throws Exception {
        GetEarliestMsgStoretimeResponseHeader header = new GetEarliestMsgStoretimeResponseHeader();
        header.setTimestamp(1024L);
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "");
        response.writeCustomHeader(header);
        response.makeCustomHeaderToNet();
        doMockClusterMapping(response);
    }

    private void doMockBrokerStats() throws Exception {
        BrokerStatsData brokerStatsData = new BrokerStatsData();
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "");
        response.setBody(brokerStatsData.encode());
        doMockClusterMapping(response);
    }
}
