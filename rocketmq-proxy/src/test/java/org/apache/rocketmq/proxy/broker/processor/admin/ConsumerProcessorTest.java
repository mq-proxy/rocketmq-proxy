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

import com.google.common.collect.Sets;
import io.netty.channel.Channel;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.admin.ConsumeStats;
import org.apache.rocketmq.common.admin.OffsetWrapper;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageId;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.ConsumeQueueData;
import org.apache.rocketmq.common.protocol.body.ConsumeStatsList;
import org.apache.rocketmq.common.protocol.body.GroupList;
import org.apache.rocketmq.common.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.common.protocol.body.LockBatchResponseBody;
import org.apache.rocketmq.common.protocol.body.QueryConsumeQueueResponseBody;
import org.apache.rocketmq.common.protocol.body.QueryConsumeTimeSpanBody;
import org.apache.rocketmq.common.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.common.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.common.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerRunningInfoRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryConsumeQueueRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryConsumeTimeSpanRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryTopicConsumeByWhoRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.proxy.broker.BrokerController;
import org.apache.rocketmq.proxy.broker.client.ClientChannelInfo;
import org.apache.rocketmq.proxy.broker.client.net.Broker2Client;
import org.apache.rocketmq.proxy.broker.processor.AdminProcessTestBase;
import org.apache.rocketmq.proxy.helper.MapBuilder;
import org.apache.rocketmq.proxy.proxy.Proxy;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.rocketmq.proxy.helper.FieldHelper.setField;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ConsumerProcessorTest extends AdminProcessTestBase {
    private final String consumerGroup = "test-consumer-group";
    private final String clientId = "test-client-id";

    @Before
    public void setUp() throws Exception {
        doMockTopicQueueMapping();
        this.processor = new ConsumerProcessor(this.brokerController);
    }

    @Test
    public void processLockBatchMQ() throws Exception {
        try (MockedStatic<Proxy> mockedStatic = mockStatic(Proxy.class)) {
            mockProxy(mockedStatic, proxyName, brokerAddr);
            Set<MessageQueue> brokerMessageQueues = Sets.newHashSet(new MessageQueue(topic, brokerName, 0));
            Set<MessageQueue> proxyMessageQueues = Sets.newHashSet(new MessageQueue(topic, proxyName, 0));
            doMockLockResponse(brokerMessageQueues);
            LockBatchRequestBody requestBody = new LockBatchRequestBody();
            requestBody.setClientId("test-client-id");
            requestBody.setConsumerGroup("test-group");
            requestBody.setMqSet(proxyMessageQueues);
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.LOCK_BATCH_MQ, null);
            request.setBody(requestBody.encode());
            RemotingCommand response = processRequest(request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
            LockBatchResponseBody responseBody = LockBatchResponseBody.decode(response.getBody(), LockBatchResponseBody.class);
            assertThat(responseBody.getLockOKMQSet()).isEqualTo(proxyMessageQueues);
        }
    }

    @Test
    public void processLockBatchMQFail() throws Exception {
        try (MockedStatic<Proxy> mockedStatic = mockStatic(Proxy.class)) {
            mockProxy(mockedStatic, proxyName, brokerAddr);
            Set<MessageQueue> proxyMessageQueues = Sets.newHashSet(new MessageQueue(topic, proxyName, 0));
            doMockFailResponse();
            LockBatchRequestBody requestBody = new LockBatchRequestBody();
            requestBody.setClientId("test-client-id");
            requestBody.setConsumerGroup("test-group");
            requestBody.setMqSet(proxyMessageQueues);
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.LOCK_BATCH_MQ, null);
            request.setBody(requestBody.encode());
            RemotingCommand response = processRequest(request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
        }
    }

    @Test
    public void processUnlockBatchMQ() throws Exception {
        try (MockedStatic<Proxy> mockedStatic = mockStatic(Proxy.class)) {
            mockProxy(mockedStatic, proxyName, brokerAddr);
            Set<MessageQueue> proxyMessageQueues = Sets.newHashSet(new MessageQueue(topic, proxyName, 0));
            doMockUnlockResponse();
            UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
            requestBody.setClientId("test-client-id");
            requestBody.setConsumerGroup("test-group");
            requestBody.setMqSet(proxyMessageQueues);
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UNLOCK_BATCH_MQ, null);
            request.setBody(requestBody.encode());
            RemotingCommand response = processRequest(request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        }
    }

    @Test
    public void processUnlockBatchMQFail() throws Exception {
        try (MockedStatic<Proxy> mockedStatic = mockStatic(Proxy.class)) {
            mockProxy(mockedStatic, proxyName, brokerAddr);
            Set<MessageQueue> proxyMessageQueues = Sets.newHashSet(new MessageQueue(topic, proxyName, 0));
            doMockFailResponse();
            UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
            requestBody.setClientId("test-client-id");
            requestBody.setConsumerGroup("test-group");
            requestBody.setMqSet(proxyMessageQueues);
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UNLOCK_BATCH_MQ, null);
            request.setBody(requestBody.encode());
            RemotingCommand response = processRequest(request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
        }
    }

    @Test
    public void processQueryTopicConsumeByWho() throws Exception {
        try (MockedStatic<Proxy> mockedStatic = mockStatic(Proxy.class)) {
            mockProxy(mockedStatic, proxyName, brokerAddr);
            doMockTopicConsumeByWho(Sets.newHashSet("test-group"));

            QueryTopicConsumeByWhoRequestHeader requestHeader = new QueryTopicConsumeByWhoRequestHeader();
            requestHeader.setTopic(topic);
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_TOPIC_CONSUME_BY_WHO, requestHeader);
            RemotingCommand response = processRequest(request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
            GroupList groupList = GroupList.decode(response.getBody(), GroupList.class);
            assertThat(groupList.getGroupList()).isEqualTo(Sets.newHashSet("test-group"));
        }
    }

    @Test
    public void processQueryConsumeTimeSpan() throws Exception{
        try (MockedStatic<Proxy> mockedStatic = mockStatic(Proxy.class)) {
            mockProxy(mockedStatic, proxyName, brokerAddr);
            doMockQueryConsumeTimeSpanBody(new MessageQueue(topic, brokerName, 0));
            QueryConsumeTimeSpanRequestHeader requestHeader = new QueryConsumeTimeSpanRequestHeader();
            requestHeader.setTopic(topic);
            requestHeader.setGroup(consumerGroup);
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUME_TIME_SPAN, requestHeader);
            RemotingCommand response = processRequest(request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

            QueryConsumeTimeSpanBody spanBody = QueryConsumeTimeSpanBody.decode(response.getBody(), QueryConsumeTimeSpanBody.class);
            assertThat(spanBody.getConsumeTimeSpanSet().get(0).getMessageQueue())
                    .isEqualTo(new MessageQueue(topic, proxyName, 0));
        }
    }

    @Test
    public void processConsumeMessageDirectly() throws Exception{
        try (
                MockedStatic<Proxy> mockedSProxy = mockStatic(Proxy.class);
                MockedStatic<MessageDecoder> mockedMessageDecoder = mockStatic(MessageDecoder.class);
                ) {
            mockProxy(mockedSProxy, proxyName, brokerAddr);
            doMockConsumeMessageDirectly();
            mockedMessageDecoder.when(() -> MessageDecoder.decodeMessageId(anyString())).thenReturn(
                    new MessageId(new InetSocketAddress("127.0.0.1", 10911), 0)
            );
            ConsumeMessageDirectlyResultRequestHeader requestHeader = new ConsumeMessageDirectlyResultRequestHeader();
            requestHeader.setConsumerGroup(consumerGroup);
            requestHeader.setClientId(clientId);
            requestHeader.setBrokerName(proxyName);
            requestHeader.setMsgId("a");
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONSUME_MESSAGE_DIRECTLY, requestHeader);
            request.setBody("hello".getBytes(StandardCharsets.UTF_8));
            RemotingCommand response = processRequest(request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

            request.setBody(null);
            response = processRequest(request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        }
    }

    @Test
    public void processFetchAllConsumeStatsInBroker() throws Exception {
        try (MockedStatic<Proxy> mockedStatic = mockStatic(Proxy.class)) {
            mockProxy(mockedStatic, proxyName, brokerAddr);
            doMockFetchAllConsumeStats();
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CONSUME_STATS, null);
            RemotingCommand response = processRequest(request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
            ConsumeStatsList consumeStats = ConsumeStatsList.decode(response.getBody(), ConsumeStatsList.class);
            assertThat(consumeStats).isNotNull();
            // proxy recompute total, should equal to 100
            assertThat(consumeStats.getTotalDiff()).isEqualTo(100);
            // proxy convert, should not equal to brokerAddr
            assertThat(consumeStats.getBrokerAddr()).isEqualTo(brokerAddr);
            assertThat(consumeStats.getConsumeStatsList().get(0).get(consumerGroup).get(0).getOffsetTable()).containsKey(
                new MessageQueue(topic, proxyName, 0)
            );
        }
    }

    @Test
    public void processQueryConsumeQueue() throws Exception {
        try (MockedStatic<Proxy> mockedStatic = mockStatic(Proxy.class)) {
            mockProxy(mockedStatic, proxyName, brokerAddr);
            doMockQueryConsumeQueue();
            QueryConsumeQueueRequestHeader requestHeader = new QueryConsumeQueueRequestHeader();
            requestHeader.setTopic(topic);
            requestHeader.setQueueId(0);
            requestHeader.setIndex(0);
            requestHeader.setCount(1);
            requestHeader.setConsumerGroup(consumerGroup);
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUME_QUEUE, requestHeader);
            RemotingCommand response = processRequest(request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
            QueryConsumeQueueResponseBody body = QueryConsumeQueueResponseBody.decode(response.getBody(), QueryConsumeQueueResponseBody.class);
            assertThat(body.getMinQueueIndex()).isEqualTo(1);
            assertThat(body.getMaxQueueIndex()).isEqualTo(100);
            assertThat(body.getQueueData()).isNotEmpty();
        }
    }

    @Test
    public void processGetConsumerRunningInfo() throws Exception {
        try (MockedStatic<Proxy> mockedStatic = mockStatic(Proxy.class)) {
            mockProxy(mockedStatic, proxyName, brokerAddr);
            GetConsumerRunningInfoRequestHeader requestHeader = new GetConsumerRunningInfoRequestHeader();
            requestHeader.setClientId(clientId);
            requestHeader.setJstackEnable(true);
            requestHeader.setConsumerGroup(consumerGroup);

            // no channel found
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_RUNNING_INFO, requestHeader);
            RemotingCommand response = processRequest(request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);

            // wrong version
            doMockRegisterClient(MQVersion.Version.V3_1_7.ordinal());
            request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_RUNNING_INFO, requestHeader);
            response = processRequest(request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);

            // mocked
            doMockRegisterClient(MQVersion.CURRENT_VERSION);
            request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_RUNNING_INFO, requestHeader);
            response = processRequest(request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        }
    }

    @Test
    public void processGetConsumerRunningInfoException() throws Exception {
        try (MockedStatic<Proxy> mockedStatic = mockStatic(Proxy.class)) {
            mockProxy(mockedStatic, proxyName, brokerAddr);
            GetConsumerRunningInfoRequestHeader requestHeader = new GetConsumerRunningInfoRequestHeader();
            requestHeader.setClientId(clientId);
            requestHeader.setJstackEnable(true);
            requestHeader.setConsumerGroup(consumerGroup);

            // timeout mocked
            doMockRegisterClient(MQVersion.CURRENT_VERSION, new RemotingTimeoutException("1"));
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_RUNNING_INFO, requestHeader);
            RemotingCommand response = processRequest(request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.CONSUME_MSG_TIMEOUT);

            // error mocked
            doMockRegisterClient(MQVersion.CURRENT_VERSION, new RemotingSendRequestException("2"));
            request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_RUNNING_INFO, requestHeader);
            response = processRequest(request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
        }
    }

    private void doMockLockResponse(Set<MessageQueue> mq) throws Exception {
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        LockBatchResponseBody body = new LockBatchResponseBody();
        if (mq != null) {
            body.setLockOKMQSet(mq);
        }
        response.setBody(body.encode());
        doMockClusterMapping(response);
    }

    private void doMockUnlockResponse() throws Exception {
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        doMockClusterMapping(response);
    }

    private void doMockFailResponse() throws Exception {
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, null);
        doMockClusterMapping(response);
    }

    private void doMockTopicConsumeByWho(HashSet<String> groups) throws Exception {
        GroupList groupList = new GroupList();
        groupList.setGroupList(groups);
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        response.setBody(groupList.encode());
        doMockClusterMapping(response);
    }

    private void doMockQueryConsumeTimeSpanBody(MessageQueue mq) throws Exception {
        QueryConsumeTimeSpanBody spanBody = new QueryConsumeTimeSpanBody();
        QueueTimeSpan queueTimeSpan = new QueueTimeSpan();
        queueTimeSpan.setMessageQueue(mq);
        spanBody.setConsumeTimeSpanSet(Collections.singletonList(queueTimeSpan));

        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        response.setBody(spanBody.encode());
        doMockClusterMapping(response);
    }

    private void doMockConsumeMessageDirectly() throws Exception {
        doMockRegisterClient(MQVersion.CURRENT_VERSION);
        doMockClusterMapping(RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null));
    }

    private void doMockFetchAllConsumeStats() throws Exception {
        ConsumeStats consumeStats = new ConsumeStats();
        OffsetWrapper offsetWrapper = new OffsetWrapper();
        offsetWrapper.setConsumerOffset(10);
        offsetWrapper.setBrokerOffset(20);
        offsetWrapper.setLastTimestamp(100);
        consumeStats.setConsumeTps(2);
        consumeStats.setOffsetTable(MapBuilder.builder(new MessageQueue(topic, brokerName, 0), offsetWrapper).build());
        ConsumeStatsList statsList = new ConsumeStatsList();
        statsList.setTotalDiff(100);
        statsList.setBrokerAddr(brokerAddr);
        statsList.setConsumeStatsList(Collections.singletonList(
                MapBuilder.builder(consumerGroup, Collections.singletonList(
                        consumeStats
                )).build()
        ));

        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        response.setBody(statsList.encode());
        doMockClusterMapping(response);
    }

    private void doMockQueryConsumeQueue() throws Exception {
        QueryConsumeQueueResponseBody body = new QueryConsumeQueueResponseBody();
        body.setFilterData("filterData");
        body.setMinQueueIndex(1);
        body.setMaxQueueIndex(100);
        body.setQueueData(Collections.singletonList(
                new ConsumeQueueData()
        ));
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        response.setBody(body.encode());
        doMockClusterMapping(response);
    }

    private void doMockRegisterClient(int version) throws Exception {
        doMockRegisterClient(version, null);
    }

    private void doMockRegisterClient(int version, Exception e) throws Exception {
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        Channel channel = mock(Channel.class);
        when(channel.remoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 12345));
        ClientChannelInfo clientChannelInfo = new ClientChannelInfo(
                channel, clientId, LanguageCode.JAVA, version
        );

        // spy remoting server
        RemotingServer remotingServer = spy(brokerController.getRemotingServer());
        if (e != null) {
            doThrow(e).when(remotingServer).invokeSync(any(), any(), anyLong());
        } else {
            doReturn(response).when(remotingServer).invokeSync(any(), any(), anyLong());
        }

        // 写入 spied remoting server
        setField(BrokerController.class, "remotingServer", this.brokerController, remotingServer);
        // 用到 broker2Client, 将spied-brokerController 写入
        setField(Broker2Client.class, "brokerController",
                this.brokerController.getBroker2Client(), brokerController);

        try {
            // unregister all, maybe null pointer
            brokerController.getConsumerManager().getConsumerGroupInfo(consumerGroup).getChannelInfoTable().forEach(
                    (c, info) ->
                            brokerController.getConsumerManager().getConsumerGroupInfo(consumerGroup).unregisterChannel(info)
            );
        } catch (Throwable t) {
        }

        brokerController.getConsumerManager().registerConsumer(
                consumerGroup,
                clientChannelInfo,
                ConsumeType.CONSUME_PASSIVELY,
                MessageModel.CLUSTERING,
                ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET,
                Sets.newHashSet(),
                true);
    }
}
