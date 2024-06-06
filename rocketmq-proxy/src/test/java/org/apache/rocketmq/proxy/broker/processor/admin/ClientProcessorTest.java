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
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.ConsumerConnection;
import org.apache.rocketmq.common.protocol.body.ProducerConnection;
import org.apache.rocketmq.common.protocol.body.ProducerInfo;
import org.apache.rocketmq.common.protocol.body.ProducerTableInfo;
import org.apache.rocketmq.common.protocol.header.GetConsumerConnectionListRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetProducerConnectionListRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.proxy.broker.client.ClientChannelInfo;
import org.apache.rocketmq.proxy.broker.processor.AdminProcessTestBase;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ClientProcessorTest extends AdminProcessTestBase {
    @Before
    public void setUp() {
        processor = new ClientProcessor(brokerController);
        Channel mockChannel = mock(Channel.class);
        when(mockChannel.remoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 8080));
        ClientChannelInfo clientChannelInfo = mock(ClientChannelInfo.class);
        when(clientChannelInfo.getChannel()).thenReturn(mockChannel);
        when(clientChannelInfo.getClientId()).thenReturn("test-client-id");

        Channel mockChannel2 = mock(Channel.class);
        when(mockChannel2.remoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 8081));
        ClientChannelInfo clientChannelInfo2 = mock(ClientChannelInfo.class);
        when(clientChannelInfo2.getChannel()).thenReturn(mockChannel2);
        when(clientChannelInfo2.getClientId()).thenReturn("test-client-id-2");

        brokerController.getConsumerManager().registerConsumer(
                "test",
                clientChannelInfo,
                ConsumeType.CONSUME_PASSIVELY,
                MessageModel.CLUSTERING,
                ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET,
                new HashSet<>(),
                true
                );

        brokerController.getProducerManager().registerProducer("test", clientChannelInfo);
        brokerController.getProducerManager().registerProducer("test-3", clientChannelInfo);
        brokerController.getProducerManager().registerProducer("test-3", clientChannelInfo2);
    }

    @Test
    public void processResponseGetConsumerConnectionList() throws Exception {
        GetConsumerConnectionListRequestHeader requestHeader = new GetConsumerConnectionListRequestHeader();
        requestHeader.setConsumerGroup("test");
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_CONNECTION_LIST, requestHeader);
        RemotingCommand response = processRequest(null, request);
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        ConsumerConnection result = ConsumerConnection.decode(response.getBody(), ConsumerConnection.class);
        assertThat(result).isNotNull();
        assertThat(result.getConnectionSet().size()).isEqualTo(1);
        assertThat(result.getConnectionSet().iterator().next().getClientId()).isEqualTo("test-client-id");

        requestHeader.setConsumerGroup("test2");
        request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_CONNECTION_LIST, requestHeader);
        response = processRequest(null, request);
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(206);
    }

    @Test
    public void processResponseGetProducerConnectionList() throws Exception {
        GetProducerConnectionListRequestHeader requestHeader = new GetProducerConnectionListRequestHeader();
        requestHeader.setProducerGroup("test");
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_PRODUCER_CONNECTION_LIST, requestHeader);
        RemotingCommand response = processRequest(null, request);
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        ProducerConnection result = ProducerConnection.decode(response.getBody(), ProducerConnection.class);
        assertThat(result).isNotNull();
        assertThat(result.getConnectionSet().size()).isEqualTo(1);
        assertThat(result.getConnectionSet().iterator().next().getClientId()).isEqualTo("test-client-id");


        requestHeader.setProducerGroup("test2");
        request = RemotingCommand.createRequestCommand(RequestCode.GET_PRODUCER_CONNECTION_LIST, requestHeader);
        response = processRequest(null, request);
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(1);
    }

    @Test
    public void processResponseGetAllProducerInfo() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_PRODUCER_INFO, null);
        RemotingCommand response = processRequest(null, request);
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        ProducerTableInfo result = ProducerTableInfo.decode(response.getBody(), ProducerTableInfo.class);
        assertThat(result).isNotNull();
        assertThat(result.getData().size()).isEqualTo(2);
        assertThat(result.getData().get("test").get(0).getClientId()).isEqualTo("test-client-id");
        assertThat(result.getData().get("test-3").stream().map(ProducerInfo::getClientId).collect(Collectors.toSet()))
                .isEqualTo(Sets.newHashSet("test-client-id", "test-client-id-2"));
    }
}
