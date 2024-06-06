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
import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.Connection;
import org.apache.rocketmq.common.protocol.body.ConsumerConnection;
import org.apache.rocketmq.common.protocol.body.ProducerConnection;
import org.apache.rocketmq.common.protocol.body.ProducerTableInfo;
import org.apache.rocketmq.common.protocol.header.GetConsumerConnectionListRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetProducerConnectionListRequestHeader;
import org.apache.rocketmq.proxy.broker.BrokerController;
import org.apache.rocketmq.proxy.broker.client.ClientChannelInfo;
import org.apache.rocketmq.proxy.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.proxy.broker.processor.AbstractProxyProcessor;
import org.apache.rocketmq.proxy.broker.processor.DefaultRequestProcessor;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ClientProcessor extends AbstractProxyProcessor implements DefaultRequestProcessor {
    public ClientProcessor(BrokerController brokerController) {
        super(brokerController);
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        switch (request.getCode()) {
            case RequestCode.GET_CONSUMER_CONNECTION_LIST:
                return getConsumerConnectionList(ctx, request);
            case RequestCode.GET_PRODUCER_CONNECTION_LIST:
                return getProducerConnectionList(ctx, request);
            case RequestCode.GET_ALL_PRODUCER_INFO:
                return getAllProducerInfo(ctx, request);
            default:
                return getUnknownCmdResponse(ctx, request);
        }
    }

    private RemotingCommand getConsumerConnectionList(ChannelHandlerContext ignore,
                                                      RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetConsumerConnectionListRequestHeader requestHeader =
                (GetConsumerConnectionListRequestHeader) request.decodeCommandCustomHeader(GetConsumerConnectionListRequestHeader.class);

        ConsumerGroupInfo consumerGroupInfo =
                this.brokerController.getConsumerManager().getConsumerGroupInfo(requestHeader.getConsumerGroup());
        if (consumerGroupInfo != null) {
            ConsumerConnection bodyData = new ConsumerConnection();
            bodyData.setConsumeFromWhere(consumerGroupInfo.getConsumeFromWhere());
            bodyData.setConsumeType(consumerGroupInfo.getConsumeType());
            bodyData.setMessageModel(consumerGroupInfo.getMessageModel());
            bodyData.getSubscriptionTable().putAll(consumerGroupInfo.getSubscriptionTable());
            bodyData.setConnectionSet(parseConnection(consumerGroupInfo.getChannelInfoTable()));

            byte[] body = bodyData.encode();
            response.setBody(body);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);

            return response;
        }

        response.setCode(ResponseCode.CONSUMER_NOT_ONLINE);
        response.setRemark("the consumer group[" + requestHeader.getConsumerGroup() + "] not online");
        return response;
    }

    private RemotingCommand getAllProducerInfo(ChannelHandlerContext ignore, RemotingCommand ignore0) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        ProducerTableInfo producerTable = this.brokerController.getProducerManager().getProducerTable();
        if (producerTable != null) {
            byte[] body = producerTable.encode();
            response.setBody(body);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }

        response.setCode(ResponseCode.SYSTEM_ERROR);
        return response;
    }

    private RemotingCommand getProducerConnectionList(ChannelHandlerContext ignore,
                                                      RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetProducerConnectionListRequestHeader requestHeader =
                (GetProducerConnectionListRequestHeader) request.decodeCommandCustomHeader(GetProducerConnectionListRequestHeader.class);

        ProducerConnection bodydata = new ProducerConnection();
        Map<Channel, ClientChannelInfo> channelInfoHashMap =
                this.brokerController.getProducerManager().getGroupChannelTable().get(requestHeader.getProducerGroup());

        if (channelInfoHashMap != null) {
            bodydata.setConnectionSet(parseConnection(channelInfoHashMap));
            byte[] body = bodydata.encode();
            response.setBody(body);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }

        response.setCode(ResponseCode.SYSTEM_ERROR);
        response.setRemark("the producer group[" + requestHeader.getProducerGroup() + "] not exist");
        return response;
    }

    private HashSet<Connection> parseConnection(Map<Channel, ClientChannelInfo> clientChannelInfoMap) {
        if (clientChannelInfoMap == null || clientChannelInfoMap.isEmpty()) {
            return Sets.newHashSet();
        }
        return clientChannelInfoMap.values().stream().map(info -> {
            Connection connection = new Connection();
            connection.setClientId(info.getClientId());
            connection.setLanguage(info.getLanguage());
            connection.setVersion(info.getVersion());
            connection.setClientAddr(RemotingHelper.parseChannelRemoteAddr(info.getChannel()));
            return connection;
        }).collect(HashSet::new, Set::add, Set::addAll);
    }
}
