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

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerRunningInfoRequestHeader;
import org.apache.rocketmq.proxy.broker.BrokerController;
import org.apache.rocketmq.proxy.broker.client.ClientChannelInfo;
import org.apache.rocketmq.proxy.broker.processor.DefaultRequestProcessor;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.net.UnknownHostException;

public class ConsumerProcessor extends AbstractAdminProcessor implements DefaultRequestProcessor {
    public ConsumerProcessor(BrokerController brokerController) {
        super(brokerController);
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        switch (request.getCode()) {
            case RequestCode.LOCK_BATCH_MQ:
                return lockBatchMQ(ctx, request);
            case RequestCode.UNLOCK_BATCH_MQ:
                return unlockBatchMQ(ctx, request);
            case RequestCode.QUERY_TOPIC_CONSUME_BY_WHO:
                return queryTopicConsumeByWho(ctx, request);
            case RequestCode.QUERY_CONSUME_TIME_SPAN:
                return queryConsumeTimeSpan(ctx, request);
            case RequestCode.CONSUME_MESSAGE_DIRECTLY:
                return consumeMessageDirectly(ctx, request);
            case RequestCode.GET_BROKER_CONSUME_STATS:
                return fetchAllConsumeStatsInBroker(ctx, request);
            case RequestCode.QUERY_CONSUME_QUEUE:
                return queryConsumeQueue(ctx, request);
            case RequestCode.GET_CONSUMER_RUNNING_INFO:
                return getConsumerRunningInfo(ctx, request);
            default:
                return getUnknownCmdResponse(ctx, request);
        }
    }

    private RemotingCommand lockBatchMQ(ChannelHandlerContext ctx,
                                        RemotingCommand request) {
        return doProxy(ctx, request);
    }

    private RemotingCommand unlockBatchMQ(ChannelHandlerContext ctx,
                                          RemotingCommand request) {
        return doProxy(ctx, request);
    }

    private RemotingCommand queryTopicConsumeByWho(ChannelHandlerContext ctx,
                                                   RemotingCommand request) throws RemotingCommandException {
        return doProxy(ctx, request, true);
    }

    private RemotingCommand queryConsumeTimeSpan(ChannelHandlerContext ctx,
                                                 RemotingCommand request) {
        return doProxy(ctx, request, true);
    }

    // broker & client consume message
    private RemotingCommand consumeMessageDirectly(ChannelHandlerContext ctx,
                                                   RemotingCommand request) throws RemotingCommandException {

        final ConsumeMessageDirectlyResultRequestHeader requestHeader = (ConsumeMessageDirectlyResultRequestHeader) request
                .decodeCommandCustomHeader(ConsumeMessageDirectlyResultRequestHeader.class);

        request.getExtFields().put("brokerName", this.brokerController.getBrokerConfig().getBrokerName());
        try {
            // if message body not set, query from broker
            if (request.getBody() == null || request.getBody().length == 0) {
                RemotingCommand brokerResponse = viewMessageOnBroker(ctx, requestHeader.getMsgId());
                if (brokerResponse != null && brokerResponse.getCode() == ResponseCode.SUCCESS) {
                    request.setBody(brokerResponse.getBody());
                }
            }
        } catch (UnknownHostException e) {
            return RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, e.getMessage());
        }

        return this.callConsumer(RequestCode.CONSUME_MESSAGE_DIRECTLY, request, requestHeader.getConsumerGroup(),
                requestHeader.getClientId());
    }

    private RemotingCommand fetchAllConsumeStatsInBroker(ChannelHandlerContext ctx, RemotingCommand request) {
        return doProxy(ctx, request);
    }

    private RemotingCommand queryConsumeQueue(ChannelHandlerContext ctx,
                                              RemotingCommand request) {
        return doProxy(ctx, request);
    }

    private RemotingCommand getConsumerRunningInfo(ChannelHandlerContext ignore,
                                                   RemotingCommand request) throws RemotingCommandException {
        final GetConsumerRunningInfoRequestHeader requestHeader =
                (GetConsumerRunningInfoRequestHeader) request.decodeCommandCustomHeader(GetConsumerRunningInfoRequestHeader.class);

        return this.callConsumer(RequestCode.GET_CONSUMER_RUNNING_INFO, request, requestHeader.getConsumerGroup(),
                requestHeader.getClientId());
    }

    private RemotingCommand callConsumer(
            final int requestCode,
            final RemotingCommand request,
            final String consumerGroup,
            final String clientId) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        ClientChannelInfo clientChannelInfo = this.brokerController.getConsumerManager().findChannel(consumerGroup, clientId);

        if (null == clientChannelInfo) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("The Consumer <%s> <%s> not online", consumerGroup, clientId));
            return response;
        }

        if (clientChannelInfo.getVersion() < MQVersion.Version.V3_1_8_SNAPSHOT.ordinal()) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("The Consumer <%s> Version <%s> too low to finish, please upgrade it to V3_1_8_SNAPSHOT",
                    clientId,
                    MQVersion.getVersionDesc(clientChannelInfo.getVersion())));
            return response;
        }

        try {
            RemotingCommand newRequest = RemotingCommand.createRequestCommand(requestCode, null);
            newRequest.setExtFields(request.getExtFields());
            newRequest.setBody(request.getBody());

            return this.brokerController.getBroker2Client().callClient(clientChannelInfo.getChannel(), newRequest);
        } catch (RemotingTimeoutException e) {
            response.setCode(ResponseCode.CONSUME_MSG_TIMEOUT);
            response
                    .setRemark(String.format("consumer <%s> <%s> Timeout: %s", consumerGroup, clientId, RemotingHelper.exceptionSimpleDesc(e)));
            return response;
        } catch (Exception e) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(
                    String.format("invoke consumer <%s> <%s> Exception: %s", consumerGroup, clientId, RemotingHelper.exceptionSimpleDesc(e)));
            return response;
        }
    }
}
