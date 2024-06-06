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

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.proxy.broker.BrokerController;
import org.apache.rocketmq.proxy.broker.client.ClientChannelInfo;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.CheckClientRequestBody;
import org.apache.rocketmq.common.protocol.header.UnregisterClientRequestHeader;
import org.apache.rocketmq.common.protocol.header.UnregisterClientResponseHeader;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.common.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.common.protocol.heartbeat.ProducerData;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;
import org.apache.rocketmq.filter.FilterFactory;
import org.apache.rocketmq.proxy.common.protocol.ProxyRequestCode;
import org.apache.rocketmq.proxy.controller.ProxyController;
import org.apache.rocketmq.proxy.proxy.processor.ProxyProcessor;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.Set;

public class ClientManageProcessor extends AbstractProxyProcessor {

    private final ProxyController proxyController;

    public ClientManageProcessor(final BrokerController brokerController, final ProxyController proxyController) {
        super(brokerController);
        this.proxyController = proxyController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        switch (request.getCode()) {
            case RequestCode.HEART_BEAT:
                return this.heartBeat(ctx, request);
            case RequestCode.UNREGISTER_CLIENT:
                return this.unregisterClient(ctx, request);
            case RequestCode.CHECK_CLIENT_CONFIG:
                return this.checkClientConfig(ctx, request);
            default:
                return this.getUnknownCmdResponse(ctx, request);
        }
    }

    /**
     * proxy heartbeat from (producer/consumer)
     *
     * @param ctx
     * @param request
     * @return
     */
    public RemotingCommand heartBeat(ChannelHandlerContext ctx, RemotingCommand request) {
        int opaque = request.getOpaque();
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "");
        response.setOpaque(opaque);

        HeartbeatData heartbeatData = HeartbeatData.decode(request.getBody(), HeartbeatData.class);
        ClientChannelInfo clientChannelInfo = new ClientChannelInfo(
                ctx.channel(),
                heartbeatData.getClientID(),
                request.getLanguage(),
                request.getVersion()
        );

        //1.unregister the channel and info registered by client itself before first time sending proxied heartbeat to broker
        unregisterBeforeFirstHeartBeat(ctx, heartbeatData);

        //2.register ConsumerData in proxy
        for (ConsumerData data : heartbeatData.getConsumerDataSet()) {
            SubscriptionGroupConfig subscriptionGroupConfig =
                    this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(
                            data.getGroupName());
            boolean isNotifyConsumerIdsChangedEnable = true;
            if (null != subscriptionGroupConfig) {
                isNotifyConsumerIdsChangedEnable = subscriptionGroupConfig.isNotifyConsumerIdsChangedEnable();
                int topicSysFlag = 0;
                if (data.isUnitMode()) {
                    topicSysFlag = TopicSysFlag.buildSysFlag(false, true);
                }
                String newTopic = MixAll.getRetryTopic(data.getGroupName());
                this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(
                        newTopic,
                        subscriptionGroupConfig.getRetryQueueNums(),
                        PermName.PERM_WRITE | PermName.PERM_READ, topicSysFlag);
            }

            //maintain the consumer state in the proxy, including updating the timestamp of channel info
            boolean changed = this.brokerController.getConsumerManager().registerConsumer(
                    data.getGroupName(),
                    clientChannelInfo,
                    data.getConsumeType(),
                    data.getMessageModel(),
                    data.getConsumeFromWhere(),
                    data.getSubscriptionDataSet(),
                    isNotifyConsumerIdsChangedEnable
            );

            if (changed) {
                log.info("registerConsumer info changed {} {}",
                        data.toString(),
                        RemotingHelper.parseChannelRemoteAddr(ctx.channel())
                );
            }
        }

        RemotingCommand newRequest = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);
        newRequest.setBody(request.getBody());

        //3.register ProducerData in proxy
        for (ProducerData data : heartbeatData.getProducerDataSet()) {
            this.brokerController.getProducerManager().registerProducer(data.getGroupName(),
                    clientChannelInfo);
        }

        //4.proxy heartbeat to broker
        response = sendHeartBeat2Brokers(newRequest, ctx.channel(), true);

        return response;
    }

    /**
     * If it is the first time the clientId sending a heartbeat to the proxy, and the switch has just been toggled to proxy
     * it is necessary to unregister the channel registered by client itself
     * Here, it needs to check:
     * Whether it is the first heartbeat after the switch
     *
     * @param ctx
     * @param heartbeatData
     */
    private void unregisterBeforeFirstHeartBeat(ChannelHandlerContext ctx, HeartbeatData heartbeatData) {
        if (!ProxyProcessor.isProxy) {
            return;
        }
        //1.Check if it is the first time the current client register on this proxy
        boolean unregister = proxyController.getHeartBeatManager().checkIfUnregisterInBroker(heartbeatData.getClientID());

        Set<ProducerData> producerSet = heartbeatData.getProducerDataSet();
        Set<ConsumerData> consumerSet = heartbeatData.getConsumerDataSet();

        String producerGroup = null != producerSet && !producerSet.isEmpty() ? producerSet.stream().findFirst().get().getGroupName() : null;
        String consumerGroup = null != consumerSet && !consumerSet.isEmpty() ? consumerSet.stream().findFirst().get().getGroupName() : null;

        if (unregister) {
            if (consumerGroup != null) {
                UnregisterClientRequestHeader requestHeader = new UnregisterClientRequestHeader();
                requestHeader.setClientID(heartbeatData.getClientID());
                requestHeader.setConsumerGroup(consumerGroup);
                //clean consumerData and channel registered by client, using clientId
                log.info("unregister consumer by clientId: {}, addr: {}, channel: {}", heartbeatData.getClientID(), ctx.channel().remoteAddress().toString(), ctx.channel());
                RemotingCommand unregisterReq = RemotingCommand.createRequestCommand(ProxyRequestCode.UNREGISTER_CLIENT_BY_CLIENTID, requestHeader);
                sendHeartBeat2Brokers(unregisterReq, ctx.channel(), true);
            }
        }
    }

    public RemotingCommand unregisterClient(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response =
                RemotingCommand.createResponseCommand(UnregisterClientResponseHeader.class);

        final UnregisterClientRequestHeader requestHeader =
                (UnregisterClientRequestHeader) request
                        .decodeCommandCustomHeader(UnregisterClientRequestHeader.class);

        ClientChannelInfo clientChannelInfo = new ClientChannelInfo(
                ctx.channel(),
                requestHeader.getClientID(),
                request.getLanguage(),
                request.getVersion());

        {
            final String group = requestHeader.getProducerGroup();
            if (group != null) {
                this.brokerController.getProducerManager().unregisterProducer(group, clientChannelInfo);
                log.info("unregister producer by channel: {}, clientId is: {}", ctx.channel(), requestHeader.getClientID());
                RemotingCommand res = unregisterFromBrokers(group, clientChannelInfo, false);
                response.setCode(res.getCode());
            }
        }

        {
            final String group = requestHeader.getConsumerGroup();
            if (group != null) {
                SubscriptionGroupConfig subscriptionGroupConfig =
                        this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(group);
                boolean isNotifyConsumerIdsChangedEnable = true;
                if (null != subscriptionGroupConfig) {
                    isNotifyConsumerIdsChangedEnable = subscriptionGroupConfig.isNotifyConsumerIdsChangedEnable();
                }
                //1.unregister ConsumerData and channel from broker firstly
                log.info("unregister consumer by channel: {}, clientId is: {}", ctx.channel(), requestHeader.getClientID());
                RemotingCommand res = unregisterFromBrokers(group, clientChannelInfo, true);
                response.setCode(response.getCode() & res.getCode());

                //2.then notify client to rebalance
                this.brokerController.getConsumerManager().unregisterConsumer(group, clientChannelInfo, isNotifyConsumerIdsChangedEnable);
            }
        }

        response.setRemark(null);
        return response;
    }

    /**
     * proxy unregister request to broker
     *
     * @param group
     * @param info
     * @param isConsumer
     * @return
     */
    public RemotingCommand unregisterFromBrokers(String group, ClientChannelInfo info, boolean isConsumer) {
        UnregisterClientRequestHeader sendHeader = new UnregisterClientRequestHeader();
        sendHeader.setClientID(info.getClientId());
        RemotingCommand req;
        RemotingCommand res;
        if (isConsumer) {
            sendHeader.setConsumerGroup(group);
            req = RemotingCommand.createRequestCommand(RequestCode.UNREGISTER_CLIENT, sendHeader);
            res = sendHeartBeat2Brokers(req, info.getChannel(), true);
        } else {
            sendHeader.setProducerGroup(group);
            req = RemotingCommand.createRequestCommand(RequestCode.UNREGISTER_CLIENT, sendHeader);
            res = sendHeartBeat2AllMasters(info.getChannel(), req, true);
        }

        return res;
    }

    public RemotingCommand checkClientConfig(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        CheckClientRequestBody requestBody = CheckClientRequestBody.decode(request.getBody(),
                CheckClientRequestBody.class);

        if (requestBody != null && requestBody.getSubscriptionData() != null) {
            SubscriptionData subscriptionData = requestBody.getSubscriptionData();

            if (ExpressionType.isTagType(subscriptionData.getExpressionType())) {
                response.setCode(ResponseCode.SUCCESS);
                response.setRemark(null);
                return response;
            }

            if (!this.brokerController.getBrokerConfig().isEnablePropertyFilter()) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("The broker does not support consumer to filter message by " + subscriptionData.getExpressionType());
                return response;
            }

            try {
                FilterFactory.INSTANCE.get(subscriptionData.getExpressionType()).compile(subscriptionData.getSubString());
            } catch (Exception e) {
                log.warn("Client {}@{} filter message, but failed to compile expression! sub={}, error={}",
                        requestBody.getClientId(), requestBody.getGroup(), requestBody.getSubscriptionData(), e.getMessage());
                response.setCode(ResponseCode.SUBSCRIPTION_PARSE_FAILED);
                response.setRemark(e.getMessage());
                return response;
            }
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }
}
