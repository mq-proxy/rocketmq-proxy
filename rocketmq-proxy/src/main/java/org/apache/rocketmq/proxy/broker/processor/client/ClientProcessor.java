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
package org.apache.rocketmq.proxy.broker.processor.client;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.common.protocol.header.NotifyConsumerIdsChangedRequestHeader;
import org.apache.rocketmq.common.protocol.header.ResetOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.UnregisterClientRequestHeader;
import org.apache.rocketmq.proxy.broker.BrokerController;
import org.apache.rocketmq.proxy.broker.client.ClientChannelInfo;
import org.apache.rocketmq.proxy.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.proxy.broker.netty.ProxyRemotingClient;
import org.apache.rocketmq.proxy.broker.processor.AbstractReplyProcessor;
import org.apache.rocketmq.proxy.broker.processor.DefaultRequestProcessor;
import org.apache.rocketmq.proxy.proxy.processor.ProxyProcessor;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class ClientProcessor extends AbstractReplyProcessor implements DefaultRequestProcessor {

    public ClientProcessor(BrokerController brokerController) {
        super(brokerController);
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        switch (request.getCode()) {
            case RequestCode.CHECK_TRANSACTION_STATE:
                return checkTransactionStat(ctx, request);
            case RequestCode.NOTIFY_CONSUMER_IDS_CHANGED:
                return notifyConsumerIdsChanged(ctx, request);
            case RequestCode.RESET_CONSUMER_CLIENT_OFFSET:
                return resetConsumerClientOffset(ctx, request);
            case RequestCode.GET_CONSUMER_STATUS_FROM_CLIENT:
                return getConsumerStatusFromClient(ctx, request);
            case RequestCode.GET_CONSUMER_RUNNING_INFO:
                return getConsumerRunningInfo(ctx, request);
            case RequestCode.CONSUME_MESSAGE_DIRECTLY:
                return consumeMessageDirectly(ctx, request);
            case RequestCode.PUSH_REPLY_MESSAGE_TO_CLIENT:
                return this.replyMessage(ctx, request); // proxy it to broker
            default:
                return getUnknownCmdResponse(ctx, request);
        }
    }

    /**
     * need to unregister the channel created by proxy when switch from proxy back to broker
     * to avoid message delay
     *
     * @param ctx
     * @param request
     * @return
     */
    private RemotingCommand notifyConsumerIdsChanged(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {

        boolean isProxy = ProxyProcessor.isProxy;

        //1.ctx is broker -> proxy
        String addr = ctx.channel().remoteAddress().toString().replace("/", "");

        NotifyConsumerIdsChangedRequestHeader inputHeader =
                (NotifyConsumerIdsChangedRequestHeader) request.decodeCommandCustomHeader(NotifyConsumerIdsChangedRequestHeader.class);

        String consumerGroup = inputHeader.getConsumerGroup();
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "");
        if (null == consumerGroup) {
            return response;
        }

        List<String> clientIds = brokerController.getProxyController().getHeartBeatProcessor().queryClientIdByGroup(consumerGroup);
        Map<String, AtomicInteger> countMap = new HashMap<>(clientIds.size());
        clientIds.forEach(ele -> {
            countMap.computeIfAbsent(ele, k -> new AtomicInteger(0)).getAndIncrement();
        });

        ConsumerGroupInfo consumerGroupInfo = brokerController.getConsumerManager().getConsumerGroupInfo(consumerGroup);
        if (consumerGroupInfo == null) {
            return response;
        }

        ConcurrentMap<Channel, ClientChannelInfo> table = brokerController.getConsumerManager().getConsumerGroupInfo(consumerGroup).getChannelInfoTable();
        if (table == null || table.isEmpty()) {
            return response;
        }
        Iterator<Map.Entry<Channel, ClientChannelInfo>> it = table.entrySet().iterator();

        Channel clientChannel = null;
        List<ClientChannelInfo> channelList = new ArrayList<>();
        while (it.hasNext()) {
            Map.Entry<Channel, ClientChannelInfo> entry = it.next();
            String clientId = entry.getValue().getClientId();
            //If there are duplicate clientId in list retrieved from broker,
            // it indicates that there are both channels proxied by proxy and registered by client itself after switching back to broker
            //In this case, the proxy needs to actively unregister the clientId it is proxying
            if (countMap.containsKey(clientId) && countMap.get(clientId).get() > 1) {
                //only execute when switch from proxy to broker
                if (!isProxy) {
                    clientChannel = entry.getKey();
                    UnregisterClientRequestHeader sendHeader = new UnregisterClientRequestHeader();
                    sendHeader.setClientID(clientId);
                    sendHeader.setConsumerGroup(consumerGroup);
                    RemotingCommand req = RemotingCommand.createRequestCommand(RequestCode.UNREGISTER_CLIENT, sendHeader);
                    //key in this channel map is the reference of client2Proxy channel
                    RemotingCommand res = sendHeartBeat2Brokers(req, clientChannel, true);
                    response.setCode(res.getCode() & response.getCode());

                    ClientChannelInfo clientChannelInfo = new ClientChannelInfo(
                            entry.getKey(),
                            clientId,
                            request.getLanguage(),
                            request.getVersion());
                    channelList.add(clientChannelInfo);
                }
            }
        }

        response.setRemark("");
        return response;
    }

    private RemotingCommand checkTransactionStat(ChannelHandlerContext ignore, RemotingCommand request) throws RemotingCommandException {
        final CheckTransactionStateRequestHeader requestHeader =
                (CheckTransactionStateRequestHeader) request.decodeCommandCustomHeader(CheckTransactionStateRequestHeader.class);
        final ByteBuffer byteBuffer = ByteBuffer.wrap(request.getBody());
        final MessageExt messageExt = MessageDecoder.decode(byteBuffer);
        if (messageExt != null) {
            final String group = messageExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
            if (group != null) {
                Channel channel = brokerController.getProducerManager().getAvailableChannel(group);
                try {
                    if (channel != null) {
                        brokerController.getBroker2Client().checkProducerTransactionState(group, channel, requestHeader, messageExt);
                    }
                } catch (Exception e) {
                    log.warn("check producer transaction state error, group={}, channel={}", group, channel);
                    throw new RemotingCommandException("check producer transaction state failed", e);
                }
            } else {
                log.warn("checkTransactionState, pick producer group failed");
            }
        } else {
            log.warn("checkTransactionState, decode message failed");
        }

        // oneway request, no response
        return null;
    }

    // notify all consumers
    private RemotingCommand resetConsumerClientOffset(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final ResetOffsetRequestHeader requestHeader =
                (ResetOffsetRequestHeader) request.decodeCommandCustomHeader(ResetOffsetRequestHeader.class);
        ConsumerGroupInfo groupInfo = brokerController.getConsumerManager().getConsumerGroupInfo(requestHeader.getGroup());
        if (groupInfo == null) {
            return RemotingCommand.createResponseCommand(ResponseCode.CONSUMER_NOT_ONLINE, "consumer not online");
        }
        groupInfo.getAllChannel().forEach(channel -> {
            try {
                this.brokerController.getRemotingServer().invokeOneway(channel, request, 10000L);
            } catch (RemotingSendRequestException | RemotingTimeoutException | InterruptedException |
                     RemotingTooMuchRequestException e) {
                throw new RuntimeException(e);
            }
        });
        return RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "");
    }

    private RemotingCommand getConsumerStatusFromClient(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        Channel clientChannel = getHeartBeatClient(ctx.channel().remoteAddress().toString().replace("/", ""))
                .getClientChannelByProxyChannel(ctx.channel());
        try {
            return brokerController.getRemotingServer().invokeSync(clientChannel, request, 5000L);
        } catch (InterruptedException | RemotingSendRequestException | RemotingTimeoutException e) {
            throw new RemotingCommandException("get consumer status failed: " + e.getMessage());
        }
    }

    //proxy to consumer
    private RemotingCommand getConsumerRunningInfo(ChannelHandlerContext ignore, RemotingCommand request) {
        return redirectToConsumer(request);
    }

    //proxy to consumer
    private RemotingCommand consumeMessageDirectly(ChannelHandlerContext ignore, RemotingCommand request) {
        return redirectToConsumer(request);
    }

    private RemotingCommand redirectToConsumer(RemotingCommand request) {
        if (request == null || request.getExtFields() == null) {
            return RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "request is null");
        }
        return redirectToConsumer(request,
                request.getExtFields().get("consumerGroup"),
                request.getExtFields().get("clientId"));
    }

    private RemotingCommand redirectToConsumer(RemotingCommand request, String group, String clientId) {
        ClientChannelInfo channel = brokerController.getConsumerManager().findChannel(group, clientId);
        if (channel != null && channel.getChannel() != null) {
            try {
                return brokerController.getBroker2Client().callClient(channel.getChannel(), request);
            } catch (RemotingSendRequestException | RemotingTimeoutException | InterruptedException e) {
                return RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR,
                        "redirect to consumer failed: " + e.getMessage());
            }
        }
        return RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR,
                "redirect to consumer failed, channel not found: group=" + group + ", clientId=" + clientId);
    }

    public void register(RemotingClient client, ExecutorService executorService) {
        if (client == null) {
            return;
        }
        client.registerProcessor(RequestCode.CHECK_TRANSACTION_STATE, this, executorService);
        client.registerProcessor(RequestCode.NOTIFY_CONSUMER_IDS_CHANGED, this, executorService);
        client.registerProcessor(RequestCode.RESET_CONSUMER_CLIENT_OFFSET, this, executorService);
        client.registerProcessor(RequestCode.GET_CONSUMER_STATUS_FROM_CLIENT, this, executorService);
        client.registerProcessor(RequestCode.GET_CONSUMER_RUNNING_INFO, this, executorService);
        client.registerProcessor(RequestCode.CONSUME_MESSAGE_DIRECTLY, this, executorService);
        client.registerProcessor(RequestCode.PUSH_REPLY_MESSAGE_TO_CLIENT, this, executorService);
    }

    public void register(ProxyRemotingClient client, ExecutorService executorService) {
        if (client == null) {
            return;
        }
        client.registerProcessor(RequestCode.CHECK_TRANSACTION_STATE, this, executorService);
        client.registerProcessor(RequestCode.NOTIFY_CONSUMER_IDS_CHANGED, this, executorService);
        client.registerProcessor(RequestCode.RESET_CONSUMER_CLIENT_OFFSET, this, executorService);
        client.registerProcessor(RequestCode.GET_CONSUMER_STATUS_FROM_CLIENT, this, executorService);
        client.registerProcessor(RequestCode.GET_CONSUMER_RUNNING_INFO, this, executorService);
        client.registerProcessor(RequestCode.CONSUME_MESSAGE_DIRECTLY, this, executorService);
        client.registerProcessor(RequestCode.PUSH_REPLY_MESSAGE_TO_CLIENT, this, executorService);
    }
}
