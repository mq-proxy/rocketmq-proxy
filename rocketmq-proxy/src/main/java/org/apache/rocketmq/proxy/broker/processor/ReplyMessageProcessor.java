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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.ReplyMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.proxy.broker.BrokerController;
import org.apache.rocketmq.proxy.broker.mqtrace.SendMessageContext;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeaderV2;
import org.apache.rocketmq.proxy.proxy.Proxy;
import org.apache.rocketmq.proxy.proxy.mapping.client.ClientType;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.MessageExtBrokerInner;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ReplyMessageProcessor extends AbstractSendMessageProcessor {
    public ReplyMessageProcessor(final BrokerController brokerController) {
        super(brokerController);
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        SendMessageContext mqtraceContext;
        SendMessageRequestHeader requestHeader = parseRequestHeader(request);
        if (requestHeader == null) {
            return null;
        }

        mqtraceContext = buildMsgContext(ctx, requestHeader);
        this.executeSendMessageHookBefore(ctx, request, mqtraceContext);
        long start = System.currentTimeMillis();
        RemotingCommand response = this.processReplyMessageRequest(ctx, request, mqtraceContext, requestHeader);
        this.brokerController.getBrokerStats().recordPut(
                System.currentTimeMillis() - start,
                request.getBody().length,
                response.getCode() == ResponseCode.SUCCESS);
        this.executeSendMessageHookAfter(response, mqtraceContext);
        return response;
    }

    @Override
    protected SendMessageRequestHeader parseRequestHeader(RemotingCommand request) throws RemotingCommandException {
        SendMessageRequestHeaderV2 requestHeaderV2 = null;
        SendMessageRequestHeader requestHeader = null;
        switch (request.getCode()) {
            case RequestCode.SEND_REPLY_MESSAGE_V2:
                requestHeaderV2 =
                    (SendMessageRequestHeaderV2) request
                        .decodeCommandCustomHeader(SendMessageRequestHeaderV2.class);
            case RequestCode.SEND_REPLY_MESSAGE:
                if (null == requestHeaderV2) {
                    requestHeader =
                        (SendMessageRequestHeader) request
                            .decodeCommandCustomHeader(SendMessageRequestHeader.class);
                } else {
                    requestHeader = SendMessageRequestHeaderV2.createSendMessageRequestHeaderV1(requestHeaderV2);
                }
            default:
                break;
        }
        return requestHeader;
    }

    private RemotingCommand processReplyMessageRequest(final ChannelHandlerContext ctx,
                                                       final RemotingCommand request,
                                                       final SendMessageContext sendMessageContext,
                                                       final SendMessageRequestHeader requestHeader)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);
        final SendMessageResponseHeader responseHeader = (SendMessageResponseHeader) response.readCustomHeader();

        response.setOpaque(request.getOpaque());

        response.addExtField(MessageConst.PROPERTY_MSG_REGION, this.brokerController.getBrokerConfig().getRegionId());
        response.addExtField(MessageConst.PROPERTY_TRACE_SWITCH, String.valueOf(this.brokerController.getBrokerConfig().isTraceOn()));

        log.debug("receive SendReplyMessage request command, {}", request);

        response.setCode(-1);
        super.msgCheck(ctx, requestHeader, response);
        if (response.getCode() != -1) {
            return response;
        }

        final byte[] body = request.getBody();

        String brokerAddr = Proxy.getMasterBrokerAddr();
        String[] hostPort = brokerAddr.split(":");
        if (hostPort.length < 2) {
            throw new RemotingCommandException("broker addr format error");
        }
        InetSocketAddress storeHost = new InetSocketAddress(hostPort[0], Integer.parseInt(hostPort[1]));

        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(requestHeader.getTopic());
        msgInner.setQueueId(requestHeader.getQueueId());
        msgInner.setBody(body);
        msgInner.setFlag(requestHeader.getFlag());
        MessageAccessor.setProperties(msgInner, MessageDecoder.string2messageProperties(requestHeader.getProperties()));
        msgInner.setPropertiesString(requestHeader.getProperties());
        msgInner.setBornTimestamp(requestHeader.getBornTimestamp());
        msgInner.setBornHost(ctx.channel().remoteAddress());
        msgInner.setStoreHost(storeHost);
        msgInner.setReconsumeTimes(requestHeader.getReconsumeTimes() == null ? 0 : requestHeader.getReconsumeTimes());

        PushReplyResult pushReplyResult = this.pushReplyMessage(ctx, requestHeader, msgInner, brokerAddr);
        if (pushReplyResult.isNeedProxy()) {
            return doProxy(ctx, request);
        }
        this.handlePushReplyResult(pushReplyResult, response, responseHeader, requestHeader.getQueueId());

        try {
            sendMessageToBroker(ctx, request, sendMessageContext, requestHeader).thenApply(brokerResponse -> {
                response.getExtFields().putAll(brokerResponse.getExtFields());
                return brokerResponse;
            }).get(DEFAULT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.warn("save reply message to broker fail", e);
        }
        return response;
    }

    @Override
    protected ClientType getClientType() {
        return ClientType.PRODUCER;
    }

    private PushReplyResult pushReplyMessage(final ChannelHandlerContext ctx,
                                             final SendMessageRequestHeader requestHeader,
                                             final Message msg,
                                             final String storeHost) {
        ReplyMessageRequestHeader replyMessageRequestHeader = new ReplyMessageRequestHeader();
        InetSocketAddress bornAddress = (InetSocketAddress)(ctx.channel().remoteAddress());
        replyMessageRequestHeader.setBornHost(bornAddress.getAddress().getHostAddress() + ":" + bornAddress.getPort());
        replyMessageRequestHeader.setStoreHost(storeHost);
        replyMessageRequestHeader.setStoreTimestamp(System.currentTimeMillis());
        replyMessageRequestHeader.setProducerGroup(requestHeader.getProducerGroup());
        replyMessageRequestHeader.setTopic(requestHeader.getTopic());
        replyMessageRequestHeader.setDefaultTopic(requestHeader.getDefaultTopic());
        replyMessageRequestHeader.setDefaultTopicQueueNums(requestHeader.getDefaultTopicQueueNums());
        replyMessageRequestHeader.setQueueId(requestHeader.getQueueId());
        replyMessageRequestHeader.setSysFlag(requestHeader.getSysFlag());
        replyMessageRequestHeader.setBornTimestamp(requestHeader.getBornTimestamp());
        replyMessageRequestHeader.setFlag(requestHeader.getFlag());
        replyMessageRequestHeader.setProperties(requestHeader.getProperties());
        replyMessageRequestHeader.setReconsumeTimes(requestHeader.getReconsumeTimes());
        replyMessageRequestHeader.setUnitMode(requestHeader.isUnitMode());

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PUSH_REPLY_MESSAGE_TO_CLIENT, replyMessageRequestHeader);
        request.setBody(msg.getBody());

        String senderId = msg.getProperties().get(MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT);
        PushReplyResult pushReplyResult = new PushReplyResult(false);

        if (senderId != null) {
            Channel channel = this.brokerController.getProducerManager().findChannel(senderId);
            if (channel != null) {
                msg.getProperties().put(MessageConst.PROPERTY_PUSH_REPLY_TIME, String.valueOf(System.currentTimeMillis()));
                replyMessageRequestHeader.setProperties(MessageDecoder.messageProperties2String(msg.getProperties()));

                try {
                    RemotingCommand pushResponse = this.brokerController.getBroker2Client().callClient(channel, request);
                    assert pushResponse != null;
                    if (pushResponse.getCode() == ResponseCode.SUCCESS) {
                        pushReplyResult.setPushOk(true);
                    } else {
                        pushReplyResult.setPushOk(false);
                        pushReplyResult.setRemark("push reply message to " + senderId + "fail.");
                        log.warn("push reply message to <{}> return fail, response remark: {}", senderId, pushResponse.getRemark());
                    }
                } catch (RemotingException | InterruptedException e) {
                    pushReplyResult.setPushOk(false);
                    pushReplyResult.setRemark("push reply message to " + senderId + "fail.");
                    log.warn("push reply message to <{}> fail. {}", senderId, channel, e);
                }
            } else {
                // find producer failed, try to proxy to broker
                pushReplyResult.setNeedProxy();
                return pushReplyResult;
            }
        } else {
            log.warn(MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT + " is null, can not reply message");
            pushReplyResult.setPushOk(false);
            pushReplyResult.setRemark("reply message properties[" + MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT + "] is null");
        }
        return pushReplyResult;
    }

    private void handlePushReplyResult(PushReplyResult pushReplyResult, final RemotingCommand response,
                                       final SendMessageResponseHeader responseHeader, int queueIdInt) {

        if (!pushReplyResult.isPushOk()) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(pushReplyResult.getRemark());
        } else {
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            //set to zore to avoid client decoding exception
            responseHeader.setMsgId("0");
            responseHeader.setQueueId(queueIdInt);
            responseHeader.setQueueOffset(0L);
        }
    }

    static class PushReplyResult {
        boolean pushOk = false;
        boolean needProxy = false;
        String remark;

        public PushReplyResult(boolean pushOk) {
            this.pushOk = pushOk;
            remark = "";
        }

        public boolean isPushOk() {
            return pushOk;
        }

        public void setPushOk(boolean pushOk) {
            this.pushOk = pushOk;
        }

        public String getRemark() {
            return remark;
        }

        public void setRemark(String remark) {
            this.remark = remark;
        }

        public void setNeedProxy() {
            needProxy = true;
        }

        public boolean isNeedProxy() {
            return needProxy;
        }
    }
}
