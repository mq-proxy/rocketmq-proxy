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

import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.proxy.broker.BrokerController;
import org.apache.rocketmq.proxy.broker.mqtrace.ConsumeMessageHook;
import org.apache.rocketmq.proxy.broker.mqtrace.SendMessageContext;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.proxy.proxy.mapping.client.ClientType;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.RemotingResponseCallback;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class SendMessageProcessor extends AbstractSendMessageProcessor {

    private List<ConsumeMessageHook> consumeMessageHookList;

    public SendMessageProcessor(final BrokerController brokerController) {
        super(brokerController);
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx,
                                          RemotingCommand request) throws RemotingCommandException {
        RemotingCommand response;
        try {
            response = asyncProcessRequest(ctx, request).get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("process SendMessage error, request : " + request, e);
            response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, e.getMessage());
        }
        return response;
    }

    @Override
    public void asyncProcessRequest(ChannelHandlerContext ctx, RemotingCommand request, RemotingResponseCallback responseCallback) throws Exception {
        asyncProcessRequest(ctx, request).thenAcceptAsync(responseCallback::callback, this.brokerController.getPutMessageFutureExecutor());
    }

    public CompletableFuture<RemotingCommand> asyncProcessRequest(ChannelHandlerContext ctx,
                                                                  RemotingCommand request) throws RemotingCommandException {
        final SendMessageContext mqtraceContext;
        if (request.getCode() == RequestCode.CONSUMER_SEND_MSG_BACK) {
            return this.asyncConsumerSendMsgBack(ctx, request);
        }
        SendMessageRequestHeader requestHeader = parseRequestHeader(request);
        if (requestHeader == null) {
            return CompletableFuture.completedFuture(null);
        }
        mqtraceContext = buildMsgContext(ctx, requestHeader);
        this.executeSendMessageHookBefore(ctx, request, mqtraceContext);
        return this.sendMessageToBroker(ctx, request, mqtraceContext, requestHeader);
    }

    private CompletableFuture<RemotingCommand> asyncConsumerSendMsgBack(ChannelHandlerContext ctx,
                                                                        RemotingCommand request) throws RemotingCommandException {
        long start = System.currentTimeMillis();
        return doProxyAsync(ctx, request)
                .thenApply(response -> {
                    this.brokerController.getBrokerStats().recordPut(
                            System.currentTimeMillis() - start,
                            request.getBody() == null ? 0 : request.getBody().length,
                            response.getCode() == ResponseCode.SUCCESS);
                    return response;
                });
    }

    @Override
    protected ClientType getClientType() {
        return ClientType.PRODUCER;
    }
}
