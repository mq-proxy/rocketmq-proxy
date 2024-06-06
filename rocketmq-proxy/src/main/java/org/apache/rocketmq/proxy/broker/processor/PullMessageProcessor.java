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
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.PullMessageResponseHeader;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.proxy.broker.BrokerController;
import org.apache.rocketmq.proxy.broker.mqtrace.ConsumeMessageContext;
import org.apache.rocketmq.proxy.broker.mqtrace.ConsumeMessageHook;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.proxy.common.log.LoggerName;
import org.apache.rocketmq.proxy.proxy.Proxy;
import org.apache.rocketmq.proxy.proxy.mapping.client.ClientType;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.List;

public class PullMessageProcessor extends AbstractProxyProcessor {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private List<ConsumeMessageHook> consumeMessageHookList;

    public PullMessageProcessor(final BrokerController brokerController) {
        super(brokerController);
    }

    @Override
    public RemotingCommand processRequest(final ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException, 
            RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, 
            RemotingTooMuchRequestException {
        return this.processRequest0(ctx, request);
    }

    @Override
    protected ClientType getClientType() {
        return ClientType.CONSUMER;
    }

    private RemotingCommand processRequest0(final ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException, RemotingConnectException, RemotingSendRequestException, 
            RemotingTimeoutException, InterruptedException, RemotingTooMuchRequestException {
        RemotingCommand response = RemotingCommand.createResponseCommand(PullMessageResponseHeader.class);
        final PullMessageRequestHeader requestHeader =
                (PullMessageRequestHeader) request.decodeCommandCustomHeader(PullMessageRequestHeader.class);

        int requestOpaqueOrigin = request.getOpaque();
        response.setOpaque(requestOpaqueOrigin);

        log.debug("receive PullMessage request command, {}", request);

        final boolean hasSuspendFlag = PullSysFlag.hasSuspendFlag(requestHeader.getSysFlag());

        // get brokerAddr
        final String addr = Proxy.getBrokerAddr(true);
        if (addr == null) {
            String errorInfo = String.format("can't lookup Broker addr by clientAddr: %s", ctx.channel().remoteAddress().toString());
            log.error(errorInfo);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(errorInfo);
            return response;
        }

        // generate new OpaqueID
        int reqOpaque = RemotingCommand.createNewRequestId();
        request.setOpaque(reqOpaque);
        
        long proxySuspendTimeout = this.brokerController.getBrokerConfig().getShortPollingTimeMills() * 2 + 1000;
        if (hasSuspendFlag) {
            // check suspend status every 5s
            proxySuspendTimeout = requestHeader.getSuspendTimeoutMillis() + 5 * 1000 + 1000;
        }
        final long suspendTimeoutMillisLong = proxySuspendTimeout;
        getRemotingClient(addr, getClientType()).invokeAsync(addr, request, suspendTimeoutMillisLong, responseFuture -> {
            if (responseFuture.isTimeout()) {
                log.warn("timeout for pull message, broker: {}, request: {}, timeout: {}",
                    addr, request, suspendTimeoutMillisLong);
                response.setCode(ResponseCode.PULL_NOT_FOUND);
                ctx.writeAndFlush(response);
                return;
            }

            RemotingCommand response1 = responseFuture.getResponseCommand();
            if (response1 == null) {
                log.warn("response null,broker: {}, request: {}",
                        addr, request, suspendTimeoutMillisLong);
                response1 = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "Proxy resposne null");
            }

            response1.setOpaque(requestOpaqueOrigin);
            ctx.writeAndFlush(response1);
        });

        if (this.hasConsumeMessageHook()) {
            ConsumeMessageContext context = new ConsumeMessageContext();
            context.setConsumerGroup(requestHeader.getConsumerGroup());
            context.setTopic(requestHeader.getTopic());
            context.setQueueId(requestHeader.getQueueId());
            
            this.executeConsumeMessageHookBefore(context);
        }
        return null;
    }

    private boolean hasConsumeMessageHook() {
        return consumeMessageHookList != null && !this.consumeMessageHookList.isEmpty();
    }

    private void executeConsumeMessageHookBefore(final ConsumeMessageContext context) {
        if (hasConsumeMessageHook()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageBefore(context);
                } catch (Throwable ignored) {
                }
            }
        }
    }

    public void registerConsumeMessageHook(List<ConsumeMessageHook> consumeMessageHookList) {
        this.consumeMessageHookList = consumeMessageHookList;
    }
}
