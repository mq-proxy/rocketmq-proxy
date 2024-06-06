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

import com.google.common.collect.Sets;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.proxy.broker.BrokerController;
import org.apache.rocketmq.proxy.broker.netty.ProxyRemotingClient;
import org.apache.rocketmq.proxy.proxy.Constants;
import org.apache.rocketmq.proxy.proxy.Proxy;
import org.apache.rocketmq.proxy.proxy.mapping.client.ClientType;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.netty.AsyncNettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

public abstract class AbstractProxyProcessor extends AsyncNettyRequestProcessor implements DefaultRequestProcessor {
    protected final BrokerController brokerController;
    protected static final InternalLogger log = InternalLoggerFactory.getLogger(Constants.PROXY_NAME);
    protected static final long DEFAULT_TIMEOUT_MILLIS = 2000;
    protected static final long DEFAULT_LONG_TIMEOUT_MILLIS = 30000;

    public AbstractProxyProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * sync request, request master by default
     * default timeout: DEFAULT_TIMEOUT_MILLIS
     * @param ctx     channel context
     * @param request remoting request
     * @return remoting response
     */
    public RemotingCommand doProxy(ChannelHandlerContext ctx, RemotingCommand request) {
        return doProxy(ctx, request, false, DEFAULT_TIMEOUT_MILLIS);
    }

    /**
     * sync request
     *
     * @param ctx     channel context
     * @param request remoting request
     * @param slaveFirst whether request from slave first
     * @return remoting response
     */
    public RemotingCommand doProxy(ChannelHandlerContext ctx, RemotingCommand request, boolean slaveFirst) {
        return doProxy(ctx, request, slaveFirst, DEFAULT_TIMEOUT_MILLIS);
    }

    /**
     * sync request
     *
     * @param ctx       channel context
     * @param request      remoting request
     * @param slaveFirst   whether request from slave first
     * @param timeoutMilli timeout in milliseconds
     * @return remoting response
     */
    public RemotingCommand doProxy(ChannelHandlerContext ctx,
                                   RemotingCommand request,
                                   boolean slaveFirst,
                                   long timeoutMilli) {
        return doBrokerDirectSync(ctx, Proxy.getBrokerAddr(slaveFirst), request, timeoutMilli);
    }

    /**
     * async request
     *
     * @param ctx     channel context
     * @param request remoting request
     * @return the Future of response
     */
    public CompletableFuture<RemotingCommand> doProxyAsync(ChannelHandlerContext ctx,
                                                           RemotingCommand request) {
        return doProxyAsync(ctx, request, DEFAULT_LONG_TIMEOUT_MILLIS);
    }

    /**
     * async request
     *
     * @param ignore          channel context
     * @param request      remoting request
     * @param timeoutMilli timeout in milliseconds
     * @return remoting response
     */
    public CompletableFuture<RemotingCommand> doProxyAsync(ChannelHandlerContext ignore,
                                                           RemotingCommand request,
                                                           long timeoutMilli) {
        final int opaque = request.getOpaque();
        CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
        final String addr = Proxy.getBrokerAddr(false);
        try {
            if (addr == null) {
                throw new RemotingSendRequestException("<null>, broker not found");
            }

            request.setOpaque(RemotingCommand.createNewRequestId());
            getRemotingClient(addr, getClientType()).invokeAsync(addr, request, timeoutMilli, responseFuture -> {
                // timeout
                if (responseFuture.isTimeout()) {
                    log.warn("async request timeout, addr: {}, request: {}, timeout: {}",
                            addr, request, timeoutMilli);
                    future.cancel(true);
                    return;
                }
                RemotingCommand response = responseFuture.getResponseCommand();
                // null
                if (response == null) {
                    response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "broker return null");
                    log.warn("response null, addr: {}, request: {}, timeout: {}",
                            addr, request, timeoutMilli);
                }
                response.setOpaque(opaque);
                future.complete(response);
            });
        } catch (InterruptedException | RemotingConnectException | RemotingSendRequestException |
                 RemotingTimeoutException | RemotingTooMuchRequestException e) {
            RemotingCommand response = RemotingCommand.createResponseCommand(Constants.PROXY_ERROR, e.getMessage());
            response.setOpaque(opaque);
            return CompletableFuture.completedFuture(response);
        }
        return future;
    }

    /**
     * request broker
     *
     * @param ctx        channel context
     * @param brokerAddr broker address
     * @param request    remoting request
     * @return remoting response
     */
    protected RemotingCommand doBrokerDirectSync(ChannelHandlerContext ctx, String brokerAddr, RemotingCommand request) {
        return doBrokerDirectSync(ctx, brokerAddr, request, DEFAULT_TIMEOUT_MILLIS);
    }

    /**
     * request broker
     *
     * @param ignore        channel context
     * @param brokerAddr broker address
     * @param request    remoting request
     * @param timeoutMilli timeout in milliseconds
     * @return remoting response
     */
    protected RemotingCommand doBrokerDirectSync(ChannelHandlerContext ignore,
                                                 String brokerAddr,
                                                 RemotingCommand request,
                                                 long timeoutMilli) {
        final int opaque = request.getOpaque();
        RemotingCommand response;
        try {
            if (brokerAddr == null) {
                throw new RemotingSendRequestException("<null>, broker not found");
            }
            request.setOpaque(RemotingCommand.createNewRequestId());
            if (request.isOnewayRPC()) {
                getRemotingClient(brokerAddr).invokeOneway(brokerAddr, request, timeoutMilli);
                response = RemotingCommand.createResponseCommand(0, "");
            } else {
                response = getRemotingClient(brokerAddr).invokeSync(brokerAddr, request, timeoutMilli);
            }
            response.setOpaque(opaque);
        } catch (InterruptedException | RemotingConnectException | RemotingSendRequestException |
                 RemotingTimeoutException | RemotingTooMuchRequestException e) {
            log.warn("send to broker failed: ", e);
            response = RemotingCommand.createResponseCommand(Constants.PROXY_ERROR, e.getMessage());
            response.setOpaque(opaque);
        }
        return response;
    }

    protected RemotingCommand sendHeartBeat2Brokers(RemotingCommand request, Channel clientChannel, boolean send) {
        int opaque = request.getOpaque();
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "");
        try {
            Set<String> addrSet = Proxy.getAllBrokerAddresses();
            if (addrSet.isEmpty()) {
                throw new RemotingSendRequestException("<null>");
            }
            response = sendHeartBeat(addrSet, clientChannel, request, send);
        } catch (InterruptedException | RemotingConnectException | RemotingSendRequestException |
                 RemotingTimeoutException e) {
            response = RemotingCommand.createResponseCommand(Constants.PROXY_ERROR, e.getMessage());
        }
        response.setOpaque(opaque);

        return response;
    }

    protected RemotingCommand sendHeartBeat2AllMasters(Channel clientChannel, RemotingCommand request, boolean send) {
        int opaque = request.getOpaque();
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "");
        try {
            String addr = Proxy.getMasterBrokerAddr();
            response = sendHeartBeat(Sets.newHashSet(addr), clientChannel, request, send);
        } catch (InterruptedException | RemotingConnectException | RemotingSendRequestException |
                 RemotingTimeoutException e) {
            response = RemotingCommand.createResponseCommand(Constants.PROXY_ERROR, e.getMessage());
        }
        // request opaqueID
        response.setOpaque(opaque);

        return response;
    }

    /**
     * send request by heartbeat channel
     *
     * @param addrSet broker addresses
     * @param clientChannel client channel, just use as key
     * @param request remoting request
     * @return remoting response
     */
    private RemotingCommand sendHeartBeat(Set<String> addrSet, Channel clientChannel, RemotingCommand request, boolean send)
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "");
        for (String addr : addrSet) {
            request.setOpaque(RemotingCommand.createNewRequestId());
            if (send) {
                response = getHeartBeatClient(addr).invokeSync(clientChannel, request, DEFAULT_TIMEOUT_MILLIS);
                if (response.getCode() != ResponseCode.SUCCESS) {
                    return response;
                }
            }
        }

        return response;
    }

    protected RemotingClient getRemotingClient(String addr) {
        return getRemotingClient(addr, null);
    }

    protected RemotingClient getRemotingClient(String addr, ClientType type) {
        RemotingClient client = this.brokerController.getProxyController().getClusterMapping()
                .getBrokerAddressRemotingClientByAddr(addr == null ? "" : addr, type == null ? getClientType() : type);
        if (client == null) {
            // use brokerController.client as default client
            return brokerController.getRemotingClient();
        }
        return client;
    }

    protected ProxyRemotingClient getHeartBeatClient(String addr) {
        return brokerController.getProxyController()
                .getClusterMapping()
                .getHeartBeatClientByAddr(addr);
    }

    protected RemotingCommand getUnknownCmdResponse(ChannelHandlerContext ignore, RemotingCommand request) {
        String error = " request type " + request.getCode() + " not supported";
        return RemotingCommand.createResponseCommand(ResponseCode.REQUEST_CODE_NOT_SUPPORTED, error);
    }

    protected ClientType getClientType() {
        return ClientType.DEFAULT;
    }
}
