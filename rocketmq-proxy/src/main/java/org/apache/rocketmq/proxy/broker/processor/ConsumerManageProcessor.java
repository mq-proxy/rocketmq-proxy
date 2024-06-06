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
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.proxy.broker.BrokerController;
import org.apache.rocketmq.proxy.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.proxy.proxy.mapping.client.ClientType;
import org.apache.rocketmq.proxy.common.log.LoggerName;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.ArrayList;

import java.util.List;

public class ConsumerManageProcessor extends AbstractProxyProcessor {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    public ConsumerManageProcessor(final BrokerController brokerController) {
        super(brokerController);
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        switch (request.getCode()) {
            case RequestCode.GET_CONSUMER_LIST_BY_GROUP:
                return this.getConsumerListByGroup(ctx, request);
            case RequestCode.UPDATE_CONSUMER_OFFSET:
                return this.updateConsumerOffset(ctx, request);
            case RequestCode.QUERY_CONSUMER_OFFSET:
                return this.queryConsumerOffset(ctx, request);
            default:
                return getUnknownCmdResponse(ctx, request);
        }
    }

    public RemotingCommand getConsumerListByGroup(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {

        return doProxy(ctx, request);
    }

    private List<String> queryAllClientId(String consumerGroup) {
        ConsumerGroupInfo consumerGroupInfo =
                this.brokerController.getConsumerManager().getConsumerGroupInfo(consumerGroup);
        if (consumerGroupInfo == null) {
            return new ArrayList<>(0);
        }
        List<String> clientIds = consumerGroupInfo.getAllClientId();
        return clientIds;
    }

    // just proxy
    private RemotingCommand updateConsumerOffset(ChannelHandlerContext ctx, RemotingCommand request) {
        return doProxy(ctx, request);
    }

    // just proxy
    private RemotingCommand queryConsumerOffset(ChannelHandlerContext ctx, RemotingCommand request) {
        return doProxy(ctx, request);
    }

    @Override
    public ClientType getClientType() {
        return ClientType.ADMIN;
    }
}
